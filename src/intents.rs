// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Write-intent table for early-fail conflict detection.
//!
//! In [`crate::WriterMode::MultiWriter`], `TableWriter::update` and
//! `TableWriter::delete` register a write intent on `(table, id)` before
//! mutating the writer's local dirty copy. If a concurrent writer already
//! holds that intent, the mutation fails immediately with
//! [`crate::Error::WriteConflict`] carrying a [`CommitWaiter`] for the
//! current holder — the retry loop blocks on the waiter until the holder
//! commits or aborts, then retries against a fresh base.
//!
//! This converts the wasted work of "run the whole transaction, then
//! abort at commit" into "fail on the first conflicting write, synchronize
//! with the winner, retry once." Holding an intent never blocks anyone —
//! the failing writer bails immediately and waits outside the txn, so
//! deadlock is impossible.
//!
//! Intents are released on commit (signal waiters, let successors retry
//! against the new snapshot) or on drop (WriteTx rolled back without
//! committing — same release path).
//!
//! [`CommitWaiter::wait`] blocks unbounded — correct only under the
//! drop-before-wait convention above. When a caller can't guarantee it
//! released its own intents first (or wants to survive a wedged holder),
//! use [`CommitWaiter::wait_timeout`] instead: it returns `true` when the
//! holder finished (rebase and retry) and `false` on timeout, so a violated
//! convention surfaces as a recoverable error rather than a hang. Sketch:
//!
//! ```ignore
//! loop {
//!     match do_write(&store) {
//!         Ok(v) => break v,
//!         Err(Error::WriteConflict { wait_for: Some(w), .. }) => {
//!             // WriteTx already dropped by `do_write` returning Err.
//!             if !w.wait_timeout(Duration::from_secs(5)) {
//!                 return Err(/* gave up: conflicting writer still live */);
//!             }
//!             // holder finished — retry against a fresh base
//!         }
//!         Err(Error::WriteConflict { wait_for: None, .. }) => { /* retry */ }
//!         Err(e) => return Err(e),
//!     }
//! }
//! ```

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::{Condvar, Mutex};

use dashmap::DashMap;

/// Internal per-writer "done" signal. One is created per `WriteTx` and
/// stored alongside every intent that writer holds; waiters block on it
/// until the writer commits or aborts.
#[derive(Debug)]
pub(crate) struct IntentWaiter {
    done: Mutex<bool>,
    cvar: Condvar,
}

impl IntentWaiter {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            done: Mutex::new(false),
            cvar: Condvar::new(),
        })
    }

    /// Wake every thread blocked in `wait()`. Idempotent: calling twice is
    /// harmless (the flag latches true).
    pub(crate) fn signal_done(&self) {
        let mut d = self.done.lock();
        *d = true;
        self.cvar.notify_all();
    }

    pub(crate) fn wait(&self) {
        let mut d = self.done.lock();
        while !*d {
            self.cvar.wait(&mut d);
        }
    }

    /// Block until signaled or until `timeout` elapses. Returns `true` if
    /// the writer signaled done (safe to retry), `false` on timeout.
    ///
    /// The deadline is computed once so a spurious wakeup re-waits against
    /// the same instant rather than restarting the full timeout — total
    /// wall-time stays bounded by `timeout` regardless of spurious wakes.
    pub(crate) fn wait_timeout_inner(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut d = self.done.lock();
        while !*d {
            if self.cvar.wait_until(&mut d, deadline).timed_out() {
                // On deadline, return the latched flag: a signal that lands
                // exactly at the deadline still reports true.
                return *d;
            }
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn is_signaled(&self) -> bool {
        *self.done.lock()
    }
}

/// Opaque handle returned via [`crate::Error::WriteConflict`] when the
/// conflict was detected by the intent table. Call [`CommitWaiter::wait`]
/// to block until the conflicting writer commits or aborts, then retry.
#[derive(Clone)]
pub struct CommitWaiter(pub(crate) Arc<IntentWaiter>);

impl CommitWaiter {
    /// Block until the conflicting writer commits or is dropped. Returns
    /// immediately if that already happened.
    pub fn wait(&self) {
        self.0.wait();
    }

    /// Block until the conflicting writer commits or aborts, or until
    /// `timeout` elapses. Returns `true` if the writer finished (safe to
    /// rebase and retry), `false` on timeout — in which case the holder is
    /// still live: you likely violated the "drop your WriteTx before
    /// waiting" convention, or the holder is wedged. Returns immediately if
    /// the writer already finished.
    pub fn wait_timeout(&self, timeout: std::time::Duration) -> bool {
        self.0.wait_timeout_inner(timeout)
    }
}

impl std::fmt::Debug for CommitWaiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CommitWaiter(..)")
    }
}

type IntentKey = (String, u64);

/// Per-key state in the intent table. `holder` is the writer id currently
/// owning `(table, id)`; `queue` holds the waiters that conflicted with it.
/// `release_all_for` always empties the queue (waking every waiter) and
/// removes the entry, so `holder` is only ever a live writer id (≥1) — there
/// is no "no holder but queued" transitioning state.
struct IntentEntry {
    holder: u64,
    queue: VecDeque<Arc<IntentWaiter>>,
}

/// Shared intent table on a [`crate::Store`]. Every `WriteTx` holds an
/// `Arc` to the same `IntentMap`. Backed by `DashMap` so per-key intent
/// operations shard across buckets rather than contending on one mutex.
///
/// On conflict, the caller's waiter is pushed to the holder's per-key
/// FIFO queue, and `try_acquire` returns that waiter. The caller blocks
/// on its OWN waiter (not the holder's). When the holder releases, only
/// the head of the queue is signaled — no thundering herd.
#[derive(Default)]
pub(crate) struct IntentMap {
    map: DashMap<IntentKey, IntentEntry>,
}

impl IntentMap {
    /// Try to claim `(table, id)` for `writer_id`.
    ///
    /// - If vacant: claim and return `Ok(())`.
    /// - If held by `writer_id`: `Ok(())` (idempotent).
    /// - Otherwise: push `my_waiter` onto the holder's FIFO queue and
    ///   return `Err(my_waiter)`. The caller blocks on its own waiter
    ///   until the holder releases and wakes the queue.
    pub(crate) fn try_acquire(
        &self,
        table: &str,
        id: u64,
        writer_id: u64,
        my_waiter: &Arc<IntentWaiter>,
    ) -> Result<(), Arc<IntentWaiter>> {
        let key = (table.to_string(), id);
        match self.map.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.holder == writer_id {
                    Ok(())
                } else {
                    entry.queue.push_back(Arc::clone(my_waiter));
                    Err(Arc::clone(my_waiter))
                }
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                e.insert(IntentEntry {
                    holder: writer_id,
                    queue: VecDeque::new(),
                });
                Ok(())
            }
        }
    }

    /// Release every intent held by `writer_id`, waking **all** queued
    /// waiters on each released key and removing the entry.
    ///
    /// Signalling only the head and keeping the rest queued (behind a
    /// `holder = 0` "transitioning" entry) deadlocks: a queued waiter belongs
    /// to a transaction that was already dropped before it began waiting
    /// (drop-before-wait), so no waiter can be promoted to holder — it would
    /// never release — and the leftover queue could only be drained by a
    /// *future* acquirer touching the same key. If every other thread is
    /// itself parked as such a waiter, no future acquirer exists and the
    /// system hangs forever. Waking all contenders lets them re-race on
    /// retry: whoever re-acquires the key first becomes the new holder and
    /// the rest re-queue behind it. Not FIFO-fair, but liveness-safe — a
    /// released key never leaves behind a waiter only a future acquirer
    /// could reach.
    ///
    /// `_my_waiter` is kept in the signature for API compatibility; nobody
    /// waits on the holder's own waiter (successors wait on their own).
    pub(crate) fn release_all_for(&self, writer_id: u64, _my_waiter: &IntentWaiter) {
        // Collect successors first, then signal outside the DashMap retain
        // closure so signal_done's downstream work (cvar notify + wake)
        // doesn't happen under any bucket lock.
        let mut to_signal: Vec<Arc<IntentWaiter>> = Vec::new();
        self.map.retain(|_k, entry| {
            if entry.holder != writer_id {
                return true;
            }
            to_signal.extend(entry.queue.drain(..));
            false
        });
        for w in to_signal {
            w.signal_done();
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    #[cfg(test)]
    pub(crate) fn queue_len(&self, table: &str, id: u64) -> usize {
        self.map
            .get(&(table.to_string(), id))
            .map(|e| e.queue.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn wait_timeout_returns_true_when_signaled_before_deadline() {
        let w = IntentWaiter::new();
        let w2 = Arc::clone(&w);
        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            w2.signal_done();
        });
        let start = Instant::now();
        let signaled = w.wait_timeout_inner(Duration::from_secs(5));
        assert!(signaled, "should observe the signal");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "should return promptly after signal, took {:?}",
            start.elapsed()
        );
        h.join().unwrap();
    }

    #[test]
    fn wait_timeout_returns_false_on_timeout_and_is_bounded() {
        let w = IntentWaiter::new();
        let start = Instant::now();
        let signaled = w.wait_timeout_inner(Duration::from_millis(50));
        let elapsed = start.elapsed();
        assert!(!signaled, "unsignaled waiter must time out");
        assert!(
            elapsed >= Duration::from_millis(50),
            "must not return before the timeout, took {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "must not hang, took {elapsed:?}"
        );
    }

    #[test]
    fn wait_timeout_wakes_early_when_signaled_mid_wait() {
        let w = IntentWaiter::new();
        let w2 = Arc::clone(&w);
        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            w2.signal_done();
        });
        let start = Instant::now();
        let signaled = w.wait_timeout_inner(Duration::from_secs(10));
        assert!(signaled);
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "woke on signal, not deadline: {:?}",
            start.elapsed()
        );
        h.join().unwrap();
    }

    #[test]
    fn wait_timeout_already_signaled_returns_immediately() {
        let w = IntentWaiter::new();
        w.signal_done();
        let start = Instant::now();
        let signaled = w.wait_timeout_inner(Duration::from_millis(0));
        assert!(signaled, "already-signaled waiter returns true");
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn commit_waiter_wait_timeout_wraps_inner() {
        let w = IntentWaiter::new();
        let cw = CommitWaiter(Arc::clone(&w));
        assert!(
            !cw.wait_timeout(Duration::from_millis(20)),
            "not signaled → false"
        );
        w.signal_done();
        assert!(
            cw.wait_timeout(Duration::from_millis(20)),
            "signaled → true"
        );
    }

    #[test]
    fn acquire_twice_by_same_writer_is_ok() {
        let map = IntentMap::default();
        let w = IntentWaiter::new();
        assert!(map.try_acquire("t", 1, 100, &w).is_ok());
        assert!(map.try_acquire("t", 1, 100, &w).is_ok());
        assert_eq!(map.len(), 1);
    }

    /// Fair queue: a conflicting caller gets back its OWN waiter, and
    /// its waiter is queued behind the holder.
    #[test]
    fn conflict_returns_callers_own_waiter_and_queues() {
        let map = IntentMap::default();
        let wa = IntentWaiter::new();
        let wb = IntentWaiter::new();
        map.try_acquire("t", 1, 100, &wa).unwrap();
        let got = map.try_acquire("t", 1, 200, &wb).unwrap_err();
        assert!(
            Arc::ptr_eq(&got, &wb),
            "caller should receive its own waiter"
        );
        assert_eq!(map.queue_len("t", 1), 1);
    }

    /// Release with only one queued successor: pops and signals it,
    /// removes the entry entirely (no lingering transitioning state).
    #[test]
    fn release_signals_one_successor_and_removes_entry() {
        let map = IntentMap::default();
        let wa = IntentWaiter::new();
        let wb = IntentWaiter::new();
        map.try_acquire("t", 1, 100, &wa).unwrap();
        let _ = map.try_acquire("t", 1, 200, &wb).unwrap_err();
        assert_eq!(map.queue_len("t", 1), 1);

        let wb_for_thread = Arc::clone(&wb);
        let handle = std::thread::spawn(move || wb_for_thread.wait());

        map.release_all_for(100, &wa);
        handle.join().unwrap();
        assert_eq!(map.len(), 0, "entry should be fully removed");
    }

    /// Release with multiple queued successors: signals ALL of them and
    /// removes the entry (no orphaned waiters, no transitioning state).
    #[test]
    fn release_with_multiple_waiters_signals_all() {
        let map = IntentMap::default();
        let wa = IntentWaiter::new();
        let wb = IntentWaiter::new();
        let wc = IntentWaiter::new();
        map.try_acquire("t", 1, 100, &wa).unwrap();
        let _ = map.try_acquire("t", 1, 200, &wb).unwrap_err();
        let _ = map.try_acquire("t", 1, 300, &wc).unwrap_err();
        assert_eq!(map.queue_len("t", 1), 2);

        map.release_all_for(100, &wa);
        // Every queued successor is signaled; the entry is gone.
        assert!(wb.is_signaled(), "successor wb should be signaled");
        assert!(wc.is_signaled(), "successor wc should be signaled");
        assert_eq!(map.len(), 0, "entry should be removed after release");
    }

    /// After a release wakes the queue, a new acquirer finds the key vacant
    /// and claims it fresh; the woken contenders re-race on retry.
    #[test]
    fn new_acquirer_after_release_claims_fresh() {
        let map = IntentMap::default();
        let wa = IntentWaiter::new();
        let wb = IntentWaiter::new();
        let wc = IntentWaiter::new();
        let wd = IntentWaiter::new();
        map.try_acquire("t", 1, 100, &wa).unwrap();
        let _ = map.try_acquire("t", 1, 200, &wb).unwrap_err();
        let _ = map.try_acquire("t", 1, 300, &wc).unwrap_err();
        map.release_all_for(100, &wa); // wb and wc both signaled, entry removed

        assert!(wb.is_signaled() && wc.is_signaled());
        assert_eq!(map.len(), 0);

        // A fresh acquirer claims the now-vacant key without being queued.
        map.try_acquire("t", 1, 400, &wd).unwrap();
        assert_eq!(map.queue_len("t", 1), 0);
    }

    /// Root-cause regression (SSI+scan quiescence deadlock): when a holder
    /// releases a key with MULTIPLE queued waiters, every queued waiter must
    /// become signalable without requiring a *new* acquirer to touch the key.
    /// Queued waiters belong to already-dropped transactions (drop-before-wait),
    /// so a woken successor is under no obligation to re-acquire the key; if the
    /// remainder of the queue could only be drained by a future acquirer, a
    /// quiesced system (all threads parked as waiters) deadlocks forever.
    #[test]
    fn release_does_not_orphan_queued_waiters() {
        let map = IntentMap::default();
        let wa = IntentWaiter::new();
        let wb = IntentWaiter::new();
        let wc = IntentWaiter::new();
        map.try_acquire("t", 1, 100, &wa).unwrap();
        map.try_acquire("t", 1, 200, &wb).unwrap_err();
        map.try_acquire("t", 1, 300, &wc).unwrap_err(); // queue = [wb, wc]

        // Holder releases. No further try_acquire on ("t",1) will happen
        // (simulating quiescence — every other thread is a parked waiter).
        map.release_all_for(100, &wa);

        // Both queued waiters must already be signaled. wait_timeout turns the
        // deadlock into a bounded, observable failure instead of hanging the
        // test runner.
        assert!(
            wb.wait_timeout_inner(Duration::from_secs(2)),
            "head waiter wb must be signaled"
        );
        assert!(
            wc.wait_timeout_inner(Duration::from_secs(2)),
            "queued waiter wc must not be orphaned by the release"
        );
    }

    #[test]
    fn other_writers_intents_untouched_by_release() {
        let map = IntentMap::default();
        let wa = IntentWaiter::new();
        let wb = IntentWaiter::new();
        map.try_acquire("t", 1, 100, &wa).unwrap();
        map.try_acquire("t", 2, 200, &wb).unwrap();
        map.release_all_for(100, &wa);
        assert_eq!(map.len(), 1);
    }
}
