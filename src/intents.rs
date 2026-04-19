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

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

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
        let mut d = self.done.lock().unwrap();
        *d = true;
        self.cvar.notify_all();
    }

    pub(crate) fn wait(&self) {
        let mut d = self.done.lock().unwrap();
        while !*d {
            d = self.cvar.wait(d).unwrap();
        }
    }

    #[cfg(test)]
    pub(crate) fn is_signaled(&self) -> bool {
        *self.done.lock().unwrap()
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
}

impl std::fmt::Debug for CommitWaiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CommitWaiter(..)")
    }
}

type IntentKey = (String, u64);

/// Per-key state in the intent table.
///
/// `holder == 0` is a sentinel meaning "no active holder, but successors
/// are queued": released by a prior holder with more waiters in line, not
/// yet claimed by the signaled successor. Any writer that sees holder=0
/// may claim the entry in place without being queued — effectively
/// granting the lock to whoever gets there first, which is fine because
/// `release_all_for` already signaled exactly one successor.
///
/// Writer ids are assigned via `fetch_add` starting at 1, so 0 is always
/// a safe sentinel; `SingleWriter` mode never creates intent entries.
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
    /// - If vacant, or marked transitioning (holder=0): claim and return `Ok(())`.
    /// - If held by `writer_id`: `Ok(())` (idempotent).
    /// - Otherwise: push `my_waiter` onto the holder's FIFO queue and
    ///   return `Err(my_waiter)`. The caller blocks on its own waiter
    ///   until the holder releases and pops it from the queue.
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
                } else if entry.holder == 0 {
                    // Transitioning entry — no active holder. Claim it.
                    entry.holder = writer_id;
                    Ok(())
                } else {
                    entry.queue.push_back(Arc::clone(my_waiter));
                    Err(Arc::clone(my_waiter))
                }
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                e.insert(IntentEntry { holder: writer_id, queue: VecDeque::new() });
                Ok(())
            }
        }
    }

    /// Release every intent held by `writer_id`. For each released key
    /// with a non-empty waiter queue, signal exactly one successor (the
    /// head). If more successors remain, mark the entry transitioning
    /// (`holder = 0`) and keep the rest of the queue so they aren't lost.
    /// If the queue is empty after the pop, remove the entry entirely.
    ///
    /// `_my_waiter` is kept in the signature for API compatibility but is
    /// no longer self-signaled: in the fair-queue model, nobody waits on
    /// the holder's own waiter — successors wait on their own.
    pub(crate) fn release_all_for(&self, writer_id: u64, _my_waiter: &IntentWaiter) {
        // Collect successors first, then signal outside the DashMap
        // retain closure so signal_done's downstream work (cvar notify +
        // wake) doesn't happen under any bucket lock.
        let mut to_signal: Vec<Arc<IntentWaiter>> = Vec::new();
        self.map.retain(|_k, entry| {
            if entry.holder != writer_id {
                return true;
            }
            match entry.queue.pop_front() {
                Some(successor) => {
                    if entry.queue.is_empty() {
                        // Final successor; drop the entry. The signaled
                        // successor's retry will either find the key free
                        // and claim it, or find a new holder and queue up.
                        to_signal.push(successor);
                        false
                    } else {
                        // More waiters in line. Mark transitioning so the
                        // signaled successor can claim via the holder=0
                        // path without being cut in front of.
                        entry.holder = 0;
                        to_signal.push(successor);
                        true
                    }
                }
                None => {
                    // No waiters; just remove.
                    false
                }
            }
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
        assert!(Arc::ptr_eq(&got, &wb), "caller should receive its own waiter");
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

    /// Release with multiple queued successors: signals only the head;
    /// entry stays with holder=0 (transitioning) carrying remaining queue.
    #[test]
    fn release_with_multiple_waiters_signals_head_only() {
        let map = IntentMap::default();
        let wa = IntentWaiter::new();
        let wb = IntentWaiter::new();
        let wc = IntentWaiter::new();
        map.try_acquire("t", 1, 100, &wa).unwrap();
        let _ = map.try_acquire("t", 1, 200, &wb).unwrap_err();
        let _ = map.try_acquire("t", 1, 300, &wc).unwrap_err();
        assert_eq!(map.queue_len("t", 1), 2);

        map.release_all_for(100, &wa);
        // Head (wb) was popped and signaled.
        assert!(wb.is_signaled(), "head successor wb should be signaled");
        assert!(!wc.is_signaled(), "queued successor wc should NOT be signaled yet");
        // Entry still present with wc in queue.
        assert_eq!(map.queue_len("t", 1), 1);
    }

    /// A new acquirer arriving while the entry is in the transitioning
    /// state (holder=0) claims it in place and preserves the queue.
    #[test]
    fn new_acquirer_claims_transitioning_entry_and_preserves_queue() {
        let map = IntentMap::default();
        let wa = IntentWaiter::new();
        let wb = IntentWaiter::new();
        let wc = IntentWaiter::new();
        let wd = IntentWaiter::new();
        map.try_acquire("t", 1, 100, &wa).unwrap();
        let _ = map.try_acquire("t", 1, 200, &wb).unwrap_err();
        let _ = map.try_acquire("t", 1, 300, &wc).unwrap_err();
        map.release_all_for(100, &wa); // wb signaled, entry holder=0, queue=[wc]

        // Writer 400 arrives, claims the transitioning entry.
        map.try_acquire("t", 1, 400, &wd).unwrap();
        assert_eq!(map.queue_len("t", 1), 1, "queue must survive the claim");

        // When 400 releases, wc (still queued) is signaled.
        let wc_for_thread = Arc::clone(&wc);
        let handle = std::thread::spawn(move || wc_for_thread.wait());
        map.release_all_for(400, &wd);
        handle.join().unwrap();
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
