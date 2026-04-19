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

/// Shared intent table on a [`crate::Store`]. Every `WriteTx` holds an
/// `Arc` to the same `IntentMap`. Backed by `DashMap` so per-key intent
/// operations shard across buckets rather than contending on one mutex —
/// important as writer count grows.
#[derive(Default)]
pub(crate) struct IntentMap {
    map: DashMap<IntentKey, (u64, Arc<IntentWaiter>)>,
}

impl IntentMap {
    /// Try to claim `(table, id)` for `writer_id`. If the key is already
    /// held by a different writer, returns that writer's waiter so the
    /// caller can block until the holder finishes. If it's held by
    /// `writer_id` already (repeated write to the same key in one txn),
    /// returns `Ok(())` — idempotent.
    pub(crate) fn try_acquire(
        &self,
        table: &str,
        id: u64,
        writer_id: u64,
        my_waiter: &Arc<IntentWaiter>,
    ) -> Result<(), Arc<IntentWaiter>> {
        let key = (table.to_string(), id);
        // DashMap `entry` locks the bucket, not the whole map. Check-or-
        // insert happens atomically within the bucket.
        match self.map.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(e) => {
                let (holder, w) = e.get();
                if *holder == writer_id {
                    Ok(())
                } else {
                    Err(Arc::clone(w))
                }
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                e.insert((writer_id, Arc::clone(my_waiter)));
                Ok(())
            }
        }
    }

    /// Drop every intent held by `writer_id` and signal the writer's
    /// waiter so blocked retry loops proceed. Called from commit and Drop.
    pub(crate) fn release_all_for(&self, writer_id: u64, my_waiter: &IntentWaiter) {
        self.map.retain(|_k, (holder, _)| *holder != writer_id);
        my_waiter.signal_done();
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
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

    #[test]
    fn acquire_by_different_writer_returns_holders_waiter() {
        let map = IntentMap::default();
        let wa = IntentWaiter::new();
        let wb = IntentWaiter::new();
        map.try_acquire("t", 1, 100, &wa).unwrap();
        let got = map.try_acquire("t", 1, 200, &wb).unwrap_err();
        assert!(Arc::ptr_eq(&got, &wa));
    }

    #[test]
    fn release_clears_keys_and_signals_waiter() {
        let map = IntentMap::default();
        let wa = IntentWaiter::new();
        map.try_acquire("t", 1, 100, &wa).unwrap();
        map.try_acquire("t", 2, 100, &wa).unwrap();
        assert_eq!(map.len(), 2);

        let wa_for_thread = Arc::clone(&wa);
        let handle = std::thread::spawn(move || {
            wa_for_thread.wait();
        });

        map.release_all_for(100, &wa);
        handle.join().unwrap();
        assert_eq!(map.len(), 0);
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
