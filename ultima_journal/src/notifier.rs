// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Signalling primitive shared by Journal and StableValue.
//!
//! Returned by mutating ops (`append`, `truncate_after`, `store`). Either
//! `wait()` (blocking) or `on_complete(callback)` to be notified of durability.

use std::sync::{Arc, Condvar, Mutex};

use crate::JournalError;

type Callback = Box<dyn FnOnce(Result<(), JournalError>) + Send + 'static>;

enum State {
    Pending(Vec<Callback>),
    Done(Result<(), JournalError>),
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Pending(_) => f.debug_tuple("Pending").field(&"<callbacks>").finish(),
            State::Done(r) => f.debug_tuple("Done").field(r).finish(),
        }
    }
}

#[derive(Debug)]
struct Inner {
    state: Mutex<State>,
    cv: Condvar,
}

#[derive(Debug)]
pub struct Notifier {
    inner: Arc<Inner>,
}

pub struct Signal {
    inner: Arc<Inner>,
}

impl Notifier {
    /// Already-resolved Notifier (e.g. `Durability::Eventual` returns this
    /// because nothing async is pending).
    pub fn done() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::Done(Ok(()))),
                cv: Condvar::new(),
            }),
        }
    }

    /// Construct a pending pair: caller holds the `Notifier`, producer holds
    /// the `Signal`.
    pub fn pending() -> (Signal, Notifier) {
        let inner = Arc::new(Inner {
            state: Mutex::new(State::Pending(Vec::new())),
            cv: Condvar::new(),
        });
        (Signal { inner: Arc::clone(&inner) }, Notifier { inner })
    }

    pub fn is_done(&self) -> bool {
        matches!(*self.inner.state.lock().unwrap(), State::Done(_))
    }

    /// Block until done. Returns the stored result.
    pub fn wait(self) -> Result<(), JournalError> {
        let mut guard = self.inner.state.lock().unwrap();
        loop {
            match &*guard {
                State::Done(r) => return r.clone(),
                State::Pending(_) => {
                    guard = self.inner.cv.wait(guard).unwrap();
                }
            }
        }
    }

    /// Register a callback. If already done, fires inline on the calling
    /// thread. Otherwise stored and fired on the producer's `Signal::complete`.
    pub fn on_complete<F>(self, f: F)
    where
        F: FnOnce(Result<(), JournalError>) + Send + 'static,
    {
        let mut guard = self.inner.state.lock().unwrap();
        match &*guard {
            State::Done(r) => {
                let r = r.clone();
                drop(guard);
                f(r);
            }
            State::Pending(_) => {
                if let State::Pending(cbs) = &mut *guard {
                    cbs.push(Box::new(f));
                }
            }
        }
    }
}

impl Signal {
    /// Mark complete, wake all waiters, fire all callbacks. Double-complete
    /// is a no-op.
    pub fn complete(self, result: Result<(), JournalError>) {
        let cbs = {
            let mut guard = self.inner.state.lock().unwrap();
            let cbs = match std::mem::replace(&mut *guard, State::Done(result.clone())) {
                State::Pending(cbs) => cbs,
                State::Done(_) => Vec::new(),
            };
            self.inner.cv.notify_all();
            cbs
        };
        for cb in cbs {
            cb(result.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn already_done_returns_immediately() {
        let n = Notifier::done();
        assert!(n.is_done());
        n.wait().unwrap();
    }

    #[test]
    fn signal_unblocks_waiter() {
        let (signal, n) = Notifier::pending();
        let h = thread::spawn(move || n.wait().unwrap());
        thread::sleep(Duration::from_millis(20));
        signal.complete(Ok(()));
        h.join().unwrap();
    }

    #[test]
    fn on_complete_fires_inline_when_already_done() {
        let n = Notifier::done();
        let fired = Arc::new(AtomicBool::new(false));
        let f2 = Arc::clone(&fired);
        n.on_complete(move |r| {
            r.unwrap();
            f2.store(true, Ordering::SeqCst);
        });
        assert!(fired.load(Ordering::SeqCst));
    }

    #[test]
    fn on_complete_fires_when_signal_arrives() {
        let (signal, n) = Notifier::pending();
        let fired = Arc::new(AtomicBool::new(false));
        let f2 = Arc::clone(&fired);
        n.on_complete(move |r| {
            r.unwrap();
            f2.store(true, Ordering::SeqCst);
        });
        assert!(!fired.load(Ordering::SeqCst));
        signal.complete(Ok(()));
        assert!(fired.load(Ordering::SeqCst));
    }

    #[test]
    fn signal_with_error_propagates() {
        let (signal, n) = Notifier::pending();
        signal.complete(Err(crate::JournalError::Closed));
        assert!(matches!(n.wait(), Err(crate::JournalError::Closed)));
    }

    #[test]
    fn debug_prints_state_variants() {
        let n = Notifier::done();
        let s = format!("{:?}", n);
        assert!(s.contains("Notifier"));
        let (_signal, n2) = Notifier::pending();
        let s = format!("{:?}", n2);
        assert!(s.contains("Notifier"));
    }
}
