// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Background segment pre-creator — keeps one preallocated temp segment ready
//! so rotation never zero-fills on the commit path. The etcd `filePipeline`
//! analog. Active only when `JournalConfig.preallocate_segments` is set.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use crate::JournalError;
use crate::journal::segment::SegmentFile;

#[allow(dead_code)]
pub(crate) struct SegmentPipeline {
    shared: Arc<Shared>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

struct Shared {
    dir: PathBuf,
    segment_size: u64,
    counter: AtomicU64,
    shutdown: AtomicBool,
    slot: Mutex<Slot>,
    cv: Condvar,
}

#[derive(Default)]
struct Slot {
    ready: Option<PathBuf>,
    failed: bool,
}

impl SegmentPipeline {
    #[allow(dead_code)]
    pub(crate) fn spawn(
        dir: PathBuf,
        segment_size: u64,
    ) -> Result<Arc<SegmentPipeline>, JournalError> {
        let shared = Arc::new(Shared {
            dir,
            segment_size,
            counter: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            slot: Mutex::new(Slot::default()),
            cv: Condvar::new(),
        });
        let worker = Arc::clone(&shared);
        let handle = thread::spawn(move || preallocator_loop(worker));

        // Pre-warm: block until the first temp is ready (or the worker failed),
        // so the first rotation never stalls.
        {
            let mut slot = shared.slot.lock().unwrap();
            while slot.ready.is_none() && !slot.failed {
                slot = shared.cv.wait(slot).unwrap();
            }
            if slot.failed {
                drop(slot);
                let _ = handle.join();
                return Err(prealloc_failed());
            }
        }
        Ok(Arc::new(SegmentPipeline {
            shared,
            handle: Mutex::new(Some(handle)),
        }))
    }

    /// Take the ready temp (blocking until one exists), then signal the worker
    /// to prepare the next. Returns the temp's path; the caller renames it into
    /// place via `SegmentFile::activate_prealloc_temp`.
    #[allow(dead_code)]
    pub(crate) fn take_ready(&self) -> Result<PathBuf, JournalError> {
        let mut slot = self.shared.slot.lock().unwrap();
        loop {
            if slot.failed {
                return Err(prealloc_failed());
            }
            if let Some(path) = slot.ready.take() {
                // Wake the worker to prepare the next one.
                self.shared.cv.notify_all();
                return Ok(path);
            }
            slot = self.shared.cv.wait(slot).unwrap();
        }
    }

    #[allow(dead_code)]
    pub(crate) fn shutdown(&self) {
        // Hold the slot lock around the store+notify so the worker cannot
        // evaluate its "slot full" predicate between our store and notify,
        // which would cause a lost wakeup and a deadlocked join().
        {
            let _slot = self.shared.slot.lock().unwrap();
            self.shared.shutdown.store(true, Ordering::Release);
            self.shared.cv.notify_all();
        }
        if let Some(h) = self.handle.lock().unwrap().take() {
            let _ = h.join();
        }
        // Remove any temp the worker left ready but unconsumed.
        let mut slot = self.shared.slot.lock().unwrap();
        if let Some(path) = slot.ready.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Drop for SegmentPipeline {
    fn drop(&mut self) {
        // shutdown() is idempotent: the second call (after an explicit
        // shutdown()) re-acquires the lock, stores true again harmlessly,
        // finds no join handle, and finds no ready temp.
        self.shutdown();
    }
}

fn prealloc_failed() -> JournalError {
    JournalError::Io(std::io::Error::other("segment preallocation failed"))
}

fn next_temp_path(shared: &Shared) -> PathBuf {
    let n = shared.counter.fetch_add(1, Ordering::Relaxed);
    shared.dir.join(format!("seg-prealloc.{n}.tmp"))
}

fn preallocator_loop(shared: Arc<Shared>) {
    loop {
        if shared.shutdown.load(Ordering::Acquire) {
            return;
        }
        // Only build a new temp when the slot is empty.
        let need_one = {
            let slot = shared.slot.lock().unwrap();
            slot.ready.is_none() && !slot.failed
        };
        if need_one {
            let path = next_temp_path(&shared);
            match SegmentFile::create_prealloc_temp(&path, shared.segment_size) {
                Ok(()) => {
                    let mut slot = shared.slot.lock().unwrap();
                    slot.ready = Some(path);
                    shared.cv.notify_all();
                }
                Err(_) => {
                    let mut slot = shared.slot.lock().unwrap();
                    slot.failed = true;
                    shared.cv.notify_all();
                    return;
                }
            }
            continue;
        }
        // Slot full (or failed): wait until it's consumed or we're told to stop.
        let mut slot = shared.slot.lock().unwrap();
        while slot.ready.is_some()
            && !shared.shutdown.load(Ordering::Acquire)
            && !slot.failed
        {
            slot = shared.cv.wait(slot).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_hands_off_then_prepares_next() {
        let dir = tempfile::tempdir().unwrap();
        let pipe = SegmentPipeline::spawn(dir.path().to_path_buf(), 256 * 1024).unwrap();

        let first = pipe.take_ready().unwrap();
        assert!(first.exists(), "first temp ready after spawn");
        assert_eq!(first.metadata().unwrap().len(), 256 * 1024, "preallocated");

        // Next must be prepared in the background; a second take_ready returns a
        // DIFFERENT path.
        let second = pipe.take_ready().unwrap();
        assert!(second.exists());
        assert_ne!(first, second, "pipeline prepared a fresh temp");

        pipe.shutdown();
    }

    #[test]
    fn shutdown_removes_unconsumed_temp() {
        let dir = tempfile::tempdir().unwrap();
        let pipe = SegmentPipeline::spawn(dir.path().to_path_buf(), 256 * 1024).unwrap();
        pipe.shutdown();
        // After shutdown no ready temp should linger on disk.
        let leftover: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let n = e.file_name();
                let s = n.to_string_lossy();
                s.starts_with("seg-prealloc.") && s.ends_with(".tmp")
            })
            .collect();
        assert!(leftover.is_empty(), "shutdown cleaned the ready temp");
    }
}
