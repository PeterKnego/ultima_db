// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::notifier::Signal;
use crate::{Durability, JournalError};

use super::segment::SegmentFile;

pub(crate) struct AppendRequest {
    pub seq: u64,
    pub meta: u64,
    pub payload: Vec<u8>,
    pub signal: Signal,
}

// ---------------------------------------------------------------------------
// SeqWatermark — fsync-durable sequence watermark (task28)
// ---------------------------------------------------------------------------

/// A one-shot durability callback, fired with `Ok(())` once the target seq is
/// fsynced, or `Err(_)` if a covering fsync failed / the journal closed first.
type DurabilityCallback = Box<dyn FnOnce(Result<(), JournalError>) + Send>;

/// Tracks the highest seq whose bytes are fsync-durable, and lets a caller wait
/// on (or be notified of) an arbitrary target seq.
///
/// The journal's fsync is a *barrier*: a single `sync_all()` makes every seq
/// written before it durable, not one specific append. So the watermark is
/// advanced to the high-water seq present in the file at fsync time (resolved
/// by the single-threaded writer in [`writer_loop`]), and a parked waiter for
/// seq `s` resolves on the first fsync whose high-water mark reaches `s`.
///
/// Strictly additive: it does not change the existing per-append [`Signal`]
/// semantics (which still fire after the buffered write in Eventual mode).
pub(crate) struct SeqWatermark {
    /// Highest seq known fsync-durable. Monotonic; only ever advances.
    durable_seq: AtomicU64,
    /// Set when the writer thread is gone; releases parked waiters so they
    /// cannot block forever on a seq that will never be reached.
    closed: AtomicBool,
    inner: Mutex<Waiters>,
    condvar: Condvar,
}

#[derive(Default)]
struct Waiters {
    /// Parked callbacks: `(target_seq, callback)`, fired once the watermark
    /// reaches `target_seq` (or an error/close covers it).
    callbacks: Vec<(u64, DurabilityCallback)>,
    /// Sticky fsync error, recorded for the highest seq that failed. Waiters
    /// at/below this seq resolve to `Err`.
    last_error: Option<(u64, JournalError)>,
}

impl SeqWatermark {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            durable_seq: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            inner: Mutex::new(Waiters::default()),
            condvar: Condvar::new(),
        })
    }

    /// Highest seq known to be fsync-durable.
    pub(crate) fn current(&self) -> u64 {
        self.durable_seq.load(Ordering::Acquire)
    }

    /// Called by the writer after a successful fsync. `seq` is the high-water
    /// mark of everything flushed by that fsync.
    pub(crate) fn publish(&self, seq: u64) {
        let ready = {
            let mut w = self.inner.lock().unwrap();
            let new = self.durable_seq.load(Ordering::Acquire).max(seq);
            // Store under the mutex so a concurrent wait/on_complete cannot read
            // the old value and then miss the notify (lost wakeup).
            self.durable_seq.store(new, Ordering::Release);
            self.condvar.notify_all();
            drain_le(&mut w.callbacks, new)
        };
        for cb in ready {
            cb(Ok(()));
        }
    }

    /// Called by the writer when an fsync fails. Waiters at/below `seq` resolve
    /// to `Err`; the watermark is NOT advanced.
    pub(crate) fn publish_error(&self, seq: u64, err: JournalError) {
        let ready = {
            let mut w = self.inner.lock().unwrap();
            match &w.last_error {
                Some((es, _)) if *es >= seq => {}
                _ => w.last_error = Some((seq, clone_err(&err))),
            }
            self.condvar.notify_all();
            drain_le(&mut w.callbacks, seq)
        };
        for cb in ready {
            cb(Err(clone_err(&err)));
        }
    }

    /// Release all parked waiters (the writer thread is gone).
    pub(crate) fn close(&self) {
        let ready = {
            let mut w = self.inner.lock().unwrap();
            self.closed.store(true, Ordering::Release);
            self.condvar.notify_all();
            std::mem::take(&mut w.callbacks)
        };
        for (_, cb) in ready {
            cb(Err(JournalError::Closed));
        }
    }

    /// Block until `seq` is durable. Returns `Err` if a covering fsync failed
    /// or the journal closed first.
    pub(crate) fn wait(&self, seq: u64) -> Result<(), JournalError> {
        let mut guard = self.inner.lock().unwrap();
        loop {
            if self.durable_seq.load(Ordering::Acquire) >= seq {
                return Ok(());
            }
            if let Some((es, err)) = &guard.last_error
                && *es >= seq
            {
                return Err(clone_err(err));
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(JournalError::Closed);
            }
            guard = self.condvar.wait(guard).unwrap();
        }
    }

    /// Register `cb` to fire once `seq` is durable. Fires inline (on the calling
    /// thread) if already durable, already errored, or already closed.
    pub(crate) fn on_complete(&self, seq: u64, cb: DurabilityCallback) {
        let mut w = self.inner.lock().unwrap();
        if self.durable_seq.load(Ordering::Acquire) >= seq {
            drop(w);
            cb(Ok(()));
            return;
        }
        if let Some((es, err)) = &w.last_error
            && *es >= seq
        {
            let err = clone_err(err);
            drop(w);
            cb(Err(err));
            return;
        }
        if self.closed.load(Ordering::Acquire) {
            drop(w);
            cb(Err(JournalError::Closed));
            return;
        }
        w.callbacks.push((seq, cb));
    }
}

/// Remove and return every callback whose target seq is `<= seq`. Order is
/// irrelevant (each callback is independent), so `swap_remove` is fine.
fn drain_le(callbacks: &mut Vec<(u64, DurabilityCallback)>, seq: u64) -> Vec<DurabilityCallback> {
    let mut ready = Vec::new();
    let mut i = 0;
    while i < callbacks.len() {
        if callbacks[i].0 <= seq {
            ready.push(callbacks.swap_remove(i).1);
        } else {
            i += 1;
        }
    }
    ready
}

pub(crate) struct WriterState {
    pub dir: PathBuf,
    pub segment_size: u64,
    pub durability: Durability,
    pub segments: Vec<SegmentFile>,
    pub last_seq: Option<u64>,
    pub first_seq: Option<u64>,
    /// Records that have been `append()`ed but not yet written to a segment by
    /// the bg writer. openraft's `RaftLogStorage::append()` contract requires an
    /// appended entry to be readable the instant `append()` returns — before the
    /// flush callback fires — so `append()` publishes here synchronously and
    /// `read`/`read_range` overlay it on the durable segments. The writer evicts
    /// each seq once it is persisted (both under this same lock, so a seq is in
    /// `pending` XOR a segment, never both and never neither). seq → (meta, payload).
    pub pending: BTreeMap<u64, (u64, Vec<u8>)>,
}

pub(crate) struct Writer {
    pub tx: Sender<AppendRequest>,
    pub handle: Option<JoinHandle<()>>,
}

impl Writer {
    pub fn spawn(state: Arc<Mutex<WriterState>>, watermark: Arc<SeqWatermark>) -> Self {
        let (tx, rx) = channel::<AppendRequest>();
        let handle = thread::spawn(move || writer_loop(rx, state, watermark));
        Self {
            tx,
            handle: Some(handle),
        }
    }
}

fn writer_loop(
    rx: Receiver<AppendRequest>,
    state: Arc<Mutex<WriterState>>,
    watermark: Arc<SeqWatermark>,
) {
    // Durability is fixed at Journal::open and never changes at runtime.
    let durability = state.lock().unwrap().durability;
    let mut last_eventual_fsync = Instant::now();
    let eventual_interval = Duration::from_millis(50);

    loop {
        let timeout = match durability {
            Durability::Consistent => None,
            Durability::Eventual => {
                let elapsed = last_eventual_fsync.elapsed();
                if elapsed >= eventual_interval {
                    // Snapshot the high-water seq, fsync, then publish it — so
                    // the watermark only advances to seqs whose bytes the fsync
                    // definitely flushed (task28). Single-threaded writer, so
                    // last_seq cannot change underneath us here.
                    let hwm = state.lock().unwrap().last_seq;
                    match fsync_active_segment(&state) {
                        Ok(()) => {
                            if let Some(s) = hwm {
                                watermark.publish(s);
                            }
                        }
                        Err(e) => {
                            if let Some(s) = hwm {
                                watermark.publish_error(s, e);
                            }
                        }
                    }
                    last_eventual_fsync = Instant::now();
                }
                // `checked_sub` avoids a panic if the OS preempted us long
                // enough that elapsed > interval between the check above and
                // here. We just fall through with a zero timeout.
                Some(
                    eventual_interval
                        .checked_sub(last_eventual_fsync.elapsed())
                        .unwrap_or(Duration::ZERO),
                )
            }
        };
        let first = match timeout {
            None => match rx.recv() {
                Ok(r) => r,
                Err(_) => break,
            },
            Some(t) => match rx.recv_timeout(t) {
                Ok(r) => r,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(_) => break,
            },
        };
        let mut batch = vec![first];
        // Drain anything already queued — this is the group-commit window.
        while let Ok(req) = rx.try_recv() {
            batch.push(req);
        }
        // Write (no fsync). `write_batch` returns the high-water seq written.
        let write_res = write_batch(&state, &batch);
        // Determine the result reported to per-append Signals, and advance the
        // durable_seq watermark. In Consistent mode the fsync happens here
        // (outside `write_batch`) so we can publish the watermark after it,
        // without holding the state lock during callback fan-out. In Eventual
        // mode the buffered write is signalled immediately and the watermark is
        // advanced later by the idle-timer fsync above.
        let result: Result<(), JournalError> = match durability {
            Durability::Consistent => match write_res {
                Ok(hwm) => match fsync_active_segment(&state) {
                    Ok(()) => {
                        if let Some(s) = hwm {
                            watermark.publish(s);
                        }
                        Ok(())
                    }
                    Err(e) => {
                        if let Some(s) = hwm {
                            watermark.publish_error(s, clone_err(&e));
                        }
                        Err(e)
                    }
                },
                Err(e) => Err(e),
            },
            Durability::Eventual => write_res.map(|_| ()),
        };
        for req in batch {
            req.signal
                .complete(result.as_ref().map(|_| ()).map_err(clone_err));
        }
    }
}

fn write_batch(
    state: &Arc<Mutex<WriterState>>,
    batch: &[AppendRequest],
) -> Result<Option<u64>, JournalError> {
    let mut st = state.lock().unwrap();

    // Records accumulated for the current (last) segment, not yet written.
    let mut run: Vec<(u64, u64, &[u8])> = Vec::new();
    // Projected size of the current segment = on-disk size + bytes buffered in
    // `run`. Drives rotation exactly like the old per-record `seg.size()` check.
    let mut projected: u64 = match st.segments.last() {
        Some(seg) => seg.size()?,
        None => 0,
    };
    // Last seq written so far in THIS batch. `append()` now owns `st.last_seq`
    // (it advances it synchronously, possibly past this batch for entries not yet
    // persisted), so the writer can no longer use `st.last_seq` for its in-batch
    // monotonic guard — it tracks the locally-written high-water instead.
    let mut prev_written: Option<u64> = None;
    for req in batch {
        // Monotonic-seq guard (redundant with append()'s enqueue check, kept as
        // defense; the channel is FIFO and append() validates global
        // monotonicity under the state lock, so this only ever guards against an
        // internal batching bug). On failure flush the good prefix first so
        // earlier records are persisted, matching the old per-record
        // write-then-error behavior. A flush error here supersedes the guard
        // error — acceptable, as the segment I/O failure is more fundamental.
        if let Some(last) = prev_written
            && req.seq <= last
        {
            flush_run(&mut st, &mut run)?;
            return Err(JournalError::NonMonotonicSeq {
                expected_gt: last,
                got: req.seq,
            });
        }
        let body_len = 16 + req.payload.len();
        let total = (4 + body_len + 4) as u64;
        if total > st.segment_size {
            flush_run(&mut st, &mut run)?;
            return Err(JournalError::PayloadTooLargeForSegment {
                segment_size: st.segment_size,
                record_size: total,
            });
        }

        // Rotate when there is no segment yet, or the current one is full.
        let need_new = st.segments.is_empty() || projected >= st.segment_size;
        if need_new {
            // Flush whatever was accumulated for the now-full segment first.
            flush_run(&mut st, &mut run)?;
            let path = st.dir.join(format!("seg-{:020}.log", req.seq));
            st.segments.push(SegmentFile::create(&path, req.seq)?);
            projected = st.segments.last().unwrap().size()?;
        }

        run.push((req.seq, req.meta, &req.payload));
        projected += total;
        prev_written = Some(req.seq);
    }

    // Flush the final run for the active segment.
    flush_run(&mut st, &mut run)?;
    // The fsync (Consistent) is issued by the caller so the durable_seq watermark
    // can be published after it, outside this lock. Return the high-water seq
    // PERSISTED by this batch — NOT `st.last_seq`, which `append()` may have
    // advanced past this batch for entries not yet written; using it would
    // over-report durability. On the Ok path the whole batch was flushed, so the
    // last entry is the persisted high-water.
    Ok(prev_written)
}

/// Write all accumulated records to the active segment with a single
/// coalesced `write_all`, then clear the run. No-op on an empty run.
fn flush_run(
    st: &mut WriterState,
    run: &mut Vec<(u64, u64, &[u8])>,
) -> Result<(), JournalError> {
    if run.is_empty() {
        return Ok(());
    }
    {
        let seg = st
            .segments
            .last_mut()
            .expect("flush_run called with a non-empty run but no active segment");
        seg.append_records(run.as_slice())?;
    }
    // These records are now segment-readable, so evict them from the in-memory
    // overlay `append()` populated — under the same state lock, so a seq is in a
    // segment XOR `pending`, never double-counted by a concurrent read. Only
    // successfully-persisted records are evicted (on an `append_records` error we
    // returned above, leaving them visible-but-unpersisted, as the failed write
    // demands).
    for (seq, _, _) in run.iter() {
        st.pending.remove(seq);
    }
    run.clear();
    Ok(())
}

fn fsync_active_segment(state: &Arc<Mutex<WriterState>>) -> Result<(), JournalError> {
    let mut st = state.lock().unwrap();
    if let Some(seg) = st.segments.last_mut() {
        seg.fsync()?;
    }
    Ok(())
}

pub(crate) fn clone_err(e: &JournalError) -> JournalError {
    use JournalError::*;
    match e {
        Io(io) => Io(std::io::Error::new(io.kind(), io.to_string())),
        Corrupted {
            segment,
            offset,
            reason,
        } => Corrupted {
            segment: segment.clone(),
            offset: *offset,
            reason: reason.clone(),
        },
        NonMonotonicSeq { expected_gt, got } => NonMonotonicSeq {
            expected_gt: *expected_gt,
            got: *got,
        },
        SeqOutOfRange => SeqOutOfRange,
        PayloadTooLargeForSegment {
            segment_size,
            record_size,
        } => PayloadTooLargeForSegment {
            segment_size: *segment_size,
            record_size: *record_size,
        },
        Closed => Closed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A failed fsync poisons waiters at/below the attempted high-water seq
    /// without advancing the watermark; a later success still advances.
    #[test]
    fn publish_error_poisons_without_advancing() {
        let wm = SeqWatermark::new();
        wm.publish_error(5, JournalError::Closed);
        assert_eq!(wm.current(), 0, "watermark must not advance on error");
        assert!(matches!(wm.wait(3), Err(JournalError::Closed)));

        // A later successful fsync advances and resolves higher seqs.
        wm.publish(6);
        assert_eq!(wm.current(), 6);
        wm.wait(6).unwrap();
        // The already-resolved low seq is durable now too (6 >= 3).
        wm.wait(3).unwrap();
    }

    /// `publish` only ever advances the watermark (monotonic), even if called
    /// with a lower seq after a higher one.
    #[test]
    fn publish_is_monotonic() {
        let wm = SeqWatermark::new();
        wm.publish(10);
        wm.publish(4);
        assert_eq!(wm.current(), 10);
    }

    /// A blocked `wait` unblocks with `Err(Closed)` when the watermark closes.
    #[test]
    fn wait_unblocks_on_close() {
        let wm = SeqWatermark::new();
        let wm2 = Arc::clone(&wm);
        let waiter = thread::spawn(move || wm2.wait(999));
        thread::sleep(Duration::from_millis(50));
        wm.close();
        assert!(matches!(waiter.join().unwrap(), Err(JournalError::Closed)));
    }
}
