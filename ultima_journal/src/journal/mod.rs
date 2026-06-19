// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

pub(crate) mod segment;
mod segment_pipeline;
mod writer;

use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};

use crate::{JournalError, Notifier};
use writer::{AppendRequest, SeqWatermark, Writer, WriterState};

#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub dir: std::path::PathBuf,
    pub segment_size_bytes: u64,
    pub durability: crate::Durability,
    /// Opt-in (default false): preallocate each segment to `segment_size_bytes`
    /// up front so the per-commit `fdatasync` skips the ext4 metadata commit a
    /// size-extending append otherwise forces. See task on segment preallocation.
    pub preallocate_segments: bool,
}

impl JournalConfig {
    pub fn new(dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            segment_size_bytes: 64 * 1024 * 1024,
            durability: crate::Durability::Consistent,
            preallocate_segments: false,
        }
    }
}

pub struct Journal {
    pub(crate) state: Arc<Mutex<WriterState>>,
    writer: Mutex<Option<Writer>>,
    /// Fsync-durable sequence watermark (task28).
    durability: Arc<SeqWatermark>,
}

impl Journal {
    pub fn open(config: JournalConfig) -> Result<Self, JournalError> {
        std::fs::create_dir_all(&config.dir)?;
        // Remove orphan preallocation temps (a crash between temp-create and
        // activation rename). They hold zero committed records — never
        // referenced until their atomic rename to seg-{seq}.log.
        if config.preallocate_segments
            && let Ok(rd) = std::fs::read_dir(&config.dir)
        {
            for ent in rd.filter_map(|e| e.ok()) {
                let n = ent.file_name();
                let s = n.to_string_lossy();
                if s.starts_with("seg-prealloc.") && s.ends_with(".tmp") {
                    let _ = std::fs::remove_file(ent.path());
                }
            }
        }
        let mut entries: Vec<_> = std::fs::read_dir(&config.dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                let n = e.file_name();
                let s = n.to_string_lossy();
                s.starts_with("seg-") && s.ends_with(".log")
            })
            .collect();
        entries.sort_by_key(|e| e.file_name());

        // Phase 1: open each segment, fix any torn tail, collect (SegmentFile, ScanResult).
        let mut segments_with_scan: Vec<(segment::SegmentFile, segment::ScanResult)> = Vec::new();
        for ent in entries {
            let mut seg = segment::SegmentFile::open_for_read(&ent.path())?;
            let scan = seg.scan()?;
            let scan = if scan.had_torn_tail {
                if config.preallocate_segments {
                    // Preserve the physical zero tail; only rewind the logical
                    // cursor to the last durable record. The writer resumes
                    // appending into preallocated space (no truncate-then-refill
                    // churn). The first scan's records/index already exclude the
                    // torn tail, so reuse it directly.
                    seg.reset_cursor(scan.last_durable_offset);
                    scan
                } else {
                    seg.truncate(scan.last_durable_offset)?;
                    seg.scan()?
                }
            } else {
                scan
            };
            segments_with_scan.push((seg, scan));
        }

        // Phase 2: sentinel completion — search backwards for a sentinel as the
        // last record of any segment.  A sentinel means a `truncate_after` started
        // (wrote the sentinel + fsync'd) but crashed before removing the sentinel
        // and/or unlinking later segments.  Complete the truncate now.
        let mut sentinel_found_at: Option<(usize, u64)> = None; // (seg_idx, sentinel_byte_offset)
        for (i, (_, scan)) in segments_with_scan.iter().enumerate().rev() {
            if let Some(last_rec) = scan.records.last()
                && segment::is_sentinel(last_rec)
            {
                // Compute the byte offset at which the sentinel record starts.
                // That is: header + sum of all record sizes before the sentinel.
                let sentinel_offset = segment::SEGMENT_HEADER_SIZE as u64
                    + scan.records[..scan.records.len() - 1]
                        .iter()
                        .map(|r| (4 + 16 + r.payload.len() + 4) as u64)
                        .sum::<u64>();
                sentinel_found_at = Some((i, sentinel_offset));
                break;
            }
        }
        if let Some((idx, sentinel_offset)) = sentinel_found_at {
            // Truncate the segment to just before the sentinel, removing it.
            segments_with_scan[idx].0.truncate(sentinel_offset)?;
            // Drop all later segments from disk.
            let later = segments_with_scan.split_off(idx + 1);
            for (seg, _) in later {
                let _ = std::fs::remove_file(seg.path());
            }
            // Re-scan the modified segment so records/last_seq are accurate.
            let new_scan = segments_with_scan[idx].0.scan()?;
            segments_with_scan[idx].1 = new_scan;
        }

        // Phase 2b: complete an interrupted `truncate_after`. A valid intent
        // file means the intent was durable but the destructive phase may not
        // have finished — re-run it idempotently. A torn/corrupt intent means
        // the crash happened while writing the intent, before anything
        // destructive — ignore it. Either way the file is consumed.
        if let Some(keep_seq) = read_truncate_intent(&config.dir) {
            let mut segs: Vec<segment::SegmentFile> =
                segments_with_scan.into_iter().map(|(s, _)| s).collect();
            apply_truncate_to_segments(&config.dir, &mut segs, keep_seq)?;
            segments_with_scan = segs
                .into_iter()
                .map(|s| {
                    let scan = s.scan()?;
                    Ok((s, scan))
                })
                .collect::<Result<Vec<_>, JournalError>>()?;
        }
        remove_truncate_intent(&config.dir)?;

        // Phase 3: compute first_seq / last_seq from final state.
        let mut first_seq: Option<u64> = None;
        let mut last_seq: Option<u64> = None;
        for (_, scan) in &segments_with_scan {
            if let Some(first) = scan.records.first()
                && first_seq.is_none()
            {
                first_seq = Some(first.seq);
            }
            if let Some(last) = scan.records.last() {
                last_seq = Some(last.seq);
            }
        }

        // Install each segment's sparse index from the scan we already performed
        // above, so point reads can use the windowed `read_record` path.
        let mut segments: Vec<segment::SegmentFile> = segments_with_scan
            .into_iter()
            .map(|(mut seg, scan)| {
                seg.set_index(scan.index);
                seg
            })
            .collect();

        // Re-preallocate the active segment so steady-state preallocation is in
        // effect immediately after restart — covers a segment written before
        // the flag was enabled, or one a prior recovery physically truncated.
        if config.preallocate_segments
            && let Some(active) = segments.last_mut()
            && active.physical_len()? < config.segment_size_bytes
        {
            active.preallocate_to(config.segment_size_bytes)?;
        }

        let pipeline = if config.preallocate_segments {
            Some(crate::journal::segment_pipeline::SegmentPipeline::spawn(
                config.dir.clone(),
                config.segment_size_bytes,
            )?)
        } else {
            None
        };

        let state = Arc::new(Mutex::new(WriterState {
            dir: config.dir.clone(),
            pipeline,
            segment_size: config.segment_size_bytes,
            durability: config.durability,
            segments,
            last_seq,
            first_seq,
            // Empty on open: recovery rebuilds visibility from the scanned
            // segments; `append()` repopulates this going forward.
            pending: std::collections::BTreeMap::new(),
            truncate_gen: 0,
            persisted_hwm: 0,
            poisoned: false,
            #[cfg(test)]
            fail_next_fsync: false,
        }));
        let durability = SeqWatermark::new();
        let writer = Writer::spawn(Arc::clone(&state), Arc::clone(&durability));
        Ok(Self {
            state,
            writer: Mutex::new(Some(writer)),
            durability,
        })
    }

    pub fn first_seq(&self) -> Option<u64> {
        self.state.lock().unwrap().first_seq
    }

    pub fn last_seq(&self) -> Option<u64> {
        self.state.lock().unwrap().last_seq
    }

    /// Highest seq known to be fsync-durable (task28).
    ///
    /// In `Durability::Eventual` this trails [`Journal::last_seq`] by up to the
    /// idle-fsync interval; in `Consistent` it trails by ~one in-flight batch.
    /// Returns 0 before anything is durable.
    pub fn durable_seq(&self) -> u64 {
        self.durability.current()
    }

    /// Block until `seq` is fsync-durable.
    ///
    /// Returns immediately for an already-durable seq. Returns `Err` if a
    /// covering fsync failed or the journal closed before reaching `seq`.
    /// Holds no journal lock while blocking.
    pub fn wait_durable(&self, seq: u64) -> Result<(), JournalError> {
        self.durability.wait(seq)
    }

    /// Register `cb` to fire once `seq` is fsync-durable.
    ///
    /// Fires inline on the calling thread if `seq` is already durable (or
    /// already failed / journal closed). Otherwise fires later on the writer
    /// thread. This is the durability hook for `Durability::Eventual`, where
    /// `append`'s own [`Notifier`] resolves at the buffered write, not fsync.
    pub fn on_durable<F>(&self, seq: u64, cb: F)
    where
        F: FnOnce(Result<(), JournalError>) + Send + 'static,
    {
        self.durability.on_complete(seq, Box::new(cb));
    }

    /// Append a record.  Monotonicity is validated here under the state lock so
    /// that concurrent callers claim seqs in a total order that matches the
    /// channel submission order.  The bg writer fsyncs and signals the Notifier.
    pub fn append(&self, seq: u64, meta: u64, payload: &[u8]) -> Result<Notifier, JournalError> {
        // Size guard (cheap, no I/O needed).
        let body_len = 16 + payload.len();
        let total = (4 + body_len + 4) as u64;

        let (signal, notifier) = Notifier::pending();

        {
            let mut st = self.state.lock().unwrap();
            if st.poisoned {
                // Fail-stop: a WAL fsync failed; the writer has halted and
                // nothing further may be acknowledged.
                return Err(JournalError::Closed);
            }
            if total > st.segment_size {
                return Err(JournalError::PayloadTooLargeForSegment {
                    segment_size: st.segment_size,
                    record_size: total,
                });
            }
            if let Some(last) = st.last_seq
                && seq <= last
            {
                return Err(JournalError::NonMonotonicSeq {
                    expected_gt: last,
                    got: seq,
                });
            }
            // Claim the seq under the lock: send to channel while still holding it,
            // so the channel ordering matches the monotonic seq order.
            let payload_vec = payload.to_vec();
            let req = AppendRequest {
                seq,
                meta,
                payload: payload_vec.clone(),
                signal,
                generation: st.truncate_gen,
            };
            let writer_guard = self.writer.lock().unwrap();
            let w = writer_guard.as_ref().ok_or(JournalError::Closed)?;
            w.tx.send(req).map_err(|_| JournalError::Closed)?;
            drop(writer_guard);

            // Publish the record for IMMEDIATE read visibility, still holding the
            // state lock, so a reader sees it the instant `append()` returns —
            // before the bg writer persists it and before the flush callback
            // (openraft's `RaftLogStorage::append()` readability contract). The
            // writer evicts this seq from `pending` once it lands in a segment;
            // both happen under this lock, so there is no window where the record
            // is invisible and none where it is double-counted. `append()` now
            // owns `last_seq`/`first_seq` advancement (the writer no longer sets
            // them — it would regress them behind these synchronous updates).
            if st.first_seq.is_none() {
                st.first_seq = Some(seq);
            }
            st.last_seq = Some(seq);
            let generation = st.truncate_gen;
            st.pending.insert(seq, (generation, meta, payload_vec));
        }

        Ok(notifier)
    }

    // NOTE: `read`, `read_range`, and `iter_range` all acquire `WriterState`'s mutex,
    // which means reads are serialized with appends (and with each other).  This is
    // correct and free of deadlocks — no caller holds the lock when calling these
    // methods — but it does mean a slow read can block a concurrent append.
    //
    // A profile-driven optimization (lock-free reads via `Arc<RwLock<Vec<Arc<SegmentRef>>>>`)
    // is deferred per the plan's open questions §12.  The refactor was scoped out of Task 15
    // ("take option (a) — add the concurrency test, keep `Mutex<WriterState>`").

    /// Read a single record by `seq`. Uses the segment's sparse index to read
    /// only a bounded window (~64 KiB) rather than scanning the whole segment;
    /// consequently a point read CRC-verifies only the records in that window,
    /// not the entire segment (recovery's `scan` does full validation).
    pub fn read(&self, seq: u64) -> Result<Option<(u64, Vec<u8>)>, JournalError> {
        let st = self.state.lock().unwrap();
        // Overlay the not-yet-persisted tail: an appended-but-unwritten record is
        // served from memory (it is not yet in any segment). Disjoint from the
        // segment path — the writer evicts a seq from `pending` exactly when it
        // becomes segment-readable, under this same lock.
        if let Some((_, meta, payload)) = st.pending.get(&seq) {
            return Ok(Some((*meta, payload.clone())));
        }
        let Some(seg) = st.segments.iter().rev().find(|s| s.base_seq() <= seq) else {
            return Ok(None);
        };
        seg.read_record(seq)
    }

    /// Read all records with `seq` in `range`, in order. Uses each segment's
    /// sparse index to read only the byte span covering the range, and prunes
    /// segments whose seq range does not overlap it. A partial range therefore
    /// CRC-verifies only the records in the spans it reads, not whole segments;
    /// a full/unbounded range reads and verifies everything, as before.
    pub fn read_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, JournalError> {
        // Bounds + reads under one lock so first/last_seq and the segment state
        // are a consistent snapshot.
        let st = self.state.lock().unwrap();
        let (lo, hi) = bounds_to_inclusive(range, st.first_seq, st.last_seq);
        let mut out = Vec::new();
        for (i, seg) in st.segments.iter().enumerate() {
            // Prune: segments are seq-ordered; once one starts above hi, so do
            // all later ones.
            if seg.base_seq() > hi {
                break;
            }
            // Prune: skip a segment whose records all fall below lo — true when
            // the next segment's base_seq <= lo (this segment's max seq < it).
            if let Some(next) = st.segments.get(i + 1)
                && next.base_seq() <= lo
            {
                continue;
            }
            out.extend(seg.read_window(lo, hi)?);
        }
        // Overlay the not-yet-persisted tail. These seqs are not in any segment
        // (disjoint — the writer evicts on persist under this lock), so append
        // then sort by seq to restore ascending order across the merge. Guard the
        // `lo > hi` case (e.g. an empty `n..n` range) — `BTreeMap::range` panics
        // on an inverted range, unlike the segment path.
        if lo <= hi {
            for (&seq, (_, meta, payload)) in st.pending.range(lo..=hi) {
                out.push((seq, *meta, payload.clone()));
            }
            out.sort_by_key(|(seq, _, _)| *seq);
        }
        Ok(out)
    }

    /// Range read returning an iterator. Note: the v1 implementation is
    /// **eager** — internally calls `read_range()` to collect into a `Vec`,
    /// then returns its iterator. The signature is iterator-shaped to leave
    /// room for a future streaming implementation; today's memory profile is
    /// the same as `read_range()`. Callers that need streaming behavior over
    /// large ranges should not assume laziness yet.
    #[allow(clippy::type_complexity)]
    pub fn iter_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<impl Iterator<Item = Result<(u64, u64, Vec<u8>), JournalError>> + '_, JournalError>
    {
        let v = self.read_range(range)?;
        Ok(v.into_iter().map(Ok))
    }

    /// Truncate the journal so that only records with `seq <= keep_seq` remain.
    ///
    /// Crash-safe via a durable intent file:
    /// 1. Write `truncate.intent` (keep_seq + CRC), fsync it and the dir —
    ///    the intent is durable *before* anything destructive happens.
    /// 2. Truncate the boundary segment past the last kept record and unlink
    ///    all later segments, then fsync the dir.
    /// 3. Remove the intent file, fsync the dir.
    ///
    /// A crash before 1 completes leaves the journal untouched (a torn
    /// intent is ignored on open); a crash any time after leaves the intent,
    /// and `Journal::open` re-runs step 2 idempotently before consuming it.
    ///
    /// Queued-but-unwritten appends past `keep_seq` are fenced: the writer
    /// drops them via the truncation generation (see `AppendRequest::gen`),
    /// and the durable-seq watermark is pulled back to `keep_seq` so a
    /// subsequent re-append at a truncated seq is only reported durable by
    /// its own fsync.
    ///
    /// Returns a `Notifier::done()` — the operation is fully synchronous.
    pub fn truncate_after(&self, keep_seq: u64) -> Result<Notifier, JournalError> {
        let mut st = self.state.lock().unwrap();
        if st.poisoned {
            return Err(JournalError::Closed);
        }

        // Drop any not-yet-persisted records past the truncation point so the
        // in-memory overlay matches the truncated log (openraft truncates a
        // conflicting suffix that may still be sitting in `pending`), and
        // bump the truncation generation so the writer skips queued requests
        // for the seqs we just dropped.
        st.pending.retain(|&s, _| s <= keep_seq);
        st.truncate_gen += 1;

        // Pull the durability watermark back: durable seqs past keep_seq no
        // longer exist, and an in-flight publish for pre-truncate bytes must
        // not re-advance it (stale generation).
        st.persisted_hwm = st.persisted_hwm.min(keep_seq);
        self.durability.reset_to(keep_seq, st.truncate_gen);

        // Intent → destructive phase → consume intent.
        let dir = st.dir.clone();
        write_truncate_intent(&dir, keep_seq)?;
        apply_truncate_to_segments(&dir, &mut st.segments, keep_seq)?;
        remove_truncate_intent(&dir)?;

        // Update in-memory bounds. Anything <= keep_seq survives (on disk or
        // in `pending`), so the journal is non-empty iff some record at or
        // below keep_seq ever existed — i.e. first_seq <= keep_seq.
        st.last_seq = if st.first_seq.is_some_and(|first| keep_seq >= first) {
            Some(keep_seq)
        } else {
            None
        };
        if st.last_seq.is_none() {
            st.first_seq = None;
        }

        Ok(Notifier::done())
    }

    /// Drop full segments whose final record's seq <= `seq`.
    /// Never drops the active (last) segment even if eligible, to ensure
    /// future appends still have a place to go.
    /// Updates `first_seq` after purging.
    pub fn purge_before(&self, seq: u64) -> Result<(), JournalError> {
        let mut st = self.state.lock().unwrap();

        // Drop segments whose final record's seq <= seq.
        let mut keep_idx = 0;
        for (i, seg) in st.segments.iter().enumerate() {
            let scan = seg.scan()?;
            let last = scan.records.last().map(|r| r.seq);
            if let Some(last) = last
                && last <= seq
            {
                keep_idx = i + 1;
                continue;
            }
            break;
        }

        // Never drop the active (last) segment even if its records all <= seq —
        // we still need a place to append.
        if keep_idx == st.segments.len() && !st.segments.is_empty() {
            keep_idx -= 1;
        }

        let removed: Vec<_> = st.segments.drain(..keep_idx).collect();
        for seg in removed {
            let _ = std::fs::remove_file(seg.path());
        }
        if let Ok(d) = std::fs::File::open(&st.dir) {
            let _ = d.sync_all();
        }

        // Recompute first_seq.
        st.first_seq = st
            .segments
            .first()
            .and_then(|s| s.scan().ok())
            .and_then(|scan| scan.records.first().map(|r| r.seq));

        Ok(())
    }

    pub fn close(self) -> Result<(), JournalError> {
        let mut g = self.writer.lock().unwrap();
        if let Some(w) = g.take() {
            drop(w.tx);
            if let Some(h) = w.handle {
                let _ = h.join();
            }
        }
        drop(g);
        // Stop the preallocator thread and remove any unconsumed temp. Done
        // after the writer is joined so no activation can race the shutdown.
        if let Some(pipe) = self.state.lock().unwrap().pipeline.take() {
            pipe.shutdown();
        }
        Ok(())
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        let mut g = self.writer.lock().unwrap();
        if let Some(w) = g.take() {
            drop(w.tx);
            if let Some(h) = w.handle {
                let _ = h.join();
            }
        }
        drop(g);
        // Stop the preallocator thread and remove any unconsumed temp. Done
        // after the writer is joined so no activation can race the shutdown.
        // Option::take() leaves None, so close() then Drop is a no-op.
        if let Some(pipe) = self.state.lock().unwrap().pipeline.take() {
            pipe.shutdown();
        }
        // The writer thread is gone and every queued batch has been published.
        // Release any waiter parked on a seq that was never written so it cannot
        // block forever (task28). Idempotent if close() already ran.
        self.durability.close();
    }
}

// ---------------------------------------------------------------------------
// Truncate intent file — crash-safe two-phase truncate_after
// ---------------------------------------------------------------------------

/// Name of the durable truncate-intent file. Written and fsynced *before*
/// any destructive step of `truncate_after`, removed only after the last
/// one; `Journal::open` completes the truncate if it finds one.
const TRUNCATE_INTENT_FILENAME: &str = "truncate.intent";
const TRUNCATE_INTENT_MAGIC: &[u8; 8] = b"ULTJINT1";

fn sync_dir(dir: &std::path::Path) -> Result<(), JournalError> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Write + fsync the intent file (magic, keep_seq, CRC), then fsync the dir.
fn write_truncate_intent(dir: &std::path::Path, keep_seq: u64) -> Result<(), JournalError> {
    use std::io::Write;
    let mut buf = Vec::with_capacity(20);
    buf.extend_from_slice(TRUNCATE_INTENT_MAGIC);
    buf.extend_from_slice(&keep_seq.to_le_bytes());
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    let mut f = std::fs::File::create(dir.join(TRUNCATE_INTENT_FILENAME))?;
    f.write_all(&buf)?;
    f.sync_all()?;
    sync_dir(dir)
}

/// Read a valid intent's keep_seq. A missing, short, or CRC-failing file
/// returns `None` — a torn intent means the crash happened while writing it,
/// before any destructive step, so ignoring it is safe.
fn read_truncate_intent(dir: &std::path::Path) -> Option<u64> {
    let buf = std::fs::read(dir.join(TRUNCATE_INTENT_FILENAME)).ok()?;
    if buf.len() != 20 || &buf[0..8] != TRUNCATE_INTENT_MAGIC {
        return None;
    }
    let stored = u32::from_le_bytes(buf[16..20].try_into().ok()?);
    if crc32fast::hash(&buf[0..16]) != stored {
        return None;
    }
    Some(u64::from_le_bytes(buf[8..16].try_into().ok()?))
}

/// Remove the intent file (idempotent) and fsync the dir.
fn remove_truncate_intent(dir: &std::path::Path) -> Result<(), JournalError> {
    match std::fs::remove_file(dir.join(TRUNCATE_INTENT_FILENAME)) {
        Ok(()) => sync_dir(dir),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Destructive phase of a truncate: drop every record with seq > `keep_seq`
/// from the segment files (truncating the boundary segment, unlinking later
/// ones), then fsync the dir. Idempotent — always runs under a durable
/// intent file, so a crash at any point is completed on the next open.
///
/// Returns the highest surviving on-disk seq, if any.
fn apply_truncate_to_segments(
    dir: &std::path::Path,
    segments: &mut Vec<segment::SegmentFile>,
    keep_seq: u64,
) -> Result<Option<u64>, JournalError> {
    match segments.iter().rposition(|s| s.base_seq() <= keep_seq) {
        None => {
            // keep_seq is below every segment — drop everything.
            let removed = std::mem::take(segments);
            for seg in removed {
                let _ = std::fs::remove_file(seg.path());
            }
            sync_dir(dir)?;
            Ok(None)
        }
        Some(seg_idx) => {
            // Byte offset just past the last record with seq <= keep_seq.
            let scan = segments[seg_idx].scan()?;
            let cut = scan.records.iter().position(|r| r.seq > keep_seq);
            let new_end = cut
                .map(|pos| {
                    segment::SEGMENT_HEADER_SIZE as u64
                        + scan.records[..pos]
                            .iter()
                            .map(|r| (4 + 16 + r.payload.len() + 4) as u64)
                            .sum::<u64>()
                })
                .unwrap_or(scan.last_durable_offset);
            let survivor = match cut {
                Some(0) => None,
                Some(pos) => Some(scan.records[pos - 1].seq),
                None => scan.records.last().map(|r| r.seq),
            };
            segments[seg_idx].truncate(new_end)?;
            let to_remove: Vec<_> = segments.drain(seg_idx + 1..).collect();
            for seg in to_remove {
                let _ = std::fs::remove_file(seg.path());
            }
            sync_dir(dir)?;
            Ok(survivor)
        }
    }
}

fn bounds_to_inclusive(
    range: impl RangeBounds<u64>,
    first: Option<u64>,
    last: Option<u64>,
) -> (u64, u64) {
    let lo = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n.saturating_add(1),
        Bound::Unbounded => first.unwrap_or(0),
    };
    let hi = match range.end_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n.saturating_sub(1),
        Bound::Unbounded => last.unwrap_or(u64::MAX),
    };
    (lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_preallocate_defaults_off() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = JournalConfig::new(dir.path());
        assert!(!cfg.preallocate_segments, "preallocation is opt-in (default off)");
    }

    #[test]
    fn open_empty_dir_returns_empty_journal() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        assert_eq!(j.first_seq(), None);
        assert_eq!(j.last_seq(), None);
    }

    #[test]
    fn open_creates_dir_if_missing() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("nested/deep");
        let j = Journal::open(JournalConfig::new(&sub)).unwrap();
        assert!(sub.exists());
        assert_eq!(j.last_seq(), None);
    }

    #[test]
    fn append_one_record_then_read_state() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        let n = j.append(1, 0, b"first").unwrap();
        n.wait().unwrap();
        assert_eq!(j.first_seq(), Some(1));
        assert_eq!(j.last_seq(), Some(1));
    }

    #[test]
    fn append_rejects_non_monotonic() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(5, 0, b"x").unwrap().wait().unwrap();
        let err = j.append(3, 0, b"y").unwrap_err();
        assert!(matches!(
            err,
            JournalError::NonMonotonicSeq {
                expected_gt: 5,
                got: 3
            }
        ));
    }

    #[test]
    fn append_rejects_record_larger_than_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        let j = Journal::open(cfg).unwrap();
        let big = vec![0u8; 1024];
        let err = j.append(1, 0, &big).unwrap_err();
        assert!(matches!(
            err,
            JournalError::PayloadTooLargeForSegment { .. }
        ));
    }

    #[test]
    fn reopen_sees_appended_records() {
        let dir = tempfile::tempdir().unwrap();
        {
            let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
            j.append(1, 11, b"a").unwrap().wait().unwrap();
            j.append(2, 22, b"bb").unwrap().wait().unwrap();
        }
        let j2 = Journal::open(JournalConfig::new(dir.path())).unwrap();
        assert_eq!(j2.first_seq(), Some(1));
        assert_eq!(j2.last_seq(), Some(2));
    }

    #[test]
    fn consistent_durability_survives_reopen() {
        // Under Consistent durability the group-commit fdatasync (sync_data)
        // must make every acked record durable, so a reopen recovers the full
        // [1, N] range. This guards that fdatasync preserves the same durable
        // prefix as a full fsync would.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Consistent;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=64u64 {
                // append(seq, term, payload) — same call shape as
                // reopen_sees_appended_records.
                j.append(i, i, format!("rec-{i}").as_bytes()).unwrap();
            }
            // Wait the highest seq: the group-commit fsync barrier must make
            // every record at or below seq 64 durable before we drop.
            j.wait_durable(64).unwrap();
            assert_eq!(
                j.durable_seq(),
                64,
                "all 64 records durable after the commit-path fsync"
            );
        } // drop the Journal — clean process exit, no extra fsync on drop
        // Reopen: recovery must locate every fsync'd record.
        let j2 = Journal::open(cfg).unwrap();
        assert_eq!(j2.first_seq(), Some(1));
        assert_eq!(j2.last_seq(), Some(64));
    }

    #[test]
    fn read_returns_appended_record() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 100, b"alpha").unwrap().wait().unwrap();
        j.append(2, 200, b"beta").unwrap().wait().unwrap();
        j.append(3, 300, b"gamma").unwrap().wait().unwrap();

        let (m, p) = j.read(2).unwrap().unwrap();
        assert_eq!(m, 200);
        assert_eq!(p, b"beta");
    }

    #[test]
    fn read_missing_seq_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        assert!(j.read(99).unwrap().is_none());
    }

    #[test]
    fn segment_rotates_when_size_exceeded() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 512;
        let j = Journal::open(cfg).unwrap();
        let payload = vec![0xAB; 200];
        for i in 1..=4u64 {
            j.append(i, 0, &payload).unwrap().wait().unwrap();
        }
        let n = j.state.lock().unwrap().segments.len();
        assert!(n >= 2, "expected segment rotation, got {n} segments");
    }

    #[test]
    fn read_range_returns_inclusive_records() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=5u64 {
            j.append(i, i * 10, format!("p{i}").as_bytes())
                .unwrap()
                .wait()
                .unwrap();
        }
        let v = j.read_range(2..=4).unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0], (2, 20, b"p2".to_vec()));
        assert_eq!(v[2], (4, 40, b"p4".to_vec()));
    }

    #[test]
    fn iter_range_streams_records() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=5u64 {
            j.append(i, 0, format!("p{i}").as_bytes())
                .unwrap()
                .wait()
                .unwrap();
        }
        let it = j.iter_range(..).unwrap();
        let collected: Vec<_> = it.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(collected.len(), 5);
        assert_eq!(collected[4].0, 5);
    }

    #[test]
    fn read_range_spans_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        let j = Journal::open(cfg).unwrap();
        for i in 1..=10u64 {
            j.append(i, 0, &[0u8; 100]).unwrap().wait().unwrap();
        }
        let v = j.read_range(3..=8).unwrap();
        assert_eq!(v.len(), 6);
        assert_eq!(v.first().unwrap().0, 3);
        assert_eq!(v.last().unwrap().0, 8);
    }

    #[test]
    fn many_appenders_complete() {
        use std::sync::Arc;
        // Concurrent appenders each claim a monotonic seq and append.
        // The shared `seq_lock` serializes both seq assignment and channel
        // submission so the channel always receives seqs in strictly increasing
        // order, which is the contract the bg writer requires.
        let dir = tempfile::tempdir().unwrap();
        let j = Arc::new(Journal::open(JournalConfig::new(dir.path())).unwrap());
        let seq_lock = Arc::new(Mutex::new(0u64));
        let mut handles = Vec::new();
        for _ in 0..200 {
            let j2 = Arc::clone(&j);
            let sl = Arc::clone(&seq_lock);
            handles.push(std::thread::spawn(move || {
                // Claim seq + submit to channel atomically under the external lock.
                let notifier = {
                    let mut g = sl.lock().unwrap();
                    *g += 1;
                    let seq = *g;
                    j2.append(seq, 0, b"x").unwrap()
                };
                // Wait for durability outside the lock.
                notifier.wait().unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(j.last_seq(), Some(200));
    }

    #[test]
    fn callback_fires_after_durable() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        let counter = Arc::new(AtomicU64::new(0));
        for i in 1..=10u64 {
            let n = j.append(i, 0, b"x").unwrap();
            let c = Arc::clone(&counter);
            n.on_complete(move |r| {
                r.unwrap();
                c.fetch_add(1, Ordering::SeqCst);
            });
        }
        // Spin briefly until all callbacks fire.
        for _ in 0..1000 {
            if counter.load(Ordering::SeqCst) == 10 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn truncate_after_drops_higher_seqs() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=10u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        j.truncate_after(5).unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), Some(5));
        assert!(j.read(7).unwrap().is_none());
        assert_eq!(j.read(5).unwrap().unwrap().1, b"x".to_vec());
        // Re-append from new tail.
        j.append(6, 0, b"new").unwrap().wait().unwrap();
        assert_eq!(j.read(6).unwrap().unwrap().1, b"new".to_vec());
    }

    #[test]
    fn truncate_after_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        let j = Journal::open(cfg).unwrap();
        for i in 1..=12u64 {
            j.append(i, 0, &[0u8; 100]).unwrap().wait().unwrap();
        }
        j.truncate_after(4).unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), Some(4));
        // Verify later segments unlinked.
        let segs = std::fs::read_dir(j.state.lock().unwrap().dir.clone())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("seg-")
            })
            .count();
        assert!(segs <= 2);
    }

    #[test]
    fn eventual_mode_returns_already_done_notifier() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Eventual;
        let j = Journal::open(cfg).unwrap();
        let n = j.append(1, 0, b"x").unwrap();
        // In Eventual mode, Notifier should resolve quickly without blocking
        // on fsync; for the public contract we just check wait() succeeds.
        n.wait().unwrap();
    }

    #[test]
    fn purge_before_drops_full_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        let j = Journal::open(cfg).unwrap();
        for i in 1..=12u64 {
            j.append(i, 0, &[0u8; 100]).unwrap().wait().unwrap();
        }
        let n_before = j.state.lock().unwrap().segments.len();
        j.purge_before(6).unwrap();
        let n_after = j.state.lock().unwrap().segments.len();
        assert!(n_after < n_before);
        // first_seq is now >= first kept segment's base_seq.
        assert!(j.first_seq().unwrap() <= 7);
    }

    #[test]
    fn open_recovers_from_unfinished_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        {
            let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
            for i in 1..=5u64 {
                j.append(i, 0, b"x").unwrap().wait().unwrap();
            }
            // Manually inject a sentinel as if a truncate started but didn't
            // complete the unlinks of later segments. Easiest: just simulate by
            // appending a sentinel record by writing to the file directly.
            let mut st = j.state.lock().unwrap();
            let seg = st.segments.last_mut().unwrap();
            seg.append_record(99, segment::SENTINEL_META, segment::SENTINEL_PAYLOAD)
                .unwrap();
            seg.fsync().unwrap();
        }
        // Reopen — recovery should treat sentinel as truncate marker, drop records >= sentinel seq.
        let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        // Sentinel at seq=99, but legit records were 1..=5. Sentinel marks "drop > 5".
        assert_eq!(j2.last_seq(), Some(5));
    }

    #[test]
    fn open_truncates_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        {
            let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
            for i in 1..=3u64 {
                j.append(i, 0, b"x").unwrap().wait().unwrap();
            }
        }
        // Append 5 garbage bytes to the only segment file.
        let entries: Vec<_> = std::fs::read_dir(&dir_path).unwrap().collect();
        let seg_path = entries[0].as_ref().unwrap().path();
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&seg_path)
            .unwrap()
            .write_all(&[0xAB; 5])
            .unwrap();
        // Reopen — torn tail should be truncated.
        let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        assert_eq!(j2.last_seq(), Some(3));
        // The torn bytes should have been truncated; subsequent append must succeed.
        j2.append(4, 0, b"new").unwrap().wait().unwrap();
    }

    /// Validates that concurrent readers and a writer thread do not deadlock or
    /// starve each other.  Reads and appends both acquire `WriterState`'s mutex,
    /// so they serialise, but neither side holds the lock across I/O for long
    /// enough to cause starvation in practice.
    #[test]
    fn eventual_periodic_fsync_runs_on_idle() {
        // Append once, then sit idle for > the bg writer's 50 ms periodic
        // fsync interval. The timer path (RecvTimeoutError::Timeout +
        // fsync_active_segment) must execute. We don't observe its effect
        // directly — the test passes if the journal still works after.
        use crate::Durability;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = Durability::Eventual;
        let j = Journal::open(cfg).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(150));
        // Append again to confirm the writer thread is still healthy.
        j.append(2, 0, b"y").unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), Some(2));
    }

    #[test]
    fn close_releases_writer_thread() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        j.close().unwrap();
    }

    #[test]
    fn truncate_after_below_first_seq_drops_everything() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 5..=8u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        // keep_seq is below all base_seqs — drops every segment.
        j.truncate_after(2).unwrap().wait().unwrap();
        assert_eq!(j.first_seq(), None);
        assert_eq!(j.last_seq(), None);
        // Re-append from scratch must work.
        j.append(10, 0, b"new").unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), Some(10));
    }

    #[test]
    fn purge_below_threshold_protects_active_segment() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=3u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        // Threshold above all records — would drop everything; the active segment
        // must be retained so future appends still work.
        j.purge_before(100).unwrap();
        j.append(4, 0, b"y").unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), Some(4));
    }

    #[test]
    fn bounds_to_inclusive_handles_excluded_variants() {
        // Excluded lo: 2..hi → starts at 3.
        let v: (u64, u64) = bounds_to_inclusive(
            (Bound::Excluded(2u64), Bound::Excluded(8u64)),
            Some(0),
            Some(20),
        );
        assert_eq!(v, (3, 7));
    }

    #[test]
    fn read_range_with_excluded_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=10u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        let v = j
            .read_range((Bound::Excluded(2u64), Bound::Excluded(6u64)))
            .unwrap();
        let seqs: Vec<u64> = v.into_iter().map(|(s, _, _)| s).collect();
        assert_eq!(seqs, vec![3, 4, 5]);
    }

    #[test]
    fn read_below_first_seq_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(10, 0, b"x").unwrap().wait().unwrap();
        // No segment has base_seq <= 5 — short-circuits.
        assert!(j.read(5).unwrap().is_none());
    }

    #[test]
    fn read_seq_in_gap_returns_none() {
        // Strictly-monotonic seqs allow gaps. Reading a seq between two records
        // hits the early-termination branch in `read` (`r.seq > seq`).
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"a").unwrap().wait().unwrap();
        j.append(5, 0, b"b").unwrap().wait().unwrap();
        j.append(10, 0, b"c").unwrap().wait().unwrap();
        assert!(j.read(3).unwrap().is_none()); // between 1 and 5
        assert!(j.read(7).unwrap().is_none()); // between 5 and 10
    }

    #[test]
    fn read_above_last_seq_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=3u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        // Linear scan walks past all records; r.seq > seq never triggers because
        // the target is beyond the tail. Falls through to Ok(None).
        assert!(j.read(99).unwrap().is_none());
    }

    /// openraft's `RaftLogStorage::append()` requires an appended entry to be
    /// readable the instant `append()` returns — before the flush callback and
    /// before durability. Verify every read path sees the record WITHOUT waiting
    /// on the Notifier (i.e. while the bg writer may not have persisted it yet),
    /// in both durability modes. Regression for the read-after-append visibility
    /// gap that tripped openraft's apply-drain debug-assert (openraft#1780): the
    /// reader used to lag `append()` until the writer thread drained the channel,
    /// yielding a short read that omitted the just-appended tail entry.
    #[test]
    fn append_is_immediately_readable_before_flush() {
        for durability in [crate::Durability::Eventual, crate::Durability::Consistent] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = JournalConfig::new(dir.path());
            cfg.durability = durability;
            let j = Journal::open(cfg).unwrap();

            for i in 1..=200u64 {
                // Do NOT wait on the notifier — visibility must not depend on the
                // bg writer having processed/persisted the record.
                let _notifier = j.append(i, i * 3, b"payload").unwrap();

                // Point read sees it immediately.
                let (meta, payload) = j.read(i).unwrap().unwrap_or_else(|| {
                    panic!("seq {i} not readable immediately after append ({durability:?})")
                });
                assert_eq!(meta, i * 3);
                assert_eq!(&payload, b"payload");

                // last_seq reflects it immediately.
                assert_eq!(j.last_seq(), Some(i));

                // Range read returns the whole prefix with NO short read — the
                // exact invariant openraft's apply path relies on.
                let v = j.read_range(1..=i).unwrap();
                assert_eq!(
                    v.len() as u64,
                    i,
                    "short read at seq {i} ({durability:?}): got {} entries",
                    v.len()
                );
                assert_eq!(v.last().unwrap().0, i);
            }

            // Drain to durability and confirm the overlay was fully evicted (no
            // double-counting once everything is persisted).
            j.wait_durable(200).unwrap();
            assert!(j.state.lock().unwrap().pending.is_empty());
            assert_eq!(j.read_range(1..=200).unwrap().len(), 200);
        }
    }

    #[test]
    fn concurrent_reads_during_appends() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let j = Arc::new(Journal::open(JournalConfig::new(dir.path())).unwrap());
        for i in 1..=100u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let j2 = Arc::clone(&j);
        let reader = std::thread::spawn(move || {
            let mut count = 0u64;
            while !stop2.load(Ordering::SeqCst) {
                let v = j2.read_range(1..=50).unwrap();
                assert_eq!(v.len(), 50);
                count += 1;
            }
            count
        });
        for i in 101..=200u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        stop.store(true, Ordering::SeqCst);
        let read_count = reader.join().unwrap();
        assert!(read_count > 0);
        assert_eq!(j.last_seq(), Some(200));
    }

    // --- task28: durable_seq watermark ---------------------------------------

    /// In Consistent mode every append is fsynced, so the watermark reaches the
    /// last appended seq once its Notifier resolves.
    #[test]
    fn durable_seq_advances_in_consistent_mode() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=5u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        assert!(j.durable_seq() >= 5);
        j.wait_durable(5).unwrap();
    }

    /// In Eventual mode the append Notifier resolves before fsync, but the
    /// watermark still lets a caller observe durability — advanced by the
    /// idle-timer fsync. `wait_durable` blocks until that fsync lands.
    #[test]
    fn durable_seq_advances_in_eventual_mode() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Eventual;
        let j = Journal::open(cfg).unwrap();
        for i in 1..=3u64 {
            j.append(i, 0, b"x").unwrap();
        }
        // Deterministic: block until the idle-timer fsync publishes seq 3.
        j.wait_durable(3).unwrap();
        assert!(j.durable_seq() >= 3);
    }

    /// `wait_durable` on an already-durable seq returns immediately.
    #[test]
    fn wait_durable_already_durable_is_immediate() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        j.wait_durable(1).unwrap();
        j.wait_durable(1).unwrap(); // second call must not block
    }

    /// `on_durable` fires exactly once with `Ok` after the seq becomes durable.
    #[test]
    fn on_durable_fires_once_after_fsync() {
        use std::sync::mpsc;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Eventual;
        let j = Journal::open(cfg).unwrap();
        let (tx, rx) = mpsc::channel();
        j.on_durable(1, move |res| tx.send(res).unwrap());
        j.append(1, 0, b"x").unwrap();
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("callback did not fire");
        assert!(got.is_ok());
        assert!(rx.recv_timeout(std::time::Duration::from_millis(50)).is_err());
    }

    /// `on_durable` fires inline when the seq is already durable.
    #[test]
    fn on_durable_fires_inline_when_already_durable() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        j.wait_durable(1).unwrap();
        let fired = Arc::new(AtomicBool::new(false));
        let f2 = Arc::clone(&fired);
        j.on_durable(1, move |res| {
            assert!(res.is_ok());
            f2.store(true, Ordering::SeqCst);
        });
        assert!(fired.load(Ordering::SeqCst));
    }

    #[test]
    fn coalesced_batch_spans_segments() {
        // Submit a burst WITHOUT waiting each append — this lets the bg writer
        // drain multiple records into one batch, exercising the coalesced write
        // path including rotation across the 256-byte segment boundary. Whatever
        // the batch grouping, the result must be correct.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        let j = Journal::open(cfg).unwrap();
        let payload = [0xCDu8; 100];
        let mut last = None;
        for i in 1..=12u64 {
            last = Some(j.append(i, i * 7, &payload).unwrap());
        }
        last.unwrap().wait().unwrap();

        assert_eq!(j.last_seq(), Some(12));
        let v = j.read_range(1..=12).unwrap();
        assert_eq!(v.len(), 12);
        assert_eq!(v[0], (1, 7, payload.to_vec()));
        assert_eq!(v[11], (12, 84, payload.to_vec()));
        let segs = j.state.lock().unwrap().segments.len();
        assert!(segs >= 2, "expected rotation across segments, got {segs}");
    }

    /// A waiter parked on a seq that is never written is released with `Err`
    /// when the journal is dropped, rather than blocking forever.
    #[test]
    fn close_releases_parked_waiter_with_err() {
        use std::sync::mpsc;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Eventual;
        let j = Journal::open(cfg).unwrap();
        let (tx, rx) = mpsc::channel();
        j.on_durable(999, move |res| tx.send(res).unwrap());
        drop(j); // joins writer, then close() drains parked waiters.
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("callback did not fire on close");
        assert!(matches!(got, Err(JournalError::Closed)));
    }

    #[test]
    fn read_range_partial_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 2 * 1024; // small → many segments
        let j = Journal::open(cfg).unwrap();
        let payload = [0x5Au8; 200];
        for i in 1..=50u64 {
            j.append(i, i * 2, &payload).unwrap().wait().unwrap();
        }
        let nseg = j.state.lock().unwrap().segments.len();
        assert!(nseg >= 4, "want several segments to exercise pruning, got {nseg}");

        // Localized partial range in the middle: leading and trailing segments prune.
        let v = j.read_range(20..=29).unwrap();
        let seqs: Vec<u64> = v.iter().map(|(s, _, _)| *s).collect();
        assert_eq!(seqs, (20..=29).collect::<Vec<_>>());
        assert_eq!(v[0], (20, 40, payload.to_vec()));
        assert_eq!(v[9], (29, 58, payload.to_vec()));

        // Full unbounded range is unchanged (no regression).
        assert_eq!(j.read_range(..).unwrap().len(), 50);
    }

    #[test]
    fn read_range_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let payload = vec![0x6Bu8; 20 * 1024]; // multi-window single segment
        {
            let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
            for i in 1..=30u64 {
                j.append(i, i * 5, &payload).unwrap().wait().unwrap();
            }
        }
        // Reopen: index installed at open → windowed range path.
        let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        let v = j2.read_range(10..=15).unwrap();
        let seqs: Vec<u64> = v.iter().map(|(s, _, _)| *s).collect();
        assert_eq!(seqs, vec![10, 11, 12, 13, 14, 15]);
        assert_eq!(v[0].1, 50); // meta = 10 * 5
        assert_eq!(v[5].2, payload);
    }

    // --- truncate_after crash-safety (intent file), watermark, fencing ---

    /// Write a truncate-intent file the way `truncate_after` does, simulating
    /// a crash after the intent was made durable but before (or during) the
    /// destructive steps.
    fn write_intent_bytes(dir: &std::path::Path, keep_seq: u64) {
        let mut buf = Vec::with_capacity(20);
        buf.extend_from_slice(b"ULTJINT1");
        buf.extend_from_slice(&keep_seq.to_le_bytes());
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(dir.join("truncate.intent"), buf).unwrap();
    }

    /// A durable truncate intent left behind by a crash must be completed on
    /// open: records past keep_seq dropped, later segments unlinked, the
    /// intent consumed, and the journal usable afterwards.
    #[test]
    fn open_completes_truncate_from_intent_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=12u64 {
                j.append(i, 0, &[0u8; 100]).unwrap().wait().unwrap();
            }
        }
        write_intent_bytes(dir.path(), 4);

        let j2 = Journal::open(cfg).unwrap();
        assert_eq!(j2.last_seq(), Some(4), "intent truncate not completed");
        assert!(j2.read(5).unwrap().is_none());
        assert!(
            !dir.path().join("truncate.intent").exists(),
            "intent file must be consumed after completion"
        );
        j2.append(5, 0, b"new").unwrap().wait().unwrap();
        assert_eq!(j2.read(5).unwrap().unwrap().1, b"new".to_vec());
    }

    /// A torn/garbage intent file means the crash happened *while writing the
    /// intent* — before any destructive step — so it is ignored and removed.
    #[test]
    fn corrupt_truncate_intent_is_ignored_and_removed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
            for i in 1..=3u64 {
                j.append(i, 0, b"x").unwrap().wait().unwrap();
            }
        }
        std::fs::write(dir.path().join("truncate.intent"), b"garbage").unwrap();

        let j2 = Journal::open(JournalConfig::new(dir.path())).unwrap();
        assert_eq!(j2.last_seq(), Some(3), "journal must be intact");
        assert!(
            !dir.path().join("truncate.intent").exists(),
            "corrupt intent must be removed"
        );
    }

    /// A completed truncate_after leaves no intent file behind.
    #[test]
    fn truncate_after_leaves_no_intent_file() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=10u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        j.truncate_after(5).unwrap().wait().unwrap();
        assert!(!dir.path().join("truncate.intent").exists());
    }

    /// `truncate_after` must pull the durable watermark back to keep_seq:
    /// otherwise a fresh append at a truncated seq is instantly reported
    /// durable (`wait_durable`/`on_durable`/`durable_seq`) before any fsync —
    /// in a Raft log that acks un-fsynced replacement entries.
    #[test]
    fn truncate_after_resets_durable_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=10u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        j.wait_durable(10).unwrap();

        j.truncate_after(5).unwrap().wait().unwrap();
        assert!(
            j.durable_seq() <= 5,
            "watermark {} not reset by truncate_after(5)",
            j.durable_seq()
        );

        // The replacement entry becomes durable through its own fsync.
        j.append(6, 0, b"new").unwrap().wait().unwrap();
        j.wait_durable(6).unwrap();
        assert!(j.durable_seq() >= 6);
    }

    /// Appends still queued in the writer channel when `truncate_after` runs
    /// must not be written afterwards: they would resurrect truncated seqs,
    /// produce non-monotonic on-disk sequences, and shadow a re-appended
    /// entry at the same seq with the stale payload.
    #[test]
    fn truncated_queued_appends_do_not_resurrect() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();

        for round in 0..200u64 {
            let base = round * 100;
            // Burst without waiting so some requests are still queued when
            // the truncate runs.
            for s in (base + 1)..=(base + 20) {
                let _ = j.append(s, 0, format!("old{round}").as_bytes()).unwrap();
            }
            j.truncate_after(base + 10).unwrap().wait().unwrap();

            // Re-append at a truncated seq with a different payload.
            j.append(base + 11, 0, format!("new{round}").as_bytes())
                .unwrap()
                .wait()
                .unwrap();

            let (_, p) = j.read(base + 11).unwrap().unwrap();
            assert_eq!(
                p,
                format!("new{round}").as_bytes(),
                "round {round}: stale queued append shadowed the re-appended entry"
            );
            let tail = j.read_range(base + 12..).unwrap();
            assert!(
                tail.is_empty(),
                "round {round}: truncated seqs resurrected: {:?}",
                tail.iter().map(|t| t.0).collect::<Vec<_>>()
            );
        }

        // On-disk state after reopen must be strictly ascending.
        drop(j);
        let j2 = Journal::open(JournalConfig::new(dir.path())).unwrap();
        let v = j2.read_range(..).unwrap();
        for w in v.windows(2) {
            assert!(
                w[0].0 < w[1].0,
                "non-monotonic on-disk seqs after reopen: {} then {}",
                w[0].0,
                w[1].0
            );
        }
    }

    /// A failed fsync must halt the journal (fail-stop): the failed batch
    /// errors, subsequent appends are refused, and the durable watermark
    /// never advances past the failure — a later successful fsync cannot
    /// retroactively make the lost bytes durable (the classic fsync-retry
    /// hazard).
    #[test]
    fn fsync_failure_halts_writer() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        j.wait_durable(1).unwrap();

        j.state.lock().unwrap().fail_next_fsync = true;
        let n = j.append(2, 0, b"y").unwrap();
        assert!(
            n.wait().is_err(),
            "append covered by a failed fsync must report the failure"
        );

        // Fail-stop: the journal refuses further work...
        let halted = match j.append(3, 0, b"z") {
            Err(JournalError::Closed) => true,
            Err(other) => panic!("expected Closed, got {other:?}"),
            Ok(n) => n.wait().is_err(),
        };
        assert!(halted, "writer kept accepting work after an fsync failure");

        // ...and the watermark stays at the last truly durable seq.
        assert_eq!(
            j.durable_seq(),
            1,
            "watermark advanced past a failed fsync"
        );
    }

    #[test]
    fn preallocated_journal_rotates_and_recovers_full_range() {
        // With the flag on, write across several segments (small segment_size to
        // force rotations), wait durable, reopen, and confirm the full range.
        // Uses 512-byte payloads × 200 records ≈ 110 KiB > 64 KiB → forces ≥2 segments.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.preallocate_segments = true;
        cfg.segment_size_bytes = 64 * 1024;
        let payload = vec![b'x'; 512];
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=200u64 {
                j.append(i, i, &payload).unwrap();
            }
            j.wait_durable(200).unwrap();
            assert_eq!(j.durable_seq(), 200);
        }
        let j2 = Journal::open(cfg).unwrap();
        assert_eq!(j2.first_seq(), Some(1));
        assert_eq!(j2.last_seq(), Some(200));
        // Confirm at least two segments were created (real rotation happened).
        let nseg = j2.state.lock().unwrap().segments.len();
        assert!(nseg >= 2, "expected ≥2 segments after rotation, got {nseg}");
    }

    #[test]
    fn open_removes_orphan_prealloc_temps() {
        let dir = tempfile::tempdir().unwrap();
        // Plant an orphan temp as if a crash hit between create and rename.
        std::fs::write(dir.path().join("seg-prealloc.9.tmp"), vec![0u8; 4096]).unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.preallocate_segments = true;
        cfg.segment_size_bytes = 64 * 1024;
        let _j = Journal::open(cfg).unwrap();
        assert!(
            !dir.path().join("seg-prealloc.9.tmp").exists(),
            "orphan temp cleaned at open"
        );
    }

    #[test]
    fn reopen_preallocated_active_segment_keeps_zero_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.preallocate_segments = true;
        cfg.segment_size_bytes = 64 * 1024;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=10u64 {
                j.append(i, i, b"x").unwrap();
            }
            j.wait_durable(10).unwrap();
        }
        let j2 = Journal::open(cfg).unwrap();
        assert_eq!(j2.first_seq(), Some(1));
        assert_eq!(j2.last_seq(), Some(10));
        // Active segment must be physically re-preallocated to segment_size, not
        // truncated to the logical end.
        let st = j2.state.lock().unwrap();
        let active = st.segments.last().unwrap();
        assert_eq!(
            active.physical_len().unwrap(),
            64 * 1024,
            "active segment re-preallocated after recovery"
        );
    }

    #[test]
    fn windowed_read_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        // Payloads large enough that the segment spans several 64 KiB index windows.
        let payload = vec![0x7Eu8; 20 * 1024];
        {
            let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
            for i in 1..=30u64 {
                j.append(i, i * 5, &payload).unwrap().wait().unwrap();
            }
        }
        // Reopen: the index is installed from the open-time scan, so reads take the
        // windowed path (not the empty-index fallback).
        let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        // 30 × 20 KiB ≈ 600 KiB over a 64 KiB gap → ~9 index entries; >= 5 is a safe,
        // tighter lower bound that also guards against a SPARSE_INDEX_GAP regression.
        assert!(j2.state.lock().unwrap().segments[0].index_snapshot().len() >= 5);
        for i in [1u64, 7, 15, 23, 30] {
            let (meta, p) = j2.read(i).unwrap().unwrap();
            assert_eq!(meta, i * 5);
            assert_eq!(p, payload);
        }
        assert!(j2.read(1000).unwrap().is_none()); // out of range
    }
}
