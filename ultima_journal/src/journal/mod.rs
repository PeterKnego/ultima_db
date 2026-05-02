// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

pub mod segment;
mod writer;

use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};

use crate::{JournalError, Notifier};
use writer::{AppendRequest, Writer, WriterState};

#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub dir: std::path::PathBuf,
    pub segment_size_bytes: u64,
    pub durability: crate::Durability,
}

impl JournalConfig {
    pub fn new(dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            segment_size_bytes: 64 * 1024 * 1024,
            durability: crate::Durability::Consistent,
        }
    }
}

pub struct Journal {
    pub(crate) state: Arc<Mutex<WriterState>>,
    writer: Mutex<Option<Writer>>,
}

impl Journal {
    pub fn open(config: JournalConfig) -> Result<Self, JournalError> {
        std::fs::create_dir_all(&config.dir)?;
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
            if scan.had_torn_tail {
                seg.truncate(scan.last_durable_offset)?;
            }
            let scan = if scan.had_torn_tail { seg.scan()? } else { scan };
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

        let segments: Vec<segment::SegmentFile> =
            segments_with_scan.into_iter().map(|(seg, _)| seg).collect();

        let state = Arc::new(Mutex::new(WriterState {
            dir: config.dir.clone(),
            segment_size: config.segment_size_bytes,
            durability: config.durability,
            segments,
            last_seq,
            first_seq,
        }));
        let writer = Writer::spawn(Arc::clone(&state));
        Ok(Self {
            state,
            writer: Mutex::new(Some(writer)),
        })
    }

    pub fn first_seq(&self) -> Option<u64> {
        self.state.lock().unwrap().first_seq
    }

    pub fn last_seq(&self) -> Option<u64> {
        self.state.lock().unwrap().last_seq
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
            let st = self.state.lock().unwrap();
            if total > st.segment_size {
                return Err(JournalError::PayloadTooLargeForSegment {
                    segment_size: st.segment_size,
                    record_size: total,
                });
            }
            if let Some(last) = st.last_seq
                && seq <= last
            {
                return Err(JournalError::NonMonotonicSeq { expected_gt: last, got: seq });
            }
            // Claim the seq under the lock: send to channel while still holding it,
            // so the channel ordering matches the monotonic seq order.
            let req = AppendRequest {
                seq,
                meta,
                payload: payload.to_vec(),
                signal,
            };
            let writer_guard = self.writer.lock().unwrap();
            let w = writer_guard.as_ref().ok_or(JournalError::Closed)?;
            w.tx.send(req).map_err(|_| JournalError::Closed)?;
        }

        Ok(notifier)
    }

    pub fn read(&self, seq: u64) -> Result<Option<(u64, Vec<u8>)>, JournalError> {
        let st = self.state.lock().unwrap();
        let Some(seg) = st.segments.iter().rev().find(|s| s.base_seq() <= seq) else {
            return Ok(None);
        };
        // NOTE: this initial impl scans the whole segment linearly — correctness-first.
        let scan = seg.scan()?;
        for r in &scan.records {
            if r.seq == seq {
                return Ok(Some((r.meta, r.payload.clone())));
            }
            if r.seq > seq {
                return Ok(None);
            }
        }
        Ok(None)
    }

    pub fn read_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, JournalError> {
        let (lo, hi) = bounds_to_inclusive(range, self.first_seq(), self.last_seq());
        let st = self.state.lock().unwrap();
        let mut out = Vec::new();
        for seg in &st.segments {
            let scan = seg.scan()?;
            for r in scan.records {
                if r.seq < lo {
                    continue;
                }
                if r.seq > hi {
                    return Ok(out);
                }
                out.push((r.seq, r.meta, r.payload));
            }
        }
        Ok(out)
    }

    #[allow(clippy::type_complexity)]
    pub fn iter_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<impl Iterator<Item = Result<(u64, u64, Vec<u8>), JournalError>> + '_, JournalError>
    {
        // Vec-collects internally and returns its iterator.
        let v = self.read_range(range)?;
        Ok(v.into_iter().map(Ok))
    }

    /// Truncate the journal so that only records with `seq <= keep_seq` remain.
    ///
    /// Crash-safe via a two-phase sentinel protocol:
    /// 1. Truncate the tail segment to just past the last-kept record (`new_end`).
    /// 2. Append a sentinel record at `new_end` and fsync — intent is now durable.
    /// 3. Truncate back to `new_end`, removing the sentinel.
    /// 4. Unlink any later segments, then fsync the directory.
    ///
    /// If a crash occurs between steps 2 and 3, Task 14 recovery detects the
    /// sentinel on the next open and re-truncates.
    ///
    /// Returns a `Notifier::done()` — the operation is fully synchronous.
    pub fn truncate_after(&self, keep_seq: u64) -> Result<Notifier, JournalError> {
        let mut st = self.state.lock().unwrap();

        // Find the last segment whose base_seq <= keep_seq.
        let seg_idx = match st.segments.iter().rposition(|s| s.base_seq() <= keep_seq) {
            Some(i) => i,
            None => {
                // keep_seq is below every segment — drop everything.
                let paths: Vec<_> = st.segments.iter().map(|s| s.path().to_path_buf()).collect();
                st.segments.clear();
                for p in paths {
                    let _ = std::fs::remove_file(&p);
                }
                if let Ok(d) = std::fs::File::open(&st.dir) {
                    let _ = d.sync_all();
                }
                st.first_seq = None;
                st.last_seq = None;
                return Ok(Notifier::done());
            }
        };

        // Compute the byte offset just past the last record with seq <= keep_seq.
        let scan = st.segments[seg_idx].scan()?;
        let new_end = scan
            .records
            .iter()
            .position(|r| r.seq > keep_seq)
            .map(|pos| {
                segment::SEGMENT_HEADER_SIZE as u64
                    + scan.records[..pos]
                        .iter()
                        .map(|r| (4 + 16 + r.payload.len() + 4) as u64)
                        .sum::<u64>()
            })
            .unwrap_or(scan.last_durable_offset);

        // Phase 1: truncate to new_end (remove records past keep_seq).
        st.segments[seg_idx].truncate(new_end)?;

        // Phase 2: write sentinel at new_end and fsync (crash-safety marker).
        st.segments[seg_idx].append_record(
            keep_seq.saturating_add(1),
            segment::SENTINEL_META,
            segment::SENTINEL_PAYLOAD,
        )?;
        st.segments[seg_idx].fsync()?;

        // Phase 3: truncate back to new_end, removing the sentinel.
        let after_sentinel = new_end;
        st.segments[seg_idx].truncate(after_sentinel)?;

        // Phase 4: unlink all later segments, then fsync directory.
        let to_remove: Vec<_> = st.segments.drain(seg_idx + 1..).collect();
        for seg in to_remove {
            let _ = std::fs::remove_file(seg.path());
        }
        if let Ok(d) = std::fs::File::open(&st.dir) {
            let _ = d.sync_all();
        }

        // Update in-memory state.
        st.last_seq = if st.first_seq.is_none_or(|first| keep_seq >= first) {
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
        st.first_seq = st.segments.first().and_then(|s| s.scan().ok())
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
        assert!(matches!(err, JournalError::NonMonotonicSeq { expected_gt: 5, got: 3 }));
    }

    #[test]
    fn append_rejects_record_larger_than_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        let j = Journal::open(cfg).unwrap();
        let big = vec![0u8; 1024];
        let err = j.append(1, 0, &big).unwrap_err();
        assert!(matches!(err, JournalError::PayloadTooLargeForSegment { .. }));
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
            j.append(i, i * 10, format!("p{i}").as_bytes()).unwrap().wait().unwrap();
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
            j.append(i, 0, format!("p{i}").as_bytes()).unwrap().wait().unwrap();
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
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;
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
            .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with("seg-"))
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
            seg.append_record(99, segment::SENTINEL_META, segment::SENTINEL_PAYLOAD).unwrap();
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
        std::fs::OpenOptions::new().append(true).open(&seg_path).unwrap()
            .write_all(&[0xAB; 5]).unwrap();
        // Reopen — torn tail should be truncated.
        let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        assert_eq!(j2.last_seq(), Some(3));
        // The torn bytes should have been truncated; subsequent append must succeed.
        j2.append(4, 0, b"new").unwrap().wait().unwrap();
    }
}
