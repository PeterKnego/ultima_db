// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

pub mod segment;

use std::path::PathBuf;
use std::sync::Mutex;

use crate::{Durability, JournalError, Notifier};
use segment::SegmentFile;

#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub dir: PathBuf,
    pub segment_size_bytes: u64,
    pub durability: Durability,
}

impl JournalConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            segment_size_bytes: 64 * 1024 * 1024,
            durability: Durability::Consistent,
        }
    }
}

pub struct Journal {
    inner: Mutex<JournalInner>,
}

pub(crate) struct JournalInner {
    pub config: JournalConfig,
    pub segments: Vec<SegmentFile>,
    pub first_seq: Option<u64>,
    pub last_seq: Option<u64>,
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

        let mut segments = Vec::new();
        let mut first_seq: Option<u64> = None;
        let mut last_seq: Option<u64> = None;
        for ent in entries {
            let seg = SegmentFile::open_for_read(&ent.path())?;
            let scan = seg.scan()?;
            if let Some(first) = scan.records.first()
                && first_seq.is_none()
            {
                first_seq = Some(first.seq);
            }
            if let Some(last) = scan.records.last() {
                last_seq = Some(last.seq);
            }
            segments.push(seg);
        }
        Ok(Self {
            inner: Mutex::new(JournalInner {
                config,
                segments,
                first_seq,
                last_seq,
            }),
        })
    }

    pub fn first_seq(&self) -> Option<u64> {
        self.inner.lock().unwrap().first_seq
    }
    pub fn last_seq(&self) -> Option<u64> {
        self.inner.lock().unwrap().last_seq
    }

    pub fn append(&self, seq: u64, meta: u64, payload: &[u8]) -> Result<Notifier, JournalError> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(last) = inner.last_seq
            && seq <= last
        {
            return Err(JournalError::NonMonotonicSeq { expected_gt: last, got: seq });
        }
        // Size guard.
        let body_len = 16 + payload.len();
        let total = (4 + body_len + 4) as u64;
        if total > inner.config.segment_size_bytes {
            return Err(JournalError::PayloadTooLargeForSegment {
                segment_size: inner.config.segment_size_bytes,
                record_size: total,
            });
        }
        // Open new segment if none, or current segment exceeds size threshold.
        let need_new = match inner.segments.last() {
            None => true,
            Some(seg) => seg.size()? >= inner.config.segment_size_bytes,
        };
        if need_new {
            let path = inner.config.dir.join(format!("seg-{:020}.log", seq));
            let seg = SegmentFile::create(&path, seq)?;
            inner.segments.push(seg);
        }
        let seg = inner.segments.last_mut().unwrap();
        seg.append_record(seq, meta, payload)?;
        seg.fsync()?;
        if inner.first_seq.is_none() {
            inner.first_seq = Some(seq);
        }
        inner.last_seq = Some(seq);
        Ok(Notifier::done())
    }

    pub fn read(&self, seq: u64) -> Result<Option<(u64, Vec<u8>)>, JournalError> {
        let inner = self.inner.lock().unwrap();
        let Some(seg) = inner.segments.iter().rev()
            .find(|s| s.base_seq() <= seq) else {
                return Ok(None);
            };
        // NOTE: this initial impl scans the whole segment linearly — correctness-first.
        // The sparse offset index built in Task 4 is unused here; the benchmark task will
        // profile and add a binary_search over the index to seek near seq before the linear
        // scan if read latency is too high.
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
        let n = j.inner.lock().unwrap().segments.len();
        assert!(n >= 2, "expected segment rotation, got {n} segments");
    }
}
