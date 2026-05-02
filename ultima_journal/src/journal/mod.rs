// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

pub mod segment;

use std::path::PathBuf;
use std::sync::Mutex;

use crate::{Durability, JournalError};
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
    #[allow(dead_code)]
    pub config: JournalConfig,
    #[allow(dead_code)]
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
}
