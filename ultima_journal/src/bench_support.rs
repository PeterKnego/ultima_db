// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bench-only hooks for isolating the two stages of a durable append.
//!
//! NOT part of the supported API — gated behind the `bench-support` feature
//! and `#[doc(hidden)]`. Exists so the autobench journal microbench can time
//! "the time to write a log entry" (encode + a single `write_all` into the
//! page cache) and "the time to fsync" (the `sync_data` durability barrier)
//! in isolation, on the calling thread, with no channel / notifier /
//! group-commit batching between the two costs. Those are exactly the
//! primitives the production writer pipeline (`journal::writer`) is built
//! around; this module just exposes them one at a time.

use std::path::Path;

use crate::error::JournalError;
use crate::journal::segment::SegmentFile;

/// A single segment file you can write to and fsync independently.
pub struct BenchSegment {
    seg: SegmentFile,
}

impl BenchSegment {
    /// Create a fresh segment at `path` with the given base seq.
    pub fn create(path: &Path, base_seq: u64) -> Result<Self, JournalError> {
        Ok(Self {
            seg: SegmentFile::create(path, base_seq)?,
        })
    }

    /// Write-only: encode one record and `write_all` it into the page cache
    /// via the real `SegmentFile::append_records` path. No fsync — the bytes
    /// are visible to readers but not yet durable.
    pub fn append_one(&mut self, seq: u64, meta: u64, payload: &[u8]) -> Result<(), JournalError> {
        self.seg.append_records(&[(seq, meta, payload)])?;
        Ok(())
    }

    /// Fsync-only: the `sync_data` barrier over every byte written so far.
    /// The same commit primitive the writer issues per group-commit.
    pub fn sync_data(&mut self) -> Result<(), JournalError> {
        self.seg.sync_data_for_bench()
    }

    /// Zero-fill + `sync_all` the segment out to `total_len` so later
    /// `append_one` calls overwrite pre-allocated, already-written extents
    /// instead of extending EOF. Lets the microbench measure a `sync_data`
    /// barrier with the size-extension metadata commit removed. See
    /// [`SegmentFile::preallocate_zerofill_for_bench`].
    pub fn preallocate(&mut self, total_len: u64) -> Result<(), JournalError> {
        self.seg.preallocate_zerofill_for_bench(total_len)
    }
}
