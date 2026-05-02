// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("corrupted segment {segment} at offset {offset}: {reason}")]
    Corrupted { segment: String, offset: u64, reason: String },
    #[error("non-monotonic seq: expected > {expected_gt}, got {got}")]
    NonMonotonicSeq { expected_gt: u64, got: u64 },
    #[error("seq out of range")]
    SeqOutOfRange,
    #[error("payload too large for segment: segment_size={segment_size}, record_size={record_size}")]
    PayloadTooLargeForSegment { segment_size: u64, record_size: u64 },
    #[error("journal closed")]
    Closed,
}

#[cfg(feature = "stable_value")]
#[derive(Debug, Error)]
pub enum StableValueError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("payload too large: limit={limit}, got={got}")]
    PayloadTooLarge { limit: u32, got: u32 },
    #[error("corrupted: {reason}")]
    Corrupted { reason: String },
    #[error("serialize: {0}")]
    Serialize(String),
    #[error("stable_value closed")]
    Closed,
}
