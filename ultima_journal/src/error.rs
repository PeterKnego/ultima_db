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

impl Clone for JournalError {
    fn clone(&self) -> Self {
        use JournalError::*;
        match self {
            Io(e) => Io(std::io::Error::new(e.kind(), e.to_string())),
            Corrupted { segment, offset, reason } => Corrupted {
                segment: segment.clone(),
                offset: *offset,
                reason: reason.clone(),
            },
            NonMonotonicSeq { expected_gt, got } => NonMonotonicSeq {
                expected_gt: *expected_gt,
                got: *got,
            },
            SeqOutOfRange => SeqOutOfRange,
            PayloadTooLargeForSegment { segment_size, record_size } => PayloadTooLargeForSegment {
                segment_size: *segment_size,
                record_size: *record_size,
            },
            Closed => Closed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_covers_every_variant() {
        let cases = vec![
            JournalError::Io(io::Error::other("boom")),
            JournalError::Corrupted {
                segment: "s".into(),
                offset: 7,
                reason: "r".into(),
            },
            JournalError::NonMonotonicSeq { expected_gt: 3, got: 1 },
            JournalError::SeqOutOfRange,
            JournalError::PayloadTooLargeForSegment {
                segment_size: 64,
                record_size: 128,
            },
            JournalError::Closed,
        ];
        for e in &cases {
            // Ensure clone() doesn't panic and produces a value with the same Display.
            let c = e.clone();
            assert_eq!(format!("{e}"), format!("{c}"));
        }
    }
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
