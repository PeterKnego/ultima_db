// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Streaming wire format for UltimaDB checkpoints.

#[cfg(feature = "persistence")]
pub mod build;
pub mod codec;
pub mod install;

#[cfg(feature = "persistence")]
pub use build::SnapshotReader;
pub use install::{InstallOptions, OnExtra, OnUnknown};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SnapshotStreamError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("bad magic")]
    BadMagic,
    #[error("unsupported format version: {0}")]
    BadFormatVersion(u16),
    #[error("truncated wire stream")]
    Truncated,
    #[error("malformed wire stream: {0}")]
    Malformed(&'static str),
    #[error("crc mismatch in {table:?}")]
    BadCrc { table: Option<String> },
    #[error("unknown table {name} (type_id {type_id})")]
    UnknownTable { name: String, type_id: u64 },
    #[error("row count mismatch: trailer claimed {trailer}, actual {actual}")]
    RowCountMismatch { trailer: u64, actual: u64 },
    #[error(
        "custom index '{index}' on table '{table}' cannot be rebuilt by install_snapshot_stream; \
         drop the index before install and redefine after"
    )]
    CustomIndexUnsupported { table: String, index: String },
    #[error("invalid wire payload for table '{table}': {reason}")]
    InvalidPayload { table: String, reason: String },
    #[error("version not found: {0}")]
    VersionNotFound(u64),
    #[error(
        "commit_version {requested} must be strictly greater than the current \
         latest_version {latest}"
    )]
    InvalidCommitVersion { requested: u64, latest: u64 },
    #[error("bulk load: {0}")]
    BulkLoad(#[from] crate::Error),
}
