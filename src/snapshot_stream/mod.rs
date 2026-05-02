// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Streaming wire format for UltimaDB checkpoints.

pub mod codec;
pub mod build;
pub mod install;

pub use build::SnapshotReader;
pub use install::{InstallOptions, OnUnknown};

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
    #[error("crc mismatch in {table:?}")]
    BadCrc { table: Option<String> },
    #[error("unknown table {name} (type_id {type_id})")]
    UnknownTable { name: String, type_id: u64 },
    #[error("bulk load: {0}")]
    Bulk(String),
}
