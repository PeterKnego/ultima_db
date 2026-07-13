// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Streaming wire format for UltimaDB checkpoints.

#[cfg(feature = "persistence")]
pub mod build;
/// Wire-format encode/decode primitives (file/table headers, magic bytes,
/// version constants) shared by the build and install paths.
pub mod codec;
pub mod install;

#[cfg(feature = "persistence")]
pub use build::SnapshotReader;
pub use install::{InstallOptions, OnExtra, OnUnknown};

use thiserror::Error;

/// Errors from building or installing a snapshot-stream wire payload
/// (`Store::snapshot_stream`, `Store::install_snapshot_stream`,
/// `Store::bulk_load_stream`). The install path treats the wire stream as
/// untrusted input and validates magic bytes, per-table and whole-file CRCs,
/// and row counts before touching destination state; all install-path
/// failures leave the destination `Store` untouched.
#[derive(Debug, Error)]
pub enum SnapshotStreamError {
    /// Underlying I/O failure while reading or writing the wire stream.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// File header did not start with the expected `ULTSNAP\0` magic bytes.
    #[error("bad magic")]
    BadMagic,
    /// File header declared a `format_ver` this build does not understand.
    #[error("unsupported format version: {0}")]
    BadFormatVersion(u16),
    /// The stream ended before all declared headers/rows/trailers were read.
    #[error("truncated wire stream")]
    Truncated,
    /// The stream contains bytes that are structurally invalid (e.g. a
    /// header field that is not valid UTF-8) — distinct from `Truncated`
    /// because the bytes are present, just not well-formed.
    #[error("malformed wire stream: {0}")]
    Malformed(&'static str),
    /// A table's row-stream CRC (or, when `table` is `None`, the whole-file
    /// trailer CRC) did not match the recomputed checksum.
    #[error("crc mismatch in {table:?}")]
    BadCrc {
        /// Name of the table whose CRC mismatched, or `None` for a
        /// whole-file trailer mismatch.
        table: Option<String>,
    },
    /// A table in the wire stream is not registered on the destination
    /// `Store` (via `Store::register_table`) and `InstallOptions::on_unknown_tables`
    /// is `OnUnknown::Error`.
    #[error("unknown table {name} (type_id {type_id})")]
    UnknownTable {
        /// Table name as it appears in the wire stream's table header.
        name: String,
        /// The record type id recorded in the wire stream (a best-effort
        /// mismatch hint — not stable across Rust builds; the install path
        /// dispatches by table name, not by this id).
        type_id: u64,
    },
    /// The file trailer's declared `total_rows` did not match the sum of
    /// per-table `row_count`s decoded from the table headers (which are
    /// themselves covered by the whole-file CRC). Catches a corrupted
    /// trailer field that the CRC alone would not detect, since the CRC is
    /// finalized before `total_rows` is written.
    #[error("row count mismatch: trailer claimed {trailer}, actual {actual}")]
    RowCountMismatch {
        /// Row count declared in the file trailer.
        trailer: u64,
        /// Row count actually decoded from the per-table headers.
        actual: u64,
    },
    /// The destination table has a custom index (`IndexKind::Custom`), which
    /// has no generic rebuild-from-rows hook. Drop the index before install
    /// and redefine it afterward.
    #[error(
        "custom index '{index}' on table '{table}' cannot be rebuilt by install_snapshot_stream; \
         drop the index before install and redefine after"
    )]
    CustomIndexUnsupported {
        /// Table the unsupported custom index is defined on.
        table: String,
        /// Name of the custom index.
        index: String,
    },
    /// A row's bytes failed to deserialize into the destination table's
    /// record type.
    #[error("invalid wire payload for table '{table}': {reason}")]
    InvalidPayload {
        /// Table the invalid row belongs to.
        table: String,
        /// Deserialization failure detail.
        reason: String,
    },
    /// `Store::snapshot_stream`/`open_checkpoint_reader` was asked for a
    /// snapshot version the store no longer retains (or a checkpoint that
    /// does not exist on disk).
    #[error("version not found: {0}")]
    VersionNotFound(u64),
    /// Reserved for a future explicit-version install path
    /// (`InstallOptions::commit_version`); not currently produced, since v1
    /// ignores that field.
    #[error(
        "commit_version {requested} must be strictly greater than the current \
         latest_version {latest}"
    )]
    InvalidCommitVersion {
        /// The requested commit version.
        requested: u64,
        /// The destination store's current latest version.
        latest: u64,
    },
    /// The final atomic install (`Store::bulk_load_batch`) failed; wraps the
    /// underlying [`crate::Error`] (e.g. `WriteConflict`) so callers can
    /// pattern-match on it directly instead of parsing a string.
    #[error("bulk load: {0}")]
    BulkLoad(#[from] crate::Error),
}
