// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use thiserror::Error;

use crate::intents::CommitWaiter;

/// The crate-wide error type returned by fallible `Store`/`Table`/transaction
/// operations. Each variant documents when it occurs and, where relevant,
/// how a caller should react (retry, abort, recover).
#[derive(Debug, Error)]
pub enum Error {
    /// Requested key or version does not exist.
    #[error("key not found")]
    KeyNotFound,
    /// Requested snapshot version does not exist.
    #[error("snapshot version {0} not found")]
    VersionNotFound(u64),
    /// Returned when a write transaction conflicts with a concurrent writer,
    /// or when an explicit version is no longer valid at `begin_write`. Callers
    /// should retry against a fresh base. If `wait_for` is `Some`, blocking on it
    /// may allow retry to succeed; if `None`, the conflicting writer has finished
    /// and retry must acquire a fresh snapshot.
    #[error("write conflict on table '{table}', keys {keys:?} (conflicting version {version})")]
    WriteConflict {
        /// Name of the table on which the conflict was detected. Empty when
        /// the conflict was a version-level check at `begin_write` rather than
        /// a key-level overlap.
        table: String,
        /// The overlapping keys that caused the conflict. Empty when the
        /// conflict was a version-level check at `begin_write`.
        keys: Vec<u64>,
        /// The conflicting writer's version: the intent holder's base version
        /// for intent conflicts, the winning commit's version for OCC, or the
        /// latest version for version-level checks at `begin_write`.
        version: u64,
        /// `Some(waiter)` when blocking may resolve the conflict (intent holder
        /// in progress); `None` otherwise (writer finished, or version-level
        /// check at `begin_write`).
        wait_for: Option<CommitWaiter>,
    },
    /// Returned when a `Serializable` WriteTx's read set was invalidated by a
    /// concurrent committed transaction. The reading tx must abort and retry
    /// against a fresh base; there is no `wait_for` because the conflicting
    /// writer has already finished.
    #[error("serialization failure on table '{table}' (conflicting version {version})")]
    SerializationFailure {
        /// Name of the table whose read set was invalidated.
        table: String,
        /// Version of the concurrent transaction that invalidated the read set.
        version: u64,
    },
    /// Returned when a MultiWriter commit contained index DDL
    /// (`define_index` / `define_custom_index`) on a table that a concurrent
    /// transaction committed to since this transaction's base snapshot. The
    /// merge slow path replays only write-set keys, so the new index
    /// definition cannot be carried over — the commit is refused instead of
    /// silently dropping the DDL (see task41).
    #[error(
        "index DDL on table '{table}' conflicts with a concurrent commit; \
         retry the DDL in its own transaction when the table is quiet"
    )]
    IndexDdlConflict {
        /// Name of the table the index DDL targeted.
        table: String,
    },
    /// Returned when `begin_write` is called while another write transaction
    /// is active in [`WriterMode::SingleWriter`](crate::WriterMode::SingleWriter) mode.
    #[error("another write transaction is active (SingleWriter mode)")]
    WriterBusy,
    /// Table opened with a different record type than its original creation.
    #[error("table '{0}' was opened with a different type")]
    TypeMismatch(String),
    /// Unique index constraint violation.
    #[error("duplicate key in unique index '{0}'")]
    DuplicateKey(String),
    /// Requested index name does not exist on the table.
    #[error("index '{0}' not found")]
    IndexNotFound(String),
    /// Index queried with a different key type than its definition.
    #[error("index '{0}' queried with wrong key type")]
    IndexTypeMismatch(String),
    /// Custom index with this name already exists.
    #[error("index '{0}' already exists")]
    IndexAlreadyExists(String),
    /// Returned when `begin_write(None)` is called but
    /// [`StoreConfig::require_explicit_version`](crate::StoreConfig::require_explicit_version) is `true`.
    #[error("explicit version required (require_explicit_version is enabled)")]
    ExplicitVersionRequired,
    /// An I/O or format error during persistence operations (WAL or checkpoint).
    #[error("persistence error: {0}")]
    Persistence(String),
    /// The store has been poisoned by a durability failure (a WAL write or
    /// fsync failed). All subsequent writes are refused; recover by dropping
    /// the `Store` and re-creating it via `Store::recover`.
    #[error("store poisoned by a durability failure: {0}")]
    Poisoned(String),
    /// Table not registered in the type registry (required for persistence).
    #[error("table '{0}' not registered in type registry")]
    TableNotRegistered(String),
    /// WAL file is corrupted (bad CRC, truncated entry, etc.).
    #[error("WAL corrupted: {0}")]
    WalCorrupted(String),
    /// Checkpoint file is corrupted (bad magic, bad CRC, truncated, etc.).
    #[error("checkpoint corrupted: {0}")]
    CheckpointCorrupted(String),
    /// Recovery found WAL commits that were made on top of a bulk load no
    /// checkpoint covers. Bulk loads are not WAL-logged, so those commits
    /// cannot be replayed (their semantics assume the post-load state).
    /// Recover from a newer checkpoint, or re-run the load and subsequent
    /// writes. Avoid the situation by loading with `checkpoint_after: true`
    /// (the default) or checkpointing manually before further commits.
    #[error(
        "recovery requires a checkpoint covering the bulk load at version {version}: \
         later WAL commits cannot be replayed against pre-load state"
    )]
    BulkLoadNotCheckpointed {
        /// The version at which the uncovered bulk load was installed.
        version: u64,
    },
    /// Table not present in the snapshot (used by bulk-load when a Delta
    /// targets a missing table or when `create_if_missing` is false).
    #[error("table '{0}' not found")]
    TableNotFound(String),
    /// Caller-provided bulk-load input failed validation (overlapping IDs
    /// across delta buckets, AutoId used in Delta, etc.).
    #[error("invalid bulk-load input: {0}")]
    InvalidBulkLoadInput(String),
}

/// Crate-wide result alias: `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_key_not_found_displays() {
        let e = Error::KeyNotFound;
        assert_eq!(e.to_string(), "key not found");
    }

    #[test]
    fn error_write_conflict_displays() {
        let e = Error::WriteConflict {
            table: "users".to_string(),
            keys: vec![1, 2],
            version: 3,
            wait_for: None,
        };
        assert_eq!(
            e.to_string(),
            "write conflict on table 'users', keys [1, 2] (conflicting version 3)"
        );
    }

    #[test]
    fn index_ddl_conflict_display() {
        let e = Error::IndexDdlConflict {
            table: "users".to_string(),
        };
        assert_eq!(
            e.to_string(),
            "index DDL on table 'users' conflicts with a concurrent commit; \
             retry the DDL in its own transaction when the table is quiet"
        );
    }

    #[test]
    fn error_writer_busy_displays() {
        let e = Error::WriterBusy;
        assert_eq!(
            e.to_string(),
            "another write transaction is active (SingleWriter mode)"
        );
    }

    #[test]
    fn error_type_mismatch_displays() {
        let e = Error::TypeMismatch("users".to_string());
        assert_eq!(
            e.to_string(),
            "table 'users' was opened with a different type"
        );
    }

    #[test]
    fn error_duplicate_key_displays() {
        let e = Error::DuplicateKey("by_email".to_string());
        assert_eq!(e.to_string(), "duplicate key in unique index 'by_email'");
    }

    #[test]
    fn error_index_not_found_displays() {
        let e = Error::IndexNotFound("by_age".to_string());
        assert_eq!(e.to_string(), "index 'by_age' not found");
    }

    #[test]
    fn error_index_type_mismatch_displays() {
        let e = Error::IndexTypeMismatch("by_email".to_string());
        assert_eq!(
            e.to_string(),
            "index 'by_email' queried with wrong key type"
        );
    }

    #[test]
    fn error_version_not_found_displays() {
        let e = Error::VersionNotFound(42);
        assert_eq!(e.to_string(), "snapshot version 42 not found");
    }

    #[test]
    fn error_index_already_exists_displays() {
        let e = Error::IndexAlreadyExists("my_index".to_string());
        assert_eq!(e.to_string(), "index 'my_index' already exists");
    }

    #[test]
    fn error_explicit_version_required_displays() {
        let e = Error::ExplicitVersionRequired;
        assert_eq!(
            e.to_string(),
            "explicit version required (require_explicit_version is enabled)"
        );
    }

    #[test]
    fn error_serialization_failure_displays() {
        let e = Error::SerializationFailure {
            table: "accounts".to_string(),
            version: 5,
        };
        assert_eq!(
            e.to_string(),
            "serialization failure on table 'accounts' (conflicting version 5)"
        );
    }

    #[test]
    fn error_poisoned_displays() {
        let e = Error::Poisoned("WAL fsync failed: disk error".to_string());
        assert_eq!(
            e.to_string(),
            "store poisoned by a durability failure: WAL fsync failed: disk error"
        );
    }
}
