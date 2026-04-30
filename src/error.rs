use thiserror::Error;

use crate::intents::CommitWaiter;

#[derive(Debug, Error)]
pub enum Error {
    /// Requested key or version does not exist.
    #[error("key not found")]
    KeyNotFound,
    /// Requested snapshot version does not exist.
    #[error("snapshot version {0} not found")]
    VersionNotFound(u64),
    /// Returned when a write transaction conflicts with a concurrent writer.
    ///
    /// Two detection sites produce this error:
    /// 1. *Early-fail* inside `TableWriter::update`/`delete` — another active
    ///    writer already holds an intent on the same key. `wait_for` is
    ///    `Some(waiter)`; callers can block on it until the holder commits or
    ///    aborts, then retry.
    /// 2. *Commit-time* OCC — a writer that already committed overlapped on
    ///    a key or deleted a table. `wait_for` is `None`; the winner is gone.
    #[error("write conflict on table '{table}', keys {keys:?} (conflicting version {version})")]
    WriteConflict {
        table: String,
        keys: Vec<u64>,
        version: u64,
        wait_for: Option<CommitWaiter>,
    },
    /// Returned when a `Serializable` WriteTx's read set was invalidated by a
    /// concurrent committed transaction. The reading tx must abort and retry
    /// against a fresh base; there is no `wait_for` because the conflicting
    /// writer has already finished.
    #[error("serialization failure on table '{table}' (conflicting version {version})")]
    SerializationFailure {
        table: String,
        version: u64,
    },
    /// Returned when `begin_write` is called while another write transaction
    /// is active in [`WriterMode::SingleWriter`] mode.
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
    /// [`StoreConfig::require_explicit_version`] is `true`.
    #[error("explicit version required (require_explicit_version is enabled)")]
    ExplicitVersionRequired,
    /// An I/O or format error during persistence operations (WAL or checkpoint).
    #[error("persistence error: {0}")]
    Persistence(String),
    /// Table not registered in the type registry (required for persistence).
    #[error("table '{0}' not registered in type registry")]
    TableNotRegistered(String),
    /// WAL file is corrupted (bad CRC, truncated entry, etc.).
    #[error("WAL corrupted: {0}")]
    WalCorrupted(String),
    /// Checkpoint file is corrupted (bad magic, bad CRC, truncated, etc.).
    #[error("checkpoint corrupted: {0}")]
    CheckpointCorrupted(String),
    /// Table not present in the snapshot (used by bulk-load when a Delta
    /// targets a missing table or when `create_if_missing` is false).
    #[error("table '{0}' not found")]
    TableNotFound(String),
    /// Caller-provided bulk-load input failed validation (overlapping IDs
    /// across delta buckets, AutoId used in Delta, etc.).
    #[error("invalid bulk-load input: {0}")]
    InvalidBulkLoadInput(String),
}

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
        assert_eq!(e.to_string(), "write conflict on table 'users', keys [1, 2] (conflicting version 3)");
    }

    #[test]
    fn error_writer_busy_displays() {
        let e = Error::WriterBusy;
        assert_eq!(e.to_string(), "another write transaction is active (SingleWriter mode)");
    }

    #[test]
    fn error_type_mismatch_displays() {
        let e = Error::TypeMismatch("users".to_string());
        assert_eq!(e.to_string(), "table 'users' was opened with a different type");
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
        assert_eq!(e.to_string(), "index 'by_email' queried with wrong key type");
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
}
