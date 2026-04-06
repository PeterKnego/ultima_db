use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    /// Requested key or version does not exist.
    #[error("key not found")]
    KeyNotFound,
    /// Requested snapshot version does not exist.
    #[error("snapshot version {0} not found")]
    VersionNotFound(u64),
    /// Returned when `begin_write` is called with a version ≤ the latest
    /// committed version. In a future multi-writer implementation this will
    /// also be returned when a write transaction conflicts with a concurrent
    /// commit.
    #[error("write conflict")]
    WriteConflict,
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
        let e = Error::WriteConflict;
        assert_eq!(e.to_string(), "write conflict");
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
}
