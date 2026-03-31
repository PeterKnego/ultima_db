use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("key not found")]
    KeyNotFound,
    #[error("write conflict")]
    WriteConflict,
    #[error("table '{0}' was opened with a different type")]
    TypeMismatch(String),
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
}
