use std::any::Any;
use std::collections::HashMap;
use crate::{Error, Result};
use crate::table::Table;

/// An in-memory store holding named typed tables.
///
/// Each table is a [`Table<R>`] keyed by auto-increment `u64`.
/// The backing B-tree implementation will be replaced in Task 3 (MVCC).
pub struct Store {
    tables: HashMap<String, Box<dyn Any>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Open a named table for records of type `R`.
    ///
    /// Creates the table if it does not exist. Returns
    /// [`Error::TypeMismatch`] if the name was previously opened
    /// with a different type.
    pub fn open_table<R: 'static>(&mut self, name: &str) -> Result<&mut Table<R>> {
        if !self.tables.contains_key(name) {
            self.tables.insert(name.to_string(), Box::new(Table::<R>::new()));
        }
        self.tables
            .get_mut(name)
            .unwrap()
            .downcast_mut::<Table<R>>()
            .ok_or_else(|| Error::TypeMismatch(name.to_string()))
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_table_creates_empty_table() {
        let mut store = Store::new();
        let table = store.open_table::<String>("users").unwrap();
        assert!(table.is_empty());
    }

    #[test]
    fn open_table_returns_same_table_on_second_call() {
        let mut store = Store::new();
        let id = {
            let table = store.open_table::<String>("users").unwrap();
            table.insert("alice".to_string())
        };
        // First borrow ended at the closing brace above.
        let table = store.open_table::<String>("users").unwrap();
        assert_eq!(table.get(id), Some(&"alice".to_string()));
    }

    #[test]
    fn open_table_returns_type_mismatch_for_different_types() {
        let mut store = Store::new();
        store.open_table::<String>("t").unwrap();
        let result = store.open_table::<u64>("t");
        assert!(matches!(result, Err(crate::Error::TypeMismatch(_))));
    }
}
