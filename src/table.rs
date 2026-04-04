use std::ops::RangeBounds;

use crate::btree::{BTree, BTreeRange};
use crate::Error;
use crate::Result;

pub struct Table<R> {
    data: BTree<R>,
    next_id: u64,
}

impl<R> Table<R> {
    pub fn new() -> Self {
        Self { data: BTree::new(), next_id: 1 }
    }

    pub fn insert(&mut self, record: R) -> u64 {
        assert!(self.next_id < u64::MAX, "Table ID overflow");
        let id = self.next_id;
        self.next_id += 1;
        self.data = self.data.insert(id, record);
        id
    }

    pub fn get(&self, id: u64) -> Option<&R> {
        self.data.get(id)
    }

    pub fn update(&mut self, id: u64, record: R) -> Result<()> {
        if self.data.get(id).is_none() {
            return Err(Error::KeyNotFound);
        }
        self.data = self.data.insert(id, record);
        Ok(())
    }

    pub fn delete(&mut self, id: u64) -> Result<()> {
        self.data = self.data.remove(id)?;
        Ok(())
    }

    pub fn range<'a>(&'a self, range: impl RangeBounds<u64> + 'a) -> BTreeRange<'a, R> {
        self.data.range(range)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<R> Clone for Table<R> {
    /// O(1): shares the underlying BTree root via Arc.
    fn clone(&self) -> Self {
        Table { data: self.data.clone(), next_id: self.next_id }
    }
}

impl<R> Default for Table<R> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_returns_id_starting_at_one() {
        let mut table: Table<String> = Table::new();
        assert_eq!(table.insert("first".to_string()), 1);
    }

    #[test]
    fn insert_returns_incrementing_ids() {
        let mut table: Table<String> = Table::new();
        assert_eq!(table.insert("a".to_string()), 1);
        assert_eq!(table.insert("b".to_string()), 2);
        assert_eq!(table.insert("c".to_string()), 3);
    }

    #[test]
    fn get_returns_inserted_record() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("hello".to_string());
        assert_eq!(table.get(id), Some(&"hello".to_string()));
    }

    #[test]
    fn get_on_absent_id_returns_none() {
        let table: Table<String> = Table::new();
        assert_eq!(table.get(99), None);
    }

    #[test]
    fn update_replaces_record() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("original".to_string());
        table.update(id, "updated".to_string()).unwrap();
        assert_eq!(table.get(id), Some(&"updated".to_string()));
    }

    #[test]
    fn update_on_absent_id_returns_key_not_found() {
        let mut table: Table<String> = Table::new();
        let result = table.update(99, "x".to_string());
        assert!(matches!(result, Err(crate::Error::KeyNotFound)));
    }

    #[test]
    fn delete_removes_record() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("bye".to_string());
        table.delete(id).unwrap();
        assert_eq!(table.get(id), None);
    }

    #[test]
    fn delete_on_absent_id_returns_key_not_found() {
        let mut table: Table<String> = Table::new();
        let result = table.delete(99);
        assert!(matches!(result, Err(crate::Error::KeyNotFound)));
    }

    #[test]
    fn range_yields_records_in_order() {
        let mut table: Table<&str> = Table::new();
        table.insert("a");
        table.insert("b");
        table.insert("c");
        let results: Vec<_> = table.range(1..=3).collect();
        assert_eq!(results, vec![(1, &"a"), (2, &"b"), (3, &"c")]);
    }

    #[test]
    fn range_with_partial_bounds() {
        let mut table: Table<&str> = Table::new();
        table.insert("a");
        table.insert("b");
        table.insert("c");
        let results: Vec<_> = table.range(2..).collect();
        assert_eq!(results, vec![(2, &"b"), (3, &"c")]);
    }

    #[test]
    fn range_on_empty_table_yields_nothing() {
        let table: Table<String> = Table::new();
        let results: Vec<_> = table.range(..).collect();
        assert!(results.is_empty());
    }

    #[test]
    fn new_table_is_empty() {
        let table: Table<String> = Table::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn len_reflects_insert_and_delete() {
        let mut table: Table<String> = Table::new();
        assert_eq!(table.len(), 0);
        let id = table.insert("a".to_string());
        assert_eq!(table.len(), 1);
        table.delete(id).unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn table_clone_is_independent() {
        let mut original: Table<String> = Table::new();
        original.insert("alice".to_string());
        let clone = original.clone();
        original.insert("bob".to_string()); // mutate original after clone
        // Clone is unaffected
        assert_eq!(clone.len(), 1);
        assert_eq!(clone.get(2), None);
    }

    #[test]
    fn table_clone_preserves_next_id() {
        let mut original: Table<String> = Table::new();
        original.insert("a".to_string()); // id 1
        original.insert("b".to_string()); // id 2
        let mut clone = original.clone();
        // Next insert in clone should continue from id 3
        let id = clone.insert("c".to_string());
        assert_eq!(id, 3);
        // Verify no ID collision
        assert_eq!(clone.get(1), Some(&"a".to_string()));
        assert_eq!(clone.get(3), Some(&"c".to_string()));
    }
}
