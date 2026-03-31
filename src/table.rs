use std::collections::BTreeMap;
use std::ops::RangeBounds;

use crate::Error;
use crate::Result;

pub struct Table<R> {
    data: BTreeMap<u64, R>,
    next_id: u64,
}

impl<R> Table<R> {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            next_id: 1,
        }
    }

    pub fn insert(&mut self, record: R) -> u64 {
        assert!(self.next_id < u64::MAX, "Table ID overflow");
        let id = self.next_id;
        self.next_id += 1;
        self.data.insert(id, record);
        id
    }

    pub fn get(&self, id: u64) -> Option<&R> {
        self.data.get(&id)
    }

    pub fn update(&mut self, id: u64, record: R) -> Result<()> {
        use std::collections::btree_map::Entry;
        match self.data.entry(id) {
            Entry::Occupied(mut e) => {
                e.insert(record);
                Ok(())
            }
            Entry::Vacant(_) => Err(Error::KeyNotFound),
        }
    }

    pub fn delete(&mut self, id: u64) -> Result<()> {
        self.data.remove(&id).map(|_| ()).ok_or(Error::KeyNotFound)
    }

    pub fn range(&self, range: impl RangeBounds<u64>) -> impl Iterator<Item = (u64, &R)> {
        self.data.range(range).map(|(&k, v)| (k, v))
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
}
