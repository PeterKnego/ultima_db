use std::collections::HashMap;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::btree::BTree;
use crate::index::{IndexKind, IndexMaintainer, ManagedIndex, NonUniqueStorage, UniqueStorage};
use crate::{Error, Result};

pub struct Table<R> {
    data: BTree<u64, R>,
    next_id: u64,
    indexes: HashMap<String, Box<dyn IndexMaintainer<R>>>,
}

impl<R: Send + Sync + 'static> Table<R> {
    /// Creates a new, empty table with auto-incrementing IDs starting at 1.
    pub fn new() -> Self {
        Self { data: BTree::new(), next_id: 1, indexes: HashMap::new() }
    }

    /// Insert a record. Returns the auto-assigned ID, or an error if a unique
    /// index constraint is violated.
    pub fn insert(&mut self, record: R) -> Result<u64> {
        assert!(self.next_id < u64::MAX, "Table ID overflow");
        let id = self.next_id;

        // Update all indexes; rollback on failure.
        // SAFETY: We collect raw pointers to index values to avoid borrowing
        // `self.indexes` mutably while iterating. This is sound because:
        // 1. We hold `&mut self`, so no concurrent access is possible.
        // 2. The HashMap is not structurally modified (no insert/remove) during
        //    this loop — only the index values themselves are mutated in place.
        // 3. Each pointer is dereferenced at most once per loop iteration.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R>>> = self
            .indexes
            .values_mut()
            .map(|v| v as *mut _)
            .collect();
        for (applied, ptr) in ptrs.iter().enumerate() {
            let idx = unsafe { &mut **ptr };
            if let Err(e) = idx.on_insert(id, &record) {
                // Rollback previously applied indexes.
                for prev_ptr in &ptrs[..applied] {
                    let prev_idx = unsafe { &mut **prev_ptr };
                    prev_idx.on_delete(id, &record);
                }
                return Err(e);
            }
        }

        self.next_id += 1;
        self.data = self.data.insert(id, record);
        Ok(id)
    }

    /// Look up a record by its ID.
    pub fn get(&self, id: u64) -> Option<&R> {
        self.data.get(&id)
    }

    /// Update a record by its ID. Returns an error if the ID does not exist
    /// or if a unique index constraint is violated.
    pub fn update(&mut self, id: u64, record: R) -> Result<()> {
        let old = self.data.get_arc(&id).ok_or(Error::KeyNotFound)?;

        // Update all indexes; rollback on failure.
        // SAFETY: Same invariants as `insert` — see comment there.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R>>> = self
            .indexes
            .values_mut()
            .map(|v| v as *mut _)
            .collect();
        for (applied, ptr) in ptrs.iter().enumerate() {
            let idx = unsafe { &mut **ptr };
            if let Err(e) = idx.on_update(id, &old, &record) {
                // Rollback previously applied indexes by reversing the update.
                for prev_ptr in &ptrs[..applied] {
                    let prev_idx = unsafe { &mut **prev_ptr };
                    // Reverse: update back from new -> old. This should never
                    // fail because we're restoring previously-valid values.
                    let rollback_result = prev_idx.on_update(id, &record, &old);
                    debug_assert!(rollback_result.is_ok(), "index rollback failed: {:?}", rollback_result);
                }
                return Err(e);
            }
        }

        self.data = self.data.insert(id, record);
        Ok(())
    }

    /// Delete a record by its ID. Returns an error if the ID does not exist.
    pub fn delete(&mut self, id: u64) -> Result<()> {
        let old = self.data.get_arc(&id).ok_or(Error::KeyNotFound)?;
        // Remove from all indexes before removing from data tree.
        for idx in self.indexes.values_mut() {
            idx.on_delete(id, &old);
        }
        self.data = self.data.remove(&id)?;
        Ok(())
    }

    /// Returns an iterator over records within the specified ID range.
    pub fn range<'a>(&'a self, range: impl RangeBounds<u64> + 'a) -> impl Iterator<Item = (u64, &'a R)> + 'a {
        self.data.range(range).map(|(&k, v)| (k, v))
    }

    /// Returns the number of records in the table.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the table contains no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    // -----------------------------------------------------------------------
    // Index management
    // -----------------------------------------------------------------------

    /// Define a secondary index. If the table already contains data, the index
    /// is backfilled. Returns an error if the index name is already taken or
    /// if backfilling hits a unique constraint violation.
    pub fn define_index<K: Ord + Clone + Send + Sync + 'static>(
        &mut self,
        name: &str,
        kind: IndexKind,
        extractor: impl Fn(&R) -> K + Send + Sync + 'static,
    ) -> Result<()> {
        if let Some(existing) = self.indexes.get(name) {
            if existing.kind() != kind {
                return Err(Error::IndexTypeMismatch(name.to_string()));
            }
            // Same name and kind — idempotent. (We can't verify the extractor
            // or key type are the same, so trust the caller.)
            return Ok(());
        }
        let extractor = Arc::new(extractor);
        let mut index: Box<dyn IndexMaintainer<R>> = match kind {
            IndexKind::Unique => Box::new(ManagedIndex::<R, K, UniqueStorage<K>>::new(
                name.to_string(),
                kind,
                extractor,
                UniqueStorage::new(),
            )),
            IndexKind::NonUnique => Box::new(ManagedIndex::<R, K, NonUniqueStorage<K>>::new(
                name.to_string(),
                kind,
                extractor,
                NonUniqueStorage::new(),
            )),
        };
        // Backfill from existing data.
        for (&id, record) in self.data.range(..) {
            index.on_insert(id, record)?;
        }
        self.indexes.insert(name.to_string(), index);
        Ok(())
    }

    /// Look up a single record by a unique index.
    pub fn get_unique<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &K,
    ) -> Result<Option<(u64, &R)>> {
        let idx = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;
        let managed = idx
            .as_any()
            .downcast_ref::<ManagedIndex<R, K, UniqueStorage<K>>>()
            .ok_or_else(|| Error::IndexTypeMismatch(index_name.to_string()))?;
        match managed.storage().get(key) {
            Some(id) => Ok(self.data.get(&id).map(|r| (id, r))),
            None => Ok(None),
        }
    }

    /// Look up records by a non-unique index key.
    pub fn get_by_index<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &K,
    ) -> Result<Vec<(u64, &R)>> {
        let idx = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;
        let managed = idx
            .as_any()
            .downcast_ref::<ManagedIndex<R, K, NonUniqueStorage<K>>>()
            .ok_or_else(|| Error::IndexTypeMismatch(index_name.to_string()))?;
        Ok(managed
            .storage()
            .get_ids(key)
            .filter_map(|id| self.data.get(&id).map(|r| (id, r)))
            .collect())
    }

    /// Look up records by index key (works for both unique and non-unique).
    pub fn get_by_key<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &K,
    ) -> Result<Vec<(u64, &R)>> {
        let idx = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        // Try unique first, then non-unique.
        if let Some(managed) = idx.as_any().downcast_ref::<ManagedIndex<R, K, UniqueStorage<K>>>() {
            return Ok(managed
                .storage()
                .get(key)
                .into_iter()
                .filter_map(|id| self.data.get(&id).map(|r| (id, r)))
                .collect());
        }
        let managed = idx
            .as_any()
            .downcast_ref::<ManagedIndex<R, K, NonUniqueStorage<K>>>()
            .ok_or_else(|| Error::IndexTypeMismatch(index_name.to_string()))?;
        Ok(managed
            .storage()
            .get_ids(key)
            .filter_map(|id| self.data.get(&id).map(|r| (id, r)))
            .collect())
    }

    /// Range scan on an index (works for both unique and non-unique).
    pub fn index_range<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        range: impl RangeBounds<K>,
    ) -> Result<Vec<(u64, &R)>> {
        let idx = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        // Try unique first, then non-unique.
        if let Some(managed) = idx.as_any().downcast_ref::<ManagedIndex<R, K, UniqueStorage<K>>>() {
            return Ok(managed
                .storage()
                .range_ids(range)
                .filter_map(|(_, id)| self.data.get(&id).map(|r| (id, r)))
                .collect());
        }
        let managed = idx
            .as_any()
            .downcast_ref::<ManagedIndex<R, K, NonUniqueStorage<K>>>()
            .ok_or_else(|| Error::IndexTypeMismatch(index_name.to_string()))?;
        let start = range.start_bound();
        let end = range.end_bound();
        Ok(managed
            .storage()
            .range_ids(start, end)
            .filter_map(|(_, id)| self.data.get(&id).map(|r| (id, r)))
            .collect())
    }
}

impl<R> Clone for Table<R> {
    /// O(1) per index + O(1) for data tree.
    fn clone(&self) -> Self {
        let indexes = self
            .indexes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone_box()))
            .collect();
        Table { data: self.data.clone(), next_id: self.next_id, indexes }
    }
}

impl<R: Send + Sync + 'static> Default for Table<R> {
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
        assert_eq!(table.insert("first".to_string()).unwrap(), 1);
    }

    #[test]
    fn insert_returns_incrementing_ids() {
        let mut table: Table<String> = Table::new();
        assert_eq!(table.insert("a".to_string()).unwrap(), 1);
        assert_eq!(table.insert("b".to_string()).unwrap(), 2);
        assert_eq!(table.insert("c".to_string()).unwrap(), 3);
    }

    #[test]
    fn get_returns_inserted_record() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("hello".to_string()).unwrap();
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
        let id = table.insert("original".to_string()).unwrap();
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
        let id = table.insert("bye".to_string()).unwrap();
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
        table.insert("a").unwrap();
        table.insert("b").unwrap();
        table.insert("c").unwrap();
        let results: Vec<_> = table.range(1..=3).collect();
        assert_eq!(results, vec![(1, &"a"), (2, &"b"), (3, &"c")]);
    }

    #[test]
    fn range_with_partial_bounds() {
        let mut table: Table<&str> = Table::new();
        table.insert("a").unwrap();
        table.insert("b").unwrap();
        table.insert("c").unwrap();
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
        let id = table.insert("a".to_string()).unwrap();
        assert_eq!(table.len(), 1);
        table.delete(id).unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn table_clone_is_independent() {
        let mut original: Table<String> = Table::new();
        original.insert("alice".to_string()).unwrap();
        let clone = original.clone();
        original.insert("bob".to_string()).unwrap(); // mutate original after clone
        // Clone is unaffected
        assert_eq!(clone.len(), 1);
        assert_eq!(clone.get(2), None);
    }

    #[test]
    fn table_clone_preserves_next_id() {
        let mut original: Table<String> = Table::new();
        original.insert("a".to_string()).unwrap(); // id 1
        original.insert("b".to_string()).unwrap(); // id 2
        let mut clone = original.clone();
        // Next insert in clone should continue from id 3
        let id = clone.insert("c".to_string()).unwrap();
        assert_eq!(id, 3);
        // Verify no ID collision
        assert_eq!(clone.get(1), Some(&"a".to_string()));
        assert_eq!(clone.get(3), Some(&"c".to_string()));
    }

    #[derive(Debug, Clone, PartialEq)]
    struct User {
        email: String,
        age: u32,
        name: String,
    }

    #[test]
    fn define_index_idempotent_same_kind() {
        let mut table: Table<User> = Table::new();
        table.define_index("idx", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        // Redefining with same name and same kind should be Ok (idempotent)
        table.define_index("idx", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();

        // The original index should still be there and be Unique
        table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        let res = table.insert(User { email: "a@x.com".into(), age: 25, name: "B".into() });
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));
    }

    #[test]
    fn define_index_rejects_kind_mismatch() {
        let mut table: Table<User> = Table::new();
        table.define_index("idx", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        // Redefining with same name but different kind should fail
        let res = table.define_index("idx", IndexKind::NonUnique, |u: &User| u.age);
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));
    }

    #[test]
    fn query_wrong_index_type_returns_error() {
        let mut table: Table<User> = Table::new();
        table.define_index("unique", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.define_index("non_unique", IndexKind::NonUnique, |u: &User| u.age).unwrap();

        // get_unique on non-unique index
        let res = table.get_unique::<u32>("non_unique", &30);
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));

        // get_by_index on unique index
        let res = table.get_by_index::<String>("unique", &"a@x.com".to_string());
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));
    }

    #[test]
    fn define_index_backfill_failure() {
        let mut table: Table<User> = Table::new();
        table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        table.insert(User { email: "a@x.com".into(), age: 25, name: "B".into() }).unwrap();

        // Defining unique index on duplicate data should fail
        let res = table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone());
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));

        // Index should NOT be registered
        let res = table.get_unique::<String>("by_email", &"a@x.com".to_string());
        assert!(matches!(res, Err(crate::Error::IndexNotFound(_))));
    }

    #[test]
    fn table_clone_indexes_are_independent() {
        let mut table: Table<User> = Table::new();
        table.define_index("idx", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();

        let mut clone = table.clone();

        // Insert into original
        table.insert(User { email: "b@x.com".into(), age: 25, name: "B".into() }).unwrap();

        // Clone should NOT have b@x.com in its index
        assert!(clone.get_unique::<String>("idx", &"b@x.com".to_string()).unwrap().is_none());

        // Insert into clone
        clone.insert(User { email: "c@x.com".into(), age: 20, name: "C".into() }).unwrap();

        // Original should NOT have c@x.com in its index
        assert!(table.get_unique::<String>("idx", &"c@x.com".to_string()).unwrap().is_none());
    }

    #[test]
    fn index_range_type_mismatch() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();

        // Querying String index with u32 range
        let res = table.index_range::<u32>("by_email", 20..30);
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));
    }
}
