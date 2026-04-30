use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::btree::BTree;
use crate::index::{CustomIndex, CustomIndexAdapter, IndexKind, IndexMaintainer, ManagedIndex, NonUniqueStorage, UniqueStorage};
use crate::persistence::Record;
use crate::{Error, Result};

// ---------------------------------------------------------------------------
// MergeableTable — the trait object carried in Snapshot.tables and WriteTx.dirty
// ---------------------------------------------------------------------------
//
// Supertrait `Any + Send + Sync` keeps existing downcast machinery working
// via the explicit `as_any()` accessor. `boxed_clone` is an O(1) CoW clone
// used at commit to take the latest snapshot's table and layer the writer's
// edits on top. `merge_keys_from` walks the writer's write_set and upserts
// each modified record from `source` into `self`.

pub(crate) trait MergeableTable: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// O(1)-CoW clone (Arc bumps on the BTree root and index internals).
    fn boxed_clone(&self) -> Box<dyn MergeableTable>;

    /// For each key in `keys`, take the writer's record at that key from
    /// `source` and apply it to `self`:
    /// - `source` has a record → upsert into self (maintains indexes)
    /// - `source` does not have a record → delete from self (if present)
    ///
    /// OCC guarantees no concurrent committed writer touched any key in
    /// `keys`, so self's state at those keys matches source's base state
    /// and the writes never fight another committer. A unique-index
    /// violation is still possible (two writers assigning the same indexed
    /// value to different rows); that bubbles up as `Error::DuplicateKey`.
    fn merge_keys_from(
        &mut self,
        source: &dyn MergeableTable,
        keys: &BTreeSet<u64>,
    ) -> Result<()>;
}

impl<R: Record> MergeableTable for Table<R> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn boxed_clone(&self) -> Box<dyn MergeableTable> {
        Box::new(self.clone())
    }

    fn merge_keys_from(
        &mut self,
        source: &dyn MergeableTable,
        keys: &BTreeSet<u64>,
    ) -> Result<()> {
        let source = source
            .as_any()
            .downcast_ref::<Table<R>>()
            .ok_or_else(|| Error::TypeMismatch("merge source".to_string()))?;
        for &k in keys {
            match (source.data.get_arc(&k), self.data.get_arc(&k)) {
                (Some(new_arc), _) => self.upsert_arc(k, new_arc)?,
                (None, Some(_)) => {
                    // Writer deleted this key. `self` still has the prior
                    // value (OCC rules out concurrent deletion at this key).
                    let _ = self.delete(k)?;
                }
                (None, None) => {
                    // Writer inserted-then-deleted in the same tx — no-op.
                }
            }
        }
        // Ensure the merged table's next_id is at least as large as the
        // writer's, so future auto-assigned inserts don't collide with any
        // id the writer already used.
        if source.next_id > self.next_id {
            self.next_id = source.next_id;
        }
        Ok(())
    }
}

/// A compile-time table definition binding a name to a record type.
#[derive(Copy, Clone)]
pub struct TableDef<R: 'static> {
    name: &'static str,
    _phantom: PhantomData<R>,
}

impl<R: 'static> TableDef<R> {
    pub const fn new(name: &'static str) -> Self {
        Self { name, _phantom: PhantomData }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }
}

/// Trait for types that can identify a table and its record type.
pub trait TableOpener<R> {
    fn table_name(&self) -> &str;
}

impl<R> TableOpener<R> for &str {
    fn table_name(&self) -> &str {
        self
    }
}

impl<R: 'static> TableOpener<R> for TableDef<R> {
    fn table_name(&self) -> &str {
        self.name
    }
}

pub struct Table<R> {
    data: BTree<u64, R>,
    next_id: u64,
    indexes: BTreeMap<String, Box<dyn IndexMaintainer<R>>>,
}

/// Captured table state for atomic batch rollback.
struct TableSnapshot<R> {
    data: BTree<u64, R>,
    next_id: u64,
    indexes: BTreeMap<String, Box<dyn IndexMaintainer<R>>>,
}

impl<R: Record> Table<R> {
    /// Creates a new, empty table with auto-incrementing IDs starting at 1.
    pub fn new() -> Self {
        Self { data: BTree::new(), next_id: 1, indexes: BTreeMap::new() }
    }

    /// Build a table from sorted `(id, Arc<record>)` pairs and a list of
    /// pre-defined indexes. Builds the data tree via `BTree::from_sorted`,
    /// then backfills each index via `rebuild_from_sorted_data`. On any
    /// index-build failure, returns `Err`; the original table (if any) is
    /// untouched because we never mutate it.
    #[allow(dead_code)]
    pub(crate) fn from_bulk(
        sorted_rows: Vec<(u64, Arc<R>)>,
        next_id: u64,
        mut index_defs: Vec<Box<dyn IndexMaintainer<R>>>,
    ) -> Result<Self> {
        // Debug-assert ascending unique ids.
        debug_assert!(
            sorted_rows.windows(2).all(|w| w[0].0 < w[1].0),
            "from_bulk: rows must be strictly ascending by id"
        );

        let data: BTree<u64, R> = BTree::from_sorted(sorted_rows);

        let mut indexes: BTreeMap<String, Box<dyn IndexMaintainer<R>>> = BTreeMap::new();
        for mut idx in index_defs.drain(..) {
            idx.rebuild_from_sorted_data(&data)?;
            indexes.insert(idx.name().to_string(), idx);
        }

        Ok(Self { data, next_id, indexes })
    }

    /// Clone each index's *definition* (extractor, name, kind, storage type)
    /// with empty storage. Used by bulk-load to rebuild indexes from new data.
    pub(crate) fn empty_index_defs(&self) -> Vec<Box<dyn IndexMaintainer<R>>> {
        self.indexes.values().map(|i| i.empty_clone()).collect()
    }

    /// Borrow the underlying data B-tree. Used by bulk-load Delta to walk
    /// the captured base in id-order while materializing the merged rows.
    pub(crate) fn data_ref(&self) -> &BTree<u64, R> {
        &self.data
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

    /// Insert-or-replace at an explicit id, reusing an existing `Arc<R>`.
    /// Maintains secondary indexes (routing to `on_insert` or `on_update`
    /// depending on whether a prior record exists at the id). Does NOT
    /// bump `next_id`. Used at commit by the per-key merge path.
    pub(crate) fn upsert_arc(&mut self, id: u64, arc: Arc<R>) -> Result<()> {
        let prior = self.data.get_arc(&id);
        let new_ref: &R = &arc;
        // SAFETY: same invariants as `insert` — see comment there.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R>>> =
            self.indexes.values_mut().map(|v| v as *mut _).collect();

        match &prior {
            Some(old_arc) => {
                let old_ref: &R = old_arc;
                for (applied, ptr) in ptrs.iter().enumerate() {
                    let idx = unsafe { &mut **ptr };
                    if let Err(e) = idx.on_update(id, old_ref, new_ref) {
                        // Roll back previously applied index updates by
                        // reversing them (new → old). Should not fail
                        // because we are restoring valid state.
                        for prev_ptr in &ptrs[..applied] {
                            let prev_idx = unsafe { &mut **prev_ptr };
                            let _ = prev_idx.on_update(id, new_ref, old_ref);
                        }
                        return Err(e);
                    }
                }
            }
            None => {
                for (applied, ptr) in ptrs.iter().enumerate() {
                    let idx = unsafe { &mut **ptr };
                    if let Err(e) = idx.on_insert(id, new_ref) {
                        for prev_ptr in &ptrs[..applied] {
                            let prev_idx = unsafe { &mut **prev_ptr };
                            prev_idx.on_delete(id, new_ref);
                        }
                        return Err(e);
                    }
                }
            }
        }

        self.data = self.data.insert_arc(id, arc);
        Ok(())
    }

    /// Delete a record by its ID. Returns the deleted record, or an error if the ID does not exist.
    pub fn delete(&mut self, id: u64) -> Result<Arc<R>> {
        let old = self.data.get_arc(&id).ok_or(Error::KeyNotFound)?;
        // Remove from all indexes before removing from data tree.
        for idx in self.indexes.values_mut() {
            idx.on_delete(id, &old);
        }
        self.data = self.data.remove(&id)?;
        Ok(old)
    }

    // -----------------------------------------------------------------------
    // Batch mutations — deferred index updates
    // -----------------------------------------------------------------------

    /// Capture current state for atomic rollback. O(1) for data BTree and
    /// O(1) per index thanks to CoW/Arc internals.
    fn snapshot(&self) -> TableSnapshot<R> {
        TableSnapshot {
            data: self.data.clone(),
            next_id: self.next_id,
            indexes: self
                .indexes
                .iter()
                .map(|(k, v)| (k.clone(), v.clone_box()))
                .collect(),
        }
    }

    /// Restore from a previously captured snapshot.
    fn restore(&mut self, snap: TableSnapshot<R>) {
        self.data = snap.data;
        self.next_id = snap.next_id;
        self.indexes = snap.indexes;
    }

    /// Insert multiple records. Returns the auto-assigned IDs, or an error
    /// if a unique index constraint is violated. On error, the table is
    /// unchanged (atomic rollback).
    ///
    /// Index updates are deferred until all records are inserted into the
    /// data tree, then applied in one pass per index.
    pub fn insert_batch(&mut self, records: Vec<R>) -> Result<Vec<u64>> {
        if records.is_empty() {
            return Ok(vec![]);
        }
        assert!(
            self.next_id.checked_add(records.len() as u64).is_some(),
            "Table ID overflow"
        );

        let snap = self.snapshot();

        // Phase 1: Insert all records into the data BTree.
        let mut ids = Vec::with_capacity(records.len());
        for record in records {
            let id = self.next_id;
            self.data = self.data.insert(id, record);
            self.next_id += 1;
            ids.push(id);
        }

        // Phase 2: Update each index for all new records.
        // SAFETY: Same invariants as single-record `insert` — see comment there.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R>>> =
            self.indexes.values_mut().map(|v| v as *mut _).collect();
        for ptr in &ptrs {
            let idx = unsafe { &mut **ptr };
            for &id in &ids {
                let record = self.data.get(&id).unwrap();
                if let Err(e) = idx.on_insert(id, record) {
                    self.restore(snap);
                    return Err(e);
                }
            }
        }

        Ok(ids)
    }

    /// Update multiple records by ID. Returns an error if any ID does not
    /// exist or if a unique index constraint is violated. On error, the
    /// table is unchanged (atomic rollback).
    ///
    /// If the same ID appears multiple times, the last value wins.
    pub fn update_batch(&mut self, updates: Vec<(u64, R)>) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        // Deduplicate: keep only the last value for each ID.
        let mut seen = BTreeMap::new();
        for (i, (id, _)) in updates.iter().enumerate() {
            seen.insert(*id, i);
        }
        let deduped_indices: Vec<usize> = {
            let mut indices: Vec<usize> = seen.values().copied().collect();
            indices.sort_unstable();
            indices
        };

        // Phase 0: Validate all unique IDs exist and collect old records.
        let mut old_records: Vec<(u64, Arc<R>)> = Vec::with_capacity(deduped_indices.len());
        for &i in &deduped_indices {
            let id = updates[i].0;
            let old = self.data.get_arc(&id).ok_or(Error::KeyNotFound)?;
            old_records.push((id, old));
        }

        let snap = self.snapshot();

        // Phase 1: Mutate data BTree for all updates (in original order so
        // last-value-wins semantics are preserved).
        for (id, record) in updates {
            self.data = self.data.insert(id, record);
        }

        // Phase 2: Update each index for all deduplicated records.
        // SAFETY: Same invariants as single-record `insert` — see comment there.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R>>> =
            self.indexes.values_mut().map(|v| v as *mut _).collect();
        for ptr in &ptrs {
            let idx = unsafe { &mut **ptr };
            for (id, old_arc) in &old_records {
                let new_record = self.data.get(id).unwrap();
                if let Err(e) = idx.on_update(*id, old_arc.as_ref(), new_record) {
                    self.restore(snap);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Delete multiple records by ID. Returns an error if any ID does not
    /// exist. On error, the table is unchanged (atomic rollback).
    ///
    /// Duplicate IDs in the input are handled gracefully (deduplicated).
    pub fn delete_batch(&mut self, ids: &[u64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        // Deduplicate IDs.
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();

        // Phase 0: Validate all IDs exist and collect old records.
        let mut old_records: Vec<(u64, Arc<R>)> = Vec::with_capacity(ids.len());
        for &id in &ids {
            let old = self.data.get_arc(&id).ok_or(Error::KeyNotFound)?;
            old_records.push((id, old));
        }

        let snap = self.snapshot();

        // Phase 1: Remove all records from data BTree.
        for &id in &ids {
            match self.data.remove(&id) {
                Ok(new_tree) => self.data = new_tree,
                Err(e) => {
                    self.restore(snap);
                    return Err(e);
                }
            }
        }

        // Phase 2: Clean indexes (on_delete is infallible).
        for idx in self.indexes.values_mut() {
            for (id, old_arc) in &old_records {
                idx.on_delete(*id, old_arc.as_ref());
            }
        }

        Ok(())
    }

    /// Returns an iterator over records within the specified ID range.
    pub fn range<'a>(&'a self, range: impl RangeBounds<u64> + 'a) -> impl Iterator<Item = (u64, &'a R)> + 'a {
        self.data.range(range).map(|(&k, v)| (k, v))
    }

    /// Returns the next auto-increment ID (the ID that the next `insert` will assign).
    pub fn next_id(&self) -> u64 {
        self.next_id
    }

    /// Insert a record with a specific ID, bypassing auto-increment.
    /// Used during recovery to reconstruct table state from WAL/checkpoint.
    /// Returns an error if the ID already exists or if a unique index
    /// constraint is violated.
    pub fn insert_with_id(&mut self, id: u64, record: R) -> Result<u64> {
        if self.data.get(&id).is_some() {
            return Err(Error::DuplicateKey(format!("id {id}")));
        }

        // Update all indexes; rollback on failure.
        // SAFETY: Same invariants as `insert` — see comment there.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R>>> = self
            .indexes
            .values_mut()
            .map(|v| v as *mut _)
            .collect();
        for (applied, ptr) in ptrs.iter().enumerate() {
            let idx = unsafe { &mut **ptr };
            if let Err(e) = idx.on_insert(id, &record) {
                for prev_ptr in &ptrs[..applied] {
                    let prev_idx = unsafe { &mut **prev_ptr };
                    prev_idx.on_delete(id, &record);
                }
                return Err(e);
            }
        }

        self.data = self.data.insert(id, record);
        if id >= self.next_id {
            self.next_id = id + 1;
        }
        Ok(id)
    }

    /// Set the next auto-increment ID. Used during recovery to restore the
    /// counter after deserializing table state.
    pub fn set_next_id(&mut self, next_id: u64) {
        self.next_id = next_id;
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

    /// Returns true if the table contains a record with the given ID.
    pub fn contains(&self, id: u64) -> bool {
        self.data.get(&id).is_some()
    }

    /// Returns the first (lowest ID) record, or `None` if empty.
    pub fn first(&self) -> Option<(u64, &R)> {
        self.data.range(..).next().map(|(&k, v)| (k, v))
    }

    /// Returns the last (highest ID) record, or `None` if empty.
    pub fn last(&self) -> Option<(u64, &R)> {
        self.data.range(..).next_back().map(|(&k, v)| (k, v))
    }

    /// Iterate over all records in ID order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &R)> + '_ {
        self.range(..)
    }

    /// Look up multiple records by ID.
    pub fn get_many(&self, ids: &[u64]) -> Vec<Option<&R>> {
        ids.iter().map(|&id| self.data.get(&id)).collect()
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
            if existing.kind() == IndexKind::Custom || existing.kind() != kind {
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
            IndexKind::Custom => {
                return Err(Error::IndexTypeMismatch(name.to_string()));
            }
        };
        // Backfill via fast bulk-build primitive (falls back to per-row for custom indexes).
        index.rebuild_from_sorted_data(&self.data)?;
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

    // -----------------------------------------------------------------------
    // Custom index management
    // -----------------------------------------------------------------------

    /// Define a custom index. If the table already contains data, the index
    /// is backfilled via [`CustomIndex::rebuild`]. Returns an error if any
    /// index (built-in or custom) with the same name already exists.
    pub fn define_custom_index<I: CustomIndex<R>>(
        &mut self,
        name: &str,
        mut index: I,
    ) -> Result<()> {
        if self.indexes.contains_key(name) {
            return Err(Error::IndexAlreadyExists(name.to_string()));
        }
        index.rebuild(self.data.range(..).map(|(&id, r)| (id, r)))?;
        let adapter = CustomIndexAdapter::new(name.to_string(), index);
        self.indexes.insert(name.to_string(), Box::new(adapter));
        Ok(())
    }

    /// Retrieve a reference to a custom index by name, downcast to the
    /// concrete index type. Returns `IndexNotFound` if the name doesn't
    /// exist, or `IndexTypeMismatch` if the type doesn't match.
    pub fn custom_index<I: CustomIndex<R>>(&self, name: &str) -> Result<&I> {
        let idx = self
            .indexes
            .get(name)
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;
        let adapter = idx
            .as_any()
            .downcast_ref::<CustomIndexAdapter<R, I>>()
            .ok_or_else(|| Error::IndexTypeMismatch(name.to_string()))?;
        Ok(adapter.inner())
    }

    /// Resolve a slice of record IDs to `(id, &record)` pairs.
    /// IDs that don't exist in the table are silently skipped.
    pub fn resolve(&self, ids: &[u64]) -> Vec<(u64, &R)> {
        ids.iter()
            .filter_map(|&id| self.get(id).map(|r| (id, r)))
            .collect()
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

impl<R: Record> Default for Table<R> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::CustomIndex;

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
        let mut table: Table<String> = Table::new();
        table.insert("a".into()).unwrap();
        table.insert("b".into()).unwrap();
        table.insert("c".into()).unwrap();
        let results: Vec<_> = table.range(1..=3).collect();
        assert_eq!(results, vec![(1, &"a".to_string()), (2, &"b".to_string()), (3, &"c".to_string())]);
    }

    #[test]
    fn range_with_partial_bounds() {
        let mut table: Table<String> = Table::new();
        table.insert("a".into()).unwrap();
        table.insert("b".into()).unwrap();
        table.insert("c".into()).unwrap();
        let results: Vec<_> = table.range(2..).collect();
        assert_eq!(results, vec![(2, &"b".to_string()), (3, &"c".to_string())]);
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
    #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
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
    fn define_index_rejects_custom_kind() {
        let mut table: Table<User> = Table::new();
        let res = table.define_index("idx", IndexKind::Custom, |u: &User| u.email.clone());
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));
    }

    #[test]
    fn define_index_rejects_collision_with_custom_index() {
        let mut table: Table<User> = Table::new();
        table.define_custom_index("idx", SumIndex::new(|u| u.age as u64)).unwrap();
        // Trying to define a built-in index with the same name as a custom index
        let res = table.define_index("idx", IndexKind::Unique, |u: &User| u.email.clone());
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

    // -------------------------------------------------------------------
    // Batch operation tests
    // -------------------------------------------------------------------

    #[test]
    fn insert_batch_returns_sequential_ids() {
        let mut table: Table<String> = Table::new();
        table.insert("pre".to_string()).unwrap(); // id 1
        let ids = table.insert_batch(vec!["a".into(), "b".into(), "c".into()]).unwrap();
        assert_eq!(ids, vec![2, 3, 4]);
        assert_eq!(table.get(2), Some(&"a".to_string()));
        assert_eq!(table.get(3), Some(&"b".to_string()));
        assert_eq!(table.get(4), Some(&"c".to_string()));
        assert_eq!(table.len(), 4);
    }

    #[test]
    fn insert_batch_empty_is_noop() {
        let mut table: Table<String> = Table::new();
        table.insert("existing".to_string()).unwrap();
        let ids = table.insert_batch(vec![]).unwrap();
        assert!(ids.is_empty());
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn insert_batch_unique_constraint_within_batch() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();

        let res = table.insert_batch(vec![
            User { email: "dup@x.com".into(), age: 30, name: "A".into() },
            User { email: "dup@x.com".into(), age: 25, name: "B".into() },
        ]);
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));
        // Table should be unchanged
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn insert_batch_unique_constraint_against_existing() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.insert(User { email: "taken@x.com".into(), age: 30, name: "A".into() }).unwrap();

        let res = table.insert_batch(vec![
            User { email: "new@x.com".into(), age: 25, name: "B".into() },
            User { email: "taken@x.com".into(), age: 20, name: "C".into() },
        ]);
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));
        // Table should still have only the original record
        assert_eq!(table.len(), 1);
        assert!(table.get_unique::<String>("by_email", &"new@x.com".to_string()).unwrap().is_none());
    }

    #[test]
    fn insert_batch_updates_all_indexes() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.define_index("by_age", IndexKind::NonUnique, |u: &User| u.age).unwrap();

        let ids = table.insert_batch(vec![
            User { email: "a@x.com".into(), age: 30, name: "A".into() },
            User { email: "b@x.com".into(), age: 30, name: "B".into() },
        ]).unwrap();

        assert_eq!(
            table.get_unique::<String>("by_email", &"a@x.com".to_string()).unwrap().map(|(id, _)| id),
            Some(ids[0])
        );
        assert_eq!(table.get_by_index::<u32>("by_age", &30).unwrap().len(), 2);
    }

    #[test]
    fn update_batch_modifies_records() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        let id1 = table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        let id2 = table.insert(User { email: "b@x.com".into(), age: 25, name: "B".into() }).unwrap();

        table.update_batch(vec![
            (id1, User { email: "a_new@x.com".into(), age: 31, name: "A".into() }),
            (id2, User { email: "b_new@x.com".into(), age: 26, name: "B".into() }),
        ]).unwrap();

        assert_eq!(table.get(id1).unwrap().email, "a_new@x.com");
        assert_eq!(table.get(id2).unwrap().email, "b_new@x.com");
        // Old index entries gone, new ones present
        assert!(table.get_unique::<String>("by_email", &"a@x.com".to_string()).unwrap().is_none());
        assert!(table.get_unique::<String>("by_email", &"a_new@x.com".to_string()).unwrap().is_some());
    }

    #[test]
    fn update_batch_missing_id_fails_fast() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("original".to_string()).unwrap();

        let res = table.update_batch(vec![
            (id, "updated".to_string()),
            (999, "missing".to_string()),
        ]);
        assert!(matches!(res, Err(crate::Error::KeyNotFound)));
        // Original should be unchanged
        assert_eq!(table.get(id), Some(&"original".to_string()));
    }

    #[test]
    fn update_batch_unique_constraint_rolls_back() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        let id1 = table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        let id2 = table.insert(User { email: "b@x.com".into(), age: 25, name: "B".into() }).unwrap();

        // Try to update both to the same email
        let res = table.update_batch(vec![
            (id1, User { email: "same@x.com".into(), age: 30, name: "A".into() }),
            (id2, User { email: "same@x.com".into(), age: 25, name: "B".into() }),
        ]);
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));
        // Both records and indexes should be unchanged
        assert_eq!(table.get(id1).unwrap().email, "a@x.com");
        assert_eq!(table.get(id2).unwrap().email, "b@x.com");
        assert!(table.get_unique::<String>("by_email", &"a@x.com".to_string()).unwrap().is_some());
    }

    #[test]
    fn delete_batch_removes_records_and_indexes() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        let id1 = table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        let id2 = table.insert(User { email: "b@x.com".into(), age: 25, name: "B".into() }).unwrap();
        let id3 = table.insert(User { email: "c@x.com".into(), age: 20, name: "C".into() }).unwrap();

        table.delete_batch(&[id1, id3]).unwrap();

        assert_eq!(table.get(id1), None);
        assert_eq!(table.get(id3), None);
        assert_eq!(table.get(id2).unwrap().email, "b@x.com");
        assert_eq!(table.len(), 1);
        assert!(table.get_unique::<String>("by_email", &"a@x.com".to_string()).unwrap().is_none());
        assert!(table.get_unique::<String>("by_email", &"c@x.com".to_string()).unwrap().is_none());
        assert!(table.get_unique::<String>("by_email", &"b@x.com".to_string()).unwrap().is_some());
    }

    #[test]
    fn delete_batch_missing_id_fails_fast() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("keep".to_string()).unwrap();

        let res = table.delete_batch(&[id, 999]);
        assert!(matches!(res, Err(crate::Error::KeyNotFound)));
        // Original should be unchanged
        assert_eq!(table.get(id), Some(&"keep".to_string()));
    }

    #[test]
    fn delete_batch_duplicate_ids() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("hello".to_string()).unwrap();

        table.delete_batch(&[id, id]).unwrap();
        assert_eq!(table.get(id), None);
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn update_batch_empty_is_noop() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("original".to_string()).unwrap();
        table.update_batch(vec![]).unwrap();
        assert_eq!(table.get(id), Some(&"original".to_string()));
    }

    #[test]
    fn delete_batch_empty_is_noop() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("original".to_string()).unwrap();
        table.delete_batch(&[]).unwrap();
        assert_eq!(table.get(id), Some(&"original".to_string()));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn update_batch_non_unique_index() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_age", IndexKind::NonUnique, |u: &User| u.age).unwrap();
        let id1 = table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        let id2 = table.insert(User { email: "b@x.com".into(), age: 30, name: "B".into() }).unwrap();

        table.update_batch(vec![
            (id1, User { email: "a@x.com".into(), age: 40, name: "A".into() }),
            (id2, User { email: "b@x.com".into(), age: 50, name: "B".into() }),
        ]).unwrap();

        assert!(table.get_by_index::<u32>("by_age", &30).unwrap().is_empty());
        assert_eq!(table.get_by_index::<u32>("by_age", &40).unwrap().len(), 1);
        assert_eq!(table.get_by_index::<u32>("by_age", &50).unwrap().len(), 1);
    }

    #[test]
    fn update_batch_unique_constraint_against_existing() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        let id1 = table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        let _id2 = table.insert(User { email: "b@x.com".into(), age: 25, name: "B".into() }).unwrap();

        // Update id1's email to collide with the untouched id2
        let res = table.update_batch(vec![
            (id1, User { email: "b@x.com".into(), age: 30, name: "A".into() }),
        ]);
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));
        // Original should be unchanged
        assert_eq!(table.get(id1).unwrap().email, "a@x.com");
    }

    #[test]
    fn update_batch_duplicate_ids_last_wins() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("original".to_string()).unwrap();

        table.update_batch(vec![
            (id, "first".to_string()),
            (id, "second".to_string()),
        ]).unwrap();
        assert_eq!(table.get(id), Some(&"second".to_string()));
    }

    #[test]
    fn insert_batch_rollback_restores_next_id() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.insert(User { email: "existing@x.com".into(), age: 30, name: "A".into() }).unwrap();
        // next_id is now 2

        let res = table.insert_batch(vec![
            User { email: "new@x.com".into(), age: 25, name: "B".into() },
            User { email: "existing@x.com".into(), age: 20, name: "C".into() }, // conflict
        ]);
        assert!(res.is_err());

        // After rollback, next insert should get id 2, not 4
        let id = table.insert(User { email: "ok@x.com".into(), age: 22, name: "D".into() }).unwrap();
        assert_eq!(id, 2);
    }

    #[test]
    fn insert_batch_multi_index_rollback() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone()).unwrap();

        table.insert(User { email: "a@x.com".into(), age: 30, name: "Alice".into() }).unwrap();

        // Second record conflicts on name (not email) — both indexes must roll back
        let res = table.insert_batch(vec![
            User { email: "b@x.com".into(), age: 25, name: "Bob".into() },
            User { email: "c@x.com".into(), age: 20, name: "Alice".into() }, // name conflict
        ]);
        assert!(res.is_err());
        assert_eq!(table.len(), 1);
        // The first record's entries in BOTH indexes should be rolled back
        assert!(table.get_unique::<String>("by_email", &"b@x.com".to_string()).unwrap().is_none());
        assert!(table.get_unique::<String>("by_name", &"Bob".to_string()).unwrap().is_none());
    }

    #[test]
    fn update_batch_multi_index_rollback() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone()).unwrap();

        let id1 = table.insert(User { email: "a@x.com".into(), age: 30, name: "Alice".into() }).unwrap();
        let id2 = table.insert(User { email: "b@x.com".into(), age: 25, name: "Bob".into() }).unwrap();

        // Update both; second conflicts on name with first's NEW name
        let res = table.update_batch(vec![
            (id1, User { email: "a_new@x.com".into(), age: 30, name: "Charlie".into() }),
            (id2, User { email: "b_new@x.com".into(), age: 25, name: "Charlie".into() }),
        ]);
        assert!(res.is_err());
        // Both records and all indexes should be unchanged
        assert_eq!(table.get(id1).unwrap().email, "a@x.com");
        assert_eq!(table.get(id1).unwrap().name, "Alice");
        assert!(table.get_unique::<String>("by_email", &"a@x.com".to_string()).unwrap().is_some());
        assert!(table.get_unique::<String>("by_name", &"Alice".to_string()).unwrap().is_some());
    }

    #[test]
    fn table_usable_after_failed_insert_batch() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();

        // Fail a batch
        let res = table.insert_batch(vec![
            User { email: "dup@x.com".into(), age: 30, name: "A".into() },
            User { email: "dup@x.com".into(), age: 25, name: "B".into() },
        ]);
        assert!(res.is_err());

        // Table should be fully functional
        let id = table.insert(User { email: "ok@x.com".into(), age: 20, name: "C".into() }).unwrap();
        assert_eq!(table.get(id).unwrap().email, "ok@x.com");
        table.update(id, User { email: "ok2@x.com".into(), age: 21, name: "C".into() }).unwrap();
        assert_eq!(table.get(id).unwrap().email, "ok2@x.com");
        table.delete(id).unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn table_usable_after_failed_update_batch() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        let id1 = table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        let id2 = table.insert(User { email: "b@x.com".into(), age: 25, name: "B".into() }).unwrap();

        // Fail a batch update
        let res = table.update_batch(vec![
            (id1, User { email: "same@x.com".into(), age: 30, name: "A".into() }),
            (id2, User { email: "same@x.com".into(), age: 25, name: "B".into() }),
        ]);
        assert!(res.is_err());

        // Table should be fully functional — can do single-record ops
        table.update(id1, User { email: "a_new@x.com".into(), age: 31, name: "A".into() }).unwrap();
        assert_eq!(table.get(id1).unwrap().email, "a_new@x.com");
        assert!(table.get_unique::<String>("by_email", &"a_new@x.com".to_string()).unwrap().is_some());
    }

    #[test]
    fn delete_batch_non_unique_index() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_age", IndexKind::NonUnique, |u: &User| u.age).unwrap();
        let id1 = table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        let id2 = table.insert(User { email: "b@x.com".into(), age: 30, name: "B".into() }).unwrap();
        let _id3 = table.insert(User { email: "c@x.com".into(), age: 25, name: "C".into() }).unwrap();

        table.delete_batch(&[id1, id2]).unwrap();

        assert!(table.get_by_index::<u32>("by_age", &30).unwrap().is_empty());
        assert_eq!(table.get_by_index::<u32>("by_age", &25).unwrap().len(), 1);
    }

    // -------------------------------------------------------------------
    // Coverage: get_by_key, index_range, single-op index rollback
    // -------------------------------------------------------------------

    #[test]
    fn get_by_key_on_unique_index() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();

        let results = table.get_by_key::<String>("by_email", &"a@x.com".to_string()).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.email, "a@x.com");

        let results = table.get_by_key::<String>("by_email", &"missing@x.com".to_string()).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn get_by_key_on_non_unique_index() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_age", IndexKind::NonUnique, |u: &User| u.age).unwrap();
        table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        table.insert(User { email: "b@x.com".into(), age: 30, name: "B".into() }).unwrap();
        table.insert(User { email: "c@x.com".into(), age: 25, name: "C".into() }).unwrap();

        let results = table.get_by_key::<u32>("by_age", &30).unwrap();
        assert_eq!(results.len(), 2);

        let results = table.get_by_key::<u32>("by_age", &99).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn get_by_key_nonexistent_index() {
        let table: Table<User> = Table::new();
        let res = table.get_by_key::<String>("nope", &"x".to_string());
        assert!(matches!(res, Err(crate::Error::IndexNotFound(_))));
    }

    #[test]
    fn get_by_key_wrong_key_type() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        // Query String index with u32 key
        let res = table.get_by_key::<u32>("by_email", &42);
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));
    }

    #[test]
    fn index_range_unique() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_age", IndexKind::Unique, |u: &User| u.age).unwrap();
        table.insert(User { email: "a@x.com".into(), age: 20, name: "A".into() }).unwrap();
        table.insert(User { email: "b@x.com".into(), age: 30, name: "B".into() }).unwrap();
        table.insert(User { email: "c@x.com".into(), age: 40, name: "C".into() }).unwrap();

        let results = table.index_range::<u32>("by_age", 25..=35).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.age, 30);

        // Full range
        let results = table.index_range::<u32>("by_age", ..).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn index_range_non_unique() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_age", IndexKind::NonUnique, |u: &User| u.age).unwrap();
        table.insert(User { email: "a@x.com".into(), age: 20, name: "A".into() }).unwrap();
        table.insert(User { email: "b@x.com".into(), age: 30, name: "B".into() }).unwrap();
        table.insert(User { email: "c@x.com".into(), age: 30, name: "C".into() }).unwrap();
        table.insert(User { email: "d@x.com".into(), age: 40, name: "D".into() }).unwrap();

        let results = table.index_range::<u32>("by_age", 25..=35).unwrap();
        assert_eq!(results.len(), 2);

        let results = table.index_range::<u32>("by_age", ..30).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.age, 20);
    }

    #[test]
    fn index_range_nonexistent_index() {
        let table: Table<User> = Table::new();
        let res = table.index_range::<u32>("nope", ..);
        assert!(matches!(res, Err(crate::Error::IndexNotFound(_))));
    }

    #[test]
    fn single_insert_multi_index_rollback() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone()).unwrap();

        table.insert(User { email: "a@x.com".into(), age: 30, name: "Alice".into() }).unwrap();

        // Conflict on one index — both indexes should roll back the failed insert
        let res = table.insert(User { email: "b@x.com".into(), age: 25, name: "Alice".into() });
        assert!(res.is_err());
        assert_eq!(table.len(), 1);
        // The non-conflicting index should NOT contain the failed record's email
        assert!(table.get_unique::<String>("by_email", &"b@x.com".to_string()).unwrap().is_none());
    }

    #[test]
    fn single_update_multi_index_rollback() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone()).unwrap();

        let id1 = table.insert(User { email: "a@x.com".into(), age: 30, name: "Alice".into() }).unwrap();
        let _id2 = table.insert(User { email: "b@x.com".into(), age: 25, name: "Bob".into() }).unwrap();

        // Update id1 to collide with id2 on name — should roll back email index change too
        let res = table.update(id1, User { email: "a_new@x.com".into(), age: 30, name: "Bob".into() });
        assert!(res.is_err());
        // Email index should still have old value, not new
        assert!(table.get_unique::<String>("by_email", &"a@x.com".to_string()).unwrap().is_some());
        assert!(table.get_unique::<String>("by_email", &"a_new@x.com".to_string()).unwrap().is_none());
    }

    #[test]
    fn single_delete_removes_index_entries() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.define_index("by_age", IndexKind::NonUnique, |u: &User| u.age).unwrap();

        let id = table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        table.delete(id).unwrap();

        assert!(table.get_unique::<String>("by_email", &"a@x.com".to_string()).unwrap().is_none());
        assert!(table.get_by_index::<u32>("by_age", &30).unwrap().is_empty());
    }

    #[test]
    fn insert_batch_large() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        table.define_index("by_age", IndexKind::NonUnique, |u: &User| u.age).unwrap();

        let records: Vec<User> = (0..500)
            .map(|i| User {
                email: format!("user{}@x.com", i),
                age: (i % 50) as u32,
                name: format!("User{}", i),
            })
            .collect();

        let ids = table.insert_batch(records).unwrap();
        assert_eq!(ids.len(), 500);
        assert_eq!(table.len(), 500);

        // Spot-check indexes
        assert!(table.get_unique::<String>("by_email", &"user0@x.com".to_string()).unwrap().is_some());
        assert!(table.get_unique::<String>("by_email", &"user499@x.com".to_string()).unwrap().is_some());
        // age 0 should have 10 records (0, 50, 100, ..., 450)
        assert_eq!(table.get_by_index::<u32>("by_age", &0).unwrap().len(), 10);
    }

    // -------------------------------------------------------------------
    // Convenience method tests
    // -------------------------------------------------------------------

    #[test]
    fn delete_returns_old_record() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("hello".to_string()).unwrap();
        let old = table.delete(id).unwrap();
        assert_eq!(*old, "hello".to_string());
    }

    #[test]
    fn contains_true_for_existing_id() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("x".to_string()).unwrap();
        assert!(table.contains(id));
    }

    #[test]
    fn contains_false_for_absent_id() {
        let table: Table<String> = Table::new();
        assert!(!table.contains(99));
    }

    #[test]
    fn first_returns_min_id_record() {
        let mut table: Table<String> = Table::new();
        table.insert("a".to_string()).unwrap();
        table.insert("b".to_string()).unwrap();
        let (id, val) = table.first().unwrap();
        assert_eq!(id, 1);
        assert_eq!(val, &"a".to_string());
    }

    #[test]
    fn first_on_empty_returns_none() {
        let table: Table<String> = Table::new();
        assert!(table.first().is_none());
    }

    #[test]
    fn last_returns_max_id_record() {
        let mut table: Table<String> = Table::new();
        table.insert("a".to_string()).unwrap();
        table.insert("b".to_string()).unwrap();
        let (id, val) = table.last().unwrap();
        assert_eq!(id, 2);
        assert_eq!(val, &"b".to_string());
    }

    #[test]
    fn last_on_empty_returns_none() {
        let table: Table<String> = Table::new();
        assert!(table.last().is_none());
    }

    #[test]
    fn iter_yields_all_in_order() {
        let mut table: Table<String> = Table::new();
        table.insert("a".into()).unwrap();
        table.insert("b".into()).unwrap();
        table.insert("c".into()).unwrap();
        let results: Vec<_> = table.iter().collect();
        assert_eq!(results, vec![(1, &"a".to_string()), (2, &"b".to_string()), (3, &"c".to_string())]);
    }

    #[test]
    fn get_many_returns_matching_records() {
        let mut table: Table<String> = Table::new();
        table.insert("a".to_string()).unwrap();
        table.insert("b".to_string()).unwrap();
        table.insert("c".to_string()).unwrap();
        let results = table.get_many(&[1, 3, 99]);
        assert_eq!(results[0], Some(&"a".to_string()));
        assert_eq!(results[1], Some(&"c".to_string()));
        assert_eq!(results[2], None);
    }

    /// Indexes are maintained in deterministic (alphabetical) order.
    /// This ensures identical error messages across replicas when a
    /// constraint violation occurs.
    #[test]
    fn index_maintenance_order_is_deterministic() {
        let mut table: Table<(String, String)> = Table::new();
        table
            .define_index("zzz_idx", IndexKind::Unique, |r: &(String, String)| {
                r.0.clone()
            })
            .unwrap();
        table
            .define_index("aaa_idx", IndexKind::Unique, |r: &(String, String)| {
                r.1.clone()
            })
            .unwrap();
        table.insert(("x".into(), "y".into())).unwrap();

        // This violates both indexes. With BTreeMap, "aaa_idx" is checked first.
        let err = table.insert(("x".into(), "y".into())).unwrap_err();
        assert!(
            matches!(err, Error::DuplicateKey(ref name) if name == "aaa_idx"),
            "expected DuplicateKey(aaa_idx), got: {err:?}"
        );
    }

    #[test]
    fn default_creates_empty_table() {
        let table: Table<String> = Table::default();
        assert_eq!(table.len(), 0);
        assert_eq!(table.next_id(), 1);
    }

    #[test]
    fn insert_with_id_duplicate_returns_error() {
        let mut table = Table::<String>::new();
        table.insert_with_id(1, "first".into()).unwrap();
        let err = table.insert_with_id(1, "duplicate".into()).unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(_)));
        // Original record is preserved.
        assert_eq!(table.get(1).unwrap(), "first");
    }

    #[test]
    fn insert_with_id_unique_index_violation_rolls_back() {
        let mut table = Table::<(String, u32)>::new();
        table
            .define_index("name", IndexKind::Unique, |r: &(String, u32)| r.0.clone())
            .unwrap();

        table.insert_with_id(1, ("alice".into(), 30)).unwrap();

        // Insert with a different ID but duplicate index key.
        let err = table.insert_with_id(2, ("alice".into(), 25)).unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(_)));

        // ID 2 should not exist; original record preserved.
        assert_eq!(table.len(), 1);
        assert!(table.get(2).is_none());
        assert_eq!(table.get(1).unwrap().0, "alice");

        // A new unique key should still insert successfully (index wasn't corrupted).
        table.insert_with_id(3, ("bob".into(), 40)).unwrap();
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn insert_with_id_multi_index_partial_rollback() {
        // Two unique indexes. The second one fails, so the first must be rolled back.
        let mut table = Table::<(String, String)>::new();
        // BTreeMap iterates alphabetically, so "idx_a" is checked before "idx_b".
        table
            .define_index("idx_a", IndexKind::Unique, |r: &(String, String)| r.0.clone())
            .unwrap();
        table
            .define_index("idx_b", IndexKind::Unique, |r: &(String, String)| r.1.clone())
            .unwrap();

        table.insert_with_id(1, ("alice".into(), "x".into())).unwrap();

        // Second insert: unique key for idx_a ("bob") but duplicate for idx_b ("x").
        // idx_a succeeds first, then idx_b fails → idx_a must be rolled back.
        let err = table.insert_with_id(2, ("bob".into(), "x".into())).unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(_)));
        assert_eq!(table.len(), 1);
        assert!(table.get(2).is_none());

        // "bob" should NOT be in idx_a after rollback.
        table.insert_with_id(3, ("bob".into(), "y".into())).unwrap();
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn table_def_const_new() {
        const DEF: TableDef<String> = TableDef::new("my_table");
        assert_eq!(DEF.name(), "my_table");
    }

    /// update_batch with duplicate IDs and a unique index should succeed
    /// (last value wins), not false-fail with DuplicateKey.
    #[test]
    fn update_batch_duplicate_ids_with_unique_index() {
        let mut table: Table<User> = Table::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        let id = table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();

        // Same ID appears twice — last value should win.
        table.update_batch(vec![
            (id, User { email: "b@x.com".into(), age: 31, name: "A".into() }),
            (id, User { email: "c@x.com".into(), age: 32, name: "A".into() }),
        ]).unwrap();

        assert_eq!(table.get(id).unwrap().email, "c@x.com");
        assert_eq!(table.get(id).unwrap().age, 32);
        // Index should reflect the final value only.
        assert!(table.get_unique::<String>("by_email", &"a@x.com".to_string()).unwrap().is_none());
        assert!(table.get_unique::<String>("by_email", &"b@x.com".to_string()).unwrap().is_none());
        assert!(table.get_unique::<String>("by_email", &"c@x.com".to_string()).unwrap().is_some());
    }

    // -------------------------------------------------------------------
    // Custom index tests
    // -------------------------------------------------------------------

    #[derive(Clone)]
    struct SumIndex {
        total: u64,
        field_extractor: Arc<dyn Fn(&User) -> u64 + Send + Sync>,
    }

    impl SumIndex {
        fn new(extractor: impl Fn(&User) -> u64 + Send + Sync + 'static) -> Self {
            Self { total: 0, field_extractor: Arc::new(extractor) }
        }

        fn total(&self) -> u64 {
            self.total
        }
    }

    impl CustomIndex<User> for SumIndex {
        fn on_insert(&mut self, _id: u64, record: &User) -> Result<()> {
            self.total += (self.field_extractor)(record);
            Ok(())
        }

        fn on_update(&mut self, _id: u64, old: &User, new: &User) -> Result<()> {
            self.total -= (self.field_extractor)(old);
            self.total += (self.field_extractor)(new);
            Ok(())
        }

        fn on_delete(&mut self, _id: u64, record: &User) {
            self.total -= (self.field_extractor)(record);
        }
    }

    #[test]
    fn define_custom_index_and_query() {
        let mut table = Table::<User>::new();
        table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();

        table.insert(User { email: "a@x.com".to_string(), age: 30, name: "Alice".to_string() }).unwrap();
        table.insert(User { email: "b@x.com".to_string(), age: 20, name: "Bob".to_string() }).unwrap();

        let idx = table.custom_index::<SumIndex>("age_sum").unwrap();
        assert_eq!(idx.total(), 50);
    }

    #[test]
    fn define_custom_index_backfills_existing_data() {
        let mut table = Table::<User>::new();
        table.insert(User { email: "a@x.com".to_string(), age: 10, name: "A".to_string() }).unwrap();
        table.insert(User { email: "b@x.com".to_string(), age: 20, name: "B".to_string() }).unwrap();

        table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();

        let idx = table.custom_index::<SumIndex>("age_sum").unwrap();
        assert_eq!(idx.total(), 30);
    }

    #[test]
    fn define_custom_index_rejects_duplicate_name() {
        let mut table = Table::<User>::new();
        table.define_custom_index("idx", SumIndex::new(|u| u.age as u64)).unwrap();
        let res = table.define_custom_index("idx", SumIndex::new(|u| u.age as u64));
        assert!(matches!(res, Err(Error::IndexAlreadyExists(_))));
    }

    #[test]
    fn define_custom_index_rejects_name_collision_with_builtin() {
        let mut table = Table::<User>::new();
        table.define_index("idx", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        let res = table.define_custom_index("idx", SumIndex::new(|u| u.age as u64));
        assert!(matches!(res, Err(Error::IndexAlreadyExists(_))));
    }

    #[test]
    fn custom_index_not_found() {
        let table = Table::<User>::new();
        let res = table.custom_index::<SumIndex>("nope");
        assert!(matches!(res, Err(Error::IndexNotFound(_))));
    }

    #[test]
    fn custom_index_type_mismatch() {
        let mut table = Table::<User>::new();
        table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
        let res = table.custom_index::<SumIndex>("by_email");
        assert!(matches!(res, Err(Error::IndexTypeMismatch(_))));
    }

    #[test]
    fn resolve_returns_matching_records() {
        let mut table = Table::<User>::new();
        let id1 = table.insert(User { email: "a@x.com".to_string(), age: 30, name: "A".to_string() }).unwrap();
        let id2 = table.insert(User { email: "b@x.com".to_string(), age: 20, name: "B".to_string() }).unwrap();

        let results = table.resolve(&[id1, id2, 999]);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, id1);
        assert_eq!(results[1].0, id2);
    }

    #[test]
    fn resolve_empty_ids() {
        let table = Table::<User>::new();
        let results = table.resolve(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn custom_index_tracks_updates() {
        let mut table = Table::<User>::new();
        table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();

        let id = table.insert(User { email: "a@x.com".to_string(), age: 30, name: "A".to_string() }).unwrap();
        assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 30);

        table.update(id, User { email: "a@x.com".to_string(), age: 40, name: "A".to_string() }).unwrap();
        assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 40);

        table.delete(id).unwrap();
        assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 0);
    }

    #[test]
    fn custom_index_works_with_batch_insert() {
        let mut table = Table::<User>::new();
        table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();

        let records = vec![
            User { email: "a@x.com".to_string(), age: 10, name: "A".to_string() },
            User { email: "b@x.com".to_string(), age: 20, name: "B".to_string() },
            User { email: "c@x.com".to_string(), age: 30, name: "C".to_string() },
        ];
        table.insert_batch(records).unwrap();

        assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 60);
    }

    #[test]
    fn custom_index_works_with_batch_delete() {
        let mut table = Table::<User>::new();
        table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();

        let ids = table.insert_batch(vec![
            User { email: "a@x.com".to_string(), age: 10, name: "A".to_string() },
            User { email: "b@x.com".to_string(), age: 20, name: "B".to_string() },
            User { email: "c@x.com".to_string(), age: 30, name: "C".to_string() },
        ]).unwrap();

        table.delete_batch(&[ids[0], ids[2]]).unwrap();
        assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 20);
    }

    #[test]
    fn custom_index_clone_is_independent() {
        let mut table = Table::<User>::new();
        table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();
        table.insert(User { email: "a@x.com".to_string(), age: 30, name: "A".to_string() }).unwrap();

        let clone = table.clone();

        table.insert(User { email: "b@x.com".to_string(), age: 20, name: "B".to_string() }).unwrap();

        assert_eq!(clone.custom_index::<SumIndex>("age_sum").unwrap().total(), 30);
        assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 50);
    }

    /// A custom index that rejects inserts when total would exceed a limit.
    #[derive(Clone)]
    struct CappedSumIndex {
        total: u64,
        cap: u64,
    }

    impl CappedSumIndex {
        fn new(cap: u64) -> Self {
            Self { total: 0, cap }
        }
    }

    impl CustomIndex<User> for CappedSumIndex {
        fn on_insert(&mut self, _id: u64, record: &User) -> Result<()> {
            let new_total = self.total + record.age as u64;
            if new_total > self.cap {
                return Err(Error::DuplicateKey("cap exceeded".to_string()));
            }
            self.total = new_total;
            Ok(())
        }

        fn on_update(&mut self, _id: u64, old: &User, new: &User) -> Result<()> {
            let new_total = self.total - old.age as u64 + new.age as u64;
            if new_total > self.cap {
                return Err(Error::DuplicateKey("cap exceeded".to_string()));
            }
            self.total = new_total;
            Ok(())
        }

        fn on_delete(&mut self, _id: u64, record: &User) {
            self.total -= record.age as u64;
        }
    }

    #[test]
    fn custom_index_veto_rejects_insert() {
        let mut table = Table::<User>::new();
        table.define_custom_index("capped", CappedSumIndex::new(50)).unwrap();

        table.insert(User { email: "a@x.com".to_string(), age: 30, name: "A".to_string() }).unwrap();
        table.insert(User { email: "b@x.com".to_string(), age: 15, name: "B".to_string() }).unwrap();

        // This would push total to 55, exceeding cap of 50
        let res = table.insert(User { email: "c@x.com".to_string(), age: 10, name: "C".to_string() });
        assert!(res.is_err());

        // Total should still be 45 (rollback)
        assert_eq!(table.custom_index::<CappedSumIndex>("capped").unwrap().total, 45);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn custom_index_veto_rejects_update() {
        let mut table = Table::<User>::new();
        table.define_custom_index("capped", CappedSumIndex::new(50)).unwrap();

        let id = table.insert(User { email: "a@x.com".to_string(), age: 30, name: "A".to_string() }).unwrap();
        table.insert(User { email: "b@x.com".to_string(), age: 15, name: "B".to_string() }).unwrap();

        // Update age 30 -> 40 would push total to 55
        let res = table.update(id, User { email: "a@x.com".to_string(), age: 40, name: "A".to_string() });
        assert!(res.is_err());

        // Total should still be 45
        assert_eq!(table.custom_index::<CappedSumIndex>("capped").unwrap().total, 45);
    }

    #[test]
    fn from_bulk_data_only_no_indexes() {
        let rows: Vec<(u64, std::sync::Arc<String>)> =
            (1u64..=5).map(|i| (i, std::sync::Arc::new(format!("v{i}")))).collect();
        let t = Table::<String>::from_bulk(rows, 6, vec![]).unwrap();
        assert_eq!(t.len(), 5);
        assert_eq!(t.get(1).map(String::as_str), Some("v1"));
        assert_eq!(t.get(5).map(String::as_str), Some("v5"));
        // Inserting after bulk should continue from next_id.
        let mut t2 = t;
        let id = t2.insert("v6".to_string()).unwrap();
        assert_eq!(id, 6);
    }

    #[test]
    fn from_bulk_with_indexes() {
        use std::sync::Arc;
        #[derive(Clone)]
        #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
        struct U {
            email: String,
            age: u32,
        }

        // Build empty index defs to hand to from_bulk.
        let unique_idx: Box<dyn IndexMaintainer<U>> = Box::new(
            ManagedIndex::<U, String, UniqueStorage<String>>::new(
                "by_email".into(),
                IndexKind::Unique,
                Arc::new(|u: &U| u.email.clone()),
                UniqueStorage::new(),
            ),
        );
        let nonunique_idx: Box<dyn IndexMaintainer<U>> = Box::new(
            ManagedIndex::<U, u32, NonUniqueStorage<u32>>::new(
                "by_age".into(),
                IndexKind::NonUnique,
                Arc::new(|u: &U| u.age),
                NonUniqueStorage::new(),
            ),
        );

        let rows: Vec<(u64, Arc<U>)> = (1u64..=5)
            .map(|i| {
                (
                    i,
                    Arc::new(U {
                        email: format!("u{i}@x"),
                        age: 10 * (i as u32 % 3),
                    }),
                )
            })
            .collect();

        let t = Table::<U>::from_bulk(rows, 6, vec![unique_idx, nonunique_idx]).unwrap();
        assert_eq!(t.len(), 5);
        let (id, _) = t.get_unique("by_email", &"u3@x".to_string()).unwrap().unwrap();
        assert_eq!(id, 3);
        let ids: Vec<u64> = t
            .get_by_index("by_age", &10u32)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(
            ids.len()
                + t.get_by_index("by_age", &20u32).unwrap().len()
                + t.get_by_index("by_age", &0u32).unwrap().len(),
            5
        );
    }

    #[test]
    fn from_bulk_unique_collision_errors() {
        use std::sync::Arc;
        #[derive(Clone)]
        #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
        struct U {
            email: String,
        }

        let unique_idx: Box<dyn IndexMaintainer<U>> = Box::new(
            ManagedIndex::<U, String, UniqueStorage<String>>::new(
                "by_email".into(),
                IndexKind::Unique,
                Arc::new(|u: &U| u.email.clone()),
                UniqueStorage::new(),
            ),
        );
        let rows: Vec<(u64, Arc<U>)> = vec![
            (1, Arc::new(U { email: "dup@x".into() })),
            (2, Arc::new(U { email: "dup@x".into() })),
        ];
        let res = Table::<U>::from_bulk(rows, 3, vec![unique_idx]);
        assert!(matches!(res, Err(Error::DuplicateKey(_))));
    }
}
