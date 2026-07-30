// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::any::Any;
use std::sync::Arc;

use crate::btree::BTree;
use crate::persistence::Record;
use crate::primary_key::PrimaryKey;
use crate::{Error, Result};

/// Whether an index enforces uniqueness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    /// Rejects a second record with the same key; backed by `BTree<IK, K>`.
    Unique,
    /// Allows multiple records to share a key; backed by
    /// `BTree<(IK, K), ()>` (row key folded into the key for multi-value storage).
    NonUnique,
    /// User-supplied index implementing [`CustomIndex`] (e.g. full-text);
    /// storage shape is opaque to the generic maintainer.
    Custom,
}

// ---------------------------------------------------------------------------
// IndexMaintainer trait — object-safe interface for type-erased indexes
// ---------------------------------------------------------------------------

pub(crate) trait IndexMaintainer<R, K: PrimaryKey>: Send + Sync {
    /// Called when a record is inserted into the table.
    fn on_insert(&mut self, key: K, record: &R) -> Result<()>;
    /// Called when a record in the table is updated.
    fn on_update(&mut self, key: K, old: &R, new: &R) -> Result<()>;
    /// Called when a record is deleted from the table.
    fn on_delete(&mut self, key: K, record: &R);
    /// Returns the kind of this index (Unique or NonUnique).
    fn kind(&self) -> IndexKind;
    /// Returns the name of this index.
    fn name(&self) -> &str;
    /// Returns a boxed clone of this index maintainer.
    fn clone_box(&self) -> Box<dyn IndexMaintainer<R, K>>;
    /// Clone the index *definition* (extractor, name, kind, storage shape)
    /// with empty storage. Used by bulk-load to rebuild the index against
    /// freshly-loaded data via [`rebuild_from_sorted_data`]. The returned
    /// box must be ready to receive a full backfill. Custom indexes have no
    /// generic "make empty" hook and return `Err` — bulk loads over them
    /// are rejected rather than silently dropping the index.
    fn empty_clone(&self) -> Result<Box<dyn IndexMaintainer<R, K>>>;
    /// Returns a reference to the underlying index as `Any`.
    fn as_any(&self) -> &dyn Any;

    /// Rebuild the index from a fully-built data tree using a sorted
    /// bottom-up build. Default impl falls back to per-row `on_insert`
    /// over an in-order walk; concrete implementations override for speed.
    fn rebuild_from_sorted_data(&mut self, data: &BTree<K, R>) -> Result<()> {
        for (key, record) in data.range(..) {
            self.on_insert(key.clone(), record)?;
        }
        Ok(())
    }
}

/// Extracts an index key of type `IK` from a record of type `R`. Implemented
/// generically for any `Fn(&R) -> IK + Send + Sync`, so a plain closure
/// passed to `define_index` satisfies this trait.
pub trait KeyExtractor<R, IK>: Send + Sync {
    /// Computes the index key for `record`.
    fn extract(&self, record: &R) -> IK;
}

impl<R, IK, F> KeyExtractor<R, IK> for F
where
    F: Fn(&R) -> IK + Send + Sync,
{
    fn extract(&self, record: &R) -> IK {
        self(record)
    }
}

pub(crate) trait IndexStorage<IK, K>: Send + Sync {
    fn insert(&mut self, key: IK, row_key: K, name: &str) -> Result<()>;
    fn delete(&mut self, key: IK, row_key: K);
}

pub(crate) struct ManagedIndex<R, IK, S> {
    extractor: Arc<dyn KeyExtractor<R, IK>>,
    storage: S,
    name: String,
    kind: IndexKind,
}

impl<R, IK, S> ManagedIndex<R, IK, S>
where
    IK: Ord + Clone + 'static,
    S: 'static,
    R: 'static,
{
    pub fn new(
        name: String,
        kind: IndexKind,
        extractor: Arc<dyn KeyExtractor<R, IK>>,
        storage: S,
    ) -> Self {
        Self {
            name,
            kind,
            extractor,
            storage,
        }
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }
}

impl<R, IK, K> IndexMaintainer<R, K> for ManagedIndex<R, IK, UniqueStorage<IK, K>>
where
    IK: Ord + Clone + Send + Sync + 'static,
    K: PrimaryKey,
    R: Record,
{
    fn on_insert(&mut self, key: K, record: &R) -> Result<()> {
        let idx_key = self.extractor.extract(record);
        self.storage.insert(idx_key, key, &self.name)
    }

    fn on_update(&mut self, key: K, old: &R, new: &R) -> Result<()> {
        let old_key = self.extractor.extract(old);
        let new_key = self.extractor.extract(new);
        if old_key != new_key {
            // Insert new key first — if it fails (e.g. unique constraint),
            // old_key is still intact and no rollback is needed.
            self.storage.insert(new_key, key.clone(), &self.name)?;
            self.storage.delete(old_key, key);
        }
        Ok(())
    }

    fn on_delete(&mut self, key: K, record: &R) {
        let idx_key = self.extractor.extract(record);
        self.storage.delete(idx_key, key);
    }

    fn kind(&self) -> IndexKind {
        self.kind
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn clone_box(&self) -> Box<dyn IndexMaintainer<R, K>> {
        Box::new(Self {
            extractor: Arc::clone(&self.extractor),
            storage: self.storage.clone(),
            name: self.name.clone(),
            kind: self.kind,
        })
    }

    fn empty_clone(&self) -> Result<Box<dyn IndexMaintainer<R, K>>> {
        Ok(Box::new(Self {
            extractor: Arc::clone(&self.extractor),
            storage: UniqueStorage::<IK, K>::new(),
            name: self.name.clone(),
            kind: self.kind,
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn rebuild_from_sorted_data(&mut self, data: &BTree<K, R>) -> Result<()> {
        // 1. Extract (index key, row key) pairs in data order.
        let mut pairs: Vec<(IK, K)> = data
            .range(..)
            .map(|(key, rec)| (self.extractor.extract(rec), key.clone()))
            .collect();
        // 2. Sort by key. Detect collisions for unique storage.
        pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        for w in pairs.windows(2) {
            if w[0].0 == w[1].0 {
                return Err(Error::DuplicateKey(self.name.clone()));
            }
        }
        // 3. Bulk-build the index B-tree.
        let arc_pairs = pairs.into_iter().map(|(ik, key)| (ik, Arc::new(key)));
        let new_tree: BTree<IK, K> = BTree::from_sorted(arc_pairs);
        self.storage = UniqueStorage::from_btree(new_tree);
        Ok(())
    }
}

impl<R, IK, K> IndexMaintainer<R, K> for ManagedIndex<R, IK, NonUniqueStorage<IK, K>>
where
    IK: Ord + Clone + Send + Sync + 'static,
    K: PrimaryKey,
    R: Record,
{
    fn on_insert(&mut self, key: K, record: &R) -> Result<()> {
        let idx_key = self.extractor.extract(record);
        self.storage.insert(idx_key, key, &self.name)
    }

    fn on_update(&mut self, key: K, old: &R, new: &R) -> Result<()> {
        let old_key = self.extractor.extract(old);
        let new_key = self.extractor.extract(new);
        if old_key != new_key {
            // Insert new key first — if it fails (e.g. unique constraint),
            // old_key is still intact and no rollback is needed.
            self.storage.insert(new_key, key.clone(), &self.name)?;
            self.storage.delete(old_key, key);
        }
        Ok(())
    }

    fn on_delete(&mut self, key: K, record: &R) {
        let idx_key = self.extractor.extract(record);
        self.storage.delete(idx_key, key);
    }

    fn kind(&self) -> IndexKind {
        self.kind
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn clone_box(&self) -> Box<dyn IndexMaintainer<R, K>> {
        Box::new(Self {
            extractor: Arc::clone(&self.extractor),
            storage: self.storage.clone(),
            name: self.name.clone(),
            kind: self.kind,
        })
    }

    fn empty_clone(&self) -> Result<Box<dyn IndexMaintainer<R, K>>> {
        Ok(Box::new(Self {
            extractor: Arc::clone(&self.extractor),
            storage: NonUniqueStorage::<IK, K>::new(),
            name: self.name.clone(),
            kind: self.kind,
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn rebuild_from_sorted_data(&mut self, data: &BTree<K, R>) -> Result<()> {
        // (index key, row key) is already strictly ordered when sorted
        // because the row key is unique.
        let mut pairs: Vec<((IK, K), ())> = data
            .range(..)
            .map(|(key, rec)| ((self.extractor.extract(rec), key.clone()), ()))
            .collect();
        pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let arc_pairs = pairs.into_iter().map(|(k, _)| (k, Arc::new(())));
        let new_tree: BTree<(IK, K), ()> = BTree::from_sorted(arc_pairs);
        self.storage = NonUniqueStorage::from_btree(new_tree);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// UniqueStorage — maps IK -> K (one primary key per index key)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct UniqueStorage<IK: Ord + Clone, K: PrimaryKey> {
    tree: BTree<IK, K>,
}

impl<IK: Ord + Clone + 'static, K: PrimaryKey> UniqueStorage<IK, K> {
    /// Creates a new, empty unique index storage.
    pub fn new() -> Self {
        Self { tree: BTree::new() }
    }

    /// Construct from a fully-built B-tree. Used by the bulk-load index primitive.
    pub(crate) fn from_btree(tree: BTree<IK, K>) -> Self {
        Self { tree }
    }

    pub fn get(&self, key: &IK) -> Option<K> {
        self.tree.get(key).cloned()
    }

    pub fn range_ids<'a>(
        &'a self,
        range: impl std::ops::RangeBounds<IK> + 'a,
    ) -> impl Iterator<Item = (&'a IK, K)> + 'a {
        self.tree.range(range).map(|(k, v)| (k, v.clone()))
    }
}

impl<IK: Ord + Clone + Send + Sync + 'static, K: PrimaryKey> IndexStorage<IK, K>
    for UniqueStorage<IK, K>
{
    fn insert(&mut self, key: IK, row_key: K, name: &str) -> Result<()> {
        if self.tree.get(&key).is_some() {
            return Err(Error::DuplicateKey(name.to_string()));
        }
        self.tree = self.tree.insert(key, row_key);
        Ok(())
    }

    fn delete(&mut self, key: IK, _row_key: K) {
        match self.tree.remove(&key) {
            Ok(new_tree) => self.tree = new_tree,
            Err(_) => debug_assert!(false, "UniqueStorage::delete called for absent key"),
        }
    }
}

// ---------------------------------------------------------------------------
// NonUniqueStorage — maps (IK, K) -> () (composite key for multi-value)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct NonUniqueStorage<IK: Ord + Clone, K: PrimaryKey> {
    tree: BTree<(IK, K), ()>,
}

impl<IK: Ord + Clone + Send + Sync + 'static, K: PrimaryKey> NonUniqueStorage<IK, K> {
    /// Creates a new, empty non-unique index storage.
    pub fn new() -> Self {
        Self { tree: BTree::new() }
    }

    /// Construct from a fully-built B-tree. Used by the bulk-load index primitive.
    pub(crate) fn from_btree(tree: BTree<(IK, K), ()>) -> Self {
        Self { tree }
    }

    /// Row keys sharing the index key `key`, ascending. O(log n + k) — the
    /// prefix group is located by descent, not by filtering a wider scan.
    pub fn get_ids<'a>(&'a self, key: &'a IK) -> impl Iterator<Item = K> + 'a {
        self.tree.range_prefix(key).map(|((_, k), _)| k.clone())
    }

    /// Row keys whose index key falls in `range`, ascending by `(IK, K)`.
    ///
    /// The bounds are on `IK` alone, which cannot be expressed as a
    /// `RangeBounds<(IK, K)>` (that would need a min/max value for the
    /// arbitrary row-key type `K`), so this goes through `BTree::range_by`
    /// with a locator that looks only at the index-key half of the composite.
    pub fn range_ids<'a>(
        &'a self,
        range: impl std::ops::RangeBounds<IK> + 'a,
    ) -> impl Iterator<Item = (&'a IK, K)> + 'a {
        use std::cmp::Ordering;
        use std::ops::Bound;
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        // Monotone: `(ik, _)` sorts by `ik` first, and a bound comparison on
        // `ik` is itself monotone — an entire `ik` group is classified alike,
        // so `Excluded` drops the whole group rather than part of it.
        self.tree
            .range_by(move |(ik, _): &(IK, K)| {
                match &start {
                    Bound::Included(s) if ik < s => return Ordering::Less,
                    Bound::Excluded(s) if ik <= s => return Ordering::Less,
                    _ => {}
                }
                match &end {
                    Bound::Included(e) if ik > e => Ordering::Greater,
                    Bound::Excluded(e) if ik >= e => Ordering::Greater,
                    _ => Ordering::Equal,
                }
            })
            .map(|((ik, id), _)| (ik, id.clone()))
    }
}

impl<IK: Ord + Clone + Send + Sync + 'static, K: PrimaryKey> IndexStorage<IK, K>
    for NonUniqueStorage<IK, K>
{
    fn insert(&mut self, key: IK, row_key: K, _name: &str) -> Result<()> {
        self.tree = self.tree.insert((key, row_key), ());
        Ok(())
    }

    fn delete(&mut self, key: IK, row_key: K) {
        match self.tree.remove(&(key, row_key)) {
            Ok(new_tree) => self.tree = new_tree,
            Err(_) => debug_assert!(false, "NonUniqueStorage::delete called for absent key"),
        }
    }
}

// ---------------------------------------------------------------------------
// CustomIndex — public trait for user-defined index structures
// ---------------------------------------------------------------------------

/// Trait for user-defined custom indexes.
///
/// Implementors have full control over their internal data structure and query
/// API. The `Clone` bound is required for CoW snapshot cloning — use
/// [`BTree<K, V>`](crate::btree::BTree) internally for O(1) clone.
pub trait CustomIndex<R: Record>: Send + Sync + Clone + 'static {
    /// Called when a record is inserted. Return `Err` to veto the mutation.
    fn on_insert(&mut self, id: u64, record: &R) -> Result<()>;

    /// Called when a record is updated. Return `Err` to veto the mutation.
    fn on_update(&mut self, id: u64, old: &R, new: &R) -> Result<()>;

    /// Called when a record is deleted.
    fn on_delete(&mut self, id: u64, record: &R);

    /// Rebuild the entire index from an iterator of `(id, record)` pairs.
    ///
    /// Used for backfilling when the index is defined on a non-empty table,
    /// and for recovery from persistence. The default implementation iterates
    /// and calls [`on_insert`](Self::on_insert) for each entry.
    fn rebuild<'a>(&mut self, data: impl Iterator<Item = (u64, &'a R)>) -> Result<()>
    where
        R: 'a,
    {
        for (id, record) in data {
            self.on_insert(id, record)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CustomIndexAdapter — bridges CustomIndex into IndexMaintainer
// ---------------------------------------------------------------------------

pub(crate) struct CustomIndexAdapter<R: Record, I: CustomIndex<R>> {
    inner: I,
    name: String,
    _phantom: std::marker::PhantomData<R>,
}

impl<R: Record, I: CustomIndex<R>> CustomIndexAdapter<R, I> {
    pub fn new(name: String, index: I) -> Self {
        Self {
            inner: index,
            name,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn inner(&self) -> &I {
        &self.inner
    }
}

// `CustomIndex` (the public API) is not yet generic over the row-key type —
// widening it is out of scope for this task. The adapter therefore implements
// `IndexMaintainer<R, u64>` specifically; `Table` still instantiates all of
// its indexes at `K = u64`, so this stays consistent with the rest of the
// crate until `CustomIndex` itself is widened.
impl<R: Record, I: CustomIndex<R> + 'static> IndexMaintainer<R, u64> for CustomIndexAdapter<R, I> {
    fn on_insert(&mut self, id: u64, record: &R) -> Result<()> {
        self.inner.on_insert(id, record)
    }

    fn on_update(&mut self, id: u64, old: &R, new: &R) -> Result<()> {
        self.inner.on_update(id, old, new)
    }

    fn on_delete(&mut self, id: u64, record: &R) {
        self.inner.on_delete(id, record)
    }

    fn kind(&self) -> IndexKind {
        IndexKind::Custom
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn clone_box(&self) -> Box<dyn IndexMaintainer<R, u64>> {
        Box::new(CustomIndexAdapter {
            inner: self.inner.clone(),
            name: self.name.clone(),
            _phantom: std::marker::PhantomData,
        })
    }

    fn empty_clone(&self) -> Result<Box<dyn IndexMaintainer<R, u64>>> {
        // Custom indexes have user-defined internal state with no generic
        // "make empty" hook; the bulk-load primitives don't yet support
        // them. Until a `CustomIndex::empty` requirement (or similar)
        // lands, the bulk-load Replace path can't preserve a custom-index
        // definition and rejects the load. See
        // `docs/tasks/task23_bulk_load.md`.
        Err(Error::InvalidBulkLoadInput(format!(
            "rebuilding custom index '{}' is not supported; \
             drop the index before bulk-loading and redefine it after",
            self.name
        )))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
    struct User {
        email: String,
        age: u32,
    }

    #[test]
    fn unique_index_insert_and_lookup() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let user = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &user).unwrap();
        assert_eq!(idx.storage().get(&"alice@example.com".to_string()), Some(1));
        assert_eq!(idx.storage().get(&"bob@example.com".to_string()), None);
    }

    #[test]
    fn unique_index_rejects_duplicate() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let u1 = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        let u2 = User {
            email: "alice@example.com".to_string(),
            age: 25,
        };
        idx.on_insert(1u64, &u1).unwrap();
        assert!(matches!(idx.on_insert(2, &u2), Err(Error::DuplicateKey(_))));
    }

    #[test]
    fn unique_index_update_changes_key() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let old = User {
            email: "alice@old.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &old).unwrap();
        let new = User {
            email: "alice@new.com".to_string(),
            age: 30,
        };
        idx.on_update(1, &old, &new).unwrap();
        assert_eq!(idx.storage().get(&"alice@old.com".to_string()), None);
        assert_eq!(idx.storage().get(&"alice@new.com".to_string()), Some(1));
    }

    #[test]
    fn unique_index_update_rejects_conflict() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let u1 = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        let u2 = User {
            email: "bob@example.com".to_string(),
            age: 25,
        };
        idx.on_insert(1u64, &u1).unwrap();
        idx.on_insert(2, &u2).unwrap();
        // Try to update bob's email to alice's — should fail
        let u2_new = User {
            email: "alice@example.com".to_string(),
            age: 25,
        };
        assert!(matches!(
            idx.on_update(2, &u2, &u2_new),
            Err(Error::DuplicateKey(_))
        ));
        // Bob's old email should still be in the index (rollback)
        assert_eq!(idx.storage().get(&"bob@example.com".to_string()), Some(2));
    }

    #[test]
    fn unique_index_delete() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let user = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &user).unwrap();
        idx.on_delete(1, &user);
        assert_eq!(idx.storage().get(&"alice@example.com".to_string()), None);
    }

    #[test]
    fn non_unique_index_insert_and_lookup() {
        let ext: Arc<dyn KeyExtractor<User, u32>> = Arc::new(|u: &User| u.age);
        let mut idx = ManagedIndex::new(
            "by_age".to_string(),
            IndexKind::NonUnique,
            ext,
            NonUniqueStorage::new(),
        );
        let u1 = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        let u2 = User {
            email: "bob@example.com".to_string(),
            age: 30,
        };
        let u3 = User {
            email: "charlie@example.com".to_string(),
            age: 25,
        };
        idx.on_insert(1, &u1).unwrap();
        idx.on_insert(2, &u2).unwrap();
        idx.on_insert(3, &u3).unwrap();
        let ids_30: Vec<u64> = idx.storage().get_ids(&30).collect();
        assert_eq!(ids_30, vec![1, 2]);
        let ids_25: Vec<u64> = idx.storage().get_ids(&25).collect();
        assert_eq!(ids_25, vec![3]);
        let ids_99: Vec<u64> = idx.storage().get_ids(&99).collect();
        assert!(ids_99.is_empty());
    }

    #[test]
    fn non_unique_index_update() {
        let ext: Arc<dyn KeyExtractor<User, u32>> = Arc::new(|u: &User| u.age);
        let mut idx = ManagedIndex::new(
            "by_age".to_string(),
            IndexKind::NonUnique,
            ext,
            NonUniqueStorage::new(),
        );
        let old = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1, &old).unwrap();
        let new = User {
            email: "alice@example.com".to_string(),
            age: 31,
        };
        idx.on_update(1, &old, &new).unwrap();
        assert_eq!(idx.storage().get_ids(&30).count(), 0);
        let ids_31: Vec<u64> = idx.storage().get_ids(&31).collect();
        assert_eq!(ids_31, vec![1]);
    }

    #[test]
    fn non_unique_index_delete() {
        let ext: Arc<dyn KeyExtractor<User, u32>> = Arc::new(|u: &User| u.age);
        let mut idx = ManagedIndex::new(
            "by_age".to_string(),
            IndexKind::NonUnique,
            ext,
            NonUniqueStorage::new(),
        );
        let u1 = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        let u2 = User {
            email: "bob@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1, &u1).unwrap();
        idx.on_insert(2, &u2).unwrap();
        idx.on_delete(1, &u1);
        let ids_30: Vec<u64> = idx.storage().get_ids(&30).collect();
        assert_eq!(ids_30, vec![2]);
    }

    #[test]
    fn clone_box_produces_independent_copy() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let user = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &user).unwrap();

        let cloned = idx.clone_box();
        // Mutate original
        let user2 = User {
            email: "bob@example.com".to_string(),
            age: 25,
        };
        idx.on_insert(2u64, &user2).unwrap();

        // Clone should not see the new insert
        let cloned = cloned
            .as_any()
            .downcast_ref::<ManagedIndex<User, String, UniqueStorage<String, u64>>>()
            .unwrap();
        assert_eq!(cloned.storage().get(&"bob@example.com".to_string()), None);
        assert_eq!(
            cloned.storage().get(&"alice@example.com".to_string()),
            Some(1)
        );
    }

    #[test]
    fn index_kind_custom_variant() {
        let kind = IndexKind::Custom;
        assert_eq!(kind, IndexKind::Custom);
        assert_ne!(kind, IndexKind::Unique);
        assert_ne!(kind, IndexKind::NonUnique);
    }

    /// A minimal custom index that tracks the sum of a numeric field.
    #[derive(Clone)]
    struct SumIndex {
        total: u64,
    }

    impl SumIndex {
        fn new() -> Self {
            Self { total: 0 }
        }

        fn total(&self) -> u64 {
            self.total
        }
    }

    impl CustomIndex<User> for SumIndex {
        fn on_insert(&mut self, _id: u64, record: &User) -> Result<()> {
            self.total += record.age as u64;
            Ok(())
        }

        fn on_update(&mut self, _id: u64, old: &User, new: &User) -> Result<()> {
            self.total -= old.age as u64;
            self.total += new.age as u64;
            Ok(())
        }

        fn on_delete(&mut self, _id: u64, record: &User) {
            self.total -= record.age as u64;
        }
    }

    #[test]
    fn custom_index_adapter_lifecycle() {
        let sum = SumIndex::new();
        let mut adapter = CustomIndexAdapter::new("sum".to_string(), sum);

        let u1 = User {
            email: "a@x.com".to_string(),
            age: 30,
        };
        adapter.on_insert(1, &u1).unwrap();

        let inner = adapter
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(inner.total(), 30);

        let u2 = User {
            email: "b@x.com".to_string(),
            age: 20,
        };
        adapter.on_insert(2, &u2).unwrap();

        let inner = adapter
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(inner.total(), 50);

        let u1_new = User {
            email: "a@x.com".to_string(),
            age: 35,
        };
        adapter.on_update(1, &u1, &u1_new).unwrap();

        let inner = adapter
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(inner.total(), 55);

        adapter.on_delete(2, &u2);

        let inner = adapter
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(inner.total(), 35);

        assert_eq!(adapter.kind(), IndexKind::Custom);
    }

    #[test]
    fn custom_index_adapter_clone_box_independent() {
        let sum = SumIndex::new();
        let mut adapter = CustomIndexAdapter::new("sum".to_string(), sum);

        let u1 = User {
            email: "a@x.com".to_string(),
            age: 30,
        };
        adapter.on_insert(1, &u1).unwrap();

        let cloned = adapter.clone_box();

        let u2 = User {
            email: "b@x.com".to_string(),
            age: 20,
        };
        adapter.on_insert(2, &u2).unwrap();

        let cloned_inner = cloned
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(cloned_inner.total(), 30);

        let orig_inner = adapter
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(orig_inner.total(), 50);
    }

    #[test]
    fn rebuild_from_sorted_data_unique_matches_incremental() {
        // Build the same index two ways: incrementally via on_insert vs.
        // bulk via rebuild_from_sorted_data. Assert identical lookups.
        use std::sync::Arc;

        #[derive(Clone)]
        #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
        struct Row {
            name: String,
        }

        let data: BTree<u64, Row> = {
            let mut t = BTree::new();
            for (id, name) in [(1u64, "a"), (2, "b"), (3, "c"), (4, "d")] {
                t = t.insert(
                    id,
                    Row {
                        name: name.to_string(),
                    },
                );
            }
            t
        };

        let extractor = Arc::new(|r: &Row| r.name.clone());
        let mut incr: ManagedIndex<Row, String, UniqueStorage<String, u64>> = ManagedIndex::new(
            "idx".into(),
            IndexKind::Unique,
            extractor.clone(),
            UniqueStorage::new(),
        );
        for (id, row) in data.range(..) {
            incr.on_insert(*id, row).unwrap();
        }

        let mut bulk: ManagedIndex<Row, String, UniqueStorage<String, u64>> = ManagedIndex::new(
            "idx".into(),
            IndexKind::Unique,
            extractor,
            UniqueStorage::new(),
        );
        bulk.rebuild_from_sorted_data(&data).unwrap();

        for key in ["a", "b", "c", "d"] {
            assert_eq!(
                incr.storage().get(&key.to_string()),
                bulk.storage().get(&key.to_string())
            );
        }
    }

    #[test]
    fn rebuild_from_sorted_data_non_unique_matches_incremental() {
        use std::sync::Arc;

        #[derive(Clone)]
        #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
        struct Row {
            age: u32,
        }

        let data: BTree<u64, Row> = {
            let mut t = BTree::new();
            for (id, age) in [(1u64, 10u32), (2, 20), (3, 10), (4, 30), (5, 20)] {
                t = t.insert(id, Row { age });
            }
            t
        };

        let extractor = Arc::new(|r: &Row| r.age);
        let mut incr: ManagedIndex<Row, u32, NonUniqueStorage<u32, u64>> = ManagedIndex::new(
            "idx".into(),
            IndexKind::NonUnique,
            extractor.clone(),
            NonUniqueStorage::new(),
        );
        for (id, row) in data.range(..) {
            incr.on_insert(*id, row).unwrap();
        }

        let mut bulk: ManagedIndex<Row, u32, NonUniqueStorage<u32, u64>> = ManagedIndex::new(
            "idx".into(),
            IndexKind::NonUnique,
            extractor,
            NonUniqueStorage::new(),
        );
        bulk.rebuild_from_sorted_data(&data).unwrap();

        for key in [10u32, 20, 30] {
            let mut a: Vec<u64> = incr.storage().get_ids(&key).collect();
            a.sort();
            let mut b: Vec<u64> = bulk.storage().get_ids(&key).collect();
            b.sort();
            assert_eq!(a, b);
        }
    }

    #[test]
    fn rebuild_from_sorted_data_unique_collision_errors() {
        use std::sync::Arc;
        #[derive(Clone)]
        #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
        struct Row {
            name: String,
        }

        let data: BTree<u64, Row> = {
            let mut t = BTree::new();
            t = t.insert(1, Row { name: "dup".into() });
            t = t.insert(2, Row { name: "dup".into() });
            t
        };
        let mut idx: ManagedIndex<Row, String, UniqueStorage<String, u64>> = ManagedIndex::new(
            "idx".into(),
            IndexKind::Unique,
            Arc::new(|r: &Row| r.name.clone()),
            UniqueStorage::new(),
        );
        let res = idx.rebuild_from_sorted_data(&data);
        assert!(matches!(res, Err(Error::DuplicateKey(_))));
    }

    #[test]
    fn unique_compound_index() {
        let ext: Arc<dyn KeyExtractor<User, (u32, String)>> =
            Arc::new(|u: &User| (u.age, u.email.clone()));
        let mut idx = ManagedIndex::new(
            "by_age_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let user = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &user).unwrap();

        assert_eq!(
            idx.storage().get(&(30, "alice@example.com".to_string())),
            Some(1)
        );
        assert_eq!(
            idx.storage().get(&(30, "bob@example.com".to_string())),
            None
        );

        // Reject duplicate (age, email)
        let user_dup = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        assert!(matches!(
            idx.on_insert(2, &user_dup),
            Err(Error::DuplicateKey(_))
        ));
    }

    // -----------------------------------------------------------------------
    // task 2: index storage generic over the row key
    // -----------------------------------------------------------------------

    #[test]
    fn unique_index_over_string_primary_key() {
        // Index key is u32 (an age); row key is String (an email).
        let mut storage: UniqueStorage<u32, String> = UniqueStorage::new();
        storage.insert(30u32, "a@x.com".to_string(), "by_age").unwrap();
        storage.insert(40u32, "b@x.com".to_string(), "by_age").unwrap();

        assert_eq!(storage.get(&30), Some("a@x.com".to_string()));
        assert_eq!(storage.get(&40), Some("b@x.com".to_string()));
        assert_eq!(storage.get(&50), None);

        // A second row at the same index key is rejected.
        let err = storage
            .insert(30u32, "c@x.com".to_string(), "by_age")
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(_)), "got {err:?}");
    }

    #[test]
    fn non_unique_index_over_string_primary_key() {
        let mut storage: NonUniqueStorage<u32, String> = NonUniqueStorage::new();
        storage.insert(30u32, "a@x.com".to_string(), "by_age").unwrap();
        storage.insert(30u32, "b@x.com".to_string(), "by_age").unwrap();
        storage.insert(40u32, "c@x.com".to_string(), "by_age").unwrap();

        let mut at_30: Vec<String> = storage.get_ids(&30).collect();
        at_30.sort();
        assert_eq!(at_30, vec!["a@x.com".to_string(), "b@x.com".to_string()]);

        let at_40: Vec<String> = storage.get_ids(&40).collect();
        assert_eq!(at_40, vec!["c@x.com".to_string()]);
    }

    #[test]
    fn non_unique_range_ids_over_string_primary_key() {
        use std::ops::Bound;
        let mut storage: NonUniqueStorage<u32, String> = NonUniqueStorage::new();
        storage.insert(10u32, "a@x.com".to_string(), "by_age").unwrap();
        storage.insert(20u32, "b@x.com".to_string(), "by_age").unwrap();
        storage.insert(20u32, "c@x.com".to_string(), "by_age").unwrap();
        storage.insert(30u32, "d@x.com".to_string(), "by_age").unwrap();

        let mut got: Vec<String> = storage
            .range_ids((Bound::Included(20u32), Bound::Included(30u32)))
            .map(|(_, k)| k)
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec!["b@x.com".to_string(), "c@x.com".to_string(), "d@x.com".to_string()]
        );

        // Excluding the lower bound must drop that whole group, not part of it.
        let mut got: Vec<String> = storage
            .range_ids((Bound::Excluded(20u32), Bound::Unbounded))
            .map(|(_, k)| k)
            .collect();
        got.sort();
        assert_eq!(got, vec!["d@x.com".to_string()]);
    }
}
