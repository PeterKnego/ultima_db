use std::any::Any;
use std::sync::Arc;

use crate::btree::BTree;
use crate::{Error, Result};

/// Whether an index enforces uniqueness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Unique,
    NonUnique,
}

// ---------------------------------------------------------------------------
// IndexMaintainer trait — object-safe interface for type-erased indexes
// ---------------------------------------------------------------------------

pub(crate) trait IndexMaintainer<R>: Send + Sync {
    /// Called when a record is inserted into the table.
    fn on_insert(&mut self, id: u64, record: &R) -> Result<()>;
    /// Called when a record in the table is updated.
    fn on_update(&mut self, id: u64, old: &R, new: &R) -> Result<()>;
    /// Called when a record is deleted from the table.
    fn on_delete(&mut self, id: u64, record: &R);
    /// Returns the kind of this index (Unique or NonUnique).
    fn kind(&self) -> IndexKind;
    /// Returns a boxed clone of this index maintainer.
    fn clone_box(&self) -> Box<dyn IndexMaintainer<R>>;
    /// Returns a reference to the underlying index as `Any`.
    fn as_any(&self) -> &dyn Any;
}

pub trait KeyExtractor<R, K>: Send + Sync {
    fn extract(&self, record: &R) -> K;
}

impl<R, K, F> KeyExtractor<R, K> for F
where
    F: Fn(&R) -> K + Send + Sync,
{
    fn extract(&self, record: &R) -> K {
        self(record)
    }
}

pub(crate) trait IndexStorage<K>: Send + Sync {
    fn insert(&mut self, key: K, id: u64, name: &str) -> Result<()>;
    fn delete(&mut self, key: K, id: u64);
}

pub(crate) struct ManagedIndex<R, K, S> {
    extractor: Arc<dyn KeyExtractor<R, K>>,
    storage: S,
    name: String,
    kind: IndexKind,
}

impl<R, K, S> ManagedIndex<R, K, S>
where
    K: Ord + Clone + 'static,
    S: IndexStorage<K> + 'static,
    R: 'static,
{
    pub fn new(name: String, kind: IndexKind, extractor: Arc<dyn KeyExtractor<R, K>>, storage: S) -> Self {
        Self { name, kind, extractor, storage }
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

}

impl<R, K, S> IndexMaintainer<R> for ManagedIndex<R, K, S>
where
    K: Ord + Clone + Send + Sync + 'static,
    S: IndexStorage<K> + Clone + 'static,
    R: Send + Sync + 'static,
{
    fn on_insert(&mut self, id: u64, record: &R) -> Result<()> {
        let key = self.extractor.extract(record);
        self.storage.insert(key, id, &self.name)
    }

    fn on_update(&mut self, id: u64, old: &R, new: &R) -> Result<()> {
        let old_key = self.extractor.extract(old);
        let new_key = self.extractor.extract(new);
        if old_key != new_key {
            // Insert new key first — if it fails (e.g. unique constraint),
            // old_key is still intact and no rollback is needed.
            self.storage.insert(new_key, id, &self.name)?;
            self.storage.delete(old_key, id);
        }
        Ok(())
    }

    fn on_delete(&mut self, id: u64, record: &R) {
        let key = self.extractor.extract(record);
        self.storage.delete(key, id);
    }

    fn kind(&self) -> IndexKind {
        self.kind
    }

    fn clone_box(&self) -> Box<dyn IndexMaintainer<R>> {
        Box::new(Self {
            extractor: Arc::clone(&self.extractor),
            storage: self.storage.clone(),
            name: self.name.clone(),
            kind: self.kind,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ---------------------------------------------------------------------------
// UniqueStorage — maps K -> u64 (one primary ID per key)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct UniqueStorage<K: Ord + Clone> {
    tree: BTree<K, u64>,
}

impl<K: Ord + Clone + 'static> UniqueStorage<K> {
    /// Creates a new, empty unique index storage.
    pub fn new() -> Self {
        Self { tree: BTree::new() }
    }

    pub fn get(&self, key: &K) -> Option<u64> {
        self.tree.get(key).copied()
    }

    pub fn range_ids<'a>(&'a self, range: impl std::ops::RangeBounds<K> + 'a) -> impl Iterator<Item = (&'a K, u64)> + 'a {
        self.tree.range(range).map(|(k, v)| (k, *v))
    }
}

impl<K: Ord + Clone + Send + Sync + 'static> IndexStorage<K> for UniqueStorage<K> {
    fn insert(&mut self, key: K, id: u64, name: &str) -> Result<()> {
        if self.tree.get(&key).is_some() {
            return Err(Error::DuplicateKey(name.to_string()));
        }
        self.tree = self.tree.insert(key, id);
        Ok(())
    }

    fn delete(&mut self, key: K, _id: u64) {
        match self.tree.remove(&key) {
            Ok(new_tree) => self.tree = new_tree,
            Err(_) => debug_assert!(false, "UniqueStorage::delete called for absent key"),
        }
    }
}

// ---------------------------------------------------------------------------
// NonUniqueStorage — maps (K, u64) -> () (composite key for multi-value)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct NonUniqueStorage<K: Ord + Clone> {
    tree: BTree<(K, u64), ()>,
}

impl<K: Ord + Clone + 'static> NonUniqueStorage<K> {
    /// Creates a new, empty non-unique index storage.
    pub fn new() -> Self {
        Self { tree: BTree::new() }
    }

    pub fn get_ids(&self, key: &K) -> impl Iterator<Item = u64> + '_ {
        self.tree
            .range((key.clone(), 0u64)..=(key.clone(), u64::MAX))
            .map(|((_, id), _)| *id)
    }

    pub fn range_ids(&self, start: std::ops::Bound<&K>, end: std::ops::Bound<&K>) -> impl Iterator<Item = (&K, u64)> + '_ {
        use std::ops::Bound;
        let range_start = match start {
            Bound::Included(k) => Bound::Included((k.clone(), 0u64)),
            Bound::Excluded(k) => Bound::Excluded((k.clone(), u64::MAX)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let range_end = match end {
            Bound::Included(k) => Bound::Included((k.clone(), u64::MAX)),
            Bound::Excluded(k) => Bound::Excluded((k.clone(), 0u64)),
            Bound::Unbounded => Bound::Unbounded,
        };
        self.tree
            .range((range_start, range_end))
            .map(|((k, id), _)| (k, *id))
    }
}

impl<K: Ord + Clone + Send + Sync + 'static> IndexStorage<K> for NonUniqueStorage<K> {
    fn insert(&mut self, key: K, id: u64, _name: &str) -> Result<()> {
        self.tree = self.tree.insert((key, id), ());
        Ok(())
    }

    fn delete(&mut self, key: K, id: u64) {
        match self.tree.remove(&(key, id)) {
            Ok(new_tree) => self.tree = new_tree,
            Err(_) => debug_assert!(false, "NonUniqueStorage::delete called for absent key"),
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct User {
        email: String,
        age: u32,
    }

    #[test]
    fn unique_index_insert_and_lookup() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new("by_email".to_string(), IndexKind::Unique, ext, UniqueStorage::new());
        let user = User { email: "alice@example.com".to_string(), age: 30 };
        idx.on_insert(1, &user).unwrap();
        assert_eq!(idx.storage().get(&"alice@example.com".to_string()), Some(1));
        assert_eq!(idx.storage().get(&"bob@example.com".to_string()), None);
    }

    #[test]
    fn unique_index_rejects_duplicate() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new("by_email".to_string(), IndexKind::Unique, ext, UniqueStorage::new());
        let u1 = User { email: "alice@example.com".to_string(), age: 30 };
        let u2 = User { email: "alice@example.com".to_string(), age: 25 };
        idx.on_insert(1, &u1).unwrap();
        assert!(matches!(idx.on_insert(2, &u2), Err(Error::DuplicateKey(_))));
    }

    #[test]
    fn unique_index_update_changes_key() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new("by_email".to_string(), IndexKind::Unique, ext, UniqueStorage::new());
        let old = User { email: "alice@old.com".to_string(), age: 30 };
        idx.on_insert(1, &old).unwrap();
        let new = User { email: "alice@new.com".to_string(), age: 30 };
        idx.on_update(1, &old, &new).unwrap();
        assert_eq!(idx.storage().get(&"alice@old.com".to_string()), None);
        assert_eq!(idx.storage().get(&"alice@new.com".to_string()), Some(1));
    }

    #[test]
    fn unique_index_update_rejects_conflict() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new("by_email".to_string(), IndexKind::Unique, ext, UniqueStorage::new());
        let u1 = User { email: "alice@example.com".to_string(), age: 30 };
        let u2 = User { email: "bob@example.com".to_string(), age: 25 };
        idx.on_insert(1, &u1).unwrap();
        idx.on_insert(2, &u2).unwrap();
        // Try to update bob's email to alice's — should fail
        let u2_new = User { email: "alice@example.com".to_string(), age: 25 };
        assert!(matches!(idx.on_update(2, &u2, &u2_new), Err(Error::DuplicateKey(_))));
        // Bob's old email should still be in the index (rollback)
        assert_eq!(idx.storage().get(&"bob@example.com".to_string()), Some(2));
    }

    #[test]
    fn unique_index_delete() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new("by_email".to_string(), IndexKind::Unique, ext, UniqueStorage::new());
        let user = User { email: "alice@example.com".to_string(), age: 30 };
        idx.on_insert(1, &user).unwrap();
        idx.on_delete(1, &user);
        assert_eq!(idx.storage().get(&"alice@example.com".to_string()), None);
    }

    #[test]
    fn non_unique_index_insert_and_lookup() {
        let ext: Arc<dyn KeyExtractor<User, u32>> = Arc::new(|u: &User| u.age);
        let mut idx = ManagedIndex::new("by_age".to_string(), IndexKind::NonUnique, ext, NonUniqueStorage::new());
        let u1 = User { email: "alice@example.com".to_string(), age: 30 };
        let u2 = User { email: "bob@example.com".to_string(), age: 30 };
        let u3 = User { email: "charlie@example.com".to_string(), age: 25 };
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
        let mut idx = ManagedIndex::new("by_age".to_string(), IndexKind::NonUnique, ext, NonUniqueStorage::new());
        let old = User { email: "alice@example.com".to_string(), age: 30 };
        idx.on_insert(1, &old).unwrap();
        let new = User { email: "alice@example.com".to_string(), age: 31 };
        idx.on_update(1, &old, &new).unwrap();
        assert_eq!(idx.storage().get_ids(&30).count(), 0);
        let ids_31: Vec<u64> = idx.storage().get_ids(&31).collect();
        assert_eq!(ids_31, vec![1]);
    }

    #[test]
    fn non_unique_index_delete() {
        let ext: Arc<dyn KeyExtractor<User, u32>> = Arc::new(|u: &User| u.age);
        let mut idx = ManagedIndex::new("by_age".to_string(), IndexKind::NonUnique, ext, NonUniqueStorage::new());
        let u1 = User { email: "alice@example.com".to_string(), age: 30 };
        let u2 = User { email: "bob@example.com".to_string(), age: 30 };
        idx.on_insert(1, &u1).unwrap();
        idx.on_insert(2, &u2).unwrap();
        idx.on_delete(1, &u1);
        let ids_30: Vec<u64> = idx.storage().get_ids(&30).collect();
        assert_eq!(ids_30, vec![2]);
    }

    #[test]
    fn clone_box_produces_independent_copy() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new("by_email".to_string(), IndexKind::Unique, ext, UniqueStorage::new());
        let user = User { email: "alice@example.com".to_string(), age: 30 };
        idx.on_insert(1, &user).unwrap();

        let cloned = idx.clone_box();
        // Mutate original
        let user2 = User { email: "bob@example.com".to_string(), age: 25 };
        idx.on_insert(2, &user2).unwrap();

        // Clone should not see the new insert
        let cloned = cloned.as_any().downcast_ref::<ManagedIndex<User, String, UniqueStorage<String>>>().unwrap();
        assert_eq!(cloned.storage().get(&"bob@example.com".to_string()), None);
        assert_eq!(cloned.storage().get(&"alice@example.com".to_string()), Some(1));
    }

    #[test]
    fn unique_compound_index() {
        let ext: Arc<dyn KeyExtractor<User, (u32, String)>> = Arc::new(|u: &User| (u.age, u.email.clone()));
        let mut idx = ManagedIndex::new("by_age_email".to_string(), IndexKind::Unique, ext, UniqueStorage::new());
        let user = User { email: "alice@example.com".to_string(), age: 30 };
        idx.on_insert(1, &user).unwrap();

        assert_eq!(idx.storage().get(&(30, "alice@example.com".to_string())), Some(1));
        assert_eq!(idx.storage().get(&(30, "bob@example.com".to_string())), None);

        // Reject duplicate (age, email)
        let user_dup = User { email: "alice@example.com".to_string(), age: 30 };
        assert!(matches!(idx.on_insert(2, &user_dup), Err(Error::DuplicateKey(_))));
    }
}
