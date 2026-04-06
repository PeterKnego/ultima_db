// Arc<Snapshot> is intentionally non-Send+Sync for now. Task 4 will add
// Send + Sync bounds when multi-threaded access is required.
#![allow(clippy::arc_with_non_send_sync)]

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::table::{Table, TableOpener};
use crate::{Error, Result};

// ---------------------------------------------------------------------------
// Snapshot — an immutable versioned view of all tables
// ---------------------------------------------------------------------------

/// An immutable snapshot of all tables at a specific version.
///
/// Tables are stored as `Arc<dyn Any>` so that building a new snapshot from an
/// existing one (at commit time) is O(number-of-tables) with O(1) per table.
///
/// `Snapshot` is not `Send + Sync` because `dyn Any` does not require those
/// bounds. Task 4 will add `Send + Sync` bounds when multi-threaded access is
/// required.
pub(crate) struct Snapshot {
    pub(crate) version: u64,
    pub(crate) tables: HashMap<String, Arc<dyn Any>>,
}

// ---------------------------------------------------------------------------
// StoreConfig
// ---------------------------------------------------------------------------

/// Configuration for [`Store`] behavior.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// How many most-recent snapshots to retain during [`Store::gc()`]. Default: 10.
    /// The latest snapshot is always retained regardless of this value.
    pub num_snapshots_retained: usize,
    /// Whether [`Store::gc()`] runs automatically after each [`WriteTx::commit()`]. Default: true.
    pub auto_snapshot_gc: bool,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            num_snapshots_retained: 10,
            auto_snapshot_gc: true,
        }
    }
}

// ---------------------------------------------------------------------------
// StoreInner — the interior-mutable state behind Store
// ---------------------------------------------------------------------------

pub(crate) struct StoreInner {
    /// All committed snapshots keyed by version. Version 0 = empty store.
    pub(crate) snapshots: HashMap<u64, Arc<Snapshot>>,
    pub(crate) latest_version: u64,
    /// Next auto-assigned write version. Always > `latest_version`.
    pub(crate) next_version: u64,
    pub(crate) config: StoreConfig,
}

// ---------------------------------------------------------------------------
// Store — manages version history via interior mutability
// ---------------------------------------------------------------------------

/// An in-memory store with MVCC snapshot isolation.
///
/// Every committed write produces a new numbered snapshot stored in the
/// version history.  [`ReadTx`] borrows a snapshot by `Arc`, keeping it alive
/// independently of the store.  [`WriteTx`] works on a mutable copy of the
/// tables it touches and atomically publishes a new snapshot on [`WriteTx::commit`].
///
/// `Store` is cheaply cloneable — all clones share the same interior state.
#[derive(Clone)]
pub struct Store {
    pub(crate) inner: Arc<RwLock<StoreInner>>,
}

impl Store {
    /// Creates a new, empty store. The initial version is 0.
    pub fn new(config: StoreConfig) -> Self {
        let empty = Arc::new(Snapshot { version: 0, tables: HashMap::new() });
        let mut snapshots = HashMap::new();
        snapshots.insert(0, empty);
        Self {
            inner: Arc::new(RwLock::new(StoreInner {
                snapshots,
                latest_version: 0,
                next_version: 1,
                config,
            })),
        }
    }

    /// The version number of the most recently committed snapshot.
    pub fn latest_version(&self) -> u64 {
        self.inner.read().unwrap().latest_version
    }

    /// Open a read transaction at `version` (latest if `None`).
    ///
    /// Returns [`Error::VersionNotFound`] if the requested version does not exist.
    pub fn begin_read(&self, version: Option<u64>) -> Result<ReadTx> {
        let inner = self.inner.read().unwrap();
        let v = version.unwrap_or(inner.latest_version);
        let snapshot = inner
            .snapshots
            .get(&v)
            .ok_or(Error::VersionNotFound(v))?
            .clone();
        Ok(ReadTx { snapshot })
    }

    /// Open a write transaction.
    ///
    /// - `version: None` — auto-assign the next available version.
    /// - `version: Some(v)` — use `v` as the commit version; `v` must be
    ///   strictly greater than the current latest, otherwise
    ///   [`Error::WriteConflict`] is returned.
    ///
    /// The base snapshot for the transaction is always the latest committed
    /// snapshot, regardless of the assigned commit version.
    pub fn begin_write(&self, version: Option<u64>) -> Result<WriteTx> {
        let mut inner = self.inner.write().unwrap();
        let commit_version = match version {
            None => inner.next_version,
            Some(v) if v > inner.latest_version => v,
            Some(_) => return Err(Error::WriteConflict),
        };
        // Keep next_version ahead of any explicitly requested version.
        if commit_version >= inner.next_version {
            inner.next_version = commit_version + 1;
        }
        let base = inner.snapshots[&inner.latest_version].clone();
        Ok(WriteTx {
            base,
            dirty: HashMap::new(),
            version: commit_version,
            store_inner: Arc::clone(&self.inner),
            deleted_tables: HashSet::new(),
        })
    }

    /// Garbage collect old snapshots that are no longer referenced by any [`ReadTx`].
    /// Always keeps the `num_snapshots_retained` most recent snapshots, plus any
    /// snapshot held by an active [`ReadTx`]. The latest snapshot is always kept
    /// even if `num_snapshots_retained` is 0.
    pub fn gc(&self) {
        let mut inner = self.inner.write().unwrap();
        gc_inner(&mut inner);
    }

    // --- Test helpers ---

    #[cfg(test)]
    fn snapshot_count(&self) -> usize {
        self.inner.read().unwrap().snapshots.len()
    }

    #[cfg(test)]
    fn has_snapshot(&self, version: u64) -> bool {
        self.inner.read().unwrap().snapshots.contains_key(&version)
    }
}

/// Run GC on an already-locked `StoreInner`.
fn gc_inner(inner: &mut StoreInner) {
    let mut versions: Vec<u64> = inner.snapshots.keys().copied().collect();
    versions.sort_unstable();

    // The N most recent versions to retain unconditionally.
    // latest_version is always kept (even if num_snapshots_retained is 0).
    let retain_count = inner.config.num_snapshots_retained.max(1);
    let cutoff_idx = versions.len().saturating_sub(retain_count);
    let protected: HashSet<u64> = versions[cutoff_idx..].iter().copied().collect();

    inner.snapshots.retain(|&v, snapshot| {
        protected.contains(&v) || Arc::strong_count(snapshot) > 1
    });
}

impl Default for Store {
    fn default() -> Self {
        Self::new(StoreConfig::default())
    }
}

// ---------------------------------------------------------------------------
// ReadTx — snapshot-isolated read transaction
// ---------------------------------------------------------------------------

/// A read-only view of the store at a fixed version.
///
/// Multiple `ReadTx` instances can coexist.  Each holds an `Arc<Snapshot>`
/// that keeps that version alive in memory even after the store advances to
/// newer versions.
pub struct ReadTx {
    snapshot: Arc<Snapshot>,
}

/// Read-only access to a snapshot.
///
/// Implemented by [`ReadTx`]. Generic code that only needs to read tables can
/// accept `impl Readable` instead of a concrete transaction type.
pub trait Readable {
    /// Borrow a table from this snapshot.
    fn open_table<R: Send + Sync + 'static>(&self, opener: impl TableOpener<R>) -> Result<&Table<R>>;
    /// Returns the names of all tables in this snapshot, sorted alphabetically.
    fn table_names(&self) -> Vec<String>;
    /// The version number this snapshot reads from.
    fn version(&self) -> u64;
}

impl ReadTx {
    /// The version number this transaction reads from.
    pub fn version(&self) -> u64 {
        self.snapshot.version
    }

    /// Returns the names of all tables in this snapshot.
    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.snapshot.tables.keys().cloned().collect();
        names.sort();
        names
    }

    /// Borrow a table from this snapshot.
    ///
    /// Returns [`Error::KeyNotFound`] if the table does not exist in this
    /// snapshot, or [`Error::TypeMismatch`] if it was created with a different
    /// record type.
    pub fn open_table<R: Send + Sync + 'static>(&self, opener: impl TableOpener<R>) -> Result<&Table<R>> {
        let name = opener.table_name();
        self.snapshot
            .tables
            .get(name)
            .ok_or(Error::KeyNotFound)?
            .downcast_ref::<Table<R>>()
            .ok_or_else(|| Error::TypeMismatch(name.to_string()))
    }
}

impl Readable for ReadTx {
    fn open_table<R: Send + Sync + 'static>(&self, opener: impl TableOpener<R>) -> Result<&Table<R>> {
        ReadTx::open_table(self, opener)
    }

    fn table_names(&self) -> Vec<String> {
        ReadTx::table_names(self)
    }

    fn version(&self) -> u64 {
        ReadTx::version(self)
    }
}

// ---------------------------------------------------------------------------
// WriteTx — write transaction with lazy CoW table copies
// ---------------------------------------------------------------------------

/// A write transaction.  Tables are lazily copied from the base snapshot on
/// first access (O(1) per table via BTree root `Arc` clone).  Changes are
/// not visible to any `ReadTx` until [`WriteTx::commit`] is called.
pub struct WriteTx {
    base: Arc<Snapshot>,
    /// Mutable working copies of tables opened for writing.
    dirty: HashMap<String, Box<dyn Any>>,
    /// The version number that will be assigned to the new snapshot on commit.
    version: u64,
    /// Reference back to the store's interior state, used during commit.
    store_inner: Arc<RwLock<StoreInner>>,
    /// Tables explicitly deleted in this transaction.
    deleted_tables: HashSet<String>,
}

impl WriteTx {
    /// The version this transaction will commit as.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Open a table for writing.
    ///
    /// On the first call for a given `name`, the table is copied (O(1)) from
    /// the base snapshot into the dirty working set.  Subsequent calls return
    /// the same mutable copy.
    ///
    /// Creates an empty table if `name` does not exist in the base snapshot.
    pub fn open_table<R: Send + Sync + 'static>(&mut self, opener: impl TableOpener<R>) -> Result<&mut Table<R>> {
        let name = opener.table_name();
        if !self.dirty.contains_key(name) {
            let table: Table<R> = if self.deleted_tables.contains(name) {
                Table::new()
            } else {
                match self.base.tables.get(name) {
                    Some(arc_any) => arc_any
                        .downcast_ref::<Table<R>>()
                        .ok_or_else(|| Error::TypeMismatch(name.to_string()))?
                        .clone(), // O(1) BTree root Arc clone
                    None => Table::new(),
                }
            };
            self.deleted_tables.remove(name);
            self.dirty.insert(name.to_string(), Box::new(table));
        }
        self.dirty
            .get_mut(name)
            .unwrap()
            .downcast_mut::<Table<R>>()
            .ok_or_else(|| Error::TypeMismatch(name.to_string()))
    }

    /// Commit this transaction, creating a new snapshot in the store.
    ///
    /// Returns the version number of the new snapshot.
    pub fn commit(self) -> Result<u64> {
        // Build the new table map: start with clones of all base tables
        // (each Arc clone is O(1)), then overwrite with dirty copies.
        let mut new_tables: HashMap<String, Arc<dyn Any>> = self
            .base
            .tables
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();

        for (name, boxed) in self.dirty {
            new_tables.insert(name, Arc::from(boxed));
        }

        for name in &self.deleted_tables {
            new_tables.remove(name);
        }

        let snapshot = Arc::new(Snapshot { version: self.version, tables: new_tables });
        let v = snapshot.version;

        let mut inner = self.store_inner.write().unwrap();
        inner.snapshots.insert(v, snapshot);
        if v > inner.latest_version {
            inner.latest_version = v;
        }
        if inner.config.auto_snapshot_gc {
            gc_inner(&mut inner);
        }

        Ok(v)
    }

    /// Delete a table. Returns `true` if the table existed.
    /// After deletion, `open_table` with the same name creates a fresh empty table.
    pub fn delete_table(&mut self, name: &str) -> bool {
        let existed_in_dirty = self.dirty.remove(name).is_some();
        let existed_in_base = self.base.tables.contains_key(name);
        if existed_in_base {
            self.deleted_tables.insert(name.to_string());
        }
        existed_in_dirty || existed_in_base
    }

    /// Returns the names of all tables visible in this transaction.
    pub fn table_names(&self) -> Vec<String> {
        let mut names: std::collections::HashSet<String> = self.base.tables.keys().cloned().collect();
        for name in self.dirty.keys() {
            names.insert(name.clone());
        }
        for name in &self.deleted_tables {
            names.remove(name);
        }
        let mut result: Vec<String> = names.into_iter().collect();
        result.sort();
        result
    }

    /// Discards this transaction without modifying the store.
    pub fn rollback(self) {
        // Dropping self is sufficient; no store mutation needed.
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Store tests ---

    #[test]
    fn new_store_has_version_zero() {
        let store = Store::default();
        assert_eq!(store.latest_version(), 0);
    }

    #[test]
    fn begin_read_none_returns_version_zero_on_fresh_store() {
        let store = Store::default();
        let rtx = store.begin_read(None).unwrap();
        assert_eq!(rtx.version(), 0);
    }

    #[test]
    fn begin_read_nonexistent_version_errors() {
        let store = Store::default();
        assert!(matches!(store.begin_read(Some(99)), Err(Error::VersionNotFound(99))));
    }

    #[test]
    fn begin_write_none_assigns_version_1() {
        let store = Store::default();
        let wtx = store.begin_write(None).unwrap();
        assert_eq!(wtx.version(), 1);
    }

    #[test]
    fn begin_write_explicit_version_gt_latest_succeeds() {
        let store = Store::default();
        let wtx = store.begin_write(Some(5)).unwrap();
        assert_eq!(wtx.version(), 5);
    }

    #[test]
    fn begin_write_explicit_version_equal_latest_is_conflict() {
        let store = Store::default(); // latest = 0
        assert!(matches!(store.begin_write(Some(0)), Err(Error::WriteConflict)));
    }

    #[test]
    fn begin_write_explicit_version_less_than_latest_is_conflict() {
        let store = Store::default();
        // Commit version 3 first
        let wtx = store.begin_write(Some(3)).unwrap();
        wtx.commit().unwrap();
        // Now latest = 3; requesting version 2 should conflict
        assert!(matches!(store.begin_write(Some(2)), Err(Error::WriteConflict)));
    }

    #[test]
    fn commit_updates_latest_version() {
        let store = Store::default();
        let wtx = store.begin_write(None).unwrap();
        let v = wtx.commit().unwrap();
        assert_eq!(v, 1);
        assert_eq!(store.latest_version(), 1);
    }

    #[test]
    fn rollback_does_not_change_store() {
        let store = Store::default();
        let wtx = store.begin_write(None).unwrap();
        wtx.rollback();
        assert_eq!(store.latest_version(), 0);
    }

    // --- ReadTx tests ---

    #[test]
    fn read_tx_open_nonexistent_table_errors() {
        let store = Store::default();
        let rtx = store.begin_read(None).unwrap();
        assert!(matches!(rtx.open_table::<String>("nope"), Err(Error::KeyNotFound)));
    }

    #[test]
    fn read_tx_open_table_type_mismatch_errors() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("hi".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let rtx = store.begin_read(None).unwrap();
        assert!(matches!(rtx.open_table::<u64>("t"), Err(Error::TypeMismatch(_))));
    }

    // --- WriteTx tests ---

    #[test]
    fn write_tx_open_new_table_creates_empty() {
        let store = Store::default();
        let mut wtx = store.begin_write(None).unwrap();
        let table = wtx.open_table::<String>("new").unwrap();
        assert!(table.is_empty());
    }

    #[test]
    fn write_tx_open_existing_table_sees_base_data() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("notes").unwrap().insert("hello".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx2 = store.begin_write(None).unwrap();
        let table = wtx2.open_table::<String>("notes").unwrap();
        assert_eq!(table.get(1), Some(&"hello".to_string()));
    }

    #[test]
    fn write_tx_mutations_invisible_to_concurrent_read_tx() {
        let store = Store::default();
        let rtx = store.begin_read(None).unwrap(); // snapshot v0
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("secret".to_string()).unwrap();
        // Do NOT commit yet — rtx should still see empty
        assert!(matches!(rtx.open_table::<String>("t"), Err(Error::KeyNotFound)));
        wtx.rollback();
    }

    #[test]
    fn write_tx_commit_makes_data_visible_to_new_read_tx() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("msgs").unwrap().insert("hello".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let rtx = store.begin_read(None).unwrap();
        assert_eq!(rtx.open_table::<String>("msgs").unwrap().get(1), Some(&"hello".to_string()));
    }

    #[test]
    fn write_tx_type_mismatch_on_dirty_reopen() {
        let store = Store::default();
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap();
        assert!(matches!(wtx.open_table::<u64>("t"), Err(Error::TypeMismatch(_))));
        wtx.rollback();
    }

    #[test]
    fn rollback_leaves_store_at_prior_version() {
        let store = Store::default();
        let wtx = store.begin_write(None).unwrap();
        wtx.rollback();
        assert_eq!(store.latest_version(), 0);
    }

    #[test]
    fn gc_removes_old_snapshots_except_latest_and_active_rtx() {
        let store = Store::new(StoreConfig {
            num_snapshots_retained: 1,
            auto_snapshot_gc: false,
            ..StoreConfig::default()
        });
        // Commit v1
        store.begin_write(None).unwrap().commit().unwrap();
        // Commit v2
        store.begin_write(None).unwrap().commit().unwrap();
        // Current snapshots: 0, 1, 2. Latest is 2.
        assert_eq!(store.snapshot_count(), 3);

        // Open read tx on v1
        let rtx1 = store.begin_read(Some(1)).unwrap();

        // Run GC
        store.gc();

        // Should keep 2 (latest, within N=1) and 1 (referenced by rtx1). Snapshot 0 should be gone.
        assert_eq!(store.snapshot_count(), 2);
        assert!(store.has_snapshot(2));
        assert!(store.has_snapshot(1));
        assert!(!store.has_snapshot(0));

        drop(rtx1);
        store.gc();
        // Now snapshot 1 should be gone, only 2 remains.
        assert_eq!(store.snapshot_count(), 1);
        assert!(store.has_snapshot(2));
    }

    #[test]
    fn gc_retains_n_most_recent_snapshots() {
        let store = Store::new(StoreConfig {
            num_snapshots_retained: 2,
            auto_snapshot_gc: false,
            ..StoreConfig::default()
        });
        // Commit v1, v2, v3
        store.begin_write(None).unwrap().commit().unwrap();
        store.begin_write(None).unwrap().commit().unwrap();
        store.begin_write(None).unwrap().commit().unwrap();
        // Snapshots: 0, 1, 2, 3. Latest is 3.
        assert_eq!(store.snapshot_count(), 4);

        store.gc();
        // Should keep 2 most recent: 2, 3. Snapshots 0 and 1 dropped.
        assert_eq!(store.snapshot_count(), 2);
        assert!(store.has_snapshot(2));
        assert!(store.has_snapshot(3));
    }

    #[test]
    fn gc_retains_snapshots_with_active_readers_beyond_n() {
        let store = Store::new(StoreConfig {
            num_snapshots_retained: 1,
            auto_snapshot_gc: false,
            ..StoreConfig::default()
        });
        // Commit v1, v2
        store.begin_write(None).unwrap().commit().unwrap();
        store.begin_write(None).unwrap().commit().unwrap();

        // Hold a reader on v1
        let _rtx = store.begin_read(Some(1)).unwrap();

        store.gc();
        // Should keep v2 (latest, within N=1) and v1 (active reader)
        assert_eq!(store.snapshot_count(), 2);
        assert!(store.has_snapshot(1));
        assert!(store.has_snapshot(2));
    }

    #[test]
    fn gc_zero_retained_keeps_only_latest_and_active() {
        let store = Store::new(StoreConfig {
            num_snapshots_retained: 0,
            auto_snapshot_gc: false,
            ..StoreConfig::default()
        });
        store.begin_write(None).unwrap().commit().unwrap();
        store.begin_write(None).unwrap().commit().unwrap();
        // Snapshots: 0, 1, 2
        assert_eq!(store.snapshot_count(), 3);

        store.gc();
        // num_snapshots_retained=0 but latest is always kept
        assert_eq!(store.snapshot_count(), 1);
        assert!(store.has_snapshot(2));
    }

    #[test]
    fn auto_gc_on_commit() {
        let store = Store::new(StoreConfig {
            num_snapshots_retained: 2,
            auto_snapshot_gc: true,
        });
        // Commit 5 versions
        for _ in 0..5 {
            store.begin_write(None).unwrap().commit().unwrap();
        }
        // Auto GC should have pruned to 2 most recent: v4, v5
        assert_eq!(store.snapshot_count(), 2);
        assert!(store.has_snapshot(4));
        assert!(store.has_snapshot(5));
    }

    #[test]
    fn auto_gc_disabled() {
        let store = Store::new(StoreConfig {
            num_snapshots_retained: 2,
            auto_snapshot_gc: false,
        });
        for _ in 0..5 {
            store.begin_write(None).unwrap().commit().unwrap();
        }
        // No auto GC — all 6 snapshots remain (v0..v5)
        assert_eq!(store.snapshot_count(), 6);
    }
}
