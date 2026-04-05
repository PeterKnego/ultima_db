// Arc<Snapshot> is intentionally non-Send+Sync for now. Task 4 will add
// Send + Sync bounds when multi-threaded access is required.
#![allow(clippy::arc_with_non_send_sync)]

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::table::Table;
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
// Store — manages version history
// ---------------------------------------------------------------------------

/// An in-memory store with MVCC snapshot isolation.
///
/// Every committed write produces a new numbered snapshot stored in the
/// version history.  [`ReadTx`] borrows a snapshot by `Arc`, keeping it alive
/// independently of the store.  [`WriteTx`] works on a mutable copy of the
/// tables it touches and atomically publishes a new snapshot on [`WriteTx::commit`].
pub struct Store {
    /// All committed snapshots keyed by version. Version 0 = empty store.
    snapshots: HashMap<u64, Arc<Snapshot>>,
    latest_version: u64,
    /// Next auto-assigned write version. Always > `latest_version`.
    next_version: u64,
}

impl Store {
    /// Creates a new, empty store. The initial version is 0.
    pub fn new() -> Self {
        let empty =
            Arc::new(Snapshot { version: 0, tables: HashMap::new() });
        let mut snapshots = HashMap::new();
        snapshots.insert(0, empty);
        Self { snapshots, latest_version: 0, next_version: 1 }
    }

    /// The version number of the most recently committed snapshot.
    pub fn latest_version(&self) -> u64 {
        self.latest_version
    }

    /// Open a read transaction at `version` (latest if `None`).
    ///
    /// Returns [`Error::KeyNotFound`] if the requested version does not exist.
    pub fn begin_read(&self, version: Option<u64>) -> Result<ReadTx> {
        let v = version.unwrap_or(self.latest_version);
        let snapshot = self
            .snapshots
            .get(&v)
            .ok_or(Error::KeyNotFound)?
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
    pub fn begin_write(&mut self, version: Option<u64>) -> Result<WriteTx> {
        let commit_version = match version {
            None => self.next_version,
            Some(v) if v > self.latest_version => v,
            Some(_) => return Err(Error::WriteConflict),
        };
        // Keep next_version ahead of any explicitly requested version.
        if commit_version >= self.next_version {
            self.next_version = commit_version + 1;
        }
        let base = self.snapshots[&self.latest_version].clone();
        Ok(WriteTx { base, dirty: HashMap::new(), version: commit_version })
    }

    /// Register a committed snapshot. Called by [`WriteTx::commit`].
    pub(crate) fn commit_snapshot(&mut self, snapshot: Arc<Snapshot>) {
        let v = snapshot.version;
        self.snapshots.insert(v, snapshot);
        if v > self.latest_version {
            self.latest_version = v;
        }
    }

    /// Garbage collect old snapshots that are no longer referenced by any [`ReadTx`].
    /// Always keeps the latest committed snapshot.
    pub fn gc(&mut self) {
        let latest = self.latest_version;
        self.snapshots.retain(|&v, snapshot| {
            v == latest || Arc::strong_count(snapshot) > 1
        });
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
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

impl ReadTx {
    /// The version number this transaction reads from.
    pub fn version(&self) -> u64 {
        self.snapshot.version
    }

    /// Borrow a table from this snapshot.
    ///
    /// Returns [`Error::KeyNotFound`] if the table does not exist in this
    /// snapshot, or [`Error::TypeMismatch`] if it was created with a different
    /// record type.
    pub fn open_table<R: 'static>(&self, name: &str) -> Result<&Table<R>> {
        self.snapshot
            .tables
            .get(name)
            .ok_or(Error::KeyNotFound)?
            .downcast_ref::<Table<R>>()
            .ok_or_else(|| Error::TypeMismatch(name.to_string()))
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
    pub fn open_table<R: Send + Sync + 'static>(&mut self, name: &str) -> Result<&mut Table<R>> {
        if !self.dirty.contains_key(name) {
            let table: Table<R> = match self.base.tables.get(name) {
                Some(arc_any) => arc_any
                    .downcast_ref::<Table<R>>()
                    .ok_or_else(|| Error::TypeMismatch(name.to_string()))?
                    .clone(), // O(1) BTree root Arc clone
                None => Table::new(),
            };
            self.dirty.insert(name.to_string(), Box::new(table));
        }
        self.dirty
            .get_mut(name)
            .unwrap()
            .downcast_mut::<Table<R>>()
            .ok_or_else(|| Error::TypeMismatch(name.to_string()))
    }

    /// Commit this transaction, creating a new snapshot in `store`.
    ///
    /// Returns the version number of the new snapshot.
    pub fn commit(self, store: &mut Store) -> Result<u64> {
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

        let snapshot = Arc::new(Snapshot { version: self.version, tables: new_tables });
        store.commit_snapshot(snapshot);
        Ok(self.version)
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
        let store = Store::new();
        assert_eq!(store.latest_version(), 0);
    }

    #[test]
    fn begin_read_none_returns_version_zero_on_fresh_store() {
        let store = Store::new();
        let rtx = store.begin_read(None).unwrap();
        assert_eq!(rtx.version(), 0);
    }

    #[test]
    fn begin_read_nonexistent_version_errors() {
        let store = Store::new();
        assert!(matches!(store.begin_read(Some(99)), Err(Error::KeyNotFound)));
    }

    #[test]
    fn begin_write_none_assigns_version_1() {
        let mut store = Store::new();
        let wtx = store.begin_write(None).unwrap();
        assert_eq!(wtx.version(), 1);
    }

    #[test]
    fn begin_write_explicit_version_gt_latest_succeeds() {
        let mut store = Store::new();
        let wtx = store.begin_write(Some(5)).unwrap();
        assert_eq!(wtx.version(), 5);
    }

    #[test]
    fn begin_write_explicit_version_equal_latest_is_conflict() {
        let mut store = Store::new(); // latest = 0
        assert!(matches!(store.begin_write(Some(0)), Err(Error::WriteConflict)));
    }

    #[test]
    fn begin_write_explicit_version_less_than_latest_is_conflict() {
        let mut store = Store::new();
        // Commit version 3 first
        let wtx = store.begin_write(Some(3)).unwrap();
        wtx.commit(&mut store).unwrap();
        // Now latest = 3; requesting version 2 should conflict
        assert!(matches!(store.begin_write(Some(2)), Err(Error::WriteConflict)));
    }

    #[test]
    fn commit_updates_latest_version() {
        let mut store = Store::new();
        let wtx = store.begin_write(None).unwrap();
        let v = wtx.commit(&mut store).unwrap();
        assert_eq!(v, 1);
        assert_eq!(store.latest_version(), 1);
    }

    #[test]
    fn rollback_does_not_change_store() {
        let mut store = Store::new();
        let wtx = store.begin_write(None).unwrap();
        wtx.rollback();
        assert_eq!(store.latest_version(), 0);
    }

    // --- ReadTx tests ---

    #[test]
    fn read_tx_open_nonexistent_table_errors() {
        let store = Store::new();
        let rtx = store.begin_read(None).unwrap();
        assert!(matches!(rtx.open_table::<String>("nope"), Err(Error::KeyNotFound)));
    }

    #[test]
    fn read_tx_open_table_type_mismatch_errors() {
        let mut store = Store::new();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("hi".to_string()).unwrap();
            wtx.commit(&mut store).unwrap();
        }
        let rtx = store.begin_read(None).unwrap();
        assert!(matches!(rtx.open_table::<u64>("t"), Err(Error::TypeMismatch(_))));
    }

    // --- WriteTx tests ---

    #[test]
    fn write_tx_open_new_table_creates_empty() {
        let mut store = Store::new();
        let mut wtx = store.begin_write(None).unwrap();
        let table = wtx.open_table::<String>("new").unwrap();
        assert!(table.is_empty());
    }

    #[test]
    fn write_tx_open_existing_table_sees_base_data() {
        let mut store = Store::new();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("notes").unwrap().insert("hello".to_string()).unwrap();
            wtx.commit(&mut store).unwrap();
        }
        let mut wtx2 = store.begin_write(None).unwrap();
        let table = wtx2.open_table::<String>("notes").unwrap();
        assert_eq!(table.get(1), Some(&"hello".to_string()));
    }

    #[test]
    fn write_tx_mutations_invisible_to_concurrent_read_tx() {
        let mut store = Store::new();
        let rtx = store.begin_read(None).unwrap(); // snapshot v0
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("secret".to_string()).unwrap();
        // Do NOT commit yet — rtx should still see empty
        assert!(matches!(rtx.open_table::<String>("t"), Err(Error::KeyNotFound)));
        wtx.rollback();
    }

    #[test]
    fn write_tx_commit_makes_data_visible_to_new_read_tx() {
        let mut store = Store::new();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("msgs").unwrap().insert("hello".to_string()).unwrap();
            wtx.commit(&mut store).unwrap();
        }
        let rtx = store.begin_read(None).unwrap();
        assert_eq!(rtx.open_table::<String>("msgs").unwrap().get(1), Some(&"hello".to_string()));
    }

    #[test]
    fn write_tx_type_mismatch_on_dirty_reopen() {
        let mut store = Store::new();
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap();
        assert!(matches!(wtx.open_table::<u64>("t"), Err(Error::TypeMismatch(_))));
        wtx.rollback();
    }

    #[test]
    fn rollback_leaves_store_at_prior_version() {
        let mut store = Store::new();
        let wtx = store.begin_write(None).unwrap();
        wtx.rollback();
        assert_eq!(store.latest_version(), 0);
    }

    #[test]
    fn gc_removes_old_snapshots_except_latest_and_active_rtx() {
        let mut store = Store::new();
        // Commit v1
        store.begin_write(None).unwrap().commit(&mut store).unwrap();
        // Commit v2
        store.begin_write(None).unwrap().commit(&mut store).unwrap();
        // Current snapshots: 0, 1, 2. Latest is 2.
        assert_eq!(store.snapshots.len(), 3);

        // Open read tx on v1
        let rtx1 = store.begin_read(Some(1)).unwrap();
        
        // Run GC
        store.gc();

        // Should keep 2 (latest) and 1 (referenced by rtx1). Snapshot 0 should be gone.
        assert_eq!(store.snapshots.len(), 2);
        assert!(store.snapshots.contains_key(&2));
        assert!(store.snapshots.contains_key(&1));
        assert!(!store.snapshots.contains_key(&0));

        drop(rtx1);
        store.gc();
        // Now snapshot 1 should be gone, only 2 remains.
        assert_eq!(store.snapshots.len(), 1);
        assert!(store.snapshots.contains_key(&2));
    }
}
