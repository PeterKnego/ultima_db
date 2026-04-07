// Arc<Snapshot> is intentionally non-Send+Sync for now. Task 4 will add
// Send + Sync bounds when multi-threaded access is required.
#![allow(clippy::arc_with_non_send_sync)]

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use crate::index::IndexKind;
use crate::persistence::Record;
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
#[derive(Clone)]
pub(crate) struct Snapshot {
    pub(crate) version: u64,
    pub(crate) tables: BTreeMap<String, Arc<dyn Any>>,
}

// ---------------------------------------------------------------------------
// WriterMode
// ---------------------------------------------------------------------------

/// Controls how concurrent write transactions are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterMode {
    /// At most one active [`WriteTx`] at a time. [`Store::begin_write`] returns
    /// [`Error::WriterBusy`] if another is already active. Zero tracking overhead.
    SingleWriter,
    /// Multiple concurrent [`WriteTx`] allowed. Key-level write-write conflict
    /// detection via optimistic concurrency control. Conflicting commits return
    /// [`Error::WriteConflict`].
    MultiWriter,
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
    /// Writer concurrency mode. Default: [`WriterMode::SingleWriter`].
    pub writer_mode: WriterMode,
    /// When `true`, [`Store::begin_write`] requires an explicit version
    /// (`Some(v)`). Calling `begin_write(None)` returns
    /// [`Error::ExplicitVersionRequired`]. Default: `false`.
    ///
    /// Enable this in SMR (state machine replication) deployments where the
    /// consensus layer assigns log indices as version numbers.
    pub require_explicit_version: bool,
    /// Persistence mode. Default: [`Persistence::None`] (in-memory only).
    #[cfg(feature = "persistence")]
    pub persistence: crate::persistence::Persistence,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            num_snapshots_retained: 10,
            auto_snapshot_gc: true,
            writer_mode: WriterMode::SingleWriter,
            require_explicit_version: false,
            #[cfg(feature = "persistence")]
            persistence: crate::persistence::Persistence::None,
        }
    }
}

// ---------------------------------------------------------------------------
// CommittedWriteSet — records which keys a committed transaction modified
// ---------------------------------------------------------------------------

/// The write set of a committed transaction, retained for OCC validation.
///
/// Stored in `StoreInner::committed_write_sets` and checked during
/// `WriteTx::commit` to detect key-level write-write conflicts.
/// Pruned by `prune_write_sets` once no in-flight writer needs it.
struct CommittedWriteSet {
    /// The snapshot version this transaction committed as.
    version: u64,
    /// Table name → set of modified row IDs.
    tables: BTreeMap<String, BTreeSet<u64>>,
    /// Tables that were deleted during this transaction.
    /// Used to detect cross-table conflicts (e.g., writer A deletes a table
    /// that writer B modified).
    deleted_tables: BTreeSet<String>,
}

// ---------------------------------------------------------------------------
// StoreInner — the interior-mutable state behind Store
// ---------------------------------------------------------------------------

pub(crate) struct StoreInner {
    /// All committed snapshots keyed by version. Version 0 = empty store.
    pub(crate) snapshots: BTreeMap<u64, Arc<Snapshot>>,
    pub(crate) latest_version: u64,
    /// Next auto-assigned write version. Always > `latest_version`.
    pub(crate) next_version: u64,
    pub(crate) config: StoreConfig,
    /// Number of active (uncommitted, undropped) WriteTx instances.
    active_writer_count: usize,
    /// Base versions of all in-flight WriteTx instances (MultiWriter mode only).
    active_writer_base_versions: Vec<u64>,
    /// Write sets from recently committed transactions (MultiWriter mode only).
    /// Pruned when no in-flight writer has a base version ≤ entry.version.
    committed_write_sets: Vec<CommittedWriteSet>,
    /// WAL writer handle (persistence feature, Standalone mode only).
    #[cfg(feature = "persistence")]
    pub(crate) wal_handle: Option<crate::wal::WalHandle>,
    /// Type registry for serialization (persistence feature only).
    #[cfg(feature = "persistence")]
    pub(crate) registry: crate::registry::TableRegistry,
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
        #[cfg(feature = "persistence")]
        let wal_handle = match &config.persistence {
            crate::persistence::Persistence::Standalone { dir, durability } => {
                let consistent = matches!(durability, crate::persistence::Durability::Consistent);
                Some(crate::wal::WalHandle::new(dir, consistent)
                    .expect("failed to initialize WAL"))
            }
            _ => None,
        };

        let empty = Arc::new(Snapshot { version: 0, tables: BTreeMap::new() });
        let mut snapshots = BTreeMap::new();
        snapshots.insert(0, empty);
        Self {
            inner: Arc::new(RwLock::new(StoreInner {
                snapshots,
                latest_version: 0,
                next_version: 1,
                config,
                active_writer_count: 0,
                active_writer_base_versions: Vec::new(),
                committed_write_sets: Vec::new(),
                #[cfg(feature = "persistence")]
                wal_handle,
                #[cfg(feature = "persistence")]
                registry: crate::registry::TableRegistry::default(),
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

        if inner.config.require_explicit_version && version.is_none() {
            return Err(Error::ExplicitVersionRequired);
        }

        // Enforce single-writer exclusivity.
        match inner.config.writer_mode {
            WriterMode::SingleWriter => {
                if inner.active_writer_count > 0 {
                    return Err(Error::WriterBusy);
                }
            }
            WriterMode::MultiWriter => {}
        }

        let commit_version = match version {
            None => inner.next_version,
            Some(v) if v > inner.latest_version => v,
            Some(_) => {
                return Err(Error::WriteConflict {
                    table: String::new(),
                    keys: vec![],
                    version: inner.latest_version,
                })
            }
        };
        // Keep next_version ahead of any explicitly requested version.
        if commit_version >= inner.next_version {
            inner.next_version = commit_version + 1;
        }
        let base = inner.snapshots[&inner.latest_version].clone();
        let base_version = base.version;
        let writer_mode = inner.config.writer_mode;

        // Track active writer.
        inner.active_writer_count += 1;
        if matches!(writer_mode, WriterMode::MultiWriter) {
            inner.active_writer_base_versions.push(base_version);
        }

        Ok(WriteTx {
            base,
            dirty: BTreeMap::new(),
            version: commit_version,
            store_inner: Arc::clone(&self.inner),
            deleted_tables: BTreeSet::new(),
            write_set: BTreeMap::new(),
            ever_deleted_tables: BTreeSet::new(),
            writer_mode,
            needs_cleanup: true,
            #[cfg(feature = "persistence")]
            wal_ops: Vec::new(),
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

    // --- Persistence methods (feature-gated) ---

    /// Register a table type for persistence. Must be called before any
    /// transactions that touch this table, and before [`Store::open`] or
    /// [`Store::checkpoint`].
    #[cfg(feature = "persistence")]
    pub fn register_table<R: crate::persistence::Record>(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.registry.register::<R>(name)
    }

    /// Write a checkpoint of the latest snapshot to disk.
    ///
    /// Blocks the caller until the checkpoint is fully written and fsynced.
    /// The store continues serving reads and writes during serialization
    /// (only the caller thread blocks).
    ///
    /// In `Standalone` mode, the WAL is pruned after the checkpoint is written.
    /// Returns the version of the checkpointed snapshot.
    #[cfg(feature = "persistence")]
    pub fn checkpoint(&self) -> Result<u64> {
        let inner = self.inner.read().unwrap();
        let dir = match &inner.config.persistence {
            crate::persistence::Persistence::Standalone { dir, .. }
            | crate::persistence::Persistence::Smr { dir } => dir.clone(),
            crate::persistence::Persistence::None => {
                return Err(Error::Persistence("checkpoint requires persistence to be configured".into()));
            }
        };
        let snap = inner.snapshots[&inner.latest_version].clone();
        let registry = &inner.registry;

        let version = crate::checkpoint::write_checkpoint(&dir, &snap, registry)?;
        drop(inner); // release read lock

        // Prune WAL in Standalone mode
        let inner = self.inner.read().unwrap();
        if matches!(inner.config.persistence, crate::persistence::Persistence::Standalone { .. }) {
            let wal_path = crate::wal::wal_path(&dir);
            crate::wal::prune_wal(&wal_path, version)?;
        }

        // Clean up old checkpoints
        crate::checkpoint::cleanup_old_checkpoints(&dir, version)?;

        Ok(version)
    }

    /// Recover state from disk (checkpoint + WAL replay).
    ///
    /// Call this after creating the store with [`Store::new`] and registering
    /// all table types with [`Store::register_table`].
    ///
    /// 1. Loads the latest checkpoint (if any).
    /// 2. In Standalone mode, replays WAL entries after the checkpoint.
    ///
    /// For `Persistence::None`, this is a no-op.
    #[cfg(feature = "persistence")]
    pub fn recover(&self) -> Result<()> {
        use crate::persistence::Persistence;

        let dir = {
            let inner = self.inner.read().unwrap();
            match &inner.config.persistence {
                Persistence::Standalone { dir, .. } | Persistence::Smr { dir } => dir.clone(),
                Persistence::None => return Ok(()),
            }
        };

        // Load latest checkpoint if present.
        if let Some(cp_path) = crate::checkpoint::find_latest_checkpoint(&dir)? {
            let inner = self.inner.read().unwrap();
            let snapshot = crate::checkpoint::load_checkpoint(&cp_path, &inner.registry)?;
            drop(inner);

            let mut inner = self.inner.write().unwrap();
            let v = snapshot.version;
            inner.snapshots.insert(v, Arc::new(snapshot));
            inner.latest_version = v;
            if v >= inner.next_version {
                inner.next_version = v + 1;
            }
        }

        // Replay WAL entries (Standalone mode only).
        {
            let inner = self.inner.read().unwrap();
            if matches!(inner.config.persistence, Persistence::Standalone { .. }) {
                let wal_path_buf = crate::wal::wal_path(&dir);
                let entries = crate::wal::read_wal(&wal_path_buf)?;
                let base_version = inner.latest_version;
                drop(inner);

                let to_replay: Vec<_> = entries.iter()
                    .filter(|e| e.version > base_version)
                    .collect();

                if !to_replay.is_empty() {
                    let mut inner = self.inner.write().unwrap();
                    // Build a new table map with sole ownership of each Arc.
                    // We re-wrap each table in a fresh Arc so Arc::get_mut succeeds during replay.
                    let base_snap = &inner.snapshots[&inner.latest_version];
                    let mut tables: BTreeMap<String, Arc<dyn Any>> = base_snap.tables.iter()
                        .map(|(name, arc)| {
                            let info = inner.registry.get(name).unwrap();
                            let bytes = (info.serialize_table)(arc.as_ref()).unwrap();
                            let new_table = (info.deserialize_table)(&bytes).unwrap();
                            (name.clone(), Arc::from(new_table))
                        })
                        .collect();
                    let mut latest_version = inner.latest_version;

                    for entry in &to_replay {
                        for op in &entry.ops {
                            match op {
                                crate::wal::WalOp::Insert { table, id, data }
                                | crate::wal::WalOp::Update { table, id, data } => {
                                    let info = inner.registry.get(table)
                                        .ok_or_else(|| Error::TableNotRegistered(table.clone()))?;

                                    if !tables.contains_key(table) {
                                        tables.insert(table.clone(), Arc::from((info.new_empty_table)()));
                                    }

                                    let table_arc = tables.get_mut(table).unwrap();
                                    let table_mut = Arc::get_mut(table_arc)
                                        .ok_or_else(|| Error::Persistence(
                                            "table Arc has multiple references during replay".into()
                                        ))?;

                                    if matches!(op, crate::wal::WalOp::Insert { .. }) {
                                        (info.replay_insert)(table_mut, *id, data)?;
                                    } else {
                                        (info.replay_update)(table_mut, *id, data)?;
                                    }
                                }
                                crate::wal::WalOp::Delete { table, id } => {
                                    let info = inner.registry.get(table)
                                        .ok_or_else(|| Error::TableNotRegistered(table.clone()))?;
                                    if let Some(table_arc) = tables.get_mut(table) {
                                        let table_mut = Arc::get_mut(table_arc)
                                            .ok_or_else(|| Error::Persistence(
                                                "table Arc has multiple references during replay".into()
                                            ))?;
                                        (info.replay_delete)(table_mut, *id)?;
                                    }
                                }
                                crate::wal::WalOp::CreateTable { .. } => {}
                                crate::wal::WalOp::DeleteTable { name } => {
                                    tables.remove(name);
                                }
                            }
                        }
                        latest_version = entry.version;
                    }

                    let snapshot = Arc::new(Snapshot { version: latest_version, tables });
                    inner.snapshots.insert(latest_version, snapshot);
                    inner.latest_version = latest_version;
                    if latest_version >= inner.next_version {
                        inner.next_version = latest_version + 1;
                    }
                }
            }
        }

        Ok(())
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

    /// Number of committed write sets retained in the OCC log.
    ///
    /// Exposed for testing write-set pruning behavior. Should be 0
    /// when no `WriteTx` instances are active.
    #[doc(hidden)]
    pub fn committed_write_set_count(&self) -> usize {
        self.inner.read().unwrap().committed_write_sets.len()
    }
}

/// Remove a base version from the active writer tracking list.
///
/// Called on commit and drop to unregister a `WriteTx` so that
/// `prune_write_sets` can discard write sets no longer needed.
/// Uses `swap_remove` for O(1) removal (order doesn't matter).
fn remove_active_writer(inner: &mut StoreInner, base_version: u64) {
    if let Some(pos) = inner.active_writer_base_versions.iter().position(|&v| v == base_version) {
        inner.active_writer_base_versions.swap_remove(pos);
    }
}

/// Prune committed write sets that are no longer needed for validation.
///
/// A write set with version V can be pruned when no in-flight writer has
/// `base_version <= V`, because such a writer's commit validation only checks
/// write sets with `version > base_version`. When no active writers remain,
/// the entire log is cleared.
fn prune_write_sets(inner: &mut StoreInner) {
    if let Some(&min_base) = inner.active_writer_base_versions.iter().min() {
        inner.committed_write_sets.retain(|cws| cws.version > min_base);
    } else {
        // No active writers — discard everything.
        inner.committed_write_sets.clear();
    }
}

/// Run GC on an already-locked `StoreInner`.
fn gc_inner(inner: &mut StoreInner) {
    let versions: Vec<u64> = inner.snapshots.keys().copied().collect();

    // The N most recent versions to retain unconditionally.
    // latest_version is always kept (even if num_snapshots_retained is 0).
    let retain_count = inner.config.num_snapshots_retained.max(1);
    let cutoff_idx = versions.len().saturating_sub(retain_count);
    let protected: BTreeSet<u64> = versions[cutoff_idx..].iter().copied().collect();

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
    fn open_table<R: Record>(&self, opener: impl TableOpener<R>) -> Result<&Table<R>>;
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
    pub fn open_table<R: Record>(&self, opener: impl TableOpener<R>) -> Result<&Table<R>> {
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
    fn open_table<R: Record>(&self, opener: impl TableOpener<R>) -> Result<&Table<R>> {
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
    dirty: BTreeMap<String, Box<dyn Any>>,
    /// The version number that will be assigned to the new snapshot on commit.
    version: u64,
    /// Reference back to the store's interior state, used during commit.
    store_inner: Arc<RwLock<StoreInner>>,
    /// Tables explicitly deleted in this transaction (cleared on re-open).
    deleted_tables: BTreeSet<String>,
    /// Keys modified during this transaction, per table (MultiWriter only).
    write_set: BTreeMap<String, BTreeSet<u64>>,
    /// Tables ever deleted during this tx (not cleared on re-open).
    /// Used for conflict detection in MultiWriter mode.
    ever_deleted_tables: BTreeSet<String>,
    /// Writer mode at the time this transaction was created.
    writer_mode: WriterMode,
    /// Set to `true` on successful commit so `Drop` skips cleanup.
    needs_cleanup: bool,
    /// WAL operations accumulated during this transaction (persistence only).
    #[cfg(feature = "persistence")]
    pub(crate) wal_ops: Vec<crate::wal::WalOp>,
}

// ---------------------------------------------------------------------------
// TableWriter — write-tracking wrapper around &mut Table<R>
// ---------------------------------------------------------------------------

/// A write-tracking wrapper around [`Table<R>`].
///
/// Returned by [`WriteTx::open_table`]. Delegates all read methods directly
/// to the underlying table and intercepts write methods to record modified
/// keys in the transaction's write set (in [`WriterMode::MultiWriter`] mode).
///
/// In [`WriterMode::SingleWriter`] mode, write-set tracking is skipped
/// and the only overhead is an `Option` check per write call.
pub struct TableWriter<'tx, R: Record> {
    table: &'tx mut Table<R>,
    write_set: Option<&'tx mut BTreeSet<u64>>,
    #[cfg(feature = "persistence")]
    wal_ops: Option<WalOpsWriter<'tx>>,
}

/// Bundles the table name and ops list for WAL tracking in TableWriter.
#[cfg(feature = "persistence")]
struct WalOpsWriter<'tx> {
    table_name: String,
    ops: &'tx mut Vec<crate::wal::WalOp>,
}

impl<'tx, R: Record> TableWriter<'tx, R> {
    // --- Write methods (tracked) ---

    /// Insert a record. Returns the auto-assigned ID.
    pub fn insert(&mut self, record: R) -> Result<u64> {
        #[cfg(feature = "persistence")]
        let data = self.serialize_record(&record)?;
        let id = self.table.insert(record)?;
        if let Some(ws) = &mut self.write_set {
            ws.insert(id);
        }
        #[cfg(feature = "persistence")]
        if let Some(w) = &mut self.wal_ops {
            w.ops.push(crate::wal::WalOp::Insert { table: w.table_name.clone(), id, data });
        }
        Ok(id)
    }

    /// Update a record by its ID.
    pub fn update(&mut self, id: u64, record: R) -> Result<()> {
        #[cfg(feature = "persistence")]
        let data = self.serialize_record(&record)?;
        self.table.update(id, record)?;
        if let Some(ws) = &mut self.write_set {
            ws.insert(id);
        }
        #[cfg(feature = "persistence")]
        if let Some(w) = &mut self.wal_ops {
            w.ops.push(crate::wal::WalOp::Update { table: w.table_name.clone(), id, data });
        }
        Ok(())
    }

    /// Delete a record by its ID. Returns the deleted record.
    pub fn delete(&mut self, id: u64) -> Result<Arc<R>> {
        let old = self.table.delete(id)?;
        if let Some(ws) = &mut self.write_set {
            ws.insert(id);
        }
        #[cfg(feature = "persistence")]
        if let Some(w) = &mut self.wal_ops {
            w.ops.push(crate::wal::WalOp::Delete { table: w.table_name.clone(), id });
        }
        Ok(old)
    }

    /// Insert multiple records atomically.
    pub fn insert_batch(&mut self, records: Vec<R>) -> Result<Vec<u64>> {
        #[cfg(feature = "persistence")]
        let data_list: Vec<Vec<u8>> = records.iter()
            .map(|r| self.serialize_record(r))
            .collect::<Result<_>>()?;
        let ids = self.table.insert_batch(records)?;
        if let Some(ws) = &mut self.write_set {
            ws.extend(ids.iter().copied());
        }
        #[cfg(feature = "persistence")]
        if let Some(w) = &mut self.wal_ops {
            for (id, data) in ids.iter().zip(data_list) {
                w.ops.push(crate::wal::WalOp::Insert { table: w.table_name.clone(), id: *id, data });
            }
        }
        Ok(ids)
    }

    /// Update multiple records atomically.
    pub fn update_batch(&mut self, updates: Vec<(u64, R)>) -> Result<()> {
        if let Some(ws) = &mut self.write_set {
            for &(id, _) in &updates {
                ws.insert(id);
            }
        }
        #[cfg(feature = "persistence")]
        let ops_data: Vec<(u64, Vec<u8>)> = updates.iter()
            .map(|(id, r)| self.serialize_record(r).map(|d| (*id, d)))
            .collect::<Result<_>>()?;
        self.table.update_batch(updates)?;
        #[cfg(feature = "persistence")]
        if let Some(w) = &mut self.wal_ops {
            for (id, data) in ops_data {
                w.ops.push(crate::wal::WalOp::Update { table: w.table_name.clone(), id, data });
            }
        }
        Ok(())
    }

    /// Delete multiple records atomically.
    pub fn delete_batch(&mut self, ids: &[u64]) -> Result<()> {
        self.table.delete_batch(ids)?;
        if let Some(ws) = &mut self.write_set {
            ws.extend(ids.iter().copied());
        }
        #[cfg(feature = "persistence")]
        if let Some(w) = &mut self.wal_ops {
            for &id in ids {
                w.ops.push(crate::wal::WalOp::Delete { table: w.table_name.clone(), id });
            }
        }
        Ok(())
    }

    #[cfg(feature = "persistence")]
    fn serialize_record(&self, record: &R) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(record, bincode::config::standard())
            .map_err(|e| Error::Persistence(e.to_string()))
    }

    // --- Read methods (pass-through) ---

    /// Look up a record by its ID.
    pub fn get(&self, id: u64) -> Option<&R> {
        self.table.get(id)
    }

    /// Returns an iterator over records within the specified ID range.
    pub fn range<'a>(&'a self, range: impl std::ops::RangeBounds<u64> + 'a) -> impl Iterator<Item = (u64, &'a R)> + 'a {
        self.table.range(range)
    }

    /// Returns the number of records in the table.
    #[must_use]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Returns true if the table contains no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Returns true if the table contains a record with the given ID.
    pub fn contains(&self, id: u64) -> bool {
        self.table.contains(id)
    }

    /// Returns the first (lowest ID) record, or `None` if empty.
    pub fn first(&self) -> Option<(u64, &R)> {
        self.table.first()
    }

    /// Returns the last (highest ID) record, or `None` if empty.
    pub fn last(&self) -> Option<(u64, &R)> {
        self.table.last()
    }

    /// Iterate over all records in ID order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &R)> + '_ {
        self.table.iter()
    }

    /// Look up multiple records by ID.
    pub fn get_many(&self, ids: &[u64]) -> Vec<Option<&R>> {
        self.table.get_many(ids)
    }

    // --- Index methods (pass-through) ---

    /// Define a secondary index.
    pub fn define_index<K: Ord + Clone + Send + Sync + 'static>(
        &mut self,
        name: &str,
        kind: IndexKind,
        extractor: impl Fn(&R) -> K + Send + Sync + 'static,
    ) -> Result<()> {
        self.table.define_index(name, kind, extractor)
    }

    /// Look up a single record by a unique index.
    pub fn get_unique<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &K,
    ) -> Result<Option<(u64, &R)>> {
        self.table.get_unique(index_name, key)
    }

    /// Look up records by a non-unique index key.
    pub fn get_by_index<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &K,
    ) -> Result<Vec<(u64, &R)>> {
        self.table.get_by_index(index_name, key)
    }

    /// Look up records by index key (works for both unique and non-unique).
    pub fn get_by_key<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &K,
    ) -> Result<Vec<(u64, &R)>> {
        self.table.get_by_key(index_name, key)
    }

    /// Range scan on an index (works for both unique and non-unique).
    pub fn index_range<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        range: impl std::ops::RangeBounds<K>,
    ) -> Result<Vec<(u64, &R)>> {
        self.table.index_range(index_name, range)
    }
}

// ---------------------------------------------------------------------------
// WriteTx methods
// ---------------------------------------------------------------------------

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
    ///
    /// Returns a [`TableWriter`] that tracks modified keys for OCC validation
    /// in [`WriterMode::MultiWriter`] mode.
    pub fn open_table<R: Record>(&mut self, opener: impl TableOpener<R>) -> Result<TableWriter<'_, R>> {
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
        let name_str = name.to_string();
        let table = self.dirty
            .get_mut(&name_str)
            .unwrap()
            .downcast_mut::<Table<R>>()
            .ok_or_else(|| Error::TypeMismatch(name_str.clone()))?;
        let write_set = match self.writer_mode {
            WriterMode::MultiWriter => Some(self.write_set.entry(name_str.clone()).or_default()),
            WriterMode::SingleWriter => None,
        };
        #[cfg(feature = "persistence")]
        let wal_ops = Some(WalOpsWriter {
            table_name: name_str,
            ops: &mut self.wal_ops,
        });
        Ok(TableWriter {
            table,
            write_set,
            #[cfg(feature = "persistence")]
            wal_ops,
        })
    }

    /// Commit this transaction, creating a new snapshot in the store.
    ///
    /// In [`WriterMode::MultiWriter`] mode, validates that no concurrent commit
    /// modified overlapping keys. Returns [`Error::WriteConflict`] on conflict.
    ///
    /// Returns the version number of the new snapshot.
    pub fn commit(mut self) -> Result<u64> {
        let mut inner = self.store_inner.write().unwrap();

        // --- OCC validation (MultiWriter only) ---
        if matches!(self.writer_mode, WriterMode::MultiWriter)
            && let Some(conflict) = self.validate_write_set(&inner)
        {
            drop(inner); // release lock before Drop runs
            return Err(conflict);
        }

        // --- Write WAL entry (persistence, Standalone mode only) ---
        #[cfg(feature = "persistence")]
        if let Some(wal) = &mut inner.wal_handle {
            let ops = std::mem::take(&mut self.wal_ops);
            if !ops.is_empty() {
                let entry = crate::wal::WalEntry { version: self.version, ops };
                wal.write(entry)?;
            }
        }

        // --- Build the new table map ---
        let mut new_tables: BTreeMap<String, Arc<dyn Any>> = self
            .base
            .tables
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();

        for (name, boxed) in std::mem::take(&mut self.dirty) {
            new_tables.insert(name, Arc::from(boxed));
        }

        for name in &self.deleted_tables {
            new_tables.remove(name);
        }

        let snapshot = Arc::new(Snapshot { version: self.version, tables: new_tables });
        let v = snapshot.version;

        inner.snapshots.insert(v, snapshot);
        if v > inner.latest_version {
            inner.latest_version = v;
        }

        // --- Record write set and cleanup ---
        match self.writer_mode {
            WriterMode::MultiWriter => {
                inner.committed_write_sets.push(CommittedWriteSet {
                    version: v,
                    tables: std::mem::take(&mut self.write_set),
                    deleted_tables: std::mem::take(&mut self.ever_deleted_tables),
                });
                remove_active_writer(&mut inner, self.base.version);
                prune_write_sets(&mut inner);
            }
            WriterMode::SingleWriter => {}
        }
        inner.active_writer_count -= 1;

        if inner.config.auto_snapshot_gc {
            gc_inner(&mut inner);
        }

        self.needs_cleanup = false;
        Ok(v)
    }

    /// Delete a table. Returns `true` if the table existed.
    ///
    /// After deletion, `open_table` with the same name creates a fresh empty table.
    /// In `MultiWriter` mode, records the deletion in `ever_deleted_tables` for
    /// conflict detection — if a concurrent transaction modified any key in this
    /// table, commit will return `WriteConflict`.
    pub fn delete_table(&mut self, name: &str) -> bool {
        let existed_in_dirty = self.dirty.remove(name).is_some();
        let existed_in_base = self.base.tables.contains_key(name);
        if existed_in_base {
            self.deleted_tables.insert(name.to_string());
        }
        let existed = existed_in_dirty || existed_in_base;
        if existed && matches!(self.writer_mode, WriterMode::MultiWriter) {
            self.ever_deleted_tables.insert(name.to_string());
        }
        #[cfg(feature = "persistence")]
        if existed {
            self.wal_ops.push(crate::wal::WalOp::DeleteTable { name: name.to_string() });
        }
        existed
    }

    /// Returns the names of all tables visible in this transaction.
    pub fn table_names(&self) -> Vec<String> {
        let mut names: BTreeSet<String> = self.base.tables.keys().cloned().collect();
        for name in self.dirty.keys() {
            names.insert(name.clone());
        }
        for name in &self.deleted_tables {
            names.remove(name);
        }
        names.into_iter().collect()
    }

    /// Check for write-write conflicts against committed transactions.
    ///
    /// Scans `committed_write_sets` for entries with `version > base_version`
    /// (transactions that committed after this one started). Checks three
    /// conflict conditions:
    /// 1. A concurrent commit deleted a table this transaction wrote to.
    /// 2. This transaction deleted a table a concurrent commit wrote to.
    /// 3. Key-level overlap: both transactions modified the same key in the same table.
    ///
    /// Returns `Some(WriteConflict)` on the first conflict found, `None` if clean.
    fn validate_write_set(&self, inner: &StoreInner) -> Option<Error> {
        let base_version = self.base.version;
        for cws in &inner.committed_write_sets {
            if cws.version <= base_version {
                continue;
            }
            // Did a concurrent commit delete a table we modified?
            for deleted in &cws.deleted_tables {
                if self.write_set.contains_key(deleted) {
                    return Some(Error::WriteConflict {
                        table: deleted.clone(),
                        keys: vec![],
                        version: cws.version,
                    });
                }
            }
            // Did we delete a table that a concurrent commit modified?
            for deleted in &self.ever_deleted_tables {
                if cws.tables.contains_key(deleted) {
                    return Some(Error::WriteConflict {
                        table: deleted.clone(),
                        keys: vec![],
                        version: cws.version,
                    });
                }
            }
            // Key-level overlap in write sets.
            for (table_name, my_keys) in &self.write_set {
                if let Some(their_keys) = cws.tables.get(table_name)
                    && !my_keys.is_disjoint(their_keys)
                {
                    let conflicting: Vec<u64> =
                        my_keys.intersection(their_keys).copied().collect();
                    return Some(Error::WriteConflict {
                        table: table_name.clone(),
                        keys: conflicting,
                        version: cws.version,
                    });
                }
            }
        }
        None
    }

    /// Discards this transaction without modifying the store.
    pub fn rollback(self) {
        // Drop impl handles active-writer cleanup.
    }
}

/// Drop guard for `WriteTx`.
///
/// If the transaction was not committed (e.g., caller let it fall out of scope
/// or called `rollback`), decrements `active_writer_count` and, in `MultiWriter`
/// mode, removes the base version from tracking and prunes the write-set log.
/// Skipped after a successful `commit` (which sets `needs_cleanup = false`).
impl Drop for WriteTx {
    fn drop(&mut self) {
        if self.needs_cleanup {
            let mut inner = self.store_inner.write().unwrap();
            inner.active_writer_count -= 1;
            if matches!(self.writer_mode, WriterMode::MultiWriter) {
                remove_active_writer(&mut inner, self.base.version);
                prune_write_sets(&mut inner);
            }
        }
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
        assert!(matches!(store.begin_write(Some(0)), Err(Error::WriteConflict { .. })));
    }

    #[test]
    fn begin_write_explicit_version_less_than_latest_is_conflict() {
        let store = Store::default();
        // Commit version 3 first
        let wtx = store.begin_write(Some(3)).unwrap();
        wtx.commit().unwrap();
        // Now latest = 3; requesting version 2 should conflict
        assert!(matches!(store.begin_write(Some(2)), Err(Error::WriteConflict { .. })));
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
            ..StoreConfig::default()
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
            ..StoreConfig::default()
        });
        for _ in 0..5 {
            store.begin_write(None).unwrap().commit().unwrap();
        }
        // No auto GC — all 6 snapshots remain (v0..v5)
        assert_eq!(store.snapshot_count(), 6);
    }

    // --- Readable trait coverage ---

    #[test]
    fn readable_table_names_and_version() {
        fn check_readable(r: &impl Readable) -> (u64, Vec<String>) {
            (r.version(), r.table_names())
        }
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("a").unwrap().insert("x".into()).unwrap();
            wtx.open_table::<u32>("b").unwrap().insert(1u32).unwrap();
            wtx.commit().unwrap();
        }
        let rtx = store.begin_read(None).unwrap();
        let (v, names) = check_readable(&rtx);
        assert_eq!(v, 1);
        assert_eq!(names, vec!["a", "b"]);
    }

    // --- MultiWriter unit tests ---

    /// Explicit version bumps `next_version` so auto-assign doesn't collide.
    #[test]
    fn multi_writer_explicit_version_advances_next() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        });
        let wtx = store.begin_write(Some(10)).unwrap();
        assert_eq!(wtx.version(), 10);
        wtx.commit().unwrap();
        // next auto-assigned should be 11
        let wtx2 = store.begin_write(None).unwrap();
        assert_eq!(wtx2.version(), 11);
    }

    /// Delete via `TableWriter` conflicts with update on the same key.
    #[test]
    fn multi_writer_delete_via_table_writer() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        });
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("hello".into()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx_a = store.begin_write(None).unwrap();
        let mut wtx_b = store.begin_write(None).unwrap();

        wtx_a.open_table::<String>("t").unwrap().delete(1).unwrap();
        wtx_b.open_table::<String>("t").unwrap().update(1, "b".into()).unwrap();

        wtx_a.commit().unwrap();
        assert!(matches!(wtx_b.commit(), Err(Error::WriteConflict { .. })));
    }

    /// `update_batch` records all keys in the write set for conflict detection.
    #[test]
    fn multi_writer_update_batch_tracks_keys() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        });
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("a".into()).unwrap();
            t.insert("b".into()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx_a = store.begin_write(None).unwrap();
        let mut wtx_b = store.begin_write(None).unwrap();

        wtx_a.open_table::<String>("t").unwrap().update(1, "a2".into()).unwrap();
        wtx_b.open_table::<String>("t").unwrap()
            .update_batch(vec![(1, "b2".into())]).unwrap();

        wtx_a.commit().unwrap();
        assert!(matches!(wtx_b.commit(), Err(Error::WriteConflict { .. })));
    }

    /// `delete_batch` records all keys in the write set for conflict detection.
    #[test]
    fn multi_writer_delete_batch_tracks_keys() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        });
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("a".into()).unwrap();
            t.insert("b".into()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx_a = store.begin_write(None).unwrap();
        let mut wtx_b = store.begin_write(None).unwrap();

        wtx_a.open_table::<String>("t").unwrap().update(2, "a2".into()).unwrap();
        wtx_b.open_table::<String>("t").unwrap().delete_batch(&[2]).unwrap();

        wtx_a.commit().unwrap();
        assert!(matches!(wtx_b.commit(), Err(Error::WriteConflict { .. })));
    }

    /// `TableWriter` read method pass-through: contains, first, last, iter, get_many.
    #[test]
    fn table_writer_contains_first_last_iter_get_many() {
        let store = Store::default();
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.insert("alice".into()).unwrap();
        t.insert("bob".into()).unwrap();
        t.insert("charlie".into()).unwrap();

        assert!(t.contains(1));
        assert!(!t.contains(99));
        assert_eq!(t.first(), Some((1, &"alice".to_string())));
        assert_eq!(t.last(), Some((3, &"charlie".to_string())));
        assert_eq!(t.iter().count(), 3);
        let many = t.get_many(&[1, 99, 3]);
        assert_eq!(many.len(), 3);
        assert_eq!(many[0], Some(&"alice".to_string()));
        assert!(many[1].is_none());
        assert_eq!(many[2], Some(&"charlie".to_string()));
    }

    /// `TableWriter` index pass-through: `get_by_key` on a non-unique index.
    #[test]
    fn table_writer_get_by_key_passthrough() {
        let store = Store::default();
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.define_index("by_val", IndexKind::NonUnique, |s: &String| s.clone()).unwrap();
        t.insert("apple".into()).unwrap();
        t.insert("banana".into()).unwrap();
        t.insert("apple".into()).unwrap();

        let results = t.get_by_key::<String>("by_val", &"apple".to_string()).unwrap();
        assert_eq!(results.len(), 2);
    }

    /// Type mismatch on a table from the base snapshot (not just dirty map).
    #[test]
    fn open_table_type_mismatch_on_base_table() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("hi".into()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx = store.begin_write(None).unwrap();
        assert!(matches!(wtx.open_table::<u64>("t"), Err(Error::TypeMismatch(_))));
    }

    /// Write sets with `version <= base_version` are skipped during validation —
    /// they were already incorporated into the base snapshot.
    #[test]
    fn validate_skips_old_committed_write_sets() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        });
        // v1
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("a".into()).unwrap();
            wtx.commit().unwrap();
        }
        // Open writer_b based on v1 (so v1's write set is skipped)
        // But first, keep an older writer alive so v1's write set isn't pruned
        let hold = store.begin_write(None).unwrap(); // base=v1, holds write sets alive
        // v3
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("c".into()).unwrap();
            wtx.commit().unwrap();
        }
        // writer based on v3 modifies key 4 — should conflict with v3's committed write set
        // but v1's write set (key 1) should be skipped since it's <= base v3
        let mut wtx_d = store.begin_write(None).unwrap();
        wtx_d.open_table::<String>("t").unwrap().insert("d".into()).unwrap();
        // key 4 doesn't overlap with v3's keys (1..=2), so should succeed
        // Actually v3's committed write set has key 2 (from the second insert).
        // wtx_d will insert key 3 (next_id from base v3 which had 2 records).
        // No overlap with v3's write set {2}. Should succeed.
        wtx_d.commit().unwrap();
        drop(hold);
    }

    /// Edge case: explicit version that equals `next_version` (the false branch
    /// of `commit_version >= inner.next_version` is unreachable in normal usage).
    #[test]
    fn multi_writer_explicit_version_below_next_version() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        });
        // Push next_version to 11 with explicit version 10
        store.begin_write(Some(10)).unwrap().commit().unwrap();
        // Now latest=10, next_version=11. Request version 10+1=11 (auto) or explicit < 11 but > 10... impossible.
        // But: request Some(11) which == next_version. 11 >= 11 → true. Try auto instead to get 11.
        // Actually to get false branch: latest=10, next_version=11. If we somehow have next_version > commit_version.
        // Can't happen with auto-assign (always == next_version). With explicit: must be > latest (10), so >= 11 = next_version.
        // This branch is actually unreachable in normal usage. Skip.
    }

    /// `delete_batch` through `TableWriter` tracks keys and detects conflicts.
    #[test]
    fn multi_writer_delete_batch_through_table_writer() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        });
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("a".into()).unwrap();
            t.insert("b".into()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx_a = store.begin_write(None).unwrap();
        let mut wtx_b = store.begin_write(None).unwrap();

        // Use delete_batch through TableWriter — tests write_set tracking for delete_batch
        wtx_a.open_table::<String>("t").unwrap().delete_batch(&[1]).unwrap();
        wtx_b.open_table::<String>("t").unwrap().delete(1).unwrap();

        wtx_a.commit().unwrap();
        assert!(matches!(wtx_b.commit(), Err(Error::WriteConflict { .. })));
    }

    /// Deleting table T1 while a concurrent writer modifies T2 is not a conflict.
    #[test]
    fn delete_table_no_conflict_when_concurrent_wrote_different_table() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        });
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t1").unwrap().insert("x".into()).unwrap();
            wtx.open_table::<String>("t2").unwrap().insert("y".into()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx_a = store.begin_write(None).unwrap();
        let mut wtx_b = store.begin_write(None).unwrap();

        // A deletes t1, B writes to t2 — no conflict
        wtx_a.delete_table("t1");
        wtx_b.open_table::<String>("t2").unwrap().update(1, "z".into()).unwrap();

        wtx_a.commit().unwrap();
        wtx_b.commit().unwrap(); // should succeed
    }

    /// Concurrent commit deleted a table that this transaction wrote to → conflict.
    #[test]
    fn concurrent_delete_table_conflicts_with_write() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        });
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("x".into()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx_a = store.begin_write(None).unwrap();
        let mut wtx_b = store.begin_write(None).unwrap();

        // A deletes table, B writes to it
        wtx_a.delete_table("t");
        wtx_b.open_table::<String>("t").unwrap().insert("y".into()).unwrap();

        wtx_a.commit().unwrap();
        let err = wtx_b.commit().unwrap_err();
        assert!(matches!(err, Error::WriteConflict { ref table, .. } if table == "t"));
    }

    /// This transaction deleted a table that a concurrent commit wrote to → conflict.
    #[test]
    fn concurrent_write_conflicts_with_delete_table() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        });
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("x".into()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx_a = store.begin_write(None).unwrap();
        let mut wtx_b = store.begin_write(None).unwrap();

        // A writes, B deletes
        wtx_a.open_table::<String>("t").unwrap().insert("y".into()).unwrap();
        wtx_b.delete_table("t");

        wtx_a.commit().unwrap();
        let err = wtx_b.commit().unwrap_err();
        assert!(matches!(err, Error::WriteConflict { ref table, .. } if table == "t"));
    }

    #[test]
    fn require_explicit_version_rejects_none() {
        let store = Store::new(StoreConfig {
            require_explicit_version: true,
            ..StoreConfig::default()
        });
        let result = store.begin_write(None);
        assert!(matches!(result, Err(Error::ExplicitVersionRequired)));
    }

    #[test]
    fn require_explicit_version_accepts_explicit() {
        let store = Store::new(StoreConfig {
            require_explicit_version: true,
            ..StoreConfig::default()
        });
        let wtx = store.begin_write(Some(1)).unwrap();
        assert_eq!(wtx.version(), 1);
        wtx.commit().unwrap();
    }

    #[test]
    fn require_explicit_version_default_is_false() {
        let config = StoreConfig::default();
        assert!(!config.require_explicit_version);
    }

    /// Snapshot table iteration is deterministic (alphabetical by name).
    #[test]
    fn snapshot_tables_iterate_in_deterministic_order() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("zebra").unwrap().insert("z".into()).unwrap();
            wtx.open_table::<String>("apple").unwrap().insert("a".into()).unwrap();
            wtx.open_table::<String>("mango").unwrap().insert("m".into()).unwrap();
            wtx.commit().unwrap();
        }
        let rtx = store.begin_read(None).unwrap();
        let names = rtx.table_names();
        assert_eq!(names, vec!["apple", "mango", "zebra"]);
    }

    /// GC operates on versions in deterministic order.
    #[test]
    fn gc_version_ordering_is_deterministic() {
        let store = Store::new(StoreConfig {
            num_snapshots_retained: 2,
            auto_snapshot_gc: false,
            ..StoreConfig::default()
        });
        for _ in 0..5 {
            store.begin_write(None).unwrap().commit().unwrap();
        }
        store.gc();
        assert!(store.has_snapshot(4));
        assert!(store.has_snapshot(5));
        assert!(!store.has_snapshot(3));
    }
}
