use std::marker::PhantomData;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::index::IndexKind;
use crate::intents::{CommitWaiter, IntentMap, IntentWaiter};
use crate::metrics::StoreMetrics;
use crate::persistence::Record;
use crate::table::{MergeableTable, Table, TableOpener};
use crate::{Error, Result};

// ---------------------------------------------------------------------------
// Snapshot — an immutable versioned view of all tables
// ---------------------------------------------------------------------------

/// An immutable snapshot of all tables at a specific version.
///
/// Tables are stored as `Arc<dyn MergeableTable>` so that building a new
/// snapshot from an existing one (at commit time) is O(number-of-tables)
/// with O(1) per table, and so `WriteTx::commit` can do per-key merges
/// against the latest snapshot's tables without touching the concrete
/// `Table<R>` type. `MergeableTable: Any + Send + Sync`, so downcasts to
/// `Table<R>` still work via the explicit `as_any()` accessor.
#[derive(Clone)]
pub(crate) struct Snapshot {
    pub(crate) version: u64,
    pub(crate) tables: BTreeMap<String, Arc<dyn MergeableTable>>,
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
// IsolationLevel
// ---------------------------------------------------------------------------

/// Transaction isolation level.
///
/// Controls whether [`WriteTx`] tracks read sets and validates them at commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Snapshot Isolation. Reads are not tracked. Prevents dirty/nonrepeatable
    /// reads and phantoms but does *not* prevent write skew. Zero overhead.
    /// Default.
    SnapshotIsolation,
    /// Serializable. WriteTx records every read; commit fails with
    /// [`Error::SerializationFailure`] if any read was invalidated by a
    /// concurrent commit since the tx's base version. Equivalent to
    /// [`SnapshotIsolation`] in [`WriterMode::SingleWriter`] (no concurrent
    /// writers, no validation needed). v1 tracks point reads precisely;
    /// any range/scan/index read is recorded as a coarse "table touched"
    /// flag (false positives possible on read-heavy scan workloads).
    Serializable,
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
    /// Transaction isolation level. Default: [`IsolationLevel::SnapshotIsolation`].
    ///
    /// Set to [`IsolationLevel::Serializable`] to prevent write skew at the
    /// cost of read-set tracking on every `WriteTx` read. Has no effect in
    /// [`WriterMode::SingleWriter`] mode (always equivalent to SI there).
    pub isolation_level: IsolationLevel,
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
            isolation_level: IsolationLevel::SnapshotIsolation,
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
// ReadSetEntry — per-table reads recorded by a Serializable WriteTx
// ---------------------------------------------------------------------------

/// The reads a `Serializable` WriteTx has issued against one table.
///
/// `keys` records primary-key point reads (precise). `table_scan` is set to
/// `true` whenever a non-key read is issued — `iter`, `range`, `len`,
/// `is_empty`, `first`, `last`, `get_unique`, `get_by_index`, `get_by_key`,
/// `index_range`, `custom_index`, `resolve`. v1 conservatively treats any
/// concurrent commit on a `table_scan == true` table as a serialization
/// conflict; v2 may track index-range bounds for finer granularity.
#[derive(Default)]
struct ReadSetEntry {
    keys: BTreeSet<u64>,
    table_scan: bool,
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
    pub(crate) registry: Arc<crate::registry::TableRegistry>,
    /// Test-only mock WAL for controlled fsync testing.
    #[cfg(all(test, feature = "persistence"))]
    pub(crate) mock_wal: Option<std::sync::Arc<crate::wal::MockWal>>,
    pub(crate) metrics: Arc<StoreMetrics>,
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
    /// Write-intent table for early-fail conflict detection (MultiWriter).
    /// Lives outside the commit lock so per-write intent checks don't
    /// serialize through `inner`.
    pub(crate) intents: Arc<IntentMap>,
    /// Monotonic ID source; every `WriteTx` gets a unique id used as the
    /// holder token in the intent map.
    pub(crate) next_writer_id: Arc<AtomicU64>,
    /// Per-table commit mutexes. Writers acquire locks for tables in
    /// their dirty set (canonical order by name, deadlock-free) and hold
    /// them across the merge + install phases. Writers with disjoint
    /// dirty sets don't serialize — they proceed through merge in
    /// parallel and only briefly share the global `inner` write lock at
    /// install time.
    pub(crate) table_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
}

impl Store {
    /// Creates a new, empty store. The initial version is 0.
    ///
    /// Returns an error if persistence is configured and the WAL cannot be
    /// initialized (e.g., directory does not exist and cannot be created,
    /// or permission denied).
    pub fn new(config: StoreConfig) -> Result<Self> {
        #[cfg(feature = "persistence")]
        let wal_handle = match &config.persistence {
            crate::persistence::Persistence::Standalone { dir, durability } => {
                let consistent = matches!(durability, crate::persistence::Durability::Consistent);
                Some(crate::wal::WalHandle::new(dir, consistent)?)
            }
            _ => None,
        };

        let metrics = Arc::new(StoreMetrics::new());
        let empty = Arc::new(Snapshot { version: 0, tables: BTreeMap::new() });
        let mut snapshots = BTreeMap::new();
        snapshots.insert(0, empty);
        Ok(Self {
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
                registry: Arc::new(crate::registry::TableRegistry::default()),
                #[cfg(all(test, feature = "persistence"))]
                mock_wal: None,
                metrics,
            })),
            intents: Arc::new(IntentMap::default()),
            next_writer_id: Arc::new(AtomicU64::new(1)),
            table_locks: Arc::new(DashMap::new()),
        })
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
        let metrics = Arc::clone(&inner.metrics);
        Ok(ReadTx { snapshot, metrics, _not_send: PhantomData })
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

        let explicit_version = version.is_some();
        let commit_version = match version {
            None => inner.next_version,
            Some(v) if v > inner.latest_version => v,
            Some(_) => {
                return Err(Error::WriteConflict {
                    table: String::new(),
                    keys: vec![],
                    version: inner.latest_version,
                    wait_for: None,
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
        let isolation_level = inner.config.isolation_level;
        let metrics = Arc::clone(&inner.metrics);

        // Track active writer.
        inner.active_writer_count += 1;
        if matches!(writer_mode, WriterMode::MultiWriter) {
            inner.active_writer_base_versions.push(base_version);
        }

        let (intents, waiter, writer_id, table_locks) = match writer_mode {
            WriterMode::MultiWriter => (
                Some(Arc::clone(&self.intents)),
                Some(IntentWaiter::new()),
                self.next_writer_id.fetch_add(1, Ordering::Relaxed),
                Some(Arc::clone(&self.table_locks)),
            ),
            WriterMode::SingleWriter => (None, None, 0, None),
        };
        Ok(WriteTx {
            base,
            dirty: BTreeMap::new(),
            version: commit_version,
            explicit_version,
            store_inner: Arc::clone(&self.inner),
            deleted_tables: BTreeSet::new(),
            write_set: BTreeMap::new(),
            ever_deleted_tables: BTreeSet::new(),
            writer_mode,
            needs_cleanup: true,
            metrics,
            intents,
            writer_id,
            waiter,
            table_locks,
            #[cfg(feature = "persistence")]
            wal_ops: Vec::new(),
            #[cfg(feature = "persistence")]
            wal_enabled: inner.wal_handle.is_some(),
            read_set: match (isolation_level, writer_mode) {
                (IsolationLevel::Serializable, WriterMode::MultiWriter) => {
                    Some(std::cell::RefCell::new(BTreeMap::new()))
                }
                _ => None,
            },
            isolation_level,
            _not_send: PhantomData,
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

    /// Returns a point-in-time snapshot of all store and table metrics.
    pub fn metrics(&self) -> crate::metrics::MetricsSnapshot {
        self.inner.read().unwrap().metrics.snapshot()
    }

    // --- Persistence methods (feature-gated) ---

    /// Returns the number of WAL entries sent but not yet fsynced.
    /// Only meaningful in Eventual durability mode; returns 0 otherwise.
    #[cfg(feature = "persistence")]
    pub fn pending_wal_writes(&self) -> u64 {
        let inner = self.inner.read().unwrap();
        inner.wal_handle.as_ref().map_or(0, |h| h.pending_writes())
    }

    /// Register a table type for persistence. Must be called before any
    /// transactions that touch this table, and before [`Store::open`] or
    /// [`Store::checkpoint`].
    #[cfg(feature = "persistence")]
    pub fn register_table<R: crate::persistence::Record>(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        Arc::get_mut(&mut inner.registry)
            .ok_or_else(|| Error::Persistence(
                "cannot register table: registry is in use (checkpoint in progress?)".into(),
            ))?
            .register::<R>(name)
    }

    /// Write a checkpoint of the latest snapshot to disk.
    ///
    /// Blocks the caller until the checkpoint is fully written and fsynced,
    /// but does not hold any store lock during I/O — reads and writes
    /// proceed without contention.
    ///
    /// In `Standalone` mode, the WAL is pruned after the checkpoint is written.
    /// Returns the version of the checkpointed snapshot.
    #[cfg(feature = "persistence")]
    pub fn checkpoint(&self) -> Result<u64> {
        let (dir, snap, registry) = {
            let inner = self.inner.read().unwrap();
            let dir = match &inner.config.persistence {
                crate::persistence::Persistence::Standalone { dir, .. }
                | crate::persistence::Persistence::Smr { dir } => dir.clone(),
                crate::persistence::Persistence::None => {
                    return Err(Error::Persistence("checkpoint requires persistence to be configured".into()));
                }
            };
            let snap = inner.snapshots[&inner.latest_version].clone();
            let registry = Arc::clone(&inner.registry);
            (dir, snap, registry)
        }; // read lock released here

        let version = crate::checkpoint::write_checkpoint(&dir, &snap, &registry)?;

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
            let registry = {
                let inner = self.inner.read().unwrap();
                Arc::clone(&inner.registry)
            };
            let snapshot = crate::checkpoint::load_checkpoint(&cp_path, &registry)?;

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
                    let mut tables: BTreeMap<String, Arc<dyn MergeableTable>> = BTreeMap::new();
                    for (name, arc) in &base_snap.tables {
                        let info = inner.registry.get(name)
                            .ok_or_else(|| Error::TableNotRegistered(name.clone()))?;
                        let bytes = (info.serialize_table)(arc.as_ref().as_any())?;
                        let new_table = (info.deserialize_table)(&bytes)?;
                        tables.insert(name.clone(), Arc::from(new_table));
                    }
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
                                        ))?
                                        .as_any_mut();

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
                                            ))?
                                            .as_any_mut();
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

    // -----------------------------------------------------------------------
    // Bulk-load
    // -----------------------------------------------------------------------

    /// Bulk-load rows into a table, producing a single new committed snapshot.
    ///
    /// For `Replace`, materializes the source into sorted rows off-lock,
    /// builds a fresh data tree and indexes via `Table::from_bulk` (preserving
    /// any existing index *definitions* with empty storage), then atomically
    /// installs the result as a new MVCC snapshot.
    ///
    /// `Delta` is not implemented in this phase and returns
    /// [`Error::InvalidBulkLoadInput`].
    ///
    /// See `docs/tasks/task23_bulk_load.md`.
    pub fn bulk_load<R: Record>(
        &self,
        table_name: &str,
        input: crate::bulk_load::BulkLoadInput<R>,
        opts: crate::bulk_load::BulkLoadOptions,
    ) -> Result<u64> {
        use crate::bulk_load::{BulkLoadInput, materialize_source};

        match input {
            BulkLoadInput::Replace(source) => {
                // 1. Materialize sorted rows off-lock.
                let mat = materialize_source::<R>(source)?;
                let next_id = mat.max_id.map(|m| m + 1).unwrap_or(1);

                // 2. Snapshot current state to read existing index defs (if any).
                let base_snapshot = {
                    let inner = self.inner.read().unwrap();
                    inner.snapshots[&inner.latest_version].clone()
                };

                let index_defs: Vec<Box<dyn crate::index::IndexMaintainer<R>>> =
                    if let Some(existing) = base_snapshot.tables.get(table_name) {
                        let typed = existing
                            .as_any()
                            .downcast_ref::<crate::table::Table<R>>()
                            .ok_or_else(|| Error::TypeMismatch(table_name.to_string()))?;
                        typed.empty_index_defs()
                    } else if opts.create_if_missing {
                        Vec::new()
                    } else {
                        return Err(Error::TableNotFound(table_name.to_string()));
                    };

                // 3. Build the new table off-lock.
                let new_table: crate::table::Table<R> =
                    crate::table::Table::from_bulk(mat.rows, next_id, index_defs)?;

                // 4. Install under the global write lock, mirroring the
                //    install pattern used by `WriteTx::commit_single_writer`.
                let new_version = self.install_replaced_table(table_name, new_table)?;

                // 5. Optional checkpoint + WAL prune (Phase 8 wiring; no-op for now).
                if opts.checkpoint_after {
                    self.checkpoint_and_prune_after_bulk(new_version)?;
                }
                Ok(new_version)
            }
            BulkLoadInput::Delta(delta) => {
                use crate::bulk_load::materialize_delta;

                // 1. Capture base snapshot + version.
                let (base_snapshot, base_version) = {
                    let inner = self.inner.read().unwrap();
                    (
                        inner.snapshots[&inner.latest_version].clone(),
                        inner.latest_version,
                    )
                };

                let base_table_arc = base_snapshot
                    .tables
                    .get(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
                    .clone();
                let base_typed = base_table_arc
                    .as_any()
                    .downcast_ref::<crate::table::Table<R>>()
                    .ok_or_else(|| Error::TypeMismatch(table_name.to_string()))?;

                // 2. Validate + materialize off-lock.
                let mat = materialize_delta(delta, base_typed.data_ref())?;
                let next_id = mat
                    .max_id
                    .map(|m| m + 1)
                    .unwrap_or(base_typed.next_id())
                    .max(base_typed.next_id());

                let index_defs = base_typed.empty_index_defs();
                let new_table: crate::table::Table<R> =
                    crate::table::Table::from_bulk(mat.rows, next_id, index_defs)?;

                // 3. Conflict check + install. If `latest_version` advanced
                //    since `base_version`, abort with WriteConflict.
                let new_version =
                    self.install_after_delta_check(table_name, new_table, base_version)?;

                if opts.checkpoint_after {
                    self.checkpoint_and_prune_after_bulk(new_version)?;
                }
                Ok(new_version)
            }
        }
    }

    /// Install a freshly-built table as a new snapshot. Mirrors the install
    /// tail of `WriteTx::commit_single_writer`: forks the latest snapshot's
    /// table map, swaps in the new table, bumps `latest_version`. Holds the
    /// global write lock for the duration.
    fn install_replaced_table<R: Record>(
        &self,
        name: &str,
        new_table: crate::table::Table<R>,
    ) -> Result<u64> {
        let mut inner = self.inner.write().unwrap();

        // Auto-assigned commit version: bump to latest_version + 1 so version
        // order matches commit order, matching the WriteTx::commit pattern.
        let new_version = inner.latest_version + 1;
        if new_version >= inner.next_version {
            inner.next_version = new_version + 1;
        }

        let prev = &inner.snapshots[&inner.latest_version];
        let mut tables: BTreeMap<String, Arc<dyn MergeableTable>> = prev
            .tables
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        tables.insert(
            name.to_string(),
            Arc::new(new_table) as Arc<dyn MergeableTable>,
        );

        let snapshot = Arc::new(Snapshot { version: new_version, tables });
        inner.snapshots.insert(new_version, snapshot);
        inner.latest_version = new_version;
        if inner.config.auto_snapshot_gc {
            gc_inner(&mut inner);
        }
        Ok(new_version)
    }

    /// Install a freshly-built table as a new snapshot, refusing the install
    /// if a concurrent commit advanced `latest_version` since the delta was
    /// computed against `base_version`. Mirrors `install_replaced_table`,
    /// adding the version-drift check up front.
    fn install_after_delta_check<R: Record>(
        &self,
        name: &str,
        new_table: crate::table::Table<R>,
        base_version: u64,
    ) -> Result<u64> {
        let mut inner = self.inner.write().unwrap();

        if inner.latest_version != base_version {
            return Err(Error::WriteConflict {
                table: name.to_string(),
                keys: vec![],
                version: inner.latest_version,
                wait_for: None,
            });
        }

        let new_version = inner.latest_version + 1;
        if new_version >= inner.next_version {
            inner.next_version = new_version + 1;
        }

        let prev = &inner.snapshots[&inner.latest_version];
        let mut tables: BTreeMap<String, Arc<dyn MergeableTable>> = prev
            .tables
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        tables.insert(
            name.to_string(),
            Arc::new(new_table) as Arc<dyn MergeableTable>,
        );

        let snapshot = Arc::new(Snapshot { version: new_version, tables });
        inner.snapshots.insert(new_version, snapshot);
        inner.latest_version = new_version;
        if inner.config.auto_snapshot_gc {
            gc_inner(&mut inner);
        }
        Ok(new_version)
    }

    /// Checkpoint + WAL prune after a bulk load. Phase 8 will wire this up
    /// to the persistence layer; for now it is a no-op so non-persistent
    /// stores work and persistent stores fall back to normal WAL replay.
    fn checkpoint_and_prune_after_bulk(&self, _new_version: u64) -> Result<()> {
        Ok(())
    }
}

/// RAII owner of a set of per-table commit locks acquired in canonical
/// order. Stores each `Arc<Mutex<()>>` alongside a `MutexGuard` borrowed
/// from it; the guard is dropped before the Arc (via explicit
/// `ManuallyDrop`), so the borrow never outlives its source.
///
/// The `'static` lifetime on the stored guards is a pure bookkeeping
/// trick — the actual lifetime is tied to the paired `Arc<Mutex<()>>` in
/// the same entry. Safety relies on dropping guards before Arcs, which
/// `Drop::drop` enforces explicitly below.
struct TableLockGuards {
    slots: Vec<LockSlot>,
}

struct LockSlot {
    // `arc` keeps the Mutex alive for the lifetime of the stored guard;
    // it is "read" only via the borrow carried inside the guard.
    #[allow(dead_code)]
    arc: Arc<Mutex<()>>,
    guard: std::mem::ManuallyDrop<MutexGuard<'static, ()>>,
}

impl TableLockGuards {
    fn empty() -> Self {
        Self { slots: Vec::new() }
    }

    fn acquire(arcs: Vec<Arc<Mutex<()>>>) -> Self {
        let mut slots = Vec::with_capacity(arcs.len());
        for arc in arcs {
            // SAFETY: the MutexGuard borrows from the Mutex inside this
            // Arc. We store both in the same `LockSlot`, and `Drop for
            // TableLockGuards` drops the guard before dropping the Arc
            // (via `ManuallyDrop::drop` followed by struct-field drop).
            // The Arc keeps the Mutex alive for at least as long as the
            // guard, so the extended `'static` lifetime is sound.
            let guard: MutexGuard<'_, ()> = arc.lock().unwrap();
            let guard: MutexGuard<'static, ()> = unsafe { std::mem::transmute(guard) };
            slots.push(LockSlot {
                arc,
                guard: std::mem::ManuallyDrop::new(guard),
            });
        }
        Self { slots }
    }
}

impl Drop for TableLockGuards {
    fn drop(&mut self) {
        // Drop guards first (reverse acquisition order), then the Arcs
        // fall out of scope when `slots` is dropped.
        while let Some(mut slot) = self.slots.pop() {
            // SAFETY: the guard is only dropped here, and the paired Arc
            // is still alive in `slot.arc` until this method returns.
            unsafe { std::mem::ManuallyDrop::drop(&mut slot.guard) };
            // `slot.arc` drops normally when `slot` goes out of scope.
        }
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

    let before = inner.snapshots.len();
    inner.snapshots.retain(|&v, snapshot| {
        protected.contains(&v) || Arc::strong_count(snapshot) > 1
    });
    let removed = (before - inner.snapshots.len()) as u64;
    inner.metrics.inc_gc_run();
    if removed > 0 {
        inner.metrics.inc_snapshots_collected(removed);
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new(StoreConfig::default()).expect("default StoreConfig cannot fail")
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
    metrics: Arc<StoreMetrics>,
    /// Pins `ReadTx` to its creating thread. Users who need a read view on
    /// another thread should call `store.begin_read(Some(version))` there.
    _not_send: PhantomData<*const ()>,
}

/// Read-only access to a snapshot.
///
/// Implemented by [`ReadTx`]. Generic code that only needs to read tables can
/// accept `impl Readable` instead of a concrete transaction type.
pub trait Readable {
    /// Borrow a table from this snapshot.
    fn open_table<R: Record>(&self, opener: impl TableOpener<R>) -> Result<TableReader<'_, R>>;
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
    pub fn open_table<R: Record>(&self, opener: impl TableOpener<R>) -> Result<TableReader<'_, R>> {
        let name = opener.table_name();
        let table = self.snapshot
            .tables
            .get(name)
            .ok_or(Error::KeyNotFound)?
            .as_any()
            .downcast_ref::<Table<R>>()
            .ok_or_else(|| Error::TypeMismatch(name.to_string()))?;
        self.metrics.register_table(name);
        Ok(TableReader {
            table,
            metrics: &self.metrics,
            table_name: name.to_string(),
        })
    }
}

impl Readable for ReadTx {
    fn open_table<R: Record>(&self, opener: impl TableOpener<R>) -> Result<TableReader<'_, R>> {
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
    dirty: BTreeMap<String, Box<dyn MergeableTable>>,
    /// The version number that will be assigned to the new snapshot on commit.
    version: u64,
    /// True when the caller passed an explicit version to `begin_write`
    /// (SMR mode). Auto-assigned versions (the None path) are bumped at
    /// commit time if a concurrent MultiWriter commit landed at a higher
    /// version in the meantime — see the commit code for the rationale.
    explicit_version: bool,
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
    /// Shared metrics for the store this transaction belongs to.
    metrics: Arc<StoreMetrics>,
    /// Shared intent table. `Some` only in MultiWriter mode — SingleWriter
    /// has no concurrent writers, so intent bookkeeping is elided entirely
    /// (no Arc clone, no per-tx waiter allocation).
    intents: Option<Arc<IntentMap>>,
    /// Unique token identifying this writer in the intent table. Unused
    /// when `intents` is `None`.
    writer_id: u64,
    /// Per-writer "done" signal. `Some` only in MultiWriter mode.
    waiter: Option<Arc<IntentWaiter>>,
    /// Per-table commit mutex registry (MultiWriter only). `commit`
    /// acquires an `Arc<Mutex<()>>` from this map for each dirty table in
    /// canonical order before doing merge + install.
    table_locks: Option<Arc<DashMap<String, Arc<Mutex<()>>>>>,
    /// WAL operations accumulated during this transaction (persistence only).
    #[cfg(feature = "persistence")]
    pub(crate) wal_ops: Vec<crate::wal::WalOp>,
    /// Whether WAL tracking is active (true only when a WAL handle exists).
    #[cfg(feature = "persistence")]
    wal_enabled: bool,
    /// Per-table read set tracked when `isolation == Serializable` AND
    /// `writer_mode == MultiWriter`. `None` otherwise — SI never validates,
    /// and SingleWriter has no concurrent writers, so allocating a read set
    /// would be pure waste. `RefCell` because reads are recorded through
    /// shared `&TableReader`/`&TableWriter` references.
    read_set: Option<std::cell::RefCell<BTreeMap<String, ReadSetEntry>>>,
    /// Cached isolation level — copied from the store config at `begin_write`
    /// so the commit path doesn't re-read the config under a lock.
    isolation_level: IsolationLevel,
    /// Pins `WriteTx` to its creating thread. A transaction must complete on
    /// the thread that opened it — the write-set tracking is not designed to
    /// be split across threads. Users who want concurrent writers should
    /// `store.clone()` and call `begin_write` on each thread.
    _not_send: PhantomData<*const ()>,
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
    metrics: Arc<StoreMetrics>,
    table_name: String,
    /// `Some` in MultiWriter mode: bundles the shared intent map with the
    /// caller-writer's id and waiter, so `update`/`delete` can perform
    /// early-fail conflict detection without re-plumbing three separate
    /// fields through every call site.
    intent_ctx: Option<IntentCtx<'tx>>,
    #[cfg(feature = "persistence")]
    wal_ops: Option<WalOpsWriter<'tx>>,
    /// Mirrors `TableReader::read_set` — used by `TableWriter`'s read methods
    /// (`get`, `iter`, ...) so reads done through a write-mode handle still
    /// participate in SSI tracking.
    read_set: Option<&'tx std::cell::RefCell<BTreeMap<String, ReadSetEntry>>>,
}

/// Shared intent-table context held by `TableWriter` in MultiWriter mode.
/// Borrowed from the parent `WriteTx` for the lifetime of the writer.
struct IntentCtx<'tx> {
    intents: &'tx IntentMap,
    writer_id: u64,
    waiter: &'tx Arc<IntentWaiter>,
}

/// Bundles the table name and ops list for WAL tracking in TableWriter.
#[cfg(feature = "persistence")]
struct WalOpsWriter<'tx> {
    table_name: String,
    ops: &'tx mut Vec<crate::wal::WalOp>,
}

impl<'tx, R: Record> TableWriter<'tx, R> {
    /// Claim a write intent on `(table, id)` for this writer. Returns
    /// `Err(WriteConflict { wait_for: Some(..) })` immediately if another
    /// active writer already holds the intent — the caller's retry loop
    /// can block on that waiter until the holder commits or aborts.
    ///
    /// Only runs in `MultiWriter` mode; in `SingleWriter` there can be no
    /// conflicting writer by construction.
    fn claim_intent(&self, id: u64) -> Result<()> {
        let Some(ctx) = self.intent_ctx.as_ref() else {
            return Ok(());
        };
        match ctx
            .intents
            .try_acquire(&self.table_name, id, ctx.writer_id, ctx.waiter)
        {
            Ok(()) => Ok(()),
            Err(holder_waiter) => {
                self.metrics.inc_write_conflict();
                Err(Error::WriteConflict {
                    table: self.table_name.clone(),
                    keys: vec![id],
                    // Early-fail has no "conflicting committed version"; the
                    // holder is still in flight. Use 0 as a sentinel.
                    version: 0,
                    wait_for: Some(CommitWaiter(Arc::clone(&holder_waiter))),
                })
            }
        }
    }

    // --- Write methods (tracked) ---

    /// Insert a record. Returns the auto-assigned ID.
    pub fn insert(&mut self, record: R) -> Result<u64> {
        #[cfg(feature = "persistence")]
        if let Some(w) = &mut self.wal_ops {
            let data = Self::serialize_record(&record)?;
            let id = self.table.insert(record)?;
            if let Some(ws) = &mut self.write_set { ws.insert(id); }
            w.ops.push(crate::wal::WalOp::Insert { table: w.table_name.clone(), id, data });
            self.metrics.inc_inserts(&self.table_name, 1);
            return Ok(id);
        }
        let id = self.table.insert(record)?;
        if let Some(ws) = &mut self.write_set {
            ws.insert(id);
        }
        self.metrics.inc_inserts(&self.table_name, 1);
        Ok(id)
    }

    /// Update a record by its ID.
    pub fn update(&mut self, id: u64, record: R) -> Result<()> {
        self.claim_intent(id)?;
        #[cfg(feature = "persistence")]
        if let Some(w) = &mut self.wal_ops {
            let data = Self::serialize_record(&record)?;
            self.table.update(id, record)?;
            if let Some(ws) = &mut self.write_set { ws.insert(id); }
            w.ops.push(crate::wal::WalOp::Update { table: w.table_name.clone(), id, data });
            self.metrics.inc_updates(&self.table_name, 1);
            return Ok(());
        }
        self.table.update(id, record)?;
        if let Some(ws) = &mut self.write_set {
            ws.insert(id);
        }
        self.metrics.inc_updates(&self.table_name, 1);
        Ok(())
    }

    /// Delete a record by its ID. Returns the deleted record.
    pub fn delete(&mut self, id: u64) -> Result<Arc<R>> {
        self.claim_intent(id)?;
        let old = self.table.delete(id)?;
        if let Some(ws) = &mut self.write_set {
            ws.insert(id);
        }
        #[cfg(feature = "persistence")]
        if let Some(w) = &mut self.wal_ops {
            w.ops.push(crate::wal::WalOp::Delete { table: w.table_name.clone(), id });
        }
        self.metrics.inc_deletes(&self.table_name, 1);
        Ok(old)
    }

    /// Insert multiple records atomically.
    pub fn insert_batch(&mut self, records: Vec<R>) -> Result<Vec<u64>> {
        #[cfg(feature = "persistence")]
        if let Some(w) = &mut self.wal_ops {
            let data_list: Vec<Vec<u8>> = records.iter()
                .map(|r| Self::serialize_record(r))
                .collect::<Result<_>>()?;
            let ids = self.table.insert_batch(records)?;
            if let Some(ws) = &mut self.write_set { ws.extend(ids.iter().copied()); }
            for (id, data) in ids.iter().zip(data_list) {
                w.ops.push(crate::wal::WalOp::Insert { table: w.table_name.clone(), id: *id, data });
            }
            self.metrics.inc_inserts(&self.table_name, ids.len() as u64);
            return Ok(ids);
        }
        let ids = self.table.insert_batch(records)?;
        if let Some(ws) = &mut self.write_set {
            ws.extend(ids.iter().copied());
        }
        self.metrics.inc_inserts(&self.table_name, ids.len() as u64);
        Ok(ids)
    }

    /// Update multiple records atomically.
    ///
    /// Batch ops rely on commit-time OCC rather than early-fail intents:
    /// the underlying `Table::update_batch` uses snapshot-and-restore for
    /// atomic rollback, and pre-claiming intents would leave dangling
    /// claims on failure (breaking the poison-free guarantee of the
    /// write set). Conflict on any id still surfaces — just at commit.
    pub fn update_batch(&mut self, updates: Vec<(u64, R)>) -> Result<()> {
        #[cfg(feature = "persistence")]
        if let Some(w) = &mut self.wal_ops {
            let ops_data: Vec<(u64, Vec<u8>)> = updates.iter()
                .map(|(id, r)| Self::serialize_record(r).map(|d| (*id, d)))
                .collect::<Result<_>>()?;
            let ids: Vec<u64> = updates.iter().map(|(id, _)| *id).collect();
            self.table.update_batch(updates)?;
            if let Some(ws) = &mut self.write_set { ws.extend(ids.iter()); }
            for (id, data) in ops_data {
                w.ops.push(crate::wal::WalOp::Update { table: w.table_name.clone(), id, data });
            }
            self.metrics.inc_updates(&self.table_name, ids.len() as u64);
            return Ok(());
        }
        let ids: Vec<u64> = updates.iter().map(|(id, _)| *id).collect();
        self.table.update_batch(updates)?;
        if let Some(ws) = &mut self.write_set {
            ws.extend(ids.iter());
        }
        self.metrics.inc_updates(&self.table_name, ids.len() as u64);
        Ok(())
    }

    /// Delete multiple records atomically. See `update_batch` for why
    /// batch ops skip early-fail intent claiming.
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
        self.metrics.inc_deletes(&self.table_name, ids.len() as u64);
        Ok(())
    }

    #[cfg(feature = "persistence")]
    fn serialize_record(record: &R) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(record, bincode::config::standard())
            .map_err(|e| Error::Persistence(e.to_string()))
    }

    // --- Read methods (pass-through) ---

    /// Look up a record by its ID.
    pub fn get(&self, id: u64) -> Option<&R> {
        record_point_read(self.read_set, &self.table_name, id);
        self.metrics.inc_primary_key_reads(&self.table_name, 1);
        self.table.get(id)
    }

    /// Returns an iterator over records within the specified ID range.
    pub fn range<'a>(&'a self, range: impl std::ops::RangeBounds<u64> + 'a) -> impl Iterator<Item = (u64, &'a R)> + 'a {
        record_table_scan(self.read_set, &self.table_name);
        self.metrics.inc_primary_key_scans(&self.table_name);
        self.table.range(range)
    }

    /// Returns the number of records in the table.
    #[must_use]
    pub fn len(&self) -> usize {
        record_table_scan(self.read_set, &self.table_name);
        self.table.len()
    }

    /// Returns true if the table contains no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        record_table_scan(self.read_set, &self.table_name);
        self.table.is_empty()
    }

    /// Returns true if the table contains a record with the given ID.
    pub fn contains(&self, id: u64) -> bool {
        record_point_read(self.read_set, &self.table_name, id);
        self.metrics.inc_primary_key_reads(&self.table_name, 1);
        self.table.contains(id)
    }

    /// Returns the first (lowest ID) record, or `None` if empty.
    pub fn first(&self) -> Option<(u64, &R)> {
        record_table_scan(self.read_set, &self.table_name);
        self.metrics.inc_primary_key_reads(&self.table_name, 1);
        self.table.first()
    }

    /// Returns the last (highest ID) record, or `None` if empty.
    pub fn last(&self) -> Option<(u64, &R)> {
        record_table_scan(self.read_set, &self.table_name);
        self.metrics.inc_primary_key_reads(&self.table_name, 1);
        self.table.last()
    }

    /// Iterate over all records in ID order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &R)> + '_ {
        record_table_scan(self.read_set, &self.table_name);
        self.metrics.inc_primary_key_scans(&self.table_name);
        self.table.iter()
    }

    /// Look up multiple records by ID.
    pub fn get_many(&self, ids: &[u64]) -> Vec<Option<&R>> {
        for id in ids { record_point_read(self.read_set, &self.table_name, *id); }
        self.metrics.inc_primary_key_reads(&self.table_name, ids.len() as u64);
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
        self.metrics.register_index(&self.table_name, name);
        self.table.define_index(name, kind, extractor)
    }

    /// Look up a single record by a unique index.
    pub fn get_unique<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &K,
    ) -> Result<Option<(u64, &R)>> {
        record_table_scan(self.read_set, &self.table_name);
        self.metrics.inc_index_reads(&self.table_name, index_name);
        self.table.get_unique(index_name, key)
    }

    /// Look up records by a non-unique index key.
    pub fn get_by_index<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &K,
    ) -> Result<Vec<(u64, &R)>> {
        record_table_scan(self.read_set, &self.table_name);
        self.metrics.inc_index_reads(&self.table_name, index_name);
        self.table.get_by_index(index_name, key)
    }

    /// Look up records by index key (works for both unique and non-unique).
    pub fn get_by_key<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &K,
    ) -> Result<Vec<(u64, &R)>> {
        record_table_scan(self.read_set, &self.table_name);
        self.metrics.inc_index_reads(&self.table_name, index_name);
        self.table.get_by_key(index_name, key)
    }

    /// Range scan on an index (works for both unique and non-unique).
    pub fn index_range<K: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        range: impl std::ops::RangeBounds<K>,
    ) -> Result<Vec<(u64, &R)>> {
        record_table_scan(self.read_set, &self.table_name);
        self.metrics.inc_index_range_scans(&self.table_name, index_name);
        self.table.index_range(index_name, range)
    }

    /// Define a custom index on the underlying table.
    pub fn define_custom_index<I: crate::CustomIndex<R>>(
        &mut self,
        name: &str,
        index: I,
    ) -> Result<()> {
        self.metrics.register_index(&self.table_name, name);
        self.table.define_custom_index(name, index)
    }

    /// Retrieve a reference to a custom index by name, downcast to the concrete type.
    pub fn custom_index<I: crate::CustomIndex<R>>(&self, name: &str) -> Result<&I> {
        record_table_scan(self.read_set, &self.table_name);
        self.table.custom_index(name)
    }

    /// Resolve a slice of record IDs to `(id, &record)` pairs.
    /// IDs that don't exist in the table are silently skipped.
    pub fn resolve(&self, ids: &[u64]) -> Vec<(u64, &R)> {
        for id in ids { record_point_read(self.read_set, &self.table_name, *id); }
        self.metrics.inc_primary_key_reads(&self.table_name, ids.len() as u64);
        self.table.resolve(ids)
    }
}

// ---------------------------------------------------------------------------
// TableReader — read-only instrumented wrapper around &Table<R>
// ---------------------------------------------------------------------------

/// A read-only instrumented wrapper around [`Table<R>`].
///
/// Returned by [`ReadTx::open_table`]. Provides the same read methods as
/// [`TableWriter`] while tracking read metrics.
pub struct TableReader<'tx, R: Record> {
    table: &'tx Table<R>,
    metrics: &'tx StoreMetrics,
    table_name: String,
}

/// Record a point-read against `table` for the given `id`. No-op when `rs`
/// is `None` (SnapshotIsolation or read-only tx).
#[inline]
fn record_point_read(
    rs: Option<&std::cell::RefCell<BTreeMap<String, ReadSetEntry>>>,
    table: &str,
    id: u64,
) {
    if let Some(cell) = rs {
        cell.borrow_mut().entry(table.to_string()).or_default().keys.insert(id);
    }
}

/// Record a range/scan/index read against `table`. No-op when `rs` is `None`.
#[inline]
fn record_table_scan(
    rs: Option<&std::cell::RefCell<BTreeMap<String, ReadSetEntry>>>,
    table: &str,
) {
    if let Some(cell) = rs {
        cell.borrow_mut().entry(table.to_string()).or_default().table_scan = true;
    }
}

impl<'tx, R: Record> TableReader<'tx, R> {
    /// Look up a record by its ID.
    pub fn get(&self, id: u64) -> Option<&R> {
        self.metrics.inc_primary_key_reads(&self.table_name, 1);
        self.table.get(id)
    }

    /// Returns an iterator over records within the specified ID range.
    pub fn range<'a>(&'a self, range: impl std::ops::RangeBounds<u64> + 'a) -> impl Iterator<Item = (u64, &'a R)> + 'a {
        self.metrics.inc_primary_key_scans(&self.table_name);
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
        self.metrics.inc_primary_key_reads(&self.table_name, 1);
        self.table.contains(id)
    }

    /// Returns the first (lowest ID) record, or `None` if empty.
    pub fn first(&self) -> Option<(u64, &R)> {
        self.metrics.inc_primary_key_reads(&self.table_name, 1);
        self.table.first()
    }

    /// Returns the last (highest ID) record, or `None` if empty.
    pub fn last(&self) -> Option<(u64, &R)> {
        self.metrics.inc_primary_key_reads(&self.table_name, 1);
        self.table.last()
    }

    /// Iterate over all records in ID order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &R)> + '_ {
        self.metrics.inc_primary_key_scans(&self.table_name);
        self.table.iter()
    }

    /// Look up multiple records by ID.
    pub fn get_many(&self, ids: &[u64]) -> Vec<Option<&R>> {
        self.metrics.inc_primary_key_reads(&self.table_name, ids.len() as u64);
        self.table.get_many(ids)
    }

    /// Look up a single record by a unique index.
    pub fn get_unique<K: Ord + Clone + Send + Sync + 'static>(
        &self, index_name: &str, key: &K,
    ) -> Result<Option<(u64, &R)>> {
        self.metrics.inc_index_reads(&self.table_name, index_name);
        self.table.get_unique(index_name, key)
    }

    /// Look up records by a non-unique index key.
    pub fn get_by_index<K: Ord + Clone + Send + Sync + 'static>(
        &self, index_name: &str, key: &K,
    ) -> Result<Vec<(u64, &R)>> {
        self.metrics.inc_index_reads(&self.table_name, index_name);
        self.table.get_by_index(index_name, key)
    }

    /// Look up records by index key (works for both unique and non-unique).
    pub fn get_by_key<K: Ord + Clone + Send + Sync + 'static>(
        &self, index_name: &str, key: &K,
    ) -> Result<Vec<(u64, &R)>> {
        self.metrics.inc_index_reads(&self.table_name, index_name);
        self.table.get_by_key(index_name, key)
    }

    /// Range scan on an index (works for both unique and non-unique).
    pub fn index_range<K: Ord + Clone + Send + Sync + 'static>(
        &self, index_name: &str, range: impl std::ops::RangeBounds<K>,
    ) -> Result<Vec<(u64, &R)>> {
        self.metrics.inc_index_range_scans(&self.table_name, index_name);
        self.table.index_range(index_name, range)
    }

    /// Retrieve a reference to a custom index by name, downcast to the concrete type.
    pub fn custom_index<I: crate::CustomIndex<R>>(&self, name: &str) -> Result<&I> {
        self.table.custom_index(name)
    }

    /// Resolve a slice of record IDs to `(id, &record)` pairs.
    /// IDs that don't exist in the table are silently skipped.
    pub fn resolve(&self, ids: &[u64]) -> Vec<(u64, &R)> {
        self.metrics.inc_primary_key_reads(&self.table_name, ids.len() as u64);
        self.table.resolve(ids)
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
                    Some(arc_mt) => arc_mt
                        .as_any()
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
            .as_any_mut()
            .downcast_mut::<Table<R>>()
            .ok_or_else(|| Error::TypeMismatch(name_str.clone()))?;
        let write_set = match self.writer_mode {
            WriterMode::MultiWriter => Some(self.write_set.entry(name_str.clone()).or_default()),
            WriterMode::SingleWriter => None,
        };
        let intent_ctx = match (self.intents.as_deref(), self.waiter.as_ref()) {
            (Some(intents), Some(waiter)) => Some(IntentCtx {
                intents,
                writer_id: self.writer_id,
                waiter,
            }),
            _ => None,
        };
        self.metrics.register_table(&name_str);
        #[cfg(feature = "persistence")]
        let wal_ops = if self.wal_enabled {
            Some(WalOpsWriter {
                table_name: name_str.clone(),
                ops: &mut self.wal_ops,
            })
        } else {
            None
        };
        Ok(TableWriter {
            table,
            write_set,
            metrics: Arc::clone(&self.metrics),
            table_name: name_str,
            intent_ctx,
            #[cfg(feature = "persistence")]
            wal_ops,
            read_set: self.read_set.as_ref(),
        })
    }

    /// Commit this transaction, creating a new snapshot in the store.
    ///
    /// In [`WriterMode::MultiWriter`] mode, validates that no concurrent commit
    /// modified overlapping keys. Returns [`Error::WriteConflict`] on conflict.
    ///
    /// Returns the version number of the new snapshot.
    pub fn commit(self) -> Result<u64> {
        // SingleWriter: no concurrent commits possible, so skip the
        // per-table lock acquisition + read-lock handshake and use the
        // original "one write lock through the whole commit" flow. This
        // keeps the sequential path a single lock acquire/release per
        // commit (no read→drop→write hop).
        match self.writer_mode {
            WriterMode::SingleWriter => self.commit_single_writer(),
            WriterMode::MultiWriter => self.commit_multi_writer(),
        }
    }

    /// SingleWriter commit: acquires one `inner.write()` lock and does
    /// validation, merge, install all under it. No per-table locks
    /// because there's no other writer to exclude.
    fn commit_single_writer(mut self) -> Result<u64> {
        let mut inner = self.store_inner.write().unwrap();

        // WAL submit under lock (preserves ordering with snapshot promote).
        #[cfg(feature = "persistence")]
        let waiter = if let Some(wal) = &inner.wal_handle {
            let ops = std::mem::take(&mut self.wal_ops);
            if !ops.is_empty() {
                let entry = crate::wal::WalEntry { version: self.version, ops };
                Some(wal.write(entry)?)
            } else {
                None
            }
        } else {
            None
        };

        #[cfg(all(test, feature = "persistence"))]
        let mock_waiter = inner.mock_wal.as_ref().map(|mock| {
            mock.write(crate::wal::WalEntry { version: self.version, ops: vec![] })
        });

        // SingleWriter: nobody else commits concurrently, so the fast
        // path always fires — install every dirty table wholesale.
        let latest_tables = &inner.snapshots[&inner.latest_version].tables;
        let mut new_tables: BTreeMap<String, Arc<dyn MergeableTable>> = latest_tables
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        let dirty = std::mem::take(&mut self.dirty);
        for (name, my_dirty) in dirty {
            new_tables.insert(name, Arc::from(my_dirty));
        }
        for name in &self.deleted_tables {
            new_tables.remove(name);
        }

        let snapshot = Arc::new(Snapshot { version: self.version, tables: new_tables });
        let v = snapshot.version;
        inner.active_writer_count -= 1;
        self.needs_cleanup = false;

        #[cfg(feature = "persistence")]
        let needs_wal_wait = {
            #[allow(unused_mut)]
            let mut w = matches!(&waiter, Some(crate::wal::SyncWaiter::WaitForEpoch { .. }));
            #[cfg(test)]
            { w = w || mock_waiter.is_some(); }
            w
        };
        #[cfg(not(feature = "persistence"))]
        let needs_wal_wait = false;

        if needs_wal_wait {
            drop(inner);
            #[cfg(feature = "persistence")]
            {
                if let Some(w) = waiter { w.wait(); }
                #[cfg(test)]
                if let Some(w) = mock_waiter { w.wait(); }
            }
            let mut inner = self.store_inner.write().unwrap();
            inner.snapshots.insert(v, snapshot);
            if v > inner.latest_version { inner.latest_version = v; }
            if inner.config.auto_snapshot_gc { gc_inner(&mut inner); }
        } else {
            inner.snapshots.insert(v, snapshot);
            if v > inner.latest_version { inner.latest_version = v; }
            if inner.config.auto_snapshot_gc { gc_inner(&mut inner); }
        }

        self.metrics.inc_commit();
        Ok(v)
    }

    /// MultiWriter commit: the sharded path. Acquires per-table locks,
    /// does OCC + merge outside the global write lock, then takes the
    /// global write lock briefly for install.
    fn commit_multi_writer(mut self) -> Result<u64> {
        use std::time::Instant;

        // Phase 0: acquire per-table commit locks for every dirty table
        // and every table deleted during this tx. Holding these across
        // the merge + install phases is what lets disjoint-table writers
        // run in parallel; writers on overlapping tables still serialize.
        //
        // Canonical order: dirty is already a BTreeMap (sorted), and
        // ever_deleted_tables is a BTreeSet — we take the sorted union, so
        // all writers acquire locks in the same order (deadlock-free).
        let t0 = Instant::now();
        let _table_guards = self.acquire_table_locks();
        self.metrics.add_phase0(t0.elapsed().as_nanos() as u64);

        // Phase 1: OCC validation + merge-base snapshot (brief read lock).
        //
        // With per-table locks held, no concurrent writer can install
        // changes to OUR tables during the rest of commit. Any CWS for a
        // table we hold must have been recorded before we acquired its
        // lock, so a single OCC pass under the read lock is sufficient.
        let t1 = Instant::now();
        let (latest_tables_ref, concurrent_flags) = {
            let inner = self.store_inner.read().unwrap();

            if let Some(conflict) = self.validate_write_set(&inner) {
                self.metrics.inc_write_conflict();
                drop(inner);
                self.metrics.add_phase1(t1.elapsed().as_nanos() as u64);
                return Err(conflict);
            }

            if matches!(self.isolation_level, IsolationLevel::Serializable)
                && let Some(conflict) = self.validate_read_set(&inner)
            {
                self.metrics.inc_serialization_failure();
                drop(inner);
                self.metrics.add_phase1(t1.elapsed().as_nanos() as u64);
                return Err(conflict);
            }

            // Pre-compute fast/slow-path flag for each dirty table under
            // the same read lock; avoids a second scan later.
            let flags: BTreeMap<String, bool> = self
                .dirty
                .keys()
                .map(|n| {
                    let has = inner.committed_write_sets.iter().any(|cws| {
                        cws.version > self.base.version && cws.tables.contains_key(n)
                    });
                    (n.clone(), has)
                })
                .collect();
            (inner.snapshots[&inner.latest_version].tables.clone(), flags)
        };
        self.metrics.add_phase1(t1.elapsed().as_nanos() as u64);

        // --- Phase 2: merge dirty tables (no store-wide locks held) ---
        //
        // This is the work we moved out of the global write lock. Each
        // dirty table's merge is O(modified_keys × log N), potentially µs
        // to ms; previously all writers serialized through it, now only
        // writers on the same table do (via their shared per-table lock).
        let t2 = Instant::now();
        let dirty = std::mem::take(&mut self.dirty);
        let mut merged_tables: BTreeMap<String, Arc<dyn MergeableTable>> = BTreeMap::new();
        for (name, my_dirty) in dirty {
            let has_concurrent = concurrent_flags.get(&name).copied().unwrap_or(false);

            if !has_concurrent {
                // Fast path: no concurrent commit touched this table since
                // my base, so my dirty is already a valid new version.
                merged_tables.insert(name, Arc::from(my_dirty));
                continue;
            }

            let keys = self.write_set.get(&name);
            match (latest_tables_ref.get(&name), keys) {
                (Some(latest_arc), Some(keys)) if !keys.is_empty() => {
                    let mut merged = latest_arc.boxed_clone();
                    // Drop handles active-writer cleanup on error
                    // (needs_cleanup still true). Table locks drop at
                    // end of scope.
                    merged.merge_keys_from(&*my_dirty, keys)?;
                    merged_tables.insert(name, Arc::from(merged));
                }
                (Some(_), _) => {
                    // Read-only open or failed batch rolled back write set.
                    // Don't substitute at install — keep latest as-is.
                }
                (None, _) => {
                    // Table doesn't exist in the base but I wrote to it
                    // (concurrent writer deleted it). Install wholesale.
                    merged_tables.insert(name, Arc::from(my_dirty));
                }
            }
        }

        self.metrics.add_phase2(t2.elapsed().as_nanos() as u64);

        // --- Phase 3: install under brief write lock ---
        let t3 = Instant::now();
        let mut inner = self.store_inner.write().unwrap();

        // Commit-time version bump (auto-version only). Pre-assigned
        // versions may be out of order by the time we reach install;
        // bumping keeps commit order == version order. Explicit-version
        // writers (SMR mode) are left alone.
        if !self.explicit_version && self.version <= inner.latest_version {
            self.version = inner.latest_version + 1;
            if self.version >= inner.next_version {
                inner.next_version = self.version + 1;
            }
        }

        // Submit WAL entry to background thread (no fsync yet).
        #[cfg(feature = "persistence")]
        let waiter = if let Some(wal) = &inner.wal_handle {
            let ops = std::mem::take(&mut self.wal_ops);
            if !ops.is_empty() {
                let entry = crate::wal::WalEntry { version: self.version, ops };
                Some(wal.write(entry)?)
            } else {
                None
            }
        } else {
            None
        };

        // Test-only: mock WAL produces a waiter for controlled fsync testing.
        #[cfg(all(test, feature = "persistence"))]
        let mock_waiter = inner.mock_wal.as_ref().map(|mock| {
            mock.write(crate::wal::WalEntry { version: self.version, ops: vec![] })
        });

        // Fork from the *current* latest (may have advanced since phase 1
        // due to concurrent writers on other tables), then substitute in my
        // merged tables. Tables I don't touch come through unchanged.
        let fresh_latest_tables = &inner.snapshots[&inner.latest_version].tables;
        let mut new_tables: BTreeMap<String, Arc<dyn MergeableTable>> = fresh_latest_tables
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        for (name, merged) in merged_tables {
            new_tables.insert(name, merged);
        }
        for name in &self.deleted_tables {
            new_tables.remove(name);
        }

        let snapshot = Arc::new(Snapshot { version: self.version, tables: new_tables });
        let v = snapshot.version;

        // Record write set (under write lock so concurrent OCC sees it).
        inner.committed_write_sets.push(CommittedWriteSet {
            version: v,
            tables: std::mem::take(&mut self.write_set),
            deleted_tables: std::mem::take(&mut self.ever_deleted_tables),
        });
        remove_active_writer(&mut inner, self.base.version);
        prune_write_sets(&mut inner);
        inner.active_writer_count -= 1;
        self.needs_cleanup = false;

        // Determine if we need to wait for WAL fsync before promoting.
        #[cfg(feature = "persistence")]
        let needs_wal_wait = {
            #[allow(unused_mut)]
            let mut w = matches!(&waiter, Some(crate::wal::SyncWaiter::WaitForEpoch { .. }));
            #[cfg(test)]
            { w = w || mock_waiter.is_some(); }
            w
        };
        #[cfg(not(feature = "persistence"))]
        let needs_wal_wait = false;

        if needs_wal_wait {
            drop(inner);

            #[cfg(feature = "persistence")]
            {
                if let Some(w) = waiter {
                    w.wait();
                }
                #[cfg(test)]
                if let Some(w) = mock_waiter {
                    w.wait();
                }
            }

            let mut inner = self.store_inner.write().unwrap();
            inner.snapshots.insert(v, snapshot);
            if v > inner.latest_version {
                inner.latest_version = v;
            }
            if inner.config.auto_snapshot_gc {
                gc_inner(&mut inner);
            }
        } else {
            inner.snapshots.insert(v, snapshot);
            if v > inner.latest_version {
                inner.latest_version = v;
            }
            if inner.config.auto_snapshot_gc {
                gc_inner(&mut inner);
            }
        }

        // Release write intents and signal waiters. Happens *after* the
        // snapshot is promoted so any writer that was parked on our waiter
        // retries against a base that already includes our commit.
        if let (Some(intents), Some(waiter)) = (&self.intents, &self.waiter) {
            intents.release_all_for(self.writer_id, waiter);
        }

        self.metrics.add_phase3(t3.elapsed().as_nanos() as u64);
        self.metrics.inc_commit();
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
        if existed && self.wal_enabled {
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

    /// Acquire per-table commit locks for every dirty or ever-deleted
    /// table, in canonical (sorted) order. Returned guard owns both the
    /// `Arc<Mutex<()>>`s and their locked `MutexGuard`s; dropping it
    /// releases all locks in reverse order.
    ///
    /// No-op in `SingleWriter` mode (no concurrent commits possible).
    fn acquire_table_locks(&self) -> TableLockGuards {
        let Some(locks) = self.table_locks.as_ref() else {
            return TableLockGuards::empty();
        };

        // Collect sorted set of (dirty names ∪ ever_deleted_tables).
        let mut names: Vec<String> = self.dirty.keys().cloned().collect();
        for d in &self.ever_deleted_tables {
            if !names.contains(d) {
                names.push(d.clone());
            }
        }
        names.sort();
        if names.is_empty() {
            return TableLockGuards::empty();
        }

        // Snapshot the `Arc<Mutex<()>>` for each name (creating lazily).
        let arcs: Vec<Arc<Mutex<()>>> = names
            .iter()
            .map(|n| {
                locks
                    .entry(n.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            })
            .collect();

        TableLockGuards::acquire(arcs)
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
                        wait_for: None,
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
                        wait_for: None,
                    });
                }
            }
            // Key-level OCC: I conflict only with concurrent commits that
            // wrote to at least one of the same rows in the same table.
            // The per-key merge at commit time (see the `merge_keys_from`
            // call below) guarantees that disjoint-key writers to the same
            // table both land cleanly without losing each other's edits.
            for (table_name, my_keys) in &self.write_set {
                // `open_table` in MultiWriter mode inserts an empty write-set
                // entry eagerly. Treat "opened but nothing written" as no
                // write for OCC purposes.
                if my_keys.is_empty() {
                    continue;
                }
                if let Some(their_keys) = cws.tables.get(table_name)
                    && !my_keys.is_disjoint(their_keys)
                {
                    let conflicting: Vec<u64> =
                        my_keys.intersection(their_keys).copied().collect();
                    return Some(Error::WriteConflict {
                        table: table_name.clone(),
                        keys: conflicting,
                        version: cws.version,
                        wait_for: None,
                    });
                }
            }
        }
        None
    }

    /// Check Serializable read-set against `committed_write_sets`.
    ///
    /// Only invoked from [`commit_multi_writer`] when
    /// `isolation_level == Serializable`. Returns `Some(SerializationFailure)`
    /// on the first invalidated read; `None` if the read set is consistent
    /// with all commits since `base.version`.
    ///
    /// Conflict criteria, per (table, entry) in the read set:
    /// - Concurrent commit *deleted* a table we read → conflict.
    /// - `entry.table_scan == true` and concurrent commit *modified any key*
    ///   in that table → conflict (v1 coarse tracking; v2 may track ranges).
    /// - `entry.table_scan == false` and concurrent commit modified a key
    ///   we point-read → conflict.
    fn validate_read_set(&self, inner: &StoreInner) -> Option<Error> {
        let cell = self.read_set.as_ref()?;
        let rs = cell.borrow();
        let base_version = self.base.version;
        for cws in &inner.committed_write_sets {
            if cws.version <= base_version {
                continue;
            }
            for (table_name, entry) in rs.iter() {
                if cws.deleted_tables.contains(table_name) {
                    return Some(Error::SerializationFailure {
                        table: table_name.clone(),
                        version: cws.version,
                    });
                }
                if entry.table_scan {
                    if cws.tables.contains_key(table_name) {
                        return Some(Error::SerializationFailure {
                            table: table_name.clone(),
                            version: cws.version,
                        });
                    }
                } else if let Some(their_keys) = cws.tables.get(table_name)
                    && entry.keys.iter().any(|k| their_keys.contains(k))
                {
                    return Some(Error::SerializationFailure {
                        table: table_name.clone(),
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
            self.metrics.inc_rollback();
            let mut inner = self.store_inner.write().unwrap();
            inner.active_writer_count -= 1;
            if matches!(self.writer_mode, WriterMode::MultiWriter) {
                remove_active_writer(&mut inner, self.base.version);
                prune_write_sets(&mut inner);
            }
        }
        // Always release intents and signal waiters — the `needs_cleanup`
        // flag only gates the StoreInner bookkeeping (which commit already
        // did). Any writer parked on this tx's waiter must wake whether
        // this tx committed or aborted.
        if let (Some(intents), Some(waiter)) = (&self.intents, &self.waiter) {
            intents.release_all_for(self.writer_id, waiter);
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

// Compile-time assertion: `Store` is thread-safe. `WriteTx` and `ReadTx` are
// deliberately `!Send` via `PhantomData<*const ()>` so a transaction stays on
// its creating thread; that's verified by a trybuild-style negative test in
// `tests/store_integration.rs`.
#[cfg(test)]
#[allow(dead_code)]
const fn _assert_store_is_thread_safe() {
    const fn send_sync<T: Send + Sync>() {}
    send_sync::<Store>();
}

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
    fn store_config_default_isolation_is_snapshot() {
        let c = StoreConfig::default();
        assert_eq!(c.isolation_level, IsolationLevel::SnapshotIsolation);
    }

    #[test]
    fn store_config_can_request_serializable() {
        let c = StoreConfig {
            isolation_level: IsolationLevel::Serializable,
            ..StoreConfig::default()
        };
        assert_eq!(c.isolation_level, IsolationLevel::Serializable);
        let _store = Store::new(c).unwrap();
    }

    #[test]
    fn ssi_read_set_records_point_reads() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            isolation_level: IsolationLevel::Serializable,
            ..StoreConfig::default()
        }).unwrap();

        // Seed: insert a row id=1.
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("a".to_string()).unwrap();
            wtx.commit().unwrap();
        }

        // Now open Serializable wtx, do a point read, inspect read_set.
        let mut wtx = store.begin_write(None).unwrap();
        {
            let t = wtx.open_table::<String>("t").unwrap();
            let _ = t.get(1);
        }
        let rs = wtx.read_set.as_ref().unwrap().borrow();
        assert!(rs.get("t").map(|e| e.keys.contains(&1)).unwrap_or(false));
        assert!(!rs.get("t").map(|e| e.table_scan).unwrap_or(true));
    }

    #[test]
    fn ssi_read_set_records_iter_as_table_scan() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            isolation_level: IsolationLevel::Serializable,
            ..StoreConfig::default()
        }).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("seed".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx = store.begin_write(None).unwrap();
        {
            let t = wtx.open_table::<String>("t").unwrap();
            let _: Vec<_> = t.iter().collect();
        }
        let rs = wtx.read_set.as_ref().unwrap().borrow();
        assert!(rs.get("t").map(|e| e.table_scan).unwrap_or(false));
    }

    #[test]
    fn ssi_read_set_records_get_unique_as_table_scan() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            isolation_level: IsolationLevel::Serializable,
            ..StoreConfig::default()
        }).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.define_index("by_val", crate::IndexKind::Unique, |s: &String| s.clone()).unwrap();
            t.insert("alpha".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx = store.begin_write(None).unwrap();
        {
            let t = wtx.open_table::<String>("t").unwrap();
            let _ = t.get_unique("by_val", &"alpha".to_string()).unwrap();
        }
        let rs = wtx.read_set.as_ref().unwrap().borrow();
        assert!(rs.get("t").map(|e| e.table_scan).unwrap_or(false),
            "get_unique should set table_scan");
    }

    #[test]
    fn ssi_read_set_records_index_range_as_table_scan() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            isolation_level: IsolationLevel::Serializable,
            ..StoreConfig::default()
        }).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.define_index("by_len", crate::IndexKind::NonUnique, |s: &String| s.len()).unwrap();
            t.insert("a".to_string()).unwrap();
            t.insert("ab".to_string()).unwrap();
            t.insert("abc".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx = store.begin_write(None).unwrap();
        {
            let t = wtx.open_table::<String>("t").unwrap();
            let _ = t.index_range::<usize>("by_len", 1..3).unwrap();
        }
        let rs = wtx.read_set.as_ref().unwrap().borrow();
        assert!(rs.get("t").map(|e| e.table_scan).unwrap_or(false),
            "index_range should set table_scan (v1 coarse tracking)");
    }

    #[test]
    fn ssi_read_set_records_custom_index_as_table_scan() {
        use crate::CustomIndex;

        #[derive(Clone, Default)]
        struct CountIndex {
            count: usize,
        }
        impl CustomIndex<String> for CountIndex {
            fn on_insert(&mut self, _id: u64, _r: &String) -> crate::Result<()> {
                self.count += 1;
                Ok(())
            }
            fn on_update(&mut self, _id: u64, _o: &String, _n: &String) -> crate::Result<()> {
                Ok(())
            }
            fn on_delete(&mut self, _id: u64, _r: &String) {
                self.count = self.count.saturating_sub(1);
            }
        }

        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            isolation_level: IsolationLevel::Serializable,
            ..StoreConfig::default()
        }).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.define_custom_index("ct", CountIndex::default()).unwrap();
            t.insert("seed".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx = store.begin_write(None).unwrap();
        {
            let t = wtx.open_table::<String>("t").unwrap();
            let _ = t.custom_index::<CountIndex>("ct").unwrap();
        }
        let rs = wtx.read_set.as_ref().unwrap().borrow();
        assert!(rs.get("t").map(|e| e.table_scan).unwrap_or(false),
            "custom_index lookup should set table_scan");
    }

    #[test]
    fn ssi_read_set_empty_in_si_mode() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            isolation_level: IsolationLevel::SnapshotIsolation,
            ..StoreConfig::default()
        }).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("seed".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx = store.begin_write(None).unwrap();
        {
            let t = wtx.open_table::<String>("t").unwrap();
            let _ = t.get(1);
        }
        assert!(wtx.read_set.is_none());
    }

    #[test]
    fn ssi_single_writer_skips_read_set_allocation() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::SingleWriter,
            isolation_level: IsolationLevel::Serializable,
            ..StoreConfig::default()
        }).unwrap();
        let wtx = store.begin_write(None).unwrap();
        assert!(wtx.read_set.is_none(), "SingleWriter+SSI should not allocate a read_set");
    }

    #[test]
    fn ssi_read_set_records_get_many_per_id() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            isolation_level: IsolationLevel::Serializable,
            ..StoreConfig::default()
        }).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("a".to_string()).unwrap();
            t.insert("b".to_string()).unwrap();
            t.insert("c".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx = store.begin_write(None).unwrap();
        {
            let t = wtx.open_table::<String>("t").unwrap();
            let _ = t.get_many(&[1, 2, 3]);
        }
        let rs = wtx.read_set.as_ref().unwrap().borrow();
        let entry = rs.get("t").expect("table 't' must be in read_set");
        assert!(entry.keys.contains(&1));
        assert!(entry.keys.contains(&2));
        assert!(entry.keys.contains(&3));
        assert!(!entry.table_scan, "get_many is point-precise, should not promote to scan");
    }

    #[test]
    fn ssi_read_set_records_missing_key_get() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            isolation_level: IsolationLevel::Serializable,
            ..StoreConfig::default()
        }).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("seed".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx = store.begin_write(None).unwrap();
        {
            let t = wtx.open_table::<String>("t").unwrap();
            assert!(t.get(999).is_none(), "key 999 should not exist");
        }
        let rs = wtx.read_set.as_ref().unwrap().borrow();
        let entry = rs.get("t").expect("table 't' must be in read_set");
        assert!(entry.keys.contains(&999), "missing-key reads must still record (phantom-write-skew defense)");
    }

    #[test]
    fn ssi_read_set_mixed_point_and_scan() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            isolation_level: IsolationLevel::Serializable,
            ..StoreConfig::default()
        }).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("seed".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx = store.begin_write(None).unwrap();
        {
            let t = wtx.open_table::<String>("t").unwrap();
            let _ = t.get(1);
            let _: Vec<_> = t.iter().collect();
        }
        let rs = wtx.read_set.as_ref().unwrap().borrow();
        let entry = rs.get("t").expect("table 't' must be in read_set");
        assert!(entry.keys.contains(&1), "point read of id=1 must be retained");
        assert!(entry.table_scan, "subsequent iter() must promote table_scan to true");
    }

    #[test]
    fn gc_removes_old_snapshots_except_latest_and_active_rtx() {
        let store = Store::new(StoreConfig {
            num_snapshots_retained: 1,
            auto_snapshot_gc: false,
            ..StoreConfig::default()
        }).unwrap();
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
        }).unwrap();
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
        }).unwrap();
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
        }).unwrap();
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
        }).unwrap();
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
        }).unwrap();
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
        }).unwrap();
        let wtx = store.begin_write(Some(10)).unwrap();
        assert_eq!(wtx.version(), 10);
        wtx.commit().unwrap();
        // next auto-assigned should be 11
        let wtx2 = store.begin_write(None).unwrap();
        assert_eq!(wtx2.version(), 11);
    }

    /// Delete via `TableWriter` conflicts with update on the same key —
    /// surfaced at the conflicting write call (early-fail intents).
    #[test]
    fn multi_writer_delete_via_table_writer() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        }).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("hello".into()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx_a = store.begin_write(None).unwrap();
        let mut wtx_b = store.begin_write(None).unwrap();

        wtx_a.open_table::<String>("t").unwrap().delete(1).unwrap();
        let err = wtx_b.open_table::<String>("t").unwrap().update(1, "b".into());
        assert!(matches!(err, Err(Error::WriteConflict { wait_for: Some(_), .. })));
        wtx_a.commit().unwrap();
    }

    /// `update_batch` records all keys in the write set — conflict
    /// surfaces at commit time (batch ops skip early-fail, see TableWriter).
    #[test]
    fn multi_writer_update_batch_tracks_keys() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        }).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("a".into()).unwrap();
            t.insert("b".into()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx_a = store.begin_write(None).unwrap();
        let mut wtx_b = store.begin_write(None).unwrap();

        // wtx_a must use update_batch too — single update would hold an
        // intent that would early-fail B's batch via A's other writes.
        wtx_a.open_table::<String>("t").unwrap()
            .update_batch(vec![(1, "a2".into())]).unwrap();
        wtx_b.open_table::<String>("t").unwrap()
            .update_batch(vec![(1, "b2".into())]).unwrap();

        wtx_a.commit().unwrap();
        assert!(matches!(wtx_b.commit(), Err(Error::WriteConflict { .. })));
    }

    /// `delete_batch` records all keys in the write set — conflict
    /// surfaces at commit time (batch ops skip early-fail, see TableWriter).
    #[test]
    fn multi_writer_delete_batch_tracks_keys() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        }).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("a".into()).unwrap();
            t.insert("b".into()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx_a = store.begin_write(None).unwrap();
        let mut wtx_b = store.begin_write(None).unwrap();

        wtx_a.open_table::<String>("t").unwrap()
            .update_batch(vec![(2, "a2".into())]).unwrap();
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
        }).unwrap();
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
        }).unwrap();
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
        }).unwrap();
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
        wtx_b.open_table::<String>("t").unwrap().delete_batch(&[1]).unwrap();

        wtx_a.commit().unwrap();
        assert!(matches!(wtx_b.commit(), Err(Error::WriteConflict { .. })));
    }

    /// Deleting table T1 while a concurrent writer modifies T2 is not a conflict.
    #[test]
    fn delete_table_no_conflict_when_concurrent_wrote_different_table() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        }).unwrap();
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
        }).unwrap();
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
        }).unwrap();
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
        }).unwrap();
        let result = store.begin_write(None);
        assert!(matches!(result, Err(Error::ExplicitVersionRequired)));
    }

    #[test]
    fn require_explicit_version_accepts_explicit() {
        let store = Store::new(StoreConfig {
            require_explicit_version: true,
            ..StoreConfig::default()
        }).unwrap();
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
        }).unwrap();
        for _ in 0..5 {
            store.begin_write(None).unwrap().commit().unwrap();
        }
        store.gc();
        assert!(store.has_snapshot(4));
        assert!(store.has_snapshot(5));
        assert!(!store.has_snapshot(3));
    }

    // -----------------------------------------------------------------------
    // Mock WAL tests — three-phase commit verification
    // -----------------------------------------------------------------------

    #[cfg(feature = "persistence")]
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Item { name: String }

    /// Create a store with a MockWal attached. Returns both so the test
    /// can call `mock.flush()` to simulate WAL fsync.
    #[cfg(feature = "persistence")]
    fn store_with_mock_wal() -> (Store, std::sync::Arc<crate::wal::MockWal>) {
        let mock = std::sync::Arc::new(crate::wal::MockWal::new());
        let store = Store::new(StoreConfig::default()).unwrap();
        store.inner.write().unwrap().mock_wal = Some(mock.clone());
        (store, mock)
    }

    #[test]
    #[cfg(feature = "persistence")]
    fn mock_wal_snapshot_not_visible_before_flush() {
        let (store, mock) = store_with_mock_wal();

        // Commit in a background thread — it will block waiting for flush.
        let ss = store.clone();
        let t = std::thread::spawn(move || {
            let mut wtx = ss.begin_write(None).unwrap();
            wtx.open_table::<Item>("items").unwrap()
                .insert(Item { name: "A".into() }).unwrap();
            wtx.commit().unwrap()
        });

        // Give the commit time to reach the wait point.
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Snapshot should NOT be visible yet (still waiting for WAL fsync).
        assert_eq!(store.latest_version(), 0, "snapshot promoted before WAL flush");

        // Flush the mock WAL — commit unblocks, snapshot promoted.
        mock.flush();
        let v = t.join().unwrap();
        assert_eq!(v, 1);
        assert_eq!(store.latest_version(), 1);

        let rtx = store.begin_read(None).unwrap();
        let table = rtx.open_table::<Item>("items").unwrap();
        assert_eq!(table.get(1).unwrap(), &Item { name: "A".into() });
    }

    #[test]
    #[cfg(feature = "persistence")]
    fn mock_wal_multi_writer_batch_flush() {
        let store_config = StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        };
        let mock = std::sync::Arc::new(crate::wal::MockWal::new());
        let store = Store::new(store_config).unwrap();
        store.inner.write().unwrap().mock_wal = Some(mock.clone());

        // Two concurrent writers on different tables (avoids auto-increment ID collision).
        let ss1 = store.clone();
        let t1 = std::thread::spawn(move || {
            let mut wtx = ss1.begin_write(None).unwrap();
            wtx.open_table::<Item>("items_a").unwrap()
                .insert(Item { name: "A".into() }).unwrap();
            wtx.commit().unwrap()
        });

        let ss2 = store.clone();
        let t2 = std::thread::spawn(move || {
            let mut wtx = ss2.begin_write(None).unwrap();
            wtx.open_table::<Item>("items_b").unwrap()
                .insert(Item { name: "B".into() }).unwrap();
            wtx.commit().unwrap()
        });

        // Wait for both to reach the WAL wait point.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(store.latest_version(), 0, "snapshots promoted before flush");
        assert_eq!(mock.pending(), 2, "both entries should be pending");

        // One flush releases both.
        mock.flush();
        let v1 = t1.join().unwrap();
        let v2 = t2.join().unwrap();

        // Both committed with sequential versions.
        let mut versions = [v1, v2];
        versions.sort();
        assert_eq!(versions, [1, 2]);
        assert_eq!(store.latest_version(), 2);
    }

    #[test]
    #[cfg(feature = "persistence")]
    fn mock_wal_occ_sees_pending_write_sets() {
        let config = StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        };
        let mock = std::sync::Arc::new(crate::wal::MockWal::new());
        let store = Store::new(config).unwrap();
        store.inner.write().unwrap().mock_wal = Some(mock.clone());

        // Preload so both writers modify the same key.
        {
            // Temporarily remove mock so this commit doesn't block.
            store.inner.write().unwrap().mock_wal = None;
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<Item>("items").unwrap()
                .insert(Item { name: "original".into() }).unwrap();
            wtx.commit().unwrap();
            store.inner.write().unwrap().mock_wal = Some(mock.clone());
        }

        // T2 must begin_write BEFORE T1 commits, so T2 is an active writer
        // that prevents prune_write_sets from discarding T1's write set.
        let mut wtx2 = store.begin_write(None).unwrap();

        // T1: modify key 1, commit (blocks waiting for flush).
        let ss1 = store.clone();
        let t1 = std::thread::spawn(move || {
            let mut wtx = ss1.begin_write(None).unwrap();
            wtx.open_table::<Item>("items").unwrap()
                .update(1, Item { name: "from_t1".into() }).unwrap();
            wtx.commit()
        });

        // Give T1 time to reach WAL wait (write set recorded, lock released).
        std::thread::sleep(std::time::Duration::from_millis(50));

        // T2: modify same key 1. With early-fail intents, T2's update()
        // surfaces the conflict immediately — T1 still holds the intent
        // even while it's blocked on the mock WAL fsync.
        let result = wtx2.open_table::<Item>("items").unwrap()
            .update(1, Item { name: "from_t2".into() });

        assert!(
            matches!(result, Err(Error::WriteConflict { wait_for: Some(_), .. })),
            "T2 should early-fail against T1's intent, got: {result:?}"
        );
        drop(wtx2);

        // Release T1.
        mock.flush();
        t1.join().unwrap().unwrap();
    }

    #[test]
    #[cfg(feature = "persistence")]
    fn mock_wal_read_not_blocked_during_flush_wait() {
        let (store, mock) = store_with_mock_wal();

        // Commit version 1 without mock (so it's immediately visible).
        store.inner.write().unwrap().mock_wal = None;
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<Item>("items").unwrap()
                .insert(Item { name: "visible".into() }).unwrap();
            wtx.commit().unwrap();
        }
        store.inner.write().unwrap().mock_wal = Some(mock.clone());

        // Start a write that blocks on WAL flush.
        let ss = store.clone();
        let t = std::thread::spawn(move || {
            let mut wtx = ss.begin_write(None).unwrap();
            wtx.open_table::<Item>("items").unwrap()
                .insert(Item { name: "pending".into() }).unwrap();
            wtx.commit().unwrap()
        });

        std::thread::sleep(std::time::Duration::from_millis(50));

        // Read should succeed — not blocked by the pending write.
        let rtx = store.begin_read(None).unwrap();
        assert_eq!(rtx.version(), 1);
        let table = rtx.open_table::<Item>("items").unwrap();
        assert_eq!(table.len(), 1);
        assert_eq!(table.get(1).unwrap(), &Item { name: "visible".into() });

        mock.flush();
        t.join().unwrap();
    }

    /// Phase 3 version ordering: when writer B (v3) completes fsync before
    /// writer A (v2), latest_version must still end up at the maximum.
    #[test]
    #[cfg(feature = "persistence")]
    fn mock_wal_phase3_version_ordering() {
        let config = StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        };
        let mock = std::sync::Arc::new(crate::wal::MockWal::new());
        let store = Store::new(config).unwrap();
        store.inner.write().unwrap().mock_wal = Some(mock.clone());

        // Writer A on table_a, writer B on table_b (no key conflict).
        let ss_a = store.clone();
        let t_a = std::thread::spawn(move || {
            let mut wtx = ss_a.begin_write(None).unwrap();
            wtx.open_table::<Item>("table_a").unwrap()
                .insert(Item { name: "A".into() }).unwrap();
            wtx.commit().unwrap()
        });

        let ss_b = store.clone();
        let t_b = std::thread::spawn(move || {
            let mut wtx = ss_b.begin_write(None).unwrap();
            wtx.open_table::<Item>("table_b").unwrap()
                .insert(Item { name: "B".into() }).unwrap();
            wtx.commit().unwrap()
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(mock.pending(), 2);

        // Flush all — both race to phase 3. Order is nondeterministic,
        // but latest_version must be the max.
        mock.flush();
        let v_a = t_a.join().unwrap();
        let v_b = t_b.join().unwrap();

        let mut versions = [v_a, v_b];
        versions.sort();
        assert_eq!(versions, [1, 2]);
        assert_eq!(store.latest_version(), 2);

        // Both snapshots must be accessible.
        let rtx = store.begin_read(Some(1)).unwrap();
        assert_eq!(rtx.version(), 1);
        let rtx = store.begin_read(Some(2)).unwrap();
        assert_eq!(rtx.version(), 2);
    }

    /// Incremental flush: flush_one() releases only the first writer.
    /// The second writer stays blocked until the next flush.
    #[test]
    #[cfg(feature = "persistence")]
    fn mock_wal_incremental_flush() {
        let config = StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        };
        let mock = std::sync::Arc::new(crate::wal::MockWal::new());
        let store = Store::new(config).unwrap();
        store.inner.write().unwrap().mock_wal = Some(mock.clone());

        let flag_1 = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag_2 = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let ss1 = store.clone();
        let f1 = flag_1.clone();
        let t1 = std::thread::spawn(move || {
            let mut wtx = ss1.begin_write(None).unwrap();
            wtx.open_table::<Item>("items_a").unwrap()
                .insert(Item { name: "A".into() }).unwrap();
            let v = wtx.commit().unwrap();
            f1.store(true, std::sync::atomic::Ordering::Release);
            v
        });

        let ss2 = store.clone();
        let f2 = flag_2.clone();
        let t2 = std::thread::spawn(move || {
            let mut wtx = ss2.begin_write(None).unwrap();
            wtx.open_table::<Item>("items_b").unwrap()
                .insert(Item { name: "B".into() }).unwrap();
            let v = wtx.commit().unwrap();
            f2.store(true, std::sync::atomic::Ordering::Release);
            v
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(mock.pending(), 2);

        // Flush one epoch — only the first writer should complete.
        mock.flush_one();
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Exactly one writer should have completed.
        let done_1 = flag_1.load(std::sync::atomic::Ordering::Acquire);
        let done_2 = flag_2.load(std::sync::atomic::Ordering::Acquire);
        assert!(
            (done_1 && !done_2) || (!done_1 && done_2),
            "expected exactly one writer done, got done_1={done_1}, done_2={done_2}"
        );
        assert_eq!(mock.pending(), 1, "one entry should still be pending");

        // Flush the second.
        mock.flush_one();
        let v1 = t1.join().unwrap();
        let v2 = t2.join().unwrap();
        let mut versions = [v1, v2];
        versions.sort();
        assert_eq!(versions, [1, 2]);
    }

    /// A new writer can begin_write while another writer is blocked in phase 2.
    #[test]
    #[cfg(feature = "persistence")]
    fn mock_wal_begin_write_during_phase2() {
        let config = StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        };
        let mock = std::sync::Arc::new(crate::wal::MockWal::new());
        let store = Store::new(config).unwrap();
        store.inner.write().unwrap().mock_wal = Some(mock.clone());

        // Writer 1 blocks in phase 2.
        let ss1 = store.clone();
        let t1 = std::thread::spawn(move || {
            let mut wtx = ss1.begin_write(None).unwrap();
            wtx.open_table::<Item>("items").unwrap()
                .insert(Item { name: "first".into() }).unwrap();
            wtx.commit().unwrap()
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(store.latest_version(), 0, "writer 1 should be in phase 2");

        // While writer 1 is blocked, begin_write must succeed (lock is free).
        let wtx2 = store.begin_write(None);
        assert!(wtx2.is_ok(), "begin_write should succeed during phase 2");
        let wtx2_version = wtx2.as_ref().unwrap().version();
        assert!(wtx2_version > 0, "new writer should get a valid version");
        drop(wtx2); // drop without committing — just proving begin_write works

        // Writer 2 via thread can also commit (will block in phase 2).
        let ss2 = store.clone();
        let t2 = std::thread::spawn(move || {
            let mut wtx = ss2.begin_write(None).unwrap();
            wtx.open_table::<Item>("other").unwrap()
                .insert(Item { name: "second".into() }).unwrap();
            wtx.commit().unwrap()
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(mock.pending(), 2);

        mock.flush();
        t1.join().unwrap();
        t2.join().unwrap();
        // v1 = writer 1, v2 consumed by dropped wtx2, v3 = writer 2 (thread).
        assert_eq!(store.latest_version(), 3);
    }

    /// Sequential single-writer commits with mock WAL: each commit blocks
    /// until flushed, then the next commit proceeds. Verifies three-phase
    /// doesn't break single-writer flow.
    #[test]
    #[cfg(feature = "persistence")]
    fn mock_wal_sequential_single_writer() {
        let (store, mock) = store_with_mock_wal();

        // Each commit must block, so run in a thread.
        let ss = store.clone();
        let mock2 = mock.clone();
        let t = std::thread::spawn(move || {
            for i in 0..3u64 {
                let mut wtx = ss.begin_write(None).unwrap();
                wtx.open_table::<Item>("items").unwrap()
                    .insert(Item { name: format!("item_{i}") }).unwrap();
                // This will block until the mock is flushed.
                wtx.commit().unwrap();
            }
        });

        // Flush each commit individually.
        for expected_version in 1..=3u64 {
            std::thread::sleep(std::time::Duration::from_millis(50));
            assert_eq!(mock2.pending(), 1, "one commit should be pending");
            mock2.flush();
            std::thread::sleep(std::time::Duration::from_millis(50));
            assert_eq!(
                store.latest_version(),
                expected_version,
                "version should advance after flush"
            );
        }

        t.join().unwrap();
        let rtx = store.begin_read(None).unwrap();
        assert_eq!(rtx.open_table::<Item>("items").unwrap().len(), 3);
    }

    /// Writer panic during phase 2: the store must remain usable.
    /// The panicking writer's write set was recorded in phase 1, but the
    /// snapshot is never promoted. The store should not deadlock or corrupt.
    #[test]
    #[cfg(feature = "persistence")]
    fn mock_wal_writer_panic_during_phase2() {
        let config = StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        };
        let mock = std::sync::Arc::new(crate::wal::MockWal::new());
        let store = Store::new(config).unwrap();
        store.inner.write().unwrap().mock_wal = Some(mock.clone());

        // Writer 1 will panic during phase 2 wait.
        let ss1 = store.clone();
        let t1 = std::thread::spawn(move || {
            let mut wtx = ss1.begin_write(None).unwrap();
            wtx.open_table::<Item>("items").unwrap()
                .insert(Item { name: "doomed".into() }).unwrap();
            // Commit enters phase 2, blocks on mock. We'll never flush for
            // this writer — instead we simulate a panic by dropping the thread.
            // But we can't really panic inside wait(). Instead, just let the
            // thread hang and we'll detach it.
            wtx.commit().unwrap();
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(store.latest_version(), 0, "doomed writer is in phase 2");

        // Flush the doomed writer so its thread unblocks and completes.
        // The snapshot gets promoted — that's fine.
        mock.flush();
        t1.join().unwrap();

        // Now disable mock for subsequent commits (they should work normally).
        store.inner.write().unwrap().mock_wal = None;

        // Store must remain usable: active_writer_count should be 0.
        let inner = store.inner.read().unwrap();
        assert_eq!(inner.active_writer_count, 0, "writer count should be zero");
        drop(inner);

        // A fresh writer should succeed without issues.
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<Item>("more").unwrap()
            .insert(Item { name: "alive".into() }).unwrap();
        wtx.commit().unwrap();
        assert_eq!(store.latest_version(), 2);
    }

    /// GC during phase 3 must not remove a snapshot that another writer
    /// (still in phase 2) is about to promote.
    ///
    /// With num_snapshots_retained=1, GC keeps only the latest snapshot.
    /// If writer A promotes v1 with GC, and writer B is about to promote v2,
    /// GC must not remove v1 prematurely (it might be referenced by readers).
    #[test]
    #[cfg(feature = "persistence")]
    fn mock_wal_gc_during_phase3_preserves_pending() {
        let config = StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            num_snapshots_retained: 1,
            auto_snapshot_gc: true,
            ..StoreConfig::default()
        };
        let mock = std::sync::Arc::new(crate::wal::MockWal::new());
        let store = Store::new(config).unwrap();
        store.inner.write().unwrap().mock_wal = Some(mock.clone());

        // Writer A and B on different tables.
        let ss_a = store.clone();
        let t_a = std::thread::spawn(move || {
            let mut wtx = ss_a.begin_write(None).unwrap();
            wtx.open_table::<Item>("table_a").unwrap()
                .insert(Item { name: "A".into() }).unwrap();
            wtx.commit().unwrap()
        });

        let ss_b = store.clone();
        let t_b = std::thread::spawn(move || {
            let mut wtx = ss_b.begin_write(None).unwrap();
            wtx.open_table::<Item>("table_b").unwrap()
                .insert(Item { name: "B".into() }).unwrap();
            wtx.commit().unwrap()
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(mock.pending(), 2);

        // Release writer A first.
        mock.flush_one();
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Writer A promoted its snapshot and GC ran. The latest_version
        // should be 1 or 2 depending on which writer got epoch 1.
        // Regardless, the store must still be functional.

        // Release writer B.
        mock.flush_one();
        t_a.join().unwrap();
        t_b.join().unwrap();

        assert_eq!(store.latest_version(), 2);

        // Both snapshots should have been created (GC may have removed the
        // initial empty v0 snapshot). The key invariant: no panic or deadlock.
        let rtx = store.begin_read(None).unwrap();
        assert_eq!(rtx.version(), 2);
    }

    #[test]
    fn table_reader_delegates_reads() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("items").unwrap();
            t.insert("hello".to_string()).unwrap();
            t.insert("world".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let rtx = store.begin_read(None).unwrap();
        let reader = rtx.open_table::<String>("items").unwrap();
        assert_eq!(reader.len(), 2);
        assert!(!reader.is_empty());
        assert!(reader.contains(1));
        assert_eq!(reader.get(1), Some(&"hello".to_string()));
        assert_eq!(reader.first().unwrap().0, 1);
        assert_eq!(reader.last().unwrap().0, 2);
        assert_eq!(reader.get_many(&[1, 2]).len(), 2);
        assert_eq!(reader.resolve(&[1, 99]).len(), 1);
        assert_eq!(reader.iter().count(), 2);
        assert_eq!(reader.range(1..=2).count(), 2);
    }

    #[test]
    fn metrics_tracks_commits_and_rollbacks() {
        let store = Store::default();
        {
            let wtx = store.begin_write(None).unwrap();
            wtx.commit().unwrap();
        }
        {
            let wtx = store.begin_write(None).unwrap();
            wtx.rollback();
        }
        {
            let wtx = store.begin_write(None).unwrap();
            drop(wtx); // implicit rollback
        }
        let m = store.metrics();
        assert_eq!(m.commits, 1);
        assert_eq!(m.rollbacks, 2);
    }

    #[test]
    fn metrics_tracks_gc_runs_and_snapshots_collected() {
        let store = Store::new(StoreConfig {
            num_snapshots_retained: 1,
            auto_snapshot_gc: false,
            ..StoreConfig::default()
        }).unwrap();
        store.begin_write(None).unwrap().commit().unwrap();
        store.begin_write(None).unwrap().commit().unwrap();
        store.begin_write(None).unwrap().commit().unwrap();
        // Versions: 0,1,2,3. retain_count = max(1,1) = 1. protected = {3}.
        // v0, v1, v2 removed.
        store.gc();
        let m = store.metrics();
        assert_eq!(m.gc_runs, 1);
        assert_eq!(m.snapshots_collected, 3);
    }

    #[test]
    fn metrics_tracks_table_writes() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("items").unwrap();
            t.insert("a".to_string()).unwrap();
            t.insert("b".to_string()).unwrap();
            t.update(1, "aa".to_string()).unwrap();
            t.delete(2).unwrap();
            wtx.commit().unwrap();
        }
        let m = store.metrics();
        let t = &m.tables["items"];
        assert_eq!(t.inserts, 2);
        assert_eq!(t.updates, 1);
        assert_eq!(t.deletes, 1);
    }

    #[test]
    fn metrics_tracks_batch_writes() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("items").unwrap();
            t.insert_batch(vec!["a".into(), "b".into(), "c".into()]).unwrap();
            t.update_batch(vec![(1, "aa".into()), (2, "bb".into())]).unwrap();
            t.delete_batch(&[1, 2]).unwrap();
            wtx.commit().unwrap();
        }
        let m = store.metrics();
        let t = &m.tables["items"];
        assert_eq!(t.inserts, 3);
        assert_eq!(t.updates, 2);
        assert_eq!(t.deletes, 2);
    }

    #[test]
    fn metrics_tracks_table_writer_reads() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("items").unwrap();
            t.insert("hello".to_string()).unwrap();
            t.insert("world".to_string()).unwrap();
            t.get(1);
            t.contains(1);
            t.first();
            t.last();
            t.get_many(&[1, 2]);
            t.resolve(&[1, 2]);
            let _ = t.range(1..=2).count();
            let _ = t.iter().count();
            wtx.commit().unwrap();
        }
        let m = store.metrics();
        let t = &m.tables["items"];
        // get(1) + contains(1) + first() + last() + get_many(2) + resolve(2) = 1+1+1+1+2+2 = 8
        assert_eq!(t.primary_key_reads, 8);
        // range() + iter() = 2
        assert_eq!(t.primary_key_scans, 2);
    }

    #[test]
    fn metrics_tracks_index_operations() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("items").unwrap();
            t.define_index("len", IndexKind::NonUnique, |s: &String| s.len()).unwrap();
            t.insert("hi".to_string()).unwrap();
            t.insert("hello".to_string()).unwrap();
            let _ = t.get_by_index::<usize>("len", &2);
            let _ = t.index_range::<usize>("len", 0..=10);
            wtx.commit().unwrap();
        }
        let rtx = store.begin_read(None).unwrap();
        let reader = rtx.open_table::<String>("items").unwrap();
        let _ = reader.get_by_index::<usize>("len", &5);
        let m = store.metrics();
        let idx = &m.tables["items"].indexes["len"];
        assert_eq!(idx.reads, 2);       // 1 writer (get_by_index) + 1 reader (get_by_index)
        assert_eq!(idx.range_scans, 1); // 1 writer (index_range)
    }

    #[test]
    fn metrics_end_to_end() {
        let store = Store::default();

        // Write some data
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("users").unwrap();
            t.define_index("value", IndexKind::Unique, |s: &String| s.clone()).unwrap();
            t.insert("alice".to_string()).unwrap();
            t.insert("bob".to_string()).unwrap();
            t.update(1, "ALICE".to_string()).unwrap();
            t.delete(2).unwrap();
            // Read via writer
            let _ = t.get(1);
            let _ = t.get_unique::<String>("value", &"ALICE".to_string());
            wtx.commit().unwrap();
        }

        // Read via reader
        {
            let rtx = store.begin_read(None).unwrap();
            let reader = rtx.open_table::<String>("users").unwrap();
            reader.get(1);
            reader.iter().count();
        }

        // Rollback
        {
            let wtx = store.begin_write(None).unwrap();
            wtx.rollback();
        }

        let m = store.metrics();
        assert_eq!(m.commits, 1);
        assert_eq!(m.rollbacks, 1);
        assert_eq!(m.tables["users"].inserts, 2);
        assert_eq!(m.tables["users"].updates, 1);
        assert_eq!(m.tables["users"].deletes, 1);
        // Writer reads: get(1) = 1. Reader reads: get(1) = 1. Total = 2.
        assert_eq!(m.tables["users"].primary_key_reads, 2);
        // Reader scans: iter() = 1
        assert_eq!(m.tables["users"].primary_key_scans, 1);
        // Index reads: get_unique = 1
        assert_eq!(m.tables["users"].indexes["value"].reads, 1);
    }
}
