# UltimaDB Architecture

UltimaDB is an embedded, versioned key-value store with snapshot-isolated transactions. Data lives in memory; durability is opt-in via the `persistence` cargo feature (WAL + checkpoints, or checkpoint-only for state-machine-replication deployments). It is written in Rust.

This document explains how UltimaDB works internally, why the design is the way it is, and where the boundaries of the current implementation lie.

---

## Overview

```
┌──────────────────────────────────────────────────────────────────┐
│  Store                                                           │
│                                                                  │
│  snapshots: BTreeMap<u64, Arc<Snapshot>>                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                       │
│  │ v0 empty │  │ v1       │  │ v2       │  ...                   │
│  └──────────┘  └──────────┘  └──────────┘                       │
│                                                                  │
│  ReadTx ──holds──▶ Arc<Snapshot v1>                              │
│  ReadTx ──holds──▶ Arc<Snapshot v2>                              │
│                                                                  │
│  WriteTx                                                         │
│    base ──────▶ Arc<Snapshot v2>   (read-only reference)         │
│    dirty ─────▶ BTreeMap<String, Box<dyn MergeableTable>>        │
│    version = 3                                                   │
└──────────────────────────────────────────────────────────────────┘

Each Snapshot contains:
  version: u64
  tables: BTreeMap<String, Arc<dyn MergeableTable>>
                            │
              ┌─────────────┘
              ▼
         Table<R>
           data: BTree<u64, R>    ◄── persistent CoW B-tree
           next_id: u64
           indexes: BTreeMap<String, Box<dyn IndexMaintainer<R>>>

         BTree<K, V>
           root: Arc<BTreeNode<K, V>>    ◄── O(1) clone
           len: usize

         BTreeNode<K, V>
           entries: Vec<(K, Arc<V>)>
           children: Vec<Arc<BTreeNode<K, V>>>
```

---

## Module structure

| Module | Purpose |
|---|---|
| `btree` | Persistent copy-on-write B-tree. `BTree<K, V>` is re-exported from the crate root as a building block for custom indexes. |
| `table` | `Table<R>` — typed collection backed by `BTree<u64, R>` with auto-incrementing IDs. Defines the `MergeableTable` trait used for type-erased storage in snapshots. |
| `index` | Secondary index infrastructure: built-in `ManagedIndex` with `KeyExtractor` pattern, and the public `CustomIndex<R>` trait for user-defined indexes. |
| `bulk_load` | Fast O(N) full restores and incremental deltas. `BTree::from_sorted` builds a packed tree; indexes rebuild from sorted data. |
| `store` | `Store`, `StoreInner`, `Snapshot`, `ReadTx`, `WriteTx`, `StoreConfig` — version history, transactions, OCC bookkeeping. |
| `transaction` | Re-exports `ReadTx` and `WriteTx` from `store` (see [circular dependency note](#circular-dependency-resolution)). |
| `intents` | Write-intent map for early-fail conflict detection in MultiWriter mode. Lives outside the commit lock so per-write checks don't serialize through `inner`. |
| `metrics` | Per-table and per-store counters (`MetricsSnapshot`, `TableMetricsSnapshot`, `IndexMetricsSnapshot`). |
| `persistence` | `Persistence` mode and `Durability` enum. `Record` trait that adds `Serialize + DeserializeOwned` bounds when the `persistence` feature is enabled. |
| `wal` | Append-only write-ahead log (Standalone mode). Background fsync for `Eventual`, in-line fsync for `Consistent`. *Feature-gated.* |
| `checkpoint` | Snapshot serialization to disk. Used for fast recovery and to bound WAL replay. *Feature-gated.* |
| `registry` | Type registry mapping table names to (de)serializers and replay shims for WAL recovery. *Feature-gated.* |
| `fulltext` | Optional full-text index. *Feature-gated on `fulltext`.* |
| `error` | `Error` enum and `Result` alias. |

---

## Persistent copy-on-write B-tree

**File:** `src/btree.rs`

The B-tree is the foundation of the entire system. It is what makes snapshot isolation cheap.

### Why not `std::BTreeMap`?

`std::BTreeMap` mutates nodes in place. If a `ReadTx` held a reference into a `BTreeMap`, a concurrent `WriteTx` modifying the same table would corrupt what the reader sees. The only options would be:

1. **Deep-copy the entire table** on every write transaction — O(n) per table open.
2. **Use a mutex** — blocks readers while a writer is active, defeating the purpose of MVCC.

A persistent B-tree avoids both problems. Mutations produce a new root by reconstructing only the O(log n) nodes along the path from root to the affected leaf. All unchanged subtrees are shared via `Arc`. The old root — and therefore the entire old version of the tree — remains intact and accessible.

### How it works

```
insert(key=7, val="x") on tree with root A:

Before:                      After (new tree):
     A [3, 5, 9]                  A' [3, 5, 9]
    / |   |   \                  / |   |    \
   B  C   D    E                B  C   D'    E     ← only D is copied
                                        │
                               (7 inserted here)

A and A' share children B, C, E via Arc.
A still exists, unmodified, for any ReadTx holding it.
```

**Parameters:** minimum degree `T = 32`, so `MAX_KEYS = 2T - 1 = 63` and `MIN_KEYS = T - 1 = 31`. These are compile-time constants in `src/btree.rs`.

**Key design choices:**

- **`Arc<R>` for values, not `R` directly.** Values are wrapped in `Arc<R>` at insertion time. This means `R` does not need to implement `Clone`. When a node is reconstructed during an insert or delete, the unchanged entries just clone their `Arc` (a pointer bump), not the value itself.

- **`Arc<BTreeNode<R>>` for children.** Same reasoning — when an internal node is reconstructed, only the modified child path gets a new `Arc`; all other children are shared.

- **Immutable mutation API.** `insert` and `remove` return a *new* `BTree<K, V>`. They do not take `&mut self`. The caller (usually `Table`) reassigns `self.data = self.data.insert(...)`. This makes the immutability guarantee structural — there is no way to accidentally mutate a tree that a reader is using.

- **Bottom-up splitting on insert.** Recursive insertion returns `InsertResult::Fit` or `InsertResult::Split`. A split propagates upward; if the root itself splits, a new single-entry root is created. This was chosen over pre-emptive (top-down) splitting because it is simpler to implement correctly with immutable nodes — you cannot "prepare" a node for splitting when you don't mutate on the way down.

- **Check-before-delete pattern.** `Table::delete` calls `self.data.get(id)` first. If the key is absent, it returns `Err(KeyNotFound)` without ever entering the tree's deletion path. This avoids the cost of CoW (copying nodes along the path) only to discover the key doesn't exist.

- **Rebalancing on delete.** When a deletion causes a node to become underfull (< MIN_KEYS entries), `fix_underfull_child` tries, in order: rotate from left sibling, rotate from right sibling, merge with left sibling, merge with right sibling. If merging propagates underfull-ness upward, the parent handles it recursively. If the root ends up empty with one child, the tree height collapses by one.

### Clone is O(1)

`BTree::clone()` increments the root `Arc`'s reference count and copies a `usize`. That's it. This is what makes `WriteTx::open_table` cheap — cloning an entire table to get a mutable working copy costs the same as cloning a single pointer.

---

## Table

**File:** `src/table.rs`

`Table<R>` is a thin wrapper around `BTree<u64, R>` that adds:

- **Auto-incrementing IDs.** Each table has a `next_id: u64` counter starting at 1. `insert` assigns the next ID and increments the counter.
- **Mutable API.** Unlike the immutable `BTree<u64, R>`, `Table<R>` uses `&mut self` methods. Internally, each mutation reassigns `self.data`:

  ```rust
  pub fn insert(&mut self, record: R) -> u64 {
      let id = self.next_id;
      self.next_id += 1;
      self.data = self.data.insert(id, record);
      id
  }
  ```

- **Error handling.** `update` and `delete` return `Result<()>` with `Error::KeyNotFound` if the ID doesn't exist.

### Clone preserves next_id

`Table::clone()` copies `next_id` along with the O(1) `BTree` clone. This ensures that when `WriteTx` forks a table from the base snapshot, subsequent inserts continue from the correct ID and never collide with existing entries.

---

## Custom indexes

**File:** `src/index.rs`

UltimaDB supports user-defined custom indexes via the `CustomIndex<R>` trait. Unlike built-in indexes (which use `KeyExtractor` + `UniqueStorage`/`NonUniqueStorage`), custom indexes have full control over their internal data structure and expose their own query API.

### How it works

1. The user implements `CustomIndex<R>` on their type, providing `on_insert`, `on_update`, and `on_delete` hooks.
2. They register it via `table.define_custom_index("name", my_index)`.
3. Internally, a `CustomIndexAdapter` wraps the custom index and implements `IndexMaintainer<R>`, so it's stored alongside built-in indexes in the same map.
4. Queries go through a typed handle: `table.custom_index::<MyIndex>("name")` returns `&MyIndex`, giving access to the index's own query methods.
5. Record resolution is separate: `table.resolve(&ids)` maps IDs to records.

### Clone and CoW

Custom indexes must implement `Clone`. For O(1) clone (critical for snapshot performance), index authors should use `BTree<K, V>` — the same persistent CoW B-tree that backs the rest of UltimaDB. `BTree` is re-exported from the crate root for this purpose.

### Persistence

Custom indexes are rebuilt from table data on recovery. The `rebuild` method (with a default implementation that iterates `on_insert`) handles both backfill-on-define and recovery-from-persistence.

---

## Store and version history

**File:** `src/store.rs`

`Store` is the entry point. It is a cheap, cloneable handle around interior-mutable state. The version history and OCC bookkeeping live in `StoreInner` behind an `RwLock`:

```rust
pub struct Store {
    inner: Arc<RwLock<StoreInner>>,
    intents: Arc<IntentMap>,            // write-intent map for early-fail OCC
    next_writer_id: Arc<AtomicU64>,
    table_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,  // per-table commit locks
}

pub(crate) struct StoreInner {
    snapshots: BTreeMap<u64, Arc<Snapshot>>,
    latest_version: u64,
    next_version: u64,
    config: StoreConfig,
    active_writer_count: usize,
    active_writer_base_versions: Vec<u64>,        // for write-set pruning
    committed_write_sets: Vec<CommittedWriteSet>, // OCC validation
    metrics: Arc<StoreMetrics>,
    // ... persistence-feature fields: wal_handle, registry
}
```

`StoreInner.snapshots` holds every retained version. The default `StoreConfig` keeps the 10 most recent snapshots and runs `gc()` automatically after each commit; this is configurable via `num_snapshots_retained` and `auto_snapshot_gc`.

The `intents` map and per-table `table_locks` exist so that disjoint-key MultiWriter commits don't serialize through `inner.write()`. See [Writer modes](#writer-modes) below and [task19](tasks/task19_key_level_occ.md).

### Snapshots

A `Snapshot` is an immutable, versioned view of all tables:

```rust
pub(crate) struct Snapshot {
    pub(crate) version: u64,
    pub(crate) tables: BTreeMap<String, Arc<dyn MergeableTable>>,
}
```

**Why `Arc<dyn MergeableTable>` for tables?**

Tables have different types (`Table<String>`, `Table<u64>`, etc.) but must coexist in a single map. `MergeableTable: Any + Send + Sync` provides type erasure plus the operations the commit path needs — `boxed_clone` (O(1) CoW) and `merge_keys_from` (per-key replay during MultiWriter rebase). Concrete `Table<R>` is recovered via `.as_any().downcast_ref::<Table<R>>()`.

`Arc` (rather than `Box`) is critical for two reasons:

1. **Snapshot sharing.** When `commit` builds a new snapshot, it starts from the latest snapshot's table map. Tables that the writer didn't touch are carried forward by cloning their `Arc` — O(1) per table, no data copying.

2. **Read transaction lifetime.** `ReadTx` holds `Arc<Snapshot>`, which keeps the snapshot (and all its `Arc<dyn MergeableTable>` table entries) alive. Multiple readers at different versions coexist without interfering with each other or the store.

The downcast to `Table<R>` happens at `open_table` time, returning `Error::TypeMismatch` if the caller's type parameter doesn't match the type the table was created with.

### Version numbering

- Version 0 is the empty store (seeded in `Store::new()`).
- `begin_write(None)` auto-assigns the next available version.
- `begin_write(Some(v))` uses an explicit version; `v` must be strictly greater than `latest_version`, otherwise `Error::WriteConflict` is returned.
- `next_version` tracks the next auto-assignable version and advances past any explicit version requests.

**Why allow explicit versions?** This supports replication and external ordering scenarios where the version stamp is determined outside the store (e.g., a distributed sequence number). Auto-assign is the common case for local use.

---

## Transactions

### ReadTx

```rust
pub struct ReadTx {
    snapshot: Arc<Snapshot>,
    metrics: Arc<StoreMetrics>,
    _not_send: PhantomData<*const ()>,  // pinned to creating thread
}
```

`ReadTx` is a read-only view pinned to a specific version. It holds an `Arc<Snapshot>`, which keeps that version's data alive independently of subsequent commits. Multiple `ReadTx` instances at different versions coexist freely.

`open_table<R>` returns a `TableReader<'_, R>` (a thin wrapper that records read metrics and downcasts to `&Table<R>`). The reader borrows from the snapshot — no copying occurs.

### WriteTx

```rust
pub struct WriteTx {
    base: Arc<Snapshot>,
    dirty: BTreeMap<String, Box<dyn MergeableTable>>,
    version: u64,
    explicit_version: bool,
    store_inner: Arc<RwLock<StoreInner>>,
    deleted_tables: BTreeSet<String>,            // cleared on re-open
    write_set: BTreeMap<String, BTreeSet<u64>>,  // MultiWriter OCC
    ever_deleted_tables: BTreeSet<String>,       // permanent for conflict check
    writer_mode: WriterMode,
    needs_cleanup: bool,                         // false after successful commit
    metrics: Arc<StoreMetrics>,
    intents: Option<Arc<IntentMap>>,             // Some only in MultiWriter
    writer_id: u64,
    waiter: Option<Arc<IntentWaiter>>,
    table_locks: Option<Arc<DashMap<String, Arc<Mutex<()>>>>>,
    isolation_level: IsolationLevel,
    read_set: Option<RefCell<BTreeMap<String, ReadSetEntry>>>,  // Serializable + MultiWriter only
    // ... persistence-feature fields: wal_ops, wal_enabled
    _not_send: PhantomData<*const ()>,
}
```

`WriteTx` implements lazy copy-on-write at the table level:

1. **First call to `open_table("t")`**: clone `Table<R>` from `base.tables["t"]` (O(1) — just an Arc bump on the BTree root) into `dirty`. If "t" doesn't exist in the base, create an empty `Table<R>`.
2. **Subsequent calls**: return a `&mut` reference to the existing dirty copy.
3. **Mutations**: all `insert`/`update`/`delete` calls go through a `TableWriter<'_, R>`, which records the modified ID in `write_set` (MultiWriter only) and registers an intent for early-fail conflict detection, then forwards to `&mut Table<R>`. The table internally reassigns `self.data` to a new `BTree<u64, R>`. None of this is visible outside the `WriteTx` until commit.

**Why `Box<dyn MergeableTable>` in dirty (not `Arc`)?** The writer needs `&mut Table<R>`, which requires exclusive ownership. `Box` gives us `downcast_mut` via `as_any_mut`. At commit time, each `Box` is converted to `Arc` via `Arc::from(boxed)` for installation into the new snapshot.

**Read-set tracking.** `read_set` is `Some` only when both `IsolationLevel::Serializable` and `WriterMode::MultiWriter` are configured. SI doesn't validate, and SingleWriter has no concurrent writers — so allocating a read set in those modes is pure overhead. See [task21](tasks/task21_serializable_isolation.md).

### Commit

`WriteTx::commit` rebases onto the latest committed snapshot — not the base it forked from — so that concurrent commits to disjoint tables (or disjoint keys within the same table under MultiWriter) don't lose updates.

1. **Conflict check (MultiWriter only).** Walk `committed_write_sets` for entries committed since `base.version`. If any of their modified `(table, key)` pairs intersect this writer's `write_set`, return `Error::WriteConflict`. Under `IsolationLevel::Serializable`, also intersect this writer's `read_set` against committed write sets — fail with `Error::SerializationFailure` if any read was invalidated.
2. **Per-table merge.** For each dirty table, take the `Arc<dyn MergeableTable>` from the *current latest* snapshot's table map. Two paths:
   - **Fast path** — if no concurrent committed writer touched this table since `base.version`, install the dirty `Box` wholesale (single `Arc::from`).
   - **Slow path** — clone the latest table (O(1) CoW), then replay only this writer's modified keys into it via `MergeableTable::merge_keys_from`, which calls `Table::upsert_arc` so secondary indexes stay consistent through the existing `on_insert`/`on_update` hooks.
3. **Snapshot install.** Build a new `Snapshot` with the merged tables for everything dirty, plus O(1) `Arc` clones of every table the writer didn't touch. Insert under the assigned `version` and bump `latest_version`.
4. **Auto-version bump.** If the writer was created with `begin_write(None)` and another commit landed at a higher version in the meantime, the assigned `version` is bumped to `latest + 1` under the commit lock so version order matches commit order. Explicit (SMR-mode) versions are left alone.
5. **Bookkeeping.** Record this transaction's `write_set` in `committed_write_sets` for future conflict checks, prune entries no in-flight writer can still need, decrement `active_writer_count`, and (with persistence) append the WAL ops and signal/wait for fsync per `Durability` mode.

Tables the writer never opened are carried forward automatically — no data is lost.

### Rollback

`WriteTx::rollback` is a no-op that drops `self`. The store is never modified. The dirty working copies are freed, and the base snapshot's reference count decrements.

---

## Isolation level

UltimaDB implements **Snapshot Isolation** by default and **Serializable Snapshot Isolation (SSI)** as an opt-in. SI prevents dirty reads, nonrepeatable reads, and phantom reads, but does *not* prevent write skew (a serialization anomaly where two concurrent transactions read overlapping data and write to disjoint subsets, producing a result impossible in any serial execution). SSI prevents write skew by tracking each `WriteTx`'s read set and aborting at commit if any read was invalidated by a concurrent committer.

Set `StoreConfig::isolation_level = IsolationLevel::Serializable` to opt in. SSI only matters under `WriterMode::MultiWriter`; under `SingleWriter` there are no concurrent writers and the level is silently equivalent to SI. v1 tracks point reads precisely; any range/scan/index read is recorded as a coarse "table touched" flag (false positives possible on read-heavy scan workloads). See [task21](tasks/task21_serializable_isolation.md).

See [isolation-levels.md](isolation-levels.md) for a detailed treatment of:

- What each SQL isolation level prevents
- Exactly which anomalies UltimaDB's SI prevents and allows
- How SSI prevents write skew on top of SI
- Concrete test patterns for verifying each guarantee

### Writer modes

`StoreConfig::writer_mode` controls concurrency:

- **`SingleWriter`** (default): at most one active `WriteTx` at a time. `begin_write` returns `Error::WriterBusy` if another is already active. No OCC tracking overhead.
- **`MultiWriter`**: multiple concurrent `WriteTx` allowed. Key-level OCC: two writers conflict only if their modified rows overlap on the same table. Disjoint rows in the same table both commit; the second commit's per-key merge pulls only its edited keys onto the current latest snapshot via the `MergeableTable` trait. Fast path: if no concurrent writer touched a given dirty table, install it wholesale (single Arc swap). See [task19_key_level_occ.md](tasks/task19_key_level_occ.md).

`Store`, `ReadTx`, and `Snapshot` are all `Send + Sync`, so the `Store` handle can be cloned across threads. `WriteTx` and `ReadTx` are deliberately `!Send` (via `PhantomData<*const ()>`) — a transaction must be opened and committed on the same thread. The intended pattern is: clone the `Store` into each thread, and call `begin_write`/`begin_read` locally.

---

## Persistence

**Files:** `src/persistence.rs`, `src/wal.rs`, `src/checkpoint.rs`, `src/registry.rs`. Gated on the `persistence` cargo feature.

By default UltimaDB is in-memory: `StoreConfig::persistence` is `Persistence::None` and the durability subsystem is compiled out entirely (no WAL handle, no registry, no on-disk files). Enabling persistence is opt-in per `Store`:

```rust
pub enum Persistence {
    /// In-memory only. No disk I/O. Default.
    None,
    /// UltimaDB owns durability. WAL for transaction durability,
    /// checkpoints for fast recovery. WAL is auto-pruned on checkpoint.
    Standalone { dir: PathBuf, durability: Durability },
    /// Consensus log owns durability. Checkpoints only — no WAL.
    /// For SMR (Raft/Paxos) deployments.
    Smr { dir: PathBuf },
}

pub enum Durability {
    /// `commit()` returns immediately; a background thread fsyncs asynchronously.
    /// Last unflushed entries may be lost on crash.
    Eventual,
    /// `commit()` blocks until the WAL entry is fsynced. No data loss on crash.
    Consistent,
}
```

### Standalone mode (WAL + checkpoints)

Each `WriteTx` accumulates `wal_ops` (insert / update / delete / create-table / delete-table) as it mutates dirty tables. At commit, after the in-memory snapshot install, the ops are framed into a single WAL entry stamped with the commit version and handed to the WAL thread. `Durability::Consistent` blocks the committer until the entry is fsynced; `Durability::Eventual` lets the committer return immediately while a background thread batches fsyncs.

`Store::checkpoint()` serializes the latest snapshot via the type registry, writes it to disk under `dir`, fsyncs it, and prunes WAL entries up to that version. It does not hold a store lock during I/O — reads and writes proceed in parallel. See [task15](tasks/task15_three_phase_consistent_persistence.md) for the consistency argument.

### Smr mode (checkpoints only)

In SMR deployments the consensus log (Raft/Paxos) is the durable record of operations; replaying it deterministically rebuilds state. UltimaDB only needs checkpoints to bound replay time. There is no WAL in this mode. `StoreConfig::require_explicit_version = true` is the typical companion setting — versions come from log indices, not from the store's auto-counter.

### Type registry

`Store::register_table::<R>("name")` must be called for every table type *before* any commit, `Store::recover()`, or `Store::checkpoint()`. The registry stores per-type serialization, deserialization, and replay shims keyed by table name. Recovery uses these to (1) load the latest checkpoint and (2) replay each WAL op against the rehydrated tables. `Record` itself adds `Serialize + DeserializeOwned` bounds when the feature is enabled (otherwise it's just `Send + Sync + 'static`).

### Recovery flow

```rust
let store = Store::new(config)?;          // opens WAL handle
store.register_table::<User>("users")?;
store.register_table::<Order>("orders")?;
store.recover()?;                         // checkpoint + WAL replay
```

`recover()` is a no-op when `Persistence::None`. Otherwise it loads the latest checkpoint (if any) into a snapshot and, in Standalone mode, replays WAL entries with `entry.version > latest_version`. The replay path uses `Arc::get_mut` on the rehydrated tables to mutate them in place — no CoW overhead for the linear recovery sequence. See [task13](tasks/task13_persistence.md).

### Bulk load

`Store::bulk_load` and `Store::bulk_load_batch` ([task23](tasks/task23_bulk_load.md), [task24](tasks/task24_vector_restore.md)) provide an O(N) restore path that bypasses per-row insertion: `BTree::from_sorted` packs leaves densely, indexes rebuild from sorted data, and the result installs as a fresh snapshot atomically. Optional `checkpoint_after` triggers a checkpoint immediately after the install.

---

## Circular dependency resolution

`Store` needs `ReadTx` and `WriteTx` as return types for `begin_read`/`begin_write`. `ReadTx` and `WriteTx` need `Snapshot` (defined alongside `Store`). If `ReadTx`/`WriteTx` were in a separate `transaction` module, both modules would need to import from each other.

The solution: define `Snapshot`, `Store`, `ReadTx`, and `WriteTx` all in `src/store.rs`. The `src/transaction.rs` module exists purely as a re-export:

```rust
pub use crate::store::{ReadTx, WriteTx};
```

This gives users a semantically clear import path (`use ultima_db::WriteTx` or `use ultima_db::transaction::WriteTx`) without introducing a circular dependency.

---

## Design decisions summary

| Decision | Alternative considered | Why this way |
|---|---|---|
| Persistent CoW B-tree | `std::BTreeMap` with deep copy or mutex | O(log n) per mutation instead of O(n); no locking; multiple versions coexist for free |
| `Arc<R>` for values | Store `R` directly, require `R: Clone` | Avoids cloning potentially large values on every node reconstruction; removes `Clone` bound from the public API |
| `Arc<BTreeNode<R>>` for children | `Box<BTreeNode<R>>` | Structural sharing — unchanged subtrees are shared across versions |
| `Arc<dyn MergeableTable>` in Snapshot | `Box<dyn MergeableTable>` | Must be cloneable (O(1) per table at commit time); `Box` is not `Clone`. `MergeableTable: Any + Send + Sync` so existing downcasts still work via `.as_any()` |
| `Box<dyn MergeableTable>` in WriteTx dirty | `Arc<dyn MergeableTable>` | Need `&mut` access for table mutations; `Box` provides `downcast_mut` through `.as_any_mut()` |
| `WriteTx` / `ReadTx` are `!Send` via `PhantomData<*const ()>` | Make them `Send` | A transaction is not designed to split work across threads; pinning to the creating thread prevents a footgun. Clone `Store` across threads instead |
| Key-level OCC in MultiWriter mode | Table-level OCC | Fewer spurious conflicts on same-table disjoint-key writes. Cost: commit clones latest table + replays writer's keys via `upsert_arc` (index-preserving). Fast-path wholesale install when no concurrent writer touched the table keeps single-writer commits cheap |
| `WriteTx::commit` rebases onto latest + per-key merge | Whole-table swap from dirty | Preserves non-conflicting concurrent commits in the final snapshot. Merge uses Arc-level record sharing (no `R: Clone` bound) via `BTree::insert_arc` |
| Auto-assigned commit version bumped to `latest + 1` | Keep pre-assigned version | Pre-assigned versions can land out of commit order under MultiWriter; rebase chain would lose updates. SMR explicit versions are left alone |
| Bottom-up splitting | Pre-emptive (top-down) splitting | Simpler with immutable nodes — no need to prepare nodes on the way down |
| Check-before-delete | Always enter deletion path | Avoids O(log n) CoW cost when the key doesn't exist |
| All core types in `store.rs` | Separate `transaction.rs` module | Avoids circular module dependency |
| Explicit version support | Auto-increment only | Supports external ordering (replication, distributed sequence numbers) |
| Snapshot Isolation by default, SSI as opt-in | Force SSI for everyone | SI has zero validation overhead and is sufficient for most workloads; SSI is opt-in via `IsolationLevel::Serializable` for callers that need write-skew prevention. See [isolation-levels.md](isolation-levels.md) |
| Persistence opt-in (cargo feature) | Persistence always on | In-memory is the common case for embedded use and tests; gating on a feature keeps the dependency surface and binary size small for callers that don't need durability. Standalone (WAL + checkpoints) and SMR (checkpoints only) cover the durability cases that exist |
| `thiserror` for errors | Manual `Display`/`Error` impls | Less boilerplate, same result |

---

## What is not yet implemented

- **Lock-free commit path.** Commit still acquires `inner.write()` briefly for the snapshot install phase, and per-table commit mutexes are held across merge + install. Under heavy MultiWriter contention with overlapping table sets, writers serialize on those locks. A lock-free design would need epoch-based reclamation for the snapshot map and a lock-free committed-write-set log.
- **Range / index-scan precision in SSI.** v1 SSI tracks point reads precisely but records any range, scan, or index read as a coarse "table touched" flag, so concurrent commits to that table abort the reader. v2 may track index-range bounds for finer granularity. See [task21](tasks/task21_serializable_isolation.md).
- **Hot-standby replication.** The Standalone WAL is a single-writer local log; there is no shipping mechanism to a follower. SMR mode delegates this to the consensus layer, but a non-SMR replication path is not provided.
