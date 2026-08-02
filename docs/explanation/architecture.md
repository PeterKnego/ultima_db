# UltimaDB Architecture

UltimaDB is an embedded, versioned key-value store with snapshot-isolated transactions. Data lives in memory; durability is opt-in via the `persistence` cargo feature (WAL + checkpoints, or checkpoint-only for state-machine-replication deployments). It is written in Rust.

This page explains how UltimaDB works internally, why the design is the way it is, and where the boundaries of the current implementation lie. It is about the *shape* of the system and the reasoning behind it — not a module-by-module API reference (that is the rustdoc), not a tuning guide (see [performance](../reference/performance.md)), and not a setup walkthrough (see the [how-to guides](../how-to/choose-a-configuration.md)).

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
│    dirty ─────▶ BTreeMap<String, DirtyEntry>                     │
│    version = 3                                                   │
└──────────────────────────────────────────────────────────────────┘

Each Snapshot contains:
  version: u64
  tables: BTreeMap<String, Arc<dyn MergeableTable>>
                            │
              ┌─────────────┘
              ▼
         Table<R, K = u64>
           data: BTree<K, R>      ◄── persistent CoW B-tree
           next_id (auto-keyed tables only)
           indexes: BTreeMap<String, Box<dyn IndexMaintainer<R>>>

         BTree<K, V>
           root: Arc<BTreeNode<K, V>>    ◄── O(1) clone
           len: usize

         BTreeNode<K, V>
           entries:  inline fixed-capacity array of (K, Arc<V>)
           children: inline fixed-capacity array of Arc<BTreeNode>
```

---

## Module structure

The crate is small enough to hold in your head: `btree` is the persistent copy-on-write B-tree everything else stands on (re-exported at the crate root as a building block for custom indexes); `table` wraps it into typed collections and defines the `MergeableTable` trait used for type-erased storage in snapshots; `index` provides secondary indexes (built-in `ManagedIndex` plus the public `CustomIndex` trait); `bulk_load` is the O(N) restore path; `store` holds `Store`, `Snapshot`, `ReadTx`, `WriteTx`, and all version-history and OCC bookkeeping, with `transaction` as a pure re-export (see [circular dependency note](#circular-dependency-resolution)); `intents` is the write-intent map for early-fail conflict detection in MultiWriter mode; `primary_key` defines the order-preserving key encoding; `metrics` and `error` are what they sound like. Behind the `persistence` feature sit `wal`, `checkpoint`, and `registry`; behind `fulltext` sits an optional full-text index. Per-module details live in the rustdoc — this page stays at the level of why the pieces are shaped the way they are.

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

**Parameters:** the default minimum degree is `T = 32`, so `MAX_KEYS = 2T - 1 = 63` and `MIN_KEYS = T - 1 = 31`. The `fanout-t8` cargo feature switches to `T = 8` for write-dominated deployments (state-machine replication in particular), because fanout pulls reads and CoW writes in opposite directions: a wider node means a shallower tree (faster traversal) but a bigger node clone on every mutation. See [cargo features](../reference/cargo-features.md).

**Key design choices:**

- **Inline fixed-capacity node storage.** Entries and children live in fixed-capacity arrays inside the node itself (a `FixedVec`), not in heap-allocated `Vec`s. A CoW node clone is therefore a single allocation, which matters because node cloning is the dominant cost on the write path.

- **`Arc<R>` for values, not `R` directly.** Values are wrapped in `Arc<R>` at insertion time. This means `R` does not need to implement `Clone`. When a node is reconstructed during an insert or delete, the unchanged entries just clone their `Arc` (a pointer bump), not the value itself.

- **`Arc<BTreeNode>` for children.** Same reasoning — when an internal node is reconstructed, only the modified child path gets a new `Arc`; all other children are shared.

- **Immutable mutation API.** `insert` and `remove` return a *new* `BTree<K, V>`. They do not take `&mut self`. The caller (usually `Table`) reassigns `self.data = self.data.insert(...)`. This makes the immutability guarantee structural — there is no way to accidentally mutate a tree that a reader is using.

- **Bottom-up splitting on insert.** Recursive insertion returns `InsertResult::Fit` or `InsertResult::Split`. A split propagates upward; if the root itself splits, a new single-entry root is created. This was chosen over pre-emptive (top-down) splitting because it is simpler to implement correctly with immutable nodes — you cannot "prepare" a node for splitting when you don't mutate on the way down.

- **Check-before-delete pattern.** `Table::delete` calls `self.data.get(id)` first. If the key is absent, it returns `Err(KeyNotFound)` without ever entering the tree's deletion path. This avoids the cost of CoW (copying nodes along the path) only to discover the key doesn't exist.

- **Rebalancing on delete.** When a deletion causes a node to become underfull (< MIN_KEYS entries), `fix_underfull_child` tries, in order: rotate from left sibling, rotate from right sibling, merge with left sibling, merge with right sibling. If merging propagates underfull-ness upward, the parent handles it recursively. If the root ends up empty with one child, the tree height collapses by one.

### Clone is O(1)

`BTree::clone()` increments the root `Arc`'s reference count and copies a `usize`. That's it. This is what makes `WriteTx::open_table` cheap — cloning an entire table to get a mutable working copy costs the same as cloning a single pointer.

---

## Table

**File:** `src/table.rs`

`Table<R, K = u64>` is a thin wrapper around `BTree<K, R>` that adds:

- **A pluggable primary key.** `K` is any type implementing `PrimaryKey` — an order-preserving byte encoding whose bytewise order matches `Ord` order (integers, strings, byte vectors, and small tuples out of the box). The default is `u64`, and the entire pre-0.3.0 API reads unchanged. See [why natural keys work this way](../reference/key-encoding-and-formats.md) and the [natural-keys how-to](../how-to/use-natural-primary-keys.md).
- **Auto-incrementing IDs — for auto-keyable tables only.** Auto-increment is gated on a separate `AutoKey` trait, implemented only for `u64`. A `u64`-keyed table gets `insert(record) -> u64` and a `next_id` counter; an explicitly-keyed table (say `Table<User, String>`) has no counter at all and uses `put(key, record)`. The gate is a compile-time answer to an unanswerable runtime question: there is no sensible "next" `String`.
- **Mutable API.** Unlike the immutable `BTree`, `Table` uses `&mut self` methods. Internally, each mutation reassigns `self.data` to the new tree returned by the immutable B-tree operation.
- **Error handling.** `update` and `delete` return `Result<()>` with `Error::KeyNotFound` if the key doesn't exist.

### Clone preserves next_id

`Table::clone()` copies the auto-increment counter along with the O(1) `BTree` clone. This ensures that when `WriteTx` forks a table from the base snapshot, subsequent inserts continue from the correct ID and never collide with existing entries.

---

## Custom indexes

**File:** `src/index.rs`

UltimaDB supports user-defined custom indexes via the `CustomIndex<R, K>` trait. Unlike built-in indexes (which use `KeyExtractor` + `UniqueStorage`/`NonUniqueStorage`), custom indexes have full control over their internal data structure and expose their own query API.

### How it works

1. The user implements `CustomIndex` on their type, providing `on_insert`, `on_update`, and `on_delete` hooks.
2. They register it via `table.define_custom_index("name", my_index)`.
3. Internally, a `CustomIndexAdapter` wraps the custom index and implements `IndexMaintainer<R>`, so it's stored alongside built-in indexes in the same map.
4. Queries go through a typed handle: `table.custom_index::<MyIndex>("name")` returns `&MyIndex`, giving access to the index's own query methods.
5. Record resolution is separate: `table.resolve(&ids)` maps keys to records.

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
    table_locks: Arc<...>,              // per-table commit locks
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

`StoreInner.snapshots` holds every retained version. The default `StoreConfig` keeps the 10 most recent snapshots and runs `gc()` automatically after each commit; retention, GC, writer mode, and isolation level are all knobs on `StoreConfig` — see [configuration](../reference/configuration.md). A `VersionPin` can hold one version alive across `gc()` runs independently of the retention window, which is the snapshot-handoff primitive replication needs (see [replicate with snapshot streams](../how-to/replicate-with-snapshot-streams.md)).

The `intents` map and per-table `table_locks` exist so that disjoint-key MultiWriter commits don't serialize through `inner.write()`. See [Writer modes](#writer-modes) below.

### Snapshots

A `Snapshot` is an immutable, versioned view of all tables:

```rust
pub(crate) struct Snapshot {
    pub(crate) version: u64,
    pub(crate) tables: BTreeMap<String, Arc<dyn MergeableTable>>,
}
```

**Why `Arc<dyn MergeableTable>` for tables?**

Tables have different record types — and, since 0.3.0, different key types — but must coexist in a single map. `MergeableTable: Any + Send + Sync` provides type erasure plus the operations the commit path needs — `boxed_clone` (O(1) CoW) and `merge_keys_from` (per-key replay during MultiWriter rebase). Concrete `Table<R, K>` is recovered via `.as_any().downcast_ref::<Table<R, K>>()`. Because one snapshot holds tables with different key types, `K` must not appear anywhere in the trait's method signatures; where the commit path needs a writer's exact modified keys, they cross the trait boundary as `&dyn Any` and the concrete impl downcasts.

`Arc` (rather than `Box`) is critical for two reasons:

1. **Snapshot sharing.** When `commit` builds a new snapshot, it starts from the latest snapshot's table map. Tables that the writer didn't touch are carried forward by cloning their `Arc` — O(1) per table, no data copying.

2. **Read transaction lifetime.** `ReadTx` holds `Arc<Snapshot>`, which keeps the snapshot (and all its `Arc<dyn MergeableTable>` table entries) alive. Multiple readers at different versions coexist without interfering with each other or the store.

The downcast to `Table<R, K>` happens at `open_table` time, returning `Error::TypeMismatch` if the caller's type parameter doesn't match the type the table was created with.

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
}
```

`ReadTx` is a read-only view pinned to a specific version. It holds an `Arc<Snapshot>`, which keeps that version's data alive independently of subsequent commits. Multiple `ReadTx` instances at different versions coexist freely.

`open_table<R>` returns a `TableReader<'_, R>` (a thin wrapper that records read metrics and downcasts to `&Table<R>`). The reader borrows from the snapshot — no copying occurs.

### WriteTx

A `WriteTx` is built around three ideas: a read-only `base` snapshot it forked from, a private `dirty` map of working table copies, and the bookkeeping that conflict detection needs. The essential fields:

```rust
pub struct WriteTx {
    base: Arc<Snapshot>,
    dirty: BTreeMap<String, DirtyEntry>,         // Box<dyn MergeableTable> + per-table handles
    version: u64,
    explicit_version: bool,
    write_set: BTreeMap<String, BTreeSet<u64>>,  // hash64 digests of modified keys (MultiWriter OCC)
    read_set: Option<RefCell<...>>,              // Serializable + MultiWriter only
    ddl_tables: RefCell<BTreeSet<String>>,       // index DDL, for conflict refusal
    // ... deleted-table tracking, intents, table locks, metrics,
    //     persistence-feature fields (wal_ops)
}
```

`WriteTx` implements lazy copy-on-write at the table level:

1. **First call to `open_table("t")`**: clone `Table<R, K>` from `base.tables["t"]` (O(1) — just an Arc bump on the BTree root) into `dirty`. If "t" doesn't exist in the base, create an empty `Table<R, K>`.
2. **Subsequent calls**: return a `&mut` reference to the existing dirty copy.
3. **Mutations**: all writes go through a `TableWriter<'_, R>`, which records the modified key (MultiWriter only) and registers an intent for early-fail conflict detection, then forwards to `&mut Table`. The table internally reassigns `self.data` to a new `BTree`. None of this is visible outside the `WriteTx` until commit.

**Why `Box<dyn MergeableTable>` in dirty (not `Arc`)?** The writer needs `&mut Table<R, K>`, which requires exclusive ownership. `Box` gives us `downcast_mut` via `as_any_mut`. At commit time, each `Box` is converted to `Arc` via `Arc::from(boxed)` for installation into the new snapshot.

**Why digests in the write set?** The conflict-detection write set holds `PrimaryKey::hash64` digests of the modified keys rather than the keys themselves, because OCC compares this set against *other* writers' sets — and two writers touching the same table need not agree on the key type `K`. A digest collision costs a spurious conflict (one retry), never a missed one, which is the safe direction to be imprecise in. The commit *merge*, by contrast, needs exact keys, so each `DirtyEntry` also carries the writer's modified keys as a type-erased `BTreeSet<K>`. Two structures, two jobs.

**Read-set tracking.** `read_set` is `Some` only when both `IsolationLevel::Serializable` and `WriterMode::MultiWriter` are configured. SI doesn't validate, and SingleWriter has no concurrent writers — so allocating a read set in those modes is pure overhead. See [isolation](isolation.md).

### Commit

`WriteTx::commit` rebases onto the latest committed snapshot — not the base it forked from — so that concurrent commits to disjoint tables (or disjoint keys within the same table under MultiWriter) don't lose updates.

1. **Conflict check (MultiWriter only).** Walk `committed_write_sets` for entries committed since `base.version`. If any of their modified `(table, key-digest)` pairs intersect this writer's `write_set`, return `Error::WriteConflict`. Under `IsolationLevel::Serializable`, also intersect this writer's `read_set` against committed write sets — fail with `Error::SerializationFailure` if any read was invalidated.
2. **Per-table merge.** For each dirty table, take the `Arc<dyn MergeableTable>` from the *current latest* snapshot's table map. Two paths:
   - **Fast path** — if no concurrent committed writer touched this table since `base.version`, install the dirty `Box` wholesale (single `Arc::from`).
   - **Slow path** — clone the latest table (O(1) CoW), then replay only this writer's modified keys into it via `MergeableTable::merge_keys_from`, which calls `Table::upsert_arc` so secondary indexes stay consistent through the existing `on_insert`/`on_update` hooks.
3. **Snapshot install.** Build a new `Snapshot` with the merged tables for everything dirty, plus O(1) `Arc` clones of every table the writer didn't touch. Insert under the assigned `version` and bump `latest_version`.
4. **Auto-version bump.** If the writer was created with `begin_write(None)` and another commit landed at a higher version in the meantime, the assigned `version` is bumped under the commit lock so version order matches commit order. Explicit (SMR-mode) versions are left alone.
5. **Bookkeeping.** Record this transaction's `write_set` in `committed_write_sets` for future conflict checks, prune entries no in-flight writer can still need, decrement `active_writer_count`, and (with persistence) append the WAL ops and signal/wait for fsync per `Durability` mode.

Tables the writer never opened are carried forward automatically — no data is lost.

One merge limitation is deliberate: index DDL (`define_index`/`define_custom_index`) cannot be carried through the slow path — only write-set keys are replayed, and an index definition is not a key. Rather than silently dropping the new index, commit refuses with `Error::IndexDdlConflict` when a DDL'd table saw a concurrent commit. Define indexes in their own transaction and retry on that error.

### Rollback

`WriteTx::rollback` is a no-op that drops `self`. The store is never modified. The dirty working copies are freed, and the base snapshot's reference count decrements.

---

## Isolation level

UltimaDB implements **Snapshot Isolation** by default and **Serializable Snapshot Isolation (SSI)** as an opt-in. SI prevents dirty reads, nonrepeatable reads, and phantom reads, but does *not* prevent write skew (a serialization anomaly where two concurrent transactions read overlapping data and write to disjoint subsets, producing a result impossible in any serial execution). SSI prevents write skew by tracking each `WriteTx`'s read set and aborting at commit if any read was invalidated by a concurrent committer.

Set `StoreConfig::isolation_level = IsolationLevel::Serializable` to opt in. SSI only matters under `WriterMode::MultiWriter`; under `SingleWriter` there are no concurrent writers and the level is silently equivalent to SI. v1 tracks point reads precisely; any range/scan/index read is recorded as a coarse "table touched" flag (false positives possible on read-heavy scan workloads).

The full story — why SI is the default, what write skew looks like, and what SSI's coarse tracking trades away — is in [isolation](isolation.md); the exact anomaly matrices and measured overhead are in the [isolation levels reference](../reference/isolation-levels.md).

### Writer modes

`StoreConfig::writer_mode` controls concurrency:

- **`SingleWriter`** (default): at most one active `WriteTx` at a time. `begin_write` returns `Error::WriterBusy` if another is already active. No OCC tracking overhead.
- **`MultiWriter`**: multiple concurrent `WriteTx` allowed. Key-level OCC: two writers conflict only if their modified rows overlap on the same table. Disjoint rows in the same table both commit; the second commit's per-key merge pulls only its edited keys onto the current latest snapshot via the `MergeableTable` trait. Fast path: if no concurrent writer touched a given dirty table, install it wholesale (single Arc swap). See [handling write conflicts](../how-to/handle-write-conflicts.md).

`Store`, `Snapshot`, `VersionPin`, and `ReadTx` are all `Send + Sync`, so the `Store` handle can be cloned across threads. `WriteTx` is `Send` but `!Sync` — it can be *moved* between threads (including held across an `.await`), but never used from two threads at once; the `RefCell`-backed write/read/DDL sets are what make it `!Sync`. No store bookkeeping is keyed by thread, so a transaction opened on one thread can be committed on another (`tests/send_bounds.rs` asserts the bounds).

The intended pattern is still: clone the `Store` into each thread, and call `begin_write`/`begin_read` locally. Moving a transaction across threads is *allowed*, but an open `WriteTx` holds the SingleWriter slot (or its MultiWriter intents); `commit` blocks; and so does `Drop`, which takes the store write lock to release the writer slot and intents — including on an early return, a panic, or a cancelled async task. On an async runtime the whole open/use/commit-or-drop sequence belongs in `spawn_blocking` — see [using UltimaDB from async code](../how-to/use-from-async-code.md).

---

## Persistence

**Files:** `src/persistence.rs`, `src/wal.rs`, `src/checkpoint.rs`, `src/registry.rs`. Gated on the `persistence` cargo feature.

By default UltimaDB is in-memory: `StoreConfig::persistence` is `Persistence::None` and the durability subsystem is compiled out entirely (no WAL handle, no registry, no on-disk files). Enabling persistence is an opt-in choice per `Store`, and it is really a choice between two philosophies of who owns durability:

- **Standalone** — UltimaDB owns durability: a write-ahead log makes each commit durable, and checkpoints bound recovery time (the WAL is auto-pruned on checkpoint). Within Standalone, `Durability` decides *when* a commit is durable (asynchronously, or before `commit()` returns) and `WalWrite` decides *how* the bytes reach the log (per-entry writes, coalesced batches, or positioned writes into a preallocated file whose fsyncs are metadata-free). These are orthogonal axes, and their variants and trade-offs are enumerated in [configuration](../reference/configuration.md) and weighed in [choosing a configuration](../how-to/choose-a-configuration.md).
- **SMR** — a consensus log (Raft/Paxos) owns durability. Replaying that log deterministically rebuilds state, so UltimaDB keeps no WAL at all and writes only checkpoints, purely to bound replay time. Versions typically come from log indices via explicit `begin_write(Some(v))`, not from the store's auto-counter.

### Standalone mode (WAL + checkpoints)

Each `WriteTx` accumulates `wal_ops` (insert / update / delete / create-table / delete-table) as it mutates dirty tables. At commit, after the in-memory snapshot install, the ops are framed into a single WAL entry stamped with the commit version and handed to the WAL thread. `Durability::Consistent` blocks the committer until the entry is fsynced; `Durability::Eventual` lets the committer return immediately while a background thread batches fsyncs.

`Store::checkpoint()` serializes the latest snapshot via the type registry, writes it to disk under `dir`, fsyncs it, and prunes WAL entries up to that version. It does not hold a store lock during I/O — reads and writes proceed in parallel.

### Type registry

`Store::register_table::<R>("name")` must be called for every table type *before* any commit, `Store::recover()`, or `Store::checkpoint()`. The registry stores per-type serialization, deserialization, and replay shims keyed by table name. Recovery uses these to (1) load the latest checkpoint and (2) replay each WAL op against the rehydrated tables. `Record` itself adds `Serialize + DeserializeOwned` bounds when the feature is enabled (otherwise it's just `Send + Sync + 'static`).

The registration-before-recovery requirement is the price of type erasure: the on-disk formats store bytes, not Rust types, and only the registry can turn a table name back into a concrete deserializer. The wiring steps are in [setting up durable persistence](../how-to/set-up-durable-persistence.md), which also covers the recovery sequence.

### Why the formats stamp a key type — and cap key length on write

Since primary keys became pluggable, every durable format (checkpoint, WAL, snapshot stream) stamps each table with a self-declared `KEY_TYPE_ID` — a `u32` the key type states about itself. It is worth explaining why the obvious alternatives don't work, because the failure mode without a tag is unusually quiet.

Encoded row keys are opaque bytes, and several key types decode the same bytes without complaint: the eight bytes of `1u64` are also a valid NUL-filled `String`; the `u64` and `i64` encodings differ only in a sign bit; `String` and `Vec<u8>` are byte-identical. Worse, because the encoding is order-preserving, reinterpreted keys sail through the strict-ascending-order validation every reader performs — the one check that might have caught the mistake. Without a tag, a directory written under one key type and reopened under another recovers `Ok` with every key silently reinterpreted.

So a tag is mandatory. But `std::any::type_name` is the wrong tag: it is neither *stable* (Rust promises nothing across compiler versions, so it can produce a false refusal — safe but noisy) nor *injective* (two binaries linking different versions of a key type's crate print the identical string — a false accept, which is the direction that corrupts). `TypeId` is exact but process-local and unprintable, useless on disk or on a wire. Hence the self-declared `u32`: stable because the type author freezes it, comparable across processes and versions, and printable in an error message. The formats still carry the type *name* alongside the id, purely so a refusal reads like something a human recognises.

The same formats share a single 64 KiB cap on encoded key length, enforced **on write as well as on read** — and the write side is the half that matters. A cap enforced only on read lets the store produce a WAL record no reader will accept: `commit()` returns `Ok` — an acknowledged, supposedly durable transaction — and then recovery fails permanently, or (in the tail-tolerant preallocated-WAL mode) silently drops the whole transaction, innocent co-committed rows included. Checking at the mutation site, at WAL serialization, and at checkpoint serialization means the caller learns about an oversized key at the offending write, while the table is still untouched, instead of at the next restart. The format details live in the [key encoding reference](../reference/key-encoding-and-formats.md).

### Recovery

Recovery is `register_table` for each table, then a single `Store::recover()` that loads the latest checkpoint and replays newer WAL entries; the full sequence, including SMR variations, is in [setting up durable persistence](../how-to/set-up-durable-persistence.md).

### Bulk load

`Store::bulk_load` and `Store::bulk_load_batch` provide an O(N) restore path that bypasses per-row insertion: `BTree::from_sorted` packs leaves densely, indexes rebuild from sorted data, and the result installs as a fresh snapshot atomically. Optional `checkpoint_after` triggers a checkpoint immediately after the install. See [bulk load and restore](../how-to/bulk-load-and-restore.md).

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
| `T = 32` default fanout, `T = 8` behind `fanout-t8` | One fixed fanout for all workloads | Fanout pulls reads and CoW writes in opposite directions (shallower tree vs. bigger node clone); no single value wins both. T=32 balances; T=8 favors write-dominated SMR apply loops |
| Inline fixed-capacity node storage (`FixedVec`) | `Vec` entries/children per node | One allocation per CoW node clone instead of three; node cloning dominates the write path |
| `Arc<R>` for values | Store `R` directly, require `R: Clone` | Avoids cloning potentially large values on every node reconstruction; removes `Clone` bound from the public API |
| `Arc<BTreeNode>` for children | `Box<BTreeNode>` | Structural sharing — unchanged subtrees are shared across versions |
| `Arc<dyn MergeableTable>` in Snapshot | `Box<dyn MergeableTable>` | Must be cloneable (O(1) per table at commit time); `Box` is not `Clone`. `MergeableTable: Any + Send + Sync` so existing downcasts still work via `.as_any()` |
| `Box<dyn MergeableTable>` in WriteTx dirty | `Arc<dyn MergeableTable>` | Need `&mut` access for table mutations; `Box` provides `downcast_mut` through `.as_any_mut()` |
| `WriteTx` / `ReadTx` are `Send` (`WriteTx` is `!Sync` via its `RefCell`s) | Keep the `PhantomData<*const ()>` `!Send` marker | The marker was a footgun guard, not a correctness requirement: no thread-local, thread-id-keyed, or non-`Send` state exists on the write path, so it only cost async users the ability to hold a transaction across an `.await` |
| Key-level OCC in MultiWriter mode | Table-level OCC | Fewer spurious conflicts on same-table disjoint-key writes. Cost: commit clones latest table + replays writer's keys via `upsert_arc` (index-preserving). Fast-path wholesale install when no concurrent writer touched the table keeps single-writer commits cheap |
| `hash64` digests in the OCC write set, exact keys in `DirtyEntry` | Exact keys everywhere | Conflict detection compares sets across writers that need not agree on `K`; digests make that comparison type-free. A collision is a spurious retry, never a missed conflict — the safe direction. The merge, which needs exact keys, gets them from `DirtyEntry` |
| `WriteTx::commit` rebases onto latest + per-key merge | Whole-table swap from dirty | Preserves non-conflicting concurrent commits in the final snapshot. Merge uses Arc-level record sharing (no `R: Clone` bound) via `BTree::insert_arc` |
| Auto-assigned commit version bumped to `latest + 1` | Keep pre-assigned version | Pre-assigned versions can land out of commit order under MultiWriter; rebase chain would lose updates. SMR explicit versions are left alone |
| Bottom-up splitting | Pre-emptive (top-down) splitting | Simpler with immutable nodes — no need to prepare nodes on the way down |
| Check-before-delete | Always enter deletion path | Avoids O(log n) CoW cost when the key doesn't exist |
| All core types in `store.rs` | Separate `transaction.rs` module | Avoids circular module dependency |
| Explicit version support | Auto-increment only | Supports external ordering (replication, distributed sequence numbers) |
| Auto-increment gated on `AutoKey` (u64 only) | Auto-increment for every key type, or none | There is no sensible "next" `String`; the gate turns a meaningless runtime question into a compile error, while `u64` tables keep the ergonomic `insert` |
| Self-declared `KEY_TYPE_ID` in every durable format | `std::any::type_name`, `TypeId`, or no tag | `type_name` is neither stable nor injective; `TypeId` is process-local; no tag means silent key reinterpretation that passes order validation. A frozen `u32` is stable, comparable across processes, and printable |
| 64 KiB key cap enforced on write and read | Read-side enforcement only | Write-side-unchecked keys produce acknowledged commits that recovery cannot read (or silently drops); failing at the offending mutation leaves the table untouched |
| Snapshot Isolation by default, SSI as opt-in | Force SSI for everyone | SI has zero validation overhead and is sufficient for most workloads; SSI is opt-in for callers that need write-skew prevention. See [isolation](isolation.md) |
| Persistence opt-in (cargo feature) | Persistence always on | In-memory is the common case for embedded use and tests; gating on a feature keeps the dependency surface and binary size small for callers that don't need durability. Standalone (WAL + checkpoints) and SMR (checkpoints only) cover the durability cases that exist |
| `thiserror` for errors | Manual `Display`/`Error` impls | Less boilerplate, same result |

---

## What is not yet implemented

- **Lock-free commit path.** Commit still acquires `inner.write()` briefly for the snapshot install phase, and per-table commit mutexes are held across merge + install. Under heavy MultiWriter contention with overlapping table sets, writers serialize on those locks. A lock-free design would need epoch-based reclamation for the snapshot map and a lock-free committed-write-set log.
- **Range / index-scan precision in SSI.** v1 SSI tracks point reads precisely but records any range, scan, or index read as a coarse "table touched" flag, so concurrent commits to that table abort the reader. v2 may track index-range bounds for finer granularity. See [isolation](isolation.md) for why the coarse direction was chosen.
- **Hot-standby replication.** The Standalone WAL is a single-writer local log; there is no shipping mechanism to a follower. SMR mode delegates this to the consensus layer, and [snapshot streams](../how-to/replicate-with-snapshot-streams.md) cover full-state handoff, but a non-SMR incremental replication path is not provided.
