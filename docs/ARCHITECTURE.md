# UltimaDB Architecture

UltimaDB is an embedded, in-memory, versioned key-value store with snapshot-isolated transactions. It is written in Rust with zero runtime dependencies beyond `thiserror`.

This document explains how UltimaDB works internally, why the design is the way it is, and where the boundaries of the current implementation lie.

---

## Overview

```
┌──────────────────────────────────────────────────────────────────┐
│  Store                                                           │
│                                                                  │
│  snapshots: HashMap<u64, Arc<Snapshot>>                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                       │
│  │ v0 empty │  │ v1       │  │ v2       │  ...                   │
│  └──────────┘  └──────────┘  └──────────┘                       │
│                                                                  │
│  ReadTx ──holds──▶ Arc<Snapshot v1>                              │
│  ReadTx ──holds──▶ Arc<Snapshot v2>                              │
│                                                                  │
│  WriteTx                                                         │
│    base ──────▶ Arc<Snapshot v2>   (read-only reference)         │
│    dirty ─────▶ HashMap<String, Box<dyn Any>>  (mutable copies)  │
│    version = 3                                                   │
└──────────────────────────────────────────────────────────────────┘

Each Snapshot contains:
  version: u64
  tables: HashMap<String, Arc<dyn Any>>
                            │
              ┌─────────────┘
              ▼
         Table<R>
           data: BTree<R>    ◄── persistent CoW B-tree
           next_id: u64

         BTree<R>
           root: Arc<BTreeNode<R>>    ◄── O(1) clone
           len: usize

         BTreeNode<R>
           entries: Vec<(u64, Arc<R>)>
           children: Vec<Arc<BTreeNode<R>>>
```

---

## Module structure

| Module | Purpose |
|---|---|
| `btree` | Persistent copy-on-write B-tree. Internal to the crate; not re-exported. |
| `table` | `Table<R>` — typed collection backed by `BTree<R>` with auto-incrementing IDs. |
| `store` | `Store`, `Snapshot`, `ReadTx`, `WriteTx` — version history and transactions. |
| `transaction` | Re-exports `ReadTx` and `WriteTx` from `store` (see [circular dependency note](#circular-dependency-resolution)). |
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

**Parameters:** minimum degree T=3, so MAX_KEYS=5, MIN_KEYS=2. These are compile-time constants.

**Key design choices:**

- **`Arc<R>` for values, not `R` directly.** Values are wrapped in `Arc<R>` at insertion time. This means `R` does not need to implement `Clone`. When a node is reconstructed during an insert or delete, the unchanged entries just clone their `Arc` (a pointer bump), not the value itself.

- **`Arc<BTreeNode<R>>` for children.** Same reasoning — when an internal node is reconstructed, only the modified child path gets a new `Arc`; all other children are shared.

- **Immutable mutation API.** `insert` and `remove` return a *new* `BTree<R>`. They do not take `&mut self`. The caller (usually `Table`) reassigns `self.data = self.data.insert(...)`. This makes the immutability guarantee structural — there is no way to accidentally mutate a tree that a reader is using.

- **Bottom-up splitting on insert.** Recursive insertion returns `InsertResult::Fit` or `InsertResult::Split`. A split propagates upward; if the root itself splits, a new single-entry root is created. This was chosen over pre-emptive (top-down) splitting because it is simpler to implement correctly with immutable nodes — you cannot "prepare" a node for splitting when you don't mutate on the way down.

- **Check-before-delete pattern.** `Table::delete` calls `self.data.get(id)` first. If the key is absent, it returns `Err(KeyNotFound)` without ever entering the tree's deletion path. This avoids the cost of CoW (copying nodes along the path) only to discover the key doesn't exist.

- **Rebalancing on delete.** When a deletion causes a node to become underfull (< MIN_KEYS entries), `fix_underfull_child` tries, in order: rotate from left sibling, rotate from right sibling, merge with left sibling, merge with right sibling. If merging propagates underfull-ness upward, the parent handles it recursively. If the root ends up empty with one child, the tree height collapses by one.

### Clone is O(1)

`BTree::clone()` increments the root `Arc`'s reference count and copies a `usize`. That's it. This is what makes `WriteTx::open_table` cheap — cloning an entire table to get a mutable working copy costs the same as cloning a single pointer.

---

## Table

**File:** `src/table.rs`

`Table<R>` is a thin wrapper around `BTree<R>` that adds:

- **Auto-incrementing IDs.** Each table has a `next_id: u64` counter starting at 1. `insert` assigns the next ID and increments the counter.
- **Mutable API.** Unlike `BTree<R>`, `Table<R>` uses `&mut self` methods. Internally, each mutation reassigns `self.data`:

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

## Store and version history

**File:** `src/store.rs`

`Store` is the entry point. It maintains a complete version history as a map of snapshots:

```rust
pub struct Store {
    snapshots: HashMap<u64, Arc<Snapshot>>,
    latest_version: u64,
    next_version: u64,
}
```

### Snapshots

A `Snapshot` is an immutable, versioned view of all tables:

```rust
pub(crate) struct Snapshot {
    pub version: u64,
    pub tables: HashMap<String, Arc<dyn Any>>,
}
```

**Why `Arc<dyn Any>` for tables?**

Tables have different types (`Table<String>`, `Table<u64>`, etc.) but must coexist in a single map. `dyn Any` provides type erasure. `Arc` (rather than `Box`) is critical for two reasons:

1. **Snapshot sharing.** When `commit` builds a new snapshot, it starts from the base snapshot's table map. Tables that the writer didn't touch are carried forward by cloning their `Arc` — O(1) per table, no data copying.

2. **Read transaction lifetime.** `ReadTx` holds `Arc<Snapshot>`, which keeps the snapshot (and all its `Arc<dyn Any>` table entries) alive. Multiple readers at different versions coexist without interfering with each other or the store.

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
}
```

`ReadTx` is a read-only view pinned to a specific version. It holds an `Arc<Snapshot>`, which keeps that version's data alive independently of subsequent commits. Multiple `ReadTx` instances at different versions coexist freely.

`open_table<R>` borrows from the snapshot via `downcast_ref`, returning `&Table<R>` with a lifetime tied to `&self`. No copying occurs.

### WriteTx

```rust
pub struct WriteTx {
    base: Arc<Snapshot>,
    dirty: HashMap<String, Box<dyn Any>>,
    version: u64,
}
```

`WriteTx` implements lazy copy-on-write at the table level:

1. **First call to `open_table("t")`**: clone `Table<R>` from `base.tables["t"]` (O(1) — just an Arc bump on the BTree root) into `dirty`. If "t" doesn't exist in the base, create an empty `Table<R>`.
2. **Subsequent calls**: return a `&mut` reference to the existing dirty copy.
3. **Mutations**: all `insert`/`update`/`delete` calls go through `&mut Table<R>`, which internally reassigns `self.data` to a new `BTree<R>`. None of this is visible outside the `WriteTx`.

**Why `Box<dyn Any>` in dirty (not `Arc`)?** The writer needs `&mut Table<R>`, which requires exclusive ownership. `Box` gives us `downcast_mut`. At commit time, the `Box` is converted to `Arc` via `Arc::from(boxed)`.

### Commit

`WriteTx::commit` builds a new snapshot in two steps:

1. Clone the base snapshot's table map — each `Arc<dyn Any>` clone is O(1).
2. Overwrite entries from `dirty` — converting each `Box<dyn Any>` to `Arc<dyn Any>`.

The result is an `Arc<Snapshot>` that shares unchanged tables with the base and owns new copies of modified tables. This is pushed into `Store::snapshots`.

Tables the writer never opened are carried forward automatically — no data is lost.

### Rollback

`WriteTx::rollback` is a no-op that drops `self`. The store is never modified. The dirty working copies are freed, and the base snapshot's reference count decrements.

---

## Isolation level

UltimaDB implements **Snapshot Isolation**, which prevents dirty reads, nonrepeatable reads, and phantom reads. It does *not* prevent write skew (a serialization anomaly where two concurrent transactions read overlapping data and write to disjoint subsets, producing a result impossible in any serial execution).

See [isolation-levels.md](isolation-levels.md) for a detailed treatment of:

- What each SQL isolation level prevents
- Exactly which anomalies UltimaDB's SI prevents and allows
- What would be required to achieve Serializable (SSI)
- Concrete test patterns for verifying each guarantee

### Single-writer constraint

The current design assumes a single active writer at a time. This is a **design-level convention**, not a runtime enforcement. Nothing prevents opening two `WriteTx` instances simultaneously — they will each see the same base snapshot and can both commit, producing divergent version histories. This is intentional: runtime write-locking (and the associated `Send + Sync` bounds on `Snapshot`) is deferred to a future task.

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
| `Arc<dyn Any>` in Snapshot | `Box<dyn Any>` | Must be cloneable (O(1) per table at commit time); `Box<dyn Any>` is not `Clone` |
| `Box<dyn Any>` in WriteTx dirty | `Arc<dyn Any>` | Need `&mut` access for table mutations; `Box` provides `downcast_mut` |
| Bottom-up splitting | Pre-emptive (top-down) splitting | Simpler with immutable nodes — no need to prepare nodes on the way down |
| Check-before-delete | Always enter deletion path | Avoids O(log n) CoW cost when the key doesn't exist |
| All core types in `store.rs` | Separate `transaction.rs` module | Avoids circular module dependency |
| Explicit version support | Auto-increment only | Supports external ordering (replication, distributed sequence numbers) |
| No `Send + Sync` on `Snapshot` | Add bounds immediately | Deferred to future task; suppressed with `#[allow(clippy::arc_with_non_send_sync)]` |
| Snapshot Isolation (not SSI) | Full serializability | SI is simpler, cheaper, and sufficient for most use cases; SSI can be [layered on later](isolation-levels.md) |
| Design-only single writer | Runtime write lock | Avoids `Send + Sync` complexity; sufficient for current single-threaded use |
| `thiserror` for errors | Manual `Display`/`Error` impls | Less boilerplate, same result |

---

## What is not yet implemented

- **Garbage collection of old snapshots.** Every committed version is retained forever. A future GC pass would drop snapshots no longer referenced by any `ReadTx`.
- **Runtime write locking.** Multiple `WriteTx` can be opened simultaneously. A future task would add a write lock (or optimistic concurrency control) and the associated `Send + Sync` bounds.
- **Serializable Snapshot Isolation (SSI).** The store does not track read sets, so write skew is possible. See [isolation-levels.md](isolation-levels.md) for what SSI would require.
- **Persistence to disk.** UltimaDB is purely in-memory. Durability (WAL, checkpointing) is out of scope for the current design.
- **`Send + Sync` bounds.** `dyn Any` does not require `Send + Sync`, so `Snapshot` (and by extension `ReadTx`) cannot be sent across threads. A future task would change to `dyn Any + Send + Sync` and require `R: Send + Sync`.
