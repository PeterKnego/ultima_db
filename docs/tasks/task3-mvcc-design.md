# UltimaDB Task 3: MVCC & Persistent B-Tree

## Goal

Replace `std::BTreeMap` with a persistent copy-on-write B-tree and add snapshot-isolated transactions (`ReadTx`, `WriteTx`) to `Store`.

---

## Architecture

**Data structure stack:**

```
Store → HashMap<u64, Arc<Snapshot>> → HashMap<String, Arc<dyn Any>> → Table<R> → BTree<R> → Arc<BTreeNode<R>>
```

Mutations create new tree roots sharing unchanged subtrees via `Arc`, so old versions stay alive for free. `Clone` is O(1) at every layer (Arc bump).

**Isolation level:** Snapshot Isolation — prevents dirty reads, nonrepeatable reads, phantom reads. Does *not* prevent write skew.

---

## Components

### `src/btree.rs` (new)

Persistent B-tree (T=3, MAX_KEYS=5). Not re-exported; internal implementation detail.

```rust
pub struct BTree<R> {
    root: Arc<BTreeNode<R>>,
    len: usize,
}

struct BTreeNode<R> {
    entries: Vec<(u64, Arc<R>)>,
    children: Vec<Arc<BTreeNode<R>>>,
}
```

- `insert`/`remove` return a *new* `BTree`; original is unchanged.
- Values stored as `Arc<R>` — no `R: Clone` bound needed.
- `Clone` is O(1) (Arc bump on root).
- Bottom-up splitting via recursive `InsertResult::Split` propagation.
- Deletion uses check-before-delete pattern and rebalances via rotate/merge.
- `range()` returns `BTreeRange<'a, R>` — a stack-based in-order iterator with bound filtering.

### `src/table.rs` (modified)

Backing store changed from `BTreeMap<u64, R>` to `BTree<R>`.

```rust
pub struct Table<R> {
    data: BTree<R>,
    next_id: u64,
}
```

- `Clone` added — O(1), preserves `next_id`. Enables lazy CoW in `WriteTx`.
- `insert`/`update`/`delete` reassign `self.data` to the new tree returned by `BTree` methods.
- `range` returns `BTreeRange<'a, R>` instead of `impl Iterator`.

### `src/store.rs` (replaced)

```rust
pub struct Store {
    snapshots: HashMap<u64, Arc<Snapshot>>,
    latest_version: u64,
    next_version: u64,
}

pub(crate) struct Snapshot {
    pub(crate) version: u64,
    pub(crate) tables: HashMap<String, Arc<dyn Any>>,
}
```

- `Store::new()` creates version 0 (empty snapshot).
- `begin_read(Option<u64>)` — returns `ReadTx` holding `Arc<Snapshot>`. Zero-copy.
- `begin_write(Option<u64>)` — returns `WriteTx`. Auto-assigns next version or accepts explicit version (must be > latest, else `WriteConflict`).

### `ReadTx` / `WriteTx` (new, defined in `src/store.rs`)

```rust
pub struct ReadTx {
    snapshot: Arc<Snapshot>,
}

pub struct WriteTx {
    base: Arc<Snapshot>,
    dirty: HashMap<String, Box<dyn Any>>,
    version: u64,
}
```

- `ReadTx::open_table<R>` — downcast from `Arc<dyn Any>`, returns `&Table<R>`. `KeyNotFound` if table doesn't exist, `TypeMismatch` if wrong type.
- `WriteTx::open_table<R>` — lazily clones table from base snapshot into `dirty` map on first access (O(1) per table). Creates empty table if not in base. Returns `&mut Table<R>`.
- `WriteTx::commit(store)` — builds new snapshot by merging base tables with dirty tables, registers in store. Returns version number.
- `WriteTx::rollback()` — drops self, no store mutation.

### `src/transaction.rs` (new)

Pure re-export of `ReadTx` and `WriteTx` from `store.rs`. Avoids circular module dependency.

### `src/lib.rs` (modified)

```rust
pub mod btree;
pub mod error;
pub mod store;
pub mod table;
pub mod transaction;

pub use error::{Error, Result};
pub use store::Store;
pub use table::Table;
pub use transaction::{ReadTx, WriteTx};
```

---

## Data Flow

```
store.begin_write(None)          → WriteTx (base = latest snapshot)
  wtx.open_table::<User>("users") → &mut Table<User> (lazy clone from base)
    table.insert(user)             → u64
    table.update(id, user)         → Result<()>
    table.delete(id)               → Result<()>
  wtx.commit(&mut store)          → new snapshot registered, version returned

store.begin_read(Some(v))        → ReadTx (Arc<Snapshot> at version v)
  rtx.open_table::<User>("users") → &Table<User> (zero-copy borrow)
    table.get(id)                  → Option<&User>
    table.range(1..=100)           → BTreeRange (iterator)
```

---

## Error Handling

| Operation | Error condition | Variant |
|---|---|---|
| `begin_read` | Version doesn't exist | `KeyNotFound` |
| `begin_write` | Explicit version ≤ latest | `WriteConflict` |
| `ReadTx::open_table` | Table not in snapshot | `KeyNotFound` |
| `ReadTx::open_table` | Wrong type | `TypeMismatch` |
| `WriteTx::open_table` | Wrong type (base or dirty) | `TypeMismatch` |

---

## Current Limitations

- Single-writer is a design convention, not runtime-enforced.
- No `Send + Sync` on snapshots (`dyn Any` doesn't require it). `#![allow(clippy::arc_with_non_send_sync)]` in `store.rs`.
- No snapshot GC.
- No disk persistence.