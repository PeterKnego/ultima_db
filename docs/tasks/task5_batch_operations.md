# Task 5: Batch CRUD Operations

## Motivation

Before this task, every `insert`, `update`, and `delete` on `Table<R>` maintained indexes inline — each mutation iterated all indexes, checked constraints, and handled rollback individually. For N records with K indexes this means N×K index update cycles, N×K potential rollback paths, and poor cache locality due to interleaving data and index mutations.

Batch operations address this by separating data mutations from index maintenance: mutate the data B-tree for all records first, then update each index in a single pass. This provides better cache locality, fewer intermediate CoW tree copies, and simpler error handling via snapshot-restore instead of incremental rollback.

---

## Design decisions

### 1. Deferred indexing via two-phase mutation

**Alternatives considered:**

| Approach | Pros | Cons |
|----------|------|------|
| **A. Two-phase: data first, then indexes** | Clear separation; one pass per index; simple rollback | Records temporarily exist in data tree without index coverage |
| B. Buffered mutations (collect ops, apply all at once) | No temporary inconsistency | Requires a separate mutation buffer; duplicates BTree logic |
| C. Batch methods on IndexMaintainer trait | Could amortize B-tree path traversal | Premature; single `on_insert`/`on_update` is already O(log n); would require bulk-load B-tree API |
| D. Parallel index updates per record (same as single-record but in a loop) | No new abstractions | Same N×K overhead; no improvement over calling `insert` in a loop |

**Chosen: A.** The two-phase approach is the simplest design that achieves the goal. The temporary inconsistency (records in data tree but not yet in indexes) is invisible — it exists only within the `&mut self` scope of the batch method. If phase 2 fails, the snapshot-restore reverts everything atomically.

### 2. Snapshot-restore rollback (not incremental)

The single-record methods use incremental rollback: if index N fails, iterate indexes 0..N-1 and reverse each. For batch operations, this becomes quadratic — a failure at the last record of the last index would require reversing all records across all previously-updated indexes.

**Alternatives considered:**

| Approach | Pros | Cons |
|----------|------|------|
| **A. Snapshot before, restore on failure** | O(1) rollback; trivially correct; no partial-undo logic | Snapshot has upfront cost (O(K) for K indexes) |
| B. Incremental undo per record per index | No upfront cost if everything succeeds | Complex bookkeeping; quadratic worst case; error-prone |
| C. Clone the entire Table, operate on clone, swap on success | Clean semantics | Identical cost to snapshot-restore but less explicit |

**Chosen: A.** The snapshot cost is negligible: `BTree::clone` is O(1) (Arc bump on root), and each index `clone_box()` is O(1) for the same reason. The restore is a simple field swap. This eliminates all rollback complexity from the batch paths.

The `TableSnapshot<R>` struct captures the three mutable fields:

```rust
struct TableSnapshot<R> {
    data: BTree<u64, R>,
    next_id: u64,
    indexes: HashMap<String, Box<dyn IndexMaintainer<R>>>,
}
```

### 3. Fail-fast validation before mutation

For `update_batch` and `delete_batch`, all IDs are validated (and old records collected via `get_arc`) *before* the snapshot is taken and any mutation begins. This avoids wasting a snapshot when the operation is guaranteed to fail.

`insert_batch` cannot fail-fast on data — the records themselves are the input and IDs are auto-assigned. Constraint violations can only be detected during the index phase.

### 4. Duplicate ID handling

| Operation | Behavior | Rationale |
|-----------|----------|-----------|
| `insert_batch` | N/A — IDs are auto-assigned | Cannot have duplicates |
| `update_batch` | Last value wins | Matches standard SQL `UPDATE` semantics; sequential application to the CoW BTree naturally produces this |
| `delete_batch` | Deduplicated via `sort_unstable` + `dedup` | Without dedup, the second `remove` would fail with `KeyNotFound` on an already-deleted record |

### 5. API signatures

```rust
pub fn insert_batch(&mut self, records: Vec<R>) -> Result<Vec<u64>>
pub fn update_batch(&mut self, updates: Vec<(u64, R)>) -> Result<()>
pub fn delete_batch(&mut self, ids: &[u64]) -> Result<()>
```

- `insert_batch` takes `Vec<R>` by value (records are consumed into the BTree) and returns assigned IDs.
- `update_batch` takes `Vec<(u64, R)>` by value (same ownership reasoning).
- `delete_batch` takes `&[u64]` — it only needs to read IDs, not consume them. Deduplication creates a local copy internally.

All three return the same `Result` error types as their single-record counterparts (`DuplicateKey`, `KeyNotFound`).

---

## Architecture

### Execution flow

```
insert_batch(records)          update_batch(updates)          delete_batch(ids)
    │                               │                              │
    ▼                               ▼                              ▼
 [overflow check]              [validate IDs exist]           [deduplicate IDs]
    │                          [collect old Arc<R>]           [validate IDs exist]
    ▼                               │                         [collect old Arc<R>]
 snapshot()                         ▼                              │
    │                          snapshot()                          ▼
    ▼                               │                         snapshot()
 Phase 1: insert all               ▼                              │
 records into data BTree       Phase 1: replace all               ▼
    │                          records in data BTree          Phase 1: remove all
    ▼                               │                         records from data BTree
 Phase 2: one pass per             ▼                              │
 index, on_insert for all     Phase 2: one pass per              ▼
    │                          index, on_update for all       Phase 2: one pass per
    ▼                               │                         index, on_delete for all
 [on error: restore()]             ▼                          (infallible)
    │                          [on error: restore()]              │
    ▼                               │                              ▼
 return Ok(ids)                return Ok(())                  return Ok(())
```

### Interaction with existing single-record methods

Batch methods are independent of single-record methods — they share no code paths. Both use the same `IndexMaintainer::on_insert`/`on_update`/`on_delete` trait methods, but differ in orchestration:

- **Single-record**: indexes updated inline per record; incremental rollback via raw-pointer iteration.
- **Batch**: data mutations batched first; indexes updated in one pass per index; snapshot-restore rollback.

The `snapshot`/`restore` helpers are private and only used by batch methods.

### MVCC integration

No changes to `Store`, `WriteTx`, `ReadTx`, or `Snapshot` were needed. Batch methods operate on the `Table<R>` obtained via `WriteTx::open_table`, which is already a CoW clone of the base snapshot's table. The commit path is unchanged — the entire `Table<R>` (with indexes) is stored in the new snapshot.

---

## Public API

### Batch insert

```rust
let mut table = wtx.open_table::<User>("users")?;
table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())?;

let ids = table.insert_batch(vec![
    User { email: "alice@x.com".into(), age: 30, name: "Alice".into() },
    User { email: "bob@x.com".into(),   age: 25, name: "Bob".into() },
])?;
// ids == [1, 2]
```

### Batch update

```rust
table.update_batch(vec![
    (1, User { email: "alice_new@x.com".into(), age: 31, name: "Alice".into() }),
    (2, User { email: "bob_new@x.com".into(),   age: 26, name: "Bob".into() }),
])?;
```

### Batch delete

```rust
table.delete_batch(&[1, 2])?;
```

### Error semantics

All batch operations are atomic: either all records succeed, or the table is unchanged. There is no partial application.

| Error | When |
|-------|------|
| `DuplicateKey(String)` | `insert_batch`/`update_batch` violates a unique index constraint (within batch or against existing data) |
| `KeyNotFound` | `update_batch`/`delete_batch` receives an ID that does not exist |

---

## Performance characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| `insert_batch` (n records, k indexes) | O(n log n + k · n log n) | n data inserts + k passes of n index inserts |
| `update_batch` (n records, k indexes) | O(n log n + k · n log n) | Validation O(n log n) + n data inserts + k passes of n index updates |
| `delete_batch` (n records, k indexes) | O(n log n + k · n log n) | Dedup O(n log n) + n data removes + k passes of n index deletes |
| Snapshot (k indexes) | O(k) | O(1) per field (Arc clone on BTree, clone_box on each index) |
| Restore | O(1) | Field swap; old state is dropped |

Compared to calling single-record methods in a loop, the batch approach:
- Eliminates per-record rollback bookkeeping (raw pointer collection, applied-index tracking)
- Groups data mutations together and index mutations together for better cache locality
- Shares intermediate BTree structural changes across sequential inserts into the same tree

---

## Files changed

| File | Change |
|------|--------|
| `src/table.rs` | Added `TableSnapshot<R>`, `snapshot()`, `restore()`, `insert_batch()`, `update_batch()`, `delete_batch()`, and 23 unit tests |
| `tests/store_integration.rs` | 5 integration tests covering commit visibility, rollback, and MVCC isolation |

---

## Limitations and future work

- **No streaming/iterator input.** Batch methods take `Vec<R>`, requiring all records in memory upfront. A future `insert_iter` could accept `impl IntoIterator<Item = R>` and stream records through, though this complicates rollback (the iterator may be partially consumed).
- **No batch-level index methods.** The `IndexMaintainer` trait still processes one record at a time. A future `on_insert_batch` could amortize B-tree path traversal if a bulk-load API were added to the B-tree.
- **Snapshot cost scales with index count.** Each batch operation snapshots all indexes, even if none have unique constraints. For tables with many non-unique indexes, a future optimization could skip snapshotting indexes that cannot fail (non-unique `on_insert` and `on_delete` are infallible).
- **`update_batch` with duplicate IDs collects old records redundantly.** If ID 5 appears three times, `get_arc` is called three times and three `on_update` calls reference stale `old_arc` values (the first two point to records already overwritten in phase 1). This is semantically correct (the index correctly transitions from the original value to the final value) but wastes work. Deduplicating updates by keeping only the last value per ID would be more efficient.
