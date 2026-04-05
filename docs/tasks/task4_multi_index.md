# Task 4: Multiple Secondary Indexes

## Motivation

Before this task, the only way to look up a record was by its auto-increment primary key (`u64`). Any query by a field value (e.g. "find user by email") required a full table scan via `range(..)`. Secondary indexes eliminate this by maintaining sorted mappings from field values to primary IDs, giving O(log n) lookups on arbitrary fields.

---

## Design decisions

### 1. Generalize BTree from `BTree<R>` to `BTree<K, V>`

The existing B-tree hardcoded `u64` as the key type. Indexes need arbitrary key types (`String`, `u32`, tuples, etc.), so the first step was generalizing the B-tree to accept any `K: Ord + Clone`.

**Alternatives considered:**

| Approach | Pros | Cons |
|----------|------|------|
| **A. Generalize existing BTree** | Single well-tested data structure; natural Rust generics; no serialization overhead | Touches every function in btree.rs |
| B. Byte-encoded keys in `BTree<u64, _>` | No BTree changes needed | Serialization overhead; ordering bugs; opaque keys |
| C. `std::collections::BTreeMap` in `Arc` | Zero implementation effort | O(n) clone on mutation — destroys MVCC structural sharing |
| D. Separate generic B-tree | Clean separation | Doubles maintenance surface of the most complex module |

**Chosen: A.** The change was mechanical — every `u64` became `K`, every `binary_search_by_key` became `binary_search_by`. The existing 20+ unit tests validated correctness after the swap. Two new tests (`string_keys_work`, `tuple_keys_work`) confirmed non-integer key types.

The key signature changes:

```
get(key: u64)    → get(key: &K)       // by reference for lookups
insert(key: u64) → insert(key: K)     // by value for storage
remove(key: u64) → remove(key: &K)    // by reference
range iterator   → yields (&K, &V)    // references, not copies
```

### 2. Indexes live inside `Table<R>`

**Alternatives considered:**

| Approach | Pros | Cons |
|----------|------|------|
| **A. Inside Table** | Auto-maintained on insert/update/delete; participates in Table's O(1) clone; simple user API | Table becomes more complex; index key types are type-erased |
| B. Separate objects in WriteTx | Table stays simple; index key types are statically typed | User must open indexes separately; WriteTx must coordinate mutations |
| C. Hybrid (defined on Table, stored separately) | Clean user API; independent CoW | More internal complexity |

**Chosen: A.** The defining advantage is that insert/update/delete automatically maintain all indexes with no user intervention. The type-erasure cost is acceptable — it's the same `dyn Any` + `downcast_ref` pattern already used for tables in snapshots.

### 3. Closure-based key extraction

Indexes are defined with a closure that extracts the index key from a record:

```rust
table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone());
table.define_index("by_age", IndexKind::NonUnique, |u: &User| u.age);
```

**Alternatives considered:**

- **Trait-based** (`impl Indexable for User`): requires one trait impl per index, needs wrapper types for multiple indexes on the same type.
- **Derive macro** (`#[derive(Indexed)]`): most magic, requires a proc-macro crate — heavy for a library this size.
- **Closure-based**: most ergonomic, no boilerplate, supports computed/composite keys like `|u| (u.last_name.clone(), u.first_name.clone())`.

### 4. Non-unique index uses composite keys

**Unique index:** `BTree<K, u64>` — maps index key directly to one primary ID.

**Non-unique index:** `BTree<(K, u64), ()>` — composite key of (index_key, primary_id) with unit value.

**Why not `BTree<K, Vec<u64>>`?** Each mutation would require cloning the `Vec` on the CoW path. With the composite-key approach, each insert/delete is a single B-tree operation. Lookups use a range scan: `(key, 0)..=(key, u64::MAX)` collects all primary IDs for a given index key in O(log n + m) where m is the result count.

This works because Rust's tuple `Ord` implementation uses lexicographic ordering, so all entries sharing the same index key are contiguous.

### 5. `Table::insert` returns `Result<u64>` (breaking change)

Previously `insert` returned `u64` (infallible). With unique indexes, insertion can fail with `DuplicateKey`. Changing the return type to `Result<u64>` was the cleanest way to surface this. All existing callers were updated in one pass.

---

## Architecture

### Data structure stack (post-implementation)

```
Store
  └─ snapshots: HashMap<u64, Arc<Snapshot>>
       └─ tables: HashMap<String, Arc<dyn Any>>
            └─ Table<R>
                 ├─ data: BTree<u64, R>              ← primary data
                 ├─ next_id: u64
                 └─ indexes: HashMap<String, Box<dyn IndexMaintainer<R>>>
                      ├─ "by_email" → UniqueIndex<R, String>
                      │                 └─ tree: BTree<String, u64>
                      └─ "by_age"   → NonUniqueIndex<R, u32>
                                       └─ tree: BTree<(u32, u64), ()>
```

### Type erasure

Each index has a different key type `K`, so they must be stored uniformly. The `IndexMaintainer<R>` trait provides an object-safe interface:

```rust
trait IndexMaintainer<R> {
    fn on_insert(&mut self, id: u64, record: &R) -> Result<()>;
    fn on_update(&mut self, id: u64, old: &R, new: &R) -> Result<()>;
    fn on_delete(&mut self, id: u64, record: &R);
    fn clone_box(&self) -> Box<dyn IndexMaintainer<R>>;
    fn as_any(&self) -> &dyn Any;
}
```

`Table` stores `HashMap<String, Box<dyn IndexMaintainer<R>>>`. Lookup methods like `get_unique::<K>()` recover the concrete type via `as_any().downcast_ref::<UniqueIndex<R, K>>()`. A mismatched key type returns `Error::IndexTypeMismatch`.

### MVCC integration

Indexes participate in snapshots automatically because they live inside `Table<R>`:

1. **`WriteTx::open_table`** clones the `Table<R>` (O(1) via BTree Arc clone). This clones all indexes too — each `clone_box()` clones the index's BTree (O(1)) and the extractor `Arc<dyn Fn>` (O(1)).
2. **Mutations** on the write transaction's copy maintain its indexes independently.
3. **`WriteTx::commit`** stores the entire `Table<R>` (with indexes) in the new snapshot.
4. **`ReadTx`** sees the indexes as they were at that snapshot version.

No changes to `Store`, `WriteTx`, `ReadTx`, or `Snapshot` were needed.

### Rollback on multi-index failure

When `insert` or `update` must update N indexes, a failure at index i (e.g. unique constraint violation) must not leave indexes 0..i-1 in an inconsistent state. The implementation uses a sequential rollback:

```
for each index i in 0..N:
    apply mutation to index i
    if error:
        for each index j in 0..i:
            reverse mutation on index j
        return error
```

For `insert` rollback, `on_delete` reverses a successful `on_insert`. For `update` rollback, `on_update(id, new, old)` reverses `on_update(id, old, new)`. This is safe because the underlying BTree operations are pure (they return new trees without mutating old ones).

---

## Public API

### Defining indexes

```rust
// Unique: one record per key, DuplicateKey error on conflict
table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())?;

// Non-unique: multiple records per key
table.define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)?;

// Composite keys work naturally via tuples
table.define_index("by_name", IndexKind::Unique, |u: &User| {
    (u.last_name.clone(), u.first_name.clone())
})?;
```

`define_index` is idempotent by name. If called on a non-empty table, it backfills the index by scanning all existing records.

### Querying indexes

```rust
// Unique lookup: Option<(u64, &R)>
let alice = table.get_unique::<String>("by_email", &"alice@example.com".into())?;

// Non-unique lookup: Vec<(u64, &R)>
let age_30 = table.get_by_index::<u32>("by_age", &30)?;

// Range scan (works for both unique and non-unique)
let young = table.index_range::<u32>("by_age", 18..=25)?;
```

### Automatic maintenance

`insert`, `update`, and `delete` maintain all defined indexes. No manual index updates are needed.

```rust
let id = table.insert(user)?;          // updates all indexes
table.update(id, updated_user)?;       // updates all indexes
table.delete(id)?;                     // removes from all indexes
```

---

## Error variants added

| Variant | When |
|---------|------|
| `DuplicateKey(String)` | Insert/update violates a unique index constraint |
| `IndexNotFound(String)` | Lookup on an index name that was never defined |
| `IndexTypeMismatch(String)` | Lookup with wrong key type (e.g. `get_unique::<u32>` on a `String`-keyed index) |

---

## Performance characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| `define_index` (empty table) | O(1) | Creates empty BTree |
| `define_index` (n records) | O(n log n) | Backfills by scanning all records |
| `insert` with k indexes | O(k log n) | One B-tree insert per index |
| `update` with k indexes | O(k log n) | One remove + one insert per changed index key |
| `delete` with k indexes | O(k log n) | One B-tree remove per index |
| `get_unique` | O(log n) | Single B-tree lookup + primary lookup |
| `get_by_index` | O(log n + m) | Range scan on composite key; m = result count |
| `index_range` | O(log n + m) | Same as get_by_index with custom bounds |
| `Table::clone` | O(k) | O(1) per index (Arc clone); k = number of indexes |

All complexities are in terms of the index size (n) and result count (m). The persistent CoW property is preserved — mutations create new B-tree roots sharing unchanged subtrees.

---

## Files changed

| File | Change |
|------|--------|
| `src/btree.rs` | Generalized `BTree<R>` to `BTree<K, V>` where `K: Ord + Clone` |
| `src/index.rs` | **New.** `IndexKind`, `IndexMaintainer` trait, `UniqueIndex`, `NonUniqueIndex` |
| `src/table.rs` | Added index storage, `define_index`, lookup methods; `insert` returns `Result<u64>` |
| `src/error.rs` | Added `DuplicateKey`, `IndexNotFound`, `IndexTypeMismatch` |
| `src/lib.rs` | Added `pub mod index; pub use index::IndexKind;` |
| `tests/store_integration.rs` | 16 new index tests (unique, non-unique, multi-index rollback, MVCC, backfill, range scan) |
| `examples/*.rs`, `benches/*.rs` | Updated for `insert() -> Result<u64>` |

---

## Limitations and future work

- **No index removal.** Once defined, an index cannot be dropped. Adding `remove_index` would be straightforward but was not in scope.
- **No partial/filtered indexes.** Every record is indexed. A filtered index (e.g. "only active users") would require extending the closure to return `Option<K>`.
- **Index definitions are not persisted in metadata.** After a `WriteTx::open_table` clone, the indexes are present (they're part of `Table`), but if a fresh `Table::new()` is created in a new write transaction for an existing table name, indexes must be re-defined. In practice this doesn't happen because `open_table` clones from the base snapshot.
- **HashMap iteration order for rollback.** Index mutation order depends on `HashMap` iteration order, which is nondeterministic. This is correct (all indexes are updated or none are) but means error messages may vary between runs when multiple indexes would fail. If deterministic ordering matters, the `indexes` field could be changed to `IndexMap` or `BTreeMap<String, _>`.
- **`unsafe` in update/delete.** A raw pointer is used to reborrow the old record from `self.data` while mutating `self.indexes`. This is sound because the data BTree is CoW (never mutated in place), but could be eliminated by cloning the extractor keys before mutation.
