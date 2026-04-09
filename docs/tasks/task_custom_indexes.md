# Task: Custom Indexes

## Motivation

Built-in secondary indexes (`define_index`) are powerful but constrained: they accept a key-extraction closure and produce a fixed-shape `BTree<K, u64>` or `BTree<(K, u64), ()>`. Some use cases require richer index structures — an inverted word index, an aggregate running total, a spatial index — where the user needs to control both the internal data structure and the query API.

Custom indexes address this by exposing a trait the user implements. The table then treats the implementation as a first-class index: it maintains it automatically on every `insert`, `update`, and `delete`, and snapshots it as part of the CoW commit cycle.

**Design spec:** `docs/superpowers/specs/2026-04-09-custom-indexes-design.md`

---

## Feature overview

| Component | Description |
|-----------|-------------|
| `CustomIndex<R>` trait | Public trait the user implements to define index behavior |
| `define_custom_index` | Registers a custom index on a `Table<R>`; backfills from existing data |
| `custom_index::<I>` | Retrieves a typed `&I` reference for querying |
| `resolve` | Maps a slice of primary IDs to `(id, &record)` pairs |
| `CustomIndexAdapter` | Internal bridge that wraps `CustomIndex<R>` into `IndexMaintainer<R>` |
| `IndexKind::Custom` | New variant on the existing enum; returned by adapter's `kind()` |
| `IndexAlreadyExists` | New error variant; returned when a name collision is detected |

---

## Design decisions

### 1. Adapter pattern: `CustomIndexAdapter` bridges `CustomIndex<R>` into `IndexMaintainer<R>`

All indexes — built-in and custom — are stored in `Table::indexes` as `Box<dyn IndexMaintainer<R>>`. Rather than adding a separate map for custom indexes, a `CustomIndexAdapter<R, I>` wraps any `I: CustomIndex<R>` and forwards `IndexMaintainer` calls through. This means:

- Custom indexes participate in the existing rollback logic (snapshot-restore for batch ops, sequential undo for single-record ops) for free.
- `Table::clone()` handles custom indexes without any special casing: `clone_box()` on the adapter delegates to `I::clone()`.
- The commit path is unchanged — the adapter is just another entry in the indexes map.

**Alternative considered:** A separate `custom_indexes: HashMap<String, Box<dyn Any>>` alongside the existing `indexes` map. Rejected because it would require duplicating the maintenance loop in `insert`/`update`/`delete` and handling rollback separately.

### 2. `Clone` bound on `CustomIndex<R>`

The trait requires `Clone` rather than providing a `clone_box`-style method. This matches how Rust idioms work for user-implemented types and keeps the trait surface small.

For O(1) clone — critical for MVCC snapshot performance — index authors should store their data in `BTree<K, V>`, the same persistent CoW B-tree used by the rest of UltimaDB. `BTree` is re-exported from the crate root (`use ultima_db::BTree`) specifically for this purpose.

Simple aggregates (e.g. a `u64` running sum) clone in O(1) trivially. Index authors who use `std::collections::BTreeMap` internally will pay O(n) on every table clone; this is their choice to make.

### 3. `BTree<K, V>` as a public building block

`BTree` was internal before this feature. Making it public via `pub use btree::BTree` in `lib.rs` lets custom index authors use the same O(1)-clone, CoW, snapshot-safe data structure that built-in indexes use. The alternative — telling users to use `std::BTreeMap` and accept O(n) clone costs — would undermine the core MVCC performance guarantee.

### 4. Typed query access via `custom_index::<I>`

```rust
let idx: &MyIndex = table.custom_index::<MyIndex>("name")?;
idx.my_query_method(...);
```

The `as_any()` method on `IndexMaintainer` is used to downcast the erased `CustomIndexAdapter<R, I>` back to the concrete type, from which `inner()` returns `&I`. This is the same downcast pattern used by `get_unique` and `get_by_index` for built-in indexes.

Errors returned:
- `IndexNotFound` — the name was never registered.
- `IndexTypeMismatch` — the name exists but `I` doesn't match the registered type.

### 5. `resolve` decouples ID lists from record access

Custom indexes return IDs, not records (the built-in pattern). `resolve(&ids)` converts a slice of IDs into `(id, &R)` pairs in a single pass, silently skipping missing IDs. This keeps `CustomIndex<R>` implementations simple — they only need to store IDs — and matches the lookup pattern of built-in `get_by_index`.

### 6. `IndexAlreadyExists` replaces silent overwrites

`define_custom_index` (and `define_index`) return `Err(IndexAlreadyExists)` if any index — built-in or custom — with the same name already exists. This prevents accidental shadowing and makes it clear that index names are a shared namespace.

### 7. Persistence strategy: rebuild from data

Custom indexes are not serialized directly. On recovery, `Table::define_custom_index` calls `index.rebuild(data.range(..))`, which by default iterates all records and calls `on_insert`. Index authors can override `rebuild` for more efficient bulk-load implementations (e.g. sorting before insertion). This keeps the persistence format simple and avoids any serde bounds on `CustomIndex<R>`.

---

## API surface

### `CustomIndex<R>` trait

```rust
pub trait CustomIndex<R: Record>: Send + Sync + Clone + 'static {
    fn on_insert(&mut self, id: u64, record: &R) -> Result<()>;
    fn on_update(&mut self, id: u64, old: &R, new: &R) -> Result<()>;
    fn on_delete(&mut self, id: u64, record: &R);

    // Optional override for bulk backfill / recovery
    fn rebuild<'a>(&mut self, data: impl Iterator<Item = (u64, &'a R)>) -> Result<()>
    where
        R: 'a,
    { ... }
}
```

`on_insert` and `on_update` may return `Err` to veto the mutation (e.g. to enforce a custom constraint). On error, the table rolls back all changes to that point.

### `Table<R>` methods

```rust
// Register a custom index. Backfills if table is non-empty.
pub fn define_custom_index<I: CustomIndex<R>>(&mut self, name: &str, index: I) -> Result<()>;

// Retrieve a typed reference to the custom index for querying.
pub fn custom_index<I: CustomIndex<R>>(&self, name: &str) -> Result<&I>;

// Map primary IDs to records. Missing IDs are silently skipped.
pub fn resolve(&self, ids: &[u64]) -> Vec<(u64, &R)>;
```

`TableWriter` (the `WriteTx` table handle) exposes all three as pass-throughs.

### Usage example

```rust
#[derive(Clone)]
struct WordIndex {
    map: BTree<String, Vec<u64>>,
}

impl CustomIndex<Article> for WordIndex {
    fn on_insert(&mut self, id: u64, record: &Article) -> Result<()> {
        for word in record.body.split_whitespace() {
            // ... index word -> id
        }
        Ok(())
    }
    // on_update, on_delete omitted for brevity
}

let mut table = wtx.open_table::<Article>("articles")?;
table.define_custom_index("words", WordIndex::new())?;
table.insert(Article { body: "hello world".into() })?;

let idx = table.custom_index::<WordIndex>("words")?;
let ids = idx.search("hello");
let results = table.resolve(&ids);
```

---

## Error variants added

| Variant | When |
|---------|------|
| `IndexAlreadyExists(String)` | `define_custom_index` (or `define_index`) called with a name already registered |

---

## Files changed

| File | Change |
|------|--------|
| `src/index.rs` | Added `IndexKind::Custom` variant, `CustomIndex<R>` public trait, `CustomIndexAdapter<R, I>` internal struct, unit tests |
| `src/table.rs` | Added `define_custom_index`, `custom_index`, `resolve`; `define_index` returns error on `IndexKind::Custom`; 16 unit tests |
| `src/error.rs` | Added `IndexAlreadyExists(String)` variant |
| `src/lib.rs` | Re-exported `BTree` and `CustomIndex` from crate root |
| `src/store.rs` | Added `define_custom_index`, `custom_index`, `resolve` pass-throughs to `TableWriter` |
| `tests/custom_index_api.rs` | Integration tests for custom indexes through `WriteTx`/`ReadTx` |
| `docs/architecture.md` | Added custom index section; added `index` row to module table |

---

## Limitations and future work

- **No index removal.** Once defined, a custom index cannot be dropped. Adding `remove_custom_index` would be straightforward but was out of scope.
- **No `mut` access for queries.** `custom_index` returns `&I`, not `&mut I`. If an index needs internal mutability for caching, the implementor must use `RefCell` or similar. This is intentional — mutable query access would conflict with the shared-reference semantics of `ReadTx::open_table`.
- **`rebuild` default is O(n log n).** The default `rebuild` implementation calls `on_insert` for each record. Indexes whose `on_insert` is O(log n) will rebuild in O(n log n) total. Implementors can override `rebuild` for O(n) bulk-load if the index structure supports it.
- **No persistence of index data.** Custom indexes are rebuilt on every recovery. For indexes that are expensive to rebuild, a future task could add optional serialization support (gated on `R: Serialize` and a separate `CustomIndex: Serialize`).
