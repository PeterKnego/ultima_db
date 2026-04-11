# Task: Custom Indexes

## Motivation

Built-in secondary indexes (`define_index`) are powerful but constrained: they accept a key-extraction closure and produce a fixed-shape `BTree<K, u64>` or `BTree<(K, u64), ()>`. Some use cases require richer index structures — an inverted word index, an aggregate running total, a spatial index — where the user needs to control both the internal data structure and the query API.

Custom indexes address this by exposing a trait the user implements. The table then treats the implementation as a first-class index: it maintains it automatically on every `insert`, `update`, and `delete`, and snapshots it as part of the CoW commit cycle.

---

## Goals

- Users can implement custom index data structures (full-text, spatial, aggregate, etc.) with arbitrary query APIs.
- Custom indexes participate in the mutation lifecycle (insert/update/delete) and can veto mutations.
- Custom indexes are CoW-friendly and work correctly with snapshot isolation.
- The public `BTree<K, V>` is available as an O(1)-clone building block for custom index authors.
- No changes to the internal `IndexMaintainer` trait or mutation dispatch logic.
- Persistence is handled via rebuild-from-data initially, with a non-breaking path to serialized persistence later.

## Non-goals

- Persistence of custom index state in WAL/checkpoints (future work).
- Thread-safe concurrent access to custom indexes (follows existing `Send + Sync` deferral).

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

## Architecture: thin adapter layer

The internal `IndexMaintainer<R>` trait remains unchanged and `pub(crate)`. A new public `CustomIndex<R>` trait defines the user-facing contract. A `CustomIndexAdapter<R, I>` bridges the two, allowing custom indexes to be stored in the existing `Table.indexes` map alongside built-in indexes.

```
User implements:     CustomIndex<R>            (public, ergonomic)
Adapter wraps:       CustomIndexAdapter<R, I>  (implements IndexMaintainer<R>)
Stored in:           Table.indexes             (single map, no duplication)
Handle obtained via: table.custom_index::<I>("name") → &I
```

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

Public API contract (all already exist):
- `BTree::new()` — empty tree
- `BTree::insert(key, value) -> BTree` — returns new tree
- `BTree::remove(&key) -> Result<BTree>` — returns new tree
- `BTree::get(&key) -> Option<&V>`
- `BTree::range(bounds) -> impl Iterator`
- `BTree::len()`, `BTree::is_empty()`
- `Clone` — O(1)

No changes to `BTree` internals.

### 4. Typed query access via `custom_index::<I>`

```rust
let idx: &MyIndex = table.custom_index::<MyIndex>("name")?;
idx.my_query_method(...);
```

The `as_any()` method on `IndexMaintainer` is used to downcast the erased `CustomIndexAdapter<R, I>` back to the concrete type, from which an `inner` accessor returns `&I`. This is the same downcast pattern used by `get_unique` and `get_by_index` for built-in indexes.

Errors returned:
- `IndexNotFound` — the name was never registered.
- `IndexTypeMismatch` — the name exists but `I` doesn't match the registered type.

### 5. `resolve` decouples ID lists from record access

Custom indexes return IDs, not records (the built-in pattern). `resolve(&ids)` converts a slice of IDs into `(id, &R)` pairs in a single pass, silently skipping missing IDs. This keeps `CustomIndex<R>` implementations simple — they only need to store IDs — and matches the lookup pattern of built-in `get_by_index`.

### 6. `IndexAlreadyExists` replaces silent overwrites

`define_custom_index` (and `define_index`) return `Err(IndexAlreadyExists)` if any index — built-in or custom — with the same name already exists. This prevents accidental shadowing and makes it clear that index names are a shared namespace.

Unlike built-in `define_index`, which allows idempotent re-registration, custom indexes cannot verify type equality at runtime, so duplicates are rejected outright.

### 7. Persistence strategy: rebuild from data

Custom indexes are not serialized directly. On recovery, the application re-registers them via `define_custom_index`, which calls `index.rebuild(data.range(..))`. The default `rebuild` iterates all records and calls `on_insert`. Index authors can override `rebuild` for more efficient bulk-load implementations (e.g. sorting before insertion). This keeps the persistence format simple and avoids any serde bounds on `CustomIndex<R>`.

**Future path (designed for, not implemented):** An optional extension trait could be added later:

```rust
pub trait PersistableIndex<R>: CustomIndex<R> + Serialize + Deserialize { }
```

The checkpoint system would check (via `as_any` downcast) whether an index implements this trait, persisting it if so and falling back to rebuild if not. No breaking changes to `CustomIndex<R>`.

---

## API surface

### The `CustomIndex<R>` trait

```rust
pub trait CustomIndex<R: Record>: Send + Sync + Clone + 'static {
    /// Called when a record is inserted. Return Err to veto.
    fn on_insert(&mut self, id: u64, record: &R) -> Result<()>;

    /// Called when a record is updated. Return Err to veto.
    fn on_update(&mut self, id: u64, old: &R, new: &R) -> Result<()>;

    /// Called when a record is deleted.
    fn on_delete(&mut self, id: u64, record: &R);

    /// Rebuild the entire index from an iterator of (id, record) pairs.
    /// Used for backfilling on define and for recovery from persistence.
    fn rebuild<'a>(&mut self, data: impl Iterator<Item = (u64, &'a R)>) -> Result<()>
    where
        R: 'a,
    {
        for (id, record) in data {
            self.on_insert(id, record)?;
        }
        Ok(())
    }
}
```

`on_insert` and `on_update` may return `Err` to veto the mutation (e.g. to enforce a custom constraint). On error, the table rolls back all changes to that point.

Key properties:
- **`Clone` bound** — required for CoW snapshot cloning via `clone_box()`. Authors should use `BTree<K, V>` internally for O(1) clone.
- **`rebuild` has a default implementation** — iterates and calls `on_insert`. Authors can override for efficiency.
- **No `KeyExtractor` involved** — the index owns its entire extraction and storage logic.
- **Same mutation signatures as `IndexMaintainer`** — makes the adapter trivial.

### The adapter (internal)

```rust
pub(crate) struct CustomIndexAdapter<R, I: CustomIndex<R>> {
    inner: I,
    name: String,
    _phantom: PhantomData<R>,
}
```

Implements `IndexMaintainer<R>` by delegating:
- `on_insert` / `on_update` / `on_delete` → `inner.on_insert` / etc.
- `clone_box()` → clones `inner` via `Clone` bound, wraps in a new adapter.
- `as_any()` → returns `self`, enabling downcast to `CustomIndexAdapter<R, I>`.
- `kind()` → returns `IndexKind::Custom`, preventing built-in query methods (`get_unique`, `get_by_index`) from accidentally attempting downcasts on custom indexes.

### `Table<R>` methods

```rust
// Register a custom index. Backfills if the table is non-empty.
pub fn define_custom_index<I: CustomIndex<R>>(&mut self, name: &str, index: I) -> Result<()>;

// Retrieve a typed reference to the custom index for querying.
pub fn custom_index<I: CustomIndex<R>>(&self, name: &str) -> Result<&I>;

// Map primary IDs to records. Missing IDs are silently skipped.
pub fn resolve(&self, ids: &[u64]) -> Vec<(u64, &R)>;
```

`TableWriter` (the `WriteTx` table handle) exposes all three as pass-throughs.

Under the hood, `define_custom_index` shares the `indexes` map with built-in indexes (so name collisions are caught), backfills via `rebuild()` before inserting the adapter, and rejects duplicate names with `IndexAlreadyExists`. `custom_index` downcasts the erased entry back to `CustomIndexAdapter<R, I>` and returns a borrow of `inner`. `resolve` keeps the handle lifetime independent from the table — users query the index for IDs, then resolve to records in a separate call.

---

## Mutation dispatch and rollback

**No changes required to existing dispatch logic.** Custom indexes are stored in the same `indexes` map as built-in indexes. The existing raw-pointer iteration loop in `Table::insert`/`update`/`delete` dispatches `on_insert`/`on_update`/`on_delete` through the `IndexMaintainer` trait, which the adapter implements. Rollback on failure (undoing previously applied indexes) works identically.

**Batch operations** (`insert_batch`/`update_batch`/`delete_batch`) use snapshot-and-restore via `Table::clone`. The adapter's `clone_box` clones the inner `CustomIndex` via its `Clone` bound. If the author used `BTree` internally, clone is O(1).

---

## Usage example

```rust
#[derive(Clone)]
struct FullTextIndex {
    inverted: BTree<String, BTree<u64, ()>>,
    tokenizer: Arc<dyn Fn(&str) -> Vec<String> + Send + Sync>,
    field_extractor: Arc<dyn Fn(&Document) -> &str + Send + Sync>,
}

impl CustomIndex<Document> for FullTextIndex {
    fn on_insert(&mut self, id: u64, record: &Document) -> Result<()> {
        let text = (self.field_extractor)(record);
        for token in (self.tokenizer)(text) {
            let ids = self.inverted.get(&token)
                .cloned()
                .unwrap_or_else(BTree::new);
            self.inverted = self.inverted.insert(token, ids.insert(id, ()));
        }
        Ok(())
    }

    fn on_update(&mut self, id: u64, old: &Document, new: &Document) -> Result<()> {
        self.on_delete(id, old);
        self.on_insert(id, new)
    }

    fn on_delete(&mut self, id: u64, record: &Document) {
        let text = (self.field_extractor)(record);
        for token in (self.tokenizer)(text) {
            if let Some(ids) = self.inverted.get(&token).cloned() {
                if let Ok(new_ids) = ids.remove(&id) {
                    self.inverted = self.inverted.insert(token, new_ids);
                }
            }
        }
    }
}

impl FullTextIndex {
    pub fn search(&self, query: &str) -> Vec<u64> {
        let terms = (self.tokenizer)(query);
        // Intersect ID sets across terms for AND semantics
        // ...
        todo!()
    }
}

// --- Usage ---
let mut table = wtx.open_table::<Document>("docs")?;
table.define_custom_index("search", FullTextIndex::new(/* ... */))?;

let id = table.insert(Document { title: "Hello world".into(), body: "...".into() })?;

let ids = table.custom_index::<FullTextIndex>("search")?.search("hello");
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
| `src/btree.rs` | No changes (already a public module) |
| `tests/custom_index_api.rs` | Integration tests for custom indexes through `WriteTx`/`ReadTx` |
| `docs/architecture.md` | Added custom index section; added `index` row to module table |

---

## Limitations and future work

- **No index removal.** Once defined, a custom index cannot be dropped. Adding `remove_custom_index` would be straightforward but was out of scope.
- **No `mut` access for queries.** `custom_index` returns `&I`, not `&mut I`. If an index needs internal mutability for caching, the implementor must use `RefCell` or similar. This is intentional — mutable query access would conflict with the shared-reference semantics of `ReadTx::open_table`.
- **`rebuild` default is O(n log n).** The default `rebuild` implementation calls `on_insert` for each record. Indexes whose `on_insert` is O(log n) will rebuild in O(n log n) total. Implementors can override `rebuild` for O(n) bulk-load if the index structure supports it.
- **No persistence of index data.** Custom indexes are rebuilt on every recovery. For indexes that are expensive to rebuild, a future task could add optional serialization support via the `PersistableIndex` extension trait sketched in the persistence section.