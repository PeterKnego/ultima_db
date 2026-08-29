# Custom Indexes Design

UltimaDB currently supports built-in secondary indexes (unique and non-unique) backed by `ManagedIndex<R, K, S>` with `KeyExtractor`-based key derivation. This design adds a general-purpose extension point where users can plug in arbitrary index structures with their own query APIs.

---

## Goals

- Users can implement custom index data structures (full-text, spatial, aggregate, etc.) with arbitrary query APIs.
- Custom indexes participate in mutation lifecycle (insert/update/delete) and can veto mutations.
- Custom indexes are CoW-friendly and work correctly with snapshot isolation.
- The public `BTree<K, V>` is available as an O(1)-clone building block for custom index authors.
- No changes to the internal `IndexMaintainer` trait or mutation dispatch logic.
- Persistence is handled via rebuild-from-data initially, with a non-breaking path to serialized persistence later.

## Non-goals

- Persistence of custom index state in WAL/checkpoints (future work).
- Thread-safe concurrent access to custom indexes (follows existing `Send + Sync` deferral).

---

## Architecture: Thin Adapter Layer (Approach C)

The internal `IndexMaintainer<R>` trait remains unchanged and `pub(crate)`. A new public `CustomIndex<R>` trait defines the user-facing contract. A `CustomIndexAdapter<R, I>` bridges the two, allowing custom indexes to be stored in the existing `Table.indexes` map alongside built-in indexes.

```
User implements:     CustomIndex<R>           (public, ergonomic)
Adapter wraps:       CustomIndexAdapter<R, I>  (implements IndexMaintainer<R>)
Stored in:           Table.indexes             (single map, no duplication)
Handle obtained via: table.custom_index::<I>("name") → &I
```

---

## The `CustomIndex<R>` trait

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
    fn rebuild(&mut self, data: impl Iterator<Item = (u64, &R)>) -> Result<()> {
        for (id, record) in data {
            self.on_insert(id, record)?;
        }
        Ok(())
    }
}
```

Key properties:
- **`Clone` bound** — required for CoW snapshot cloning via `clone_box()`. Authors should use `BTree<K, V>` internally for O(1) clone.
- **`rebuild` has a default implementation** — iterates and calls `on_insert`. Authors can override for efficiency.
- **No `KeyExtractor` involved** — the index owns its entire extraction and storage logic.
- **Same mutation signatures as `IndexMaintainer`** — makes the adapter trivial.

---

## The adapter

```rust
pub(crate) struct CustomIndexAdapter<R, I: CustomIndex<R>> {
    inner: I,
    name: String,
    _phantom: PhantomData<R>,
}
```

Implements `IndexMaintainer<R>` by delegating:
- `on_insert` / `on_update` / `on_delete` → `inner.on_insert` / etc.
- `clone_box()` → clones `inner` via `Clone` bound, wraps in new adapter.
- `as_any()` → returns `self`, enabling downcast to `CustomIndexAdapter<R, I>`.
- `kind()` → returns a new `IndexKind::Custom` variant (or a sentinel value — see below).

### `IndexKind` extension

Add `IndexKind::Custom` to the existing enum. This prevents built-in query methods (`get_unique`, `get_by_index`) from accidentally attempting downcasts on custom indexes. The adapter returns `IndexKind::Custom` from `kind()`.

---

## Registration and handle retrieval on `Table<R>`

### Registration

```rust
impl<R: Record> Table<R> {
    pub fn define_custom_index<I: CustomIndex<R>>(
        &mut self,
        name: &str,
        mut index: I,
    ) -> Result<()> {
        if self.indexes.contains_key(name) {
            return Err(Error::IndexAlreadyExists(name.to_string()));
        }
        index.rebuild(self.data.range(..).map(|(&id, r)| (id, r)))?;
        let adapter = CustomIndexAdapter {
            inner: index,
            name: name.to_string(),
            _phantom: PhantomData,
        };
        self.indexes.insert(name.to_string(), Box::new(adapter));
        Ok(())
    }
}
```

- Shares the `indexes` map with built-in indexes — name collisions are caught.
- Rejects duplicate names with `IndexAlreadyExists` (unlike built-in `define_index` which allows idempotent re-registration, custom indexes cannot verify type equality at runtime, so duplicates are rejected outright).
- Backfills via `rebuild()` before inserting.

### Handle retrieval

```rust
impl<R: Record> Table<R> {
    pub fn custom_index<I: CustomIndex<R>>(&self, name: &str) -> Result<&I> {
        let idx = self.indexes.get(name)
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;
        let adapter = idx.as_any()
            .downcast_ref::<CustomIndexAdapter<R, I>>()
            .ok_or_else(|| Error::IndexTypeMismatch(name.to_string()))?;
        Ok(&adapter.inner)
    }
}
```

Returns `&I` directly — the user's concrete type with full access to its query methods.

### Record resolution

```rust
impl<R: Record> Table<R> {
    pub fn resolve(&self, ids: &[u64]) -> Vec<(u64, &R)> {
        ids.iter()
            .filter_map(|&id| self.get(id).map(|r| (id, r)))
            .collect()
    }
}
```

Users query the index handle for IDs, then resolve to records via `table.resolve()`. This keeps the handle lifetime independent from the table.

---

## Making `BTree<K, V>` public

Currently `btree` is a public module but documented as internal. To support custom index authors:

- Re-export from `lib.rs`: `pub use btree::BTree;`
- Public API contract (all already exist):
  - `BTree::new()` — empty tree
  - `BTree::insert(key, value) -> BTree` — returns new tree
  - `BTree::remove(&key) -> Result<BTree>` — returns new tree
  - `BTree::get(&key) -> Option<&V>`
  - `BTree::range(bounds) -> impl Iterator`
  - `BTree::len()`, `BTree::is_empty()`
  - `Clone` — O(1)

No changes to `BTree` internals.

---

## Mutation dispatch and rollback

**No changes required.** Custom indexes are stored in the same `indexes` map as built-in indexes. The existing raw-pointer iteration loop in `Table::insert`/`update`/`delete` dispatches `on_insert`/`on_update`/`on_delete` through the `IndexMaintainer` trait, which the adapter implements. Rollback on failure (undoing previously applied indexes) works identically.

**Batch operations** (`insert_batch`/`update_batch`/`delete_batch`) use snapshot-and-restore via `Table::clone`. The adapter's `clone_box` clones the inner `CustomIndex` via its `Clone` bound. If the author used `BTree` internally, clone is O(1).

---

## Persistence strategy

### Initial implementation: rebuild from data

Custom indexes are not persisted in WAL/checkpoints. On recovery, the application re-registers custom indexes via `define_custom_index`, which triggers `rebuild()` from the restored table data. No serialization requirements on custom index types.

### Future path (designed for, not implemented)

An optional extension trait could be added later:

```rust
pub trait PersistableIndex<R>: CustomIndex<R> + Serialize + Deserialize { }
```

The checkpoint system would check (via `as_any` downcast) whether an index implements this trait, persisting it if so and falling back to rebuild if not. No breaking changes to `CustomIndex<R>`.

---

## Example: full-text search index

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
    }
}

// --- Usage ---
let mut table: Table<Document> = Table::new();
table.define_custom_index("search", FullTextIndex::new(/* ... */))?;

let id = table.insert(Document { title: "Hello world", body: "..." })?;

let ids = table.custom_index::<FullTextIndex>("search")?.search("hello");
let results = table.resolve(&ids);
```

---

## Changes summary

| File | Change |
|---|---|
| `src/index.rs` | Add `IndexKind::Custom` variant. Add `CustomIndex<R>` trait. Add `CustomIndexAdapter<R, I>`. |
| `src/table.rs` | Add `define_custom_index`, `custom_index`, `resolve` methods. |
| `src/lib.rs` | Re-export `BTree`, `CustomIndex`. |
| `src/btree.rs` | No changes (already public module). |
| `src/store.rs` | No changes. |
| `src/error.rs` | Add `IndexAlreadyExists` variant if not already present. |
