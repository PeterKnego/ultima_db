# Custom Indexes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a general-purpose custom index extension point so users can plug in arbitrary index structures with their own query APIs.

**Architecture:** A public `CustomIndex<R>` trait defines the user-facing contract. A `CustomIndexAdapter<R, I>` bridges it into the internal `IndexMaintainer<R>` trait. Custom indexes are stored in the existing `Table.indexes` map alongside built-in indexes. `BTree<K, V>` is re-exported as a public building block.

**Tech Stack:** Rust, no new dependencies.

**Spec:** `docs/superpowers/specs/2026-04-09-custom-indexes-design.md`

---

### Task 1: Add `IndexKind::Custom` variant and `IndexAlreadyExists` error

**Files:**
- Modify: `src/index.rs:9-13` (IndexKind enum)
- Modify: `src/error.rs:1-54` (Error enum)

- [ ] **Step 1: Write failing test for `IndexKind::Custom` variant**

In `src/index.rs`, add a test at the end of the `mod tests` block:

```rust
#[test]
fn index_kind_custom_variant() {
    let kind = IndexKind::Custom;
    assert_eq!(kind, IndexKind::Custom);
    assert_ne!(kind, IndexKind::Unique);
    assert_ne!(kind, IndexKind::NonUnique);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test index::tests::index_kind_custom_variant`
Expected: FAIL — no variant `Custom` in `IndexKind`

- [ ] **Step 3: Add `Custom` variant to `IndexKind`**

In `src/index.rs`, modify the `IndexKind` enum:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Unique,
    NonUnique,
    Custom,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test index::tests::index_kind_custom_variant`
Expected: PASS

- [ ] **Step 5: Write failing test for `IndexAlreadyExists` error**

In `src/error.rs`, add a test at the end of the `mod tests` block:

```rust
#[test]
fn error_index_already_exists_displays() {
    let e = Error::IndexAlreadyExists("my_index".to_string());
    assert_eq!(e.to_string(), "index 'my_index' already exists");
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `cargo test error::tests::error_index_already_exists_displays`
Expected: FAIL — no variant `IndexAlreadyExists`

- [ ] **Step 7: Add `IndexAlreadyExists` variant to `Error`**

In `src/error.rs`, add after the `IndexTypeMismatch` variant:

```rust
/// Custom index with this name already exists.
#[error("index '{0}' already exists")]
IndexAlreadyExists(String),
```

- [ ] **Step 8: Run all tests to verify nothing broke**

Run: `cargo test`
Expected: All tests PASS

Run: `cargo clippy -- -D warnings`
Expected: No warnings

- [ ] **Step 9: Commit**

```bash
git add src/index.rs src/error.rs
git commit -m "feat: add IndexKind::Custom variant and IndexAlreadyExists error"
```

---

### Task 2: Define the `CustomIndex<R>` trait and `CustomIndexAdapter<R, I>`

**Files:**
- Modify: `src/index.rs` (add trait + adapter after existing code, before tests)

- [ ] **Step 1: Write failing test for `CustomIndexAdapter` implementing `IndexMaintainer`**

In `src/index.rs`, add inside `mod tests`:

```rust
/// A minimal custom index that tracks the sum of a numeric field.
#[derive(Clone)]
struct SumIndex {
    total: u64,
}

impl SumIndex {
    fn new() -> Self {
        Self { total: 0 }
    }

    fn total(&self) -> u64 {
        self.total
    }
}

impl CustomIndex<User> for SumIndex {
    fn on_insert(&mut self, _id: u64, record: &User) -> Result<()> {
        self.total += record.age as u64;
        Ok(())
    }

    fn on_update(&mut self, _id: u64, old: &User, new: &User) -> Result<()> {
        self.total -= old.age as u64;
        self.total += new.age as u64;
        Ok(())
    }

    fn on_delete(&mut self, _id: u64, record: &User) {
        self.total -= record.age as u64;
    }
}

#[test]
fn custom_index_adapter_lifecycle() {
    let sum = SumIndex::new();
    let mut adapter = CustomIndexAdapter::new("sum".to_string(), sum);

    let u1 = User { email: "a@x.com".to_string(), age: 30 };
    adapter.on_insert(1, &u1).unwrap();

    // Access inner via as_any downcast
    let inner = adapter
        .as_any()
        .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
        .unwrap()
        .inner();
    assert_eq!(inner.total(), 30);

    let u2 = User { email: "b@x.com".to_string(), age: 20 };
    adapter.on_insert(2, &u2).unwrap();

    let inner = adapter
        .as_any()
        .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
        .unwrap()
        .inner();
    assert_eq!(inner.total(), 50);

    // Update
    let u1_new = User { email: "a@x.com".to_string(), age: 35 };
    adapter.on_update(1, &u1, &u1_new).unwrap();

    let inner = adapter
        .as_any()
        .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
        .unwrap()
        .inner();
    assert_eq!(inner.total(), 55);

    // Delete
    adapter.on_delete(2, &u2);

    let inner = adapter
        .as_any()
        .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
        .unwrap()
        .inner();
    assert_eq!(inner.total(), 35);

    // kind() returns Custom
    assert_eq!(adapter.kind(), IndexKind::Custom);
}

#[test]
fn custom_index_adapter_clone_box_independent() {
    let sum = SumIndex::new();
    let mut adapter = CustomIndexAdapter::new("sum".to_string(), sum);

    let u1 = User { email: "a@x.com".to_string(), age: 30 };
    adapter.on_insert(1, &u1).unwrap();

    let cloned = adapter.clone_box();

    // Mutate original
    let u2 = User { email: "b@x.com".to_string(), age: 20 };
    adapter.on_insert(2, &u2).unwrap();

    // Clone should not see the new insert
    let cloned_inner = cloned
        .as_any()
        .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
        .unwrap()
        .inner();
    assert_eq!(cloned_inner.total(), 30);

    let orig_inner = adapter
        .as_any()
        .downcast_ref::<CustomIndexAdapter<User, SumIndex>>()
        .unwrap()
        .inner();
    assert_eq!(orig_inner.total(), 50);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test index::tests::custom_index_adapter_lifecycle`
Expected: FAIL — `CustomIndex` and `CustomIndexAdapter` not defined

- [ ] **Step 3: Implement `CustomIndex<R>` trait and `CustomIndexAdapter<R, I>`**

In `src/index.rs`, add after the `NonUniqueStorage` impl block (before `#[cfg(test)]`):

```rust
// ---------------------------------------------------------------------------
// CustomIndex — public trait for user-defined index structures
// ---------------------------------------------------------------------------

/// Trait for user-defined custom indexes.
///
/// Implementors have full control over their internal data structure and query
/// API. The `Clone` bound is required for CoW snapshot cloning — use
/// [`BTree<K, V>`](crate::btree::BTree) internally for O(1) clone.
pub trait CustomIndex<R: Record>: Send + Sync + Clone + 'static {
    /// Called when a record is inserted. Return `Err` to veto the mutation.
    fn on_insert(&mut self, id: u64, record: &R) -> Result<()>;

    /// Called when a record is updated. Return `Err` to veto the mutation.
    fn on_update(&mut self, id: u64, old: &R, new: &R) -> Result<()>;

    /// Called when a record is deleted.
    fn on_delete(&mut self, id: u64, record: &R);

    /// Rebuild the entire index from an iterator of `(id, record)` pairs.
    ///
    /// Used for backfilling when the index is defined on a non-empty table,
    /// and for recovery from persistence. The default implementation iterates
    /// and calls [`on_insert`](Self::on_insert) for each entry.
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

// ---------------------------------------------------------------------------
// CustomIndexAdapter — bridges CustomIndex into IndexMaintainer
// ---------------------------------------------------------------------------

pub(crate) struct CustomIndexAdapter<R: Record, I: CustomIndex<R>> {
    inner: I,
    name: String,
    _phantom: std::marker::PhantomData<R>,
}

impl<R: Record, I: CustomIndex<R>> CustomIndexAdapter<R, I> {
    pub fn new(name: String, index: I) -> Self {
        Self {
            inner: index,
            name,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn inner(&self) -> &I {
        &self.inner
    }
}

impl<R: Record, I: CustomIndex<R> + 'static> IndexMaintainer<R> for CustomIndexAdapter<R, I> {
    fn on_insert(&mut self, id: u64, record: &R) -> Result<()> {
        self.inner.on_insert(id, record)
    }

    fn on_update(&mut self, id: u64, old: &R, new: &R) -> Result<()> {
        self.inner.on_update(id, old, new)
    }

    fn on_delete(&mut self, id: u64, record: &R) {
        self.inner.on_delete(id, record)
    }

    fn kind(&self) -> IndexKind {
        IndexKind::Custom
    }

    fn clone_box(&self) -> Box<dyn IndexMaintainer<R>> {
        Box::new(CustomIndexAdapter {
            inner: self.inner.clone(),
            name: self.name.clone(),
            _phantom: std::marker::PhantomData,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test index::tests::custom_index_adapter`
Expected: Both `custom_index_adapter_lifecycle` and `custom_index_adapter_clone_box_independent` PASS

- [ ] **Step 5: Run full test suite and clippy**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All PASS, no warnings

- [ ] **Step 6: Commit**

```bash
git add src/index.rs
git commit -m "feat: add CustomIndex<R> trait and CustomIndexAdapter bridge"
```

---

### Task 3: Add `define_custom_index`, `custom_index`, and `resolve` to `Table<R>`

**Files:**
- Modify: `src/table.rs` (add methods in the index management section, add tests)

- [ ] **Step 1: Write failing test for `define_custom_index` and `custom_index`**

In `src/table.rs`, add a `SumIndex` test helper and tests inside `mod tests`. The `User` struct is already defined in the test module there — check its fields first. Add the `SumIndex` helper after the existing test helpers:

```rust
#[derive(Clone)]
struct SumIndex {
    total: u64,
    field_extractor: Arc<dyn Fn(&User) -> u64 + Send + Sync>,
}

impl SumIndex {
    fn new(extractor: impl Fn(&User) -> u64 + Send + Sync + 'static) -> Self {
        Self { total: 0, field_extractor: Arc::new(extractor) }
    }

    fn total(&self) -> u64 {
        self.total
    }
}

impl CustomIndex<User> for SumIndex {
    fn on_insert(&mut self, _id: u64, record: &User) -> Result<()> {
        self.total += (self.field_extractor)(record);
        Ok(())
    }

    fn on_update(&mut self, _id: u64, old: &User, new: &User) -> Result<()> {
        self.total -= (self.field_extractor)(old);
        self.total += (self.field_extractor)(new);
        Ok(())
    }

    fn on_delete(&mut self, _id: u64, record: &User) {
        self.total -= (self.field_extractor)(record);
    }
}
```

Then add the tests:

```rust
#[test]
fn define_custom_index_and_query() {
    let mut table = Table::<User>::new();
    table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();

    table.insert(User { email: "a@x.com".to_string(), age: 30, name: "Alice".to_string() }).unwrap();
    table.insert(User { email: "b@x.com".to_string(), age: 20, name: "Bob".to_string() }).unwrap();

    let idx = table.custom_index::<SumIndex>("age_sum").unwrap();
    assert_eq!(idx.total(), 50);
}

#[test]
fn define_custom_index_backfills_existing_data() {
    let mut table = Table::<User>::new();
    table.insert(User { email: "a@x.com".to_string(), age: 10, name: "A".to_string() }).unwrap();
    table.insert(User { email: "b@x.com".to_string(), age: 20, name: "B".to_string() }).unwrap();

    // Define index after data exists — should backfill
    table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();

    let idx = table.custom_index::<SumIndex>("age_sum").unwrap();
    assert_eq!(idx.total(), 30);
}

#[test]
fn define_custom_index_rejects_duplicate_name() {
    let mut table = Table::<User>::new();
    table.define_custom_index("idx", SumIndex::new(|u| u.age as u64)).unwrap();
    let res = table.define_custom_index("idx", SumIndex::new(|u| u.age as u64));
    assert!(matches!(res, Err(Error::IndexAlreadyExists(_))));
}

#[test]
fn define_custom_index_rejects_name_collision_with_builtin() {
    let mut table = Table::<User>::new();
    table.define_index("idx", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
    let res = table.define_custom_index("idx", SumIndex::new(|u| u.age as u64));
    assert!(matches!(res, Err(Error::IndexAlreadyExists(_))));
}

#[test]
fn custom_index_not_found() {
    let table = Table::<User>::new();
    let res = table.custom_index::<SumIndex>("nope");
    assert!(matches!(res, Err(Error::IndexNotFound(_))));
}

#[test]
fn custom_index_type_mismatch() {
    let mut table = Table::<User>::new();
    // Define a built-in index, then try to retrieve it as a custom index
    table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone()).unwrap();
    let res = table.custom_index::<SumIndex>("by_email");
    assert!(matches!(res, Err(Error::IndexTypeMismatch(_))));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test table::tests::define_custom_index_and_query`
Expected: FAIL — `define_custom_index` method not found

- [ ] **Step 3: Implement the three methods on `Table<R>`**

In `src/table.rs`, add a `use crate::index::CustomIndex;` import at the top, then add these methods inside the `impl<R: Record> Table<R>` block, after the existing `index_range` method:

```rust
    // -----------------------------------------------------------------------
    // Custom index management
    // -----------------------------------------------------------------------

    /// Define a custom index. If the table already contains data, the index
    /// is backfilled via [`CustomIndex::rebuild`]. Returns an error if any
    /// index (built-in or custom) with the same name already exists.
    pub fn define_custom_index<I: CustomIndex<R>>(
        &mut self,
        name: &str,
        mut index: I,
    ) -> Result<()> {
        if self.indexes.contains_key(name) {
            return Err(Error::IndexAlreadyExists(name.to_string()));
        }
        index.rebuild(self.data.range(..).map(|(&id, r)| (id, r)))?;
        let adapter = CustomIndexAdapter::new(name.to_string(), index);
        self.indexes.insert(name.to_string(), Box::new(adapter));
        Ok(())
    }

    /// Retrieve a reference to a custom index by name, downcast to the
    /// concrete index type. Returns `IndexNotFound` if the name doesn't
    /// exist, or `IndexTypeMismatch` if the type doesn't match.
    pub fn custom_index<I: CustomIndex<R>>(&self, name: &str) -> Result<&I> {
        let idx = self
            .indexes
            .get(name)
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;
        let adapter = idx
            .as_any()
            .downcast_ref::<CustomIndexAdapter<R, I>>()
            .ok_or_else(|| Error::IndexTypeMismatch(name.to_string()))?;
        Ok(adapter.inner())
    }

    /// Resolve a slice of record IDs to `(id, &record)` pairs.
    /// IDs that don't exist in the table are silently skipped.
    pub fn resolve(&self, ids: &[u64]) -> Vec<(u64, &R)> {
        ids.iter()
            .filter_map(|&id| self.get(id).map(|r| (id, r)))
            .collect()
    }
```

Also add the import at the top of `src/table.rs`:

```rust
use crate::index::{CustomIndex, CustomIndexAdapter, IndexKind, IndexMaintainer, ManagedIndex, NonUniqueStorage, UniqueStorage};
```

(Extending the existing import to include `CustomIndex` and `CustomIndexAdapter`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test table::tests::define_custom_index`
Expected: All five `define_custom_index*` tests PASS

Run: `cargo test table::tests::custom_index`
Expected: Both `custom_index_not_found` and `custom_index_type_mismatch` PASS

- [ ] **Step 5: Write failing test for `resolve`**

In `src/table.rs` tests:

```rust
#[test]
fn resolve_returns_matching_records() {
    let mut table = Table::<User>::new();
    let id1 = table.insert(User { email: "a@x.com".to_string(), age: 30, name: "A".to_string() }).unwrap();
    let id2 = table.insert(User { email: "b@x.com".to_string(), age: 20, name: "B".to_string() }).unwrap();

    let results = table.resolve(&[id1, id2, 999]);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, id1);
    assert_eq!(results[1].0, id2);
}

#[test]
fn resolve_empty_ids() {
    let table = Table::<User>::new();
    let results = table.resolve(&[]);
    assert!(results.is_empty());
}
```

- [ ] **Step 6: Run tests to verify they pass** (implementation already added in step 3)

Run: `cargo test table::tests::resolve`
Expected: Both PASS

- [ ] **Step 7: Run full test suite and clippy**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All PASS, no warnings

- [ ] **Step 8: Commit**

```bash
git add src/table.rs
git commit -m "feat: add define_custom_index, custom_index, and resolve to Table"
```

---

### Task 4: Custom indexes participate in mutation lifecycle

This task verifies that custom indexes are correctly updated on `insert`, `update`, `delete`, and batch operations — and that rollback works.

**Files:**
- Modify: `src/table.rs` (add tests only — no implementation changes needed)

- [ ] **Step 1: Write tests for mutation lifecycle**

In `src/table.rs` tests:

```rust
#[test]
fn custom_index_tracks_updates() {
    let mut table = Table::<User>::new();
    table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();

    let id = table.insert(User { email: "a@x.com".to_string(), age: 30, name: "A".to_string() }).unwrap();
    assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 30);

    table.update(id, User { email: "a@x.com".to_string(), age: 40, name: "A".to_string() }).unwrap();
    assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 40);

    table.delete(id).unwrap();
    assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 0);
}

#[test]
fn custom_index_works_with_batch_insert() {
    let mut table = Table::<User>::new();
    table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();

    let records = vec![
        User { email: "a@x.com".to_string(), age: 10, name: "A".to_string() },
        User { email: "b@x.com".to_string(), age: 20, name: "B".to_string() },
        User { email: "c@x.com".to_string(), age: 30, name: "C".to_string() },
    ];
    table.insert_batch(records).unwrap();

    assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 60);
}

#[test]
fn custom_index_works_with_batch_delete() {
    let mut table = Table::<User>::new();
    table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();

    let ids = table.insert_batch(vec![
        User { email: "a@x.com".to_string(), age: 10, name: "A".to_string() },
        User { email: "b@x.com".to_string(), age: 20, name: "B".to_string() },
        User { email: "c@x.com".to_string(), age: 30, name: "C".to_string() },
    ]).unwrap();

    table.delete_batch(&[ids[0], ids[2]]).unwrap();
    assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 20);
}

#[test]
fn custom_index_clone_is_independent() {
    let mut table = Table::<User>::new();
    table.define_custom_index("age_sum", SumIndex::new(|u| u.age as u64)).unwrap();
    table.insert(User { email: "a@x.com".to_string(), age: 30, name: "A".to_string() }).unwrap();

    let clone = table.clone();

    table.insert(User { email: "b@x.com".to_string(), age: 20, name: "B".to_string() }).unwrap();

    // Clone should not see the new insert
    assert_eq!(clone.custom_index::<SumIndex>("age_sum").unwrap().total(), 30);
    assert_eq!(table.custom_index::<SumIndex>("age_sum").unwrap().total(), 50);
}
```

- [ ] **Step 2: Run tests to verify they pass** (mutation dispatch is already handled by existing `IndexMaintainer` loop)

Run: `cargo test table::tests::custom_index_tracks`
Expected: PASS

Run: `cargo test table::tests::custom_index_works_with_batch`
Expected: Both PASS

Run: `cargo test table::tests::custom_index_clone`
Expected: PASS

- [ ] **Step 3: Write test for veto (custom index rejects mutation)**

Add a rejecting custom index to the test module:

```rust
/// A custom index that rejects inserts when total would exceed a limit.
#[derive(Clone)]
struct CappedSumIndex {
    total: u64,
    cap: u64,
}

impl CappedSumIndex {
    fn new(cap: u64) -> Self {
        Self { total: 0, cap }
    }
}

impl CustomIndex<User> for CappedSumIndex {
    fn on_insert(&mut self, _id: u64, record: &User) -> Result<()> {
        let new_total = self.total + record.age as u64;
        if new_total > self.cap {
            return Err(Error::DuplicateKey("cap exceeded".to_string()));
        }
        self.total = new_total;
        Ok(())
    }

    fn on_update(&mut self, _id: u64, old: &User, new: &User) -> Result<()> {
        let new_total = self.total - old.age as u64 + new.age as u64;
        if new_total > self.cap {
            return Err(Error::DuplicateKey("cap exceeded".to_string()));
        }
        self.total = new_total;
        Ok(())
    }

    fn on_delete(&mut self, _id: u64, record: &User) {
        self.total -= record.age as u64;
    }
}

#[test]
fn custom_index_veto_rejects_insert() {
    let mut table = Table::<User>::new();
    table.define_custom_index("capped", CappedSumIndex::new(50)).unwrap();

    table.insert(User { email: "a@x.com".to_string(), age: 30, name: "A".to_string() }).unwrap();
    table.insert(User { email: "b@x.com".to_string(), age: 15, name: "B".to_string() }).unwrap();

    // This would push total to 55, exceeding cap of 50
    let res = table.insert(User { email: "c@x.com".to_string(), age: 10, name: "C".to_string() });
    assert!(res.is_err());

    // Total should still be 45 (rollback)
    assert_eq!(table.custom_index::<CappedSumIndex>("capped").unwrap().total, 45);
    assert_eq!(table.len(), 2);
}

#[test]
fn custom_index_veto_rejects_update() {
    let mut table = Table::<User>::new();
    table.define_custom_index("capped", CappedSumIndex::new(50)).unwrap();

    let id = table.insert(User { email: "a@x.com".to_string(), age: 30, name: "A".to_string() }).unwrap();
    table.insert(User { email: "b@x.com".to_string(), age: 15, name: "B".to_string() }).unwrap();

    // Update age 30 -> 40 would push total to 55
    let res = table.update(id, User { email: "a@x.com".to_string(), age: 40, name: "A".to_string() });
    assert!(res.is_err());

    // Total should still be 45
    assert_eq!(table.custom_index::<CappedSumIndex>("capped").unwrap().total, 45);
}
```

- [ ] **Step 4: Run veto tests**

Run: `cargo test table::tests::custom_index_veto`
Expected: Both PASS

- [ ] **Step 5: Run full test suite and clippy**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All PASS, no warnings

- [ ] **Step 6: Commit**

```bash
git add src/table.rs
git commit -m "test: custom index mutation lifecycle, batches, clone, and veto"
```

---

### Task 5: Re-export `BTree` and `CustomIndex` from `lib.rs`

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Write failing test that imports from crate root**

Create `tests/custom_index_api.rs`:

```rust
//! Integration test verifying the custom index public API.

use ultima_db::{BTree, CustomIndex, Table, Result, Record};

/// A simple ID-set index backed by the public BTree.
#[derive(Clone)]
struct IdSetIndex {
    ids: BTree<u64, ()>,
}

impl IdSetIndex {
    fn new() -> Self {
        Self { ids: BTree::new() }
    }

    fn contains(&self, id: u64) -> bool {
        self.ids.get(&id).is_some()
    }

    fn len(&self) -> usize {
        self.ids.len()
    }
}

impl CustomIndex<String> for IdSetIndex {
    fn on_insert(&mut self, id: u64, _record: &String) -> Result<()> {
        self.ids = self.ids.insert(id, ());
        Ok(())
    }

    fn on_update(&mut self, _id: u64, _old: &String, _new: &String) -> Result<()> {
        Ok(())
    }

    fn on_delete(&mut self, id: u64, _record: &String) {
        if let Ok(new_ids) = self.ids.remove(&id) {
            self.ids = new_ids;
        }
    }
}

#[test]
fn custom_index_public_api_with_btree() {
    let mut table = Table::<String>::new();
    table.define_custom_index("id_set", IdSetIndex::new()).unwrap();

    let id1 = table.insert("hello".to_string()).unwrap();
    let id2 = table.insert("world".to_string()).unwrap();

    let idx = table.custom_index::<IdSetIndex>("id_set").unwrap();
    assert!(idx.contains(id1));
    assert!(idx.contains(id2));
    assert!(!idx.contains(999));
    assert_eq!(idx.len(), 2);

    // Test resolve
    let results = table.resolve(&[id1, id2, 999]);
    assert_eq!(results.len(), 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test custom_index_api`
Expected: FAIL — `BTree` and `CustomIndex` not found in `ultima_db`

- [ ] **Step 3: Add re-exports to `lib.rs`**

In `src/lib.rs`, add to the existing `pub use` block:

```rust
pub use btree::BTree;
pub use index::CustomIndex;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test custom_index_api`
Expected: PASS

- [ ] **Step 5: Run full test suite and clippy**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All PASS, no warnings

- [ ] **Step 6: Commit**

```bash
git add src/lib.rs tests/custom_index_api.rs
git commit -m "feat: re-export BTree and CustomIndex from crate root"
```

---

### Task 6: Integration test — custom index through transactions

This verifies that custom indexes work correctly through `WriteTx`/`ReadTx` and across snapshots.

**Files:**
- Modify: `tests/custom_index_api.rs`

- [ ] **Step 1: Write integration test for custom index through WriteTx**

Append to `tests/custom_index_api.rs`:

```rust
use ultima_db::Store;

#[test]
fn custom_index_through_write_tx() {
    let store = Store::default();

    // First transaction: create table with custom index and insert data
    let mut wtx = store.begin_write(None).unwrap();
    {
        let table = wtx.open_table::<String>("docs");
        table.define_custom_index("id_set", IdSetIndex::new()).unwrap();
        table.insert("hello".to_string()).unwrap();
        table.insert("world".to_string()).unwrap();
    }
    wtx.commit(&store).unwrap();

    // Read transaction: verify custom index is accessible
    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<String>("docs").unwrap();
    let idx = table.custom_index::<IdSetIndex>("id_set").unwrap();
    assert_eq!(idx.len(), 2);
    assert!(idx.contains(1));
    assert!(idx.contains(2));
}

#[test]
fn custom_index_snapshot_isolation() {
    let store = Store::default();

    // Transaction 1: create table with custom index
    let mut wtx = store.begin_write(None).unwrap();
    {
        let table = wtx.open_table::<String>("docs");
        table.define_custom_index("id_set", IdSetIndex::new()).unwrap();
        table.insert("v1".to_string()).unwrap();
    }
    wtx.commit(&store).unwrap();

    // Take a read snapshot at version 1
    let rtx_v1 = store.begin_read(None).unwrap();

    // Transaction 2: insert more data
    let mut wtx2 = store.begin_write(None).unwrap();
    {
        let table = wtx2.open_table::<String>("docs");
        table.insert("v2".to_string()).unwrap();
    }
    wtx2.commit(&store).unwrap();

    // Read snapshot at v1 should still see only 1 record
    let table_v1 = rtx_v1.open_table::<String>("docs").unwrap();
    let idx_v1 = table_v1.custom_index::<IdSetIndex>("id_set").unwrap();
    assert_eq!(idx_v1.len(), 1);

    // Latest read should see 2 records
    let rtx_v2 = store.begin_read(None).unwrap();
    let table_v2 = rtx_v2.open_table::<String>("docs").unwrap();
    let idx_v2 = table_v2.custom_index::<IdSetIndex>("id_set").unwrap();
    assert_eq!(idx_v2.len(), 2);
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test --test custom_index_api`
Expected: All PASS

- [ ] **Step 3: Run full test suite and clippy**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All PASS, no warnings

- [ ] **Step 4: Commit**

```bash
git add tests/custom_index_api.rs
git commit -m "test: custom index integration through transactions and snapshots"
```

---

### Task 7: Documentation

**Files:**
- Modify: `docs/architecture.md` (add custom index section)
- Create: `docs/tasks/task_custom_indexes.md`

- [ ] **Step 1: Add custom index section to architecture.md**

Add a new section after the "Table" section in `docs/architecture.md`:

```markdown
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
```

- [ ] **Step 2: Create task doc**

Create `docs/tasks/task_custom_indexes.md` following the project convention — document the feature, architectural decisions, and implementation details. Reference the spec at `docs/superpowers/specs/2026-04-09-custom-indexes-design.md`.

- [ ] **Step 3: Commit**

```bash
git add docs/architecture.md docs/tasks/task_custom_indexes.md
git commit -m "docs: add custom index architecture and task documentation"
```
