# API Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve UltimaDB's public API based on comparison with RocksDB, redb, and fjall — 11 changes covering ergonomics, convenience methods, and discoverability.

**Architecture:** Interior mutability for Store (Arc<Mutex<StoreInner>>), TableOpener trait for dual open_table styles, DoubleEndedIterator for BTreeRange, new error variants, convenience methods on Table, table management on WriteTx, and a Readable trait for ReadTx.

**Tech Stack:** Rust, std::sync::{Arc, Mutex}, std::marker::PhantomData

---

## File Structure

| File | Responsibility | Tasks |
|------|---------------|-------|
| `src/error.rs` | Error types | 2 |
| `src/store.rs` | Store, StoreInner, ReadTx, WriteTx, Readable trait | 1, 3, 6, 7, 8 |
| `src/table.rs` | Table convenience methods, TableDef, TableOpener | 3, 4, 5 |
| `src/btree.rs` | DoubleEndedIterator for BTreeRange | 4 |
| `src/lib.rs` | Re-exports | 3, 8 |
| `src/transaction.rs` | Re-exports | 8 |
| `tests/store_integration.rs` | Integration tests | 1, 2, 3, 4, 5, 6, 7, 8 |
| `benches/ycsb_bench.rs` | Update commit calls | 1 |
| `benches/store_bench.rs` | No changes needed (doesn't use commit) | — |
| `examples/basic_usage.rs` | Update commit calls | 1 |
| `examples/multi_store.rs` | Update commit calls | 1 |

---

### Task 1: Store interior mutability (spec item 1)

**Files:**
- Modify: `src/store.rs`
- Modify: `tests/store_integration.rs`
- Modify: `benches/ycsb_bench.rs`
- Modify: `examples/basic_usage.rs`
- Modify: `examples/multi_store.rs`

- [ ] **Step 1: Restructure Store with StoreInner**

In `src/store.rs`, add `use std::sync::Mutex;` to the imports. Extract the Store fields into `StoreInner` and wrap in `Arc<Mutex<>>`:

```rust
pub(crate) struct StoreInner {
    pub(crate) snapshots: HashMap<u64, Arc<Snapshot>>,
    pub(crate) latest_version: u64,
    next_version: u64,
    pub(crate) config: StoreConfig,
}

#[derive(Clone)]
pub struct Store {
    pub(crate) inner: Arc<Mutex<StoreInner>>,
}
```

Update `Store::new` to wrap in `Arc::new(Mutex::new(StoreInner { ... }))`.

Update `Store::latest_version` to lock and read:

```rust
pub fn latest_version(&self) -> u64 {
    self.inner.lock().unwrap().latest_version
}
```

- [ ] **Step 2: Update begin_read to take &self**

Change `begin_read` to `pub fn begin_read(&self, version: Option<u64>) -> Result<ReadTx>`. Lock `self.inner`, read from the locked guard:

```rust
pub fn begin_read(&self, version: Option<u64>) -> Result<ReadTx> {
    let inner = self.inner.lock().unwrap();
    let v = version.unwrap_or(inner.latest_version);
    let snapshot = inner
        .snapshots
        .get(&v)
        .ok_or(Error::KeyNotFound)?
        .clone();
    Ok(ReadTx { snapshot })
}
```

- [ ] **Step 3: Update begin_write to take &self, WriteTx holds Arc<Mutex<StoreInner>>**

Change `begin_write` to `pub fn begin_write(&self, version: Option<u64>) -> Result<WriteTx>`:

```rust
pub fn begin_write(&self, version: Option<u64>) -> Result<WriteTx> {
    let mut inner = self.inner.lock().unwrap();
    let commit_version = match version {
        None => inner.next_version,
        Some(v) if v > inner.latest_version => v,
        Some(_) => return Err(Error::WriteConflict),
    };
    if commit_version >= inner.next_version {
        inner.next_version = commit_version + 1;
    }
    let base = inner.snapshots[&inner.latest_version].clone();
    Ok(WriteTx {
        base,
        dirty: HashMap::new(),
        version: commit_version,
        store_inner: Arc::clone(&self.inner),
    })
}
```

Add the `store_inner` field to `WriteTx`:

```rust
pub struct WriteTx {
    base: Arc<Snapshot>,
    dirty: HashMap<String, Box<dyn Any>>,
    version: u64,
    store_inner: Arc<Mutex<StoreInner>>,
}
```

- [ ] **Step 4: Update WriteTx::commit to take only self (no store argument)**

Change the commit method:

```rust
pub fn commit(self) -> Result<u64> {
    let mut new_tables: HashMap<String, Arc<dyn Any>> = self
        .base
        .tables
        .iter()
        .map(|(k, v)| (k.clone(), Arc::clone(v)))
        .collect();

    for (name, boxed) in self.dirty {
        new_tables.insert(name, Arc::from(boxed));
    }

    let snapshot = Arc::new(Snapshot { version: self.version, tables: new_tables });

    let mut inner = self.store_inner.lock().unwrap();
    let v = snapshot.version;
    inner.snapshots.insert(v, snapshot);
    if v > inner.latest_version {
        inner.latest_version = v;
    }
    if inner.config.auto_snapshot_gc {
        gc_inner(&mut inner);
    }
    Ok(v)
}
```

Remove the old `commit_snapshot` method from Store.

- [ ] **Step 5: Update gc() to use StoreInner**

Extract GC logic into a free function `gc_inner(inner: &mut StoreInner)`:

```rust
fn gc_inner(inner: &mut StoreInner) {
    let mut versions: Vec<u64> = inner.snapshots.keys().copied().collect();
    versions.sort_unstable();
    let retain_count = inner.config.num_snapshots_retained.max(1);
    let cutoff_idx = versions.len().saturating_sub(retain_count);
    let protected: std::collections::HashSet<u64> =
        versions[cutoff_idx..].iter().copied().collect();
    inner.snapshots.retain(|&v, snapshot| {
        protected.contains(&v) || Arc::strong_count(snapshot) > 1
    });
}
```

Update `Store::gc()`:

```rust
pub fn gc(&self) {
    let mut inner = self.inner.lock().unwrap();
    gc_inner(&mut inner);
}
```

- [ ] **Step 6: Update Default impl**

```rust
impl Default for Store {
    fn default() -> Self {
        Self::new(StoreConfig::default())
    }
}
```

The `Store::new` constructor now creates `Arc::new(Mutex::new(StoreInner { ... }))`.

- [ ] **Step 7: Update all unit tests in store.rs**

Replace all `let mut store` with `let store` (no longer needs mut). Replace all `wtx.commit(&mut store)` with `wtx.commit()`. Replace direct field access `store.snapshots.len()` in tests with a helper or adjust test approach.

For tests that access `store.snapshots.len()`, add a test helper:

```rust
#[cfg(test)]
impl Store {
    fn snapshot_count(&self) -> usize {
        self.inner.lock().unwrap().snapshots.len()
    }

    fn has_snapshot(&self, version: u64) -> bool {
        self.inner.lock().unwrap().snapshots.contains_key(&version)
    }
}
```

Then replace `store.snapshots.len()` → `store.snapshot_count()` and `store.snapshots.contains_key(&v)` → `store.has_snapshot(v)` in tests.

- [ ] **Step 8: Update integration tests**

In `tests/store_integration.rs`:
- Replace all `let mut store` with `let store` where the `mut` was only needed for `begin_write`/`commit`.
- Replace all `wtx.commit(&mut store).unwrap()` with `wtx.commit().unwrap()`.
- Replace all `store.begin_write(None)` — no `&mut` needed anymore.
- The `Error::KeyNotFound` match in `two_tables_independent_across_versions` for a missing table stays the same for now (item 2 changes it later).

- [ ] **Step 9: Update examples**

In `examples/basic_usage.rs`:
- `let mut store` → `let store`
- `wtx.commit(&mut store)` → `wtx.commit()`

In `examples/multi_store.rs`:
- Same changes.

- [ ] **Step 10: Update YCSB benchmark**

In `benches/ycsb_bench.rs`:
- `preload_store`: `let mut store` → `let store`, `wtx.commit(&mut store)` → `wtx.commit()`
- `execute_ops`: `store: &mut Store` → `store: &Store`, `wtx.commit(store)` → `wtx.commit()`
- All `bench_workload_*` functions: `let mut store` → `let store`, `&mut store` → `&store`

- [ ] **Step 11: Run tests and verify**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All tests pass, no warnings.

- [ ] **Step 12: Commit**

```bash
git add src/store.rs tests/store_integration.rs benches/ycsb_bench.rs examples/basic_usage.rs examples/multi_store.rs
git commit -m "feat: Store interior mutability — commit(self) without &mut Store"
```

---

### Task 2: Error::VersionNotFound (spec item 4)

**Files:**
- Modify: `src/error.rs`
- Modify: `src/store.rs`
- Modify: `tests/store_integration.rs`

- [ ] **Step 1: Write failing test**

In `tests/store_integration.rs`, add:

```rust
#[test]
fn begin_read_nonexistent_version_returns_version_not_found() {
    let store = Store::default();
    let err = store.begin_read(Some(99)).unwrap_err();
    assert!(matches!(err, Error::VersionNotFound(99)));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test begin_read_nonexistent_version_returns_version_not_found`
Expected: FAIL — `Error::VersionNotFound` doesn't exist yet.

- [ ] **Step 3: Add VersionNotFound variant to Error**

In `src/error.rs`, add the variant:

```rust
/// Requested snapshot version does not exist.
#[error("snapshot version {0} not found")]
VersionNotFound(u64),
```

- [ ] **Step 4: Update begin_read to use VersionNotFound**

In `src/store.rs`, in `begin_read`, change:

```rust
.ok_or(Error::KeyNotFound)?
```

to:

```rust
.ok_or(Error::VersionNotFound(v))?
```

- [ ] **Step 5: Update existing tests that match on KeyNotFound for version lookups**

In `src/store.rs` unit test `begin_read_nonexistent_version_errors`, change:

```rust
assert!(matches!(store.begin_read(Some(99)), Err(Error::KeyNotFound)));
```

to:

```rust
assert!(matches!(store.begin_read(Some(99)), Err(Error::VersionNotFound(99))));
```

In `tests/store_integration.rs`, update `read_at_version_zero_sees_empty_store` — this test reads version 0 which exists, so no change needed. But check `two_tables_independent_across_versions` — the `Err(Error::KeyNotFound)` on line 156 is for a missing *table*, not a missing version, so no change needed.

- [ ] **Step 6: Add error display test**

In `src/error.rs` tests, add:

```rust
#[test]
fn error_version_not_found_displays() {
    let e = Error::VersionNotFound(42);
    assert_eq!(e.to_string(), "snapshot version 42 not found");
}
```

- [ ] **Step 7: Run tests**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All pass.

- [ ] **Step 8: Commit**

```bash
git add src/error.rs src/store.rs tests/store_integration.rs
git commit -m "feat: add Error::VersionNotFound for clearer version lookup errors"
```

---

### Task 3: TableDef and TableOpener trait (spec item 2)

**Files:**
- Modify: `src/table.rs`
- Modify: `src/store.rs`
- Modify: `src/lib.rs`
- Modify: `tests/store_integration.rs`

- [ ] **Step 1: Write failing test using TableDef**

In `tests/store_integration.rs`, add:

```rust
use ultima_db::TableDef;

const NOTES: TableDef<String> = TableDef::new("notes");

#[test]
fn table_def_const_open_table() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table(NOTES).unwrap();
    let id = table.insert("hello".to_string()).unwrap();
    wtx.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table(NOTES).unwrap();
    assert_eq!(table.get(id), Some(&"hello".to_string()));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test table_def_const_open_table`
Expected: FAIL — `TableDef` doesn't exist.

- [ ] **Step 3: Add TableDef and TableOpener to table.rs**

At the top of `src/table.rs`, add:

```rust
use std::marker::PhantomData;
```

Then add before the `Table` struct:

```rust
/// A compile-time table definition binding a name to a record type.
///
/// Use with [`ReadTx::open_table`] or [`WriteTx::open_table`] to avoid
/// specifying the type at every call site.
pub struct TableDef<R: 'static> {
    name: &'static str,
    _phantom: PhantomData<R>,
}

impl<R: 'static> TableDef<R> {
    /// Create a new table definition.
    pub const fn new(name: &'static str) -> Self {
        Self { name, _phantom: PhantomData }
    }

    /// The table name.
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

/// Trait for types that can identify a table and its record type.
///
/// Implemented by `&str` (requires turbofish `open_table::<R>("name")`)
/// and `TableDef<R>` (type inferred from the definition).
pub trait TableOpener<R> {
    /// The table name.
    fn table_name(&self) -> &str;
}

impl<R> TableOpener<R> for &str {
    fn table_name(&self) -> &str {
        self
    }
}

impl<R: 'static> TableOpener<R> for TableDef<R> {
    fn table_name(&self) -> &str {
        self.name
    }
}
```

- [ ] **Step 4: Update ReadTx::open_table to accept TableOpener**

In `src/store.rs`, change:

```rust
pub fn open_table<R: 'static>(&self, name: &str) -> Result<&Table<R>> {
    self.snapshot
        .tables
        .get(name)
        .ok_or(Error::KeyNotFound)?
        .downcast_ref::<Table<R>>()
        .ok_or_else(|| Error::TypeMismatch(name.to_string()))
}
```

to:

```rust
pub fn open_table<R: 'static>(&self, opener: impl TableOpener<R>) -> Result<&Table<R>> {
    let name = opener.table_name();
    self.snapshot
        .tables
        .get(name)
        .ok_or(Error::KeyNotFound)?
        .downcast_ref::<Table<R>>()
        .ok_or_else(|| Error::TypeMismatch(name.to_string()))
}
```

Add `use crate::table::TableOpener;` to the imports in `src/store.rs`.

- [ ] **Step 5: Update WriteTx::open_table to accept TableOpener**

Change:

```rust
pub fn open_table<R: Send + Sync + 'static>(&mut self, name: &str) -> Result<&mut Table<R>> {
```

to:

```rust
pub fn open_table<R: Send + Sync + 'static>(&mut self, opener: impl TableOpener<R>) -> Result<&mut Table<R>> {
    let name = opener.table_name();
```

The rest of the method body uses `name` as before (it already binds a local `name` variable — adjust to use the extracted `name`).

- [ ] **Step 6: Re-export TableDef and TableOpener from lib.rs**

In `src/lib.rs`, add:

```rust
pub use table::{TableDef, TableOpener};
```

- [ ] **Step 7: Run tests**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All pass. Existing tests using `open_table::<R>("name")` still work because `&str` implements `TableOpener<R>`.

- [ ] **Step 8: Commit**

```bash
git add src/table.rs src/store.rs src/lib.rs tests/store_integration.rs
git commit -m "feat: add TableDef<R> const pattern for compile-time table definitions"
```

---

### Task 4: Reverse iteration (spec item 3)

**Files:**
- Modify: `src/btree.rs`

- [ ] **Step 1: Write failing test for reverse iteration**

In `src/btree.rs` tests, add:

```rust
#[test]
fn range_full_reverse_yields_all_keys_descending() {
    let t = insert_range(1, 10);
    let results: Vec<u64> = t.range(..).rev().map(|(k, _)| *k).collect();
    assert_eq!(results, vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
}

#[test]
fn range_bounded_reverse() {
    let t = insert_range(1, 10);
    let results: Vec<u64> = t.range(3u64..=7).rev().map(|(k, _)| *k).collect();
    assert_eq!(results, vec![7, 6, 5, 4, 3]);
}

#[test]
fn range_reverse_empty_tree() {
    let t: BTree<u64, u64> = BTree::new();
    let results: Vec<u64> = t.range(..).rev().map(|(k, _)| *k).collect();
    assert!(results.is_empty());
}

#[test]
fn range_reverse_single_element() {
    let t = BTree::new().insert(5u64, 50u64);
    let results: Vec<u64> = t.range(..).rev().map(|(k, _)| *k).collect();
    assert_eq!(results, vec![5]);
}

#[test]
fn range_reverse_across_split_boundary() {
    let t = insert_range(1, 200);
    let results: Vec<u64> = t.range(50u64..=150).rev().map(|(k, _)| *k).collect();
    let expected: Vec<u64> = (50..=150).rev().collect();
    assert_eq!(results, expected);
}

#[test]
fn range_mixed_forward_and_reverse() {
    let t = insert_range(1, 10);
    let mut iter = t.range(3u64..=8);
    assert_eq!(iter.next().map(|(k, _)| *k), Some(3));
    assert_eq!(iter.next_back().map(|(k, _)| *k), Some(8));
    assert_eq!(iter.next().map(|(k, _)| *k), Some(4));
    assert_eq!(iter.next_back().map(|(k, _)| *k), Some(7));
    assert_eq!(iter.next().map(|(k, _)| *k), Some(5));
    assert_eq!(iter.next_back().map(|(k, _)| *k), Some(6));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next_back(), None);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test range_full_reverse`
Expected: FAIL — `DoubleEndedIterator` not implemented.

- [ ] **Step 3: Add backward stack and start bound to BTreeRange**

Update the `BTreeRange` struct to include a backward stack and start bound:

```rust
pub struct BTreeRange<'a, K, V> {
    stack: Vec<(&'a BTreeNode<K, V>, usize)>,
    back_stack: Vec<(&'a BTreeNode<K, V>, usize)>,
    start: Bound<K>,
    end: Bound<K>,
    done: bool,
}
```

Update `BTree::range` to initialize both stacks:

```rust
pub fn range<'a>(&'a self, range: impl RangeBounds<K> + 'a) -> BTreeRange<'a, K, V> {
    let start = range.start_bound().cloned();
    let end = range.end_bound().cloned();
    let mut iter = BTreeRange {
        stack: vec![],
        back_stack: vec![],
        start: start.clone(),
        end: end.clone(),
        done: false,
    };
    iter.descend_left_from(&self.root, &start);
    iter.descend_right_from(&self.root, &end);
    iter
}
```

- [ ] **Step 4: Add backward traversal methods to BTreeRange**

```rust
impl<'a, K: Ord + Clone, V> BTreeRange<'a, K, V> {
    /// Push stack frames for the rightmost path that is <= `end`.
    fn descend_right_from(&mut self, node: &'a Arc<BTreeNode<K, V>>, end: &Bound<K>) {
        let n = node.as_ref();
        let entry_end = match end {
            Bound::Unbounded => n.entries.len(),
            Bound::Included(k) => {
                let pos = n.entries.partition_point(|(ek, _)| ek <= k);
                pos
            }
            Bound::Excluded(k) => {
                let pos = n.entries.partition_point(|(ek, _)| ek < k);
                pos
            }
        };
        // entry_end is one past the last valid index for backward iteration.
        // We store it as-is; next_back will decrement before reading.
        self.back_stack.push((n, entry_end));
        if !n.children.is_empty() && entry_end < n.children.len() {
            self.descend_right_from(&n.children[entry_end], end);
        }
    }

    /// Push stack frames for the rightmost leaf of `node` (no range restriction).
    fn descend_rightmost(&mut self, node: &'a Arc<BTreeNode<K, V>>) {
        let n = node.as_ref();
        self.back_stack.push((n, n.entries.len()));
        if !n.children.is_empty() {
            self.descend_rightmost(n.children.last().unwrap());
        }
    }

    fn in_start_bound(&self, key: &K) -> bool {
        match &self.start {
            Bound::Unbounded => true,
            Bound::Included(k) => key >= k,
            Bound::Excluded(k) => key > k,
        }
    }
}
```

Also add `in_start_bound` is already referenced; the existing `in_end_bound` stays.

- [ ] **Step 5: Implement DoubleEndedIterator**

```rust
impl<'a, K: Ord + Clone, V> DoubleEndedIterator for BTreeRange<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            let stack_len = self.back_stack.len();
            if stack_len == 0 {
                self.done = true;
                return None;
            }

            let (node, entry_idx) = self.back_stack[stack_len - 1];

            if entry_idx == 0 {
                self.back_stack.pop();
                continue;
            }

            let actual_idx = entry_idx - 1;
            let key = &node.entries[actual_idx].0;
            let val = &*node.entries[actual_idx].1;

            if !self.in_start_bound(key) {
                self.done = true;
                return None;
            }

            // Check overlap with forward iterator: if forward and backward
            // have met or crossed, we're done.
            if !self.in_end_bound(key) {
                self.done = true;
                return None;
            }

            // Decrement the back stack entry index.
            self.back_stack[stack_len - 1].1 = actual_idx;

            // For internal nodes, before yielding entries[actual_idx], descend
            // into the rightmost path of children[actual_idx].
            if !node.children.is_empty() && actual_idx < node.children.len() {
                self.descend_rightmost(&node.children[actual_idx]);
            }

            return Some((key, val));
        }
    }
}
```

- [ ] **Step 6: Prevent forward/backward overlap**

To prevent double-yielding when mixing `next()` and `next_back()`, track yielded items. The simplest correct approach: track last-yielded keys from each direction and check in both `next()` and `next_back()`.

Update the `done` flag check. In `next()`, after extracting `key`, add:

```rust
// Check if we've met the backward iterator
if !self.in_start_bound(key) {
    self.stack.clear();
    self.done = true;
    return None;
}
```

Actually, the simplest approach: track `last_forward_key` and `last_backward_key` as `Option<&'a K>`. In `next()`, if `last_backward_key.is_some()` and `key >= last_backward_key`, return None. Vice versa in `next_back()`.

Alternatively, since keys are unique and ordered, just store `Option<&'a K>` for last yielded from each end:

```rust
pub struct BTreeRange<'a, K, V> {
    stack: Vec<(&'a BTreeNode<K, V>, usize)>,
    back_stack: Vec<(&'a BTreeNode<K, V>, usize)>,
    start: Bound<K>,
    end: Bound<K>,
    done: bool,
    last_forward: Option<&'a K>,
    last_backward: Option<&'a K>,
}
```

In `next()`, before returning `Some((key, val))`:

```rust
if let Some(bk) = self.last_backward {
    if key >= bk {
        self.done = true;
        return None;
    }
}
self.last_forward = Some(key);
```

In `next_back()`, before returning `Some((key, val))`:

```rust
if let Some(fk) = self.last_forward {
    if key <= fk {
        self.done = true;
        return None;
    }
}
self.last_backward = Some(key);
```

Initialize both as `None` in `BTree::range`.

- [ ] **Step 7: Update existing Iterator::next to include done check**

At the top of the existing `next()` method, add:

```rust
if self.done {
    return None;
}
```

- [ ] **Step 8: Run tests**

Run: `cargo test btree::tests && cargo clippy -- -D warnings`
Expected: All pass including the new reverse tests.

- [ ] **Step 9: Commit**

```bash
git add src/btree.rs
git commit -m "feat: implement DoubleEndedIterator for BTreeRange (reverse iteration)"
```

---

### Task 5: Table convenience methods (spec items 5, 6, 7, 11)

**Files:**
- Modify: `src/table.rs`
- Modify: `tests/store_integration.rs`

- [ ] **Step 1: Write failing tests**

In `src/table.rs` tests, add:

```rust
#[test]
fn delete_returns_old_record() {
    let mut table: Table<String> = Table::new();
    let id = table.insert("hello".to_string()).unwrap();
    let old = table.delete(id).unwrap();
    assert_eq!(*old, "hello".to_string());
}

#[test]
fn contains_true_for_existing_id() {
    let mut table: Table<String> = Table::new();
    let id = table.insert("x".to_string()).unwrap();
    assert!(table.contains(id));
}

#[test]
fn contains_false_for_absent_id() {
    let table: Table<String> = Table::new();
    assert!(!table.contains(99));
}

#[test]
fn first_returns_min_id_record() {
    let mut table: Table<String> = Table::new();
    table.insert("a".to_string()).unwrap();
    table.insert("b".to_string()).unwrap();
    let (id, val) = table.first().unwrap();
    assert_eq!(id, 1);
    assert_eq!(val, &"a".to_string());
}

#[test]
fn first_on_empty_returns_none() {
    let table: Table<String> = Table::new();
    assert!(table.first().is_none());
}

#[test]
fn last_returns_max_id_record() {
    let mut table: Table<String> = Table::new();
    table.insert("a".to_string()).unwrap();
    table.insert("b".to_string()).unwrap();
    let (id, val) = table.last().unwrap();
    assert_eq!(id, 2);
    assert_eq!(val, &"b".to_string());
}

#[test]
fn last_on_empty_returns_none() {
    let table: Table<String> = Table::new();
    assert!(table.last().is_none());
}

#[test]
fn iter_yields_all_in_order() {
    let mut table: Table<&str> = Table::new();
    table.insert("a").unwrap();
    table.insert("b").unwrap();
    table.insert("c").unwrap();
    let results: Vec<_> = table.iter().collect();
    assert_eq!(results, vec![(1, &"a"), (2, &"b"), (3, &"c")]);
}

#[test]
fn get_many_returns_matching_records() {
    let mut table: Table<String> = Table::new();
    table.insert("a".to_string()).unwrap();
    table.insert("b".to_string()).unwrap();
    table.insert("c".to_string()).unwrap();
    let results = table.get_many(&[1, 3, 99]);
    assert_eq!(results[0], Some(&"a".to_string()));
    assert_eq!(results[1], Some(&"c".to_string()));
    assert_eq!(results[2], None);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test table::tests::delete_returns_old_record`
Expected: FAIL — `delete` returns `Result<()>` not `Result<Arc<R>>`.

- [ ] **Step 3: Change delete() return type to Result<Arc<R>>**

In `src/table.rs`, change the `delete` method:

```rust
/// Delete a record by its ID. Returns the deleted record, or an error if
/// the ID does not exist.
pub fn delete(&mut self, id: u64) -> Result<Arc<R>> {
    let old = self.data.get_arc(&id).ok_or(Error::KeyNotFound)?;
    for idx in self.indexes.values_mut() {
        idx.on_delete(id, &old);
    }
    self.data = self.data.remove(&id)?;
    Ok(old)
}
```

- [ ] **Step 4: Add contains, first, last, iter, get_many**

In `src/table.rs`, add these methods in the `impl<R: Send + Sync + 'static> Table<R>` block, after `is_empty`:

```rust
/// Returns true if the table contains a record with the given ID.
pub fn contains(&self, id: u64) -> bool {
    self.data.get(&id).is_some()
}

/// Returns the first (lowest ID) record, or `None` if empty.
pub fn first(&self) -> Option<(u64, &R)> {
    self.data.range(..).next().map(|(&k, v)| (k, v))
}

/// Returns the last (highest ID) record, or `None` if empty.
pub fn last(&self) -> Option<(u64, &R)> {
    self.data.range(..).next_back().map(|(&k, v)| (k, v))
}

/// Iterate over all records in ID order.
pub fn iter(&self) -> impl DoubleEndedIterator<Item = (u64, &R)> {
    self.range(..)
}

/// Look up multiple records by ID. Returns a `Vec` with one `Option<&R>`
/// per input ID, in the same order.
pub fn get_many(&self, ids: &[u64]) -> Vec<Option<&R>> {
    ids.iter().map(|&id| self.data.get(&id)).collect()
}
```

- [ ] **Step 5: Update existing delete tests that don't use the return value**

The existing tests `delete_removes_record` and `delete_on_absent_id_returns_key_not_found` in `src/table.rs` use `table.delete(id).unwrap();` — these still compile because the `Arc<R>` result is just unused. No changes needed.

In `tests/store_integration.rs`, `table.delete(id).unwrap();` calls also still compile.

- [ ] **Step 6: Add integration test for delete return value**

In `tests/store_integration.rs`, add:

```rust
#[test]
fn delete_returns_removed_record() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<String>("t").unwrap();
    let id = table.insert("hello".to_string()).unwrap();
    let old = table.delete(id).unwrap();
    assert_eq!(*old, "hello".to_string());
    wtx.commit().unwrap();
}
```

- [ ] **Step 7: Run tests**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All pass.

- [ ] **Step 8: Commit**

```bash
git add src/table.rs tests/store_integration.rs
git commit -m "feat: Table convenience methods — delete returns old record, contains, first, last, iter, get_many"
```

---

### Task 6: delete_table on WriteTx (spec item 9)

**Files:**
- Modify: `src/store.rs`
- Modify: `tests/store_integration.rs`

- [ ] **Step 1: Write failing test**

In `tests/store_integration.rs`, add:

```rust
#[test]
fn delete_table_removes_table_from_snapshot() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string()).unwrap();
        wtx.open_table::<String>("logs").unwrap().insert("event".to_string()).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        assert!(wtx.delete_table("users"));
        assert!(!wtx.delete_table("nonexistent"));
        wtx.commit().unwrap();
    }

    let rtx = store.begin_read(None).unwrap();
    assert!(matches!(rtx.open_table::<String>("users"), Err(Error::KeyNotFound)));
    assert_eq!(rtx.open_table::<String>("logs").unwrap().get(1), Some(&"event".to_string()));
}

#[test]
fn delete_table_then_recreate_in_same_tx() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("old".to_string()).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.delete_table("t");
        let table = wtx.open_table::<String>("t").unwrap();
        assert!(table.is_empty()); // fresh table, old data gone
        table.insert("new".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<String>("t").unwrap();
    assert_eq!(table.len(), 1);
    assert_eq!(table.get(1), Some(&"new".to_string()));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test delete_table_removes`
Expected: FAIL — `delete_table` doesn't exist.

- [ ] **Step 3: Add deleted_tables field to WriteTx**

In `src/store.rs`, add `use std::collections::HashSet;` to imports. Add the field:

```rust
pub struct WriteTx {
    base: Arc<Snapshot>,
    dirty: HashMap<String, Box<dyn Any>>,
    version: u64,
    store_inner: Arc<Mutex<StoreInner>>,
    deleted_tables: HashSet<String>,
}
```

Initialize it as `HashSet::new()` in `begin_write`.

- [ ] **Step 4: Add delete_table method to WriteTx**

```rust
/// Delete a table. Returns `true` if the table existed.
///
/// After deletion, `open_table` with the same name creates a fresh empty table.
pub fn delete_table(&mut self, name: &str) -> bool {
    let existed_in_dirty = self.dirty.remove(name).is_some();
    let existed_in_base = self.base.tables.contains_key(name);
    if existed_in_base {
        self.deleted_tables.insert(name.to_string());
    }
    existed_in_dirty || existed_in_base
}
```

- [ ] **Step 5: Update open_table to respect deleted_tables**

In `WriteTx::open_table`, after the `if !self.dirty.contains_key(name)` check, when looking up the base table, skip it if the table was deleted:

```rust
pub fn open_table<R: Send + Sync + 'static>(&mut self, opener: impl TableOpener<R>) -> Result<&mut Table<R>> {
    let name = opener.table_name();
    if !self.dirty.contains_key(name) {
        let table: Table<R> = if self.deleted_tables.contains(name) {
            Table::new()
        } else {
            match self.base.tables.get(name) {
                Some(arc_any) => arc_any
                    .downcast_ref::<Table<R>>()
                    .ok_or_else(|| Error::TypeMismatch(name.to_string()))?
                    .clone(),
                None => Table::new(),
            }
        };
        // If this table was previously deleted, opening it means we're recreating.
        self.deleted_tables.remove(name);
        self.dirty.insert(name.to_string(), Box::new(table));
    }
    self.dirty
        .get_mut(name)
        .unwrap()
        .downcast_mut::<Table<R>>()
        .ok_or_else(|| Error::TypeMismatch(name.to_string()))
}
```

- [ ] **Step 6: Update commit to exclude deleted tables**

In `WriteTx::commit`, after building `new_tables`, remove deleted tables:

```rust
pub fn commit(self) -> Result<u64> {
    let mut new_tables: HashMap<String, Arc<dyn Any>> = self
        .base
        .tables
        .iter()
        .map(|(k, v)| (k.clone(), Arc::clone(v)))
        .collect();

    for (name, boxed) in self.dirty {
        new_tables.insert(name, Arc::from(boxed));
    }

    for name in &self.deleted_tables {
        new_tables.remove(name);
    }

    let snapshot = Arc::new(Snapshot { version: self.version, tables: new_tables });

    let mut inner = self.store_inner.lock().unwrap();
    let v = snapshot.version;
    inner.snapshots.insert(v, snapshot);
    if v > inner.latest_version {
        inner.latest_version = v;
    }
    if inner.config.auto_snapshot_gc {
        gc_inner(&mut inner);
    }
    Ok(v)
}
```

- [ ] **Step 7: Run tests**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All pass.

- [ ] **Step 8: Commit**

```bash
git add src/store.rs tests/store_integration.rs
git commit -m "feat: add WriteTx::delete_table for table lifecycle management"
```

---

### Task 7: table_names() on ReadTx and WriteTx (spec item 8)

**Files:**
- Modify: `src/store.rs`
- Modify: `tests/store_integration.rs`

- [ ] **Step 1: Write failing tests**

In `tests/store_integration.rs`, add:

```rust
#[test]
fn read_tx_table_names() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap();
        wtx.open_table::<u64>("posts").unwrap();
        wtx.commit().unwrap();
    }
    let rtx = store.begin_read(None).unwrap();
    let mut names = rtx.table_names();
    names.sort();
    assert_eq!(names, vec!["posts", "users"]);
}

#[test]
fn write_tx_table_names_includes_dirty_and_base() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap();
        wtx.commit().unwrap();
    }
    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<String>("logs").unwrap();
    let mut names = wtx.table_names();
    names.sort();
    assert_eq!(names, vec!["logs", "users"]);
}

#[test]
fn write_tx_table_names_excludes_deleted() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap();
        wtx.open_table::<String>("logs").unwrap();
        wtx.commit().unwrap();
    }
    let mut wtx = store.begin_write(None).unwrap();
    wtx.delete_table("users");
    let names = wtx.table_names();
    assert_eq!(names, vec!["logs"]);
}

#[test]
fn read_tx_table_names_empty_store() {
    let store = Store::default();
    let rtx = store.begin_read(None).unwrap();
    assert!(rtx.table_names().is_empty());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test read_tx_table_names`
Expected: FAIL — `table_names` doesn't exist.

- [ ] **Step 3: Add table_names to ReadTx**

In `src/store.rs`, in the `impl ReadTx` block:

```rust
/// Returns the names of all tables in this snapshot.
pub fn table_names(&self) -> Vec<String> {
    self.snapshot.tables.keys().cloned().collect()
}
```

- [ ] **Step 4: Add table_names to WriteTx**

In `src/store.rs`, in the `impl WriteTx` block:

```rust
/// Returns the names of all tables visible in this transaction,
/// including dirty (newly created) tables and excluding deleted tables.
pub fn table_names(&self) -> Vec<String> {
    let mut names: HashSet<String> = self.base.tables.keys().cloned().collect();
    for name in self.dirty.keys() {
        names.insert(name.clone());
    }
    for name in &self.deleted_tables {
        names.remove(name);
    }
    let mut result: Vec<String> = names.into_iter().collect();
    result.sort();
    result
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add src/store.rs tests/store_integration.rs
git commit -m "feat: add table_names() to ReadTx and WriteTx"
```

---

### Task 8: Readable trait (spec item 10)

**Files:**
- Modify: `src/store.rs`
- Modify: `src/lib.rs`
- Modify: `src/transaction.rs`
- Modify: `tests/store_integration.rs`

- [ ] **Step 1: Write failing test**

In `tests/store_integration.rs`, add:

```rust
use ultima_db::Readable;

fn count_records<R: Send + Sync + 'static>(reader: &impl Readable, opener: impl ultima_db::TableOpener<R>) -> usize {
    reader.open_table(opener).map(|t| t.len()).unwrap_or(0)
}

#[test]
fn readable_trait_works_with_read_tx() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("hello".to_string()).unwrap();
        wtx.commit().unwrap();
    }
    let rtx = store.begin_read(None).unwrap();
    assert_eq!(count_records::<String>(&rtx, "t"), 1);
    assert_eq!(rtx.table_names().len(), 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test readable_trait_works`
Expected: FAIL — `Readable` trait doesn't exist.

- [ ] **Step 3: Define the Readable trait**

In `src/store.rs`, after the `ReadTx` impl block, add:

```rust
/// Read-only access to a snapshot's tables.
///
/// Implemented by [`ReadTx`]. Enables generic functions that read from
/// any snapshot without caring about the concrete transaction type.
pub trait Readable {
    /// Open a table for reading.
    fn open_table<R: Send + Sync + 'static>(&self, opener: impl TableOpener<R>) -> Result<&Table<R>>;

    /// List all table names.
    fn table_names(&self) -> Vec<String>;

    /// The snapshot version.
    fn version(&self) -> u64;
}
```

- [ ] **Step 4: Implement Readable for ReadTx**

```rust
impl Readable for ReadTx {
    fn open_table<R: Send + Sync + 'static>(&self, opener: impl TableOpener<R>) -> Result<&Table<R>> {
        // Delegate to the existing inherent method.
        // Note: the inherent method has R: 'static, which is a subset of
        // Send + Sync + 'static. We keep the tighter bound on the trait for
        // consistency with WriteTx.
        ReadTx::open_table(self, opener)
    }

    fn table_names(&self) -> Vec<String> {
        ReadTx::table_names(self)
    }

    fn version(&self) -> u64 {
        ReadTx::version(self)
    }
}
```

Note: The existing `ReadTx::open_table` has bound `R: 'static`. The trait uses `R: Send + Sync + 'static`. Since `Send + Sync + 'static` is stricter, all types that satisfy the trait bound also satisfy the inherent method's bound. This is fine.

However, the existing inherent method on ReadTx has `R: 'static` not `R: Send + Sync + 'static`. We need to either relax the trait or tighten the inherent method. Since the trait is meant to match WriteTx's constraints (which needs `Send + Sync`), keep `Send + Sync + 'static` on the trait. The inherent `ReadTx::open_table` can keep `R: 'static` for direct usage — the trait impl just delegates.

Actually, for the delegation to work, the inherent method must accept any `R: 'static` which includes `R: Send + Sync + 'static`. So this works.

- [ ] **Step 5: Re-export Readable from lib.rs and transaction.rs**

In `src/lib.rs`:

```rust
pub use store::Readable;
```

In `src/transaction.rs`, add:

```rust
pub use crate::store::Readable;
```

- [ ] **Step 6: Run tests**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: All pass.

- [ ] **Step 7: Commit**

```bash
git add src/store.rs src/lib.rs src/transaction.rs tests/store_integration.rs
git commit -m "feat: add Readable trait for generic snapshot access on ReadTx"
```

---

## Summary

| Task | Spec Items | Description |
|------|-----------|-------------|
| 1 | 1 | Store interior mutability — `commit(self)` |
| 2 | 4 | `Error::VersionNotFound` |
| 3 | 2 | `TableDef<R>` + `TableOpener` trait |
| 4 | 3 | Reverse iteration (`DoubleEndedIterator`) |
| 5 | 5, 6, 7, 11 | Table convenience methods |
| 6 | 9 | `WriteTx::delete_table` |
| 7 | 8 | `table_names()` on ReadTx/WriteTx |
| 8 | 10 | `Readable` trait |
