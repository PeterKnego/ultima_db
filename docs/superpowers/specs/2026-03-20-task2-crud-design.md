# UltimaDB Task 2: CRUD Operations — Design

## Goal

Add named tables and basic CRUD + range scan to `ultima-db`. Each table is an independent, typed, in-memory B-tree keyed by auto-increment `u64`. No serialization, no secondary indexes, no MVCC — those come in later tasks.

---

## Architecture

`Store` holds a map of named tables. Each table is a `Table<R>` generic over any Rust type `R`. `Store` uses `HashMap<String, Box<dyn Any>>` to store heterogeneous table types under string names. `open_table::<R>` creates the table on first call and returns a mutable reference to it on subsequent calls; if the same name is opened with a different `R`, it returns `Error::TypeMismatch`.

`Table<R>` owns a `BTreeMap<u64, R>` and a `next_id: u64` counter starting at 1. Inserts assign the current `next_id`, increment it, and store the record. Reads borrow from the map. Updates and deletes return `Error::KeyNotFound` if the ID does not exist. Range scan delegates to `BTreeMap::range`, yielding `(u64, &R)` pairs in ascending ID order.

---

## Components

### `src/table.rs` (new)

```rust
pub struct Table<R> {
    data: BTreeMap<u64, R>,
    next_id: u64,
}

impl<R> Table<R> {
    pub fn new() -> Self
    pub fn insert(&mut self, record: R) -> u64
    pub fn get(&self, id: u64) -> Option<&R>
    pub fn update(&mut self, id: u64, record: R) -> Result<()>
    pub fn delete(&mut self, id: u64) -> Result<()>
    pub fn range(&self, range: impl RangeBounds<u64>) -> impl Iterator<Item = (u64, &R)>
    pub fn len(&self) -> usize
    pub fn is_empty(&self) -> bool
}
```

**Implementation notes:**

- `next_id` starts at `1`. On overflow (`next_id == u64::MAX` before increment), panic — inserting 2^64 records into a single in-memory table is not a supported use case.
- `insert` always succeeds; returns the assigned ID.
- `update` / `delete` return `Err(Error::KeyNotFound)` if ID is absent.
- `range` uses `BTreeMap::range` which yields `(&u64, &R)`. Map the key with `.map(|(&k, v)| (k, v))` to yield owned `u64` keys. The concrete return type is a `Map` adaptor, so `impl Iterator<Item = (u64, &R)>` is the correct signature.
- No `Clone` bound — all reads are borrows.

### `src/store.rs` (replace)

Replace the existing `Store` struct (which held `BTreeMap<Vec<u8>, Vec<u8>>`) entirely:

```rust
pub struct Store {
    tables: HashMap<String, Box<dyn Any>>,
}

impl Store {
    pub fn new() -> Self
    pub fn open_table<R: 'static>(&mut self, name: &str) -> Result<&mut Table<R>>
}

impl Default for Store { ... }  // delegates to new()
```

**`open_table` implementation note:** The borrow checker rejects simultaneously checking and mutably borrowing the map in one pass. Use a two-phase approach: if the key is absent, insert a fresh `Box<Table<R>>`; then call `get_mut` and downcast. This requires two map lookups but compiles safely without the nightly raw-entry API.

```rust
pub fn open_table<R: 'static>(&mut self, name: &str) -> Result<&mut Table<R>> {
    if !self.tables.contains_key(name) {
        self.tables.insert(name.to_string(), Box::new(Table::<R>::new()));
    }
    self.tables
        .get_mut(name)
        .unwrap()
        .downcast_mut::<Table<R>>()
        .ok_or_else(|| Error::TypeMismatch(name.to_string()))
}
```

**Existing `Store` methods:** `is_empty()` and `len()` on `Store` now reflect the number of named tables (not rows). The existing unit tests `new_store_is_empty` and `store_reports_len_zero` should be **deleted** — they tested the old placeholder and are no longer meaningful. New tests cover `open_table` behaviour instead.

**Stale doc comment:** Remove the existing doc comment on `Store` that says "backing structure will be replaced with a custom B-tree in Task 2" — the custom B-tree is deferred to Task 3.

### `src/error.rs` (extend)

Add one variant:

```rust
#[error("table '{0}' was opened with a different type")]
TypeMismatch(String),
```

### `src/lib.rs` (extend)

Add `pub mod table;` and `pub use table::Table;`.

---

## Data Flow

```
caller
  │
  ▼
store.open_table::<User>("users")   →  &mut Table<User>
  │
  ├── table.insert(user)            →  u64 (assigned id, starts at 1)
  ├── table.get(id)                 →  Option<&User>
  ├── table.update(id, user)        →  Result<()>
  ├── table.delete(id)              →  Result<()>
  └── table.range(1..=100)          →  Iterator<(u64, &User)>
```

---

## Error Handling

| Operation | Error condition | Error variant |
|---|---|---|
| `open_table` | Name exists with different type | `TypeMismatch(name)` |
| `get` | ID absent | `None` (not an error) |
| `update` | ID absent | `KeyNotFound` |
| `delete` | ID absent | `KeyNotFound` |
| `insert` | `next_id` overflow | panic (unsupported) |

---

## Testing

**Unit tests in `src/table.rs`:**
- Insert returns IDs starting at 1, incrementing by 1
- Get returns a reference to the inserted record
- Get on absent ID returns None
- Update replaces the record; get after update returns new value
- Update on absent ID returns `Error::KeyNotFound`
- Delete removes the record; get after delete returns None
- Delete on absent ID returns `Error::KeyNotFound`
- Range yields `(id, &record)` pairs in ascending order for the specified bounds
- Range on empty table yields nothing
- `is_empty` and `len` reflect current record count

**Unit tests in `src/store.rs`:**
- `open_table` creates a new table on first call (table is empty)
- `open_table` returns the same table on second call: insert a record via the first reference, re-open the same name, verify the record is still present
- `open_table` returns `Error::TypeMismatch` when same name is opened with a different `R`

**Integration test in `tests/store_integration.rs`:**
- End-to-end: `open_table`, insert multiple records, get, update, delete, range scan — verify results at each step

---

## What This Is Not

- No serialization — records are stored as owned Rust values in memory
- No secondary indexes — only primary key (u64) access
- No MVCC — single-writer, no transactions (Task 3)
- No custom B-tree node implementation — `std::BTreeMap` is the backing store; the custom B-tree node structure will be introduced alongside MVCC in Task 3
