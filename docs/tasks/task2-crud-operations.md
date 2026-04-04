# UltimaDB Task 2: CRUD Operations

## Goal

Add named tables and basic CRUD + range scan to `ultima-db`. Each table is an independent, typed, in-memory B-tree keyed by auto-increment `u64`. No serialization, no secondary indexes, no MVCC — those come in later tasks.

---

## Architecture

`Store` holds a map of named tables. Each table is a `Table<R>` generic over any Rust type `R`. `Store` uses `HashMap<String, Box<dyn Any>>` to store heterogeneous table types under string names. `open_table::<R>` creates the table on first call and returns a mutable reference to it on subsequent calls; if the same name is opened with a different `R`, it returns `Error::TypeMismatch`.

`Table<R>` owns a `BTreeMap<u64, R>` and a `next_id: u64` counter starting at 1. Inserts assign the current `next_id`, increment it, and store the record. Reads borrow from the map. Updates and deletes return `Error::KeyNotFound` if the ID does not exist. Range scan delegates to `BTreeMap::range`, yielding `(u64, &R)` pairs in ascending ID order.

**Tech Stack:** Rust 2024 edition, `std::collections::{BTreeMap, HashMap}`, `std::any::Any`, `thiserror` (existing).

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

## What This Is Not

- No serialization — records are stored as owned Rust values in memory
- No secondary indexes — only primary key (u64) access
- No MVCC — single-writer, no transactions (Task 3)
- No custom B-tree node implementation — `std::BTreeMap` is the backing store; the custom B-tree node structure will be introduced alongside MVCC in Task 3

---

## File Map

| File | Action | What changes |
|---|---|---|
| `src/error.rs` | Modify | Add `TypeMismatch(String)` variant |
| `src/table.rs` | Create | `Table<R>` with insert, get, update, delete, range, len, is_empty |
| `src/lib.rs` | Modify | Add `pub mod table;` + `pub use table::Table;` (committed alongside table.rs) |
| `src/store.rs` | Replace entirely | New `Store` with `HashMap<String, Box<dyn Any>>` + `open_table`; old `is_empty`/`len` methods and old tests are removed |
| `tests/store_integration.rs` | Replace entirely | End-to-end CRUD integration test |
| `examples/basic_usage.rs` | Replace | Show insert + get + update + delete via open_table |
| `examples/multi_store.rs` | Create | Show two typed tables in one store |

---

## Implementation Plan

### Task 1: Add `TypeMismatch` to Error

**Files:** `src/error.rs`

- [ ] **Step 1: Write the failing test** — add to the existing `mod tests` block:

  ```rust
  #[test]
  fn error_type_mismatch_displays() {
      let e = Error::TypeMismatch("users".to_string());
      assert_eq!(e.to_string(), "table 'users' was opened with a different type");
  }
  ```

- [ ] **Step 2: Run — verify compile error** (`cargo test error`)
- [ ] **Step 3: Add the variant** after `WriteConflict`:

  ```rust
  #[error("table '{0}' was opened with a different type")]
  TypeMismatch(String),
  ```

- [ ] **Step 4: Run — verify 3 tests pass** (`cargo test error`)
- [ ] **Step 5: Commit** — `feat: add TypeMismatch error variant`

---

### Task 2: Create `Table<R>` — insert and get

**Files:** `src/table.rs` (create), `src/lib.rs` (modify)

Note: `src/lib.rs` is updated in this task (not Task 1) so it is never committed in a broken state — `table.rs` and the `lib.rs` change land in the same commit.

- [ ] **Step 1: Write failing tests in new `src/table.rs`**

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn insert_returns_id_starting_at_one() {
          let mut table: Table<String> = Table::new();
          assert_eq!(table.insert("first".to_string()), 1);
      }

      #[test]
      fn insert_returns_incrementing_ids() {
          let mut table: Table<String> = Table::new();
          assert_eq!(table.insert("a".to_string()), 1);
          assert_eq!(table.insert("b".to_string()), 2);
          assert_eq!(table.insert("c".to_string()), 3);
      }

      #[test]
      fn get_returns_inserted_record() {
          let mut table: Table<String> = Table::new();
          let id = table.insert("hello".to_string());
          assert_eq!(table.get(id), Some(&"hello".to_string()));
      }

      #[test]
      fn get_on_absent_id_returns_none() {
          let table: Table<String> = Table::new();
          assert_eq!(table.get(99), None);
      }
  }
  ```

- [ ] **Step 2: Add `pub mod table` to `src/lib.rs`** — replace entire file:

  ```rust
  pub mod error;
  pub mod store;
  pub mod table;

  pub use error::{Error, Result};
  pub use store::Store;
  pub use table::Table;
  ```

- [ ] **Step 3: Run — verify compile error** (`cargo test table`)
- [ ] **Step 4: Add implementation above `#[cfg(test)]`**

  ```rust
  use std::collections::BTreeMap;

  pub struct Table<R> {
      data: BTreeMap<u64, R>,
      next_id: u64,
  }

  impl<R> Table<R> {
      pub fn new() -> Self {
          Self {
              data: BTreeMap::new(),
              next_id: 1,
          }
      }

      pub fn insert(&mut self, record: R) -> u64 {
          assert!(self.next_id < u64::MAX, "Table ID overflow");
          let id = self.next_id;
          self.next_id += 1;
          self.data.insert(id, record);
          id
      }

      pub fn get(&self, id: u64) -> Option<&R> {
          self.data.get(&id)
      }
  }

  impl<R> Default for Table<R> {
      fn default() -> Self {
          Self::new()
      }
  }
  ```

- [ ] **Step 5: Run — verify 4 table tests pass** (`cargo test table`)
- [ ] **Step 6: Commit** — `feat: add Table<R> with insert and get`

---

### Task 3: Add `update` and `delete` to `Table`

**Files:** `src/table.rs`

- [ ] **Step 1: Add failing tests** inside `mod tests`:

  ```rust
  #[test]
  fn update_replaces_record() {
      let mut table: Table<String> = Table::new();
      let id = table.insert("original".to_string());
      table.update(id, "updated".to_string()).unwrap();
      assert_eq!(table.get(id), Some(&"updated".to_string()));
  }

  #[test]
  fn update_on_absent_id_returns_key_not_found() {
      let mut table: Table<String> = Table::new();
      let result = table.update(99, "x".to_string());
      assert!(matches!(result, Err(crate::Error::KeyNotFound)));
  }

  #[test]
  fn delete_removes_record() {
      let mut table: Table<String> = Table::new();
      let id = table.insert("bye".to_string());
      table.delete(id).unwrap();
      assert_eq!(table.get(id), None);
  }

  #[test]
  fn delete_on_absent_id_returns_key_not_found() {
      let mut table: Table<String> = Table::new();
      let result = table.delete(99);
      assert!(matches!(result, Err(crate::Error::KeyNotFound)));
  }
  ```

- [ ] **Step 2: Run — verify compile error** (`cargo test table`)
- [ ] **Step 3: Add imports and methods**

  Add imports:
  ```rust
  use crate::Error;
  use crate::Result;
  ```

  Add methods to `impl<R> Table<R>`:
  ```rust
  pub fn update(&mut self, id: u64, record: R) -> Result<()> {
      if self.data.contains_key(&id) {
          self.data.insert(id, record);
          Ok(())
      } else {
          Err(Error::KeyNotFound)
      }
  }

  pub fn delete(&mut self, id: u64) -> Result<()> {
      self.data.remove(&id).map(|_| ()).ok_or(Error::KeyNotFound)
  }
  ```

- [ ] **Step 4: Run — verify 8 table tests pass** (`cargo test table`)
- [ ] **Step 5: Commit** — `feat: add Table update and delete`

---

### Task 4: Add `range` to `Table`

**Files:** `src/table.rs`

- [ ] **Step 1: Add failing tests**

  ```rust
  #[test]
  fn range_yields_records_in_order() {
      let mut table: Table<&str> = Table::new();
      table.insert("a");
      table.insert("b");
      table.insert("c");
      let results: Vec<_> = table.range(1..=3).collect();
      assert_eq!(results, vec![(1, &"a"), (2, &"b"), (3, &"c")]);
  }

  #[test]
  fn range_with_partial_bounds() {
      let mut table: Table<&str> = Table::new();
      table.insert("a");
      table.insert("b");
      table.insert("c");
      let results: Vec<_> = table.range(2..).collect();
      assert_eq!(results, vec![(2, &"b"), (3, &"c")]);
  }

  #[test]
  fn range_on_empty_table_yields_nothing() {
      let table: Table<String> = Table::new();
      let results: Vec<_> = table.range(..).collect();
      assert!(results.is_empty());
  }
  ```

- [ ] **Step 2: Run — verify compile error** (`cargo test table`)
- [ ] **Step 3: Add import and method**

  ```rust
  use std::ops::RangeBounds;
  ```

  ```rust
  pub fn range(&self, range: impl RangeBounds<u64>) -> impl Iterator<Item = (u64, &R)> {
      self.data.range(range).map(|(&k, v)| (k, v))
  }
  ```

- [ ] **Step 4: Run — verify 11 table tests pass** (`cargo test table`)
- [ ] **Step 5: Commit** — `feat: add Table range scan`

---

### Task 5: Add `len` and `is_empty` to `Table`

**Files:** `src/table.rs`

- [ ] **Step 1: Add failing tests**

  ```rust
  #[test]
  fn new_table_is_empty() {
      let table: Table<String> = Table::new();
      assert!(table.is_empty());
      assert_eq!(table.len(), 0);
  }

  #[test]
  fn len_reflects_insert_and_delete() {
      let mut table: Table<String> = Table::new();
      assert_eq!(table.len(), 0);
      let id = table.insert("a".to_string());
      assert_eq!(table.len(), 1);
      table.delete(id).unwrap();
      assert_eq!(table.len(), 0);
  }
  ```

- [ ] **Step 2: Run — verify compile error** (`cargo test table`)
- [ ] **Step 3: Add methods**

  ```rust
  #[must_use]
  pub fn len(&self) -> usize {
      self.data.len()
  }

  #[must_use]
  pub fn is_empty(&self) -> bool {
      self.data.is_empty()
  }
  ```

- [ ] **Step 4: Run — verify 13 table tests pass** (`cargo test table`)
- [ ] **Step 5: Commit** — `feat: add Table len and is_empty`

---

### Task 6: Replace `Store` with `open_table`

**Files:** `src/store.rs`

This task **completely replaces** `src/store.rs`. The old `Store` struct, its methods, and its unit tests are all removed.

- [ ] **Step 1: Overwrite `src/store.rs` entirely** with tests only:

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn open_table_creates_empty_table() {
          let mut store = Store::new();
          let table = store.open_table::<String>("users").unwrap();
          assert!(table.is_empty());
      }

      #[test]
      fn open_table_returns_same_table_on_second_call() {
          let mut store = Store::new();
          let id = {
              let table = store.open_table::<String>("users").unwrap();
              table.insert("alice".to_string())
          };
          let table = store.open_table::<String>("users").unwrap();
          assert_eq!(table.get(id), Some(&"alice".to_string()));
      }

      #[test]
      fn open_table_returns_type_mismatch_for_different_types() {
          let mut store = Store::new();
          store.open_table::<String>("t").unwrap();
          let result = store.open_table::<u64>("t");
          assert!(matches!(result, Err(crate::Error::TypeMismatch(_))));
      }
  }
  ```

- [ ] **Step 2: Run — verify compile error** (`cargo test store`)
- [ ] **Step 3: Add implementation** (see `open_table` implementation in Components section above)
- [ ] **Step 4: Run — verify 3 store tests pass** (`cargo test store`)
- [ ] **Step 5: Run full test suite** — integration test will fail (expected, fixed in Task 7)
- [ ] **Step 6: Commit** — `feat: replace Store with open_table backed by HashMap<String, Box<dyn Any>>`

---

### Task 7: Replace integration test

**Files:** `tests/store_integration.rs`

This task **completely replaces** `tests/store_integration.rs`.

- [ ] **Step 1: Overwrite `tests/store_integration.rs`**

  ```rust
  use ultima_db::{Error, Store};

  #[test]
  fn end_to_end_crud() {
      let mut store = Store::new();
      let table = store.open_table::<String>("notes").unwrap();

      // Insert
      let id1 = table.insert("first note".to_string());
      let id2 = table.insert("second note".to_string());
      assert_eq!(id1, 1);
      assert_eq!(id2, 2);

      // Get
      assert_eq!(table.get(id1), Some(&"first note".to_string()));
      assert_eq!(table.get(id2), Some(&"second note".to_string()));
      assert_eq!(table.get(99), None);

      // Update
      table.update(id1, "updated first".to_string()).unwrap();
      assert_eq!(table.get(id1), Some(&"updated first".to_string()));
      assert!(matches!(
          table.update(99, "x".to_string()),
          Err(Error::KeyNotFound)
      ));

      // Delete
      table.delete(id2).unwrap();
      assert_eq!(table.get(id2), None);
      assert_eq!(table.len(), 1);
      assert!(matches!(table.delete(99), Err(Error::KeyNotFound)));

      // Range scan
      let id3 = table.insert("third note".to_string());
      let results: Vec<_> = table.range(id1..=id3).collect();
      assert_eq!(results.len(), 2);
      assert_eq!(results[0], (id1, &"updated first".to_string()));
      assert_eq!(results[1], (id3, &"third note".to_string()));
  }

  #[test]
  fn two_tables_are_independent() {
      let mut store = Store::new();

      store.open_table::<String>("users").unwrap().insert("alice".to_string());
      store.open_table::<u64>("posts").unwrap().insert(42u64);

      {
          let users = store.open_table::<String>("users").unwrap();
          assert_eq!(users.len(), 1);
          assert_eq!(users.get(1), Some(&"alice".to_string()));
      }

      {
          let posts = store.open_table::<u64>("posts").unwrap();
          assert_eq!(posts.get(1), Some(&42u64));
      }
  }
  ```

- [ ] **Step 2: Run integration tests** (`cargo test --test store_integration`) — 2 pass
- [ ] **Step 3: Run full test suite** (`cargo test`) — all pass
- [ ] **Step 4: Commit** — `test: replace integration tests with end-to-end CRUD suite`

---

### Task 8: Update examples

**Files:** `examples/basic_usage.rs`, `examples/multi_store.rs`

- [ ] **Step 1: Replace `examples/basic_usage.rs`**

  ```rust
  use ultima_db::Store;

  fn main() {
      let mut store = Store::new();
      let table = store.open_table::<String>("notes").unwrap();

      let id = table.insert("Hello, UltimaDB!".to_string());
      println!("Inserted note with id={id}");

      if let Some(note) = table.get(id) {
          println!("Retrieved: {note}");
      }

      table.update(id, "Hello, updated!".to_string()).unwrap();
      println!("Updated note: {}", table.get(id).unwrap());

      table.delete(id).unwrap();
      println!("Deleted. Table empty: {}", table.is_empty());
  }
  ```

- [ ] **Step 2: Create `examples/multi_store.rs`**

  ```rust
  use ultima_db::Store;

  fn main() {
      let mut store = Store::new();

      store.open_table::<String>("users").unwrap().insert("alice".to_string());
      store.open_table::<String>("users").unwrap().insert("bob".to_string());
      store.open_table::<u32>("scores").unwrap().insert(100u32);
      store.open_table::<u32>("scores").unwrap().insert(200u32);

      println!("users table:");
      for (id, name) in store.open_table::<String>("users").unwrap().range(..) {
          println!("  {id}: {name}");
      }

      println!("scores table:");
      for (id, score) in store.open_table::<u32>("scores").unwrap().range(..) {
          println!("  {id}: {score}");
      }
  }
  ```

- [ ] **Step 3: Run both examples** — verify output
- [ ] **Step 4: Commit** — `feat: update examples for CRUD API`

---

### Task 9: Final verification

- [ ] **Step 1: Full test suite** (`cargo test`) — all pass
- [ ] **Step 2: Clippy** (`cargo clippy -- -D warnings`) — zero warnings
- [ ] **Step 3: Docs** (`cargo doc --no-deps`) — clean
- [ ] **Step 4: Commit if any fixes were needed** — `chore: fix clippy/doc warnings`

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

## End State

```
src/
├── lib.rs       # pub mod table added; pub use table::Table added
├── error.rs     # TypeMismatch(String) variant added
├── table.rs     # Table<R>: insert, get, update, delete, range, len, is_empty
└── store.rs     # Store: HashMap<String, Box<dyn Any>> + open_table<R>
                 # (no is_empty or len on Store — those were placeholder methods)
tests/
└── store_integration.rs   # end_to_end_crud + two_tables_are_independent
examples/
├── basic_usage.rs          # insert/get/update/delete demo
└── multi_store.rs          # two typed tables in one store
```

Task 3 (MVCC) will introduce transactions, version stamps, and a custom CoW B-tree to replace `std::BTreeMap`.