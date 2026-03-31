# UltimaDB Task 1: Project Structure Setup

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the stub `ultima-db` crate into a well-structured library with clear module boundaries, integration tests, a benchmark scaffold, and runnable examples — ready for B-tree CRUD implementation in Task 2.

**Architecture:** Single `ultima-db` crate (no workspace needed at this scope). Library code lives in `src/` with one file per responsibility. Integration tests in `tests/`, examples in `examples/`, benchmarks in `benches/`. No B-tree logic yet — this task only scaffolds types, modules, and the project skeleton. `src/lib.rs` declares modules and re-exports the public API surface.

**Tech Stack:** Rust 2024 edition (requires Rust ≥ 1.85), `thiserror 2` (typed errors), `criterion 0.5` (benchmarks, dev-dep), `std::collections::BTreeMap` as a placeholder backing store.

**B-tree Decision (for reference in Task 2):** After research, the recommended path is:
- Start with `std::BTreeMap` as a placeholder (this task).
- In Task 2, evaluate replacing with [`btree-slab`](https://crates.io/crates/btree-slab) (pluggable slab allocator, raw node access via `BTreeExt`) or [`sweep-bptree`](https://crates.io/crates/sweep-bptree) (2–5x faster sequential/range scans, augmented-node support).
- For MVCC design in Task 3, study [`redb`'s design](https://www.redb.org/design.html) — the only production-quality Rust CoW B-tree + MVCC engine.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `rust-toolchain.toml` | Create | Pin Rust ≥ 1.85 (required for edition 2024) |
| `Cargo.toml` | Modify | Add `thiserror`, `criterion`, bench entry |
| `src/lib.rs` | Modify | Module declarations + `pub use` re-exports for public API |
| `src/error.rs` | Create | `Error` enum + `Result<T>` alias |
| `src/store.rs` | Create | `Store` struct — public API surface |
| `tests/store_integration.rs` | Create | Integration test scaffold |
| `examples/basic_usage.rs` | Create | Demo: show empty store state |
| `examples/multi_store.rs` | Create | Demo: show multiple independent stores |
| `benches/store_bench.rs` | Create | Criterion benchmark scaffold |

---

### Task 1: Pin toolchain, clean up the stub, and add dependencies

**Files:**
- Create: `rust-toolchain.toml`
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`

- [ ] **Step 1: Create `rust-toolchain.toml`**

  Create `rust-toolchain.toml` at the project root:

  ```toml
  [toolchain]
  channel = "stable"
  ```

  This ensures `rustup` selects stable ≥ 1.85. If you need a specific pin, replace `"stable"` with e.g. `"1.85.0"`.

- [ ] **Step 2: Verify toolchain version**

  ```bash
  rustc --version
  ```

  Expected: `rustc 1.85.0` or higher. If lower, run `rustup update stable` before continuing.

- [ ] **Step 3: Replace `src/lib.rs` with module declarations and re-exports**

  ```rust
  pub mod error;
  pub mod store;

  pub use error::{Error, Result};
  pub use store::Store;
  ```

- [ ] **Step 4: Update `Cargo.toml`**

  Replace the entire file contents with:

  ```toml
  [package]
  name = "ultima-db"
  version = "0.1.0"
  edition = "2024"
  description = "Embedded in-memory B-tree store with MVCC"

  [dependencies]
  thiserror = "2"

  [dev-dependencies]
  criterion = { version = "0.5", features = ["html_reports"] }

  [[bench]]
  name = "store_bench"
  harness = false
  ```

- [ ] **Step 5: Run `cargo check` — expect a compile error** (modules declared but files don't exist yet — this is expected)

  ```bash
  cargo check
  ```

  Expected: errors about missing `error` and `store` modules.

- [ ] **Step 6: Commit**

  ```bash
  git add rust-toolchain.toml Cargo.toml src/lib.rs
  git commit -m "chore: pin toolchain, clean stub, add thiserror and criterion deps"
  ```

---

### Task 2: Define error types

**Files:**
- Create: `src/error.rs`

- [ ] **Step 1: Write the failing test**

  Create `src/error.rs` with the test only:

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn error_key_not_found_displays() {
          let e = Error::KeyNotFound;
          assert_eq!(e.to_string(), "key not found");
      }

      #[test]
      fn error_write_conflict_displays() {
          let e = Error::WriteConflict;
          assert_eq!(e.to_string(), "write conflict");
      }
  }
  ```

- [ ] **Step 2: Run test — verify it fails to compile**

  ```bash
  cargo test error
  ```

  Expected: compile error — `Error` not defined.

- [ ] **Step 3: Implement `Error` above the tests**

  Add above the `#[cfg(test)]` block in `src/error.rs`:

  ```rust
  use thiserror::Error;

  #[derive(Debug, Error)]
  pub enum Error {
      #[error("key not found")]
      KeyNotFound,
      #[error("write conflict")]
      WriteConflict,
  }

  pub type Result<T> = std::result::Result<T, Error>;
  ```

- [ ] **Step 4: Run test — verify it passes**

  ```bash
  cargo test error
  ```

  Expected: 2 tests pass.

- [ ] **Step 5: Commit**

  ```bash
  git add src/error.rs
  git commit -m "feat: add Error enum and Result alias"
  ```

---

### Task 3: Create the Store module skeleton

**Files:**
- Create: `src/store.rs`

- [ ] **Step 1: Write the failing test**

  Create `src/store.rs` with tests only:

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn new_store_is_empty() {
          let store = Store::new();
          assert!(store.is_empty());
      }

      #[test]
      fn store_reports_len_zero() {
          let store = Store::new();
          assert_eq!(store.len(), 0);
      }
  }
  ```

- [ ] **Step 2: Run test — verify compile error**

  ```bash
  cargo test store
  ```

  Expected: compile error — `Store` not defined.

- [ ] **Step 3: Implement `Store` stub above the tests**

  Add above `#[cfg(test)]` in `src/store.rs`:

  ```rust
  use std::collections::BTreeMap;

  /// An in-memory key-value store backed by a BTreeMap.
  ///
  /// This is a placeholder; the backing structure will be replaced
  /// with a custom B-tree in Task 2.
  pub struct Store {
      data: BTreeMap<Vec<u8>, Vec<u8>>,
  }

  impl Store {
      pub fn new() -> Self {
          Self {
              data: BTreeMap::new(),
          }
      }

      #[must_use]
      pub fn is_empty(&self) -> bool {
          self.data.is_empty()
      }

      #[must_use]
      pub fn len(&self) -> usize {
          self.data.len()
      }
  }

  impl Default for Store {
      fn default() -> Self {
          Self::new()
      }
  }
  ```

- [ ] **Step 4: Run test — verify it passes**

  ```bash
  cargo test store
  ```

  Expected: 2 tests pass.

- [ ] **Step 5: Commit**

  ```bash
  git add src/store.rs
  git commit -m "feat: add Store stub with BTreeMap placeholder"
  ```

---

### Task 4: Add integration tests

**Files:**
- Create: `tests/store_integration.rs`

- [ ] **Step 1: Write the integration test**

  Create `tests/store_integration.rs`:

  ```rust
  use ultima_db::Store;

  #[test]
  fn store_is_empty_on_creation() {
      let store = Store::new();
      assert!(store.is_empty());
      assert_eq!(store.len(), 0);
  }
  ```

  Note: uses `ultima_db::Store` — the re-export from `lib.rs`, not the internal module path.

- [ ] **Step 2: Verify the test passes** (Store is already implemented — integration tests verify the public API surface, not drive implementation)

  ```bash
  cargo test --test store_integration
  ```

  Expected: 1 test passes.

- [ ] **Step 3: Commit**

  ```bash
  git add tests/store_integration.rs
  git commit -m "test: add integration test scaffold"
  ```

---

### Task 5: Add example applications

**Files:**
- Create: `examples/basic_usage.rs`
- Create: `examples/multi_store.rs`

- [ ] **Step 1: Create `examples/basic_usage.rs`**

  ```rust
  use ultima_db::Store;

  fn main() {
      let store = Store::new();
      println!("UltimaDB store created.");
      println!("  Empty: {}", store.is_empty());
      println!("  Len:   {}", store.len());
  }
  ```

- [ ] **Step 2: Create `examples/multi_store.rs`**

  ```rust
  use ultima_db::Store;

  fn main() {
      let store_a = Store::new();
      let store_b = Store::new();
      println!("Two independent stores:");
      println!("  store_a empty: {}", store_a.is_empty());
      println!("  store_b empty: {}", store_b.is_empty());
  }
  ```

- [ ] **Step 3: Run both examples — verify output**

  ```bash
  cargo run --example basic_usage
  ```

  Expected:
  ```
  UltimaDB store created.
    Empty: true
    Len:   0
  ```

  ```bash
  cargo run --example multi_store
  ```

  Expected:
  ```
  Two independent stores:
    store_a empty: true
    store_b empty: true
  ```

- [ ] **Step 4: Commit**

  ```bash
  git add examples/basic_usage.rs examples/multi_store.rs
  git commit -m "feat: add basic_usage and multi_store examples"
  ```

---

### Task 6: Add benchmark scaffold

**Files:**
- Create: `benches/store_bench.rs`

- [ ] **Step 1: Create the benchmark**

  Create `benches/store_bench.rs`:

  ```rust
  use criterion::{criterion_group, criterion_main, Criterion};
  use ultima_db::Store;

  fn bench_create(c: &mut Criterion) {
      c.bench_function("store_create", |b| b.iter(Store::new));
  }

  criterion_group!(benches, bench_create);
  criterion_main!(benches);
  ```

- [ ] **Step 2: Run benchmarks — verify they compile and run**

  ```bash
  cargo bench
  ```

  Expected: benchmark runs and reports a time (e.g., `store_create: 15 ns`).

- [ ] **Step 3: Commit**

  ```bash
  git add benches/store_bench.rs
  git commit -m "test: add criterion benchmark scaffold"
  ```

---

### Task 7: Final verification

- [ ] **Step 1: Full test suite**

  ```bash
  cargo test
  ```

  Expected: all unit + integration tests pass, no failures.

- [ ] **Step 2: Clippy — zero warnings**

  ```bash
  cargo clippy -- -D warnings
  ```

  Expected: no warnings or errors. (`#[must_use]` on `is_empty` and `len` prevents `must_use_candidate` warnings in Rust 2024 edition.)

- [ ] **Step 3: Docs build clean**

  ```bash
  cargo doc --no-deps
  ```

  Expected: docs generated with no warnings.

- [ ] **Step 4: Commit (if clippy/doc required any fixes)**

  ```bash
  git add -p
  git commit -m "chore: fix clippy warnings"
  ```

---

## End State

After this plan completes, the repo has:

```
ultima-db/
├── rust-toolchain.toml     # pins Rust stable ≥ 1.85
├── Cargo.toml              # thiserror + criterion declared
├── src/
│   ├── lib.rs              # pub mod + pub use re-exports
│   ├── error.rs            # Error enum, Result<T> alias
│   └── store.rs            # Store { data: BTreeMap } stub, #[must_use] methods
├── tests/
│   └── store_integration.rs
├── examples/
│   ├── basic_usage.rs
│   └── multi_store.rs
└── benches/
    └── store_bench.rs
```

Task 2 (CRUD) will replace the `BTreeMap` placeholder with a real B-tree node structure and implement `get`, `insert`, `delete`, and `scan` on `Store`.
