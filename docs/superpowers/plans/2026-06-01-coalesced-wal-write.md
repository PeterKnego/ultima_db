# Coalesced WAL Write (config option) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose a production-safe WAL write strategy — coalesce a commit batch into a single `write` per fsync while keeping full `sync_all` durability — selectable via `Persistence::Standalone { .., wal_write: WalWrite }`.

**Architecture:** Generalize the existing `BufferedFileSink` with a `datasync: bool` (sync_all vs sync_data); add an ungated `WalSinkKind::Coalesced` (= coalesced + sync_all); add a public `WalWrite { PerEntry, Coalesced }` enum as a new field on `Persistence::Standalone`; map it to the sink in `Store::new` via the existing `WalHandle::with_sink_kind`. Default stays `PerEntry` (today's behavior). `fdatasync` remains bench-only.

**Tech Stack:** Rust 2024, the existing `WalSink` trait, criterion.

**Spec:** `docs/superpowers/specs/2026-06-01-coalesced-wal-write-design.md`

**Branch:** `feat/wal-coalesced-write` off `main` (`4ae6aa4`).

---

## Setup (do once before Task 1)

```bash
git checkout main
git checkout -b feat/wal-coalesced-write
```

## File Structure

- `src/wal.rs` — generalize `BufferedFileSink` (add `datasync`), add `WalSinkKind::Coalesced` + arm, sink unit tests.
- `src/persistence.rs` — new `WalWrite` enum, new field on `Persistence::Standalone`.
- `src/lib.rs` — re-export `WalWrite`.
- `src/store.rs` — wire `wal_write` → `WalSinkKind` in the `Standalone` arm of `Store::new`.
- `benches/wal_bench.rs` — add `("coalesced", WalSinkKind::Coalesced)` to `KINDS`.
- `tests/persistence_integration.rs` — store-level recovery tests through `WalWrite::Coalesced`.
- `examples/profile_commit.rs`, `tests/store_integration.rs` — update `Persistence::Standalone` constructors for the new field.
- `docs/tasks/task31_coalesced_wal_write.md` — results + recommendation (Task 5).

---

## Task 1: Generalize `BufferedFileSink` with `datasync: bool`

Make the coalescing sink support both `sync_all` (fsync) and `sync_data` (fdatasync), selected by a field.

**Files:**
- Modify: `src/wal.rs` (`BufferedFileSink` struct, `open`, `WalSink::sync`, the `with_sink_kind` `BufferedFile` arm, the existing test)
- Test: `src/wal.rs` (`mod tests`)

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `src/wal.rs`:

```rust
#[test]
fn buffered_file_sink_sync_all_roundtrips_via_read_wal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut sink = BufferedFileSink::open(dir.path(), false).unwrap(); // sync_all
        for v in 1..=5u64 {
            sink.append(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), id: v, data: vec![v as u8; 32] }] }).unwrap();
        }
        sink.sync().unwrap();
    }
    let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
    assert_eq!(read.len(), 5);
    assert_eq!(read[4].version, 5);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features persistence buffered_file_sink_sync_all -- --nocapture`
Expected: FAIL — compile error, `BufferedFileSink::open` takes 1 argument but 2 were supplied.

- [ ] **Step 3: Generalize the sink**

In `src/wal.rs`, change the struct:

```rust
struct BufferedFileSink {
    file: File,
    buf: Vec<u8>,
    /// When true, `sync` uses `sync_data` (fdatasync); when false, `sync_all`
    /// (full fsync). Coalescing (one `write` per batch) is identical either way.
    datasync: bool,
}
```

Change `open` to take `datasync`:

```rust
impl BufferedFileSink {
    fn open(dir: &Path, datasync: bool) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let wal_path = dir.join(WAL_FILENAME);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        sync_dir(dir)?;
        Ok(BufferedFileSink { file, buf: Vec::new(), datasync })
    }
}
```

Change `sync` to branch on `datasync`:

```rust
impl WalSink for BufferedFileSink {
    fn append(&mut self, entry: &WalEntry) -> Result<()> {
        self.buf.extend_from_slice(&frame_entry(entry)?);
        Ok(())
    }
    fn sync(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.file.write_all(&self.buf).map_err(|e| Error::Persistence(e.to_string()))?;
            self.buf.clear(); // retains capacity for the next batch
        }
        if self.datasync {
            self.file.sync_data().map_err(|e| Error::Persistence(e.to_string()))
        } else {
            self.file.sync_all().map_err(|e| Error::Persistence(e.to_string()))
        }
    }
}
```

Update the existing caller in `with_sink_kind` (the `BufferedFile` arm) to pass `true` (preserves the current fdatasync behavior):

```rust
            WalSinkKind::BufferedFile => {
                Ok(Self::with_sink(BufferedFileSink::open(dir, true)?, consistent, poison))
            }
```

Update the existing test `buffered_file_sink_roundtrips_via_read_wal` — change its `BufferedFileSink::open(dir.path())` call to `BufferedFileSink::open(dir.path(), true)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features persistence wal`
Expected: PASS — both `buffered_file_sink_roundtrips_via_read_wal` (datasync=true) and the new `buffered_file_sink_sync_all_roundtrips_via_read_wal` (datasync=false) pass, plus all existing WAL tests.

Run: `cargo test --features bench-internals buffered -- --nocapture` (bench-internals build still compiles).
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/wal.rs
git commit -m "refactor(wal): parameterize BufferedFileSink with datasync (sync_all vs sync_data)"
```

---

## Task 2: Add `WalSinkKind::Coalesced` + bench column

Expose coalesced + `sync_all` as an internal sink kind and add it to the benchmark matrix.

**Files:**
- Modify: `src/wal.rs` (`WalSinkKind` enum, `with_sink_kind`)
- Modify: `benches/wal_bench.rs` (`KINDS`)
- Test: `src/wal.rs` (`mod tests`)

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `src/wal.rs`:

```rust
#[test]
fn with_sink_kind_coalesced_writes_recoverable_wal() {
    let dir = tempfile::tempdir().unwrap();
    let poison = Arc::new(WalPoison::new());
    {
        let h = WalHandle::with_sink_kind(dir.path(), true, poison, WalSinkKind::Coalesced).unwrap();
        h.write(WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] })
            .unwrap()
            .wait()
            .unwrap();
    }
    let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].version, 1);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features persistence with_sink_kind_coalesced -- --nocapture`
Expected: FAIL — no variant named `Coalesced` found for enum `WalSinkKind`.

- [ ] **Step 3: Add the variant + arm**

In `src/wal.rs`, add the `Coalesced` variant to `WalSinkKind` immediately after `FsWrite` (it is ungated — dependency-free and production-reachable):

```rust
pub enum WalSinkKind {
    /// Baseline: one `write_all` per entry (the framed record) + `sync_all` per batch.
    FsWrite,
    /// Coalesced single `write` per batch + `sync_all` (fsync). Production-safe.
    Coalesced,
    /// Coalesced single `write` per batch + `sync_data` (fdatasync). Bench comparison only.
    BufferedFile,
    #[cfg(feature = "bench-internals")]
    Mmap,
    #[cfg(all(target_os = "linux", feature = "wal-iouring"))]
    IoUring,
}
```

Add the matching arm in `with_sink_kind`, right after the `FsWrite` arm:

```rust
            WalSinkKind::Coalesced => {
                Ok(Self::with_sink(BufferedFileSink::open(dir, false)?, consistent, poison))
            }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features persistence wal`
Expected: PASS — new test green; build clean across configs (the variant is ungated so the `match` stays exhaustive everywhere).

- [ ] **Step 5: Add the bench column**

In `benches/wal_bench.rs`, add a `coalesced` entry to the `KINDS` const(s). The non-iouring const becomes:

```rust
#[cfg(not(all(target_os = "linux", feature = "wal-iouring")))]
const KINDS: &[(&str, WalSinkKind)] = &[
    ("fswrite", WalSinkKind::FsWrite),
    ("coalesced", WalSinkKind::Coalesced),
    ("buffered", WalSinkKind::BufferedFile),
    ("mmap", WalSinkKind::Mmap),
];
```

and the iouring const becomes:

```rust
#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
const KINDS: &[(&str, WalSinkKind)] = &[
    ("fswrite", WalSinkKind::FsWrite),
    ("coalesced", WalSinkKind::Coalesced),
    ("buffered", WalSinkKind::BufferedFile),
    ("mmap", WalSinkKind::Mmap),
    ("iouring", WalSinkKind::IoUring),
];
```

- [ ] **Step 6: Verify the bench compiles + smoke-run the coalesced case**

Run: `cargo build --benches --features bench-internals`
Expected: builds clean.

Run: `cargo bench --bench wal_bench --features bench-internals -- "wal_eventual_batch/coalesced/1KiB"`
Expected: prints the `[wal_bench] WAL dir ... fs = ext4` guard line and a `coalesced/1KiB` result. (Full matrix run is deferred to Task 5.)

- [ ] **Step 7: Commit**

```bash
git add src/wal.rs benches/wal_bench.rs
git commit -m "feat(wal): add Coalesced sink kind (coalesced write + sync_all) + bench column"
```

---

## Task 3: Public `WalWrite` enum + `Persistence::Standalone` field + store wiring

Expose the option in the public API and honor it when constructing the WAL. **This is a breaking change** to `Persistence::Standalone` — every literal constructor must add the field.

**Files:**
- Modify: `src/persistence.rs` (`WalWrite` enum, `Persistence::Standalone` field)
- Modify: `src/lib.rs` (re-export)
- Modify: `src/store.rs` (wire the `Standalone` arm in `Store::new`)
- Modify: constructors in `src/wal.rs` (2), `examples/profile_commit.rs`, `tests/persistence_integration.rs`, `tests/store_integration.rs`
- Test: `tests/persistence_integration.rs`

- [ ] **Step 1: Write the failing test**

Add to `tests/persistence_integration.rs` (it already has `standalone_config`, `open_store`, `User`):

```rust
#[test]
fn standalone_wal_recovery_consistent_coalesced() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig {
        persistence: Persistence::Standalone {
            dir: dir.path().to_path_buf(),
            durability: Durability::Consistent,
            wal_write: WalWrite::Coalesced,
        },
        ..StoreConfig::default()
    };

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap().insert(User { name: "Alice".into(), age: 30 }).unwrap();
        wtx.commit().unwrap();
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap().insert(User { name: "Bob".into(), age: 25 }).unwrap();
        wtx.commit().unwrap();
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.version(), 2);
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 2);
    assert_eq!(table.get(1).unwrap(), &User { name: "Alice".into(), age: 30 });
    assert_eq!(table.get(2).unwrap(), &User { name: "Bob".into(), age: 25 });
}
```

Also add `WalWrite` to the test file's imports: change `use ultima_db::{Durability, Persistence, Store, StoreConfig};` to `use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features persistence --test persistence_integration standalone_wal_recovery_consistent_coalesced -- --nocapture`
Expected: FAIL — compile error: no `WalWrite` in `ultima_db`, and `Persistence::Standalone` has no field `wal_write`.

- [ ] **Step 3: Add the `WalWrite` enum**

In `src/persistence.rs`, after the `Durability` enum, add:

```rust
// ---------------------------------------------------------------------------
// WalWrite — how the WAL writes a committed batch to disk
// ---------------------------------------------------------------------------

/// How the WAL writes a committed batch to disk (Standalone mode).
///
/// Orthogonal to [`Durability`], which controls *when* `commit()` returns. All
/// four combinations are valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalWrite {
    /// One `write` per entry, then `sync_all` per batch. The original behavior.
    #[default]
    PerEntry,
    /// The whole batch is coalesced into a single `write`, then `sync_all`. Same
    /// durability as `PerEntry` (full fsync); fewer syscalls per batch — better
    /// group-commit throughput under Eventual / high-concurrency loads.
    Coalesced,
}
```

Add the field to the `Standalone` variant:

```rust
    Standalone {
        /// Directory for WAL and checkpoint files.
        dir: PathBuf,
        /// WAL fsync behavior.
        durability: Durability,
        /// How the WAL writes each committed batch to disk.
        wal_write: WalWrite,
    },
```

- [ ] **Step 4: Re-export `WalWrite`**

In `src/lib.rs`, change line 34:

```rust
pub use persistence::{Durability, Persistence, Record, WalWrite};
```

- [ ] **Step 5: Wire the store**

In `src/store.rs`, replace the `Standalone` arm of the `wal_handle` match in `Store::new` (currently destructures `{ dir, durability }` and calls `WalHandle::new`) with:

```rust
            crate::persistence::Persistence::Standalone { dir, durability, wal_write } => {
                let consistent = matches!(durability, crate::persistence::Durability::Consistent);
                let kind = match wal_write {
                    crate::persistence::WalWrite::PerEntry => crate::wal::WalSinkKind::FsWrite,
                    crate::persistence::WalWrite::Coalesced => crate::wal::WalSinkKind::Coalesced,
                };
                Some(crate::wal::WalHandle::with_sink_kind(
                    dir,
                    consistent,
                    Arc::clone(&wal_poison),
                    kind,
                )?)
            }
```

(`WalHandle::new` is unchanged and still used by nothing else in the store; leave it as-is.)

- [ ] **Step 6: Update every other `Persistence::Standalone { .. }` literal constructor**

Add `wal_write: WalWrite::PerEntry` (or the crate-qualified path appropriate to each file) to each of these literal constructions. The `{ dir, .. }` / `{ .. }` *pattern matches* elsewhere in `store.rs` are fine and need no change — only literal constructions break.

- `src/wal.rs:1368` (`wal_store` test helper) — uses `crate::Persistence::Standalone { dir:.., durability: crate::Durability::Consistent }`; add `wal_write: crate::persistence::WalWrite::PerEntry,`.
- `src/wal.rs:1782` (`store_wait_durable_eventual_end_to_end`) — add `wal_write: crate::persistence::WalWrite::PerEntry,`.
- `examples/profile_commit.rs:244` — add `wal_write: ultima_db::WalWrite::PerEntry,`.
- `tests/persistence_integration.rs` `standalone_config` helper (~line 17) — add `wal_write: WalWrite::PerEntry,` (import already added in Step 1).
- `tests/store_integration.rs:1816` — add `wal_write: WalWrite::PerEntry,`; ensure `WalWrite` is imported in that file (add to its `use ultima_db::{...}` line; if it imports `Persistence`/`Durability` already, add `WalWrite` alongside).

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test --features persistence wal` then `cargo test --features persistence --test persistence_integration` then `cargo test --features persistence --test store_integration`
Expected: PASS — the new `standalone_wal_recovery_consistent_coalesced` test passes, and ALL existing persistence tests still pass (default `PerEntry` path unchanged).

Run all build configs (the field addition must not break any):
```
cargo build
cargo build --features persistence
cargo build --features bench-internals
cargo build --features "bench-internals wal-iouring"
cargo build --examples --features persistence
```
Expected: all clean.

- [ ] **Step 8: Commit**

```bash
git add src/persistence.rs src/lib.rs src/store.rs src/wal.rs examples/profile_commit.rs tests/persistence_integration.rs tests/store_integration.rs
git commit -m "feat(persistence): add WalWrite option to Persistence::Standalone (Coalesced WAL writes)"
```

---

## Task 4: Additional store-level recovery coverage

Cover the Eventual×Coalesced combination and confirm the default path is unchanged.

**Files:**
- Test: `tests/persistence_integration.rs`

- [ ] **Step 1: Write the tests**

Add to `tests/persistence_integration.rs`:

```rust
#[test]
fn standalone_wal_recovery_eventual_coalesced() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig {
        persistence: Persistence::Standalone {
            dir: dir.path().to_path_buf(),
            durability: Durability::Eventual,
            wal_write: WalWrite::Coalesced,
        },
        ..StoreConfig::default()
    };

    {
        let store = open_store(config.clone());
        for (name, age) in [("Alice", 30u32), ("Bob", 25), ("Carol", 41)] {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users").unwrap().insert(User { name: name.into(), age }).unwrap();
            wtx.commit().unwrap();
        }
        // Eventual: dropping the store at the end of this scope joins the WAL
        // background thread, which fsyncs all pending writes before we reopen.
        // (Same guarantee exercised by `eventual_drop_flushes_all_pending_wal_writes`.)
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 3);
    assert_eq!(table.get(3).unwrap(), &User { name: "Carol".into(), age: 41 });
}

#[test]
fn standalone_perentry_default_still_recovers() {
    // Sanity: the default WalWrite::PerEntry path behaves as before.
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent); // sets PerEntry
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap().insert(User { name: "Alice".into(), age: 30 }).unwrap();
        wtx.commit().unwrap();
    }
    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.open_table::<User>("users").unwrap().len(), 1);
}
```

> Note: if `Store::checkpoint()` is not the correct API name for forcing durability before reopen, use the same mechanism the existing Eventual recovery tests in this file use (search for an existing `Eventual` recovery test and mirror its "flush before reopen" step). Do NOT invent an API — mirror what already passes in this file.

- [ ] **Step 2: Run to verify they pass**

Run: `cargo test --features persistence --test persistence_integration -- standalone_wal_recovery_eventual_coalesced standalone_perentry_default_still_recovers --nocapture`
Expected: PASS both.

- [ ] **Step 3: Run the whole persistence test suite**

Run: `cargo test --features persistence`
Expected: PASS, 0 failures.

- [ ] **Step 4: Commit**

```bash
git add tests/persistence_integration.rs
git commit -m "test(persistence): store-level recovery coverage for Coalesced WAL writes"
```

---

## Task 5: Bench run + `task31` doc

**Files:**
- Create: `docs/tasks/task31_coalesced_wal_write.md`

- [ ] **Step 1: Run the full matrix**

Run: `cargo bench --bench wal_bench --features "bench-internals wal-iouring" 2>&1 | tee target/wal-bench-coalesced.txt`
Confirm the startup line shows `fs = ext4` (not tmpfs). Extract the `wal_eventual_batch` medians for `fswrite`, `coalesced`, `buffered` (and the others) across 1/2/4/8/16 KiB.

- [ ] **Step 2: Write `docs/tasks/task31_coalesced_wal_write.md`**

Read `docs/tasks/task30_wal_backends.md` first to match tone. The doc must include:
- **Goal:** expose coalesced + `sync_all` WAL writes as a production option (`WalWrite::Coalesced`), without the fdatasync durability change.
- **API:** the `WalWrite { PerEntry, Coalesced }` enum, that it's a field on `Persistence::Standalone`, orthogonal to `Durability`, default `PerEntry` (unchanged behavior). Note the breaking field addition.
- **Architecture:** `BufferedFileSink { datasync }`; `WalSinkKind::Coalesced` = coalesced + sync_all; `WalSinkKind::BufferedFile` = coalesced + fdatasync (bench-only); store maps `WalWrite` → kind via `with_sink_kind`.
- **Results:** the `wal_eventual_batch` table from Step 1 with the `coalesced` column added next to `fswrite` and `buffered` (real numbers). State where `coalesced` lands (expected: between fswrite and the fdatasync buffered — it gets the write-coalescing win but pays full fsync).
- **Recommendation/guidance:** when to choose `Coalesced` (high-throughput / batched / Eventual workloads) vs `PerEntry`; that `fdatasync` (`BufferedFile`) and `Mmap`/`IoUring` remain bench-only/experimental and are NOT production options.
- Cross-reference `task30_wal_backends.md`.

- [ ] **Step 3: Final verification**

Run: `cargo test --features "bench-internals wal-iouring"` and `cargo clippy --features "bench-internals wal-iouring" -- -D warnings`
Expected: all tests pass; zero warnings.

- [ ] **Step 4: Commit**

```bash
git add docs/tasks/task31_coalesced_wal_write.md
git commit -m "docs(wal): document Coalesced WAL write config option + results (task31)"
```

---

## Notes for the implementer

- `WalWrite` and `WalSinkKind::Coalesced` are BOTH ungated (no feature) — coalesced+sync_all is dependency-free and production-reachable, unlike `Mmap`/`IoUring`. Do not put `#[cfg(...)]` on them.
- The bench is run with `--features bench-internals` (or `+ wal-iouring`); the production tests run with `--features persistence`.
- Keep the default `WalWrite::PerEntry` so existing deployments are unaffected — this work only *adds* an opt-in.
- After every task, `cargo test --features persistence` must stay green (proves the default/production path is intact).
