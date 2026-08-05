# Table-lifecycle Race Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cover the MultiWriter OCC table-lifecycle surface — where five silent-data-loss bugs were found by reading on 2026-08-05 and where nothing is tested — with a deterministic pairwise race matrix that re-finds all five.

**Architecture:** A 7×6 matrix of (in-flight transaction state) × (concurrently committed operation), run **sequentially** — `begin_write` → B commits → `A.commit()` — because every one of the five bugs was a two-party race and every reproducer was sequential. No threads, no scheduler, no timing. Each cell asserts its commit outcome against a stated rule, plus one umbrella property: anything that committed `Ok` is never silently reverted.

**Tech Stack:** Rust, `cargo test`, the existing `Store`/`WriteTx` API. No new dependencies.

**Design spec:** `docs/superpowers/specs/2026-08-05-table-lifecycle-races-design.md` — read it before Task 1.

## Global Constraints

- `WriterMode::MultiWriter` throughout. The mechanisms under test are MultiWriter-only; SingleWriter refuses a concurrent install with `WriterBusy`.
- Build stores with `StoreConfig::builder()` in integration tests — that is the convention in `tests/*.rs` (19 builder calls, 0 struct literals). `src/store.rs`'s inline tests use struct literals; do not copy that style into `tests/`.
- `cargo clippy --features persistence,fulltext --all-targets -- -D warnings` clean; also `persistence,fulltext,metrics`.
- Run `cargo test --features persistence,fulltext,metrics` before every commit — CI uses that combination and this repo has shipped a red `main` by testing only `persistence,fulltext`.
- **Deterministic: no threads, no sleeps, no timing assumptions.** If a cell seems to need one, it is the wrong cell — report it.
- Do not run `cargo fmt` (repo-wide rustfmt-version drift; match surrounding style).
- Nothing under `src/` may change. This plan adds tests only. If a cell cannot be expressed without a production change, **stop and report** — that is a finding, not a licence.
- Known flake, not yours: `tests/store_integration.rs::concurrent_same_table_overlapping_keys_with_retry`, ~1 in 5, `expected at least one WriteConflict ... got 0`. Re-run isolated.
- `CARGO_TARGET_DIR` must not be under `/tmp` — it is tmpfs here and `src/test_scratch.rs`'s durability guard will refuse. Use `/home/claude/.cargo-target-main`.

---

## File Structure

- **Create `tests/table_lifecycle_races.rs`** — the whole suite. One file: the axes, the cell runner, the expectation table, and the calibration record. It is a matrix over one subject; splitting it by axis would scatter the oracle.
- Nothing else. No production change, no new helper crate, no Elle modification.

---

### Task 1: The harness, and the two cells that prove it works

**Files:**
- Create: `tests/table_lifecycle_races.rs`

**Interfaces:**
- Produces: `enum AState`, `enum BOp`, `enum Expect`, `fn run_cell(a: AState, b: BOp) -> CellOutcome`, `struct CellOutcome { commit: Result<u64>, rows_after: Vec<(u64, String)>, table_present: bool }`, and `fn check_cell(a: AState, b: BOp, expect: Expect)`.

**Why these two cells first:** `A1×B1` and `A3×B1` are bugs 1 and 2 from `b990951`. If the harness cannot express them, it cannot express the matrix.

- [ ] **Step 1: Write the failing test**

Create `tests/table_lifecycle_races.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Deterministic pairwise races between an in-flight MultiWriter transaction
//! and a concurrently committed table-lifecycle operation.
//!
//! Five silent-data-loss bugs were found here by reading on 2026-08-05
//! (b990951, dbd56d4, 68cd794) and nothing tested this surface: the Elle
//! harness generates only row `Read`/`Append`, and `bulk_load`/`delete_table`
//! never raced a commit in any automated test.
//!
//! Every one of those five was a two-party race whose reproducer was
//! *sequential* — A begins, B commits, A commits. So is every cell here. No
//! threads, no scheduler, no timing.
//!
//! See `docs/superpowers/specs/2026-08-05-table-lifecycle-races-design.md`.

use ultima_db::{
    BulkLoadInput, BulkLoadOptions, BulkSource, Error, IndexKind, Store, StoreConfig, WriterMode,
};

const T: &str = "t";

/// What the in-flight transaction A did before B committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AState {
    /// Opened the table, wrote nothing.
    OpenOnly,
    /// Opened and wrote one row.
    OpenWrite,
    /// Opened and defined an index — DDL, no row write.
    OpenDdl,
    /// Deleted the table.
    Delete,
    /// Deleted, then reopened (fresh, empty), wrote nothing.
    DeleteRecreate,
    /// Deleted, reopened, wrote one row.
    DeleteRecreateWrite,
    /// Wrote a row, then deleted, then reopened, wrote nothing.
    WriteDeleteRecreate,
}

/// What committed concurrently, between A's begin and A's commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BOp {
    /// `Store::bulk_load` Replace — 3 rows, "bulk1".."bulk3".
    BulkReplace,
}

/// The expected commit outcome for a cell.
#[derive(Debug, Clone, Copy)]
enum Expect {
    /// A commits; see the umbrella check for what must be visible.
    Commits,
    /// A must fail with `Error::WriteConflict`.
    Conflicts,
    /// A must fail with `Error::IndexDdlConflict`.
    DdlConflicts,
}

struct CellOutcome {
    commit: Result<u64, Error>,
    rows_after: Vec<(u64, String)>,
    table_present: bool,
}

fn mw_store() -> Store {
    Store::new(
        StoreConfig::builder()
            .writer_mode(WriterMode::MultiWriter)
            .build(),
    )
    .unwrap()
}

/// Seeds `T` with two rows, ids 1 and 2.
fn seed(store: &Store) {
    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table::<String>(T).unwrap();
    t.insert("seed1".to_string()).unwrap();
    t.insert("seed2".to_string()).unwrap();
    wtx.commit().unwrap();
}

/// B's marker payload: whatever B installs is distinctive, so the umbrella
/// check can tell "B's effect survived" from "A's stale clone won".
fn bulk_replace_input() -> BulkLoadInput<String> {
    let rows: Vec<(u64, String)> = (1u64..=3).map(|i| (i, format!("bulk{i}"))).collect();
    BulkLoadInput::Replace(BulkSource::sorted_vec(rows))
}

fn run_cell(a: AState, b: BOp) -> CellOutcome {
    let store = mw_store();
    seed(&store);

    // ── A begins and does its thing ─────────────────────────────────────
    let mut wtx = store.begin_write(None).unwrap();
    match a {
        AState::OpenOnly => {
            let t = wtx.open_table::<String>(T).unwrap();
            let _ = t.len();
        }
        AState::OpenWrite => {
            wtx.open_table::<String>(T)
                .unwrap()
                .update(1, "from_a".to_string())
                .unwrap();
        }
        AState::OpenDdl => {
            wtx.open_table::<String>(T)
                .unwrap()
                .define_index("by_val", IndexKind::Unique, |s: &String| s.clone())
                .unwrap();
        }
        AState::Delete => {
            assert!(wtx.delete_table(T));
        }
        AState::DeleteRecreate => {
            assert!(wtx.delete_table(T));
            let t = wtx.open_table::<String>(T).unwrap();
            let _ = t.len();
        }
        AState::DeleteRecreateWrite => {
            assert!(wtx.delete_table(T));
            wtx.open_table::<String>(T)
                .unwrap()
                .insert("recreated".to_string())
                .unwrap();
        }
        AState::WriteDeleteRecreate => {
            wtx.open_table::<String>(T)
                .unwrap()
                .update(1, "from_a".to_string())
                .unwrap();
            assert!(wtx.delete_table(T));
            let t = wtx.open_table::<String>(T).unwrap();
            let _ = t.len();
        }
    }

    // ── B commits ───────────────────────────────────────────────────────
    match b {
        BOp::BulkReplace => {
            store
                .bulk_load::<String>(T, bulk_replace_input(), BulkLoadOptions::default())
                .unwrap();
        }
    }

    // ── A commits; observe ──────────────────────────────────────────────
    let commit = wtx.commit();

    let rtx = store.begin_read(None).unwrap();
    let (rows_after, table_present) = match rtx.open_table::<String>(T) {
        Ok(t) => (
            t.iter().map(|(k, v)| (*k, v.clone())).collect::<Vec<_>>(),
            true,
        ),
        Err(_) => (Vec::new(), false),
    };
    CellOutcome { commit, rows_after, table_present }
}

/// Runs a cell and asserts both halves of the oracle.
fn check_cell(a: AState, b: BOp, expect: Expect) {
    let out = run_cell(a, b);
    let label = format!("{a:?} x {b:?}");

    // (i) the outcome is the one the rules predict
    match expect {
        Expect::Commits => assert!(
            out.commit.is_ok(),
            "{label}: expected commit, got {:?}",
            out.commit
        ),
        Expect::Conflicts => assert!(
            matches!(out.commit, Err(Error::WriteConflict { .. })),
            "{label}: expected WriteConflict, got {:?}",
            out.commit
        ),
        Expect::DdlConflicts => assert!(
            matches!(out.commit, Err(Error::IndexDdlConflict { .. })),
            "{label}: expected IndexDdlConflict, got {:?}",
            out.commit
        ),
    }

    // (ii) THE UMBRELLA PROPERTY: B committed Ok, so B's effect is never
    // silently reverted. B installed bulk1..bulk3 over the seed; A may have
    // modified or removed rows on top of that, but the seed must never come
    // back, because that would mean A's pre-B clone was installed wholesale.
    if out.table_present {
        for (k, v) in &out.rows_after {
            assert!(
                !v.starts_with("seed"),
                "{label}: pre-B row ({k}, {v:?}) resurrected — B's committed \
                 bulk_load was silently reverted. rows={:?} commit={:?}",
                out.rows_after,
                out.commit
            );
        }
    }
}

/// Bug 1 (fixed in b990951): a transaction that opened the table and wrote
/// nothing reinstated its pre-`bulk_load` clone. Commit returned `Ok`.
#[test]
fn open_only_does_not_revert_a_bulk_replace() {
    check_cell(AState::OpenOnly, BOp::BulkReplace, Expect::Commits);
}

/// Bug 2 (fixed in b990951): same, for a DDL-only transaction — `define_index`
/// records into `ddl_tables`, never into `write_set`, so it slipped past
/// `validate_write_set` exactly like a write-free one.
#[test]
fn ddl_only_over_a_bulk_replace_fails_loudly() {
    check_cell(AState::OpenDdl, BOp::BulkReplace, Expect::DdlConflicts);
}
```

- [ ] **Step 2: Run to verify it compiles and passes**

Run: `cargo test --features persistence,fulltext --test table_lifecycle_races`
Expected: 2 passed. If `update` or `define_index` signatures differ, **read the real ones in `src/store.rs`** and adjust — do not guess.

- [ ] **Step 3: Prove the umbrella check has teeth**

Temporarily revert the `has_concurrent` predicate in `src/store.rs` — find the `concurrent_flags` computation and drop the `|| cws.deleted_tables.contains(n)` term, restoring `&& cws.tables.contains_key(n)` alone.

Run: `cargo test --features persistence,fulltext --test table_lifecycle_races`
Expected: **both tests fail**, `open_only_...` on the umbrella assertion naming a resurrected `seed` row.

Restore `src/store.rs` exactly (`git checkout -- src/store.rs`); confirm `git status` is clean and the tests pass again. **Paste the failure output into your report.**

- [ ] **Step 4: Commit**

```bash
git add tests/table_lifecycle_races.rs
git commit -m "test(races): pairwise table-lifecycle harness + the two bulk_load cells

The umbrella property is the point: B committed Ok, so B's effect is never
silently reverted. Verified by dropping the deleted_tables term from
has_concurrent and watching the seed rows come back."
```

---

### Task 2: Complete axis A against the bulk-install column

**Files:**
- Modify: `tests/table_lifecycle_races.rs`

**Interfaces:**
- Consumes: `AState`, `BOp`, `Expect`, `check_cell` from Task 1.
- Produces: `BOp::BulkDelta`, and a `bulk_delta_input()` helper.

- [ ] **Step 1: Add the Delta variant**

Extend `BOp` with `BulkDelta`, add the helper, and extend `run_cell`'s `match b`:

```rust
/// B's Delta: inserts id 4, updates id 1, deletes id 2 — so it touches rows
/// A may also have touched.
fn bulk_delta_input() -> BulkLoadInput<String> {
    BulkLoadInput::Delta(ultima_db::BulkDelta {
        inserts: vec![(4, "delta4".to_string())],
        updates: vec![(1, "delta1".to_string())],
        deletes: vec![2],
    })
}
```

```rust
        BOp::BulkDelta => {
            store
                .bulk_load::<String>(T, bulk_delta_input(), BulkLoadOptions::default())
                .unwrap();
        }
```

**The umbrella check needs widening for Delta**, because a Delta leaves seed rows in place by design — `seed1` at id 1 becomes `delta1`, but id 3 (if any) is untouched. Change the marker test from "no row starts with `seed`" to a per-`BOp` expectation:

```rust
/// What must be true of the table after B's effect, regardless of A.
fn b_effect_intact(b: BOp, rows: &[(u64, String)]) -> Result<(), String> {
    match b {
        // Replace installed exactly bulk1..bulk3 over the seed. Any surviving
        // `seed` row means A's pre-B clone was installed wholesale.
        BOp::BulkReplace => rows
            .iter()
            .find(|(_, v)| v.starts_with("seed"))
            .map_or(Ok(()), |(k, v)| {
                Err(format!("pre-B row ({k}, {v:?}) resurrected"))
            }),
        // Delta updated id 1 and deleted id 2. If id 1 reads `seed1` again, or
        // id 2 is back as `seed2`, B's delta was reverted.
        BOp::BulkDelta => {
            for (k, v) in rows {
                if *k == 1 && v == "seed1" {
                    return Err("delta's update to id 1 was reverted".to_string());
                }
                if *k == 2 && v == "seed2" {
                    return Err("delta's delete of id 2 was reverted".to_string());
                }
            }
            Ok(())
        }
    }
}
```

and call it from `check_cell` in place of the inline loop:

```rust
    if out.table_present
        && let Err(why) = b_effect_intact(b, &out.rows_after)
    {
        panic!(
            "{label}: {why} — B committed Ok and was silently reverted. \
             rows={:?} commit={:?}",
            out.rows_after, out.commit
        );
    }
```

- [ ] **Step 2: Add the remaining cells for both bulk columns**

```rust
/// A wrote rows and B replaced the table under it: A's writes were decided
/// against contents that no longer exist.
#[test]
fn write_over_a_bulk_replace_conflicts() {
    check_cell(AState::OpenWrite, BOp::BulkReplace, Expect::Conflicts);
}

/// Bug 4 (fixed in dbd56d4): a delete racing a wholesale install must
/// conflict, not silently drop the freshly loaded data.
#[test]
fn delete_over_a_bulk_replace_conflicts() {
    check_cell(AState::Delete, BOp::BulkReplace, Expect::Conflicts);
}

#[test]
fn delete_recreate_over_a_bulk_replace_conflicts() {
    check_cell(AState::DeleteRecreate, BOp::BulkReplace, Expect::Conflicts);
}

#[test]
fn delete_recreate_write_over_a_bulk_replace_conflicts() {
    check_cell(AState::DeleteRecreateWrite, BOp::BulkReplace, Expect::Conflicts);
}

/// Bug 5 (fixed in 68cd794): stale write-set digests made this behave
/// differently from `DeleteRecreate` purely because A wrote first.
#[test]
fn write_delete_recreate_over_a_bulk_replace_conflicts() {
    check_cell(AState::WriteDeleteRecreate, BOp::BulkReplace, Expect::Conflicts);
}

#[test]
fn open_only_does_not_revert_a_bulk_delta() {
    check_cell(AState::OpenOnly, BOp::BulkDelta, Expect::Commits);
}

#[test]
fn ddl_only_over_a_bulk_delta_fails_loudly() {
    check_cell(AState::OpenDdl, BOp::BulkDelta, Expect::DdlConflicts);
}

#[test]
fn write_over_a_bulk_delta_conflicts() {
    check_cell(AState::OpenWrite, BOp::BulkDelta, Expect::Conflicts);
}

#[test]
fn delete_over_a_bulk_delta_conflicts() {
    check_cell(AState::Delete, BOp::BulkDelta, Expect::Conflicts);
}

#[test]
fn delete_recreate_over_a_bulk_delta_conflicts() {
    check_cell(AState::DeleteRecreate, BOp::BulkDelta, Expect::Conflicts);
}

#[test]
fn delete_recreate_write_over_a_bulk_delta_conflicts() {
    check_cell(AState::DeleteRecreateWrite, BOp::BulkDelta, Expect::Conflicts);
}

#[test]
fn write_delete_recreate_over_a_bulk_delta_conflicts() {
    check_cell(AState::WriteDeleteRecreate, BOp::BulkDelta, Expect::Conflicts);
}
```

- [ ] **Step 3: Run and reconcile**

Run: `cargo test --features persistence,fulltext --test table_lifecycle_races`

**Some of these expectations may be wrong.** They are derived from the spec's rules, not from observation, which is the point — a cell that fails is either a bug or a rule that needs refining. For each failure, decide which and **report it explicitly**. Do not change an expectation to match observed behaviour without saying so and giving the reasoning.

- [ ] **Step 4: Commit**

```bash
git add tests/table_lifecycle_races.rs
git commit -m "test(races): complete axis A against bulk_load Replace and Delta"
```

---

### Task 3: The concurrent-transaction column

**Files:**
- Modify: `tests/table_lifecycle_races.rs`

**Interfaces:**
- Consumes: everything from Task 2.
- Produces: `BOp::TxWrite`, `BOp::TxDelete`.

This column carries bugs 3 and 5.

- [ ] **Step 1: Add the two variants**

```rust
    /// Another transaction writes a row.
    TxWrite,
    /// Another transaction deletes the table.
    TxDelete,
```

```rust
        BOp::TxWrite => {
            let mut b = store.begin_write(None).unwrap();
            b.open_table::<String>(T)
                .unwrap()
                .update(2, "from_b".to_string())
                .unwrap();
            b.commit().unwrap();
        }
        BOp::TxDelete => {
            let mut b = store.begin_write(None).unwrap();
            assert!(b.delete_table(T));
            b.commit().unwrap();
        }
```

Extend `b_effect_intact`:

```rust
        // B updated id 2. Reading `seed2` there means B was reverted.
        BOp::TxWrite => rows
            .iter()
            .find(|(k, v)| *k == 2 && v == "seed2")
            .map_or(Ok(()), |_| Err("B's update to id 2 was reverted".to_string())),
        // B deleted the table. Presence is checked by the caller, not here —
        // see `table_present` in the cell expectations below.
        BOp::TxDelete => Ok(()),
```

- [ ] **Step 2: Add a table-presence expectation**

`TxDelete` cells need an assertion `b_effect_intact` cannot express. Add a third field to the oracle:

```rust
/// Whether the table must exist after the race, for cells where that is the
/// property under test. `None` = not asserted.
fn expected_presence(a: AState, b: BOp) -> Option<bool> {
    match (a, b) {
        // Bug 3 (fixed in dbd56d4): a write-free open must not resurrect a
        // table a concurrent delete removed.
        (AState::OpenOnly, BOp::TxDelete) => Some(false),
        // Pinned choice, 2026-08-05: a write-free recreate does not survive a
        // concurrent delete. See
        // `delete_then_reopen_without_writing_leaves_a_concurrently_deleted_table_absent`.
        (AState::DeleteRecreate, BOp::TxDelete) => Some(false),
        // Bug 5: same shape, but A wrote before deleting. Must match the line
        // above — that it did not was the bug.
        (AState::WriteDeleteRecreate, BOp::TxDelete) => Some(false),
        _ => None,
    }
}
```

and assert it in `check_cell` when `Some`.

- [ ] **Step 3: Add the cells**

```rust
#[test]
fn open_only_does_not_revert_a_concurrent_write() {
    check_cell(AState::OpenOnly, BOp::TxWrite, Expect::Commits);
}

#[test]
fn write_over_a_concurrent_write_to_a_different_row_commits() {
    // Key-level OCC: A updates id 1, B updates id 2 — disjoint, both land.
    check_cell(AState::OpenWrite, BOp::TxWrite, Expect::Commits);
}

#[test]
fn ddl_only_over_a_concurrent_write_fails_loudly() {
    check_cell(AState::OpenDdl, BOp::TxWrite, Expect::DdlConflicts);
}

#[test]
fn delete_over_a_concurrent_write_conflicts() {
    check_cell(AState::Delete, BOp::TxWrite, Expect::Conflicts);
}

/// Bug 3 (fixed in dbd56d4): commit returned Ok and the deleted table came
/// back with its pre-delete contents.
#[test]
fn open_only_does_not_resurrect_a_concurrently_deleted_table() {
    check_cell(AState::OpenOnly, BOp::TxDelete, Expect::Commits);
}

/// Bug 5 (fixed in 68cd794): identical to the line below except that A wrote
/// before deleting; the stale digests made it return Err where this returns Ok.
#[test]
fn write_delete_recreate_matches_delete_recreate_against_a_concurrent_delete() {
    check_cell(AState::WriteDeleteRecreate, BOp::TxDelete, Expect::Commits);
    check_cell(AState::DeleteRecreate, BOp::TxDelete, Expect::Commits);
}
```

- [ ] **Step 4: Surface the cells that need a ruling**

The spec names three. Add them as `#[ignore]`d tests carrying the question, so they are visible and not silently omitted:

```rust
/// UNRESOLVED — needs a ruling before it is pinned.
///
/// Delete vs delete: non-conflicting under SnapshotIsolation, but SSI's
/// `validate_read_set` aborts it. Pre-existing and undocumented. Whether SI
/// should conflict here is a design question, not a bug report.
#[test]
#[ignore = "unresolved: see the doc comment; do not pin until ruled on"]
fn delete_vs_concurrent_delete_outcome_is_unruled() {
    check_cell(AState::Delete, BOp::TxDelete, Expect::Commits);
}
```

Run each `#[ignore]`d cell with `-- --ignored`, record what it currently does in the doc comment, and **do not** convert it to a live test.

- [ ] **Step 5: Run, reconcile, commit**

Run: `cargo test --features persistence,fulltext --test table_lifecycle_races`
Run: `cargo test --features persistence,fulltext --test table_lifecycle_races -- --ignored`

```bash
git add tests/table_lifecycle_races.rs
git commit -m "test(races): concurrent-transaction column; unresolved cells marked ignored"
```

---

### Task 4: The multi-table install column

**Files:**
- Modify: `tests/table_lifecycle_races.rs`

**Interfaces:**
- Consumes: everything from Task 3.
- Produces: `BOp::BulkBatch`, `BOp::StreamInstallDrop`.

`StreamInstallDrop` needs more setup than the others — a source store, a drained stream, and `register_table` on the destination — and is `persistence`-gated. Put it behind `#[cfg(feature = "persistence")]` and give it its own `run_cell` arm.

- [ ] **Step 1: Add `BulkBatch`**

```rust
        BOp::BulkBatch => {
            let mut batch = store.bulk_load_batch();
            batch
                .add::<String>(T, bulk_replace_input(), Default::default())
                .unwrap();
            batch.commit().unwrap();
        }
```

`BulkLoadBatch::add` rejects `Delta` with `Error::InvalidBulkLoadInput` — Replace only. Confirm `commit`'s exact name in `src/bulk_load.rs` before writing this.

Its `b_effect_intact` arm is the same as `BulkReplace`.

- [ ] **Step 2: Add `StreamInstallDrop`**

Follow the round-trip shape in `tests/snapshot_stream.rs::build_then_install_roundtrips_full_state`: build a source store, seed a *different* table, drain via `src.snapshot_stream(None)`, then install into the subject store with `on_extra_tables: OnExtra::Drop` so `T` is dropped as an extra.

The destination store must `register_table::<String>(T)` for recovery/registry reasons — check whether the subject store in `run_cell` needs it, and add it in `mw_store()` under `#[cfg(feature = "persistence")]` if so.

- [ ] **Step 3: Add the cells for both, run, reconcile**

Mirror Task 2's cell set for `BulkBatch`. For `StreamInstallDrop`, the interesting cells are `OpenOnly` (does a write-free open resurrect a keep-set-dropped table?) and `Delete` (does deleting an already-dropped table conflict?). **`OpenOnly × StreamInstallDrop` is one of the spec's three unresolved cells** — mark it `#[ignore]` with the question, as in Task 3 Step 4.

- [ ] **Step 4: Commit**

```bash
git add tests/table_lifecycle_races.rs
git commit -m "test(races): bulk_load_batch and snapshot-stream-install columns"
```

---

### Task 5: Calibration — re-find all five bugs

**Files:**
- Modify: `tests/table_lifecycle_races.rs` (a doc comment recording the result)

This is the acceptance gate for the whole suite. **A matrix that passes against the buggy code proves nothing.**

- [ ] **Step 1: Revert each fix and record which cells go red**

For each of the three commits, revert *only* its production hunk in `src/store.rs` (not its tests), run the suite, record which named tests fail, and restore exactly.

| commit | hunk to revert | bugs |
|---|---|---|
| `b990951` | drop `\|\| cws.deleted_tables.contains(n)` from the `concurrent_flags` predicate | 1, 2 |
| `dbd56d4` | (a) restore the `(None, _)` install arm to unconditional; (b) drop `\|\| cws.installed_tables.contains(deleted)` from `validate_write_set`'s second loop | 3, 4 |
| `68cd794` | drop the `write_set.get_mut(name) { digests.clear(); }` block from `delete_table` | 5 |

**All five must be re-found**, each by at least one named cell. If any is not, the matrix has a gap — find the missing cell and add it. Report the mapping from bug → failing cell.

- [ ] **Step 2: Record the calibration in the file**

Add a module-level comment listing bug → cell → the revert that reproduces it, so the next reader can re-run the calibration without re-deriving it.

- [ ] **Step 3: Confirm determinism**

Run the suite 20 times: `for i in $(seq 1 20); do cargo test --features persistence,fulltext --test table_lifecycle_races 2>&1 | grep "^test result"; done`
Expected: 20 identical passing lines, zero flakes.

- [ ] **Step 4: Commit**

```bash
git add tests/table_lifecycle_races.rs
git commit -m "test(races): calibration — all five 2026-08-05 bugs re-found

Each of the three fix commits reverted in turn; the mapping from bug to
failing cell is recorded in the file so it can be re-run without
re-deriving it. 20/20 deterministic."
```

---

### Task 6: The Serializable pass

**Files:**
- Modify: `tests/table_lifecycle_races.rs`

Phase 2 from the spec. Lower expected yield than phase 1 — sequenced last and droppable.

- [ ] **Step 1: Parameterise the store by isolation level**

Change `mw_store()` to `mw_store(iso: IsolationLevel)` and thread it through `run_cell`/`check_cell`. Keep the phase-1 tests on `SnapshotIsolation`.

- [ ] **Step 2: Add the Serializable cells**

Run the same matrix under `IsolationLevel::Serializable`. Expect differences: `validate_read_set` conflicts on `cws.deleted_tables` *before* it consults `tables`, so cells that commit `Ok` under SI may return `Error::SerializationFailure` under SSI. That is not a bug — SSI is strictly stronger.

Where an outcome differs between SI and SSI, **assert both explicitly** rather than loosening the assertion to accept either. A cell that accepts two outcomes tests nothing.

- [ ] **Step 3: Run, reconcile, commit**

```bash
git add tests/table_lifecycle_races.rs
git commit -m "test(races): the same matrix under Serializable isolation"
```

---

## Self-Review

**Spec coverage.** Axis A (7 states) → Task 1 defines all seven; Tasks 2–4 exercise them. Axis B (6 ops) → `BulkReplace` Task 1, `BulkDelta` Task 2, `TxWrite`/`TxDelete` Task 3, `BulkBatch`/`StreamInstallDrop` Task 4. Oracle rules (i) and (ii) → Task 1's `check_cell` and `b_effect_intact`, widened in Tasks 2–3. Cells needing a ruling → Task 3 Step 4 and Task 4 Step 3, as `#[ignore]`d tests. Success criterion 2 (re-find all five) → Task 5. Criterion 5 (determinism) → Task 5 Step 3. Phase 2 → Task 6. Phase 3 (three-way) is explicitly out of scope and has no task, per the spec.

**Known soft spots, flagged rather than hidden.**

1. **The expectation table is a hypothesis.** Tasks 2–4 each say so, and instruct the implementer to report a mismatch as either a bug or a rule refinement rather than silently editing the expectation. That is the plan's main risk and its main potential value.
2. **`StreamInstallDrop` may not fit `run_cell`'s shape** — it needs a second store and `register_table`, and is feature-gated. Task 4 gives it its own arm and warns that `mw_store()` may need a `persistence` variant. If it turns out to distort the harness, splitting it into standalone tests is a reasonable call for the implementer to make and report.
3. **Some `Expect::Conflicts` cells may be `DdlConflicts` or `Ok`.** I derived them from the spec's rules; only Task 5's calibration proves the five known ones. The rest are genuinely unverified until run.
4. **`b_effect_intact` for `TxDelete` returns `Ok(())` unconditionally** — its real assertion lives in `expected_presence`. That split is a little awkward but keeps the row-content and table-existence properties separate, which they are.
