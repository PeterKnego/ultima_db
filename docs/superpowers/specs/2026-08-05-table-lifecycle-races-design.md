# Table-lifecycle race coverage — design

**Date:** 2026-08-05
**Status:** approved design, pre-implementation
**Motivation:** the five MultiWriter OCC bugs fixed on 2026-08-05 (`b990951`,
`dbd56d4`, `68cd794`), every one found by reading rather than by a test.

## The problem, stated from evidence

Five bugs landed in one sitting, all in MultiWriter commit-path handling of
*table*-level operations:

1. A write-free `open_table` reverted a concurrent `bulk_load`.
2. A DDL-only transaction did the same.
3. A write-free `open_table` resurrected a concurrently deleted table.
4. `delete_table` did not conflict with a concurrent install, dropping fresh
   data — and a write-free delete-and-recreate silently *lost the delete*.
5. `delete_table` left stale write-set digests, making (3)'s pinned semantic
   conditional on whether the transaction happened to write first.

Four of the five were silent: the commit returned `Ok`, with no conflict and no
error. All five share one root — `validate_write_set` and the Phase 2 installer
each ask *"did anything concurrent touch this table?"* of **different fields** of
`CommittedWriteSet`, so a `bulk_load` (recorded only in `deleted_tables`, with
`tables` left empty) was invisible to whichever one consulted `tables`.

**Nothing in the repo tests this surface.** The Elle harness
(`autobench/src/bin/elle-history.rs`) generates exactly two operations, `Read`
and `Append`, against rows; `define_index` appears in it once, in *setup* at
line 353, and never as a concurrent operation. `bulk_load` and `delete_table`
never race a commit in any automated test. The coverage boundary is precisely
the bug boundary: rows are checked, tables are not.

Reading found these five. Reading will not find the sixth.

## Approach

**A deterministic pairwise race matrix.** Every one of the five bugs was a
two-party race, and every reproducer was *sequential*:

```rust
let mut wtx = store.begin_write(None)?;   // A begins, does something
store.bulk_load::<String>("t", …)?;       // B commits
let res = wtx.commit();                   // A commits — assert here
```

No threads, no scheduler, no timing. That is the single most important design
fact: this is a deterministic table test. It is reproducible on the first run,
debuggable under a breakpoint, and immune to the flakiness that has dogged the
one existing concurrency test in this repo
(`concurrent_same_table_overlapping_keys_with_retry`, ~1 in 5).

### Axis A — the in-flight transaction's state

| id | A does |
|---|---|
| `A1` | `open_table` only — writes nothing |
| `A2` | `open_table` + one row write |
| `A3` | `open_table` + `define_index` (DDL, no row write) |
| `A4` | `delete_table` |
| `A5` | `delete_table` + reopen (fresh, empty), writes nothing |
| `A6` | `delete_table` + reopen + one row write |
| `A7` | row write + `delete_table` + reopen, writes nothing |

`A7` exists because bug 5 was exactly the difference between it and `A5`.

### Axis B — the concurrently committed operation

| id | B does |
|---|---|
| `B1` | `Store::bulk_load` `Replace` |
| `B2` | `Store::bulk_load` `Delta` |
| `B3` | `Store::bulk_load_batch` (multi-table atomic install) |
| `B4` | another transaction's row write |
| `B5` | another transaction's `delete_table` |
| `B6` | `install_snapshot_stream` with `InstallOptions::on_extra_tables = OnExtra::Drop` |

`B6` is included because a keep-set drop is a *deletion* that reaches
`CommittedWriteSet::deleted_tables` by a different route than `delete_table`,
and the `installed_tables` fix deliberately excludes it.

7 × 6 = 42 cells, of which roughly 35 are meaningful. Today's five bugs occupy
five of them.

## The oracle

This is the part that decides whether the suite is worth building. A table of
golden values recording current behaviour would be a change-detector, not a
correctness test — and this project has repeatedly rejected tests that cannot
fail for the right reason.

Each cell asserts two things.

**(i) The commit outcome is in that cell's allowed set**, derived from these
rules rather than from observation:

1. If A contributed **nothing** to table `T` — no row write, no DDL — then A
   must not affect `T`. B's effect stands.
2. If A **wrote rows** to `T` and B replaced or deleted `T` → `WriteConflict`.
3. If A holds **DDL** on `T` and B touched `T` → `IndexDdlConflict`.
4. If A **deleted** `T` and B *installed* `T` → conflict. (B *deleting* `T` is
   not a conflict: both parties want it gone.)
5. **Disjoint tables** → both commit, both effects visible.

**(ii) The umbrella property**, which is where the value is:

> **Anything that committed `Ok` has its full effect visible afterwards.
> Nothing that returned `Ok` may be silently reverted.**

Rule (ii) alone would have caught all five bugs. It is checked on every cell,
including the ones whose expected outcome is a conflict — a conflicting A must
leave B's effect entirely intact.

### Cells that need a ruling before they are pinned

Some outcomes are *arguably* correct rather than clearly correct. Pinning those
without a decision freezes an accident. Known in advance:

- **`A4`/`A5` × `B5`** (delete vs delete). Non-conflicting under `SnapshotIsolation`
  today, but SSI's `validate_read_set` aborts it. Pre-existing and undocumented.
- **`A5` × `B5`** — the recreate is dropped, so the table ends absent. That was a
  deliberate choice on 2026-08-05, and is pinned by
  `delete_then_reopen_without_writing_leaves_a_concurrently_deleted_table_absent`.
- **`A1` × `B6`** — whether a write-free open should resurrect a keep-set-dropped
  table. The `installed_tables` fix excludes keep-set drops from conflicting,
  and the `(None, _)` skip means the table stays dropped, but the two mechanisms
  were decided independently.

The implementer must **surface these rather than choose**, and no such cell is
pinned until a ruling exists.

## Scope

**In:** `WriterMode::MultiWriter`; `IsolationLevel::SnapshotIsolation` for
phase 1, `Serializable` for phase 2; the 42-cell matrix; `u64`-keyed `String`
tables (the key type is not the variable under test).

**Out, deliberately:**
- **Not a threaded stress test.** The deterministic form is strictly better for
  this bug class: it is exhaustive over the matrix, reproducible, and adds no
  flake. A threaded harness would cover interleavings the sequential form
  cannot, but none of the five bugs needed one.
- **Not an Elle extension.** Elle checks list-append histories against a
  consistency model; table-lifecycle operations do not fit it. Forcing them in
  would weaken the row checker.
- **Not three-way races.** Deferred to phase 3, and only if phase 1 finds a bug
  in a cell that pairwise reasoning called safe — which would be the evidence
  that pairwise coverage is insufficient.
- **Not `SingleWriter`.** The mechanisms under test are MultiWriter-only;
  SingleWriter refuses a concurrent install with `WriterBusy`.

## Success criteria

1. The matrix is complete and every live cell has a stated expected outcome
   traceable to a rule above, not to observed behaviour.
2. **The five known bugs are re-found.** Reverting any one of the three fix
   commits must turn a specific, named cell red. This is the calibration
   requirement, and it is not optional — a suite that passes against the buggy
   code proves nothing. Each of the five must be demonstrated.
3. Rule (ii) is checked on every cell, conflicts included.
4. No cell with an unresolved ruling is pinned.
5. The suite is deterministic: 20 consecutive runs, zero flakes.

## Risks

- **The matrix encodes an accident.** Mitigated by the ruling requirement and
  by criterion 1 (expected outcomes derive from rules, not observation).
- **Combinatorial busywork.** ~35 cells is enough to be tedious and not enough
  to be interesting on its own. Mitigated by generating the matrix from the two
  axes with a per-cell expectation table, rather than 35 hand-written tests.
- **Phase 2 yields little.** Possible; SSI's differences here are narrow. It is
  sequenced second for that reason and can be dropped without affecting phase 1.
