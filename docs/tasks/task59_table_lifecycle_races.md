# Task 59 — table-lifecycle race coverage

**Delivered:** `tests/table_lifecycle_races.rs` — a deterministic 42-cell
pairwise race matrix over MultiWriter table-lifecycle operations, run at both
isolation levels, with a mutation-calibrated oracle.

**Design history:** `docs/superpowers/specs/2026-08-05-table-lifecycle-races-design.md`
(carries a dated 2026-08-06 correction).

---

## 1. Why this suite exists

On 2026-08-05, five bugs were fixed in the MultiWriter commit path, all in the
handling of *table*-level operations (`b990951`, `dbd56d4`, `68cd794`):

1. A write-free `open_table` reverted a concurrent `bulk_load`.
2. A DDL-only transaction did the same.
3. A write-free `open_table` resurrected a concurrently deleted table.
4. `delete_table` did not conflict with a concurrent install, dropping freshly
   loaded data — and a write-free delete-and-recreate silently lost the delete.
5. `delete_table` left stale write-set digests, making (3)'s pinned semantic
   conditional on whether the transaction happened to write before deleting.

**Four of the five were silent**: `commit()` returned `Ok`, no conflict, no
error, data gone. And **all five were found by reading**, because nothing in the
repo executed this surface. The Elle harness generates exactly two operations,
`Read` and `Append`, against *rows*; `bulk_load` and `delete_table` never raced a
commit in any automated test. The coverage boundary was precisely the bug
boundary: rows were checked, tables were not.

### The origin, which is the part worth remembering

All five trace to `2769202` (2026-06-12) — *itself* the fix for this same class
of bug. That commit brought bulk-load installs into the concurrency machinery,
and it fixed the case where the in-flight writer had **written**. It then
deliberately exempted the write-free case, in its own words:

> The deleted-tables check now skips empty write-set entries, so a read-only
> open no longer conflicts.

That is correct reasoning about the **read** side: a transaction that only read a
table has no update to lose, and conflicting it would be a spurious abort. What
it missed is that the same transaction still goes on to **write its stale
snapshot back** in Phase 2 of commit. The exemption was sound for validation and
unsound for installation, and nothing connected the two.

The bugs then sat latent for roughly **eight weeks**, through a period of active
development on the commit path, and were found only when someone re-read it.

**The lesson generalises past this suite:** a fix that exempts a case from one
half of a two-half mechanism has to state what the other half does with that
case. `2769202` is a careful commit with a detailed message; it still shipped
five silent-data-loss bugs, because the exemption was reasoned about locally.

## 2. The root shape

One sentence covers all five:

> `validate_write_set` and the Phase 2 installer each ask *"did anything
> concurrent touch this table?"* of **different fields** of `CommittedWriteSet`.

A `bulk_load` is recorded only in `deleted_tables` (plus `installed_tables`),
with `tables` left **empty** — because it never opens a table and writes rows, it
substitutes the whole `Arc<dyn MergeableTable>`. So a load is invisible to
whichever half consults `tables`. An ordinary transaction's commit is the exact
opposite: `tables` names the table, and `deleted_tables`/`installed_tables` are
empty. A `delete_table` is a third shape, and a snapshot-stream keep-set drop is
a fourth — it lands in `deleted_tables` but deliberately **not**
`installed_tables` ("The drops are removals, not installs", `src/store.rs:1867`),
which is the only place in the codebase where those two fields disagree about a
table.

Four encodings of "something happened to this table", two consumers, and no
single place that says which field means what to whom. That is the defect
generator, and it is why the matrix's `BOp` axis is built out of *encodings*
rather than out of API surface.

## 3. Design: deterministic, sequential, pairwise

Every one of the five bugs was a two-party race, and every reproducer was
**sequential**:

```rust
let mut wtx = store.begin_write(None)?;   // A begins, does something
store.bulk_load::<String>("t", …)?;       // B commits
let res = wtx.commit();                   // A commits — assert here
```

No threads, no scheduler, no sleeps, no timing. The axes:

- **Axis A** (7 states) — what the in-flight transaction did before B committed:
  open-only, open+write, open+DDL, delete, delete+recreate,
  delete+recreate+write, write+delete+recreate. The last exists because bug 5
  was exactly the difference between it and delete+recreate.
- **Axis B** (6 ops) — what committed concurrently: `bulk_load` Replace,
  `bulk_load` Delta, `bulk_load_batch` (multi-table atomic), another
  transaction's row write, another transaction's `delete_table`, and
  `install_snapshot_stream` with `OnExtra::Drop`.

7 × 6 = 42 cells, each at 2 isolation levels = 84 cell-runs.

**The determinism is not a stylistic preference, it is the main result.** This
suite is **80/80 clean** across 20 consecutive runs × 2 feature sets × 2 passes.
The repo's one threaded concurrency test,
`tests/store_integration.rs::concurrent_same_table_overlapping_keys_with_retry`,
flakes roughly **1 in 5**. A sequential harness that reproduces the whole bug
class is strictly better than a threaded one that reproduces it sometimes — and
none of the five needed an interleaving a sequential form cannot produce.

## 4. The oracle — the actual deliverable

The cell count is not the deliverable; a table of golden values recording current
behaviour would be a change-detector. Each cell asserts three things.

**(i) The commit outcome**, derived from five stated rules rather than from
observation — e.g. "if A contributed nothing to `T`, B's effect stands"; "if A
deleted `T` and B *installed* `T`, conflict (B *deleting* `T` is not a conflict:
both parties want it gone)".

**(ii) The umbrella property**, which is where the value is:

> **Anything that committed `Ok` has its full effect visible afterwards. Nothing
> that returned `Ok` may be silently reverted.**

Checked on **every** cell, conflicts included — a conflicting A must leave B's
effect entirely intact. Rule (ii) alone would have caught all five bugs.

**(iii) Table existence**, where a specific ruling pinned it, each arm carrying
the citation that decided it.

### The umbrella was twice found one-directional, and mutation is what found it

This is the part to carry forward, because it is how the oracle earned trust:

1. **First version checked only the negative half** — "no `seed`-prefixed row is
   present". A partial-merge corruption that dropped *some* of B's rows without
   resurrecting any of A's would sail straight through. Strengthened to assert
   B's rows are present *with B's exact values*, and that no row B did not
   install is present, using the fact that both bulk variants hand the installer
   a fully materialized row vector.
2. **Then the exemption itself was the hole.** Rows A was entitled to change were
   excluded from B's check — so on exactly the keys A claimed, *nothing was
   asserted by anybody*. Proven by stubbing Phase 2's `merge_keys_from` to a
   no-op: a textbook silent lost update (`commit=Ok`, A's write reverted to the
   seed value) left the whole suite green. Fixed by making the exemption carry
   keys **and values** as one list, so the obligation transfers to A rather than
   being dropped. Two functions would have let the hole reopen the moment one
   drifted; as one list it cannot.

Both holes were invisible to inspection and obvious to a mutation. **Assert on
the exempt set, not just around it.**

## 5. Calibration record

A matrix that passes against the buggy code proves nothing, so the gate is that
reverting each fix turns named cells red. All five were re-found. Full detail
lives in the module docs of `tests/table_lifecycle_races.rs`; the summary:

| bug | fix | revert | SI cells red |
|---|---|---|---|
| 1, 2 | `b990951` | drop `\|\| cws.deleted_tables.contains(n)` from `concurrent_flags` | 11 live + 1 ignored |
| 3 | `dbd56d4` | weaken Phase 2's install guard to `(None, Some(_))` | 3 live + 1 ignored |
| 4 | `dbd56d4` | drop `\|\| cws.installed_tables.contains(deleted)` from `validate_write_set`'s second loop | 9 live |
| 5 | `68cd794` | drop `delete_table`'s `digests.clear()` block | 2 live |

**Restore with `git checkout`, or `touch src/store.rs` after restoring.** Cargo
decides what to rebuild from mtime, so a `cp -p` restore — or any copy from a
backup taken before the mutation — leaves the *mutated* binary in place and the
next run reports the mutation's failures against what looks like clean source.
This has cost at least one reader a run.

### Two structural caveats on those counts

**The counts are not constants.** 9 tracks the number of *installing* columns
(Replace, Delta, Batch); 2 tracks the number of *removing* columns (`TxDelete`,
`StreamInstallDrop`). Add a column in either family and the count moves.
Re-measure rather than trusting the number.

**Mutation A necessarily over-triggers, and that is not a gap.** It makes
`has_concurrent` false, so Phase 2 takes its fast-path `continue` and never
reaches the arms where bug 3 lives. Any cell capable of detecting bug 3 must
therefore also fail under A — A removes the only route to the code B mutates. So
no cell exists that could isolate bug 3 from A, and none needs to be added:
**mutation B is what shows bug 3 has independent coverage.** A reader who sees
only A's 11-cell list should not conclude otherwise.

## 6. The repo-wide finding: a stronger isolation level is not a superset

Phase 2 of the work ran the same 42 cells under `IsolationLevel::Serializable`.
Ten cells diverge, every one `Ok` under SI and `SerializationFailure` under SSI:
`OpenOnly` against all six columns, and the two recreate states against the two
removing columns. The mechanism is `validate_read_set`'s deleted-table arm
(`src/store.rs:4541`), which aborts unconditionally and is consulted before
either key-comparing arm.

That much is unsurprising — SSI is strictly stronger, and an abort where SI
commits is correct behaviour. **What is surprising, and what generalises:**

> **Reverting bug 3's fix reddens three `SnapshotIsolation` cells and *zero*
> `Serializable` cells.**

Every cell that can observe bug 3 is an `OpenOnly` cell, and under SSI those
abort in `validate_read_set` **before Phase 2 runs at all**. The transaction that
would have resurrected the table never reaches the defective code. The stronger
isolation level *masks the bug outright*, at exactly the cells built to catch it.

Bug 4 is masked **partially** by the same mechanism, and a raw count of reds
hides it: reverting its fix reddens nine Serializable cells, but only the three
`Delete` ones exhibit bug 4's actual signature, the silent `Ok(4)`. The other six
are the two recreate states, which SSI aborts on their read of the recreated
table before the missing write-set term can produce a wrong commit — red because
the *expected* value was `WriteConflict`, without ever exhibiting the data loss.

Mutation A shows the same shape in miniature: four of its eleven SI cells are
`OpenOnly` and contribute nothing under SSI; all five SSI cells it does redden
are `OpenDdl`, which is checked before the read set.

**Consequences for anyone calibrating a MultiWriter test in this repo:**

- **Do not treat an SSI pass as covering an SI pass.** If the two must be run
  separately, it is the SI one that has to survive.
- **Do not report "N cells went red" as the calibration result.** Report which
  cells failed *for the bug's own signature* versus which merely disagreed with
  an expectation.
- The general rule: SSI aborts a transaction for having **read** something, which
  pre-empts every bug whose mechanism is in what the commit path does *after*
  deciding not to conflict. That is most of this bug class.

## 7. What is not covered

- **Three-way races.** Deferred; every one of the five was pairwise, and the
  trigger for going further would be a bug in a cell that pairwise reasoning
  called safe.
- **Threaded interleavings.** Deliberately out — see §3. A threaded harness would
  cover interleavings the sequential form cannot, but would trade the 80/80
  determinism for the ~1-in-5 flake rate the repo already has an example of.
- **`SingleWriter`.** The mechanisms under test are MultiWriter-only; SingleWriter
  refuses a concurrent install with `WriterBusy`.
- **Non-`u64` keys.** The key type is not the variable under test.
- **Three unruled cells**, carried `#[ignore]`d rather than omitted, so the
  question is visible in the matrix instead of being a hole nobody can see. Each
  is observed at both isolation levels (six tests, three cells), and CI and
  `make test/lifecycle-races` both run the `--ignored` pass — a recorded
  behaviour nothing executes is a comment, not a test.

  | cell | question |
  |---|---|
  | `DeleteRecreateWrite × TxDelete` | `write_set` cannot distinguish the deleted table from the recreated one, so the first loop reads "A wrote rows to a table B deleted" when A wrote rows to a *new* table sharing the name. Bug 5's conflation from the other side: stale digests there, live digests here. Committing is defensible; so is refusing. That is a ruling, not a fix. |
  | `DeleteRecreateWrite × StreamInstallDrop` | The same question through a different `CommittedWriteSet` encoding. Carried separately because a ruling that moved one without the other would be a real divergence, and only two cells can show it. |
  | `OpenOnly × StreamInstallDrop` | Whether a write-free open should survive a snapshot install that dropped the table. Two mechanisms decide it and were decided independently; their agreement is a coincidence worth an explicit decision. SSI does **not** vote here — it aborts before the disputed code runs. |

  A red `--ignored` test does not mean the same thing as a red main test: those
  cells assert an *observation*, not a requirement. Red means "an unruled cell's
  behaviour changed" — re-read the doc comment and decide. Red anywhere else is a
  bug.

## 8. Maintenance notes

- **Ruling on a cell is three edits, not two**: pin it as a live SI test with its
  citation; delete both of its `#[ignore]`d observation tests; remove it from
  `unruled()`. Do the third before the second and an assertion catches you. Do
  the second without the third and the cell is silently dropped from the
  Serializable columns — that direction is not checkable (`#[ignore]` is an
  attribute, no test can enumerate it) and is the one real gap in the harness.
- **`check_cell(a, b, expect)` is a `SnapshotIsolation` wrapper**, kept so that
  parameterising isolation changed **no** phase-1 cell textually. The calibration
  measurements rest on those bodies not having moved since they were measured.
- Adding a `BOp` column means re-measuring **two** calibration tables, not one.
