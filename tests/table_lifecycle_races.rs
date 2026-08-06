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
//! The matrix is complete as of Task 4: 7 [`AState`]s × 6 [`BOp`]s = 42 cells,
//! 39 of them live (two tests pack two cells each) and 3 carried `#[ignore]`d
//! as questions awaiting a ruling. Run those with `-- --ignored` — CI and
//! `make test/lifecycle-races` both do, because a recorded behaviour nothing
//! executes is a comment, not a test, and plain `cargo test` skips all three.
//!
//! Every one of those 42 runs at **both** isolation levels. The tests above the
//! `Phase 2` divider are the `SnapshotIsolation` pass, one test per cell, and
//! are the level all five bugs were found at; the section below it is the
//! `Serializable` pass, one test per [`BOp`] column. Ten cells differ between
//! the two, and [`ssi_expect`] is where that list lives. The `#[ignore]`d cells
//! are ignored at both levels, so the `--ignored` pass is six tests, not three.
//!
//! The ignored count went 4 → 5 → 3: review un-pinned `Delete × StreamInstallDrop`
//! on 2026-08-05, then the ruling of 2026-08-06 pinned it *and* its
//! `TxDelete` twin against `src/store.rs:283-288`, retiring two questions.
//!
//! `docs/tasks/task59_table_lifecycle_races.md` is the canonical record — why
//! the suite exists, the root shape all five bugs share, the oracle's two
//! mutation-found holes, and the calibration caveats. The design history is
//! `docs/superpowers/specs/2026-08-05-table-lifecycle-races-design.md`.
//!
//! # Calibration — measured 2026-08-06
//!
//! A matrix that passes against the buggy code proves nothing, so the gate on
//! this suite is that reverting each fix turns named cells red. All five bugs
//! were re-found. Reproduce with the mutations below applied one at a time to
//! `src/store.rs`, each on its own against an otherwise clean tree:
//!
//! ```text
//! cargo test --features persistence,fulltext --test table_lifecycle_races
//! cargo test --features persistence,fulltext --test table_lifecycle_races -- --ignored
//! ```
//!
//! **Restore with `git checkout`, or `touch src/store.rs` after restoring.**
//! Cargo decides what to rebuild from mtime, so a restore that preserves the
//! original timestamp — `cp -p`, or any copy from a backup taken before the
//! mutation — leaves the *mutated* binary in place and the next run reports the
//! mutation's failures against what looks like clean source. This has cost at
//! least one reader a run. The failure mode is silent in the direction that
//! matters: it makes a clean tree look broken, never the reverse.
//!
//! **The four cell lists below are the `SnapshotIsolation` pass**, unchanged by
//! the Serializable pass — which added no textual change to any SI cell, so the
//! 2026-08-06 measurements still stand as measured. What the Serializable
//! columns add on top of each is in `## What the Serializable pass adds` below;
//! the short version is that it adds cells to three of the four mutations and
//! is **blind to the fourth**.
//!
//! **Mutation A — bugs 1 and 2** (`b990951`). In the `concurrent_flags`
//! predicate (`WriteTx::commit`), drop `|| cws.deleted_tables.contains(n)`, so
//! the flag is computed from `cws.tables` alone. **11 live cells + 1 ignored:**
//!
//! - `open_only_does_not_revert_a_bulk_{replace, delta, batch}` — bug 1's
//!   named cell is the `replace` one; each reports `row 1: B installed …,
//!   found "seed1"`, `commit=Ok(4)`.
//! - `ddl_only_over_a_bulk_{replace, delta, batch}_fails_loudly` — bug 2's
//!   named cell is the `replace` one; `expected IndexDdlConflict, got Ok(4)`.
//! - `ddl_only_over_a_concurrent_delete_fails_loudly`,
//!   `ddl_only_over_a_stream_install_drop_fails_loudly`,
//!   `open_only_does_not_resurrect_a_concurrently_deleted_table`, and **both**
//!   packed `write_delete_recreate_matches_delete_recreate_against_*` (first
//!   line, as a resurrection: `still present as []`, not a `WriteConflict`).
//! - Ignored: `open_only_vs_stream_install_drop_outcome_is_unruled`.
//!
//! The blast radius is wider than the bulk columns because the two *removal*
//! columns (`TxDelete`, `StreamInstallDrop`) also record `T` only in
//! `deleted_tables`. The predicate is what routes both families to the merge
//! slow path, so all five extra cells are the same mechanism, not a second one.
//!
//! **Mutation B — bug 3** (`dbd56d4`). Weaken Phase 2's install guard from
//! `(None, Some(digests)) if !digests.is_empty()` to `(None, Some(_))`.
//! **3 live + 1 ignored:**
//!
//! - `open_only_does_not_resurrect_a_concurrently_deleted_table` — bug 3's
//!   named cell. `T` comes back as `[(1, "seed1"), (2, "seed2")]` with
//!   `commit=Ok(4)`.
//! - both packed `write_delete_recreate_matches_delete_recreate_against_*`,
//!   again as a resurrection (`still present as []`).
//! - Ignored: `open_only_vs_stream_install_drop_outcome_is_unruled`.
//!
//! Restoring the arm to an unconditional `(None, _) => install` instead —
//! the other spelling of the same revert — yields the identical cell list.
//!
//! **Mutations A and B share three cells, and that overlap is structural — not
//! a gap in the matrix.** Mutation A makes `has_concurrent` false, so Phase 2
//! takes the fast-path `continue` at `src/store.rs:4053-4058` and never reaches
//! the two `None` arms at `:4082-4110`, where bug 3 lives. Any cell capable of
//! detecting bug 3 must therefore also fail under A: A removes the only route to
//! the code B mutates. So no cell exists that would isolate bug 3 from A, and
//! none needs to be added — **mutation B is what shows bug 3 has independent
//! coverage**, and a reader who sees only A's 11-cell list should not conclude
//! otherwise.
//!
//! The two `None` arms are cited as a **pair** deliberately, so please do not
//! narrow this to one of them. Bug 3's fix *is* the split between them —
//! `(None, Some(digests)) if !digests.is_empty()` at `:4082-4087` installing
//! wholesale, and `(None, _)` at `:4088-4110` skipping — and mutation B's
//! second spelling ("restore to an unconditional `(None, _)`") collapses the
//! pair back into one arm. Citing only `:4088-4110` would name where the bug
//! lived without showing what its guard distinguishes it *from*, which is the
//! whole content of the fix. (`:4065-4087` appeared here between 2026-08-06 and
//! this correction; it spanned the `match` header and two unrelated `Some` arms
//! and stopped one line short of `(None, _)`.)
//!
//! **Mutation C — bug 4** (`dbd56d4`). Drop
//! `|| cws.installed_tables.contains(deleted)` from `validate_write_set`'s
//! second loop. **Exactly 9 live cells, 0 ignored** — the three
//! delete-flavoured [`AState`]s (`Delete`, `DeleteRecreate`,
//! `WriteDeleteRecreate`) against each of the three *installing* columns
//! (`BulkReplace`, `BulkDelta`, `BulkBatch`), all `expected WriteConflict, got
//! Ok(4)`. Bug 4's named cell is `delete_over_a_bulk_replace_conflicts`. Note
//! `delete_recreate_write_over_a_bulk_*` stays **green** and the
//! `StreamInstallDrop` column is untouched — see
//! [`delete_recreate_write_over_a_bulk_replace_conflicts`] for why neither is a
//! gap. The count tracks the number of installing columns; re-measure it if one
//! is added.
//!
//! **Mutation D — bug 5** (`68cd794`). Drop the
//! `if let Some(digests) = self.write_set.get_mut(name) { digests.clear(); }`
//! block from `delete_table`. **Exactly 2 live cells, 0 ignored** — the two
//! packed `write_delete_recreate_matches_delete_recreate_against_{a_concurrent_delete,
//! a_stream_install_drop}`, and only their **first** line
//! (`WriteDeleteRecreate`), `expected commit, got Err(WriteConflict { table:
//! "t", … })`. As with mutation C, the count is structural: it tracks the
//! number of *removing* columns (`TxDelete`, `StreamInstallDrop`) — the mirror
//! of C's installing ones — so re-measure it rather than trusting it if another
//! removal spelling is added. Neither 9 nor 2 is a constant.
//!
//! ## What the Serializable pass adds
//!
//! Measured 2026-08-06, the same five mutations one at a time. The SI lists
//! above are unchanged in every case; these are the Serializable column tests
//! that go red *in addition*, with the cells each one names.
//!
//! | mutation | SI cells | + Serializable cells |
//! |---|---|---|
//! | A (bugs 1, 2) | 11 live + 1 ignored | **5** — `OpenDdl` against every column but `TxWrite` |
//! | B (bug 3) | 3 live + 1 ignored | **0 — sees nothing** |
//! | C (bug 4) | 9 live | **9** — the same three delete-flavoured states × three installing columns, but see below: only 3 fail *as bug 4* |
//! | D (bug 5) | 2 live | **2** — `WriteDeleteRecreate` against each removing column |
//! | ruling reversal | 4 live tests, 6 cells (2 masked) | **6** — all three delete-flavoured states × both removing columns, none masked |
//!
//! **Mutation B is the one to read.** Bug 3 was a write-free `open_table`
//! resurrecting a deleted table, and every cell that can see it is an `OpenOnly`
//! cell — which under SSI aborts with `SerializationFailure` before Phase 2 runs
//! at all. The stronger isolation level *masks the bug*: the transaction that
//! would have resurrected the table never gets to commit, so the defect is
//! invisible at exactly the cells built to catch it. Mutation A is the same
//! story in miniature — its 11 SI cells include four `OpenOnly` ones that
//! contribute nothing under SSI, and all five SSI cells it does redden are
//! `OpenDdl`, which is checked before the read set and so still reaches the
//! mutated predicate.
//!
//! **Mutation C makes the same point more finely, and a raw count of reds
//! hides it.** Its nine Serializable cells are all red, but they are not all
//! red *for bug 4's reason*. Bug 4 is a delete that fails to conflict with a
//! concurrent install and silently drops the fresh data — its signature is
//! `Ok(4)`. Only the three `Delete` cells show that. The other six —
//! `DeleteRecreate` and `WriteDeleteRecreate` against the three installing
//! columns — report `Err(SerializationFailure)`, because those two states read
//! the table they recreated and SSI aborts them on the read before the missing
//! write-set term can produce a wrong commit. They are red only because the
//! *expected* value is `WriteConflict`, so they detect that something changed
//! without exhibiting the data loss. SSI masks bug 4's symptom on two thirds of
//! that column just as it masks bug 3's on all of its own — which is why "nine
//! cells went red under both levels" would be the wrong summary.
//!
//! So **the Serializable pass is not a substitute for the SI pass**, and if the
//! two ever have to be run separately it is the SI one that must survive. That
//! is a general property of this surface rather than a quirk of these
//! mutations: SSI aborts a transaction for having *read* something, which
//! pre-empts every bug whose mechanism is in what the commit path does *after*
//! deciding not to conflict.
//!
//! What it adds where it is not blind is a sharper failure report. The column
//! tests run all seven cells of a column and name every one that failed, so
//! under mutation D the `TxDelete` column prints exactly
//! `WriteDeleteRecreate x TxDelete`, with `DeleteRecreate` conspicuously absent
//! — the halves-moved-apart signal, without the line-swapping the packed SI test
//! needs. Under the ruling reversal the same column prints all three
//! delete-flavoured cells, which is halves-moved-together. See the section
//! below for what that distinction means.
//!
//! ## The discriminator the packed tests exist for
//!
//! Both packed tests assert an *equality*, so what matters is not that they go
//! red but **how**. Measured both ways, on both tests:
//!
//! - **Bug 5's revert (mutation D) moves the halves apart.** Swap the two
//!   `check_cell` lines and re-run: `DeleteRecreate` now passes and
//!   `WriteDeleteRecreate` still fails. That asymmetry — the outcome depending
//!   on whether the transaction happened to write before deleting — *is* bug 5.
//! - **Reversing the 2026-08-06 ruling at the mechanism level moves them
//!   together.** Change `validate_write_set`'s second loop to
//!   `cws.tables.contains_key(deleted) || cws.deleted_tables.contains(deleted)`
//!   (i.e. make a removal conflict like an install, contradicting
//!   `src/store.rs:283-288`) and both halves conflict: swapped, it is the
//!   `DeleteRecreate` line that fails. The equality survives; only the pinned
//!   value changes, `Commits` → `Conflicts`. That revert also turns
//!   `delete_vs_{concurrent_delete, stream_install_drop}_commits` red, which
//!   mutation D does not — a second, independent way to tell the two apart.
//!
//! So a red packed test means "the bug came back" only if the halves moved
//! apart; if they moved together, the ruling changed and these are the cells
//! that record it.
//!
//! Since the Serializable pass landed there is a third way to tell, and it is
//! the quickest: [`serializable_concurrent_delete_column`] and
//! [`serializable_stream_install_drop_column`] run both halves as separate
//! cells and report each by name, so their failure text says outright which
//! moved. Measured on both mutations — D names `WriteDeleteRecreate` alone, the
//! ruling reversal names all three delete-flavoured states. That does not
//! retire the packing: the SI cells are still where the equality is *asserted*,
//! and the SSI columns assert their own values, which happen to be
//! `SerializationFailure` for both halves rather than `Ok`.

use std::collections::{BTreeMap, BTreeSet};

use ultima_db::{
    AddOptions, BulkDelta, BulkLoadInput, BulkLoadOptions, BulkSource, Error, IndexKind,
    IsolationLevel, Store, StoreConfig, WriterMode,
};

/// The table under test. Every [`AState`] acts on this one and no other.
const T: &str = "t";

/// A second table, touched only by the multi-table [`BOp`]s and never by A.
///
/// It is what makes the two new columns distinguishable from the single-table
/// ones. [`BOp::BulkBatch`] installs `T` and `U` in one atomic snapshot, and
/// [`BOp::StreamInstallDrop`] acts on `T` *by omitting it* — the wire stream
/// declares `U` alone, so `T` is dropped as an extra.
const U: &str = "u";

/// Every table [`run_cell`] observes after the race.
const WATCHED: &[&str] = &[T, U];

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
    /// `Store::bulk_load` Delta — inserts id 4, updates id 1, deletes id 2,
    /// so it touches rows A may also have touched.
    BulkDelta,
    /// Another transaction updates id 2 — a row `AState::OpenWrite` does *not*
    /// touch, so the disjoint-key case is the one under test.
    TxWrite,
    /// Another transaction deletes the table. Unlike every other `BOp` this one
    /// leaves `T` absent, which is why the oracle has to ask
    /// [`b_leaves_table`] before it can say what "B's effect intact" means.
    TxDelete,
    /// `Store::bulk_load_batch` — a **multi-table atomic** install of `T` *and*
    /// `U` in one snapshot.
    ///
    /// The second table is not decoration. `Store::bulk_load` with a `Replace`
    /// input *is* `bulk_load_batch` with a single `add` (`src/store.rs:1425-1426`),
    /// so a one-table batch column would run the same code as
    /// [`BOp::BulkReplace`] through a different spelling and assert nothing new.
    /// What only a batch can get wrong is atomicity across tables — A's commit
    /// rebasing onto `latest` and taking `T` back while dropping `U`, or the
    /// reverse — and that is a claim no single-table observation can make.
    BulkBatch,
    /// `install_snapshot_stream` with `InstallOptions::on_extra_tables =
    /// OnExtra::Drop`, from a source store holding only `U`.
    ///
    /// The odd one out in two ways. First, B's act on `T` is an *omission*: the
    /// stream never mentions `T`, and `T` disappears because the keep-set does
    /// not contain it. Second, that removal reaches `CommittedWriteSet` by a
    /// different route than every other delete here — `install_batch_inner`
    /// puts keep-set drops in `deleted_tables` but deliberately **not** in
    /// `installed_tables` ("The drops are removals, not installs",
    /// `src/store.rs:1867`), so this column is the only one where those two
    /// fields disagree about a table.
    #[cfg(feature = "persistence")]
    StreamInstallDrop,
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
    /// A must fail with `Error::SerializationFailure` — reachable only under
    /// [`IsolationLevel::Serializable`], where `validate_read_set` runs.
    ///
    /// Deliberately its own variant rather than a widening of
    /// [`Expect::Conflicts`]: SSI aborts a cell for a different reason than
    /// OCC does (A *read* something a concurrent commit invalidated, not A
    /// *wrote* something it collided with), and the two error values are what
    /// tell a maintainer which validator fired.
    SerializationFails,
}

/// Post-race observation of **every** table in [`WATCHED`], not just `T`.
///
/// Task 1 recorded `rows_after`/`table_present` for `T` alone, which was
/// sufficient while every `BOp` was single-table. Both of Task 4's break that,
/// in opposite directions:
///
/// - [`BOp::BulkBatch`] installs two tables in one snapshot, so "B's effect
///   survived" is a claim about both of them and a single-table check would
///   miss a commit that took `T` back correctly while losing `U`. Measured: a
///   batch that stages `T` only turns **all seven** cells of that column red,
///   committed and conflicted alike, every one of them through
///   [`side_tables_intact`].
/// - [`BOp::StreamInstallDrop`] acts on `T` by *not* streaming it, so `U` is
///   the only positive evidence the install happened at all. Measured against
///   an install made into a silent no-op (`U` left unregistered on the
///   destination, which is the `pending.is_empty()` early return in
///   `install_snapshot_stream`): all **six** tests of that column fail with the
///   side clause on; with it disabled, **five** still fail — caught by outcome
///   (i) or by `T`'s own disposition — and
///   [`delete_vs_stream_install_drop_commits`] passes a do-nothing install
///   outright. A deletes `T`, commits `Ok`, and `T` ends absent, which is
///   exactly what that cell demands; observing `U` is the whole of what it
///   asserts about B.
///
/// Counted in *tests*, not cells: the stream column is seven cells in six
/// tests, because
/// [`write_delete_recreate_matches_delete_recreate_against_a_stream_install_drop`]
/// packs two. An earlier revision of this comment said "four of the six cells"
/// and was wrong twice over — wrong number, wrong unit.
///
/// The sole side-clause-only cell is [`delete_vs_stream_install_drop_commits`],
/// which was briefly `#[ignore]`d and is live again since the 2026-08-06
/// ruling — so that evidence is back on the default CI path. The clause is
/// independently load-bearing through [`BOp::BulkBatch`], whose seven cells are
/// all live and all depend on it.
///
/// [`rows_after`](Self::rows_after) and [`table_present`](Self::table_present)
/// are kept as accessors so every cell Tasks 1–3 wrote reads unchanged.
struct CellOutcome {
    commit: Result<u64, Error>,
    /// Rows of each watched table that exists; an absent table has no entry.
    tables: BTreeMap<&'static str, Vec<(u64, String)>>,
}

impl CellOutcome {
    /// `T`'s rows, or empty when `T` is absent — indistinguishable on their
    /// own, which is why [`table_present`](Self::table_present) exists.
    fn rows_after(&self) -> &[(u64, String)] {
        self.tables.get(T).map_or(&[], Vec::as_slice)
    }

    fn table_present(&self) -> bool {
        self.tables.contains_key(T)
    }
}

fn mw_store(iso: IsolationLevel) -> Store {
    let store = Store::new(
        StoreConfig::builder()
            .writer_mode(WriterMode::MultiWriter)
            .isolation_level(iso)
            .build(),
    )
    .unwrap();
    // `install_snapshot_stream` reconstructs each incoming table through the
    // registry, and `OnUnknown::Drop` (the default) silently discards anything
    // unregistered — leaving `pending` empty, which makes the whole install a
    // no-op that returns `base_version` without touching the store. So `U` must
    // be registered on *both* ends of the stream for `StreamInstallDrop` to act
    // at all, and `b_side_tables`' demand that `U` be present afterwards is what
    // holds it to that.
    //
    // `T` is deliberately left unregistered: `TableRegistry::validate_type`
    // passes on unknown names ("If not registered, we don't enforce — allows
    // non-persistent tables", `src/registry.rs:326`), so nothing needs it, and
    // leaving it out keeps every Tasks 1–3 cell running exactly as it did.
    #[cfg(feature = "persistence")]
    store.register_table::<String>(U).unwrap();
    store
}

/// Seeds `T` with two rows, ids 1 and 2.
fn seed(store: &Store) {
    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table::<String>(T).unwrap();
    t.insert("seed1".to_string()).unwrap();
    t.insert("seed2".to_string()).unwrap();
    wtx.commit().unwrap();
}

/// The table's **complete** contents immediately after B committed, and
/// before A commits.
///
/// Both bulk variants hand the installer a fully materialized row vector
/// (`materialize_rows` / `materialize_delta`), so this is exhaustive — every
/// row present, and no other row present. That exhaustiveness is what lets
/// rule (ii) be checked *positively* (B's rows are there) instead of only
/// negatively (A's rows are not).
///
/// Every value is distinctive, so the umbrella check can tell "B's effect
/// survived" from "A's stale pre-B clone won".
fn b_rows(b: BOp) -> Vec<(u64, String)> {
    match b {
        BOp::BulkReplace => (1u64..=3).map(|i| (i, format!("bulk{i}"))).collect(),
        // Hand-derived, unlike the Replace arm, because a delta's result
        // depends on the fixture: this is `seed()`'s {1: seed1, 2: seed2} with
        // `bulk_delta_input()` applied — update 1, delete 2, insert 4. **Change
        // `seed()` or `bulk_delta_input()` and this must change with them**, or
        // the failure will point at the store instead of at the fixture.
        BOp::BulkDelta => vec![(1, "delta1".to_string()), (4, "delta4".to_string())],
        // Also hand-derived: `seed()` with id 2 updated in place. Keeping id 1
        // at its seed value is deliberate — it is the row `AState::OpenWrite`
        // claims, so the disjoint-key merge has something to get wrong.
        BOp::TxWrite => vec![(1, "seed1".to_string()), (2, "from_b".to_string())],
        // Distinct from the Replace arm on purpose. The two install paths are
        // the same code, so identical values would make a column mix-up
        // invisible; `batch{i}` vs `bulk{i}` makes it a failure.
        BOp::BulkBatch => (1u64..=3).map(|i| (i, format!("batch{i}"))).collect(),
        // Nothing left to enumerate; presence is the whole of B's effect here.
        BOp::TxDelete => Vec::new(),
        // Likewise — but for the opposite reason. `TxDelete` removes `T`
        // explicitly; this removes it by never streaming it.
        #[cfg(feature = "persistence")]
        BOp::StreamInstallDrop => Vec::new(),
    }
}

/// B's effect on the watched tables **other than `T`**: the complete list of
/// those it leaves present, with their complete contents. Any watched table not
/// listed here must be absent afterwards.
///
/// Kept apart from [`b_rows`] because these need none of its exemption
/// machinery. No `AState` opens, writes, or deletes a table other than `T`, so
/// A can never hold a legitimate claim on one — the check is exact in both
/// directions and unconditional on A's outcome, which is a strictly stronger
/// assertion than anything `T` can carry.
///
/// [`side_rows`] feeds the *fixture* from this same list, so the rows B is told
/// to install and the rows the oracle demands cannot drift apart.
fn b_side_tables(b: BOp) -> &'static [(&'static str, &'static [(u64, &'static str)])] {
    match b {
        BOp::BulkBatch => &[(U, &[(1, "batch_u1"), (2, "batch_u2")])],
        #[cfg(feature = "persistence")]
        BOp::StreamInstallDrop => &[(U, &[(1, "streamed_u1"), (2, "streamed_u2")])],
        BOp::BulkReplace | BOp::BulkDelta | BOp::TxWrite | BOp::TxDelete => &[],
    }
}

/// The owned form of one [`b_side_tables`] entry, for handing to an installer.
///
/// Panics rather than returning empty when `name` is not declared: an
/// undeclared side table would make B install rows the oracle then demands be
/// absent, and the resulting failure would name the wrong party.
fn side_rows(b: BOp, name: &str) -> Vec<(u64, String)> {
    b_side_tables(b)
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, rows)| rows.iter().map(|(k, v)| (*k, v.to_string())).collect())
        .unwrap_or_else(|| panic!("{b:?} declares no side table {name:?}"))
}

/// Whether B left `T` in existence.
///
/// Split out from [`b_rows`] because "B installed no rows" and "B removed the
/// table" are the same empty vector and opposite post-conditions: the first
/// demands the table be present and empty, the second demands it be absent.
/// Collapsing them would let `TxDelete` cells pass with the table resurrected
/// but emptied — which is within one row of the shape of bug 3.
fn b_leaves_table(b: BOp) -> bool {
    match b {
        BOp::BulkReplace | BOp::BulkDelta | BOp::TxWrite | BOp::BulkBatch => true,
        BOp::TxDelete => false,
        // A keep-set drop is a removal, exactly like `delete_table` — the
        // difference is only in how it reaches `CommittedWriteSet`.
        #[cfg(feature = "persistence")]
        BOp::StreamInstallDrop => false,
    }
}

/// B's Replace: a wholesale install of `b_rows`, dropping the seed entirely.
fn bulk_replace_input() -> BulkLoadInput<String> {
    BulkLoadInput::Replace(BulkSource::sorted_vec(b_rows(BOp::BulkReplace)))
}

/// B's Delta: inserts id 4, updates id 1, deletes id 2 — so it touches rows
/// A may also have touched.
fn bulk_delta_input() -> BulkLoadInput<String> {
    BulkLoadInput::Delta(BulkDelta {
        inserts: vec![(4, "delta4".to_string())],
        updates: vec![(1, "delta1".to_string())],
        deletes: vec![2],
    })
}

fn run_cell(a: AState, b: BOp, iso: IsolationLevel) -> CellOutcome {
    let store = mw_store(iso);
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
        BOp::BulkDelta => {
            store
                .bulk_load::<String>(T, bulk_delta_input(), BulkLoadOptions::default())
                .unwrap();
        }
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
        BOp::BulkBatch => {
            let mut batch = store.bulk_load_batch();
            for (name, rows) in [
                (T, b_rows(BOp::BulkBatch)),
                (U, side_rows(BOp::BulkBatch, U)),
            ] {
                batch
                    .add::<String>(
                        name,
                        BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
                        AddOptions::default(),
                    )
                    .unwrap();
            }
            // One `commit`, so both tables land in one snapshot version — the
            // atomicity the column exists to test.
            batch.commit(BulkLoadOptions::default()).unwrap();
        }
        #[cfg(feature = "persistence")]
        BOp::StreamInstallDrop => {
            use std::io::Read as _;

            use ultima_db::{InstallOptions, OnExtra};

            // The source holds `U` and nothing else, so the stream declares
            // `U` alone and `T` is an extra at the destination. It is built at
            // the cell's isolation level for uniformity only — nothing races on
            // it, so its `WriteTx` has no concurrent commit to validate against
            // and the choice cannot affect the cell either way.
            let src = mw_store(iso);
            {
                let mut w = src.begin_write(None).unwrap();
                let mut t = w.open_table::<String>(U).unwrap();
                // `put`, not `insert`: the keys are asserted by
                // `b_side_tables`, so they are stated here rather than left to
                // whatever the auto-increment counter happens to hand out.
                for (k, v) in side_rows(BOp::StreamInstallDrop, U) {
                    t.put(k, v).unwrap();
                }
                w.commit().unwrap();
            }
            let mut bytes = Vec::new();
            src.snapshot_stream(None)
                .unwrap()
                .read_to_end(&mut bytes)
                .unwrap();

            store
                .install_snapshot_stream(
                    std::io::Cursor::new(&bytes),
                    InstallOptions {
                        on_extra_tables: OnExtra::Drop,
                        ..Default::default()
                    },
                )
                .unwrap();
        }
    }

    // ── A commits; observe ──────────────────────────────────────────────
    let commit = wtx.commit();

    let rtx = store.begin_read(None).unwrap();
    let mut tables = BTreeMap::new();
    for name in WATCHED {
        if let Ok(t) = rtx.open_table::<String>(*name) {
            tables.insert(
                *name,
                t.iter().map(|(k, v)| (k, v.clone())).collect::<Vec<_>>(),
            );
        }
    }
    CellOutcome { commit, tables }
}

/// The rows A is *entitled* to have changed on top of B, **and the values it
/// must have left there** — and only when A's commit returned `Ok`, since a
/// conflicting A must leave B's effect entirely intact.
///
/// Keys and values are one list on purpose. An earlier revision returned keys
/// alone, and [`effect_intact`] used them only to *exclude* those rows from B's
/// check — so on exactly the keys A claimed, nothing was asserted by anybody.
/// Replacing Phase 2's `merge_keys_from` call with a no-op (a textbook silent
/// lost update: `commit=Ok`, A's `update(1, "from_a")` reverted to `seed1`) left
/// the whole suite green. Splitting these into two functions would let that hole
/// reopen the moment one drifted from the other; as one list it cannot.
///
/// `None` means "A replaced the whole table": it deleted `T`, so per-key claims
/// about B's rows do not survive it. Those states are checked against
/// [`a_whole_table`] instead — `None` is *not* a licence to assert nothing.
fn a_touched(a: AState) -> Option<&'static [(u64, &'static str)]> {
    match a {
        // Contributed nothing to `T`: no row write, and DDL touches no row.
        AState::OpenOnly | AState::OpenDdl => Some(&[]),
        // `update(1, "from_a")`.
        AState::OpenWrite => Some(&[(1, "from_a")]),
        AState::Delete
        | AState::DeleteRecreate
        | AState::DeleteRecreateWrite
        | AState::WriteDeleteRecreate => None,
    }
}

/// A's **complete** post-state, for the states that replace the table wholesale
/// (the ones [`a_touched`] reports `None` for).
///
/// Outer `None` means the table is absent; `Some(rows)` means present with
/// exactly those rows and no others. Every one of these is fully determined by
/// what A did, which is what lets rule (ii) be enforced on A rather than
/// abandoned when A's commit stands.
///
/// It takes `b` for one reason: a write-free *recreate* is not an unconditional
/// promise that the table comes back. When a concurrent commit removed `T` from
/// latest and A contributed no row to it, Phase 2's `(None, _)` arm skips the
/// install and A's recreate is dropped — the pinned choice of 2026-08-05, see
/// [`expected_presence`]. Only `BOp::TxDelete` removes `T` from latest, so only
/// that arm differs; a bulk install leaves `T` present, and Phase 2 then takes
/// the `(Some(_), _)` arm — which is moot in practice, since every recreate
/// cell against a bulk install conflicts before reaching Phase 2 at all.
fn a_whole_table(a: AState, b: BOp) -> Option<Vec<(u64, String)>> {
    match a {
        AState::Delete => None,
        // The reopen does not retract the delete; the table comes back fresh
        // and empty, and A wrote nothing into it — or, against a concurrent
        // delete, does not come back at all.
        AState::DeleteRecreate | AState::WriteDeleteRecreate => {
            if b_leaves_table(b) {
                Some(Vec::new())
            } else {
                None
            }
        }
        // Fresh table, so auto-increment restarts at 1. Unconditional, unlike
        // the arm above: A wrote a row, so Phase 2's `(None, Some(digests))`
        // arm installs it wholesale even when a concurrent commit removed `T`.
        AState::DeleteRecreateWrite => Some(vec![(1, "recreated".to_string())]),
        AState::OpenOnly | AState::OpenWrite | AState::OpenDdl => {
            unreachable!("{a:?} does not replace the table wholesale")
        }
    }
}

/// **Rule (ii)** applied to A: A committed `Ok`, so A's full effect is visible.
///
/// Only meaningful for the wholesale-replacement states, where A's commit
/// determines the table's entire contents.
fn a_effect_intact(a: AState, b: BOp, out: &CellOutcome) -> Result<(), String> {
    match (a_whole_table(a, b), out.table_present()) {
        (None, false) => Ok(()),
        (None, true) => Err(format!(
            "A deleted the table and committed Ok; it is still present as {:?}",
            out.rows_after()
        )),
        (Some(_), false) => {
            Err("A recreated the table and committed Ok; it is absent".to_string())
        }
        (Some(want), true) if out.rows_after() == want => Ok(()),
        (Some(want), true) => Err(format!(
            "A recreated the table as {want:?} and committed Ok; found {:?}",
            out.rows_after()
        )),
    }
}

/// **Rule (ii)** on the watched tables other than `T`.
///
/// Unconditional in both directions and independent of A's outcome, because no
/// `AState` touches these: a table B installed must be present with exactly B's
/// rows, and one B did not install must be absent. Nothing here is ever
/// exempted, so this is the strictest clause in the oracle — and the only one
/// that can see a multi-table install come apart.
///
/// Run first in [`effect_intact`], ahead of anything about `T`. A `BulkBatch`
/// that lost `U` while getting `T` right is a broken atomic install, and
/// reporting it as such names the defect; letting a later `T` clause fire first
/// would not.
fn side_tables_intact(b: BOp, out: &CellOutcome) -> Result<(), String> {
    let declared: BTreeMap<&str, &[(u64, &str)]> = b_side_tables(b).iter().copied().collect();
    for name in WATCHED.iter().filter(|n| **n != T) {
        match (declared.get(name), out.tables.get(*name)) {
            (None, None) => {}
            (None, Some(got)) => {
                return Err(format!(
                    "table {name:?} exists as {got:?}; B installed no such table"
                ));
            }
            (Some(want), None) => {
                return Err(format!(
                    "B installed table {name:?} as {want:?}; it is now absent"
                ));
            }
            (Some(want), Some(got)) => {
                let want: Vec<(u64, String)> =
                    want.iter().map(|(k, v)| (*k, (*v).to_string())).collect();
                if *got != want {
                    return Err(format!(
                        "table {name:?}: B installed {want:?}, found {got:?}"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// **Rule (ii)**: whatever committed `Ok` has its full effect visible
/// afterwards, and is never silently reverted.
///
/// Checked on every cell, conflicts included. Task 1 checked only the negative
/// half — "no `seed`-prefixed row is present" — which a partial-merge
/// corruption that dropped *some* of B's rows without resurrecting any of A's
/// would sail straight through. Since `b_rows` is the table's complete post-B
/// content, this asserts both directions:
///
/// - every row B installed is present, with B's exact value;
/// - no row B did not install is present.
///
/// Both are relaxed only on the keys A was entitled to change, and only if A
/// actually committed — and on exactly those keys A's *own* value is asserted
/// instead, so the relaxation transfers the obligation rather than dropping it.
/// Without that transfer the exempt keys would be checked by nobody, and a
/// Phase 2 merge that silently discarded A's write would pass. See
/// [`a_touched`].
///
/// When A committed *and* replaced the table wholesale, B's rows are
/// legitimately gone and no claim about them survives — but that is not licence
/// to assert nothing, because A is now the party that committed `Ok`. The check
/// switches to [`a_effect_intact`] rather than returning early. (Reverting the
/// `installed_tables` term makes `DeleteRecreate x Bulk*` commit with A's
/// delete silently discarded, leaving B's rows in place: that is a rule (ii)
/// violation against **A**, and it is this arm that catches it.)
///
/// The one thing rule (ii) genuinely cannot adjudicate is A committing a delete
/// over B's install — both committed `Ok`, and one of them must lose. Rule 4
/// exists precisely so that never happens, and enforcing it is the
/// *expectation* half's job, not this one.
fn effect_intact(a: AState, b: BOp, out: &CellOutcome) -> Result<(), String> {
    side_tables_intact(b, out)?;

    let exempt: &[(u64, &str)] = match (out.commit.is_ok(), a_touched(a)) {
        (true, None) => return a_effect_intact(a, b, out),
        (true, Some(rows)) => rows,
        // A did not commit: it gets no exemption at all.
        (false, _) => &[],
    };
    let is_exempt = |k: u64| exempt.iter().any(|(ek, _)| *ek == k);

    let after: BTreeMap<u64, &str> = out
        .rows_after()
        .iter()
        .map(|(k, v)| (*k, v.as_str()))
        .collect();

    // **Rule (ii) applied to A**, on exactly the keys the exemption above takes
    // out of B's check. This runs before the presence match deliberately: a
    // committed row of A's that vanished with the table is still a lost update,
    // and reporting it as "B deleted the table" would name the wrong party.
    for (k, want) in exempt {
        match after.get(k) {
            Some(got) if got == want => {}
            Some(got) => {
                return Err(format!(
                    "row {k}: A committed {want:?}, found {got:?} — A's write was reverted"
                ));
            }
            None => {
                return Err(format!(
                    "row {k}: A committed {want:?}, now absent — A's write was reverted"
                ));
            }
        }
    }

    // A left `T`'s existence alone — every [`a_touched`]-`Some` state only opens
    // the table — so B's disposition of it is the whole answer, in both
    // directions. The second arm is the one that catches a resurrection.
    match (b_leaves_table(b), out.table_present()) {
        (true, false) => return Err("B installed the table; it is now absent".to_string()),
        (false, true) => {
            return Err(format!(
                "B deleted the table; it is present as {:?}",
                out.rows_after()
            ));
        }
        _ => {}
    }
    if !out.table_present() {
        // B removed it and it is gone: `b_rows` is empty and so is `rows_after`,
        // so both loops below would be vacuous.
        return Ok(());
    }

    for (k, want) in b_rows(b) {
        if is_exempt(k) {
            continue;
        }
        match after.get(&k) {
            Some(got) if *got == want => {}
            Some(got) => return Err(format!("row {k}: B installed {want:?}, found {got:?}")),
            None => return Err(format!("row {k}: B installed {want:?}, now absent")),
        }
    }

    let installed: BTreeSet<u64> = b_rows(b).into_iter().map(|(k, _)| k).collect();
    for (k, v) in out.rows_after() {
        if !installed.contains(k) && !is_exempt(*k) {
            return Err(format!(
                "row ({k}, {v:?}) is not one B installed — pre-B state resurrected"
            ));
        }
    }
    Ok(())
}

/// Whether the table must exist after the race, for the cells where that — and
/// not the row contents — is the property under test. `None` = not asserted.
///
/// [`effect_intact`] already enforces every one of these structurally, and the
/// two must agree. This exists anyway because a generic oracle cannot carry a
/// citation: each arm below names the commit that decided the outcome, so a
/// future reader who wants to change one knows whose ruling they are
/// overturning. If these ever disagree, the bug is in the test, not the store.
fn expected_presence(a: AState, b: BOp) -> Option<bool> {
    match (a, b) {
        // Bug 3 (fixed in dbd56d4): a write-free open must not resurrect a
        // table a concurrent delete removed. Pre-fix it came back carrying A's
        // pre-delete clone, `seed1`/`seed2` and all.
        (AState::OpenOnly, BOp::TxDelete) => Some(false),
        // Pinned choice, 2026-08-05 (dbd56d4): a write-free recreate does not
        // survive a concurrent delete. See
        // `delete_then_reopen_without_writing_leaves_a_concurrently_deleted_table_absent`
        // in `src/store.rs`.
        (AState::DeleteRecreate, BOp::TxDelete) => Some(false),
        // Bug 5 (fixed in 68cd794): same shape, but A wrote before deleting.
        // Must match the line above — that it did not was the bug.
        (AState::WriteDeleteRecreate, BOp::TxDelete) => Some(false),
        // The same pinned choice, reached through a keep-set drop instead of a
        // `delete_table`. Cited separately because the ruling was made about
        // Phase 2's `(None, _)` arm, and that arm is indifferent to *how* the
        // table left latest — these two cells are what say so.
        #[cfg(feature = "persistence")]
        (
            AState::DeleteRecreate | AState::WriteDeleteRecreate,
            BOp::StreamInstallDrop,
        ) => Some(false),
        _ => None,
    }
}

/// Runs a cell under [`IsolationLevel::SnapshotIsolation`] and asserts all
/// three parts of the oracle.
///
/// Every cell Tasks 1–5 wrote calls this, and calls it exactly as it did before
/// the isolation level became a parameter. Keeping the two-argument spelling is
/// not laziness: it means the Serializable pass added no textual change to a
/// single SI cell, so the five calibrations still measure what they measured.
fn check_cell(a: AState, b: BOp, expect: Expect) {
    check_cell_at(a, b, IsolationLevel::SnapshotIsolation, expect);
}

/// Runs a cell at a named isolation level and asserts all three parts of the
/// oracle.
fn check_cell_at(a: AState, b: BOp, iso: IsolationLevel, expect: Expect) {
    if let Some(why) = cell_failure(a, b, iso, expect) {
        panic!("{why}");
    }
}

/// The body of [`check_cell_at`], returning the first violated clause instead of
/// panicking on it. `None` means the cell passed.
///
/// Split out for the column tests of the Serializable pass, which run seven
/// cells apiece and must report *all* of the failures rather than aborting on
/// the first. This file already knows what abort-on-first costs: the two packed
/// bug-5 tests hide their second cell, and the module docs have to explain twice
/// that a naive run shows eight failures where ten cells moved.
fn cell_failure(a: AState, b: BOp, iso: IsolationLevel, expect: Expect) -> Option<String> {
    let out = run_cell(a, b, iso);
    let label = format!("{a:?} x {b:?} @ {iso:?}");

    // (i) the outcome is the one the rules predict
    let (matched, wanted) = match expect {
        Expect::Commits => (out.commit.is_ok(), "commit"),
        Expect::Conflicts => (
            matches!(out.commit, Err(Error::WriteConflict { .. })),
            "WriteConflict",
        ),
        Expect::DdlConflicts => (
            matches!(out.commit, Err(Error::IndexDdlConflict { .. })),
            "IndexDdlConflict",
        ),
        Expect::SerializationFails => (
            matches!(out.commit, Err(Error::SerializationFailure { .. })),
            "SerializationFailure",
        ),
    };
    if !matched {
        return Some(format!(
            "{label}: expected {wanted}, got {:?}",
            out.commit
        ));
    }

    // (ii) THE UMBRELLA PROPERTY: whatever committed Ok is never silently
    // reverted.
    if let Err(why) = effect_intact(a, b, &out) {
        return Some(format!(
            "{label}: {why} — something that committed Ok was silently \
             reverted. rows={:?} commit={:?}",
            out.rows_after(), out.commit
        ));
    }

    // (iii) the table's existence, where a specific ruling pinned it
    if let Some(want) = expected_presence(a, b)
        && out.table_present() != want
    {
        return Some(format!(
            "{label}: expected table_present={want}, got {}, rows={:?} commit={:?}",
            out.table_present(), out.rows_after(), out.commit
        ));
    }
    None
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

// ── Axis A × B1 (`bulk_load` Replace), remaining cells ──────────────────────

/// Rule 2: A wrote rows and B replaced the table under it, so A's write was
/// decided against contents that no longer exist.
#[test]
fn write_over_a_bulk_replace_conflicts() {
    check_cell(AState::OpenWrite, BOp::BulkReplace, Expect::Conflicts);
}

/// Rule 4, and bug 4 (fixed in dbd56d4): a delete racing a wholesale install
/// must conflict, not silently drop the freshly loaded data.
#[test]
fn delete_over_a_bulk_replace_conflicts() {
    check_cell(AState::Delete, BOp::BulkReplace, Expect::Conflicts);
}

/// Rule 4: reopening after the delete does not retract it — `ever_deleted_tables`
/// still carries `T`, so the install is still being deleted out from under.
#[test]
fn delete_recreate_over_a_bulk_replace_conflicts() {
    check_cell(AState::DeleteRecreate, BOp::BulkReplace, Expect::Conflicts);
}

/// Rule 4 — but **never rely on this cell alone as the canary for the
/// `installed_tables` term.** Unlike its four delete-flavoured siblings it has
/// a second, independent path to the same conflict: the recreate's `insert`
/// leaves a non-empty `write_set[T]`, and the install put `T` in
/// `deleted_tables`, so `validate_write_set`'s *first* loop conflicts it too.
/// Verified — dropping `|| cws.installed_tables.contains(deleted)` turns its
/// **nine** delete-flavoured siblings in the three bulk columns red (`Delete`,
/// `DeleteRecreate` and `WriteDeleteRecreate`, each against Replace, Delta and
/// Batch) and leaves this one green. The count was six before Task 4 added the
/// `BulkBatch` column, and five in a still earlier revision of this comment; it
/// tracks the number of *installing* columns, so re-measure it rather than
/// trusting it if another is added.
///
/// The `StreamInstallDrop` column is untouched by that revert, which is not a
/// gap: a keep-set drop deliberately never puts `T` in `installed_tables`, so
/// the term has nothing to bite on there. That column's delete-flavoured cells
/// are decided by the term's *absence*, and they stay green.
#[test]
fn delete_recreate_write_over_a_bulk_replace_conflicts() {
    check_cell(
        AState::DeleteRecreateWrite,
        BOp::BulkReplace,
        Expect::Conflicts,
    );
}

/// Rule 4. **This is not a bug-5 calibrator, despite the shape** — an earlier
/// draft of this suite claimed it was, and a reverted `68cd794` will not turn
/// it red.
///
/// Bug 5 was `delete_table` leaving stale write-set digests behind. Reverting
/// it cannot flip this cell, because the *second* loop conflicts
/// `ever_deleted_tables` against the install's `installed_tables` whether or
/// not the digests were cleared; pre-fix, the stale digests merely give the
/// first loop a redundant second path to the same answer. `delete_table`'s own
/// comment says as much: "It adds no defence against a delete-and-recreate
/// being overwritten by latest — the second loop is the only thing preventing
/// that, before and after."
///
/// Bug 5's observable effect is on delete-vs-**delete**, where the stale
/// digests raised a spurious conflict against a concurrent commit that merely
/// *removed* the table. Calibrating it needs the `delete_table` column.
#[test]
fn write_delete_recreate_over_a_bulk_replace_conflicts() {
    check_cell(
        AState::WriteDeleteRecreate,
        BOp::BulkReplace,
        Expect::Conflicts,
    );
}

// ── Axis A × B2 (`bulk_load` Delta) ─────────────────────────────────────────
//
// A Delta reaches `install_pending` with a fully materialized row vector,
// exactly as Replace does, and records the same `installed_tables` /
// `deleted_tables` entries. So the rules predict the same column twice; these
// cells exist to hold that equivalence, which is a property of the *install*
// path, not something either variant declares.

/// Rule 1: A contributed nothing to `T`, so B's delta stands untouched.
#[test]
fn open_only_does_not_revert_a_bulk_delta() {
    check_cell(AState::OpenOnly, BOp::BulkDelta, Expect::Commits);
}

/// Rule 3: A holds DDL on `T` and B touched `T`.
#[test]
fn ddl_only_over_a_bulk_delta_fails_loudly() {
    check_cell(AState::OpenDdl, BOp::BulkDelta, Expect::DdlConflicts);
}

/// Rule 2: A updated id 1 and so did the delta.
#[test]
fn write_over_a_bulk_delta_conflicts() {
    check_cell(AState::OpenWrite, BOp::BulkDelta, Expect::Conflicts);
}

/// Rule 4: a delete racing a delta install must conflict — the delta is an
/// install of the whole table, no less than a Replace is.
#[test]
fn delete_over_a_bulk_delta_conflicts() {
    check_cell(AState::Delete, BOp::BulkDelta, Expect::Conflicts);
}

#[test]
fn delete_recreate_over_a_bulk_delta_conflicts() {
    check_cell(AState::DeleteRecreate, BOp::BulkDelta, Expect::Conflicts);
}

/// Rule 4. Carries the same caveat as its Replace twin: the recreate's write
/// gives it a second path to the conflict, so it is not a canary for the
/// `installed_tables` term.
#[test]
fn delete_recreate_write_over_a_bulk_delta_conflicts() {
    check_cell(
        AState::DeleteRecreateWrite,
        BOp::BulkDelta,
        Expect::Conflicts,
    );
}

/// Rule 4. Not a bug-5 calibrator — see the Replace twin for why a reverted
/// `68cd794` leaves this green.
#[test]
fn write_delete_recreate_over_a_bulk_delta_conflicts() {
    check_cell(
        AState::WriteDeleteRecreate,
        BOp::BulkDelta,
        Expect::Conflicts,
    );
}

// ── Axis A × B4 (a concurrent transaction's row write) ──────────────────────
//
// B here is an ordinary commit: its `CommittedWriteSet` names `T` in `tables`,
// and leaves both `deleted_tables` and `installed_tables` empty. That is the
// *opposite* field pattern from the bulk columns above, where `tables` is empty
// and only `deleted_tables` names `T`. Reading the wrong one of the two was the
// root of all five bugs, so what this column buys is that the validator and the
// Phase 2 installer have to be right about a table under both encodings.

/// Rule 1: A contributed nothing to `T`, so B's write stands. Phase 2 reaches
/// the `(Some(_), _)` skip arm — `T` is still in latest, and A's empty write set
/// means there is nothing of A's to merge onto it.
#[test]
fn open_only_does_not_revert_a_concurrent_write() {
    check_cell(AState::OpenOnly, BOp::TxWrite, Expect::Commits);
}

/// Key-level OCC: A updates id 1, B updates id 2 — disjoint, so both land.
///
/// This is the only cell in the matrix that reaches Phase 2's *merge* arm; every
/// other `Commits` cell takes a skip arm. So it is the sole guard on
/// `merge_keys_from`, in both directions: id 2 is not exempt, so B's `from_b`
/// must survive A committing on top, and id 1 *is* exempt, so [`a_touched`]'s
/// value check is what proves A's `from_a` was not quietly dropped.
///
/// An earlier revision of this comment claimed the exemption was "a relaxation
/// and not a hole" while `a_touched` still returned bare keys — at which point it
/// was precisely a hole, and stubbing `merge_keys_from` to a no-op left the
/// suite green. Both halves are asserted now; the claim is true as of the commit
/// that fixed `a_touched`, and not before.
#[test]
fn write_over_a_concurrent_write_to_a_different_row_commits() {
    check_cell(AState::OpenWrite, BOp::TxWrite, Expect::Commits);
}

/// Rule 3: A holds DDL on `T` and B wrote to `T`.
#[test]
fn ddl_only_over_a_concurrent_write_fails_loudly() {
    check_cell(AState::OpenDdl, BOp::TxWrite, Expect::DdlConflicts);
}

/// Rule 4: A deleted `T` and B modified it, so A's delete was decided against
/// contents that no longer exist. `validate_write_set`'s second loop, reached
/// through its `cws.tables.contains_key` half rather than the `installed_tables`
/// half the bulk column exercises.
#[test]
fn delete_over_a_concurrent_write_conflicts() {
    check_cell(AState::Delete, BOp::TxWrite, Expect::Conflicts);
}

/// Rule 4: the reopen does not retract the delete — `ever_deleted_tables` still
/// carries `T`.
#[test]
fn delete_recreate_over_a_concurrent_write_conflicts() {
    check_cell(AState::DeleteRecreate, BOp::TxWrite, Expect::Conflicts);
}

/// Rule 4. Unlike its bulk twins this one has no second path to the conflict:
/// A's write set for `T` is `{digest(1)}` (the recreate's `insert`) and B's is
/// `{digest(2)}`, which are disjoint, and B's `deleted_tables` is empty so the
/// first loop is vacuous. Only the second loop conflicts it.
#[test]
fn delete_recreate_write_over_a_concurrent_write_conflicts() {
    check_cell(AState::DeleteRecreateWrite, BOp::TxWrite, Expect::Conflicts);
}

/// Rule 4.
#[test]
fn write_delete_recreate_over_a_concurrent_write_conflicts() {
    check_cell(AState::WriteDeleteRecreate, BOp::TxWrite, Expect::Conflicts);
}

// ── Axis A × B5 (a concurrent transaction's `delete_table`) ─────────────────
//
// The column bugs 3 and 5 live in — but **not** the only one that can
// calibrate them, and this header said otherwise until 2026-08-06. Task 4's
// `StreamInstallDrop` column is a second calibrator for both: measured,
// mutation D reddens that column's packed test as well as this one's, and
// mutation B reddens both. See the module-level calibration list, and
// [`write_delete_recreate_matches_delete_recreate_against_a_concurrent_delete`]
// for the per-cell version of the same correction.
//
// What is still unique here is the *shape* of B's commit. Its
// `CommittedWriteSet` names `T` in `deleted_tables` **and nowhere else** —
// `tables` is empty because `delete_table` never opens the table, and
// `installed_tables` is empty because an ordinary commit installs nothing. So
// this is the one column where "B removed `T`" is the entire post-condition,
// and `table_present` carries the assertion that row contents cannot.
// `StreamInstallDrop` reaches the same `deleted_tables`-only shape for `T` by a
// different route (a keep-set drop), which is why it calibrates the same two
// bugs; it is not a *narrower* column, just a differently spelled one.

/// Bug 3 (fixed in dbd56d4): a write-free `open_table` resurrected a table a
/// concurrent `delete_table` removed — commit returned `Ok` and `T` came back
/// carrying A's pre-delete clone, `seed1`/`seed2` and all. Phase 2's `(None, _)`
/// arm installed A's dirty table despite its own comment saying "but I wrote to
/// it"; it now installs only when the transaction contributed something.
///
/// Calibrated 2026-08-05: restoring that arm to an unconditional install turns
/// this red on both the presence assertion and the umbrella check.
#[test]
fn open_only_does_not_resurrect_a_concurrently_deleted_table() {
    check_cell(AState::OpenOnly, BOp::TxDelete, Expect::Commits);
}

/// Rule 2: A wrote rows to `T` and B deleted `T`. `validate_write_set`'s first
/// loop — A's write set for `T` is non-empty and `T` is in B's `deleted_tables`.
#[test]
fn write_over_a_concurrent_delete_conflicts() {
    check_cell(AState::OpenWrite, BOp::TxDelete, Expect::Conflicts);
}

/// Rule 3: A holds DDL on `T` and B touched `T` — deleting it counts. The DDL
/// check reads the same `flags` map Phase 2 does, and that map ors
/// `deleted_tables` in, so a delete reaches it. Failing loudly is the right
/// answer for the same reason task41 gave: A's index definition cannot survive
/// the merge, and Phase 2 would otherwise drop it in silence.
#[test]
fn ddl_only_over_a_concurrent_delete_fails_loudly() {
    check_cell(AState::OpenDdl, BOp::TxDelete, Expect::DdlConflicts);
}

/// Bug 5 (fixed in 68cd794), and **one of the only two cells that can
/// calibrate it** — the bulk columns cannot, see
/// `write_delete_recreate_over_a_bulk_replace_conflicts`. The other is the
/// keep-set-drop twin, [`write_delete_recreate_matches_delete_recreate_against_a_stream_install_drop`],
/// added in Task 4; this comment said "the only" before it existed. The
/// 2026-08-06 calibration measured exactly those two and nothing else.
///
/// The two halves are deliberately in one test, because bug 5 was not a wrong
/// answer in isolation: it was the *difference* between them. `delete_table`
/// left `write_set[T]` describing rows that no longer existed, in a table about
/// to be recreated empty, so `validate_write_set`'s first loop conflicted
/// `WriteDeleteRecreate` against B's `deleted_tables` while `DeleteRecreate`,
/// which never wrote, sailed through. The outcome depended on whether the
/// transaction happened to write before deleting. Asserting the two together is
/// what states that it must not.
///
/// Calibrated 2026-08-05: reverting `68cd794`'s digest-clearing block turns the
/// first line red (`WriteConflict` where `Ok` is required) and leaves the second
/// green.
///
/// One property of packing them: the halves run in order, so an abort in the
/// first masks the second. Under the bug-5 revert the reported failure is
/// `WriteDeleteRecreate` only, and whether `DeleteRecreate` also moved is not
/// visible from the output — swap the two lines to see the other side.
/// Confirmed by doing so: swapped, the *`DeleteRecreate`* line passes and
/// `WriteDeleteRecreate` still fails, which is the asymmetry. Packing is still
/// right, because the property under test is the equality of the two, not either
/// outcome alone.
///
/// **If this test is red, you do not have to swap anything to find out which
/// cause it is.** Check whether [`delete_vs_concurrent_delete_commits`] and
/// [`delete_vs_stream_install_drop_commits`] are also red. They are under a
/// reversal of the 2026-08-06 ruling and are *not* under a bug-5 revert, so the
/// failure list alone separates "the bug came back" (halves moved apart) from
/// "the ruling changed" (halves moved together). Full mapping and both reverts:
/// the `# Calibration` section in the module docs.
#[test]
fn write_delete_recreate_matches_delete_recreate_against_a_concurrent_delete() {
    check_cell(AState::WriteDeleteRecreate, BOp::TxDelete, Expect::Commits);
    check_cell(AState::DeleteRecreate, BOp::TxDelete, Expect::Commits);
}

/// **Rule 4's parenthesis, pinned.** Delete vs delete: both parties want `T`
/// gone, and neither can observe the other's rows afterwards, so A commits.
///
/// Mechanically, `validate_write_set` declines to conflict A twice over. Its
/// first loop needs a non-empty write set for `T`, and A has none —
/// `delete_table` alone never opens the table, so no entry is ever created. Its
/// second loop needs `T` in B's `tables` or `installed_tables`, and B's commit
/// puts it in neither.
///
/// **The authority is `src/store.rs:283-288`**, the doc comment on
/// `CommittedWriteSet::installed_tables`:
///
/// > a table this commit merely removed (`delete_table`, **or a snapshot-stream
/// > keep-set drop**) agrees with that transaction's delete and must not
/// > [conflict].
///
/// That sentence names **both** spellings of a removal and rules them
/// identically, so it settles this cell and
/// [`delete_vs_stream_install_drop_commits`] at once — which is why two cells
/// share one citation. Note the citation is `:283-288` and **not** `:1867`:
/// `:1867` ("The drops are removals, not installs") establishes only *which set*
/// a keep-set drop lands in, which is classification, not a semantic ruling.
/// Confusing the two is what got the stream twin wrongly pinned when Task 4
/// first landed.
///
/// Carried `#[ignore]`d from Task 1 until 2026-08-06, on the reading that
/// whether SI *should* conflict here was an open preference. The ruling is that
/// `:283-288` already answers it.
///
/// **`Serializable` is not the reason, contrary to the design spec** (`…
/// races-design.md:119-121`, which listed this cell as one where "SSI's
/// `validate_read_set` aborts it"). It does not. `AState::Delete` calls only
/// `delete_table`, which never reads, so there is no read-set entry for `T` and
/// `validate_read_set`'s delete arm (`src/store.rs:4541`) cannot fire. Measured
/// under `IsolationLevel::Serializable`: still `Ok`, and
/// [`serializable_concurrent_delete_column`] now asserts it. The spec carries a
/// dated correction to that effect as of 2026-08-06.
///
/// Where SSI *does* diverge — ten cells, every one `Ok` under SI and
/// `SerializationFailure` under SSI — is stated and asserted in the Phase 2
/// section; [`ssi_expect`] is the list. Nine of the ten are pinned there and the
/// tenth is an unruled cell recorded in the `--ignored` pass.
///
/// Two notes that used to live here and are now settled. The second group of
/// divergences is the bug-5 pair in both of its spellings, so the equality those
/// packed tests assert had to be decided per isolation level: it is asserted at
/// **both**, and holds at both, at `Ok` under SI and `SerializationFailure`
/// under SSI — the halves move together across the change, which is the whole
/// content of the property. And the count is stated per *cell*, not per test,
/// because the packed tests abort on the first of their two: a naive SI run
/// shows eight where ten cells moved. The Serializable columns do not have that
/// problem — they report every failing cell in the column — so the ten are
/// directly observable now rather than reconstructed by swapping lines.
///
/// **Current behaviour, measured 2026-08-05 under SI and unchanged by the
/// pinning:** commits `Ok`; `T` is absent afterwards. Absence is asserted, not
/// merely observed — `a_whole_table` returns `None` for `Delete`, so
/// `a_effect_intact`'s `(None, true)` arm fails this cell if `T` survives.
#[test]
fn delete_vs_concurrent_delete_commits() {
    check_cell(AState::Delete, BOp::TxDelete, Expect::Commits);
}

// ── Axis A × B3 (`bulk_load_batch`, multi-table atomic install) ─────────────
//
// `Store::bulk_load` with a `Replace` input **is** a one-element
// `bulk_load_batch` (`src/store.rs:1425-1426`), so on `T` alone this column is the
// Replace column re-spelled, and its outcomes are the same by construction
// rather than by coincidence.
//
// What is new is `U`. Both tables go in through one `add` each and one
// `commit`, landing in a single snapshot version, so every cell below carries
// an atomicity claim that no single-table column can state: whatever A's commit
// does to `T`, `U` must be present with B's exact rows. `side_tables_intact`
// asserts it unconditionally — A never touches `U`, so it gets no exemption
// there in any cell, committed or conflicted.

/// Rules 1 and 5 together: A contributed nothing to `T`, so B's install of `T`
/// stands — and A never touched `U`, so B's install of `U` stands with it.
///
/// The one cell in this column where A commits, which makes it the only one
/// where a Phase 2 arm runs against a multi-table install. It takes the
/// `(Some(_), _)` skip — `T` is in latest and A's write set for it is empty —
/// and Phase 3 then re-forks from the current latest, which is what carries `U`
/// through. A promotion that rebuilt the snapshot from A's *base* instead would
/// leave `U` absent, and `side_tables_intact` is what says so.
#[test]
fn open_only_does_not_revert_a_bulk_batch() {
    check_cell(AState::OpenOnly, BOp::BulkBatch, Expect::Commits);
}

/// Rule 3: A holds DDL on `T` and B replaced `T`.
#[test]
fn ddl_only_over_a_bulk_batch_fails_loudly() {
    check_cell(AState::OpenDdl, BOp::BulkBatch, Expect::DdlConflicts);
}

/// Rule 2: A wrote id 1 and B replaced the whole table under it.
#[test]
fn write_over_a_bulk_batch_conflicts() {
    check_cell(AState::OpenWrite, BOp::BulkBatch, Expect::Conflicts);
}

/// Rule 4: `T` is in the batch's `installed_tables`, so A's delete was decided
/// against contents that no longer exist.
#[test]
fn delete_over_a_bulk_batch_conflicts() {
    check_cell(AState::Delete, BOp::BulkBatch, Expect::Conflicts);
}

/// Rule 4: the reopen does not retract the delete.
#[test]
fn delete_recreate_over_a_bulk_batch_conflicts() {
    check_cell(AState::DeleteRecreate, BOp::BulkBatch, Expect::Conflicts);
}

/// Rule 4. Same caveat as its Replace and Delta twins: the recreate's `insert`
/// gives `validate_write_set`'s first loop an independent path to the same
/// conflict, so this is not a canary for the `installed_tables` term.
#[test]
fn delete_recreate_write_over_a_bulk_batch_conflicts() {
    check_cell(AState::DeleteRecreateWrite, BOp::BulkBatch, Expect::Conflicts);
}

/// Rule 4. Not a bug-5 calibrator — see the Replace twin for why a reverted
/// `68cd794` leaves this green.
#[test]
fn write_delete_recreate_over_a_bulk_batch_conflicts() {
    check_cell(AState::WriteDeleteRecreate, BOp::BulkBatch, Expect::Conflicts);
}

// ── Axis A × B6 (`install_snapshot_stream`, `OnExtra::Drop`) ────────────────
//
// The column where B's `CommittedWriteSet` has `deleted_tables` and
// `installed_tables` naming **different** tables: `install_batch_inner` records
// the streamed `U` in both, and the keep-set-dropped `T` in `deleted_tables`
// only, on the stated grounds that "The drops are removals, not installs"
// (`src/store.rs:1867`). Nowhere else in the matrix do those two fields
// disagree, and the disagreement is exactly what decides this column: rule 4's
// antecedent is "B *installed* `T`", and here it is false, so every one of A's
// delete-flavoured states is a delete-vs-delete rather than a delete-vs-install.
//
// That makes this column the `TxDelete` column's twin, reached by a different
// route — and every question against `TxDelete` is the same question here, by
// the same authority rather than an analogous one: the ruling comment on
// `CommittedWriteSet::installed_tables` (`src/store.rs:283-288`) names both
// spellings in one breath — "a table this commit merely removed (`delete_table`,
// or a snapshot-stream keep-set drop) agrees with that transaction's delete and
// must not [conflict]". Whatever it rules, it rules for both, so no cell here
// can be pinned — or left unpinned — by appealing to a distinction that sentence
// does not draw.
//
// It settles the delete-vs-delete corner: `Delete` is pinned below against that
// citation, jointly with its `TxDelete` twin (ruling of 2026-08-06). The
// `DeleteRecreate` and `WriteDeleteRecreate` corners are pinned by the bug-5
// equality between them, which holds independently of that ruling. Two of this
// column's seven cells remain `#[ignore]`d: `OpenOnly`, and the fourth corner
// `DeleteRecreateWrite`.

/// Rule 2: A wrote id 1 to `T` and B's keep-set dropped `T`.
/// `validate_write_set`'s first loop — A's write set for `T` is non-empty and
/// `T` is in B's `deleted_tables`, which a keep-set drop populates exactly as
/// `delete_table` does.
#[cfg(feature = "persistence")]
#[test]
fn write_over_a_stream_install_drop_conflicts() {
    check_cell(AState::OpenWrite, BOp::StreamInstallDrop, Expect::Conflicts);
}

/// Rule 3: A holds DDL on `T` and B removed `T`. The `flags` map ors
/// `deleted_tables` in, so a keep-set drop reaches the DDL check on the same
/// terms a `delete_table` does.
#[cfg(feature = "persistence")]
#[test]
fn ddl_only_over_a_stream_install_drop_fails_loudly() {
    check_cell(AState::OpenDdl, BOp::StreamInstallDrop, Expect::DdlConflicts);
}

/// **Rule 4's parenthesis again, in its keep-set-drop spelling, pinned.** A
/// deleted `T`; B installed `U` from a wire stream and dropped `T` as an extra.
/// `validate_write_set`'s second loop asks
/// `cws.tables.contains_key(T) || cws.installed_tables.contains(T)`, and a
/// keep-set drop puts `T` in neither, so this is delete-vs-delete and commits —
/// exactly as [`delete_vs_concurrent_delete_commits`] does.
///
/// **Same authority, `src/store.rs:283-288`**, and deliberately the same one:
///
/// > a table this commit merely removed (`delete_table`, **or a snapshot-stream
/// > keep-set drop**) agrees with that transaction's delete and must not
/// > [conflict].
///
/// One sentence, both routes, one rule. It names the keep-set drop explicitly,
/// so this cell is not decided by analogy to its twin — it is decided by the
/// same clause, and the two must move together if it is ever revised.
///
/// This cell has been pinned, un-pinned and re-pinned, which is worth recording
/// because the reasoning changed each time and only the last is sound. Task 4
/// pinned it by *contrasting* it with its twin, citing `src/store.rs:1867` ("The
/// drops are removals, not installs") as an independent ruling this cell had and
/// the twin lacked. Review found that `:1867` is classification, not a ruling,
/// and that the actual ruling at `:283-288` covers both spellings — so the
/// contrast was fictitious and the cell was `#[ignore]`d alongside its twin. The
/// ruling of 2026-08-06 pins **both** against `:283-288`, which is the outcome
/// Task 4 reached from a bad argument.
///
/// The SMR use case this column models agrees: an `InstallSnapshot` should leave
/// the follower mirroring the leader whatever a local writer was doing, and
/// nothing either party committed is lost either way.
///
/// **Current behaviour, measured 2026-08-05 and unchanged by the pinning:**
/// commits `Ok`; `T` is absent afterwards and `U` is present with the streamed
/// rows. Both are asserted, not merely observed — `a_whole_table` returns `None`
/// for `Delete`, and [`side_tables_intact`] holds `U` unconditionally.
///
/// This is the one cell in the column [`side_tables_intact`] catches *alone*
/// (see [`CellOutcome`]), so pinning it puts that evidence back on the default
/// CI path, where it was not while the cell was ignored.
#[cfg(feature = "persistence")]
#[test]
fn delete_vs_stream_install_drop_commits() {
    check_cell(AState::Delete, BOp::StreamInstallDrop, Expect::Commits);
}

/// Bug 5 (fixed in 68cd794) again, and the **second** cell able to calibrate it
/// — reached through the keep-set-drop route rather than `delete_table`.
///
/// Packed as an equality for the same reason
/// [`write_delete_recreate_matches_delete_recreate_against_a_concurrent_delete`]
/// is: bug 5 was not a wrong answer in isolation but the *difference* between
/// these two lines. `delete_table` left `write_set[T]` describing rows of an
/// incarnation that no longer exists, so the first loop conflicted the state
/// that happened to write before deleting and let the one that did not through.
///
/// That this reproduces against a keep-set drop is the point of running it here.
/// Bug 5's mechanism is on A's side entirely — stale digests meeting *any*
/// `deleted_tables` entry — so it must be independent of how B's removal was
/// spelled, and the `TxDelete` twin alone cannot say that.
///
/// Both halves also assert, through [`a_whole_table`], that the write-free
/// recreate does **not** bring `T` back: `b_leaves_table` is false here, so the
/// pinned 2026-08-05 choice applies by the same Phase 2 `(None, _)` arm.
///
/// **If this test is red**, the same shortcut applies as for the `TxDelete`
/// twin: check whether [`delete_vs_concurrent_delete_commits`] and
/// [`delete_vs_stream_install_drop_commits`] are also red. Both red means the
/// 2026-08-06 ruling was reversed (the halves moved *together*, equality intact,
/// only the pinned value changed); both green means bug 5 came back (the halves
/// moved *apart*). No line-swapping needed to tell them apart. Full mapping: the
/// `# Calibration` section in the module docs.
#[cfg(feature = "persistence")]
#[test]
fn write_delete_recreate_matches_delete_recreate_against_a_stream_install_drop() {
    check_cell(
        AState::WriteDeleteRecreate,
        BOp::StreamInstallDrop,
        Expect::Commits,
    );
    check_cell(
        AState::DeleteRecreate,
        BOp::StreamInstallDrop,
        Expect::Commits,
    );
}

// ── Cells awaiting a ruling ────────────────────────────────────────────────
//
// Carried as `#[ignore]`d tests rather than omitted, so the question is visible
// in the matrix instead of being a hole nobody can see. Per the design spec:
// "The implementer must surface these rather than choose, and no such cell is
// pinned until a ruling exists." Run them with `-- --ignored`; the doc comments
// record what they do *today*, which is not the same as what they should do.
//
// **Reading a failure here.** CI and `make test/lifecycle-races` run this pass
// (added 2026-08-06 — before that nothing did, and the "measured" values below
// were executed nowhere). But a red `--ignored` pass does not mean the same
// thing as a red main pass. These cells assert an *observation*, not a
// requirement: nobody has ruled that the recorded outcome is correct. So a
// failure here means "the behaviour of an unruled cell changed" — which is a
// prompt to re-read the doc comment and decide whether the new behaviour is
// better, and to update the recorded value if it is. It is not, on its own,
// a regression. The live cells above are where a red result means a bug.
//
// **Three remain**, down from five. The delete-vs-delete pair —
// `Delete × TxDelete` and `Delete × StreamInstallDrop` — left this section on
// 2026-08-06, when `src/store.rs:283-288` was ruled to be the answer rather than
// a classification; both are now pinned live in their own columns. That ruling
// *retired* two questions rather than adding one, which is the direction this
// section is supposed to move in.
//
// The survivors are all genuinely open:
//
// - `DeleteRecreateWrite × TxDelete`
// - `DeleteRecreateWrite × StreamInstallDrop`
// - `OpenOnly × StreamInstallDrop`
//
// Each is observed at **both** isolation levels: the three tests here, and three
// more at the end of the Phase 2 section. So the `--ignored` pass is six tests
// over three cells, not six cells. Running the matrix under `Serializable` did
// not settle any of them — only one even changes behaviour, and it changes it by
// aborting *before* the disputed code runs, which is not a vote. See
// [`open_only_vs_stream_install_drop_under_serializable_is_unruled`].
//
// Note the first two are the *fourth corner* of the delete/recreate square in
// each removal column, and they are not covered by the 2026-08-06 ruling:
// `:283-288` speaks to a transaction that **deleted** a table meeting a commit
// that merely removed it, whereas these two wrote rows into a *recreated* table
// and are conflicted by `validate_write_set`'s **first** loop on live digests.
// Different loop, different question, still unruled.

/// UNRESOLVED — needs a ruling before it is pinned.
///
/// Delete-then-recreate-*with-a-write* vs a concurrent delete. This is the
/// fourth corner of a square whose other three — `Delete`, `DeleteRecreate` and
/// `WriteDeleteRecreate`, all against `TxDelete` — commit `Ok`. It is the only
/// one that conflicts, and the reason is the same conflation `68cd794` fixed for
/// bug 5, arrived at from the other side: `write_set[T]` does not distinguish
/// the deleted table from the recreated one, so `validate_write_set`'s first
/// loop reads "A wrote rows to a table B deleted" when what A did was write rows
/// to a *new* table that happens to share the name. Bug 5 was stale digests from
/// the old incarnation; this is live digests from the new one, hitting the same
/// loop.
///
/// It is not obviously wrong. Committing would take Phase 2's
/// `(None, Some(digests))` arm and install A's recreated table wholesale, which
/// is a defensible outcome — but so is refusing a delete-and-recreate that raced
/// a delete. `68cd794` deliberately went no further than clearing the stale
/// digests, and its commit message is explicit that it adds no new defence;
/// extending it to this cell would change a real outcome, not remove a spurious
/// one. That is a ruling, not a fix.
///
/// **Current behaviour, measured 2026-08-05:** `Err(WriteConflict)`; `T` is
/// absent afterwards.
#[test]
#[ignore = "unresolved: see the doc comment; do not pin until ruled on"]
fn delete_recreate_write_vs_concurrent_delete_outcome_is_unruled() {
    check_cell(AState::DeleteRecreateWrite, BOp::TxDelete, Expect::Conflicts);
}

/// UNRESOLVED — needs a ruling before it is pinned. **This is the cell the
/// design spec names**: "`A1` × `B6` — whether a write-free open should
/// resurrect a keep-set-dropped table."
///
/// A opened `T`, wrote nothing, and committed; B's snapshot install dropped `T`
/// as an extra. Two mechanisms decide the outcome and they were decided
/// independently, which is the whole of the question:
///
/// - `validate_write_set` does not conflict A. Its first loop needs a non-empty
///   `write_set[T]` (A has only the eager empty entry `open_table` leaves) and
///   its second needs `T` in `cws.tables` or `cws.installed_tables` — and the
///   `installed_tables` fix deliberately keeps keep-set drops out of the latter
///   ("The drops are removals, not installs", `src/store.rs:1867`). So A
///   commits.
/// - Phase 2 then finds `T` gone from latest with an empty write set and takes
///   the `(None, _)` skip arm, so A's clone is not reinstalled and `T` stays
///   dropped.
///
/// Neither was chosen with the other in view. Together they give "commits, and
/// the table stays gone", which is the same answer bug 3's fix pinned for
/// `OpenOnly × TxDelete` — but there the two halves were decided together, in
/// one commit, about one mechanism. Here the agreement is a coincidence of two
/// rulings, and it is worth an explicit decision that it is the intended one.
/// The alternative — conflicting A, on the grounds that a snapshot install is a
/// wholesale state replacement and an SMR follower should refuse a local
/// transaction that spans it — would mean putting keep-set drops back into
/// `installed_tables`, which is precisely what the fix declined to do.
///
/// **Current behaviour, measured 2026-08-05:** commits `Ok`; `T` is absent
/// afterwards and `U` is present with the streamed rows.
#[cfg(feature = "persistence")]
#[test]
#[ignore = "unresolved: see the doc comment; do not pin until ruled on"]
fn open_only_vs_stream_install_drop_outcome_is_unruled() {
    check_cell(AState::OpenOnly, BOp::StreamInstallDrop, Expect::Commits);
}

/// UNRESOLVED — needs a ruling before it is pinned. The keep-set-drop twin of
/// [`delete_recreate_write_vs_concurrent_delete_outcome_is_unruled`], and open
/// for exactly the same reason rather than a new one.
///
/// A deleted `T`, recreated it, and wrote a row into the new incarnation; B's
/// keep-set dropped `T`. `validate_write_set`'s first loop sees a non-empty
/// `write_set[T]` against `T` in `deleted_tables` and conflicts — but the
/// digests describe the *recreated* table, not the one B removed, and
/// `write_set` cannot tell the two incarnations apart. That is bug 5's
/// conflation arrived at from the other side: stale digests there, live digests
/// from a new table here.
///
/// It is the fourth corner of a square whose other three
/// (`Delete`, `DeleteRecreate`, `WriteDeleteRecreate`, all against
/// `StreamInstallDrop`) commit, and the odd one out only because A happened to
/// write after recreating. Committing would take Phase 2's
/// `(None, Some(digests))` arm and install A's recreated table wholesale over a
/// snapshot install that had just dropped it — which for the SMR case this
/// column models is arguably *worse* than the conflict, and is the reason this
/// is a ruling and not a fix.
///
/// Carried separately from its `TxDelete` twin rather than folded into it: the
/// two reach the same loop through different `CommittedWriteSet` encodings, so a
/// ruling that changed one without the other would be a real divergence, and
/// only two cells can show it.
///
/// **Current behaviour, measured 2026-08-05:** `Err(WriteConflict)`; `T` is
/// absent afterwards and `U` is present with the streamed rows.
#[cfg(feature = "persistence")]
#[test]
#[ignore = "unresolved: see the doc comment; do not pin until ruled on"]
fn delete_recreate_write_vs_stream_install_drop_outcome_is_unruled() {
    check_cell(
        AState::DeleteRecreateWrite,
        BOp::StreamInstallDrop,
        Expect::Conflicts,
    );
}


// ── Phase 2: the same matrix under `IsolationLevel::Serializable` ───────────
//
// Everything above runs under `SnapshotIsolation`, which is the level all five
// bugs were found and fixed at. This section runs the same 42 cells under
// `Serializable` — same fixtures, same oracle, same determinism, one line of
// `StoreConfig` different.
//
// **Ten of the 42 diverge**, and every one of them the same way: `Ok` under SI,
// `Error::SerializationFailure` under SSI. Nothing goes the other way, and no
// cell changes between two *error* kinds. Measured 2026-08-06 across all 42,
// reproducing the reading Task 4 recorded in
// [`delete_vs_concurrent_delete_commits`].
//
// The mechanism is `validate_read_set`, which SSI runs and SI does not, and one
// of its arms accounts for nine of the ten: a concurrent commit that *deleted* a
// table this transaction read is an unconditional abort
// (`src/store.rs:4541-4545`), consulted before either of the arms that compare
// keys. Every `BOp` in this matrix except `TxWrite` puts `T` in `deleted_tables`
// — the bulk installs no less than the two removals, since an install replaces
// the table wholesale — so any A that *read* `T` aborts against five of the six
// columns. `TxWrite` is the sixth, and it reaches `OpenOnly` through the
// `table_scan` arm instead: `Table::len()` records a scan, and B modified a key
// in `T`.
//
// Which A-states read, then, is the whole of the story:
//
// - `OpenOnly` does — its `t.len()` is a scan — so it diverges in **all six**
//   columns. Six of the ten.
// - `DeleteRecreate` and `WriteDeleteRecreate` do, on the reopened table, so
//   they diverge in the two columns where they committed under SI: `TxDelete`
//   and `StreamInstallDrop`. Four more, and that is the ten.
// - `OpenWrite` reads only the row it updates, so the point-read arm applies
//   and `OpenWrite x TxWrite` still commits — A read key 1, B wrote key 2.
//   This is the one cell in the matrix where SSI's read tracking is *finer*
//   than a table-level rule would be, and it is why the abort arm above cannot
//   be described as "SSI aborts anything that opened the table".
// - `OpenDdl`, `Delete` and `DeleteRecreateWrite` are unchanged in every column,
//   for three different reasons: DDL is checked before the read set, a bare
//   `delete_table` never reads, and `DeleteRecreateWrite` was already
//   conflicting on its write set everywhere.
//
// **`Delete x TxDelete` is not among the ten**, contrary to the design spec —
// see [`delete_vs_concurrent_delete_commits`], and the dated correction in
// `docs/superpowers/specs/2026-08-05-table-lifecycle-races-design.md`.
//
// SSI aborting where SI commits is not a bug: SSI is strictly stronger, and an
// abort is a safe answer that a caller retries. What matters is that it is
// pinned *as its own expectation* rather than absorbed by loosening the SI
// cells to accept either outcome — a cell that accepts two outcomes tests
// nothing. Hence [`Expect::SerializationFails`] as a distinct variant, and
// hence [`ssi_expect`] listing every cell rather than only the interesting ones.

/// Every [`AState`], in the order the enum declares them.
const ALL_A: &[AState] = &[
    AState::OpenOnly,
    AState::OpenWrite,
    AState::OpenDdl,
    AState::Delete,
    AState::DeleteRecreate,
    AState::DeleteRecreateWrite,
    AState::WriteDeleteRecreate,
];

/// The cells carried `#[ignore]`d because no ruling pins them.
///
/// The Serializable column tests skip these, so the `--ignored` cells stay
/// unpinned at *both* isolation levels: an unruled cell is unruled whatever the
/// store is configured to do. Their SSI behaviour is recorded in the three
/// `#[ignore]`d tests at the end of this section — and one of them, `OpenOnly x
/// StreamInstallDrop`, is the tenth divergent cell, which is why this section's
/// live tests pin only nine of the ten.
///
/// # This list is duplicated, and only half the duplication is checked
///
/// It restates the six `#[ignore]`d tests, and `#[ignore]` is an attribute — no
/// test can enumerate it. So the two are tied by assertion in the direction
/// that can be tied: **every `#[ignore]`d cell test asserts `unruled` is true
/// of its own cell**, so clearing an entry here while leaving the tests in
/// place turns the `--ignored` pass red and names the cell.
///
/// The other direction is not checkable and is a real gap: delete an
/// `#[ignore]`d test without deleting the entry here and that cell is silently
/// dropped from the Serializable columns while looking covered. **Ruling on a
/// cell is therefore three edits, not two**, and they are safe in this order:
///
/// 1. pin the cell as a live `SnapshotIsolation` test above, with its citation;
/// 2. delete both of its `#[ignore]`d tests;
/// 3. remove it here — at which point the SSI column picks it up and
///    [`ssi_expect`] must already give it the right value.
///
/// Do (3) before (2) and the assertion catches you. Do (2) without (3) and
/// nothing does, which is the gap; the count in the section header above is the
/// only backstop, so keep it current.
fn unruled(a: AState, b: BOp) -> bool {
    match (a, b) {
        (AState::DeleteRecreateWrite, BOp::TxDelete) => true,
        #[cfg(feature = "persistence")]
        (AState::OpenOnly | AState::DeleteRecreateWrite, BOp::StreamInstallDrop) => true,
        _ => false,
    }
}

/// The commit outcome every cell must produce under
/// [`IsolationLevel::Serializable`].
///
/// Stated for all 42 cells, including the 32 that behave exactly as they do
/// under SI. Listing only the divergent ten would make this a diff against a
/// table that does not exist as data — the SI expectations live in 37 hand
/// written test bodies, each with its own citation, and that is where they
/// belong. Here every cell says what it is.
///
/// The [`Expect::SerializationFails`] arms *are* the divergence list: ten cells,
/// nine of them pinned by the column tests below and the tenth (`OpenOnly x
/// StreamInstallDrop`) unruled and merely recorded. Every other arm is the same
/// value the corresponding SI test asserts.
fn ssi_expect(a: AState, b: BOp) -> Expect {
    let b_removes = !b_leaves_table(b);
    match a {
        // Reads the table (`t.len()` is a scan) and contributes nothing, so
        // every column invalidates it: five through the deleted-table arm, and
        // `TxWrite` through the scan arm.
        AState::OpenOnly => Expect::SerializationFails,
        // Point reads only. Disjoint from B's key in the `TxWrite` column, and
        // conflicted on the write set before the read set matters elsewhere.
        AState::OpenWrite => match b {
            BOp::TxWrite => Expect::Commits,
            _ => Expect::Conflicts,
        },
        // The DDL check precedes read-set validation, so SSI never gets a say.
        AState::OpenDdl => Expect::DdlConflicts,
        // `delete_table` alone never reads: no read-set entry for `T` exists,
        // so `validate_read_set`'s deleted-table arm has nothing to fire on.
        // This is the cell the design spec got wrong.
        AState::Delete => {
            if b_removes {
                Expect::Commits
            } else {
                Expect::Conflicts
            }
        }
        // Both read the recreated table, so where they committed under SI —
        // against the two removing columns — SSI aborts them instead. Against
        // an installing column they conflict on the write set first, exactly as
        // under SI. The bug-5 equality between these two therefore *survives*
        // the change of isolation level: both halves move together, which is
        // the property those packed tests assert.
        AState::DeleteRecreate | AState::WriteDeleteRecreate => {
            if b_removes {
                Expect::SerializationFails
            } else {
                Expect::Conflicts
            }
        }
        // Already conflicting on its write set in every column.
        AState::DeleteRecreateWrite => Expect::Conflicts,
    }
}

/// Runs an `#[ignore]`d observation cell, first asserting it is still listed in
/// [`unruled`].
///
/// This is the half of the duplication that can be checked: if someone rules on
/// a cell and removes it from [`unruled`] but leaves its observation tests
/// standing, the `--ignored` pass goes red and names the cell instead of quietly
/// asserting an outcome twice under two contradictory policies.
///
/// Only the three Serializable observations call it. The three
/// `SnapshotIsolation` ones above are left **byte-identical** to what Tasks 1–4
/// wrote, deliberately: two of them appear in mutation A's and B's calibration
/// lists, and the value of those lists rests on the cells not having been
/// touched since they were measured. One assertion per cell is enough to tie the
/// lists together, and it costs nothing to put it on the new tests rather than
/// the calibrated ones.
fn check_unruled_cell(a: AState, b: BOp, iso: IsolationLevel, expect: Expect) {
    assert!(
        unruled(a, b),
        "{a:?} x {b:?} is not in `unruled`, but is still carried as an \
         `#[ignore]`d observation. Ruling on a cell means deleting its \
         observation tests too — see the procedure on `unruled`."
    );
    check_cell_at(a, b, iso, expect);
}

/// Runs one whole column under `Serializable` and reports **every** failing
/// cell, not just the first.
///
/// The unruled cells are skipped; see [`unruled`].
fn check_ssi_column(b: BOp) {
    let failures: Vec<String> = ALL_A
        .iter()
        .filter(|a| !unruled(**a, b))
        .filter_map(|a| cell_failure(*a, b, IsolationLevel::Serializable, ssi_expect(*a, b)))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of the {:?} column failed under Serializable:\n  {}",
        failures.len(),
        b,
        failures.join("\n  ")
    );
}

/// `OpenOnly` aborts (the read of a table B replaced); everything else is as
/// under SI.
#[test]
fn serializable_bulk_replace_column() {
    check_ssi_column(BOp::BulkReplace);
}

#[test]
fn serializable_bulk_delta_column() {
    check_ssi_column(BOp::BulkDelta);
}

/// The only column reached through `validate_read_set`'s *scan* arm rather than
/// its deleted-table arm, and so the only one where the two `OpenOnly`-shaped
/// states part company: `OpenOnly`'s `len()` is a scan and aborts, while
/// `OpenWrite`'s point read of key 1 does not collide with B's key 2 and still
/// commits.
#[test]
fn serializable_concurrent_write_column() {
    check_ssi_column(BOp::TxWrite);
}

/// The bug-3 and bug-5 column. Three of its cells commit under SI and abort
/// here — `OpenOnly`, `DeleteRecreate`, `WriteDeleteRecreate` — and the last two
/// abort *together*, so the equality the packed bug-5 test asserts under SI
/// holds under SSI as well, at a different value. That is the answer to the
/// question Task 4 left open ("whether that equality is asserted per isolation
/// level or only under SI"): per isolation level, and it holds at both.
#[test]
fn serializable_concurrent_delete_column() {
    check_ssi_column(BOp::TxDelete);
}

#[test]
fn serializable_bulk_batch_column() {
    check_ssi_column(BOp::BulkBatch);
}

/// The `TxDelete` column's keep-set-drop twin, and it diverges identically —
/// which is the point. `validate_read_set` keys off `deleted_tables`, and a
/// keep-set drop populates that field exactly as `delete_table` does, so SSI
/// cannot tell the two spellings apart either. Two of this column's seven cells
/// are unruled and skipped; five run.
#[cfg(feature = "persistence")]
#[test]
fn serializable_stream_install_drop_column() {
    check_ssi_column(BOp::StreamInstallDrop);
}

// ── The unruled cells, observed under `Serializable` ────────────────────────
//
// Recorded, not pinned — the same standing as their SI twins above, and for the
// same reason: no ruling exists, and running the matrix at a second isolation
// level does not create one. A red test here means the behaviour of an unruled
// cell changed; re-read the SI twin's doc comment and decide, do not assume a
// regression.

/// UNRESOLVED. The SSI reading of
/// [`delete_recreate_write_vs_concurrent_delete_outcome_is_unruled`].
///
/// **Unchanged from SI, measured 2026-08-06:** `Err(WriteConflict)`. A's write
/// set for the recreated `T` is non-empty and `T` is in B's `deleted_tables`, so
/// `validate_write_set` conflicts it before `validate_read_set` is consulted —
/// which is why the read of the recreated table, the very thing that moves
/// `DeleteRecreate` and `WriteDeleteRecreate` to `SerializationFailure` in this
/// column, changes nothing here.
///
/// So SSI does not help with the open question. It does narrow it slightly: the
/// fourth corner is now the odd one out under *both* isolation levels, and any
/// ruling that makes it commit has to survive both.
#[test]
#[ignore = "unresolved: see the doc comment; do not pin until ruled on"]
fn delete_recreate_write_vs_concurrent_delete_under_serializable_is_unruled() {
    check_unruled_cell(
        AState::DeleteRecreateWrite,
        BOp::TxDelete,
        IsolationLevel::Serializable,
        Expect::Conflicts,
    );
}

/// UNRESOLVED. The SSI reading of
/// [`open_only_vs_stream_install_drop_outcome_is_unruled`], and **the tenth
/// divergent cell** — the one the live tests in this section do not pin.
///
/// **Changed from SI, measured 2026-08-06:** `Ok` under SI, `SerializationFailure`
/// here. A's `t.len()` records a scan of `T`, B's keep-set drop puts `T` in
/// `deleted_tables`, and `validate_read_set` aborts on that without reaching
/// either key-comparing arm.
///
/// This does not resolve the SI question, and is not evidence about it. The open
/// question is whether a write-free open should survive a snapshot install that
/// dropped the table — a question about `installed_tables` and Phase 2's
/// `(None, _)` arm. SSI answers a *different* one, by refusing the transaction
/// on the grounds that it read a table the install removed, which it would do
/// whatever those two mechanisms decided. A cell that aborts before the disputed
/// code runs cannot be a vote on it.
#[cfg(feature = "persistence")]
#[test]
#[ignore = "unresolved: see the doc comment; do not pin until ruled on"]
fn open_only_vs_stream_install_drop_under_serializable_is_unruled() {
    check_unruled_cell(
        AState::OpenOnly,
        BOp::StreamInstallDrop,
        IsolationLevel::Serializable,
        Expect::SerializationFails,
    );
}

/// UNRESOLVED. The SSI reading of
/// [`delete_recreate_write_vs_stream_install_drop_outcome_is_unruled`].
///
/// **Unchanged from SI, measured 2026-08-06:** `Err(WriteConflict)`, for the
/// same reason as its `TxDelete` twin — the write set decides it first. The two
/// stay twins under SSI, so a ruling still has to move them together.
#[cfg(feature = "persistence")]
#[test]
#[ignore = "unresolved: see the doc comment; do not pin until ruled on"]
fn delete_recreate_write_vs_stream_install_drop_under_serializable_is_unruled() {
    check_unruled_cell(
        AState::DeleteRecreateWrite,
        BOp::StreamInstallDrop,
        IsolationLevel::Serializable,
        Expect::Conflicts,
    );
}
