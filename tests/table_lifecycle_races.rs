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

use std::collections::{BTreeMap, BTreeSet};

use ultima_db::{
    BulkDelta, BulkLoadInput, BulkLoadOptions, BulkSource, Error, IndexKind, Store, StoreConfig,
    WriterMode,
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
    /// `Store::bulk_load` Delta — inserts id 4, updates id 1, deletes id 2,
    /// so it touches rows A may also have touched.
    BulkDelta,
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
        BOp::BulkDelta => {
            store
                .bulk_load::<String>(T, bulk_delta_input(), BulkLoadOptions::default())
                .unwrap();
        }
    }

    // ── A commits; observe ──────────────────────────────────────────────
    let commit = wtx.commit();

    let rtx = store.begin_read(None).unwrap();
    let (rows_after, table_present) = match rtx.open_table::<String>(T) {
        Ok(t) => (
            t.iter().map(|(k, v)| (k, v.clone())).collect::<Vec<_>>(),
            true,
        ),
        Err(_) => (Vec::new(), false),
    };
    CellOutcome { commit, rows_after, table_present }
}

/// The keys A is *entitled* to have changed on top of B — and only when A's
/// commit returned `Ok`, since a conflicting A must leave B's effect entirely
/// intact.
///
/// `None` means "A replaced the whole table": it deleted `T`, so per-key claims
/// about B's rows do not survive it. Those states are checked against
/// [`a_whole_table`] instead — `None` is *not* a licence to assert nothing.
fn a_touched(a: AState) -> Option<&'static [u64]> {
    match a {
        // Contributed nothing to `T`: no row write, and DDL touches no row.
        AState::OpenOnly | AState::OpenDdl => Some(&[]),
        // `update(1, "from_a")`.
        AState::OpenWrite => Some(&[1]),
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
fn a_whole_table(a: AState) -> Option<Vec<(u64, String)>> {
    match a {
        AState::Delete => None,
        // The reopen does not retract the delete; the table comes back fresh
        // and empty, and A wrote nothing into it.
        AState::DeleteRecreate | AState::WriteDeleteRecreate => Some(Vec::new()),
        // Fresh table, so auto-increment restarts at 1.
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
fn a_effect_intact(a: AState, out: &CellOutcome) -> Result<(), String> {
    match (a_whole_table(a), out.table_present) {
        (None, false) => Ok(()),
        (None, true) => Err(format!(
            "A deleted the table and committed Ok; it is still present as {:?}",
            out.rows_after
        )),
        (Some(_), false) => {
            Err("A recreated the table and committed Ok; it is absent".to_string())
        }
        (Some(want), true) if out.rows_after == want => Ok(()),
        (Some(want), true) => Err(format!(
            "A recreated the table as {want:?} and committed Ok; found {:?}",
            out.rows_after
        )),
    }
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
/// actually committed.
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
    let exempt: &[u64] = match (out.commit.is_ok(), a_touched(a)) {
        (true, None) => return a_effect_intact(a, out),
        (true, Some(keys)) => keys,
        // A did not commit: it gets no exemption at all.
        (false, _) => &[],
    };

    if !out.table_present {
        return Err("B installed the table; it is now absent".to_string());
    }

    let after: BTreeMap<u64, &str> = out
        .rows_after
        .iter()
        .map(|(k, v)| (*k, v.as_str()))
        .collect();

    for (k, want) in b_rows(b) {
        if exempt.contains(&k) {
            continue;
        }
        match after.get(&k) {
            Some(got) if *got == want => {}
            Some(got) => return Err(format!("row {k}: B installed {want:?}, found {got:?}")),
            None => return Err(format!("row {k}: B installed {want:?}, now absent")),
        }
    }

    let installed: BTreeSet<u64> = b_rows(b).into_iter().map(|(k, _)| k).collect();
    for (k, v) in &out.rows_after {
        if !installed.contains(k) && !exempt.contains(k) {
            return Err(format!(
                "row ({k}, {v:?}) is not one B installed — pre-B state resurrected"
            ));
        }
    }
    Ok(())
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

    // (ii) THE UMBRELLA PROPERTY: whatever committed Ok is never silently
    // reverted.
    if let Err(why) = effect_intact(a, b, &out) {
        panic!(
            "{label}: {why} — something that committed Ok was silently \
             reverted. rows={:?} commit={:?}",
            out.rows_after, out.commit
        );
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
/// Verified — dropping `|| cws.installed_tables.contains(deleted)` turns the
/// other five red and leaves this one green.
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
