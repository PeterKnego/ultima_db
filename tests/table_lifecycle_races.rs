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
    /// Another transaction updates id 2 — a row `AState::OpenWrite` does *not*
    /// touch, so the disjoint-key case is the one under test.
    TxWrite,
    /// Another transaction deletes the table. Unlike every other `BOp` this one
    /// leaves `T` absent, which is why the oracle has to ask
    /// [`b_leaves_table`] before it can say what "B's effect intact" means.
    TxDelete,
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
        // Also hand-derived: `seed()` with id 2 updated in place. Keeping id 1
        // at its seed value is deliberate — it is the row `AState::OpenWrite`
        // claims, so the disjoint-key merge has something to get wrong.
        BOp::TxWrite => vec![(1, "seed1".to_string()), (2, "from_b".to_string())],
        // Nothing left to enumerate; presence is the whole of B's effect here.
        BOp::TxDelete => Vec::new(),
    }
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
        BOp::BulkReplace | BOp::BulkDelta | BOp::TxWrite => true,
        BOp::TxDelete => false,
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
    match (a_whole_table(a, b), out.table_present) {
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
    let exempt: &[(u64, &str)] = match (out.commit.is_ok(), a_touched(a)) {
        (true, None) => return a_effect_intact(a, b, out),
        (true, Some(rows)) => rows,
        // A did not commit: it gets no exemption at all.
        (false, _) => &[],
    };
    let is_exempt = |k: u64| exempt.iter().any(|(ek, _)| *ek == k);

    let after: BTreeMap<u64, &str> = out
        .rows_after
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
    match (b_leaves_table(b), out.table_present) {
        (true, false) => return Err("B installed the table; it is now absent".to_string()),
        (false, true) => {
            return Err(format!(
                "B deleted the table; it is present as {:?}",
                out.rows_after
            ));
        }
        _ => {}
    }
    if !out.table_present {
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
    for (k, v) in &out.rows_after {
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
        _ => None,
    }
}

/// Runs a cell and asserts all three parts of the oracle.
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

    // (iii) the table's existence, where a specific ruling pinned it
    if let Some(want) = expected_presence(a, b) {
        assert_eq!(
            out.table_present, want,
            "{label}: expected table_present={want}, rows={:?} commit={:?}",
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
/// Verified — dropping `|| cws.installed_tables.contains(deleted)` turns its
/// **six** delete-flavoured siblings in the two bulk columns red (`Delete`,
/// `DeleteRecreate` and `WriteDeleteRecreate`, each against Replace and Delta)
/// and leaves this one green. The count is six, not the five an earlier
/// revision of this comment claimed.
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
// The column bugs 3 and 5 live in, and the only one that can calibrate them.
// B's `CommittedWriteSet` here names `T` in `deleted_tables` **and nowhere
// else** — `tables` is empty because `delete_table` never opens the table, and
// `installed_tables` is empty because an ordinary commit installs nothing. So
// this is the one column where "B removed `T`" is the entire post-condition,
// and `table_present` carries the assertion that row contents cannot.

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

/// Bug 5 (fixed in 68cd794), and **the only cell that can calibrate it** — the
/// bulk columns cannot, see `write_delete_recreate_over_a_bulk_replace_conflicts`.
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
#[test]
fn write_delete_recreate_matches_delete_recreate_against_a_concurrent_delete() {
    check_cell(AState::WriteDeleteRecreate, BOp::TxDelete, Expect::Commits);
    check_cell(AState::DeleteRecreate, BOp::TxDelete, Expect::Commits);
}

// ── Cells awaiting a ruling ────────────────────────────────────────────────
//
// Carried as `#[ignore]`d tests rather than omitted, so the question is visible
// in the matrix instead of being a hole nobody can see. Per the design spec:
// "The implementer must surface these rather than choose, and no such cell is
// pinned until a ruling exists." Run them with `-- --ignored`; the doc comments
// record what they do *today*, which is not the same as what they should do.

/// UNRESOLVED — needs a ruling before it is pinned.
///
/// Delete vs delete. Non-conflicting under `SnapshotIsolation`, which is what
/// this suite runs: `validate_write_set`'s first loop needs a non-empty write
/// set for `T` (A has none — `delete_table` alone never opens the table, so no
/// entry is ever created) and its second loop needs `T` in B's `tables` or
/// `installed_tables` (B's commit puts it in neither). Both parties want the
/// table gone, so nothing is lost.
///
/// So what is left unruled is narrow: whether SI *should* conflict when both
/// parties delete the same table, given that neither can observe the other's
/// rows afterwards. Rule (ii) holds either way, so this is a question about
/// outcome (i) alone.
///
/// **`Serializable` is not the reason, contrary to the design spec** (`…
/// races-design.md:119-121`, which lists this cell as one where "SSI's
/// `validate_read_set` aborts it"). It does not. `AState::Delete` calls only
/// `delete_table`, which never reads, so there is no read-set entry for `T` and
/// `validate_read_set`'s delete arm (`src/store.rs:4541`) cannot fire. Measured
/// under `IsolationLevel::Serializable`: still `Ok`. Anyone extending this suite
/// to SSI should not start here.
///
/// Where SSI *does* diverge, measured 2026-08-05 across all 28 cells — six of
/// them, every one `Ok` under SI and `SerializationFailure` under SSI:
///
/// - `OpenOnly x BulkReplace`, `OpenOnly x BulkDelta`,
///   `OpenOnly x TxWrite`, `OpenOnly x TxDelete`
/// - `DeleteRecreate x TxDelete`, `WriteDeleteRecreate x TxDelete`
///
/// That is the real scope of an SSI pass: `OpenOnly`'s `t.len()` records a read
/// that every B invalidates, and the two recreate cells read the table they
/// recreated. Note the second pair is the bug-5 cell, so an SSI pass must decide
/// whether that equality is asserted per isolation level or only under SI.
///
/// **Current behaviour, measured 2026-08-05 under SI:** commits `Ok`; `T` is
/// absent afterwards. Absence is asserted, not merely observed — `a_whole_table`
/// returns `None` for `Delete`, so `a_effect_intact`'s `(None, true)` arm fails
/// this cell if `T` survives.
#[test]
#[ignore = "unresolved: see the doc comment; do not pin until ruled on"]
fn delete_vs_concurrent_delete_outcome_is_unruled() {
    check_cell(AState::Delete, BOp::TxDelete, Expect::Commits);
}

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
