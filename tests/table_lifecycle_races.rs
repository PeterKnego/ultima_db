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
///
/// Only `OpenOnly` and `OpenDdl` are exercised by this task's two cells; the
/// rest of the matrix (later tasks in this series) uses the remaining
/// variants, hence `allow(dead_code)` here rather than trimming the enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
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
///
/// `Conflicts` is unused by this task's two cells; later cells in the
/// matrix use it, hence `allow(dead_code)` here rather than trimming it.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
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
            t.iter().map(|(k, v)| (k, v.clone())).collect::<Vec<_>>(),
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
