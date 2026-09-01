// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 8: one accounting unit everywhere — credit (fault-in,
//! `PagedSource::read_node`, task 4), debit (demote, `BTree::demote_leaves`),
//! dirty-credit (`Child::resident_new`/`make_mut`), and the checkpoint-end
//! reconciliation walk (`resident_leaf_estimate`) must all speak
//! `BTreeNode::leaf_bytes()` — not a flat `NODE_BYTES` that under-credits
//! any leaf holding a value block. See
//! `docs/superpowers/specs/2026-08-31-paged-leaf-arena-memory-honesty-design.md`
//! §5 and `docs/tasks/task*_paged_leaf_value_blocks.md`.
//!
//! Pre-task-8, the debit side (`Store::demote_pass_inner`) multiplied
//! `demoted_count * paged_node_bytes()` (a flat per-node estimate), while
//! the credit side already used `leaf_bytes()` (task 4) — a real block leaf
//! is under-debited by its block bytes on every demote, so the resident
//! estimate drifts upward every checkpoint→fault→checkpoint cycle instead
//! of returning to the same value. The first test below is exactly that
//! regression, run for three cycles.

#![cfg(feature = "persistence")]

use ultima_db::{Durability, PagedOptions, Persistence, Store, StoreConfig, WalWrite, WriterMode};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

fn store_with(dir: &std::path::Path, opts: PagedOptions) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(opts)
        .unwrap();
    let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s.register_table_paged::<Row>("rows").unwrap();
    s
}

/// Like [`store_with`], but `MultiWriter` — whose write-overlay cap is
/// unconditionally `0` (see `CLAUDE.md`: "MultiWriter stores... overlay cap
/// is always 0"). A single-row `update()` on a `SingleWriter` table this
/// small buffers in the bounded write overlay (`src/overlay.rs`, cap 32) and
/// never touches `Child::make_mut` at all, which would make the dirty-credit
/// test below observe nothing. `MultiWriter` sends every single-row write
/// straight to the tree, exercising the real block-leaf CoW path.
fn multiwriter_store_with(dir: &std::path::Path, opts: PagedOptions) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(opts)
        .unwrap();
    let s = Store::new(
        StoreConfig::builder()
            .persistence(p)
            .writer_mode(WriterMode::MultiWriter)
            .build(),
    )
    .unwrap();
    s.register_table_paged::<Row>("rows").unwrap();
    s
}

/// One batch insert (not a loop of `insert()`s): the bulk fast path builds
/// leaves directly with no leaf marked "accessed" just from being built —
/// see the identical helper and its doc in `tests/paged_demotion.rs`. Needed
/// here for the same reason: a put-loop-built tree's first demote pass would
/// legitimately demote nothing (every leaf still holds its "just touched"
/// second chance), which is not what these tests are measuring.
fn write_rows(s: &Store, n: u64) {
    let mut w = s.begin_write(None).unwrap();
    let mut t = w.open_table::<Row>("rows").unwrap();
    t.insert_batch((0..n).map(|v| Row { v }).collect()).unwrap();
    w.commit().unwrap();
}

/// The Step 1 regression, credit/debit/reconcile as one unit.
///
/// A naive "does `resident_leaf_bytes_est` return to 0 after a full
/// checkpoint→fault→checkpoint cycle" check does NOT discriminate old vs.
/// new code here: `Store::checkpoint_impl_paged`'s F1 reconciliation
/// (spec §5 "the safety net") re-bases the running counter from an exact
/// tree walk (`resident_leaf_estimate`) at the end of *every* `checkpoint()`
/// call whenever `memory_budget_bytes` is `Some` — so once every leaf is
/// actually demoted again, the walk sums zero leaves and reports 0
/// regardless of whether the walk's own per-leaf formula (or the demote
/// debit that ran moments earlier) was ever correct. The debit's accuracy
/// is invisible from outside a single `checkpoint()` call for exactly that
/// reason: the very next reconcile overwrites whatever it left behind.
///
/// So this test instead keeps a fixed set of "hot" leaves permanently
/// resident (re-touched every cycle, so they win their second chance and
/// are never actually demoted — same mechanic as
/// `accessed_leaf_survives_one_pass` in `tests/paged_demotion.rs`) and
/// checks the reconciled estimate against an *independently measured*
/// reference: the exact fault-in credit of a single one of those leaves
/// (task 4's `PagedSource::read_node` credit, already `leaf_bytes()`-based
/// and unaffected by this task). Every interior leaf of a densely
/// bulk-built tree holds exactly `MAX_KEYS` rows (only the tail leaf is
/// underfull — see `from_sorted_tail_underfull` in `btree.rs`), so one
/// leaf's fault-in credit is the per-leaf byte cost for all five hot
/// leaves below, and the reconciled total across 3 repeated cycles must
/// equal exactly `5 * unit` every time — not merely "the same wrong value
/// every time", which the walk's own self-consistency would produce
/// regardless of formula correctness.
///
/// Pre-task-8, `resident_leaf_estimate` (the walk the reconciliation calls)
/// summed a flat `Child::NODE_BYTES` per resident leaf instead of
/// `leaf_bytes()` — smaller than `unit` for any leaf holding rows — so the
/// reconciled total undershoots `5 * unit`. That's this test's red.
#[test]
fn demote_debit_matches_fault_in_credit_across_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let s = store_with(dir.path(), PagedOptions::builder().memory_budget_bytes(1 << 30).build());
    write_rows(&s, 2_000);
    s.checkpoint().unwrap(); // demotes every quiet leaf (all of them, batch-built)
    s.checkpoint().unwrap(); // sweep any stragglers
    assert_eq!(
        s.paged_stats().unwrap().resident_leaf_bytes_est,
        0,
        "fully demoted tree must report 0 resident leaf bytes"
    );

    // Five distinct, safely-interior leaves (well clear of the last ~63
    // keys, which may make up an underfull tail leaf).
    let hot_keys = [5u64, 200, 400, 600, 800];

    // Reference unit: the exact fault-in credit of ONE of these leaves.
    let unit = {
        let before = s.paged_stats().unwrap().resident_leaf_bytes_est;
        let r = s.begin_read(None).unwrap();
        assert!(r.open_table::<Row>("rows").unwrap().get(hot_keys[0]).is_some());
        s.paged_stats().unwrap().resident_leaf_bytes_est - before
    };
    assert!(unit > 0, "a real leaf's fault-in credit must be nonzero");

    for cycle in 0..3 {
        // Touch every hot leaf (re-touching hot_keys[0] too, so its
        // accessed bit is freshly set for this cycle's second chance).
        {
            let r = s.begin_read(None).unwrap();
            let t = r.open_table::<Row>("rows").unwrap();
            for &k in &hot_keys {
                assert!(t.get(k).is_some(), "cycle {cycle}: key {k} missing");
            }
        }
        // One checkpoint: demote_pass gives every hot leaf its second
        // chance (all freshly accessed, so none actually demotes — nothing
        // else is resident to compete with them), then the F1
        // reconciliation re-bases the counter from the exact walk.
        s.checkpoint().unwrap();
        let est = s.paged_stats().unwrap().resident_leaf_bytes_est;
        let expected = hot_keys.len() as u64 * unit;
        assert_eq!(
            est,
            expected,
            "cycle {cycle}: reconciled resident_leaf_bytes_est must equal exactly \
             {} x the single-leaf fault-in credit; drift = {}",
            hot_keys.len(),
            est as i64 - expected as i64
        );
    }
}

/// The dirty-credit half of the same unit (spec §4's clause, assigned to
/// this task): a block-leaf CoW must credit `dirty_bytes` by exactly
/// `NODE_BYTES + block bytes` (`BTreeNode::leaf_bytes()`), not the flat
/// `NODE_BYTES` a non-block node still credits.
///
/// No internal constant (`Child::NODE_BYTES`) is available from an
/// integration test, so this proves the point structurally instead: fault
/// leaf A back in and dirty it (via a committed update to the same key),
/// which also CoWs the root — the sole internal level above a 2,000-row
/// tree — crediting the root's own flat `NODE_BYTES` once. That leaves the
/// root permanently dirty (until the next checkpoint), so a *second*
/// fault+update on a different key/leaf (B) no longer re-credits the root
/// (`Child::make_mut`'s `was_clean` gate) — isolating leaf B's dirty-credit
/// contribution exactly. That isolated `dirty_bytes` delta must equal the
/// `resident_leaf_bytes_est` delta the earlier fault-in credited for that
/// same leaf B — same leaf, same entry count (a same-key update never
/// changes `entries.len()`), so `leaf_bytes()` computed at fault time and at
/// dirty time must be numerically identical. Pre-task-8, the dirty credit
/// was a flat `NODE_BYTES` while the fault-in credit was already
/// `leaf_bytes()` — the two would only coincide by construction on an
/// (impossible, for a real row) zero-entry leaf.
#[test]
fn dirty_credit_matches_fault_in_credit_for_the_same_leaf() {
    let dir = tempfile::tempdir().unwrap();
    let s = multiwriter_store_with(dir.path(), PagedOptions::builder().memory_budget_bytes(1 << 30).build());
    write_rows(&s, 2_000);
    s.checkpoint().unwrap();
    s.checkpoint().unwrap();
    assert_eq!(s.paged_stats().unwrap().resident_leaf_bytes_est, 0);

    // Fault leaf A (key 5) and dirty it — also CoWs (and dirties) the root.
    {
        let r = s.begin_read(None).unwrap();
        assert!(r.open_table::<Row>("rows").unwrap().get(5).is_some());
    }
    {
        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<Row>("rows").unwrap();
            t.update(5, Row { v: 999 }).unwrap();
        }
        w.commit().unwrap();
    }

    // Fault leaf B (key 1,000 — far enough from key 5 to land in a
    // different leaf at MAX_KEYS=63-ish density) and capture the resident
    // credit delta: exactly leaf B's `leaf_bytes()`.
    let resident_before = s.paged_stats().unwrap().resident_leaf_bytes_est;
    {
        let r = s.begin_read(None).unwrap();
        assert!(r.open_table::<Row>("rows").unwrap().get(1_000).is_some());
    }
    let resident_after = s.paged_stats().unwrap().resident_leaf_bytes_est;
    let resident_delta_b = resident_after - resident_before;
    assert!(resident_delta_b > 0, "faulting a fresh leaf must raise the resident estimate");

    // Dirty leaf B — the root is already dirty from the first commit above,
    // so this commit's `dirty_bytes` delta is leaf B's contribution alone.
    let dirty_before = s.paged_stats().unwrap().dirty_bytes;
    {
        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<Row>("rows").unwrap();
            t.update(1_000, Row { v: 999 }).unwrap();
        }
        w.commit().unwrap();
    }
    let dirty_after = s.paged_stats().unwrap().dirty_bytes;
    let dirty_delta_b = dirty_after - dirty_before;

    assert_eq!(
        dirty_delta_b, resident_delta_b,
        "a block-leaf CoW must credit dirty_bytes by exactly the same leaf_bytes() the \
         fault-in credited to resident_leaf_bytes for the identical leaf (pre-task-8 the \
         dirty credit was a flat NODE_BYTES, smaller than leaf_bytes() for any leaf with \
         entries)"
    );
}
