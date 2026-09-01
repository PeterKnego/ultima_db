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

use ultima_db::{Durability, PagedOptions, Persistence, Residency, Store, StoreConfig, WalWrite, WriterMode};

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

// ---------------------------------------------------------------------------
// Task 9: pin-aware reconciliation + `pinned_leaf_bytes`.
//
// Retained snapshots older than the latest CoW-share pre-demotion leaves
// with it; `demote_pass` only ever demotes the LATEST snapshot's tables
// (`Store::demote_pass_inner` reads `inner.snapshots[&latest]`), so a leaf
// that only an older retained snapshot still references never gets a
// demote debit — the bytes stay resident in memory, invisible to
// `resident_leaf_bytes_est`. `pinned_leaf_bytes` is the checkpoint-end
// reconciliation's answer to "how much of that is there right now".
// ---------------------------------------------------------------------------

/// Like [`multiwriter_store_with`], but with a caller-chosen
/// `num_snapshots_retained` — needed here to control exactly how many
/// older snapshots stay retained (and therefore pinned) at a time.
fn multiwriter_store_with_retention(dir: &std::path::Path, opts: PagedOptions, retained: usize) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(opts)
        .unwrap();
    let s = Store::new(
        StoreConfig::builder()
            .persistence(p)
            .writer_mode(WriterMode::MultiWriter)
            .num_snapshots_retained(retained)
            .build(),
    )
    .unwrap();
    s.register_table_paged::<Row>("rows").unwrap();
    s
}

/// Task 9 review I-1: the accounting proptest oracle
/// (`store::tests::pin_aware_reconcile` in `src/store.rs`) is tautological
/// — `pinned := total - resident` makes `resident + pinned == total` true
/// by construction, and its "independent" reference walk re-invokes the
/// very dedup function under test, so it proves only that a set union is
/// order-independent. Mutation-proven: with dedup disabled entirely (the
/// `seen` check ignored), the whole suite — including that proptest —
/// stayed green.
///
/// This is the point test that actually exercises dedup: two retained
/// snapshots whose "rows" table is not merely *unmodified since*, but
/// LITERALLY the same tree — same `Child` pointers all the way down. `v2`
/// (latest) only ever opens "other", never "rows", so `v1`'s and `v2`'s
/// "rows" entries are the identical `Arc`-shared table (see
/// `untouched_tables_survive_commit` in `tests/store_integration.rs` for
/// the same guarantee this relies on).
///
/// "rows" is pinned `Residency::Resident` so `demote_pass` skips it
/// entirely. This was not just belt-and-suspenders while getting this test
/// right: a first version without it failed even on CORRECT (deduped)
/// code, because `demote_pass` demotes by CoW-replacing the parent chain
/// of whatever it demotes in latest's OWN tree (never mutating a shared
/// `Child` in place — see the review's own Priority-1 soundness note on
/// `BTree::demote_leaves`) — so demoting "rows" from `v2` (latest) would
/// itself orphan `v1`'s still-resident original copy, which is exactly the
/// pin *mechanism* this whole feature exists to detect, just triggered by
/// the demote pass instead of a write. `Residency::Resident` sidesteps
/// that entirely so this test isolates the ONE thing it's here to check:
/// dedup of content that never diverges at all.
///
/// With dedup working, `v1`'s walk retraces `v2`'s already-`seen` pointers
/// and contributes nothing new: `pinned_leaf_bytes` must be exactly `0`.
/// Without dedup, `v1`'s walk would independently re-sum "rows"'s full
/// resident bytes, and `pinned_leaf_bytes` would equal that (nonzero)
/// amount instead.
///
/// Verified red against the mutant this targets (dedup disabled — see
/// task-9-report.md's fix-round-1 section for the exact mutation and
/// failure output).
#[test]
fn pinned_leaf_bytes_is_zero_for_a_fully_shared_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let p = Persistence::standalone(dir.path(), Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().memory_budget_bytes(1 << 30).build())
        .unwrap();
    let s = Store::new(
        StoreConfig::builder()
            .persistence(p)
            .writer_mode(WriterMode::MultiWriter)
            .num_snapshots_retained(4)
            .build(),
    )
    .unwrap();
    s.register_table_paged::<Row>("rows").unwrap();
    s.register_table_paged::<Row>("other").unwrap();

    // v1: "rows" populated (2,000 rows). "other" does not exist yet.
    write_rows(&s, 2_000);
    s.set_residency("rows", Residency::Resident).unwrap();
    // v2 (latest): a commit that opens ONLY "other" — "rows" is never
    // touched, so v2's "rows" entry is v1's exact same Arc-shared table,
    // not a clone that merely happens to still agree.
    {
        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<Row>("other").unwrap();
            t.insert(Row { v: 1 }).unwrap();
        }
        w.commit().unwrap();
    }

    // A checkpoint writes every dirty leaf (both tables, first time either
    // has been checkpointed) and would normally run demote_pass — but
    // "rows" is pinned Resident, so demote_pass skips it unconditionally;
    // its leaves stay resident, real bytes for a broken dedup to
    // double-count, and structurally UNTOUCHED (no CoW divergence risk
    // from the demote pass itself).
    s.checkpoint().unwrap();

    let stats = s.paged_stats().unwrap();
    assert!(
        stats.resident_leaf_bytes_est > 0,
        "rows' leaves must still be resident (Residency::Resident, never demoted) -- \
         otherwise there is nothing here for a broken dedup to double-count, and this test \
         would pass vacuously either way"
    );
    assert_eq!(
        stats.pinned_leaf_bytes, 0,
        "v1 (retained, non-latest) and v2 (latest) share the IDENTICAL rows tree -- a \
         correctly deduped walk counts it once (as v2's own resident), not once per \
         retained snapshot"
    );
}

/// The Step 1 scenario: `num_snapshots_retained(4)`, several commits that
/// each CoW the SAME key's leaf (so every commit orphans the previous
/// leaf, rather than superseding it the way distinct-key updates would —
/// see this test's own walkthrough below), checkpoints that demote the
/// latest snapshot's own leaves but cannot reach the 3 older retained
/// snapshots' orphaned ones.
///
/// Every commit below is immediately followed by its own `checkpoint()`
/// call, deliberately — not batched at the end. Batching would let
/// `PagedState::last_root` (the checkpoint diff base held for the
/// dead-page-punch schedule; it also keeps its target version's `Arc`
/// alive across `gc()` regardless of the retention window) lag several
/// versions behind `latest_version` for the whole batch, during which
/// EVERY leaf any of those commits touches gets faulted in as a shared
/// `Child` before its own commit's CoW splits it away — orphaning a copy
/// in the stale, artificially-extended-lifetime snapshot `last_root` is
/// still pointing at, on top of whatever this test intends to measure.
/// Checkpointing every commit keeps `last_root == latest_version`
/// throughout, so retention behaves exactly like "keep the `N` most
/// recent snapshots" with no extra lag term to account for.
///
/// This test does NOT try to bring `pinned_leaf_bytes` back to `0` by
/// committing more writes to this same store — see
/// `pinned_leaf_bytes_returns_to_zero_once_not_retained` below for why
/// that specific approach (suggested as one option in the original task
/// brief) does not work, and what does.
#[test]
fn pinned_leaf_bytes_reflects_older_retained_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let s = multiwriter_store_with_retention(
        dir.path(),
        PagedOptions::builder().memory_budget_bytes(1 << 30).build(),
        4,
    );
    write_rows(&s, 2_000);
    s.checkpoint().unwrap();
    s.checkpoint().unwrap(); // demote everything, second sweeps stragglers
    let base = s.paged_stats().unwrap();
    assert_eq!(base.resident_leaf_bytes_est, 0, "fully demoted base tree");
    assert_eq!(base.pinned_leaf_bytes, 0, "nothing retained yet diverges from latest");

    // Reference unit: the exact fault-in credit of one safely-interior leaf
    // (key 200 — same safe pick `demote_debit_matches_fault_in_credit_across_cycles`
    // uses), independent of the key (5) this test repeatedly updates below.
    let unit = {
        let before = s.paged_stats().unwrap().resident_leaf_bytes_est;
        let r = s.begin_read(None).unwrap();
        assert!(r.open_table::<Row>("rows").unwrap().get(200).is_some());
        s.paged_stats().unwrap().resident_leaf_bytes_est - before
    };
    assert!(unit > 0, "a real leaf's fault-in credit must be nonzero");
    // Demote the key-200 leaf back out so it doesn't pollute the counts below.
    s.checkpoint().unwrap();
    s.checkpoint().unwrap();
    assert_eq!(s.paged_stats().unwrap().resident_leaf_bytes_est, 0);
    assert_eq!(s.paged_stats().unwrap().pinned_leaf_bytes, 0);

    // Update the SAME key (5) six times, checkpointing after each: every
    // commit CoWs a brand-new in-memory leaf for it, orphaning the leaf the
    // PREVIOUS commit just created — that previous leaf is referenced only
    // by the snapshot version that commit produced, never again touched,
    // and never demoted (demote_pass only walks the latest snapshot's
    // tree, and the orphaned leaf isn't part of it once superseded).
    // `auto_snapshot_gc` (default on, runs at commit time) plus
    // `num_snapshots_retained(4)` keeps only the most recent 4 snapshots at
    // any point, so once six updates have gone by, exactly 3 non-latest
    // snapshots remain, each pinning its own distinct orphaned key-5 leaf.
    // Each commit's own checkpoint gives its freshly-touched leaf a second
    // chance (the accessed bit set at creation/fault-in survives one
    // sweep) rather than demoting it immediately — that's fine, it just
    // means the LAST commit's checkpoint leaves latest's own key-5 leaf
    // resident for one more cycle, cleaned up by the extra checkpoint
    // below.
    for i in 0..6u64 {
        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<Row>("rows").unwrap();
            t.update(5, Row { v: 1_000 + i }).unwrap();
        }
        w.commit().unwrap();
        s.checkpoint().unwrap();
    }
    s.checkpoint().unwrap(); // one more pass: demotes latest's own now-quiet key-5 leaf

    let stats = s.paged_stats().unwrap();
    assert_eq!(
        stats.resident_leaf_bytes_est, 0,
        "latest's own key-5 leaf must be fully demoted after the extra checkpoint"
    );
    assert_eq!(
        stats.pinned_leaf_bytes,
        3 * unit,
        "exactly the 3 non-latest retained snapshots' own orphaned key-5 leaves, each the \
         size of one interior leaf, must be counted pinned"
    );

    // ------------------------------------------------------------------
    // What does NOT bring this back to 0, and why (investigated, not
    // guessed): committing more writes to keep `latest_version` moving,
    // hoping the 3 pinning snapshots above age out of the retention
    // window. They DO age out — but EVERY further write that touches an
    // on-disk (previously-demoted) leaf shared with its own base snapshot
    // faults that leaf in for BOTH before its own CoW splits them apart,
    // permanently pinning a fresh orphan in whichever snapshot it was
    // built from. With `num_snapshots_retained(4)` (3 non-latest slots
    // always occupied) this is a steady state, not a transient: each new
    // write's own predecessor becomes a new pin at the same moment the
    // oldest one ages out — this is exactly the spec's "56x" pin
    // phenomenon (§1), not a test artifact, and Task 11's enforcement
    // (adaptive retention shrink) exists because ordinary retry/backoff
    // traffic cannot self-resolve it. `s.gc()` alone doesn't help either:
    // with exactly `num_snapshots_retained` snapshots present,
    // `gc_inner`'s `len <= retain_count` fast path has nothing to evict —
    // every one of the 4 present is legitimately within the configured
    // window.
    //
    // Review M-5: asserted below, not left as prose — two different
    // continuations of the SAME steady state, matching what the review's
    // own probe measured (§0/§6 of task-9-review.md).
    // ------------------------------------------------------------------

    // Same-key continuation: 6 more update+checkpoint rounds, still all on
    // key 5. The review's probe confirmed this holds EXACTLY at `3 * unit`
    // for 12 further rounds — a genuine steady state, not a one-off
    // snapshot of this test's own specific setup.
    for i in 6..12u64 {
        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<Row>("rows").unwrap();
            t.update(5, Row { v: 1_000 + i }).unwrap();
        }
        w.commit().unwrap();
        s.checkpoint().unwrap();
        s.checkpoint().unwrap();
        assert_eq!(
            s.paged_stats().unwrap().pinned_leaf_bytes,
            3 * unit,
            "round {i}: same-key updates hold pinned_leaf_bytes at EXACTLY 3 * unit — a real \
             steady state, each round's new orphan replacing the one that just aged out"
        );
    }

    // Spread-key continuation — the review's empirical CORRECTION to this
    // test's own original framing: updating a DIFFERENT key each round is
    // NOT bounded to the same-key case's exact `3 * unit`. Every commit's
    // base snapshot still picks up its own orphan (same mechanism as
    // above), but a spread of keys touches MORE distinct leaves per
    // retained snapshot before that snapshot ages out — the review
    // measured `4-5 * unit` here, oscillating, never settling. What DOES
    // still hold is the floor: `(num_snapshots_retained - 1) * unit`
    // (== `3 * unit`) is a lower bound regardless of key pattern, since
    // this mechanism only ever ADDS orphans relative to the same-key case,
    // never fewer — assert that, not an exact value the workload doesn't
    // actually hit.
    for key in [400u64, 600, 800, 1_000, 1_200, 1_400] {
        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<Row>("rows").unwrap();
            t.update(key, Row { v: 2_000 + key }).unwrap();
        }
        w.commit().unwrap();
        s.checkpoint().unwrap();
        s.checkpoint().unwrap();
        let pinned = s.paged_stats().unwrap().pinned_leaf_bytes;
        assert!(
            pinned >= 3 * unit,
            "key {key}: pinned_leaf_bytes ({pinned}) must never drop below the \
             (num_snapshots_retained - 1) * unit floor ({}) — spread-key writes can and do \
             oscillate ABOVE it (the review measured 4-5x unit here), never below",
            3 * unit
        );
    }
}

/// What DOES bring `pinned_leaf_bytes` back to `0`: retention no longer
/// keeping a diverged snapshot alive at all. A fresh store with
/// `num_snapshots_retained(1)` runs the identical divergent-update
/// workload as the test above; once writes stop and the one WriteTx-held
/// reference to its own base snapshot (kept alive across exactly the
/// `gc()` call inside its own `commit()` — see the walkthrough below) is
/// dropped, an explicit `Store::gc()` call collects it and
/// `pinned_leaf_bytes` reads `0`.
///
/// The mid-loop stats prove the WriteTx-reference mechanism, not just the
/// end state: `num_snapshots_retained(1)` still shows exactly 2 retained
/// snapshots (`latest` and its immediate predecessor) and `pinned_leaf_bytes
/// == unit` after every single update+checkpoint in the loop — never 0
/// mid-stream, even though only 1 snapshot was asked to be retained. Each
/// iteration's `WriteTx` holds its own base snapshot's `Arc` alive
/// internally until it is dropped at the end of its scope, so the `gc()`
/// call inside that SAME `commit()` still sees `Arc::strong_count > 1` on
/// it and skips it; only the FOLLOWING iteration's commit (after the
/// previous `WriteTx` has gone out of scope) finds it unprotected. A plain
/// `checkpoint()` cannot substitute for the final explicit `gc()` here —
/// checkpointing reconciles whatever `inner.snapshots` currently holds, it
/// does not itself evict; only `gc()` (automatic at commit, or called
/// explicitly) removes a map entry.
#[test]
fn pinned_leaf_bytes_returns_to_zero_once_not_retained() {
    let dir = tempfile::tempdir().unwrap();
    let s = multiwriter_store_with_retention(
        dir.path(),
        PagedOptions::builder().memory_budget_bytes(1 << 30).build(),
        1,
    );
    write_rows(&s, 2_000);
    s.checkpoint().unwrap();
    s.checkpoint().unwrap();
    assert_eq!(s.paged_stats().unwrap().resident_leaf_bytes_est, 0);
    assert_eq!(s.paged_stats().unwrap().pinned_leaf_bytes, 0);

    let unit = {
        let before = s.paged_stats().unwrap().resident_leaf_bytes_est;
        let r = s.begin_read(None).unwrap();
        assert!(r.open_table::<Row>("rows").unwrap().get(200).is_some());
        s.paged_stats().unwrap().resident_leaf_bytes_est - before
    };
    assert!(unit > 0);
    s.checkpoint().unwrap();
    s.checkpoint().unwrap();
    assert_eq!(s.paged_stats().unwrap().pinned_leaf_bytes, 0);

    for i in 0..6u64 {
        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<Row>("rows").unwrap();
            t.update(5, Row { v: 1_000 + i }).unwrap();
        }
        w.commit().unwrap();
        s.checkpoint().unwrap();
        s.checkpoint().unwrap();
        let stats = s.paged_stats().unwrap();
        assert_eq!(
            stats.resident_leaf_bytes_est, 0,
            "iteration {i}: latest's own leaf must be fully demoted by the second checkpoint"
        );
        assert_eq!(
            stats.pinned_leaf_bytes, unit,
            "iteration {i}: exactly one trailing snapshot (this commit's own base, still \
             referenced by its now-out-of-scope WriteTx at commit time) stays pinned even \
             under num_snapshots_retained(1)"
        );
    }

    // Writes have stopped; the last iteration's WriteTx is out of scope, so
    // nothing protects its base snapshot from gc() anymore.
    s.gc();
    s.checkpoint().unwrap();
    s.checkpoint().unwrap();
    let aged = s.paged_stats().unwrap();
    assert_eq!(
        aged.pinned_leaf_bytes, 0,
        "with nothing but latest ever retained, an explicit gc() after writes stop must \
         collect the one trailing snapshot and bring pinned_leaf_bytes to 0"
    );
    assert_eq!(aged.resident_leaf_bytes_est, 0);
}

/// Task 10, spec §6 ("Hard-cap clock eviction"): a single demote sweep
/// under second-chance semantics can evict ~0 bytes even while deeply over
/// budget — every leaf that was resident and read since the last pass
/// survives sweep 1 with its accessed bit merely cleared, not evicted (the
/// same mechanic `tests/paged_demotion.rs::accessed_leaf_survives_one_pass`
/// checks at one-leaf scale, here at whole-table scale). Pre-task-10,
/// `demote_pass` only ever swept once per `checkpoint()`, so a store like
/// this one — read all over right before a checkpoint under real budget
/// pressure — would stay over budget forever, one checkpoint after
/// another, since every pass's lone sweep just re-clears the same bits
/// its predecessor cleared without ever harvesting them. This is that red:
/// touch every leaf, then checkpoint under a budget the whole resident set
/// overshoots, and require the pass to converge within the ONE
/// `checkpoint()` call — not merely make another checkpoint's worth of
/// progress next time.
///
/// Fix round 1 (review Important-2, Important-3): a `Store` with
/// `memory_budget_bytes` set wakes the background checkpointer the moment
/// a fault-in crosses budget (`PagedSource::read_node`, edge-triggered —
/// see `PagedStats::wake_checkpointer`), and that thread's own
/// `checkpoint()` call runs its own cycling `demote_pass` concurrently
/// with the read-scan below — the review instrumented this exact test
/// body and measured `clock_cycles`/`forced_evictions` already nonzero
/// (3 / 29) by the time the scan finished, so asserting on raw totals
/// after the *measured* `checkpoint()` call cannot tell that call's own
/// work apart from the background thread's. Both counters are captured
/// immediately before the measured call and asserted as **deltas**. The
/// review also measured the pre-measurement precondition
/// (`resident_leaf_bytes_est > BUDGET`) holding by only a ~2x margin at
/// 5,000 rows (126,960 vs 65,536) even after that background activity —
/// thin enough that a slightly more effective background pass could shave
/// it under budget before the measured call ever starts pressure. 50,000
/// rows (10x the tree) widens that margin by roughly the same factor,
/// without changing what either counter measures.
#[test]
fn hard_cap_clock_cycles_to_convergence() {
    const BUDGET: u64 = 64 * 1024;
    const ROWS: u64 = 50_000;
    let dir = tempfile::tempdir().unwrap();
    let s = store_with(dir.path(), PagedOptions::builder().memory_budget_bytes(BUDGET).build());
    write_rows(&s, ROWS);
    // Writes every leaf to disk and assigns page ids; demotes everything
    // (freshly batch-built, nothing accessed yet — see `write_rows`'s doc),
    // so resident starts back at ~0 going into the read-scan below.
    s.checkpoint().unwrap();

    // Read-scan every row: faults every leaf back in and marks it
    // accessed, right before the checkpoint this test actually measures.
    {
        let r = s.begin_read(None).unwrap();
        let t = r.open_table::<Row>("rows").unwrap();
        for k in 1..=ROWS {
            assert!(t.get(k).is_some(), "key {k} missing");
        }
    }
    let before = s.paged_stats().unwrap();
    assert!(
        before.resident_leaf_bytes_est > BUDGET,
        "the read-scan above must put the whole table back over budget, or this test proves \
         nothing (resident {}, budget {BUDGET})",
        before.resident_leaf_bytes_est
    );

    s.checkpoint().unwrap();

    let after = s.paged_stats().unwrap();
    assert!(
        after.resident_leaf_bytes_est <= BUDGET,
        "resident {} must converge back under budget {BUDGET} within this one checkpoint's \
         pass, not merely clear second-chance bits for a later pass to harvest",
        after.resident_leaf_bytes_est
    );
    assert!(
        after.forced_evictions - before.forced_evictions > 0,
        "leaves that survived cycle 1's second chance must be evicted on a later cycle of \
         THIS pass — before={}, after={}",
        before.forced_evictions, after.forced_evictions
    );
    assert!(
        after.clock_cycles - before.clock_cycles >= 2,
        "convergence here needs at least 2 cycles within THIS pass: cycle 1 only clears \
         second-chance bits (every leaf was just read), cycle 2 harvests them — before={}, \
         after={}",
        before.clock_cycles, after.clock_cycles
    );
}

/// Task 10 un-evictable floor: a `Residency::Resident` table is exempt
/// from demotion outright (`demote_pass_inner`'s per-table check, from
/// before this task) — the clock must recognize that and terminate rather
/// than spin forever chasing bytes it can never reclaim.
///
/// Fix round 1 (review Important-4): a lone `Resident` table short-circuits
/// `demote_pass_inner`'s per-table loop before it ever calls `paged_demote`
/// or looks at a single leaf (`src/store.rs`, the `if tbl.residency() ==
/// Residency::Resident { break; }` check) — so the original version of
/// this test only proved the pass terminates when there is *nothing to
/// sweep at all*. It never exercised "swept a real tree, every leaf came
/// back second-chanced or pinned", which is exactly Critical-1's shape
/// (this same `Resident`-floor-over-budget setup, but with a SECOND,
/// evictable table under concurrent *random-key* point reads that keep
/// re-arming its leaves' accessed bits every cycle — a sequential reader
/// would settle into the zero-cycle exit; random access does not). Before
/// the `Store::MAX_DEMOTE_CYCLES` hard cap landed, this exact
/// configuration measured 60k+ cycles / 6+ seconds in review, returning
/// only when the reader workload stopped — a regression here would hang,
/// not fail, so this now asserts the pass is bounded by the cap rather
/// than merely "eventually returns" (which a hang trivially also does,
/// from the test harness's perspective, once it times out).
#[test]
fn hard_cap_clock_bounded_under_concurrent_random_access() {
    // Mirrors `Store::MAX_DEMOTE_CYCLES` (private to the crate, not part
    // of the public API — hardcoded here, not imported).
    const MAX_DEMOTE_CYCLES: u64 = 8;
    const BUDGET: u64 = 64 * 1024;
    const HOT_ROWS: u64 = 200_000; // un-evictable floor, well over BUDGET alone
    const COLD_ROWS: u64 = 200_000; // evictable, spread across many leaves -- scale matters:
    // review's repro needed ~200k rows (thousands of leaves) for random-key
    // reader pressure to reliably outrun the sweep; smaller trees (tried
    // 20k) sometimes settle under the cap on their own even without it,
    // which would make this a flaky, not reliable, regression check.

    let dir = tempfile::tempdir().unwrap();
    let p = Persistence::standalone(dir.path(), Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().memory_budget_bytes(BUDGET).build())
        .unwrap();
    let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s.register_table_paged::<Row>("hot").unwrap();
    s.register_table_paged::<Row>("rows").unwrap();

    {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("hot").unwrap();
        t.insert_batch((0..HOT_ROWS).map(|v| Row { v }).collect()).unwrap();
        w.commit().unwrap();
    }
    write_rows(&s, COLD_ROWS);
    s.checkpoint().unwrap(); // writes+demotes both tables while still Lazy
    s.set_residency("hot", Residency::Resident).unwrap();

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..4u64)
        .map(|seed| {
            let s = s.clone();
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                // xorshift64 — no external RNG dependency, deterministic
                // per-thread seed, genuinely non-sequential key order
                // (the property Critical-1's repro needs; a sequential
                // scan would re-touch each leaf in a tight window and
                // settle into the zero-cycle exit instead).
                let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ (seed.wrapping_mul(0x1000_0001) | 1);
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let k = 1 + (x % COLD_ROWS);
                    let r = s.begin_read(None).unwrap();
                    let _ = r.open_table::<Row>("rows").unwrap().get(k);
                }
            })
        })
        .collect();

    // Give the readers a moment to actually start hammering before the
    // measured checkpoint begins.
    std::thread::sleep(std::time::Duration::from_millis(20));

    let before = s.paged_stats().unwrap();
    s.checkpoint().unwrap(); // must return, bounded by MAX_DEMOTE_CYCLES
    let after = s.paged_stats().unwrap();

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in readers {
        h.join().unwrap();
    }

    assert!(
        after.clock_cycles - before.clock_cycles <= MAX_DEMOTE_CYCLES,
        "one demote_pass must never exceed MAX_DEMOTE_CYCLES ({MAX_DEMOTE_CYCLES}) cycles, \
         even under sustained concurrent random-key pressure — before={}, after={}",
        before.clock_cycles, after.clock_cycles
    );
    assert!(
        after.resident_leaf_bytes_est > BUDGET,
        "the un-evictable 'hot' table alone exceeds budget, so pressure must remain even \
         after the pass returns (resident {})",
        after.resident_leaf_bytes_est
    );
}
