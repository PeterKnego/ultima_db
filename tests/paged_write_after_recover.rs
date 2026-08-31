// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 4 fix round 1 (opus review, Criticals 1/2): regression coverage for
//! two block-leaf-mutation paths the `materialize()`-in-`make_mut` stopgap
//! did NOT cover, both reachable from the public API on a *recovered* paged
//! store (block leaves only exist post-decode) with unmodified source:
//!
//! - `Table::delete` triggering a leaf merge (`fix_underfull_child` ->
//!   `merge_with_right` -> `absorb`). `absorb` consumes the sibling **by
//!   value** as a `Child`, never through `make_mut`, so the stopgap's
//!   choke point never ran on it — a block-leaf sibling's values were
//!   silently dropped (unique-owner branch) or the I-A invariant was
//!   corrupted (shared branch, plain `Clone` on a block leaf), surfacing
//!   later as `"I-A: in-block entry requires a block"`.
//! - `Table::insert_batch`'s task51 bulk-append fast path
//!   (`BTree::extend_from_sorted` -> `BulkBuilder::seed_from_spine`, and a
//!   split mid-batch -> `redistribute_tail`), which reads a block-leaf
//!   spine/sibling by shared reference for a move into a fresh builder
//!   level — also never routes through `make_mut`.
//!
//! Both were dead code before task 4 (nothing produced a live block leaf to
//! exercise them); task 4's decode-to-block made them live. Fixed in
//! `src/btree.rs`'s `absorb`/`seed_from_spine`/`redistribute_tail`.
//!
//! Both tests use `WriterMode::MultiWriter` to bypass the SingleWriter
//! write overlay (`src/overlay.rs`, cap 32) — with the overlay live, a
//! handful of `Table::delete`/`insert` calls just buffer as tombstones/puts
//! and never reach the B-tree's structural insert/delete code at all, which
//! would make these regression tests pass for the wrong reason (never
//! exercising the buggy path) rather than the right one.

#![cfg(feature = "persistence")]

use ultima_db::{Durability, PagedOptions, Persistence, Store, StoreConfig, WalWrite, WriterMode};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

/// Same shape as `tests/paged_recovery.rs`'s helper of the same name, plus
/// `WriterMode::MultiWriter` (see the module doc) so every write bypasses
/// the write overlay and reaches the tree directly.
fn store(dir: &std::path::Path) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().build())
        .unwrap();
    let s = Store::new(StoreConfig::builder().persistence(p).writer_mode(WriterMode::MultiWriter).build()).unwrap();
    s.register_table_paged::<Row>("rows").unwrap();
    s
}

fn write_rows(s: &Store, n: u64) {
    let mut w = s.begin_write(None).unwrap();
    let mut t = w.open_table::<Row>("rows").unwrap();
    for i in 0..n {
        t.insert(Row { v: i }).unwrap();
    }
    w.commit().unwrap();
}

fn delete_key(s: &Store, k: u64) {
    let mut w = s.begin_write(None).unwrap();
    w.open_table::<Row>("rows").unwrap().delete(k).unwrap();
    w.commit().unwrap();
}

/// Critical 1 repro (opus review). Deleting key-by-key from the very front
/// of a freshly recovered tree turns out NOT to reach a genuinely
/// untouched sibling on its own: at the default fanout every leaf but the
/// last is frozen at exactly 32 entries (the left half of its own long-ago
/// split — sequential ascending insert always grows the *right*-continuing
/// half), one more than `MIN_KEYS` (31), so `fix_underfull_child` always
/// finds a `rotate_left` available first — and `rotate_left` itself calls
/// `make_mut` on the right sibling, safely materializing it *before* any
/// merge ever touches it. Verified empirically (this test's construction
/// was derived by tracing `fix_underfull_child`/`absorb` against the
/// pre-fix code, not guessed): a plain "reopen once, delete downward"
/// sweep never hits `absorb` on a block leaf.
///
/// So this test *plants* a same-session-untouched `MIN_KEYS`-sized sibling
/// across a checkpoint boundary instead:
/// 1. Build 500 rows (>2 leaves at MAX_KEYS=63), checkpoint.
/// 2. Reopen + recover, delete key 33 alone (33 lands in leaf 1; this
///    single delete shrinks it 32 -> 31 = `MIN_KEYS` without underflowing
///    it — no rebalance fires, so leaf 1 is untouched by anything else).
///    Checkpoint again: leaf 1 is now permanently 31 entries on disk.
/// 3. Reopen + recover *again* (both leaf 0 and leaf 1 come back cold,
///    block-shaped — a fresh `recover()` resets materialization state,
///    though not entry counts). Delete keys 1 and 2: leaf 0 (32 -> 31 ->
///    30) underflows below `MIN_KEYS`. `fix_underfull_child(idx=0)` has no
///    left sibling to rotate from and finds the right sibling (leaf 1) at
///    exactly `MIN_KEYS` — not `> MIN_KEYS`, so no `rotate_left` — forcing
///    `merge_with_right` -> `absorb` on a leaf 1 that has not been
///    `make_mut`'d even once this session. This is the counterexample the
///    module doc describes.
#[test]
fn delete_after_recover_survives_a_leaf_merge_of_a_never_touched_sibling() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        write_rows(&s, 500);
        s.checkpoint().unwrap();
    }
    {
        let s = store(d.path());
        s.recover().unwrap();
        delete_key(&s, 33); // 32 -> 31 = MIN_KEYS, quietly, no rebalance
        s.checkpoint().unwrap();
    }
    let s = store(d.path());
    s.recover().unwrap();
    delete_key(&s, 1); // leaf 0: 32 -> 31
    delete_key(&s, 2); // leaf 0: 31 -> 30, underfull -> merge_with_right(leaf 1)

    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.len(), 497);
    for k in 3..500u64 {
        if k == 33 {
            continue; // deleted in round 2
        }
        assert_eq!(t.get(k).unwrap().v, k - 1, "row {k}");
    }
}

/// Critical 2 repro (opus review): `Table::insert_batch` on a recovered
/// paged table. All-new, ascending keys past the current max take the
/// task51 `BulkBuilder` fast path (`extend_from_sorted`), which unzips the
/// tree's right spine (`seed_from_spine`) — ending at the rightmost leaf,
/// freshly decoded and block-shaped since `recover()`. A large-enough batch
/// also forces at least one `redistribute_tail` rebalance.
#[test]
fn insert_batch_after_recover_reads_back() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        write_rows(&s, 500);
        s.checkpoint().unwrap();
    }
    let s = store(d.path());
    s.recover().unwrap();

    let mut w = s.begin_write(None).unwrap();
    {
        let mut t = w.open_table::<Row>("rows").unwrap();
        let batch: Vec<Row> = (500..700u64).map(|v| Row { v }).collect();
        let ids = t.insert_batch(batch).unwrap();
        assert_eq!(ids.len(), 200);
    }
    w.commit().unwrap();

    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.len(), 700);
    for k in 1..=700u64 {
        assert_eq!(t.get(k).unwrap().v, k - 1, "row {k}");
    }
}
