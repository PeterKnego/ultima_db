// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 5/6 equivalence oracle for value-block leaves (spec §8.1).
//!
//! A paged table's leaves come back from `NodeCodec::decode` **block**-shaped
//! (task 4): the values live inline in the node's `block`, not behind
//! per-entry `Arc`s. Every mutation of such a leaf has to rebuild that block
//! rather than shift `entries` out from under it. This drives identical op
//! sequences against a paged (block-leaf) store and a plain in-memory store
//! and asserts they agree — after the batch, and again after
//! `checkpoint()` + reopen + `recover()`, which is what forces the *cold*
//! reads back through freshly decoded block leaves.
//!
//! Scope note (why the interesting assertions live in two places): whether
//! a leaf is block-backed is invisible from outside the crate — no public
//! API exposes it — so every *representation* assertion lives in a test
//! module inside `src/`, and this file asserts behaviour (values) only.
//! Since Task 6 the representation half is pinned at two levels:
//!
//! - `src/btree.rs`'s `btree::tests::block_leaves` — both mutation paths at
//!   `BTree` level, plus the exact `clone_value` counts that pin spec §4's
//!   one-pass rule;
//! - `src/store.rs`'s
//!   `paged_table_leaves_stay_block_backed_across_a_mixed_table_workload` —
//!   the end-to-end claim, that a real `Store`/`Table` workload over a
//!   recovered paged table leaves **every** data leaf block-backed. That is
//!   the store-level assertion Task 5's review asked for (warning 3) and
//!   what this file cannot make.
//!
//! `Table` drives the in-place (`_mut`) mutation family, which Task 6 made
//! block-aware, plus the rebalance code it shares with the immutable path
//! (`fix_underfull_child` -> `rotate_right`/`rotate_left` /
//! `merge_with_left`/`merge_with_right` -> `absorb`, block-preserving since
//! Task 5) — and the whole decode -> mutate -> encode -> decode round trip.
//!
//! `WriterMode::MultiWriter` throughout, for the reason
//! `tests/paged_write_after_recover.rs` documents: the SingleWriter write
//! overlay (`src/overlay.rs`, cap 32) buffers small single-row writes and
//! would keep most of an op sequence from ever reaching the B-tree's
//! structural insert/delete code at all.

#![cfg(feature = "persistence")]

use std::path::Path;

use proptest::prelude::*;
use ultima_db::{Durability, PagedOptions, Persistence, Store, StoreConfig, WalWrite, WriterMode};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

/// A paged store over `dir` with the table registered. Not recovered.
fn paged_store(dir: &Path) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().build())
        .unwrap();
    let s = Store::new(
        StoreConfig::builder()
            .persistence(p)
            .writer_mode(WriterMode::MultiWriter)
            .build(),
    )
    .unwrap();
    s.register_table_paged::<Row>("t").unwrap();
    s
}

/// The in-memory oracle: same table, no persistence, no blocks — a plain
/// all-`Arc` B-tree. `MultiWriter` too, so both sides see the same overlay
/// (i.e. none) and the same commit path.
fn plain_store() -> Store {
    let s = Store::new(
        StoreConfig::builder()
            .writer_mode(WriterMode::MultiWriter)
            .build(),
    )
    .unwrap();
    s.register_table::<Row>("t").unwrap();
    s
}

fn reopen_and_recover(dir: &Path) -> Store {
    let s = paged_store(dir);
    s.recover().unwrap();
    s
}

#[derive(Clone, Debug)]
enum Op {
    /// Explicit-key write (insert-or-replace).
    Put(u64, u64),
    /// Replace only if the key exists — exercises the `update` probe, which
    /// reads a value out of a block leaf before writing.
    UpdateIfPresent(u64, u64),
    /// Delete only if the key exists — the underflow/rebalance driver.
    DeleteIfPresent(u64),
    /// Auto-id batch append past the current max key: the task51
    /// `BulkBuilder` fast path (`extend_from_sorted` -> `seed_from_spine`),
    /// which reads the recovered tree's right-spine leaf while it is still
    /// block-shaped.
    InsertBatch(usize),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0u64..500, 0u64..1_000).prop_map(|(k, v)| Op::Put(k, v)),
        2 => (0u64..500, 0u64..1_000).prop_map(|(k, v)| Op::UpdateIfPresent(k, v)),
        3 => (0u64..500).prop_map(Op::DeleteIfPresent),
        1 => (1usize..40).prop_map(Op::InsertBatch),
    ]
}

/// Apply `ops` to `s`, one transaction per op (so each one really commits
/// through the tree rather than accumulating in a single dirty table).
fn apply_ops(s: &Store, ops: &[Op]) {
    for op in ops {
        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<Row>("t").unwrap();
            match *op {
                Op::Put(k, v) => t.put(k, Row { v }).unwrap(),
                Op::UpdateIfPresent(k, v) => {
                    if t.get(k).is_some() {
                        t.update(k, Row { v }).unwrap();
                    }
                }
                Op::DeleteIfPresent(k) => {
                    let before = t.get(k).map(|r| r.v);
                    if let Some(before) = before {
                        let removed = t.delete(k).unwrap();
                        // `Table::delete` hands back the removed value, which
                        // on a block leaf has to be cloned out of the block —
                        // it must be the live value, not a stale or dangling
                        // slot. (Read *before* the delete: reading after it
                        // would always be `None` and assert nothing.)
                        assert_eq!(removed.v, before, "delete({k}) returned the wrong value");
                        assert!(t.get(k).is_none(), "delete({k}) left the row behind");
                    }
                }
                Op::InsertBatch(n) => {
                    let batch: Vec<Row> = (0..n as u64).map(|i| Row { v: 10_000 + i }).collect();
                    t.insert_batch(batch).unwrap();
                }
            }
        }
        w.commit().unwrap();
    }
}

fn dump(s: &Store) -> Vec<(u64, u64)> {
    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("t").unwrap();
    let mut out: Vec<(u64, u64)> = t.iter().map(|(k, row)| (k, row.v)).collect();
    out.sort_unstable();
    assert_eq!(out.len(), t.len(), "iter and len disagree");
    out
}

fn assert_tables_equal(a: &Store, b: &Store, what: &str) {
    let (da, db) = (dump(a), dump(b));
    assert_eq!(da.len(), db.len(), "{what}: row count differs");
    assert_eq!(da, db, "{what}: contents differ");
    // Point reads too: `iter` and `get` take different routes into a leaf.
    let r = a.begin_read(None).unwrap();
    let t = r.open_table::<Row>("t").unwrap();
    for (k, v) in &db {
        assert_eq!(t.get(k).map(|row| row.v), Some(*v), "{what}: get({k})");
    }
}

proptest! {
    // No `cfg!(miri)` case cap here, deliberately (review round 1, Minor 4):
    // this file does real filesystem I/O — `tempfile::tempdir`, `checkpoint`,
    // `recover` — which Miri refuses without `-Zmiri-disable-isolation`, so
    // an integration test in this file never runs under Miri and a cap would
    // read as coverage that does not exist. The Miri-facing block-leaf tests
    // are the `--lib` ones in `src/btree.rs::tests::block_leaves`, whose
    // `cfg!(miri)` caps are live.
    //
    // 64 cases of up to 120 ops is the figure the task brief specifies and
    // costs well under a second here; it was reduced during implementation
    // and is restored (review round 1, Warning 2).
    #![proptest_config(ProptestConfig {
        cases: 64,
        ..ProptestConfig::default()
    })]

    /// The oracle. Identical op sequences against a block-leaf paged table
    /// and a plain in-memory table must agree, and must still agree after
    /// a checkpoint + reopen + recover has round-tripped every leaf
    /// through the page file (which is what makes them block-shaped again
    /// on the way back in).
    #[test]
    fn paged_blocks_equal_in_memory_oracle(ops in prop::collection::vec(op_strategy(), 1..120)) {
        let dir = tempfile::tempdir().unwrap();
        let plain = plain_store();

        {
            let paged = paged_store(dir.path());
            apply_ops(&paged, &ops);
            apply_ops(&plain, &ops);
            assert_tables_equal(&paged, &plain, "after ops");
            paged.checkpoint().unwrap();
        }

        // Cold: every leaf comes back off `pages.bin` block-shaped.
        let reopened = reopen_and_recover(dir.path());
        assert_tables_equal(&reopened, &plain, "after recover");

        // ...and mutating those cold block leaves keeps agreeing. This is
        // the half that actually exercises block-shaped mutation, since
        // the pre-checkpoint run above starts from an all-Arc tree.
        let more: Vec<Op> = ops.iter().rev().take(30).cloned().collect();
        apply_ops(&reopened, &more);
        apply_ops(&plain, &more);
        assert_tables_equal(&reopened, &plain, "after mutating cold block leaves");

        // And once more across a second checkpoint boundary, so the
        // mutated (block-rebuilt) leaves are themselves re-encoded and
        // re-decoded.
        reopened.checkpoint().unwrap();
        drop(reopened);
        let again = reopen_and_recover(dir.path());
        assert_tables_equal(&again, &plain, "after second recover");
    }
}

/// Deterministic companion to the proptest: a workload shaped to force leaf
/// **merges and rotations** on cold, block-shaped leaves (the shared
/// `fix_underfull_child` machinery Task 5 made block-preserving), rather
/// than relying on a random sequence to stumble into them.
#[test]
fn deleting_a_recovered_tree_down_through_merges_matches_the_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let plain = plain_store();
    let ops: Vec<Op> = (0..600u64).map(|k| Op::Put(k, k * 3)).collect();
    {
        let paged = paged_store(dir.path());
        apply_ops(&paged, &ops);
        apply_ops(&plain, &ops);
        paged.checkpoint().unwrap();
    }
    let s = reopen_and_recover(dir.path());
    assert_tables_equal(&s, &plain, "after recover");

    // Delete two thirds of the keys, interleaved so leaves underflow at
    // many different points: every rotation and merge here runs against
    // leaves that came off disk block-shaped this session.
    let deletes: Vec<Op> = (0..600u64).filter(|k| k % 3 != 0).map(Op::DeleteIfPresent).collect();
    apply_ops(&s, &deletes);
    apply_ops(&plain, &deletes);
    assert_tables_equal(&s, &plain, "after merge-heavy deletes");

    s.checkpoint().unwrap();
    drop(s);
    let again = reopen_and_recover(dir.path());
    assert_tables_equal(&again, &plain, "after recover of the merged tree");
    assert_eq!(dump(&again).len(), 200);
}

/// Deterministic companion aimed at the in-place **update** hot path
/// (Task 6): every key of a recovered, block-shaped table is overwritten,
/// twice, one transaction at a time. A `block[pos] = v` store that used the
/// wrong slot — the failure mode a rebuild-based path cannot have, and the
/// one an op-mix proptest only hits by luck — shows up here as a value
/// mismatch on a *neighbouring* key, and survives into the next checkpoint.
#[test]
fn updating_every_key_of_a_recovered_tree_in_place_matches_the_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let plain = plain_store();
    let ops: Vec<Op> = (0..800u64).map(|k| Op::Put(k, k)).collect();
    {
        let paged = paged_store(dir.path());
        apply_ops(&paged, &ops);
        apply_ops(&plain, &ops);
        paged.checkpoint().unwrap();
    }
    let s = reopen_and_recover(dir.path());
    assert_tables_equal(&s, &plain, "after recover");

    for round in 1..=2u64 {
        let updates: Vec<Op> = (0..800u64).map(|k| Op::UpdateIfPresent(k, k + round * 1_000)).collect();
        apply_ops(&s, &updates);
        apply_ops(&plain, &updates);
        assert_tables_equal(&s, &plain, "after in-place update round");
    }

    s.checkpoint().unwrap();
    drop(s);
    let again = reopen_and_recover(dir.path());
    assert_tables_equal(&again, &plain, "after recover of the updated tree");
    assert_eq!(dump(&again), (0..800u64).map(|k| (k, k + 2_000)).collect::<Vec<_>>());
}
