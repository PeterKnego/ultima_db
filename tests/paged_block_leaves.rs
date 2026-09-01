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

// ---------------------------------------------------------------------------
// Task 7: API boundaries — delete clone-out, bulk-path zero-clone, and the
// task59 MultiWriter merge, all against a recovered, block-backed table.
// ---------------------------------------------------------------------------

/// Step 1(a): `Table::delete` on a recovered, block-backed leaf returns the
/// exact value that was removed (the `merged_get_arc` -> `BTree::get_arc`
/// clone-out documented at `Table::delete`), and the table matches the
/// in-memory oracle afterward. `deleting_a_recovered_tree_down_through_
/// merges_matches_the_oracle` above already stresses delete-driven
/// rebalancing broadly; this is the narrow, dedicated version the task
/// brief asks for: value correctness on a handful of individually deleted,
/// still block-shaped keys, spanning the front, middle, and tail of the
/// tree.
#[test]
fn delete_on_a_recovered_block_backed_table_returns_the_correct_value() {
    let dir = tempfile::tempdir().unwrap();
    let plain = plain_store();
    let ops: Vec<Op> = (0..300u64).map(|k| Op::Put(k, k * 7 + 1)).collect();
    {
        let paged = paged_store(dir.path());
        apply_ops(&paged, &ops);
        apply_ops(&plain, &ops);
        paged.checkpoint().unwrap();
    }
    let s = reopen_and_recover(dir.path());
    assert_tables_equal(&s, &plain, "after recover");

    for k in [0u64, 1, 2, 149, 150, 151, 298, 299] {
        let expected = k * 7 + 1;
        let mut w = s.begin_write(None).unwrap();
        let removed = w.open_table::<Row>("t").unwrap().delete(k).unwrap();
        w.commit().unwrap();
        assert_eq!(
            removed.v, expected,
            "delete({k}) on a block-backed leaf returned the wrong value"
        );

        let mut wp = plain.begin_write(None).unwrap();
        wp.open_table::<Row>("t").unwrap().delete(k).unwrap();
        wp.commit().unwrap();
    }
    assert_tables_equal(&s, &plain, "after targeted deletes of block-backed rows");

    // A repeat delete of an already-removed key is `KeyNotFound`, not a
    // stale clone of the old value — the negative side of the same
    // clone-out boundary.
    let mut w = s.begin_write(None).unwrap();
    let err = w.open_table::<Row>("t").unwrap().delete(0u64);
    assert!(matches!(err, Err(ultima_db::Error::KeyNotFound)));
}

/// Static clone counter for [`CountingRow`]. Module-scoped, and now shared
/// by three tests (the bulk zero-clone test plus the two `delete` cost-model
/// pins added in fix round 1) — under the crate's default parallel test
/// execution those three would otherwise race on the same counter (a
/// concurrently-running test's clones would leak into another's `before`/
/// `after` delta). `COUNTING_CLONES_LOCK` below serializes them; every test
/// that reads or resets this counter must hold it for its entire body.
static COUNTING_CLONES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Serializes every test that measures [`COUNTING_CLONES`] against the
/// other such tests in this binary, so a `before`/`after` delta in one can
/// never observe clones from another running concurrently. Held for the
/// duration of each guarded test's body (`let _guard = COUNTING_CLONES_LOCK
/// .lock().unwrap();`), not just around the measured section — the whole
/// point is that no *other* guarded test's body can interleave with it.
static COUNTING_CLONES_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct CountingRow {
    v: u64,
}

impl Clone for CountingRow {
    fn clone(&self) -> Self {
        COUNTING_CLONES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        CountingRow { v: self.v }
    }
}

fn counting_paged_store(dir: &Path) -> Store {
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
    s.register_table_paged::<CountingRow>("c").unwrap();
    s
}

/// Step 1(b): the task51 `BulkBuilder` fast path (`Table::insert_batch` ->
/// `BTree::extend_from_sorted`, taken here because the batch lands on a
/// fresh table with no prior max key) owns every value it inserts — the
/// caller's `Vec<R>` is moved straight into fresh `Arc`s
/// (`records.into_iter().map(Arc::new)`) — and `BulkBuilder::freeze_leaf`/
/// `freeze_internal` always build `block: None` nodes, so there is no block
/// to clone out of. The bulk path must therefore cost exactly zero
/// `R::clone` calls — the same fn `PagedSource::clone_value` rides
/// (captured at `register_table_paged` as `<R as Clone>::clone`) —
/// end to end: build, checkpoint (serde, not `Clone`), and cold read-back
/// after `recover()` (zero-copy `get`, not `get_arc`). `CountingRow::clone`
/// is the instrument.
#[test]
fn insert_batch_on_a_fresh_paged_table_clones_zero_values() {
    let _guard = COUNTING_CLONES_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let before = COUNTING_CLONES.load(std::sync::atomic::Ordering::Relaxed);

    {
        let s = counting_paged_store(dir.path());
        let batch: Vec<CountingRow> = (0..600u64).map(|v| CountingRow { v }).collect();
        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<CountingRow>("c").unwrap();
            let ids = t.insert_batch(batch).unwrap();
            assert_eq!(ids.len(), 600);
        }
        w.commit().unwrap();
        assert_eq!(
            COUNTING_CLONES.load(std::sync::atomic::Ordering::Relaxed),
            before,
            "insert_batch on a fresh paged table must not clone any values"
        );

        s.checkpoint().unwrap();
        assert_eq!(
            COUNTING_CLONES.load(std::sync::atomic::Ordering::Relaxed),
            before,
            "checkpoint serializes via serde, not Clone — still zero"
        );
    }

    // Cold: this is where the leaves actually turn block-shaped
    // (`NodeCodec::decode` on the first fault-in), the case Task 7's ruling
    // says the bulk path itself never has to pay for.
    let s2 = counting_paged_store(dir.path());
    s2.recover().unwrap();

    let r = s2.begin_read(None).unwrap();
    let t = r.open_table::<CountingRow>("c").unwrap();
    assert_eq!(t.len(), 600);
    // Auto-increment ids start at 1; `insert_batch` assigned them in the
    // same order the rows were built (`v` ascending from 0).
    for id in 1..=600u64 {
        assert_eq!(t.get(id).unwrap().v, id - 1, "row {id}");
    }
    assert_eq!(
        COUNTING_CLONES.load(std::sync::atomic::Ordering::Relaxed),
        before,
        "reading back cold, block-shaped rows through `get` (zero-copy) must not clone either"
    );
}

/// Step 1(c): MultiWriter's key-level OCC merge (`Table::upsert_arc`, the
/// per-key slow path `merge_keys_from` falls back to whenever the losing
/// writer's base predates the table's latest committed write) against a
/// recovered, block-backed leaf — the same disjoint-keys-both-commit shape
/// `tests/store_integration.rs`'s
/// `multi_writer_disjoint_keys_same_table_both_commit` and the task59 race
/// matrix cover for an all-Arc table, replayed here on block leaves.
#[test]
fn multi_writer_disjoint_keys_on_a_paged_block_backed_table_both_commit() {
    let dir = tempfile::tempdir().unwrap();
    let ops: Vec<Op> = (0..200u64).map(|k| Op::Put(k, k)).collect();
    {
        let paged = paged_store(dir.path());
        apply_ops(&paged, &ops);
        paged.checkpoint().unwrap();
    }
    let s = reopen_and_recover(dir.path());

    let mut wa = s.begin_write(None).unwrap();
    let mut wb = s.begin_write(None).unwrap();

    // Writer A updates an existing (block-backed) key in place; writer B
    // inserts a brand-new one. Disjoint key sets on the same table, both
    // based on the same recovered snapshot.
    wa.open_table::<Row>("t").unwrap().update(50, Row { v: 999 }).unwrap();
    wb.open_table::<Row>("t").unwrap().put(500, Row { v: 12_345 }).unwrap();

    wa.commit().unwrap();
    // B rebases onto A's just-committed snapshot: `merge_keys_from` clones
    // the current latest table (O(1) CoW) and replays B's one modified key
    // into it via `upsert_arc` — the merge path this test targets.
    wb.commit().unwrap();

    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("t").unwrap();
    assert_eq!(t.len(), 201);
    assert_eq!(t.get(50).unwrap().v, 999, "writer A's edit must survive the merge");
    assert_eq!(t.get(500).unwrap().v, 12_345, "writer B's edit must survive the merge");
    for k in 0..200u64 {
        if k != 50 {
            assert_eq!(t.get(k).unwrap().v, k, "row {k} must be untouched by the merge");
        }
    }
}

// ---------------------------------------------------------------------------
// Fix round 1 (Task 7 review, Important): `Table::delete`'s true clone cost
// on a block leaf. The review measured 33-65 `clone_value` calls per delete
// where the doc comment claimed exactly one -- `Child::make_mut`'s CoW
// (`clone_with`) clones every surviving entry of a *shared* block leaf
// before `remove_from_block_leaf_mut` drops the removed one, which is
// itself cloned once for nothing in that case. These two tests pin both
// halves of the corrected cost model using the `CountingRow`/
// `COUNTING_CLONES` instrument from the zero-clone bulk test above: unique
// ownership costs exactly 1 (the `get_arc` boundary clone-out only); a
// shared leaf costs `1 + n` where `n` is the leaf's entry count at CoW time.
// ---------------------------------------------------------------------------

/// Seed a fresh paged `CountingRow` table with `n` rows (ids `1..=n`, one
/// leaf since `n` is kept well under `MAX_KEYS`), checkpoint, and drop the
/// store -- leaving a cold, block-shaped leaf on disk with nothing in any
/// live process having faulted it in yet.
fn seed_counting_table(dir: &Path, n: u64) {
    let s = counting_paged_store(dir);
    let batch: Vec<CountingRow> = (0..n).map(|v| CountingRow { v }).collect();
    let mut w = s.begin_write(None).unwrap();
    w.open_table::<CountingRow>("c").unwrap().insert_batch(batch).unwrap();
    w.commit().unwrap();
    s.checkpoint().unwrap();
}

/// Delete on a leaf that is the **sole** owner of its `Arc<BTreeNode>`:
/// nothing has read or written it since `recover()`, so `Table::delete`'s
/// own `merged_get_arc` traversal is the very first fault of this leaf, and
/// that fault lands in this `WriteTx`'s own freshly-forked `Child` slot --
/// the live snapshot's slot is still unloaded and shares nothing with it.
/// `remove_mut`'s subsequent `make_mut` therefore finds `Arc::get_mut`
/// succeeding (unique) and edits in place: no `clone_with` CoW.
#[test]
fn delete_on_a_unique_block_leaf_clones_only_the_returned_value() {
    let _guard = COUNTING_CLONES_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    seed_counting_table(dir.path(), 10);

    let s = counting_paged_store(dir.path());
    s.recover().unwrap();

    let before = COUNTING_CLONES.load(std::sync::atomic::Ordering::Relaxed);
    let mut w = s.begin_write(None).unwrap();
    let removed = w.open_table::<CountingRow>("c").unwrap().delete(5u64).unwrap();
    w.commit().unwrap();
    assert_eq!(removed.v, 4, "id 5 holds CountingRow{{v: 4}} (auto-ids are 1-based)");

    // Exactly 1: `merged_get_arc`'s own clone-out of the returned `Arc<R>`
    // (`get_arc_in_node`'s `clone_value` fallback, Task 4). Nothing else on
    // this path clones a value -- `remove_from_block_leaf_mut` drops the
    // removed slot from an already-uniquely-owned block without touching
    // `clone_value` at all.
    assert_eq!(
        COUNTING_CLONES.load(std::sync::atomic::Ordering::Relaxed) - before,
        1,
        "delete on a uniquely-owned block leaf must cost exactly one clone (the boundary clone-out)"
    );

    let r = s.begin_read(None).unwrap();
    assert_eq!(r.open_table::<CountingRow>("c").unwrap().len(), 9);
}

/// Delete on a leaf that is **shared** with the store's live snapshot: a
/// plain `get` through a read transaction (zero-copy, no `clone_value` of
/// its own) faults the leaf into the live snapshot's `Child` slot first --
/// the ordinary case whenever anything touched the leaf before this delete
/// (exactly what
/// `delete_on_a_recovered_block_backed_table_returns_the_correct_value`'s
/// preceding `assert_tables_equal` oracle pass does to every leaf). The
/// later `WriteTx`'s table clone then shares that same `Arc<BTreeNode>`, so
/// `make_mut` takes the `clone_with` CoW branch.
#[test]
fn delete_on_a_shared_block_leaf_clones_the_whole_block() {
    let _guard = COUNTING_CLONES_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    seed_counting_table(dir.path(), 10);

    let s = counting_paged_store(dir.path());
    s.recover().unwrap();

    // Prime: fault the leaf into the *live* snapshot via a read-only get
    // (zero-copy -- contributes 0 to the counter itself).
    {
        let r = s.begin_read(None).unwrap();
        assert_eq!(r.open_table::<CountingRow>("c").unwrap().get(5).unwrap().v, 4);
    }
    let before = COUNTING_CLONES.load(std::sync::atomic::Ordering::Relaxed);

    let mut w = s.begin_write(None).unwrap();
    let removed = w.open_table::<CountingRow>("c").unwrap().delete(5u64).unwrap();
    w.commit().unwrap();
    assert_eq!(removed.v, 4);

    // 11 = 1 (`merged_get_arc`'s boundary clone-out) + 10 (`Child::make_mut`
    // -> `clone_with`'s whole-block CoW: `Arc::get_mut` fails because the
    // leaf is shared with the live snapshot from the priming read above, so
    // every one of the 10 entries resident at CoW time -- including the one
    // about to be removed -- is duplicated via `clone_value`, and the
    // removed entry's copy is then dropped for nothing by
    // `remove_from_block_leaf_mut`).
    assert_eq!(
        COUNTING_CLONES.load(std::sync::atomic::Ordering::Relaxed) - before,
        11,
        "delete on a shared block leaf costs 1 (get_arc) + n=10 (clone_with's whole-block CoW)"
    );

    let r = s.begin_read(None).unwrap();
    assert_eq!(r.open_table::<CountingRow>("c").unwrap().len(), 9);
}
