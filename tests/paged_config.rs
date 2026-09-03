// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 14: config-surface completeness for paged checkpoints —
//! `checkpoint_chain_max`'s inertness, `Persistence::standalone_fast`
//! composing with `.paged(..)`, MultiWriter concurrent writers on disjoint
//! keys with checkpoints interleaved, and `Store::bulk_load` on a paged
//! store. See `docs/tasks/task63_paged_btree.md` and
//! `docs/superpowers/specs/2026-08-30-paged-btree-stage-1-2-design.md`.

#![cfg(feature = "persistence")]

use std::sync::{Arc, Barrier};
use std::thread;

use ultima_db::{
    BulkLoadInput, BulkLoadOptions, BulkSource, Durability, Error, PagedOptions, Persistence,
    Store, StoreConfig, WalWrite, WriterMode,
};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

fn store(dir: &std::path::Path) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().build())
        .unwrap();
    let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s.register_table_paged::<Row>("rows").unwrap();
    s
}

/// `StoreConfig::checkpoint_chain_max` is a row-format-only knob (see
/// `Store::checkpoint_impl`'s doc: a paged checkpoint always branches to
/// `checkpoint_impl_paged` before `checkpoint_chain_max` is even read).
/// Setting it alongside `Persistence::paged` must not be rejected at
/// `Store::new`, and every checkpoint it produces must still be a
/// self-contained `.root` record — never a row-format `.bin` delta or full.
#[test]
fn checkpoint_chain_max_is_inert_in_paged_mode() {
    let d = tempfile::tempdir().unwrap();
    let p = Persistence::standalone(d.path(), Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().build())
        .unwrap();
    // A chain_max > 1 would, in row-format mode, eventually produce a delta
    // checkpoint sharing the `.bin` namespace with fulls. Set here to prove
    // paged mode ignores it rather than merely never having been asked.
    let s = Store::new(StoreConfig::builder().persistence(p).checkpoint_chain_max(4).build())
        .unwrap();
    s.register_table_paged::<Row>("rows").unwrap();

    for round in 0..4u64 {
        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<Row>("rows").unwrap();
            for i in 0..50u64 {
                t.put(round * 50 + i, Row { v: i }).unwrap();
            }
        }
        w.commit().unwrap();
        s.checkpoint().unwrap();
    }

    // Only `checkpoint_{version}.bin` is the row-format namespace — `pages.bin`
    // (the paged node store itself) and `wal.bin` also end in `.bin` and must
    // not be mistaken for one.
    let (mut roots, mut bins) = (0u32, 0u32);
    for entry in std::fs::read_dir(d.path()).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        if name.starts_with("checkpoint_") && name.ends_with(".root") {
            roots += 1;
        }
        if name.starts_with("checkpoint_") && name.ends_with(".bin") {
            bins += 1;
        }
    }
    assert!(roots >= 1, "expected at least one checkpoint_*.root file, found none");
    assert_eq!(
        bins, 0,
        "checkpoint_chain_max must not produce row-format checkpoint_*.bin checkpoints in paged mode"
    );
}

/// `Persistence::standalone_fast` (`ConsistentInline` + `CoalescedPrealloc`)
/// composes with `.paged(..)` — the durability/WAL-write knobs are
/// orthogonal to paged checkpoints (spec §8's "interactions" list). Full
/// write / checkpoint / recover / read round trip.
#[test]
fn standalone_fast_paged_end_to_end() {
    let d = tempfile::tempdir().unwrap();
    let mut ids = Vec::new();
    {
        let p = Persistence::standalone_fast(d.path()).paged(PagedOptions::builder().build()).unwrap();
        let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
        s.register_table_paged::<Row>("rows").unwrap();

        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<Row>("rows").unwrap();
            for i in 0..500u64 {
                ids.push(t.insert(Row { v: i }).unwrap());
            }
        }
        w.commit().unwrap();
        s.checkpoint().unwrap();
    }

    let p = Persistence::standalone_fast(d.path()).paged(PagedOptions::builder().build()).unwrap();
    let s2 = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s2.register_table_paged::<Row>("rows").unwrap();
    s2.recover().unwrap();

    let rtx = s2.begin_read(None).unwrap();
    let t = rtx.open_table::<Row>("rows").unwrap();
    assert_eq!(t.len(), 500);
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(t.get(id), Some(&Row { v: i as u64 }));
    }
}

/// MultiWriter + paged: four threads write disjoint key ranges concurrently,
/// each periodically calling `Store::checkpoint()` so checkpoints interleave
/// with the still-running writers (rather than only ever landing on a quiet
/// store) — after every writer joins, a fresh recovery must see every row
/// from every thread.
#[test]
fn multiwriter_disjoint_writers_with_interleaved_checkpoints_survive_recovery() {
    const THREADS: u64 = 4;
    const ROWS_PER_THREAD: u64 = 200;

    let d = tempfile::tempdir().unwrap();
    {
        let p = Persistence::standalone(d.path(), Durability::Eventual, WalWrite::Coalesced)
            .paged(PagedOptions::builder().build())
            .unwrap();
        let s = Store::new(
            StoreConfig::builder().persistence(p).writer_mode(WriterMode::MultiWriter).build(),
        )
        .unwrap();
        s.register_table_paged::<Row>("rows").unwrap();

        let barrier = Arc::new(Barrier::new(THREADS as usize));
        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let s = s.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let base = tid * ROWS_PER_THREAD;
                    for i in 0..ROWS_PER_THREAD {
                        let key = base + i;
                        loop {
                            let mut w = s.begin_write(None).unwrap();
                            {
                                let mut t = w.open_table::<Row>("rows").unwrap();
                                t.put(key, Row { v: key }).unwrap();
                            }
                            match w.commit() {
                                Ok(_) => break,
                                // Disjoint keys still hash into the same OCC
                                // digest bucket on rare occasion (see
                                // `PrimaryKey::hash64`'s doc: a collision
                                // costs a spurious conflict, never a missed
                                // one) — retry rather than treat it as a bug.
                                Err(Error::WriteConflict { .. }) => continue,
                                Err(e) => panic!("unexpected commit error: {e}"),
                            }
                        }
                        // Interleave: every writer thread also drives
                        // checkpoints, so a checkpoint's dirty-node walk can
                        // land while other threads are still mid-write.
                        // `checkpoint_lock` serializes these against each
                        // other; none of them are expected to fail.
                        if i % 40 == 0 {
                            s.checkpoint().unwrap();
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // Final checkpoint so the very last commits are durable too.
        s.checkpoint().unwrap();
    }

    let p = Persistence::standalone(d.path(), Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().build())
        .unwrap();
    let s2 = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s2.register_table_paged::<Row>("rows").unwrap();
    s2.recover().unwrap();

    let rtx = s2.begin_read(None).unwrap();
    let t = rtx.open_table::<Row>("rows").unwrap();
    assert_eq!(t.len() as u64, THREADS * ROWS_PER_THREAD);
    for tid in 0..THREADS {
        let base = tid * ROWS_PER_THREAD;
        for i in 0..ROWS_PER_THREAD {
            let key = base + i;
            assert_eq!(t.get(key), Some(&Row { v: key }), "missing row for key {key}");
        }
    }
}

/// `Store::bulk_load` on a paged store, then an explicit checkpoint, then
/// recovery — a bulk-loaded table must attach a paged source and page out
/// correctly at checkpoint time just like a table built through ordinary
/// writes (spec §8: "`bulk_load` allowed, builds in memory").
#[test]
fn bulk_load_then_checkpoint_then_recover() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        let rows: Vec<(u64, Row)> = (0..2_000u64).map(|i| (i, Row { v: i })).collect();
        let input = BulkLoadInput::Replace(BulkSource::sorted_vec(rows));
        s.bulk_load::<Row>(
            "rows",
            input,
            BulkLoadOptions { create_if_missing: true, checkpoint_after: false },
        )
        .unwrap();
        s.checkpoint().unwrap();
    }

    let s2 = store(d.path());
    s2.recover().unwrap();

    let rtx = s2.begin_read(None).unwrap();
    let t = rtx.open_table::<Row>("rows").unwrap();
    assert_eq!(t.len(), 2_000);
    for i in 0..2_000u64 {
        assert_eq!(t.get(i), Some(&Row { v: i }));
    }
}

// ---------------------------------------------------------------------------
// Task 3: `register_table_paged` + `Error::PagedNeedsClone`. See
// `docs/tasks/task64_paged_leaf_value_blocks.md` (or the task's
// `docs/superpowers/specs/2026-08-31-paged-leaf-arena-memory-honesty-design.md`
// §4 "Public surface") for the design this locks in.
// ---------------------------------------------------------------------------

/// Plain `Store::register_table` on a paged store cannot produce the clone
/// fn a block-leaf CoW needs (`NodeSource::clone_value`) — it must be
/// refused up front with `Error::PagedNeedsClone`, not silently accepted
/// and left to fail (or panic) the first time a leaf actually needs to
/// clone a value.
#[test]
fn plain_register_on_paged_store_errors_needs_clone() {
    let dir = tempfile::tempdir().unwrap();
    let p = Persistence::standalone(dir.path(), Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().build())
        .unwrap();
    let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    let e = s.register_table::<Row>("rows").unwrap_err();
    assert!(matches!(e, Error::PagedNeedsClone { .. }), "{e:?}");
}

/// `Store::register_table_paged` is the required registration on a paged
/// store, and additive elsewhere: it also succeeds, behaving as plain
/// registration, on a non-paged store.
#[test]
fn register_table_paged_works_on_paged_and_plain_stores() {
    let dir = tempfile::tempdir().unwrap();
    let p = Persistence::standalone(dir.path(), Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().build())
        .unwrap();
    let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s.register_table_paged::<Row>("rows").unwrap();

    // Harmless (compiles and succeeds, behaving as plain registration) on a
    // non-paged store too.
    let plain = Store::default();
    plain.register_table_paged::<Row>("rows").unwrap();
}
