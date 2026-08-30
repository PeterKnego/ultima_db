// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 10: the demotion pass — `Store::demote_pass` (batched, same-version
//! re-publish, wired into `checkpoint_impl_paged`'s phase 3 whenever
//! `memory_budget_bytes` is set), `Store::set_residency`, and the
//! `resident_leaf_bytes` estimate maintenance. See
//! `docs/superpowers/specs/2026-08-30-paged-btree-stage-1-2-design.md`.

#![cfg(feature = "persistence")]

use ultima_db::{Durability, PagedOptions, Persistence, Residency, Store, StoreConfig, WalWrite};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

fn store_with(dir: &std::path::Path, opts: PagedOptions) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(opts)
        .unwrap();
    let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s.register_table::<Row>("rows").unwrap();
    s
}

/// One batch insert, not a loop of single `insert()` calls: a fresh table's
/// batch append goes through the dense `BulkBuilder` fast path
/// (`Table::insert_batch`'s `extend_from_sorted` branch, task51), which
/// builds leaves directly rather than walking root-to-leaf through
/// `Child::make_mut`/`load` per row — so no leaf ends up with its accessed
/// bit set just from having been built. A loop of single inserts touches
/// (and so marks accessed) every leaf along the way, including ones later
/// sealed by a split, which would make the very first demote pass find
/// everything "recently used" and demote nothing — the tests below need a
/// tree that demotes on pass one to exercise the second-chance behavior
/// they're actually testing.
fn write_rows(s: &Store, n: u64) {
    let mut w = s.begin_write(None).unwrap();
    let mut t = w.open_table::<Row>("rows").unwrap();
    t.insert_batch((0..n).map(|v| Row { v }).collect()).unwrap();
    w.commit().unwrap();
}

/// A budget of 1 byte is always exceeded, so every checkpoint's phase 3
/// runs to completion. 20,000 rows at the default T=32 fanout (MAX_KEYS=63)
/// works out to a few hundred leaves — comfortably over the `300` floor
/// this asserts. Demotion re-publishes the same version (no WAL entry, no
/// commit) and clears every leaf's residency, so a subsequent read through
/// `rows` has to fault every touched leaf back in.
#[test]
fn demotion_keeps_version_and_frees_after_gc() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().memory_budget_bytes(1).build());
    write_rows(&s, 20_000);
    let v = s.latest_version();
    s.checkpoint().unwrap();
    assert_eq!(s.latest_version(), v, "demotion re-publishes, never bumps the version");
    assert!(
        s.paged_stats().unwrap().leaves_demoted > 300,
        "leaves_demoted={}",
        s.paged_stats().unwrap().leaves_demoted
    );

    // fault count proves leaves are gone from the latest version
    let f0 = s.paged_stats().unwrap().page_faults;
    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    t.get(1);
    t.get(2);
    t.get(15_000);
    assert_eq!(s.paged_stats().unwrap().page_faults, f0 + 2, "keys 1 and 2 share a leaf");
}

/// The second-chance accessed bit: a leaf touched since the last demote
/// pass survives exactly one more pass, then goes on the pass after that —
/// *provided* it stays unread in between. `Child::load` (the ordinary read
/// path, unlike the checkpoint walk's `load_quiet`) marks accessed on every
/// touch, fault or not (see its doc), so a verification read of key 5
/// between the second-chance pass and the eviction pass would re-arm it and
/// this test would be asserting something false — this checks
/// `leaves_demoted`'s per-pass delta directly instead of re-reading key 5
/// to "prove" it is still resident.
#[test]
fn accessed_leaf_survives_one_pass() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().memory_budget_bytes(1).build());
    write_rows(&s, 20_000);
    s.checkpoint().unwrap(); // demotes everything (freshly batch-built, nothing accessed yet)
    {
        let r = s.begin_read(None).unwrap();
        r.open_table::<Row>("rows").unwrap().get(5); // faults key 5's leaf back in, marks accessed
    }
    let f = s.paged_stats().unwrap().page_faults;
    let d0 = s.paged_stats().unwrap().leaves_demoted;

    s.checkpoint().unwrap(); // second chance: key 5's leaf is the only resident one, and survives
    assert_eq!(
        s.paged_stats().unwrap().leaves_demoted,
        d0,
        "a leaf accessed since the last pass must survive this one"
    );

    s.checkpoint().unwrap(); // untouched since the second-chance pass cleared its bit — demoted now
    assert_eq!(
        s.paged_stats().unwrap().leaves_demoted,
        d0 + 1,
        "and go on the very next pass, having gone unread since"
    );

    let r = s.begin_read(None).unwrap();
    r.open_table::<Row>("rows").unwrap().get(5); // must fault: back on disk
    assert_eq!(s.paged_stats().unwrap().page_faults, f + 1, "gone on the third pass");
}

/// A `ReadTx` taken before the first checkpoint holds `Arc`s into a tree
/// that was never paged-attached at all: every one of its nodes is the
/// original in-memory `Arc<BTreeNode>`, and paging always demotes by
/// building a *new* leaf-parent (`Child::on_disk` swapped into a freshly
/// CoW'd parent — see `BTree::demote_leaves`) rather than mutating an
/// existing one in place. So no amount of demotion against `latest_version`
/// can ever reach back and evict a page out from under an old reader's
/// snapshot — reading through it must fault zero times, however many
/// checkpoints (and therefore demote passes) run in between.
///
/// This also exercises the "clamp at 0" half of `resident_leaf_bytes`:
/// these 20,000 rows were inserted in memory and never faulted in through
/// `PagedSource::read_node` before being written and demoted, so the
/// running estimate goes deeply negative internally (decremented by every
/// demoted leaf, credited for none of them) — `paged_stats` must still
/// report a small, sane `u64`, not a wrapped-looking huge one.
#[test]
fn old_reader_keeps_its_leaves_across_demotion() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().memory_budget_bytes(1).build());
    write_rows(&s, 20_000);
    let r = s.begin_read(None).unwrap(); // taken BEFORE any checkpoint
    s.checkpoint().unwrap(); // attaches, writes, demotes
    s.checkpoint().unwrap(); // a second pass, still touching only `latest_version`'s tables

    let f0 = s.paged_stats().unwrap().page_faults;
    {
        let t = r.open_table::<Row>("rows").unwrap();
        for k in 1..=200u64 {
            t.get(k);
        }
    }
    assert_eq!(
        s.paged_stats().unwrap().page_faults,
        f0,
        "an old reader's pre-attachment leaves are untouched by any later demotion"
    );

    drop(r);
    s.gc();
    let est = s.paged_stats().unwrap().resident_leaf_bytes_est;
    assert!(
        est < 1 << 30,
        "resident_leaf_bytes_est must clamp at 0, not wrap to a huge u64: got {est}"
    );
}

/// `demote_batch(1)` forces one leaf-parent per install, so a three-checkpoint
/// demote pass over ~300+ leaves interleaves with a concurrently-committing
/// writer many times over. Every commit's key-level OCC merge and every
/// demote batch's same-version re-publish target the store lock, so nothing
/// here should be lost regardless of interleaving — the final read proves
/// every one of the writer's 200 updates landed and the row count is intact.
#[test]
fn commit_interleaved_with_demotion_loses_nothing() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(
        d.path(),
        PagedOptions::builder().memory_budget_bytes(1).demote_batch(1).build(),
    );
    write_rows(&s, 20_000);
    let s2 = s.clone();
    let writer = std::thread::spawn(move || {
        for i in 0..200u64 {
            let mut w = s2.begin_write(None).unwrap();
            let mut t = w.open_table::<Row>("rows").unwrap();
            t.update(i * 97 + 1, Row { v: 1 }).unwrap();
            w.commit().unwrap();
        }
    });
    for _ in 0..3 {
        s.checkpoint().unwrap();
    }
    writer.join().unwrap();

    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    for i in 0..200u64 {
        assert_eq!(t.get(i * 97 + 1).unwrap().v, 1);
    }
    assert_eq!(t.len(), 20_000);
}

/// `Residency::Resident` opts a table out of demotion entirely — the guard
/// in `MergeableTable::paged_demote` (`self.residency == Resident` returns
/// `(clone, 0, None)`) and `Store::demote_pass`'s own loop-exit check both
/// have to hold for this to stay `0`.
#[test]
fn resident_table_is_never_demoted() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().memory_budget_bytes(1).build());
    write_rows(&s, 20_000);
    s.set_residency("rows", Residency::Resident).unwrap();
    s.checkpoint().unwrap();
    assert_eq!(s.paged_stats().unwrap().leaves_demoted, 0);

    // And the table is still fully readable, faulting nothing (it was
    // never demoted, so nothing needs to be faulted back in).
    let f0 = s.paged_stats().unwrap().page_faults;
    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    t.get(1);
    t.get(15_000);
    assert_eq!(s.paged_stats().unwrap().page_faults, f0);
}

/// IMPORTANT #2 fix: `Store::set_residency` serializes against an
/// in-flight `checkpoint()` (both hold `checkpoint_lock` for their whole
/// body), so a `Resident` request can never be silently undone by a demote
/// batch that read the table as `Lazy` before the request landed.
///
/// Best effort, not deterministic: the window this closes (between a
/// demote batch reading the table as `Lazy` and that same batch's own
/// install) is only a few instructions wide, so `set_residency` is hammered
/// in a loop for the checkpoint's entire duration rather than called once
/// at a guessed delay — `demote_batch(1)` makes the checkpoint's own demote
/// pass do one `install_paged_tables` call per leaf-parent (dozens, for
/// 20,000 rows), so many attempts spread across all of them give a real
/// chance of landing inside the window on whichever side of the fix is
/// running. Whichever order actually happens, the assertion below must
/// hold — that is the entire point of serializing on the lock: if
/// `set_residency` wins a given race, that checkpoint batch sees `Resident`
/// before demoting; if it loses, it simply waits and then flips a (by then
/// possibly already fully demoted) table to `Resident` — either way, no
/// checkpoint from this point on may demote another leaf.
#[test]
fn set_residency_serialized_against_an_in_flight_checkpoint() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let d = tempfile::tempdir().unwrap();
    let s = store_with(
        d.path(),
        PagedOptions::builder().memory_budget_bytes(1).demote_batch(1).build(),
    );
    write_rows(&s, 20_000);

    let done = Arc::new(AtomicBool::new(false));

    let s2 = s.clone();
    let done2 = Arc::clone(&done);
    let checkpoint_thread = std::thread::spawn(move || {
        let v = s2.checkpoint().unwrap();
        done2.store(true, Ordering::Relaxed);
        v
    });

    let s3 = s.clone();
    let done3 = Arc::clone(&done);
    let racer = std::thread::spawn(move || {
        while !done3.load(Ordering::Relaxed) {
            let _ = s3.set_residency("rows", Residency::Resident);
        }
    });

    checkpoint_thread.join().unwrap();
    racer.join().unwrap();
    // One more call after both threads have settled: the assertion below
    // must hold regardless of which order the race above actually resolved
    // in.
    s.set_residency("rows", Residency::Resident).unwrap();

    let before = s.paged_stats().unwrap().leaves_demoted;
    s.checkpoint().unwrap();
    assert_eq!(
        s.paged_stats().unwrap().leaves_demoted,
        before,
        "a Resident table must not lose leaves to a checkpoint that starts after set_residency has returned, regardless of how the race above resolved"
    );
}

/// `set_residency` on an absent table is `Error::TableNotFound`, not a
/// silent no-op.
#[test]
fn set_residency_on_unknown_table_errors() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().build());
    let err = s.set_residency("nope", Residency::Resident).unwrap_err();
    assert!(matches!(err, ultima_db::Error::TableNotFound(_)), "unexpected error: {err}");
}

/// A plain `checkpoint()` with no budget configured must never demote —
/// spec §8: `None` disables demotion entirely, every leaf stays resident
/// once faulted in.
#[test]
fn no_budget_never_demotes() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().build());
    write_rows(&s, 20_000);
    s.checkpoint().unwrap();
    s.checkpoint().unwrap();
    assert_eq!(s.paged_stats().unwrap().leaves_demoted, 0);
}
