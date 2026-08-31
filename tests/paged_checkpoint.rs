// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 8: `PagedOptions`, `Persistence::paged`, and the paged checkpoint
//! path (phases 1-2 — dirty write + root record; no demotion, no recovery
//! yet). See `docs/superpowers/specs/2026-08-30-paged-btree-stage-1-2-design.md`.

#![cfg(feature = "persistence")]

use ultima_db::{Durability, PagedOptions, Persistence, Store, StoreConfig, WalWrite};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

fn store(dir: &std::path::Path) -> Store {
    // `Persistence::paged` returns `Result` (the controller's pre-flight
    // ruling): `Persistence::None` cannot carry `PagedOptions` at all (it
    // has no directory for `pages.bin`), so the constructor has to be able
    // to reject that case rather than silently drop the options.
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().build())
        .unwrap();
    let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s.register_table_paged::<Row>("rows").unwrap();
    s
}

#[test]
fn paged_checkpoint_writes_root_and_pages_only_for_dirty_nodes() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path());
    {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        for i in 0..20_000 {
            t.insert(Row { v: i }).unwrap();
        }
        w.commit().unwrap();
    }
    let v1 = s.checkpoint().unwrap();
    assert!(d.path().join(format!("checkpoint_{v1}.root")).exists());
    assert!(d.path().join("pages.bin").exists());
    let pages_after_first = s.paged_stats().unwrap().pages_written;
    assert!(pages_after_first > 300);
    let installs_after_first = s.paged_install_count_for_test().unwrap();
    assert!(installs_after_first > 0, "the first checkpoint must install (attach + write)");

    // no-op checkpoint writes nothing, and installs nothing: the table was
    // already attached, `paged_write` took the no-overlay fast path and
    // found nothing dirty, so there is no clone diverging from what is
    // already published to re-publish.
    s.checkpoint().unwrap();
    assert_eq!(s.paged_stats().unwrap().pages_written, pages_after_first);
    assert_eq!(
        s.paged_install_count_for_test().unwrap(),
        installs_after_first,
        "a no-op checkpoint must not call install_paged_tables"
    );

    // one update → one path
    {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        t.update(5, Row { v: 99 }).unwrap();
        w.commit().unwrap();
    }
    s.checkpoint().unwrap();
    let pages_after_update = s.paged_stats().unwrap().pages_written;
    let delta = pages_after_update - pages_after_first;
    assert!((3..=5).contains(&delta), "leaf + inner path + root, got {delta}");
    let installs_after_update = s.paged_install_count_for_test().unwrap();
    assert!(
        installs_after_update > installs_after_first,
        "a checkpoint that actually wrote new pages must install"
    );

    // A fourth checkpoint, with no intervening commit, must be a true
    // no-op: zero pages written, zero installs. Round-1 of this fix got
    // this wrong — the update's checkpoint (the one just above) wrote the
    // live table's root page id onto a throwaway clone only, since
    // `newly_attached`/`flushed.is_some()` were both false there (the
    // table was already attached, and the update went through the
    // no-overlay fast path). Without installing that clone, `live`'s own
    // root `Child` stayed dirty forever, so *this* checkpoint would find
    // the root dirty again and write it once more.
    s.checkpoint().unwrap();
    assert_eq!(
        s.paged_stats().unwrap().pages_written,
        pages_after_update,
        "a checkpoint with nothing new to write must write zero pages"
    );
    assert_eq!(
        s.paged_install_count_for_test().unwrap(),
        installs_after_update,
        "a checkpoint with nothing new to write must install nothing"
    );
}

#[test]
fn multiwriter_paged_checkpoint_settles_to_a_true_no_op() {
    // `WriterMode::MultiWriter` always sets the write-overlay cap to 0 (see
    // `StoreInner::overlay_cap`'s doc), so `MergeableTable::paged_write`
    // never takes the clone-and-flush path — `flushed` is unconditionally
    // `None` under MultiWriter. Before this fix, that meant an
    // already-attached MultiWriter table's writes could never trigger an
    // install: the `wrote_pages > 0` condition is what covers this case.
    let d = tempfile::tempdir().unwrap();
    let p = Persistence::standalone(d.path(), Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().build())
        .unwrap();
    let s = Store::new(
        StoreConfig::builder()
            .persistence(p)
            .writer_mode(ultima_db::WriterMode::MultiWriter)
            .build(),
    )
    .unwrap();
    s.register_table_paged::<Row>("rows").unwrap();

    {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        for i in 0..1_000 {
            t.insert(Row { v: i }).unwrap();
        }
        w.commit().unwrap();
    }
    s.checkpoint().unwrap();
    let pages_after_first = s.paged_stats().unwrap().pages_written;
    assert!(pages_after_first > 0);

    {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        t.update(5, Row { v: 999 }).unwrap();
        w.commit().unwrap();
    }
    s.checkpoint().unwrap();
    let pages_after_update = s.paged_stats().unwrap().pages_written;
    assert!(
        pages_after_update > pages_after_first,
        "the update's checkpoint must write new pages even with no overlay involved"
    );
    let installs_after_update = s.paged_install_count_for_test().unwrap();

    // No-op: no intervening commit. Must write zero pages and install
    // nothing — proves the previous checkpoint's install correctly
    // republished the live root, rather than leaving it permanently dirty.
    s.checkpoint().unwrap();
    assert_eq!(
        s.paged_stats().unwrap().pages_written,
        pages_after_update,
        "a MultiWriter no-op checkpoint must write zero pages"
    );
    assert_eq!(
        s.paged_install_count_for_test().unwrap(),
        installs_after_update,
        "a MultiWriter no-op checkpoint must install nothing"
    );
}

#[test]
fn paged_checkpoint_refuses_on_a_rooted_directory_without_recover() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        for i in 0..100 {
            t.insert(Row { v: i }).unwrap();
        }
        w.commit().unwrap();
        s.checkpoint().unwrap();
    } // store (and its PageFile handle) dropped here

    let pages_path = d.path().join("pages.bin");
    let before = std::fs::read(&pages_path).unwrap();
    assert!(!before.is_empty(), "the first store's checkpoint must have written pages");

    // A brand-new `Store` over the same directory: `Store::new` opens
    // `pages.bin` at cursor 0 (recovery, which would read the `.root` file
    // and reposition the cursor, is task 9 and is deliberately not called
    // here). `checkpoint()` must refuse rather than start appending at
    // byte 0 over pages the existing root names.
    let s2 = store(d.path());
    let err = s2.checkpoint().unwrap_err();
    assert!(
        err.to_string().contains(
            "paged checkpoint refused: directory contains paged roots but the store has not recovered; call Store::recover() first"
        ),
        "unexpected error: {err}"
    );

    let after = std::fs::read(&pages_path).unwrap();
    assert_eq!(
        before, after,
        "a refused checkpoint must not touch pages.bin at all"
    );
}

#[test]
fn paged_requires_disk_persistence() {
    // Controller amendment: `Persistence::None.paged(..)` fails at
    // construction time, before `Store::new` is ever called — there is no
    // `Standalone`/`Smr` variant to attach the options to.
    let p = Persistence::None.paged(PagedOptions::builder().build());
    assert!(p.is_err());
}
