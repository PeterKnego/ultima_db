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
    s.register_table::<Row>("rows").unwrap();
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
    let delta = s.paged_stats().unwrap().pages_written - pages_after_first;
    assert!((3..=5).contains(&delta), "leaf + inner path + root, got {delta}");
    assert!(
        s.paged_install_count_for_test().unwrap() > installs_after_first,
        "a checkpoint that actually wrote new pages must install"
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
