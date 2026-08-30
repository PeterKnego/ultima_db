// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 8: `PagedOptions`, `Persistence::paged`, and the paged checkpoint
//! path (phases 1-2 — dirty write + root record; no demotion, no recovery
//! yet). See `docs/tasks/task8_paged_checkpoint.md`.

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

    // no-op checkpoint writes nothing
    s.checkpoint().unwrap();
    assert_eq!(s.paged_stats().unwrap().pages_written, pages_after_first);

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
}

#[test]
fn paged_requires_disk_persistence() {
    // Controller amendment: `Persistence::None.paged(..)` fails at
    // construction time, before `Store::new` is ever called — there is no
    // `Standalone`/`Smr` variant to attach the options to.
    let p = Persistence::None.paged(PagedOptions::builder().build());
    assert!(p.is_err());
}
