// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 11: dead-page lists in root records, and hole-punch reclaim gated
//! on root retention (Controller amendment — punch root v's dead list only
//! once root v-1 is actually deleted by `cleanup_old_roots`, never earlier).
//! See `docs/superpowers/specs/2026-08-30-paged-btree-stage-1-2-design.md`.

#![cfg(feature = "persistence")]

use ultima_db::{Durability, PagedOptions, Persistence, Store, StoreConfig, WalWrite};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

/// Same shape as `tests/paged_checkpoint.rs`'s helper of the same name,
/// parameterized on `PagedOptions` so each test can set its own
/// `retained_checkpoints`.
fn store_with(dir: &std::path::Path, opts: PagedOptions) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(opts)
        .unwrap();
    let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s.register_table::<Row>("rows").unwrap();
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

fn write_one_update(s: &Store) {
    let mut w = s.begin_write(None).unwrap();
    let mut t = w.open_table::<Row>("rows").unwrap();
    t.update(1, Row { v: 999_999 }).unwrap();
    w.commit().unwrap();
}

#[test]
fn dead_list_equals_replaced_path_and_is_punched_after_retention() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().retained_checkpoints(1).build());
    write_rows(&s, 20_000);
    let v1 = s.checkpoint().unwrap();
    write_one_update(&s);
    let v2 = s.checkpoint().unwrap();

    let dead = ultima_db::paged_root_dead_pages_for_test(d.path(), v2).unwrap();
    assert!(
        dead.len() >= 3 && dead.len() <= 5,
        "one updated key should replace root + inner + leaf on the CoW path, got {dead:?}"
    );

    // `retained_checkpoints(1)`: writing v2's root leaves 2 roots on disk
    // (v1, v2), over the retained budget of 1 — v1 must be deleted.
    assert!(
        !d.path().join(format!("checkpoint_{v1}.root")).exists(),
        "retained_checkpoints = 1 must have deleted the superseded root"
    );

    // v1 (the root v2's dead list was diffed against) is now gone, so v2's
    // dead list is immediately punchable in the same checkpoint() call that
    // wrote it and deleted v1.
    assert_eq!(
        s.paged_stats().unwrap().dead_pages_punched,
        dead.len() as u64,
        "v1's deletion should have made v2's whole dead list punchable"
    );
}

#[test]
fn dropped_table_pages_are_dead() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().build());
    s.register_table::<Row>("extra").unwrap();

    // Only "extra" gets rows — "rows" stays empty (no root page, no pages
    // written for it either way) so every page this first checkpoint writes
    // is attributable to "extra" alone.
    {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("extra").unwrap();
        for i in 0..5_000 {
            t.insert(Row { v: i }).unwrap();
        }
        w.commit().unwrap();
    }
    s.checkpoint().unwrap();
    let extra_pages = s.paged_stats().unwrap().pages_written;
    assert!(extra_pages > 50, "expected a multi-level tree, got {extra_pages} pages");

    {
        let mut w = s.begin_write(None).unwrap();
        assert!(w.delete_table("extra"));
        w.commit().unwrap();
    }
    let v2 = s.checkpoint().unwrap();

    let dead = ultima_db::paged_root_dead_pages_for_test(d.path(), v2).unwrap();
    assert_eq!(
        dead.len(),
        extra_pages as usize,
        "every page \"extra\" ever wrote must be reported dead once the table is dropped"
    );
}

#[test]
fn punch_is_gated_on_the_naming_roots_predecessor_being_deleted() {
    // Controller amendment (Task 8 review M8): punch root v's dead list only
    // once root v-1 is actually deleted — never while any retained root's
    // version is <= the version the dead list was diffed against, even once
    // a later checkpoint's own diff supersedes it.
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().retained_checkpoints(3).build());

    write_rows(&s, 20_000);
    let v1 = s.checkpoint().unwrap(); // dead=[] (no predecessor yet)
    assert_eq!(s.paged_stats().unwrap().dead_pages_punched, 0);

    write_one_update(&s);
    let v2 = s.checkpoint().unwrap(); // dead = diff(v1, v2), non-empty
    let dead_v2 = ultima_db::paged_root_dead_pages_for_test(d.path(), v2).unwrap();
    assert!(!dead_v2.is_empty(), "an update must leave something dead");
    // Only 2 roots exist (v1, v2) <= retained_checkpoints(3): nothing
    // deleted, so v2's dead list — diffed against v1 — must not be punched
    // while v1 is still retained.
    assert!(d.path().join(format!("checkpoint_{v1}.root")).exists());
    assert_eq!(
        s.paged_stats().unwrap().dead_pages_punched,
        0,
        "v1 is still retained; v2's dead list must not be punched yet"
    );

    write_one_update(&s);
    let v3 = s.checkpoint().unwrap(); // dead = diff(v2, v3)
    // 3 roots exist (v1, v2, v3) <= retained_checkpoints(3): still nothing
    // deleted.
    assert!(d.path().join(format!("checkpoint_{v1}.root")).exists());
    assert_eq!(
        s.paged_stats().unwrap().dead_pages_punched,
        0,
        "v1 is still retained; still nothing punchable"
    );
    let _ = v3;

    write_one_update(&s);
    let _v4 = s.checkpoint().unwrap(); // dead = diff(v3, v4)
    // 4 roots > retained_checkpoints(3): the oldest (v1) is deleted now.
    assert!(
        !d.path().join(format!("checkpoint_{v1}.root")).exists(),
        "v1 must finally be deleted once retention is exceeded"
    );
    // v1's deletion is exactly what makes v2's dead list (diffed against v1)
    // punchable — the checkpoint that wrote v4 is what applies it.
    assert_eq!(
        s.paged_stats().unwrap().dead_pages_punched,
        dead_v2.len() as u64,
        "v1's deletion must punch v2's dead list, and nothing else yet"
    );
}
