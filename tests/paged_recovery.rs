// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 9: recovery from a paged root — the paged sibling of
//! `tests/persistence_integration.rs`'s row-format recovery coverage. See
//! `docs/superpowers/specs/2026-08-30-paged-btree-stage-1-2-design.md`.

#![cfg(feature = "persistence")]

use ultima_db::{Durability, Error, PagedOptions, Persistence, Store, StoreConfig, WalWrite};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

/// Same shape as `tests/paged_checkpoint.rs`'s helper of the same name.
fn store(dir: &std::path::Path) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().build())
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
fn recover_from_paged_root_loads_inner_levels_only_then_faults_on_read() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        write_rows(&s, 20_000);
        s.checkpoint().unwrap();
    }
    let s = store(d.path());
    s.recover().unwrap();
    let st = s.paged_stats().unwrap();
    assert!(st.page_faults < 40, "inner levels + one probe leaf, got {}", st.page_faults);
    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get(1234).map(|r| r.v), Some(1233));
    assert_eq!(s.paged_stats().unwrap().page_faults, st.page_faults + 1);
}

#[test]
fn wal_replay_after_paged_root_faults_only_touched_leaves() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        write_rows(&s, 20_000);
        s.checkpoint().unwrap();
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        t.update(3, Row { v: 1 }).unwrap();
        t.update(19_000, Row { v: 2 }).unwrap();
        w.commit().unwrap();
    }
    let s = store(d.path());
    s.recover().unwrap();
    let f0 = s.paged_stats().unwrap().page_faults;
    assert!(f0 < 40 + 2);
    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get(3).unwrap().v, 1);
    assert_eq!(t.get(19_000).unwrap().v, 2);
}

#[test]
fn legacy_directory_upgrades_on_first_paged_checkpoint() {
    let d = tempfile::tempdir().unwrap();
    {
        let p = Persistence::standalone(d.path(), Durability::Eventual, WalWrite::Coalesced);
        let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
        s.register_table::<Row>("rows").unwrap();
        write_rows(&s, 5_000);
        s.checkpoint().unwrap();
    }
    let s = store(d.path());
    s.recover().unwrap();
    assert_eq!(s.paged_stats().unwrap().page_faults, 0, "legacy load is fully resident");
    s.checkpoint().unwrap();
    assert!(s.paged_stats().unwrap().pages_written > 70, "first paged checkpoint writes every node");
    let s2 = store(d.path());
    s2.recover().unwrap();
    assert_eq!(
        s2.begin_read(None).unwrap().open_table::<Row>("rows").unwrap().get(4_000).unwrap().v,
        3_999
    );
}

#[test]
fn row_config_refuses_paged_directory() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        write_rows(&s, 10);
        s.checkpoint().unwrap();
    }
    let p = Persistence::standalone(d.path(), Durability::Eventual, WalWrite::Coalesced);
    let e = Store::new(StoreConfig::builder().persistence(p).build())
        .err()
        .unwrap();
    assert!(matches!(e, Error::PagedFormatRequired { .. }), "{e:?}");
}

#[test]
fn garbage_past_file_end_is_overwritten() {
    let d = tempfile::tempdir().unwrap();
    let end = {
        let s = store(d.path());
        write_rows(&s, 1_000);
        s.checkpoint().unwrap();
        std::fs::metadata(d.path().join("pages.bin")).unwrap().len()
    };
    // Append torn bytes at the end — inside the zero-filled prealloc region
    // past `file_end`, not colliding with any page a durable root names
    // (`PagedOptions::default().prealloc_chunk_bytes` is 16 MiB, comfortably
    // more than 1 KiB past `file_end` for 1,000 rows).
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(d.path().join("pages.bin"))
            .unwrap();
        f.write_at(&[0xEE; 500], end - 1024).unwrap();
    }
    let s = store(d.path());
    s.recover().unwrap();
    write_one_update(&s);
    s.checkpoint().unwrap();
    let s2 = store(d.path());
    s2.recover().unwrap();
    assert_eq!(s2.begin_read(None).unwrap().open_table::<Row>("rows").unwrap().len(), 1_000);
}

/// Controller amendment (Task 6 review): `recover()` treats the root
/// record's body `version` as authoritative, not the `.root` filename. A
/// root physically renamed so the filename disagrees with its body must be
/// refused with `CheckpointCorrupted` rather than silently trusted either
/// way.
#[test]
fn renamed_root_file_is_refused_as_corrupted() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        write_rows(&s, 10);
        s.checkpoint().unwrap();
    }
    assert!(d.path().join("checkpoint_1.root").exists());
    std::fs::rename(
        d.path().join("checkpoint_1.root"),
        d.path().join("checkpoint_2.root"),
    )
    .unwrap();

    let s = store(d.path());
    let err = s.recover().unwrap_err();
    assert!(matches!(err, Error::CheckpointCorrupted(_)), "{err:?}");
}

/// Controller amendment (Task 8 I1 ruling): `checkpoint_impl_paged` refuses
/// to write into a rooted directory until `Store::recover()` has set
/// `PagedState::last_root` and repositioned the page file's cursor.
/// `recover()`'s paged branch must lift that refusal, and the checkpoint
/// that follows must append after the recovered `file_end`, never restart
/// from 0.
#[test]
fn recover_then_checkpoint_appends_at_file_end_not_zero() {
    let d = tempfile::tempdir().unwrap();
    let v1_end = {
        let s = store(d.path());
        write_rows(&s, 1_000);
        s.checkpoint().unwrap();
        s.paged_file_end().unwrap()
    };
    assert!(v1_end > 0);

    let s = store(d.path());
    s.recover().unwrap();
    assert_eq!(
        s.paged_file_end().unwrap(),
        v1_end,
        "recover() must reposition the cursor to the recovered root's file_end"
    );

    write_one_update(&s);
    s.checkpoint().unwrap();
    let v2_end = s.paged_file_end().unwrap();
    assert!(
        v2_end > v1_end,
        "checkpoint() after recover() must append after file_end, not restart at 0"
    );
}
