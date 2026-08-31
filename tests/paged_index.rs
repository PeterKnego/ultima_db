// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 13: attaching persisted index contents after recovery —
//! `define_persisted_index`'s attach branch, `Error::IndexDefinitionMismatch`,
//! generation semantics, and carrying still-pending index metadata through
//! checkpoints. See
//! `docs/superpowers/specs/2026-08-30-paged-btree-stage-1-2-design.md` §6 and
//! `.superpowers/sdd/2026-08-30-paged-btree-stage-1-2/task-13-brief.md`.

#![cfg(feature = "persistence")]

use ultima_db::{
    CustomIndex, Durability, Error, IndexDef, IndexKind, PagedOptions, Persistence, Result,
    Store, StoreConfig, WalWrite,
};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

/// Same shape as `tests/paged_recovery.rs`'s helper of the same name.
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

/// Define `by_v` (`v % 100`, `NonUnique`) under `def` in its own transaction.
fn define_by_v(s: &Store, kind: IndexKind, def: IndexDef) -> Result<()> {
    let mut w = s.begin_write(None)?;
    {
        let mut t = w.open_table::<Row>("rows")?;
        t.define_persisted_index::<u64>("by_v", kind, def, |r: &Row| r.v % 100)?;
    }
    w.commit().map(|_| ())
}

/// Attaching (or rebuilding) `by_v` from a fully recovered, up-to-date
/// pending entry must read zero data pages — the whole point of a paged
/// index attach.
#[test]
fn attach_reads_no_data_pages() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        define_by_v(&s, IndexKind::NonUnique, IndexDef::new(1)).unwrap();
        write_rows(&s, 20_000);
        s.checkpoint().unwrap();
    }

    let s = store(d.path());
    s.recover().unwrap();
    let before = s.paged_stats().unwrap();

    define_by_v(&s, IndexKind::NonUnique, IndexDef::new(1)).unwrap();

    let after = s.paged_stats().unwrap();
    assert_eq!(
        after.data_page_faults, before.data_page_faults,
        "attach must not read a single data page"
    );
    assert!(
        after.index_page_faults > before.index_page_faults,
        "attach must load the index tree (index_page_faults must increase): \
         before={before:?} after={after:?}"
    );

    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get_by_index::<u64>("by_v", &7).unwrap().len(), 200);
}

/// `ik_type_id`/`kind` mismatches against a pending entry are refused with
/// `Error::IndexDefinitionMismatch` before anything is rebuilt; a
/// `generation` change is accepted and rebuilds by scanning the live data
/// (faulting every data leaf, unlike attach).
#[test]
fn kind_or_type_mismatch_errors_and_generation_rebuilds() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        define_by_v(&s, IndexKind::NonUnique, IndexDef::new(1)).unwrap();
        write_rows(&s, 5_000);
        s.checkpoint().unwrap();
    }

    let s = store(d.path());
    s.recover().unwrap();

    // `kind` disagrees with the pending entry (persisted NonUnique,
    // redefined Unique) — refused before any rebuild is attempted. If this
    // fell through to a rebuild instead, it would also fail (duplicate
    // keys under a unique index), but for the wrong reason and only after
    // wasted work.
    {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        let err = t
            .define_persisted_index::<u64>("by_v", IndexKind::Unique, IndexDef::new(1), |r: &Row| {
                r.v
            })
            .unwrap_err();
        match err {
            Error::IndexDefinitionMismatch { table, index, reason } => {
                assert_eq!(table, "rows");
                assert_eq!(index, "by_v");
                assert!(reason.contains("kind"), "reason: {reason}");
            }
            other => panic!("expected IndexDefinitionMismatch, got {other:?}"),
        }
        // The failed call must not have touched the table's own indexes or
        // pending metadata — the next scope's generation-mismatch retry
        // depends on the pending entry still being there.
        assert!(
            t.get_by_index::<u64>("by_v", &7).is_err(),
            "'by_v' must not exist yet — the mismatch was refused before any rebuild"
        );
        // `w` is dropped, not committed, at the end of this scope: the
        // failed define is discarded rather than persisted.
    }
    // `ik_type_id` disagrees (persisted `u64`, redefined `String`).
    {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        let err = t
            .define_persisted_index::<String>(
                "by_v",
                IndexKind::NonUnique,
                IndexDef::new(1),
                |r: &Row| r.v.to_string(),
            )
            .unwrap_err();
        match err {
            Error::IndexDefinitionMismatch { reason, .. } => {
                assert!(reason.contains("ik_type_id") || reason.contains("key type"), "{reason}");
            }
            other => panic!("expected IndexDefinitionMismatch, got {other:?}"),
        }
    }

    // `generation` disagrees — accepted, rebuilds by scanning `self.data`
    // (every data leaf faults, unlike an attach).
    let before = s.paged_stats().unwrap();
    define_by_v(&s, IndexKind::NonUnique, IndexDef::new(2)).unwrap();
    let after = s.paged_stats().unwrap();
    assert!(
        after.data_page_faults - before.data_page_faults > 20,
        "a generation change must rebuild by scan, faulting many data leaves: \
         before={before:?} after={after:?}"
    );

    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get_by_index::<u64>("by_v", &7).unwrap().len(), 50);
}

/// A plain (non-persisted) `define_index` over a name with pending
/// persisted contents rebuilds in memory only and drops the pending
/// metadata — its on-disk pages are then unreachable and must show up in
/// the next checkpoint's dead-page list.
#[test]
fn plain_define_index_over_persisted_contents_rebuilds_and_orphans_pages() {
    let d = tempfile::tempdir().unwrap();
    let index_pages_written = {
        let s = store(d.path());
        write_rows(&s, 2_000);
        s.checkpoint().unwrap();
        let before = s.paged_stats().unwrap().pages_written;
        define_by_v(&s, IndexKind::NonUnique, IndexDef::new(1)).unwrap();
        // Isolates the index's own pages: the data tree is unchanged since
        // the checkpoint above, so this call writes only the freshly-built
        // index tree's pages.
        s.checkpoint().unwrap();
        s.paged_stats().unwrap().pages_written - before
    };
    assert!(index_pages_written > 0, "defining the index must have written pages");

    let s = store(d.path());
    s.recover().unwrap();
    {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        // Plain, non-persisted redefinition of the same name.
        t.define_index("by_v", IndexKind::NonUnique, |r: &Row| r.v % 100)
            .unwrap();
        w.commit().unwrap();
    }
    let v = s.checkpoint().unwrap();
    let dead = ultima_db::paged_root_dead_pages_for_test(d.path(), v).unwrap();
    let dead_len: u64 = dead.len() as u64;
    assert!(
        dead_len >= index_pages_written,
        "dropping the pending persisted index should orphan at least its \
         {index_pages_written} pages, got {dead_len} dead range(s): {dead:?}"
    );

    // The plain index still works in memory — it just isn't paged.
    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get_by_index::<u64>("by_v", &7).unwrap().len(), 20);
}

/// Task 9 hazard (controller amendment): a recovered-but-not-yet-reattached
/// persisted index must survive an intervening checkpoint that never
/// re-`define_persisted_index`s it — `paged_write_tree` must carry the
/// pending entry's metadata forward into the new root, or a second recovery
/// loses it. A later `define_persisted_index` must still attach with zero
/// data-page reads.
#[test]
fn pending_index_survives_a_recover_checkpoint_round_trip_with_no_reattach() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        define_by_v(&s, IndexKind::NonUnique, IndexDef::new(1)).unwrap();
        write_rows(&s, 5_000);
        s.checkpoint().unwrap();
    }
    {
        // Recover without ever calling `define_persisted_index`, make an
        // unrelated write (so this checkpoint has something new to write
        // and isn't skipped as a true no-op), and checkpoint again. The
        // pending index is never touched by either the write or the
        // define call — this is the exact "carry it forward unchanged"
        // case.
        let s = store(d.path());
        s.recover().unwrap();
        write_rows(&s, 1);
        s.checkpoint().unwrap();
    }

    let s = store(d.path());
    s.recover().unwrap();
    let before = s.paged_stats().unwrap();
    define_by_v(&s, IndexKind::NonUnique, IndexDef::new(1)).unwrap();
    let after = s.paged_stats().unwrap();
    assert_eq!(
        after.data_page_faults, before.data_page_faults,
        "attach after a no-reattach checkpoint round-trip must still read zero data pages"
    );
    assert!(after.index_page_faults > before.index_page_faults);

    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get_by_index::<u64>("by_v", &7).unwrap().len(), 50);
}

/// Fix round 1 (controller review): a table dropped via
/// `WriteTx::delete_table` while it still has a never-reattached pending
/// persisted index must not leak that index's on-disk pages.
/// `Table::paged_changed_pages`'s pending-index diff needs `prev`'s
/// `paged_file` as a fallback: `paged_dead_page_ids`'s dropped-table
/// branch diffs against a *synthetic empty* table (`new_empty_table`)
/// which was never itself attached, so `self.paged_file` alone would
/// have skipped the whole pending-index diff for a dropped table.
#[test]
fn dropped_table_with_pending_index_reclaims_its_pages() {
    // Baseline: drop a recovered table with NO pending persisted index —
    // establishes how many pages a bare data-tree drop reports dead.
    let baseline = {
        let d = tempfile::tempdir().unwrap();
        {
            let s = store(d.path());
            write_rows(&s, 2_000);
            s.checkpoint().unwrap();
        }
        let s = store(d.path());
        s.recover().unwrap();
        {
            let mut w = s.begin_write(None).unwrap();
            assert!(w.delete_table("rows"));
            w.commit().unwrap();
        }
        let v = s.checkpoint().unwrap();
        ultima_db::paged_root_dead_pages_for_test(d.path(), v).unwrap().len()
    };

    // Same shape, but with a still-pending persisted index nobody ever
    // reattached: the drop must reclaim the index's own on-disk pages
    // *on top of* the data tree's.
    let d = tempfile::tempdir().unwrap();
    let index_pages_written = {
        let s = store(d.path());
        write_rows(&s, 2_000);
        s.checkpoint().unwrap();
        let before = s.paged_stats().unwrap().pages_written;
        define_by_v(&s, IndexKind::NonUnique, IndexDef::new(1)).unwrap();
        s.checkpoint().unwrap();
        s.paged_stats().unwrap().pages_written - before
    };
    assert!(index_pages_written > 0);

    let s = store(d.path());
    s.recover().unwrap();
    {
        let mut w = s.begin_write(None).unwrap();
        assert!(w.delete_table("rows"));
        w.commit().unwrap();
    }
    let v = s.checkpoint().unwrap();
    let with_index = ultima_db::paged_root_dead_pages_for_test(d.path(), v).unwrap().len() as u64;

    assert!(
        with_index >= baseline as u64 + index_pages_written,
        "dropping a table with a still-pending persisted index must reclaim at least its          {index_pages_written} pages on top of the {baseline}-range data-only baseline, got          {with_index}"
    );
}

#[derive(Clone)]
struct IdSetIndex {
    ids: ultima_db::BTree<u64, ()>,
}

impl IdSetIndex {
    fn new() -> Self {
        Self { ids: ultima_db::BTree::new() }
    }
}

impl CustomIndex<Row> for IdSetIndex {
    fn on_insert(&mut self, id: u64, _record: &Row) -> Result<()> {
        self.ids = self.ids.insert(id, ());
        Ok(())
    }

    fn on_update(&mut self, _id: u64, _old: &Row, _new: &Row) -> Result<()> {
        Ok(())
    }

    fn on_delete(&mut self, id: u64, _record: &Row) {
        if let Ok(new_ids) = self.ids.remove(&id) {
            self.ids = new_ids;
        }
    }
}

/// A `CustomIndex` (never paged, opaque to the generic maintainer) defined
/// on a table recovered from a paged root always rebuilds by a full scan —
/// unchanged by this task, but exercised here on an actually-paged store
/// rather than the in-memory-only coverage `tests/custom_index_api.rs`
/// already has.
#[test]
fn custom_index_on_paged_store_rebuilds_by_scan() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        write_rows(&s, 5_000);
        s.checkpoint().unwrap();
    }

    let s = store(d.path());
    s.recover().unwrap();
    let f0 = s.paged_stats().unwrap().data_page_faults;
    {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        t.define_custom_index("id_set", IdSetIndex::new()).unwrap();
        w.commit().unwrap();
    }
    let f1 = s.paged_stats().unwrap().data_page_faults;
    assert!(
        f1 - f0 > 20,
        "a custom index backfill must scan the whole table, faulting many data leaves \
         (f0={f0} f1={f1})"
    );

    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    let idx = t.custom_index::<IdSetIndex>("id_set").unwrap();
    assert!(idx.ids.get(&1).is_some());
    assert_eq!(idx.ids.len(), 5_000);
}
