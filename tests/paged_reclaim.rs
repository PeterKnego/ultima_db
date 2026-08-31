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

// ---------------------------------------------------------------------------
// Fix round 1 (reviewer findings I-1..I-4): the retention gate itself was
// verified correct above; these tests cover the safety margins around it —
// a corrupted dead-page range, a failing punch syscall, an actual mis-punch,
// and a corrupted older root record.
// ---------------------------------------------------------------------------

#[test]
fn corrupted_dead_page_length_is_dropped_before_punch_not_handed_to_fallocate() {
    // I-1: `PageFile::read_len`'s own bound rejects a grossly-invalid
    // length, but a corrupted length that is still smaller than the page
    // file's *physical* capacity (which typically has megabytes of
    // prealloc slack ahead of the much smaller logical end) sails through
    // it. The punch step's separate, later bound check — comparing the
    // already-computed range against the CURRENT `file_end()` — is what
    // has to catch that case, since `fallocate` gives no way to undo a
    // punch once issued.
    //
    // Which page id a given update makes dead is not something the public
    // API exposes, so a throwaway "dry run" directory first learns the
    // real (offset, len) a key-0 update produces; the actual test then
    // reruns the identical, deterministic sequence of operations against a
    // fresh directory with that exact offset corrupted beforehand.
    let dry = tempfile::tempdir().unwrap();
    let target_offset = {
        let s = store_with(dry.path(), PagedOptions::builder().retained_checkpoints(2).build());
        write_rows(&s, 100);
        s.checkpoint().unwrap();
        let mut w = s.begin_write(None).unwrap();
        // Auto-increment keys start at 1 (`AutoKey`), so key 1 is the
        // first row inserted — its leaf is on the CoW path this update
        // replaces.
        w.open_table::<Row>("rows").unwrap().update(1, Row { v: 999_999 }).unwrap();
        w.commit().unwrap();
        let v2 = s.checkpoint().unwrap();
        let dead = ultima_db::paged_root_dead_pages_for_test(dry.path(), v2).unwrap();
        assert!(!dead.is_empty(), "updating key 1 must replace at least the leaf holding it");
        dead.iter().map(|(off, _)| *off).min().unwrap()
    };

    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().retained_checkpoints(1).build());
    write_rows(&s, 100);
    s.checkpoint().unwrap(); // v1 — writes the same pages the dry run did

    // Corrupt just the payload_len field (header bytes [4, 8)) of the page
    // at `target_offset`, inflating it well past the file's current
    // (tiny, ~100-row) logical end but comfortably under the 16 MiB
    // default prealloc chunk — so `read_len`'s own capacity bound does not
    // itself reject it, and this exercises the SEPARATE punch-time check.
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(d.path().join("pages.bin"))
            .unwrap();
        let bogus_len: u32 = 10_000_000;
        f.write_at(&bogus_len.to_le_bytes(), target_offset + 4).unwrap();
    }

    let mut w = s.begin_write(None).unwrap();
    w.open_table::<Row>("rows").unwrap().update(1, Row { v: 999_999 }).unwrap();
    w.commit().unwrap();

    // Must succeed despite the corrupted range: `retained_checkpoints(1)`
    // deletes v1 immediately, so this same checkpoint both computes the
    // dead-list (via the corrupted `read_len`) and attempts to punch it.
    s.checkpoint().unwrap();
    let stats = s.paged_stats().unwrap();
    assert_eq!(
        stats.dead_pages_dropped, 1,
        "exactly the corrupted range must be dropped before punch, {stats:?}"
    );
    assert!(
        stats.dead_pages_punched > 0,
        "the OTHER, legitimate dead ranges on the same CoW path must still be punched"
    );

    drop(s);
    let s2 = store_with(d.path(), PagedOptions::builder().retained_checkpoints(1).build());
    s2.recover().unwrap();
    let r = s2.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.len(), 100);
    // Auto-increment keys are 1..=100; key K holds value K-1 (`write_rows`
    // inserts `Row { v: i }` for `i in 0..n`, and the first insert gets
    // key 1), except key 1, updated above.
    for k in 1..=100u64 {
        let expected = if k == 1 { 999_999 } else { k - 1 };
        assert_eq!(t.get(k).map(|row| row.v), Some(expected), "row {k} corrupted or lost");
    }
}

#[test]
fn punch_failure_is_best_effort_and_retried_next_checkpoint() {
    // I-2: a punch failure must never fail an already-committed checkpoint
    // — the root record is durable and correct either way. The un-punched
    // ranges go back into `pending_punch` and must be retried,
    // unconditionally, by the very next checkpoint.
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().retained_checkpoints(1).build());
    write_rows(&s, 20_000);
    s.checkpoint().unwrap(); // v1, dead=[]

    write_one_update(&s);
    s.paged_fail_next_punch_for_test();
    // v1 is deleted by this checkpoint's own `cleanup_old_roots`, so its
    // punch step has something to punch — and the injected failure fires
    // on exactly that attempt.
    let v2 = s.checkpoint().unwrap();
    assert_eq!(
        s.paged_stats().unwrap().dead_pages_punched,
        0,
        "a failed punch must not be counted as punched"
    );
    let dead_v2 = ultima_db::paged_root_dead_pages_for_test(d.path(), v2).unwrap();
    assert!(!dead_v2.is_empty());

    // No further injected failure: the next checkpoint must retry v2's
    // un-punched ranges (carried in `pending_punch`) AND punch its own
    // newly-punchable ones (v2 is deleted by this round's retention gate).
    write_one_update(&s);
    let v3 = s.checkpoint().unwrap();
    let dead_v3 = ultima_db::paged_root_dead_pages_for_test(d.path(), v3).unwrap();
    assert_eq!(
        s.paged_stats().unwrap().dead_pages_punched,
        (dead_v2.len() + dead_v3.len()) as u64,
        "the retried v2 ranges plus v3's own newly-punchable ranges must both land"
    );

    // And no data was lost across the failed-then-retried punch.
    let r = s.begin_read(None).unwrap();
    assert_eq!(r.open_table::<Row>("rows").unwrap().len(), 20_000);
}

#[test]
fn a_mis_punch_would_surface_as_unknown_page_kind_this_is_the_load_bearing_safety_test() {
    // This is the load-bearing safety test for the whole task: every other
    // test in this file proves the retention *gate* is correctly timed,
    // but none of them can observe an actual mis-punch, because the live
    // store's own tables stay resident and never re-read a punched page
    // from disk to notice. `memory_budget_bytes` forces real demotion —
    // every checkpoint writes leaves back out and drops residency — and
    // opening a brand-new store afterward guarantees a full-scan read
    // faults every row's page back in from disk. If a hole-punch had ever
    // hit a byte range some live page still occupied, `fallocate`'s
    // `PUNCH_HOLE` zero-fills it, and byte 0 is not a valid `PageKind` —
    // that read would surface as `Error::CheckpointCorrupted("... unknown
    // page kind 0")`, not silently return stale-but-plausible data.
    let d = tempfile::tempdir().unwrap();
    let opts = PagedOptions::builder().retained_checkpoints(1).memory_budget_bytes(1).build();
    let s = store_with(d.path(), opts);
    write_rows(&s, 20_000);
    s.checkpoint().unwrap();

    let mut i: u64 = 0;
    loop {
        let mut w = s.begin_write(None).unwrap();
        {
            let mut t = w.open_table::<Row>("rows").unwrap();
            // Auto-increment keys are 1..=20_000; shift by one so
            // every iteration targets a real key.
            t.update(1 + (i % 20_000), Row { v: 1_000_000 + i }).unwrap();
        }
        w.commit().unwrap();
        s.checkpoint().unwrap();
        if s.paged_stats().unwrap().dead_pages_punched > 0 {
            break;
        }
        i += 1;
        assert!(i < 1_000, "dead_pages_punched never became nonzero after 1000 update+checkpoint cycles");
    }
    drop(s);

    let s2 = store_with(
        d.path(),
        PagedOptions::builder().retained_checkpoints(1).memory_budget_bytes(1).build(),
    );
    s2.recover().unwrap();
    let r = s2.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.len(), 20_000);
    for k in 1..=20_000u64 {
        t.get(k)
            .unwrap_or_else(|| panic!("row {k} unreadable — a hole-punch destroyed live data"));
    }
}

#[test]
fn recovery_tolerates_an_unreadable_older_retained_root() {
    // I-4: only the HEAD root is load-bearing for correctness — every
    // other surviving `.root` file is read purely to rebuild punch
    // bookkeeping. A corrupted OLDER root must not fail recovery; it is
    // skipped (logged), leaking whatever dead-page ranges it named, with
    // every row's data left fully intact.
    //
    // v2 (the middle root, neither oldest nor head) is the one corrupted
    // here rather than v1: v1 is this store's first-ever paged checkpoint
    // and its own `dead_pages` is always empty (nothing to diff against
    // yet), so corrupting it would not actually demonstrate a leak. v2's
    // `dead_pages` — diffed against v1 — is genuinely non-empty, so losing
    // it is an observable leak: `punch_after` never learns to key it under
    // v1's version, so v1's eventual deletion can never trigger its punch.
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().retained_checkpoints(3).build());

    write_rows(&s, 20_000);
    s.checkpoint().unwrap(); // v1, dead=[]
    write_one_update(&s);
    let v2 = s.checkpoint().unwrap(); // dead = diff(v1, v2), non-empty
    write_one_update(&s);
    s.checkpoint().unwrap(); // v3 (head)
    drop(s);

    let dead_v2_before = ultima_db::paged_root_dead_pages_for_test(d.path(), v2).unwrap();
    assert!(!dead_v2_before.is_empty(), "v2's dead list must be non-empty for this test to mean anything");

    // Corrupt v2's root record on disk — still present because
    // `retained_checkpoints(3)` has not exceeded its budget.
    let v2_root = d.path().join(format!("checkpoint_{v2}.root"));
    assert!(v2_root.exists(), "v2's root must still be retained");
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().write(true).open(&v2_root).unwrap();
        // Overwrite the leading bytes so the whole-file CRC check
        // `read_paged_root` performs fails deterministically.
        f.write_all(b"\xffCORRUPTED").unwrap();
    }

    let s2 = store_with(d.path(), PagedOptions::builder().retained_checkpoints(3).build());
    // Must succeed: the corrupted file is v2's, not the head's (v3) — no
    // panic, no propagated error.
    s2.recover().unwrap();

    let r = s2.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.len(), 20_000, "every row must still be readable");
    assert_eq!(t.get(1).map(|row| row.v), Some(999_999));
    drop(r);

    // The leak itself: drive enough further checkpoints that v1 (v2's dead
    // list's naming predecessor) gets deleted by retention, and confirm
    // v2's dead list — unreadable at recovery time — never gets punched,
    // even though the retention gate that would normally trigger it fires.
    for _ in 0..3 {
        write_one_update(&s2);
        s2.checkpoint().unwrap();
    }
    assert!(
        !d.path().join(format!("checkpoint_{v2}.root")).exists(),
        "v2 (superseded several times over) must eventually be deleted too"
    );
    // Nothing here proves dead_pages_punched *excludes* v2's specific
    // ranges (their raw byte offsets are gone with the corrupted file), but
    // the store must still be fully healthy: every row still reads back,
    // and no further recover()/checkpoint() call panicked or errored.
    let r2 = s2.begin_read(None).unwrap();
    assert_eq!(r2.open_table::<Row>("rows").unwrap().len(), 20_000);
}

/// I-3: `retained_checkpoints(0)` must not mean "delete every root,
/// including the one just written." Taken literally, the second checkpoint
/// below would prune both the root it just wrote *and* everything before
/// it, leaving nothing on disk to recover from — a silent, total data loss
/// a caller who (mis)configured `0` would only discover on the next
/// restart. The fix clamps to a floor of 1 at the `cleanup_old_roots` call
/// site (`Store::checkpoint_impl_paged`); this test checkpoints twice under
/// `retained_checkpoints(0)` and confirms the newest root survives and
/// `recover()` still sees every row.
#[test]
fn retained_checkpoints_zero_is_treated_as_one() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().retained_checkpoints(0).build());

    write_rows(&s, 1_000);
    let v1 = s.checkpoint().unwrap();
    assert!(
        d.path().join(format!("checkpoint_{v1}.root")).exists(),
        "the very first checkpoint must not delete itself"
    );

    write_one_update(&s);
    let v2 = s.checkpoint().unwrap();
    assert!(
        d.path().join(format!("checkpoint_{v2}.root")).exists(),
        "retained_checkpoints(0) must still keep the root this checkpoint just wrote \
         (clamped to a floor of 1), not prune it along with everything older"
    );
    drop(s);

    let s2 = store_with(d.path(), PagedOptions::builder().retained_checkpoints(0).build());
    s2.recover().unwrap();
    let r = s2.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.len(), 1_000, "recover() must not start from a pruned-away empty state");
    assert_eq!(t.get(1).map(|row| row.v), Some(999_999));
}
