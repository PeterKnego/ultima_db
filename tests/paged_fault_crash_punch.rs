// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 11 crash point: `Mutation::CrashBeforePunch` — the paged
//! checkpoint's new root record is durably renamed into place and
//! `cleanup_old_roots` has already deleted whatever old roots retention no
//! longer allows, but the ranges that deletion made punchable were never
//! actually punched. This leaks disk space (an unpunched hole), never data:
//! recovery must read back every row, and a following checkpoint must
//! apply the pending punch (`PagedStatsSnapshot::dead_pages_punched` grows).
//!
//! ## Why this file exists on its own, and how the once-per-process fault
//! reaches a specific checkpoint call
//!
//! Same constraint as `tests/paged_fault_crash_root.rs` — see that file's
//! module doc for the full explanation of `mutation::active()`'s
//! process-wide memoisation. Here the crash check's precondition is
//! `!deleted.is_empty()` (this checkpoint's own `cleanup_old_roots` call
//! actually deleted something) rather than `last_root_before.is_some()`: a
//! checkpoint with nothing pending crashing "before a punch" would prove
//! nothing about the punch step. With `retained_checkpoints(1)`, the first
//! checkpoint after a prior root exists is the first one whose
//! `cleanup_old_roots` deletes anything, so it is also the first (and,
//! behind the `AtomicBool` latch in `Store::checkpoint_impl_paged`, the
//! only) call this crash can fire on.
#![cfg(all(feature = "persistence", feature = "mutation-testing"))]

use ultima_db::{Durability, PagedOptions, Persistence, Store, StoreConfig, WalWrite};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

fn store(dir: &std::path::Path) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().retained_checkpoints(1).build())
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
fn crash_before_punch_leaks_space_not_data() {
    // SAFETY: this binary is run with `--test-threads=1` — see
    // `tests/paged_fault_crash_root.rs`'s module doc.
    unsafe { std::env::set_var("ULTIMA_MUTATION", "crash-before-punch") };

    let d = tempfile::tempdir().unwrap();
    let v1 = {
        let s = store(d.path());
        write_rows(&s, 5_000);
        // First-ever checkpoint: only one root exists afterward, so
        // `cleanup_old_roots(dir, 1)` deletes nothing. The crash check's
        // `!deleted.is_empty()` precondition is unmet — succeeds normally.
        let v1 = s.checkpoint().unwrap();
        assert_eq!(s.paged_stats().unwrap().dead_pages_punched, 0);

        write_one_update(&s);
        // Second checkpoint: writing v2's root leaves 2 roots on disk, over
        // the retained budget of 1, so `cleanup_old_roots` deletes v1 —
        // `deleted` is non-empty and the crash fires here, after v1 is
        // already gone from disk but before v2's now-punchable dead list
        // (diffed against v1) is actually punched.
        let err = s.checkpoint().unwrap_err();
        assert!(
            err.to_string().contains("root record renamed and old roots pruned, before punch"),
            "unexpected error: {err}"
        );
        v1
    };

    // v1 really is gone — the crash lands *after* `cleanup_old_roots`, not
    // before it. v2's root, on the other hand, made it to disk (the crash
    // is after the rename); `recover()` below confirms it is readable and
    // every row survived.
    assert!(!d.path().join(format!("checkpoint_{v1}.root")).exists());

    let s = store(d.path());
    s.recover().unwrap();
    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.len(), 5_000, "no data may be lost to an unpunched hole");
    assert_eq!(t.get(1).map(|row| row.v), Some(999_999));
    drop(r);
    assert_eq!(
        s.paged_stats().unwrap().dead_pages_punched,
        0,
        "recovery only rebuilds bookkeeping; it must not punch anything itself"
    );

    // The mutation already fired once; the `AtomicBool` latch means this
    // checkpoint succeeds despite `ULTIMA_MUTATION` still reading as
    // "active", and it must apply the pending punch recovery queued.
    write_one_update(&s);
    s.checkpoint().unwrap();
    let punched_after_pending = s.paged_stats().unwrap().dead_pages_punched;
    assert!(
        punched_after_pending > 0,
        "the following checkpoint must punch the pending list recovery reconstructed"
    );

    // Punch idempotence: `pending_punch` is drained (`Vec::append`) and
    // `punch_after` entries are removed on use, so a further checkpoint with
    // nothing newly deleted must not re-punch the same ranges — the count
    // only grows from genuinely new work, never from re-applying what
    // recovery already queued once.
    s.checkpoint().unwrap();
    assert_eq!(
        s.paged_stats().unwrap().dead_pages_punched,
        punched_after_pending,
        "a checkpoint with nothing newly deleted must not re-punch the pending list"
    );

    // Hygiene only: `active()` has already memoised, so this does not
    // deactivate the fault for the rest of the process.
    //
    // SAFETY: as above — `--test-threads=1`.
    unsafe { std::env::remove_var("ULTIMA_MUTATION") };
}
