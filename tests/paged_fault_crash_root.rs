// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 11 crash point: `Mutation::CrashAfterPageSync` — the paged
//! checkpoint's new pages are durable (`PageFile::sync()` succeeded) but the
//! root record naming them was never written. Recovery must fall back to
//! the previous root plus WAL replay, and a following checkpoint must
//! overwrite the orphaned pages rather than reference them.
//!
//! ## Why this file exists on its own
//!
//! `crate::mutation::active()` memoises `ULTIMA_MUTATION` in a `OnceLock`,
//! so the first read wins for the whole process (see `src/mutation.rs`'s
//! module doc and `tests/wal_fault_fsync.rs` for the established pattern).
//! `ULTIMA_MUTATION=crash-after-page-sync` therefore has to be set before
//! `Store::new` — before any store, and therefore before the mutation is
//! ever read — which means it is "active" for *every* `checkpoint()` call
//! in this process, not just the one this test means to fail.
//!
//! `Store::checkpoint_impl_paged`'s crash check closes that gap two ways:
//! it only fires when `last_root_before.is_some()` (there is a previous
//! root to fall back to — this store's first-ever checkpoint has nothing to
//! supersede, so it is exempt), and it is latched behind a
//! process-lifetime `AtomicBool` so it fires at most once even though the
//! mutation stays "active" for the rest of the process (see
//! `CRASH_AFTER_PAGE_SYNC_FIRED` in `src/store.rs`). That is what makes
//! "checkpoint v1 successfully, update, then have the *next* checkpoint
//! crash" reachable at all under a once-per-process fault.
#![cfg(all(feature = "persistence", feature = "mutation-testing"))]

use ultima_db::{Durability, PagedOptions, Persistence, Store, StoreConfig, WalWrite};

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

fn write_one_update(s: &Store) {
    let mut w = s.begin_write(None).unwrap();
    let mut t = w.open_table::<Row>("rows").unwrap();
    t.update(1, Row { v: 999_999 }).unwrap();
    w.commit().unwrap();
}

#[test]
fn crash_between_page_sync_and_root_rename_recovers_previous_root() {
    // SAFETY: this binary is run with `--test-threads=1` (mirrors
    // `tests/wal_fault_fsync.rs` — `scratch_dir`/`Store::new` both read
    // other env vars via `std::env::var*`, and a concurrent getenv/setenv
    // pair is UB). Set before any store exists, so `mutation::active()`'s
    // `OnceLock` memoises this value rather than "no mutation".
    unsafe { std::env::set_var("ULTIMA_MUTATION", "crash-after-page-sync") };

    let d = tempfile::tempdir().unwrap();
    let v1 = {
        let s = store(d.path());
        write_rows(&s, 5_000);
        // This store's first-ever checkpoint: `last_root_before` is `None`,
        // so the crash check's precondition is unmet and it must succeed
        // normally regardless of the mutation being "active".
        let v1 = s.checkpoint().unwrap();

        write_one_update(&s);
        // Now `last_root_before` is `Some(v1)`: the crash fires here, right
        // after the update's pages are synced but before the new root
        // (which would supersede v1) is written.
        let err = s.checkpoint().unwrap_err();
        assert!(
            err.to_string().contains("after page sync, before root record"),
            "unexpected error: {err}"
        );
        v1
    }; // store (and its PageFile handle) dropped here — not a real crash,
       // but nothing below relies on anything the drop would have flushed
       // that wasn't already durable before the injected error.

    // The failed checkpoint must not have replaced the durable root.
    assert!(d.path().join(format!("checkpoint_{v1}.root")).exists());

    // A new store over the same directory recovers root v1 plus WAL replay
    // of the update that was committed (and durably logged) before the
    // checkpoint that tried to supersede v1 crashed.
    let s = store(d.path());
    s.recover().unwrap();
    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get(1).map(|row| row.v), Some(999_999));
    assert_eq!(t.len(), 5_000);
    drop(r);

    // The mutation already fired once (during the first store's second
    // checkpoint call); the AtomicBool latch is process-lifetime, so this
    // checkpoint — despite `ULTIMA_MUTATION` still reading as "active" —
    // must succeed and overwrite the pages orphaned by the crash.
    s.checkpoint().unwrap();
    let r = s.begin_read(None).unwrap();
    assert_eq!(r.open_table::<Row>("rows").unwrap().get(1).map(|row| row.v), Some(999_999));

    // Hygiene only: `active()` has already memoised, so this does not
    // deactivate the fault for the rest of the process.
    //
    // SAFETY: as above — `--test-threads=1`.
    unsafe { std::env::remove_var("ULTIMA_MUTATION") };
}
