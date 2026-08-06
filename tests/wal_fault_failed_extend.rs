// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! In-flight WAL faults: a syscall fails *partway through* an operation, while
//! the sink still holds in-memory state.
//!
//! Distinct from `tests/corruption_recovery.rs`, which edits a closed file and
//! then recovers. That cannot produce the state these tests target — F2's
//! defect is precisely that `preallocate_to` leaves the file longer than
//! `capacity` *while the sink is mid-extend*.
//!
//! ## One mutation value per test binary
//!
//! `crate::mutation::active()` memoises `ULTIMA_MUTATION` in a `OnceLock`, so
//! the *first* read wins for the whole process and every later `set_var` is
//! silently ignored. A second test in this file would therefore run under the
//! first test's fault while naming its own — and still pass, because a
//! wrong-fault error is still an error. That is not hypothetical: a scratch
//! `tear-frame-at=64` test co-resident with this one observed
//! `Err(Poisoned(..injected ENOSPC))` and a 0-byte WAL, but run alone with
//! `--exact` observed `Ok` and a 16 MiB WAL. Same code, opposite behaviour.
//!
//! `--test-threads=1` does **not** fix this — the falsifying run above already
//! had it. The only remedy is process isolation, so each mutation value gets
//! its own `tests/*.rs` file (cargo gives one binary per file, no `[[test]]`
//! stanza needed). Hence `wal_fault_failed_extend.rs` rather than one shared
//! `wal_inflight_faults.rs`. **Do not add a test with a different
//! `ULTIMA_MUTATION` value to this file.**
//!
//! Requires `--features mutation-testing`.
#![cfg(feature = "mutation-testing")]

use std::path::Path;
use ultima_db::{Durability, Error, Persistence, Store, StoreConfig, WalWrite};

mod common;

fn standalone_prealloc_config(dir: &Path) -> StoreConfig {
    StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.to_path_buf(),
            Durability::Consistent,
            WalWrite::CoalescedPrealloc,
        ))
        .build()
}

/// A failed extend must leave the on-disk size one we know is durable.
///
/// `preallocate_to` zero-fills then `sync_all`s, establishing task37 §4
/// invariant 2. When the zero-fill dies partway the error escapes before both
/// the sync and `capacity = new_cap`, so without the rollback the file is
/// physically longer than `capacity` and that extension was never synced — and
/// the next `open` adopts it via `metadata().len()`.
#[test]
fn a_failed_extend_does_not_leave_the_file_longer_than_capacity() {
    // SAFETY: single-threaded test binary; set before the store is built.
    // The payload is load-bearing in BOTH directions, and this test is vacuous
    // if it drifts outside those bounds:
    //
    //   * It must be > 0. With `fail-write-after=0` nothing is ever written,
    //     so there is no partial extension and the file is 0 bytes with and
    //     without the rollback — the test would pass against the very bug it
    //     exists to catch.
    //   * It must be < the extend size (the first extend is 0 -> 16 MiB, one
    //     `WAL_PREALLOC_CHUNK`, src/wal.rs:1157). At or above it the injection
    //     never fires, `preallocate_to` simply succeeds, and the test passes
    //     for the wrong reason against a 16 MiB file.
    //
    // 65536 is one 64 KiB zero-fill iteration: the smallest amount that leaves
    // a real partial extension. If `WAL_PREALLOC_CHUNK` changes this value
    // still holds; only a change to the zero-fill buffer size moves it.
    unsafe { std::env::set_var("ULTIMA_MUTATION", "fail-write-after=65536") };
    // Real disk, not tmpfs: fsync is a no-op there, which would void the
    // durability semantics these faults are about (see src/test_scratch.rs).
    let dir = common::test_scratch::scratch_dir();

    let store = Store::new(standalone_prealloc_config(dir.path())).unwrap();
    store.register_table::<String>("t").unwrap();

    // Write enough to force an extend past the first chunk.
    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table::<String>("t").unwrap();
    for i in 0..5000u64 {
        t.insert(format!("row{i}")).unwrap();
    }
    let res = wtx.commit();

    // The commit must fail loudly — never silently half-durable. Pin the error
    // *identity*, not just `is_err()`: under a memoised-mutation mix-up a
    // commit can fail for a fault this test never asked for, and a bare
    // `is_err()` would accept that and report a green gate for the wrong
    // reason. Under `Durability::Consistent` the WAL background thread owns
    // the sink, so the sink's error reaches the committing thread through the
    // durability waiter and the poison latch — hence `Poisoned`, not
    // `Persistence`, with the original message nested inside.
    match &res {
        Err(Error::Poisoned(msg)) => assert!(
            msg.contains("injected ENOSPC"),
            "commit failed with a poison unrelated to this test's fault: {msg}"
        ),
        other => panic!("an injected ENOSPC must surface as Error::Poisoned, got {other:?}"),
    }
    drop(store);

    // The invariant: the file on disk is exactly the last size that was
    // sync_all'd. A fresh prealloc WAL opens with `capacity = 0`, so a failed
    // FIRST extend must roll back to 0. Without the rollback the file is
    // 65536 bytes — the partial zero-fill that ENOSPC interrupted.
    //
    // NOT `len % chunk == 0`: `WAL_PREALLOC_CHUNK` is 16 MiB, but 65536 is
    // itself a multiple of 4096 and of every smaller power of two, so a
    // modulo assertion would hold both with and without the rollback and this
    // acceptance gate would be vacuous. Assert the exact length.
    let wal = dir.path().join("wal.bin");
    let len = std::fs::metadata(&wal).unwrap().len();
    assert_eq!(
        len, 0,
        "wal.bin is {len} bytes; a failed first extend must leave it at the \
         capacity that was last sync_all'd (0), not at the partial zero-fill"
    );

    unsafe { std::env::remove_var("ULTIMA_MUTATION") };
}
