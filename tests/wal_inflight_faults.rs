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
//! `ULTIMA_MUTATION` is process-global and `OnceLock`-memoised, so this binary
//! must run with `--test-threads=1`; see `make test/wal-faults`.
//!
//! Requires `--features mutation-testing`.
#![cfg(feature = "mutation-testing")]

use std::path::Path;
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};

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
    unsafe { std::env::set_var("ULTIMA_MUTATION", "fail-write-after=65536") };
    let dir = tempfile::tempdir().unwrap();

    let store = Store::new(standalone_prealloc_config(dir.path())).unwrap();
    store.register_table::<String>("t").unwrap();

    // Write enough to force an extend past the first chunk.
    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table::<String>("t").unwrap();
    for i in 0..5000u64 {
        t.insert(format!("row{i}")).unwrap();
    }
    let res = wtx.commit();

    // The commit must fail loudly — never silently half-durable. Under
    // `Durability::Consistent` the WAL background thread owns the sink, so the
    // sink's error reaches the committing thread through the durability
    // waiter and the poison latch: `Err(Error::Poisoned("WAL durability
    // failure: persistence error: injected ENOSPC"))`.
    assert!(res.is_err(), "an injected ENOSPC must surface, got {res:?}");
    drop(store);

    // The invariant: the file on disk is exactly the last size that was
    // sync_all'd. A fresh prealloc WAL opens with `capacity = 0`, so a failed
    // FIRST extend must roll back to 0. Without the rollback the file is
    // 65536 bytes — the partial zero-fill that ENOSPC interrupted.
    //
    // NOT `len % chunk == 0`: `WAL_PREALLOC_CHUNK` is 16 MiB, but 65536 is
    // itself a multiple of 4096 and of every smaller power of two, so a
    // modulo assertion would hold with *and* without the rollback and this
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
