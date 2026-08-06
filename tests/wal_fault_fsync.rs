// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! In-flight WAL fault: the durability barrier itself fails.
//!
//! Nothing anywhere in the repo currently assumes `sync_all`/`sync_data` can
//! fail — `tests/corruption_recovery.rs` edits a *closed* file, which can only
//! model damage that happened after a successful sync. This file covers the
//! opposite: the batch is physically written into the preallocated region and
//! then the barrier that was supposed to make it durable returns an error.
//!
//! ## One mutation value per test binary
//!
//! `crate::mutation::active()` memoises `ULTIMA_MUTATION` in a `OnceLock`, so
//! the *first* read wins for the whole process and every later `set_var` /
//! `remove_var` is silently ignored. A second test in this file declaring a
//! different value would run under this one's fault while naming its own — and
//! still pass, because a wrong-fault error is still an error. That is not
//! hypothetical; see the module docs of `tests/wal_fault_failed_extend.rs` for
//! the observed case. `--test-threads=1` does not help *with that*: the
//! `OnceLock` is per-process, and it is one process either way. The only remedy
//! is process isolation, so each mutation value gets its own `tests/*.rs` file
//! (cargo gives one binary per file, no `[[test]]` stanza needed). **Do not add
//! a test with a different `ULTIMA_MUTATION` value to this file.**
//!
//! ## …but this binary must still be run with `--test-threads=1`
//!
//! Two separate facts, and conflating them is how the `unsafe` below acquired a
//! false justification once already: **one mutation value per binary** is what
//! makes the fault *deterministic*, and no thread count substitutes for it;
//! **`--test-threads=1`** is what makes the `unsafe { env::set_var }` below
//! *sound*, and nothing else substitutes for that.
//!
//! This binary is not single-test: `mod common;` pulls in `src/test_scratch.rs`,
//! whose `#[cfg(test)] mod tests` contributes two more `#[test]` fns. libtest
//! runs them concurrently by default, and both `scratch_dir()`
//! (`ULTIMA_ALLOW_TMPFS`, `src/test_scratch.rs:63`) and `Store::new`
//! (`ULTIMA_OVERLAY_CAP`, `src/store.rs:563`) call `std::env::var*`. A getenv
//! concurrent with a setenv is UB, so every gate invocation passes
//! `--test-threads=1` — see `make test/wal-faults` and `.github/workflows/ci.yml`.
//! Run it by hand the same way:
//!
//! ```text
//! cargo test --features persistence,fulltext,mutation-testing \
//!            --test wal_fault_fsync -- --test-threads=1
//! ```
//!
//! Requires `--features mutation-testing`.
#![cfg(feature = "mutation-testing")]

use std::path::Path;
use ultima_db::{Durability, Error, Persistence, Store, StoreConfig, WalWrite};

mod common;

/// Pre-size for `wal.bin`. Only has to exceed the test's batch (a few hundred
/// bytes) so `PreallocFileSink::sync` finds `need <= capacity` and skips the
/// extend block; the exact value is not load-bearing and deliberately does not
/// track `WAL_PREALLOC_CHUNK`.
const PRESIZED_WAL_BYTES: u64 = 1024 * 1024;

/// Rows in the single transaction. > 1 so recovery can be checked for
/// all-or-nothing rather than merely non-empty.
const ROWS: usize = 3;

fn standalone_prealloc_config(dir: &Path) -> StoreConfig {
    StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.to_path_buf(),
            Durability::Consistent,
            WalWrite::CoalescedPrealloc,
        ))
        .build()
}

/// A failing fsync must never be reported as a durable commit.
///
/// The contract is not that the commit succeeds — it is that a commit which
/// returned `Ok` survives recovery, so one that could not be synced must not
/// return `Ok`, must not be promoted into a visible snapshot, and must leave
/// the store refusing further writes rather than continuing on a WAL it no
/// longer trusts.
///
/// `Durability::Consistent` is deliberate. Under `Eventual` the commit returns
/// *before* the fsync, so its `Ok` carries no durability claim at all and any
/// assertion built on it is meaningless — there is no promise to falsify.
/// Under `Consistent` the committing thread parks on the WAL background
/// thread's durability channel, so the sink's error is the commit's error.
///
/// ### Why `wal.bin` is pre-sized
///
/// `Mutation::FailSync` fires at *both* injection points and cannot be aimed
/// from the env var: `preallocate_to`'s `sync_all` and `PreallocFileSink::sync`'s
/// trailing `sync_data`. On a fresh store the first commit must extend, so the
/// fault lands in `preallocate_to` — which fails *before* any record is
/// written, rolls the size back to 0, and re-covers ground already pinned by
/// `tests/wal_fault_failed_extend.rs`. Pre-sizing `wal.bin` reproduces the
/// state after any prior run or prune (`open_with_chunk` adopts
/// `metadata().len()` as `capacity`), so the extend block is skipped and the
/// fault lands on the steady-state barrier — the only arrangement in which the
/// data has actually reached the file at the moment durability is lost.
#[test]
fn a_failing_fsync_is_never_reported_as_a_durable_commit() {
    // SAFETY: this binary is run with `--test-threads=1` (see the module docs,
    // `make test/wal-faults` and `.github/workflows/ci.yml`), so no other test
    // is executing while the environment is mutated — which matters because
    // `scratch_dir()` and `Store::new` both call `std::env::var*`. Set before
    // the store, and therefore before any WAL thread, exists.
    unsafe { std::env::set_var("ULTIMA_MUTATION", "fail-sync") };
    // Real disk, not tmpfs: fsync is a no-op there, which for *this* test in
    // particular would void the entire subject matter (see src/test_scratch.rs).
    let dir = common::test_scratch::scratch_dir();
    let wal = dir.path().join("wal.bin");

    // Pre-existing preallocated capacity, as after a prior run or a prune.
    {
        let f = std::fs::File::create(&wal).unwrap();
        f.set_len(PRESIZED_WAL_BYTES).unwrap();
        f.sync_all().unwrap();
    }

    let store = Store::new(standalone_prealloc_config(dir.path())).unwrap();
    store.register_table::<String>("t").unwrap();
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut t = wtx.open_table::<String>("t").unwrap();
        for i in 0..ROWS {
            t.insert(format!("row{i}")).unwrap();
        }
    }
    let res = wtx.commit();

    // 1. The commit must fail, and fail as *this* fault. Pin the error
    //    identity, not `is_err()`: under a memoised-mutation mix-up a commit
    //    can fail for a fault this test never ran, and a bare `is_err()` would
    //    accept that and report a green gate for the wrong reason. Under
    //    `Consistent` the WAL background thread owns the sink, so the sink's
    //    error reaches the committing thread through the durability waiter and
    //    the poison latch — hence `Poisoned`, not `Persistence`, with the
    //    original message nested inside.
    match &res {
        Err(Error::Poisoned(msg)) => assert!(
            msg.contains("injected fsync failure"),
            "commit failed with a poison unrelated to this test's fault: {msg}"
        ),
        other => panic!("an injected fsync failure must surface as Error::Poisoned, got {other:?}"),
    }

    // 2. Not reported durable also means not *visible*: the un-synced write
    //    must not be promoted. A fresh store is at version 0, so a promotion
    //    would show up here as 1.
    assert_eq!(
        store.latest_version(),
        0,
        "a commit whose fsync failed was promoted into a visible snapshot"
    );

    // 3. And the store must stop accepting writes rather than carry on
    //    appending to a WAL whose durability it can no longer vouch for.
    assert!(
        matches!(store.begin_write(None), Err(Error::Poisoned(_))),
        "the store kept accepting writes after a WAL durability failure"
    );
    drop(store);

    // 4. Anti-vacuity guard. Everything above would also hold if the fault had
    //    landed in `preallocate_to`, which fails before `write_all` and is
    //    already covered elsewhere — so prove the batch physically reached the
    //    file and it was the *steady-state* barrier that failed. In the extend
    //    case the rollback leaves offset 0 untouched (still preallocated
    //    zeros), so a non-zero frame length prefix is the discriminator. The
    //    unchanged file size says the same thing from the other side: no
    //    extend was attempted.
    let bytes = std::fs::read(&wal).unwrap();
    assert_eq!(
        bytes.len() as u64,
        PRESIZED_WAL_BYTES,
        "wal.bin changed size; the fault landed on the extend path, not the \
         steady-state barrier this test exists to reach"
    );
    assert_ne!(
        u32::from_le_bytes(bytes[..4].try_into().unwrap()),
        0,
        "no frame at offset 0; the batch never reached the file, so the fsync \
         that failed had nothing to make durable and this test is vacuous"
    );

    // 5. Recovery must be consistent with that outcome. The un-acked batch may
    //    or may not come back — `write` without `fsync` still survives a
    //    process fault via the page cache, and this harness cannot simulate
    //    power loss — and either is legal for a commit that returned `Err`.
    //    What is *not* legal is an unreadable WAL or a half-applied
    //    transaction, so pin those: recovery succeeds, and the row count is
    //    all-or-nothing. (Observed on Linux: all `ROWS` rows replay.)
    let store = Store::new(standalone_prealloc_config(dir.path())).unwrap();
    store.register_table::<String>("t").unwrap();
    store
        .recover()
        .expect("an un-synced tail must leave the WAL readable, not unrecoverable");
    let n = store
        .begin_read(None)
        .unwrap()
        .open_table::<String>("t")
        .map(|t| t.len())
        .unwrap_or(0);
    assert!(
        n == 0 || n == ROWS,
        "recovery replayed {n} of {ROWS} rows — a transaction that was never \
         acked came back half-applied"
    );

    // Hygiene only: `active()` has already memoised, so this does not
    // deactivate the fault for the rest of the process.
    //
    // SAFETY: as above — `--test-threads=1`, so no other test is running. The
    // recovery store above is still alive, but its WAL thread reads the
    // environment only through `mutation::active()`, whose `OnceLock` was
    // initialised long before this line.
    unsafe { std::env::remove_var("ULTIMA_MUTATION") };
}
