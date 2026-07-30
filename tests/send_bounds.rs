// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Compile-time assertions about the thread-safety bounds of the public API.
//!
//! `Store` must be `Send + Sync` so clones can be shared across threads.
//! `VersionPin` must be `Send + Sync` so a pinned snapshot can be handed to
//! another thread. The transaction types are `Send` — `ReadTx` is also
//! `Sync`, `WriteTx` is not (it holds `RefCell`s) — which is the outcome of
//! the audit in `docs/tasks/task55_send_audit.md`.
//!
//! These are part of the public contract, not incidental. If a change to
//! `WriteTx` or `ReadTx` introduces a thread-affine field (a `parking_lot`
//! guard, an `Rc`, a raw pointer), this test stops compiling — that is the
//! point. Removing an auto-trait impl is a breaking change; do not "fix" a
//! failure here by deleting the assertion.

#[cfg(feature = "persistence")]
mod common;

use std::sync::mpsc;
use std::time::Duration;

use ultima_db::{Error, ReadTx, Store, StoreConfig, VersionPin, WriteTx, WriterMode};

/// Every cross-thread handshake below is bounded, so a regression that breaks
/// a wakeup fails the test instead of hanging the suite.
const TIMEOUT: Duration = Duration::from_secs(10);

const fn assert_send<T: Send>() {}
const fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn public_types_have_expected_thread_bounds() {
    assert_send_sync::<Store>();
    assert_send_sync::<VersionPin>();
}

/// A transaction can be moved to another thread. It is *not* `Sync` in the
/// `WriteTx` case — the `RefCell`-backed write/read/DDL sets mean it may
/// only ever be touched by one thread at a time, which the compiler
/// enforces.
#[test]
fn transactions_are_send() {
    assert_send::<WriteTx>();
    assert_send::<ReadTx>();
    // `ReadTx` holds nothing but `Arc`s, so it is shareable by reference too.
    assert_send_sync::<ReadTx>();
}

/// The bounds are not just nominal: a transaction opened on one thread
/// really can be committed on another.
///
/// This is the easy configuration — `Store::default()` is SingleWriter with
/// `Persistence::None`, so the intent map, the promotion gate and the WAL
/// park are all inert. The two tests below cover the paths where the audit's
/// claims actually have to hold.
#[test]
fn write_tx_commits_on_a_different_thread() {
    let store = Store::default();

    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut t = wtx.open_table::<String>("notes").unwrap();
        t.insert("hello".to_string()).unwrap();
    }

    let version = std::thread::spawn(move || wtx.commit().unwrap())
        .join()
        .unwrap();

    let rtx = store.begin_read(Some(version)).unwrap();
    let value = std::thread::spawn(move || {
        let t = rtx.open_table::<String>("notes").unwrap();
        t.get(1).cloned()
    })
    .join()
    .unwrap();
    assert_eq!(value.as_deref(), Some("hello"));
}

/// The MultiWriter machinery is keyed by writer id, not by thread, so a
/// transaction that holds an intent can be committed from a foreign thread
/// and still release that intent — waking a writer parked on it.
///
/// Shape: writer A takes the intent on key 1 on the main thread; writer B
/// collides on key 1 from thread 2, drops its own transaction, and parks on
/// A's `CommitWaiter`; A is then *moved to thread 3* and committed there.
/// If `release_all_for` were thread-affine — or if the commit path recorded
/// anything about its opening thread — B would never wake and the bounded
/// wait would fail the test.
#[test]
fn multi_writer_tx_commits_on_a_foreign_thread_and_wakes_a_parked_writer() {
    let store = Store::new(
        StoreConfig::builder()
            .writer_mode(WriterMode::MultiWriter)
            .build(),
    )
    .unwrap();

    // Seed key 1 so both writers contend on an existing row.
    let mut seed = store.begin_write(None).unwrap();
    seed.open_table::<String>("accounts")
        .unwrap()
        .insert("seed".to_string())
        .unwrap();
    seed.commit().unwrap();

    // Writer A: opened here, takes the intent on ("accounts", 1).
    let mut a = store.begin_write(None).unwrap();
    a.open_table::<String>("accounts")
        .unwrap()
        .update(1, "from A".to_string())
        .unwrap();

    let (parked_tx, parked_rx) = mpsc::channel();
    let b = std::thread::spawn({
        let store = store.clone();
        move || {
            // Writer B collides and bails immediately with A's waiter.
            let waiter = {
                let mut b = store.begin_write(None).unwrap();
                let err = b
                    .open_table::<String>("accounts")
                    .unwrap()
                    .update(1, "from B".to_string())
                    .expect_err("A holds the intent on key 1");
                match err {
                    Error::WriteConflict {
                        wait_for: Some(w), ..
                    } => w,
                    other => panic!("expected an intent conflict, got {other:?}"),
                }
                // `b` drops here — the drop-before-wait convention.
            };
            parked_tx.send(()).unwrap();

            assert!(
                waiter.wait_timeout(TIMEOUT),
                "A committed on another thread but never released its intent"
            );

            // Rebase and retry; A's value must already be visible.
            let mut b = store.begin_write(None).unwrap();
            {
                let mut t = b.open_table::<String>("accounts").unwrap();
                assert_eq!(t.get(1), Some(&"from A".to_string()));
                t.update(1, "from B".to_string()).unwrap();
            }
            b.commit().unwrap()
        }
    });

    parked_rx
        .recv_timeout(TIMEOUT)
        .expect("writer B never reached the conflict");

    // A moves to a third thread and commits there.
    let version_a = std::thread::spawn(move || a.commit().unwrap())
        .join()
        .unwrap();
    let version_b = b.join().unwrap();

    assert!(
        version_b > version_a,
        "B rebased onto A: {version_b} should follow {version_a}"
    );
    let rtx = store.begin_read(None).unwrap();
    assert_eq!(
        rtx.open_table::<String>("accounts").unwrap().get(1),
        Some(&"from B".to_string())
    );
}

/// A durable commit parks the committing thread until the data is on disk —
/// on the WAL background thread's fsync under [`Durability::Consistent`], or
/// on its own fsync under [`Durability::ConsistentInline`]. Neither wait is
/// tied to the thread that opened the transaction: the commit is performed
/// on a foreign thread here, and the acknowledged version is then proved
/// durable by recovering it into a fresh store.
#[cfg(feature = "persistence")]
#[test]
fn durable_commit_survives_moving_the_tx_to_another_thread() {
    use ultima_db::{Durability, Persistence, WalWrite};

    for durability in [Durability::Consistent, Durability::ConsistentInline] {
        let dir = common::test_scratch::scratch_dir();
        let config = StoreConfig::builder()
            .persistence(Persistence::standalone(
                dir.path().to_path_buf(),
                durability,
                WalWrite::PerEntry,
            ))
            .build();

        let version = {
            let store = Store::new(config.clone()).unwrap();
            store.register_table::<String>("notes").unwrap();
            store.recover().unwrap();

            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("notes")
                .unwrap()
                .insert("durable".to_string())
                .unwrap();

            // The fsync wait happens on this thread, not the opening one.
            std::thread::spawn(move || wtx.commit().unwrap())
                .join()
                .unwrap()
        };

        // Reopen from the same directory: if the cross-thread commit had not
        // really been fsynced before it returned, the record would be gone.
        let recovered = Store::new(config).unwrap();
        recovered.register_table::<String>("notes").unwrap();
        recovered.recover().unwrap();
        assert_eq!(
            recovered.latest_version(),
            version,
            "{durability:?}: recovered version mismatch"
        );
        let rtx = recovered.begin_read(None).unwrap();
        assert_eq!(
            rtx.open_table::<String>("notes").unwrap().get(1),
            Some(&"durable".to_string()),
            "{durability:?}: acked commit was lost"
        );
    }
}
