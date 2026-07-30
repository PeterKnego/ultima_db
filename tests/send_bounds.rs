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

use ultima_db::{ReadTx, Store, VersionPin, WriteTx};

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
