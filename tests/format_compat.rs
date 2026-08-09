// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![cfg(feature = "persistence")]

//! Reading checkpoint files written by earlier releases.
//!
//! The fixtures under `tests/fixtures/formats/` are real bytes produced by
//! the actual released builds (see that directory's README), not
//! reconstructions — a hand-rolled fixture would encode what we *believe*
//! the old format was, which is the belief these tests exist to check.
//!
//! An earlier version of this file asserted today's build *rejects* these
//! fixtures — deliberately, at a point before the readers existed, so the
//! fixtures were proven to be genuinely old-format bytes rather than
//! something the current build already happened to accept. Now that the
//! readers are real, the tests below assert successful recovery instead;
//! that red-to-green transition is the evidence the readers do something.

mod common;

use std::path::{Path, PathBuf};
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct User {
    name: String,
    age: u32,
}

fn fixture(version: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/formats")
        .join(version)
        .join("checkpoint_2.bin")
}

/// Copy a fixture into a fresh scratch dir and open a store over it.
fn store_over_fixture(version: &str) -> (tempfile::TempDir, Store) {
    let dir = common::test_scratch::scratch_dir();
    std::fs::copy(fixture(version), dir.path().join("checkpoint_2.bin")).unwrap();
    let config = StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.path().to_path_buf(),
            Durability::Consistent,
            WalWrite::PerEntry,
        ))
        .build();
    let store = Store::new(config).unwrap();
    store.register_table::<User>("users").unwrap();
    (dir, store)
}

/// The pre-0.3.0 shape: v1 container, v1 table payload.
#[test]
fn a_0_2_0_checkpoint_recovers() {
    let (_dir, store) = store_over_fixture("v0_2_0");
    store.recover().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<User>("users").unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t.get(1).unwrap().name, "alice");
    assert_eq!(t.get(2).unwrap().name, "bob");
    assert_eq!(rtx.version(), 2);
}

/// The released-0.3.0 shape, and the one that matters most: v1 container
/// wrapping a *v2* table payload. The two version axes were bumped in
/// different releases, so this combination is not hypothetical.
#[test]
fn a_0_3_0_checkpoint_recovers() {
    let (_dir, store) = store_over_fixture("v0_3_0");
    store.recover().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<User>("users").unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t.get(1).unwrap().name, "alice");
    assert_eq!(rtx.version(), 2);
}

/// Reading must be *lossless*, not approximately right: checkpointing what
/// we read and reloading it must produce the same state.
#[test]
fn an_old_checkpoint_round_trips_through_the_current_writer() {
    let (dir, store) = store_over_fixture("v0_2_0");
    store.recover().unwrap();
    store.checkpoint().unwrap();
    drop(store);

    let config = StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.path().to_path_buf(),
            Durability::Consistent,
            WalWrite::PerEntry,
        ))
        .build();
    let store2 = Store::new(config).unwrap();
    store2.register_table::<User>("users").unwrap();
    store2.recover().unwrap();
    let rtx = store2.begin_read(None).unwrap();
    let t = rtx.open_table::<User>("users").unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t.get(1).unwrap().name, "alice");
    assert_eq!(t.get(2).unwrap().name, "bob");
}

/// `next_id` must survive, or the recovered store reissues used primary
/// keys. A rows-only assertion passes while this is broken — that exact
/// failure shipped once already (see task61's `next_id` finding).
#[test]
fn an_old_checkpoint_preserves_next_id() {
    let (_dir, store) = store_over_fixture("v0_2_0");
    store.recover().unwrap();
    let mut wtx = store.begin_write(None).unwrap();
    let id = wtx
        .open_table::<User>("users")
        .unwrap()
        .insert(User { name: "carol".into(), age: 41 })
        .unwrap();
    wtx.commit().unwrap();
    assert_eq!(id, 3, "the next id must continue from the old counter, not restart");
}

/// A v1 payload carries no key type. Reading it into a differently-keyed
/// table must fail loudly rather than reinterpret 8-byte ids as String keys.
#[test]
fn a_0_2_0_checkpoint_is_refused_for_a_differently_keyed_table() {
    let dir = common::test_scratch::scratch_dir();
    std::fs::copy(fixture("v0_2_0"), dir.path().join("checkpoint_2.bin")).unwrap();
    let config = StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.path().to_path_buf(),
            Durability::Consistent,
            WalWrite::PerEntry,
        ))
        .build();
    let store = Store::new(config).unwrap();
    store.register_table_keyed::<User, String>("users").unwrap();
    let err = store.recover().unwrap_err();
    assert!(
        format!("{err}").contains("v1 table payloads are u64-keyed"),
        "expected the key-type refusal, got {err:?}"
    );
}
