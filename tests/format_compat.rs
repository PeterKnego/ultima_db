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
//! Both tests below assert today's build *rejects* these fixtures. That is
//! deliberate: a fixture the current build already accepts would not prove
//! anything about old-format bytes. A later task adds the readers and flips
//! these assertions to acceptance; the red-to-green transition across that
//! boundary is the evidence the readers actually do something.

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

#[test]
fn v0_2_0_checkpoint_is_currently_rejected() {
    let (_dir, store) = store_over_fixture("v0_2_0");
    let err = store.recover().unwrap_err();
    assert!(
        format!("{err}").contains("unsupported format version"),
        "expected a named version rejection, got {err:?}"
    );
}

#[test]
fn v0_3_0_checkpoint_is_currently_rejected() {
    let (_dir, store) = store_over_fixture("v0_3_0");
    let err = store.recover().unwrap_err();
    assert!(
        format!("{err}").contains("unsupported format version"),
        "expected a named version rejection, got {err:?}"
    );
}
