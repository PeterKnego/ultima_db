// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 4 format-compat coverage: a paged directory written by pre-block-
//! decode code (`tests/fixtures/paged_prechange/`, real bytes, task62
//! discipline — see that directory's README) must still recover and read
//! correctly now that `NodeCodec::decode` builds value blocks for
//! `DataLeaf` pages. Wire format is unchanged (spec §3): only the in-memory
//! representation `decode` builds from those bytes changes, never the
//! bytes themselves — that's also what the golden-bytes unit test in
//! `src/pagecodec.rs` (`encode_bytes_identical_across_representations`)
//! pins directly against the codec, without going through a whole store.

#![cfg(feature = "persistence")]

use std::path::Path;

use ultima_db::{Durability, PagedOptions, Persistence, Store, StoreConfig, WalWrite};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

/// Recursive copy of a fixture directory into a fresh tempdir — the fixture
/// dir (`pages.bin`, a `checkpoint_*.root`, `wal.bin`, and this one's
/// `README.md`) must never be mutated by a test run, since it's committed,
/// real, and load-bearing for future format-compat runs.
fn copy_fixture_to_temp(name: &str) -> tempfile::TempDir {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name);
    let dst = tempfile::tempdir().unwrap();
    copy_dir_recursive(&src, dst.path());
    dst
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if path.is_dir() {
            std::fs::create_dir_all(&target).unwrap();
            copy_dir_recursive(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

/// Open (but do not recover) a paged store over `dir`.
fn open_paged(dir: &Path) -> Store {
    let p = Persistence::standalone(dir.to_path_buf(), Durability::Eventual, WalWrite::Coalesced)
        .paged(PagedOptions::builder().build())
        .unwrap();
    Store::new(StoreConfig::builder().persistence(p).build()).unwrap()
}

#[test]
fn prechange_directory_recovers_and_reads() {
    let dir = copy_fixture_to_temp("paged_prechange");
    let s = open_paged(dir.path());
    s.register_table_paged::<Row>("rows").unwrap();
    s.recover().unwrap();
    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    for k in 1..=200u64 {
        assert_eq!(t.get(k).unwrap().v, k, "row {k}");
    }
}
