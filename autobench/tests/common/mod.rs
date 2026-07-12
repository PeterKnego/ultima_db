// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared test helpers for autobench integration tests.
//!
//! `scratch_dir` returns a real-disk (non-tmpfs) scratch directory so
//! durability/torture tests actually exercise fsync — see the module docs in
//! ultima_db's `src/test_scratch.rs` for why tmpfs would void the guarantee.
//! Reuses the crate's own `diskcheck` real-disk guard.

use ultima_autobench::diskcheck;

/// Fresh, auto-cleaned scratch directory on a real disk. Drop-in for
/// `tempfile::tempdir().unwrap()`. Panics if the base is tmpfs/ramfs (bypass
/// with `ULTIMA_ALLOW_TMPFS=1`).
pub fn scratch_dir() -> tempfile::TempDir {
    let root = diskcheck::bench_root("test-scratch");
    diskcheck::assert_real_disk(&root, std::env::var_os("ULTIMA_ALLOW_TMPFS").is_some());
    tempfile::Builder::new()
        .prefix("ultima-test-")
        .tempdir_in(&root)
        .expect("create scratch dir")
}
