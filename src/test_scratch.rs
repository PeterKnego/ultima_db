// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Real-disk scratch dirs for durability tests.
//!
//! `TMPDIR` is frequently unset, in which case `tempfile::tempdir()` resolves
//! to `/tmp` — a tmpfs RAM disk in many environments. fsync on tmpfs is a
//! no-op, so a WAL/checkpoint/crash-recovery test that runs there proves
//! nothing about real durability: the data was never actually at risk.
//!
//! [`scratch_dir`] is a drop-in replacement for `tempfile::tempdir().unwrap()`
//! that roots the scratch dir under the crate's `target/` (a real filesystem)
//! and asserts — once per process — that the backing store is not tmpfs/ramfs.
//!
//! This file is the single source of truth: unit tests reach it via
//! `#[cfg(test)] mod test_scratch;` in `lib.rs`, and integration tests via
//! `tests/common/mod.rs`, which `#[path]`-includes it.

use std::path::{Path, PathBuf};
use std::sync::Once;

/// Resolve the real-disk base directory for test scratch.
///
/// Prefers `CARGO_TARGET_TMPDIR` (set by cargo for integration tests and
/// benches, and pointed at `target/`), falling back to
/// `CARGO_MANIFEST_DIR/target/test-scratch` for lib unit tests, where
/// `CARGO_TARGET_TMPDIR` is not provided. Both live on real disk.
fn base_dir() -> PathBuf {
    match option_env!("CARGO_TARGET_TMPDIR") {
        Some(d) => PathBuf::from(d),
        None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/test-scratch"),
    }
}

/// Resolve the `(mount point, fstype)` backing `path` by scanning
/// `/proc/mounts`. Ported from `autobench/src/diskcheck.rs`.
fn backing_fs(path: &Path) -> (String, String) {
    let canon = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
    let mut best: (usize, String, String) = (0, String::new(), "unknown".to_string());
    for line in mounts.lines() {
        let mut f = line.split_whitespace();
        let _dev = f.next();
        let mp = match f.next() {
            Some(m) => m,
            None => continue,
        };
        let fstype = f.next().unwrap_or("unknown");
        if canon.starts_with(Path::new(mp)) && mp.len() >= best.0 {
            best = (mp.len(), mp.to_string(), fstype.to_string());
        }
    }
    (best.1, best.2)
}

/// Panic if `base` is on tmpfs/ramfs, where fsync is a no-op and every
/// durability guarantee under test is silently void. Runs its check once per
/// process. Opt out with `ULTIMA_ALLOW_TMPFS=1` (e.g. a quick local run where
/// durability is knowingly not being measured).
fn assert_real_disk(base: &Path) {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("ULTIMA_ALLOW_TMPFS").is_some() {
            return;
        }
        let (mount, fs) = backing_fs(base);
        assert!(
            fs != "tmpfs" && fs != "ramfs",
            "test scratch dir {} is on {fs} (mount {mount}); fsync is a no-op there, \
             so durability tests would be meaningless. Point TMPDIR/CARGO_TARGET_TMPDIR \
             at a real disk, or set ULTIMA_ALLOW_TMPFS=1 to bypass.",
            base.display(),
        );
    });
}

/// Create a fresh, auto-cleaned scratch directory on a real (non-tmpfs) disk.
///
/// Drop-in for `tempfile::tempdir().unwrap()`: returns a [`tempfile::TempDir`]
/// with the same `.path()` accessor and RAII cleanup on drop. Unlike the bare
/// call, the directory is guaranteed to be backed by a real filesystem so that
/// fsync — and thus the durability behavior under test — is genuine.
pub fn scratch_dir() -> tempfile::TempDir {
    let base = base_dir();
    std::fs::create_dir_all(&base).expect("create test scratch base dir");
    assert_real_disk(&base);
    tempfile::Builder::new()
        .prefix("ultima-test-")
        .tempdir_in(&base)
        .expect("create scratch dir")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The scratch base this build resolved to must be a real disk — otherwise
    /// every durability test in the crate is silently meaningless.
    #[test]
    fn scratch_base_is_not_tmpfs() {
        let base = base_dir();
        std::fs::create_dir_all(&base).unwrap();
        let (_mount, fs) = backing_fs(&base);
        assert!(
            fs != "tmpfs" && fs != "ramfs",
            "scratch base {} is on {fs}",
            base.display(),
        );
    }

    /// The guard has teeth: `backing_fs` correctly classifies a tmpfs mount, so
    /// `assert_real_disk` would fire if a base ever resolved onto one. `/dev/shm`
    /// is a tmpfs on Linux; skip the assertion if it is absent or not tmpfs
    /// (non-standard host) rather than fail spuriously.
    #[test]
    fn backing_fs_detects_tmpfs() {
        let shm = Path::new("/dev/shm");
        if shm.exists() {
            let (_mount, fs) = backing_fs(shm);
            if fs == "tmpfs" {
                return; // detection works
            }
        }
        eprintln!("[test_scratch] no tmpfs mount available to exercise detection; skipping");
    }
}
