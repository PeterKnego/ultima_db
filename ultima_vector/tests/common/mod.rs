// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Real-disk scratch dirs for ultima_vector durability tests.
//!
//! `TMPDIR` is often unset, so `tempfile::tempdir()` lands on `/tmp` — a tmpfs
//! RAM disk in many environments, where fsync is a no-op and durability tests
//! prove nothing. `scratch_dir` roots scratch on a real disk (under `target/`)
//! and asserts the base is not tmpfs. Mirrors ultima_db's `src/test_scratch.rs`.

use std::path::{Path, PathBuf};
use std::sync::Once;

fn base_dir() -> PathBuf {
    match option_env!("CARGO_TARGET_TMPDIR") {
        Some(d) => PathBuf::from(d),
        None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/test-scratch"),
    }
}

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
             so durability tests would be meaningless. Set ULTIMA_ALLOW_TMPFS=1 to bypass.",
            base.display(),
        );
    });
}

/// Fresh, auto-cleaned scratch directory on a real disk. Drop-in for
/// `tempfile::tempdir().unwrap()`.
pub fn scratch_dir() -> tempfile::TempDir {
    let base = base_dir();
    std::fs::create_dir_all(&base).expect("create test scratch base dir");
    assert_real_disk(&base);
    tempfile::Builder::new()
        .prefix("ultima-test-")
        .tempdir_in(&base)
        .expect("create scratch dir")
}
