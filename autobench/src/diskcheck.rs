// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Real-disk guard for fsync-meaningful microbenches. fsync on tmpfs/ramfs is
//! a no-op, which would make every durability number meaningless. Ported from
//! `benches/wal_bench.rs` (the parsing logic is copied verbatim).

use std::path::{Path, PathBuf};

/// Resolve the (mount point, fstype) backing `path` by scanning /proc/mounts.
pub fn backing_fs(path: &Path) -> (String, String) {
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

/// Create and return an on-disk benchmark root under the crate's `target/`.
/// CARGO_MANIFEST_DIR here is `autobench/`, so this resolves to
/// `autobench/target/<name>` — which is git-ignored via `autobench/.gitignore`.
pub fn bench_root(name: &str) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target").join(name);
    std::fs::create_dir_all(&root).expect("create bench root");
    root
}

/// Assert that `root` is on a real (non-tmpfs/ramfs) filesystem, unless
/// `allow_tmpfs` (quick/CI mode, where results are not durability-meaningful
/// anyway). Always prints the backing fs for the run log.
pub fn assert_real_disk(root: &Path, allow_tmpfs: bool) {
    let (mount, fs) = backing_fs(root);
    eprintln!("[autobench] dir = {} | mount = {mount} | fs = {fs}", root.display());
    if allow_tmpfs {
        return; // quick/CI mode: results are not durability-meaningful anyway
    }
    assert!(
        fs != "tmpfs" && fs != "ramfs",
        "bench dir is on {fs}; fsync is a no-op there — point at a real disk"
    );
}
