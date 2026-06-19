// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Fitness function for the `journal-commit` task. Mirrors the Raft-log
//! usage in ultima_cluster (unbatched appends, notifier acks, iter_range
//! replay, truncate/purge with metadata fsync). Emits a flat metric map.

use std::collections::BTreeMap;
use std::time::Instant;

use ultima_journal::{Durability, Journal, JournalConfig};

use crate::diskcheck;
use crate::sampling::{batched_samples_ns, percentile};

/// Every key `run()` emits, in stable order. The smoke test and
/// `--write-baseline` iterate this list.
pub const METRIC_KEYS: &[&str] = &[
    "group_commit_throughput",
    // Same burst loop with `preallocate_segments: true` — the end-to-end
    // pipeline win from folding preallocation into group-commit (see the
    // group_commit block in `run`).
    "group_commit_throughput_prealloc",
    "append_consistent_p99_ns",
    "append_consistent_p99_ns_64b",
    "append_consistent_p99_ns_4k",
    "append_eventual_ack_p99_ns",
    "append_eventual_throughput",
    "replay_throughput_entries_s",
    "truncate_purge_p99_ns",
    "reopen_ms",
    // The two stages of a Consistent commit, isolated (see the
    // write_only/fsync_only block in `run`).
    "write_only_p50_ns",
    "write_only_p99_ns",
    "fsync_only_p50_ns",
    "fsync_only_p99_ns",
    // fsync_only on a pre-written (zero-filled) full segment, so the per-commit
    // barrier carries no size-extension metadata commit (see the prealloc block).
    "fsync_prealloc_p50_ns",
    "fsync_prealloc_p99_ns",
];

pub struct Config {
    pub group_rounds: usize,
    pub consistent_samples: usize,
    pub eventual_samples: usize,
    pub fill_entries: u64,
    pub trunc_samples: usize,
    pub allow_tmpfs: bool,
}

impl Config {
    pub fn standard() -> Self {
        Self {
            group_rounds: 50,
            consistent_samples: 400,
            eventual_samples: 400,
            fill_entries: 100_000,
            trunc_samples: 100,
            allow_tmpfs: false,
        }
    }

    pub fn quick() -> Self {
        Self {
            group_rounds: 4,
            consistent_samples: 20,
            eventual_samples: 20,
            fill_entries: 2_000,
            trunc_samples: 8,
            allow_tmpfs: true,
        }
    }

    /// `standard()` unless AUTOBENCH_QUICK=1.
    pub fn from_env() -> Self {
        if std::env::var("AUTOBENCH_QUICK").as_deref() == Ok("1") {
            Self::quick()
        } else {
            Self::standard()
        }
    }
}

const BURST: u64 = 256;
const PAYLOAD: usize = 1024;

fn fresh(root: &std::path::Path, durability: Durability) -> (tempfile::TempDir, Journal) {
    fresh_cfg(root, durability, false)
}

/// `fresh`, but optionally opens the journal with `preallocate_segments: true`
/// so the writer appends into zero-filled extents (no per-commit `i_size`
/// metadata commit). Used to quantify the end-to-end preallocation win.
fn fresh_prealloc(root: &std::path::Path, durability: Durability) -> (tempfile::TempDir, Journal) {
    fresh_cfg(root, durability, true)
}

fn fresh_cfg(
    root: &std::path::Path,
    durability: Durability,
    preallocate: bool,
) -> (tempfile::TempDir, Journal) {
    // Callers bind `let (_d, j) = fresh(..)`; pattern bindings drop in reverse
    // declaration order, so `j` (the Journal, joining its writer thread) drops
    // before `_d` removes the directory.
    let dir = tempfile::Builder::new().prefix("jb").tempdir_in(root).unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.durability = durability;
    cfg.preallocate_segments = preallocate;
    let j = Journal::open(cfg).unwrap();
    (dir, j)
}

/// 256-entry bursts, waiting only the last notifier per round (the fsync
/// barrier makes the last seq's batch fsync cover every earlier seq). Returns
/// entries/sec. Shared by the baseline and preallocated group-commit metrics.
fn measure_group_commit(j: &Journal, group_rounds: usize, payload: &[u8]) -> f64 {
    let mut seq = 0u64;
    let t = Instant::now();
    for _ in 0..group_rounds {
        let mut last = None;
        for _ in 0..BURST {
            seq += 1;
            last = Some(j.append(seq, 0, payload).unwrap());
        }
        last.unwrap().wait().unwrap();
    }
    let total = (group_rounds as u64 * BURST) as f64;
    total / t.elapsed().as_secs_f64()
}

pub fn run(cfg: &Config) -> BTreeMap<String, f64> {
    let root = diskcheck::bench_root("autobench-journal");
    diskcheck::assert_real_disk(&root, cfg.allow_tmpfs);
    let mut m = BTreeMap::new();
    let payload = vec![0xABu8; PAYLOAD];

    // -- group_commit_throughput: 256-entry bursts, wait the last notifier.
    // Consistent: last notifier resolves after its batch's fsync; the writer
    // coalesces the queued burst into group-commit batches, so this times the
    // fsync pipeline, not page-cache drain. (FIFO channel + fsync-as-barrier
    // means the last seq's batch fsync covers every earlier seq, so waiting
    // only the last notifier implies the whole burst is durable.)
    {
        let (_d, j) = fresh(&root, Durability::Consistent);
        m.insert(
            "group_commit_throughput".into(),
            measure_group_commit(&j, cfg.group_rounds, &payload),
        );
    }

    // -- group_commit_throughput_prealloc: identical burst loop, but the
    // journal is opened with `preallocate_segments: true`, so the writer
    // appends into zero-filled extents and every group-commit `sync_data`
    // skips the `i_size`/extent-map metadata commit a size-extending append
    // forces (the same effect the fsync_prealloc microbench isolates, now
    // through the full append → group-commit → notifier pipeline). The
    // prealloc vs baseline ratio is the end-to-end throughput win.
    {
        let (_d, j) = fresh_prealloc(&root, Durability::Consistent);
        m.insert(
            "group_commit_throughput_prealloc".into(),
            measure_group_commit(&j, cfg.group_rounds, &payload),
        );
    }

    // -- append_consistent_p99_ns at 64 B / 1 KiB / 4 KiB
    for (key, size) in [
        ("append_consistent_p99_ns_64b", 64usize),
        ("append_consistent_p99_ns", 1024),
        ("append_consistent_p99_ns_4k", 4096),
    ] {
        let (_d, j) = fresh(&root, Durability::Consistent);
        let p = vec![0xABu8; size];
        let mut seq = 0u64;
        let mut s = batched_samples_ns(cfg.consistent_samples, 1, || {
            seq += 1;
            j.append(seq, 0, &p).unwrap().wait().unwrap();
        });
        m.insert(key.into(), percentile(&mut s, 99.0));
    }

    // -- write_only / fsync_only: the two stages of a Consistent commit,
    // isolated. `append_consistent_p99_ns` above times them coupled (write +
    // fsync + channel + notifier); here we drive the real segment primitives
    // on the calling thread so each cost stands alone:
    //   write_only_*  = encode + one `write_all` into the page cache
    //                   (`SegmentFile::append_records`) — "time to write a
    //                   log entry", no fsync.
    //   fsync_only_*  = the `sync_data` durability barrier over the bytes
    //                   written so far — "time to fsync", the same commit
    //                   primitive the writer issues per group-commit.
    // No channel / notifier / group-commit batching is in either path, so
    // these isolate the syscall costs the writer pipeline is built around,
    // not its scheduling. 1 KiB payload matches `append_consistent_p99_ns`.
    {
        use ultima_journal::bench_support::BenchSegment;
        let dir = tempfile::Builder::new().prefix("jb-stage").tempdir_in(&root).unwrap();
        let mut seg = BenchSegment::create(&dir.path().join("seg.log"), 1).unwrap();
        let mut seq = 0u64;

        // write_only: one record append (no fsync) per sample.
        let mut w = batched_samples_ns(cfg.consistent_samples, 1, || {
            seq += 1;
            seg.append_one(seq, 0, &payload).unwrap();
        });
        m.insert("write_only_p50_ns".into(), percentile(&mut w, 50.0));
        m.insert("write_only_p99_ns".into(), percentile(&mut w, 99.0));

        // fsync_only: flush the backlog the write_only loop left dirty, then
        // measure one `sync_data` per freshly-appended record — so each sample
        // is the barrier cost over a single 1 KiB entry, the real per-commit
        // fsync, not a one-off flush of the whole accumulated tail.
        seg.sync_data().unwrap();
        let mut f = Vec::with_capacity(cfg.consistent_samples);
        for _ in 0..cfg.consistent_samples {
            seq += 1;
            seg.append_one(seq, 0, &payload).unwrap();
            let t = Instant::now();
            seg.sync_data().unwrap();
            f.push(t.elapsed().as_nanos() as f64);
        }
        m.insert("fsync_only_p50_ns".into(), percentile(&mut f, 50.0));
        m.insert("fsync_only_p99_ns".into(), percentile(&mut f, 99.0));
    }

    // -- fsync_prealloc: identical loop to fsync_only, but the segment is
    // zero-filled + fsync'd to the full 64 MiB default segment size up front,
    // so every `append_one` overwrites already-written extents instead of
    // moving EOF. The per-commit `sync_data` therefore flushes no `i_size` /
    // extent-map change — on ext4 (data=ordered) that drops the jbd2 journal
    // transaction commit a size-extending append otherwise forces on top of the
    // device flush. The fsync_prealloc vs fsync_only delta is exactly that
    // metadata-commit cost; if it's ~zero, the barrier is the raw device/virt
    // flush and preallocation buys nothing here. This is the etcd WAL trick.
    {
        use ultima_journal::bench_support::BenchSegment;
        let dir = tempfile::Builder::new().prefix("jb-prealloc").tempdir_in(&root).unwrap();
        let mut seg = BenchSegment::create(&dir.path().join("seg.log"), 1).unwrap();
        seg.preallocate(64 * 1024 * 1024).unwrap();
        let mut seq = 0u64;
        // Prime one dirty page + flush so the first measured sample is steady-state.
        seq += 1;
        seg.append_one(seq, 0, &payload).unwrap();
        seg.sync_data().unwrap();
        let mut f = Vec::with_capacity(cfg.consistent_samples);
        for _ in 0..cfg.consistent_samples {
            seq += 1;
            seg.append_one(seq, 0, &payload).unwrap();
            let t = Instant::now();
            seg.sync_data().unwrap();
            f.push(t.elapsed().as_nanos() as f64);
        }
        m.insert("fsync_prealloc_p50_ns".into(), percentile(&mut f, 50.0));
        m.insert("fsync_prealloc_p99_ns".into(), percentile(&mut f, 99.0));
    }

    // -- append_eventual_ack_p99_ns (notifier resolution on the write path)
    {
        let (_d, j) = fresh(&root, Durability::Eventual);
        let mut seq = 0u64;
        let mut s = batched_samples_ns(cfg.eventual_samples, 1, || {
            seq += 1;
            j.append(seq, 0, &payload).unwrap().wait().unwrap();
        });
        m.insert("append_eventual_ack_p99_ns".into(), percentile(&mut s, 99.0));
    }

    // -- fill once; measure eventual throughput, replay, reopen on it
    {
        let dir = tempfile::Builder::new().prefix("jb").tempdir_in(&root).unwrap();
        let mut jcfg = JournalConfig::new(dir.path());
        jcfg.durability = Durability::Eventual;
        let j = Journal::open(jcfg).unwrap();
        let n = cfg.fill_entries;
        let t = Instant::now();
        for seq in 1..=n {
            j.append(seq, 0, &payload).unwrap();
        }
        j.wait_durable(n).unwrap();
        m.insert("append_eventual_throughput".into(), n as f64 / t.elapsed().as_secs_f64());

        // iter_range is eager (collects into a Vec internally), but the
        // iterator shape is the real public API and the loop only needs the
        // count, so we use it directly. Timing includes eager materialization
        // of all payloads (~100 MiB at standard config) — it tracks the current
        // public replay API cost, not a streaming recovery.
        let t = Instant::now();
        let mut count = 0u64;
        for rec in j.iter_range(1..=n).unwrap() {
            let _ = rec.unwrap();
            count += 1;
        }
        assert_eq!(count, n, "replay lost records");
        m.insert("replay_throughput_entries_s".into(), n as f64 / t.elapsed().as_secs_f64());

        // Warm-cache reopen: segments were just written and reside in page
        // cache; this tracks open-path CPU/scan cost, not cold-start I/O.
        j.close().unwrap();
        let mut opens = Vec::new();
        for _ in 0..9 {
            let mut jcfg = JournalConfig::new(dir.path());
            jcfg.durability = Durability::Eventual;
            let t = Instant::now();
            let j = Journal::open(jcfg).unwrap();
            opens.push(t.elapsed().as_secs_f64() * 1e3);
            j.close().unwrap();
        }
        m.insert("reopen_ms".into(), percentile(&mut opens, 50.0));
    }

    // -- truncate_purge_p99_ns: rolling window, truncate tail + purge head.
    // NOTE: purge_before is SEGMENT-granular (drops only whole non-active
    // segments whose last seq <= arg). With 1 KiB payloads and the default
    // 64 MiB segments, every purge call here is a cheap no-op — but that's
    // fine: this metric measures the truncate + fsync + purge *call* cost,
    // which is exactly what the cluster pays on each log-compaction step.
    {
        let (_d, j) = fresh(&root, Durability::Consistent);
        let mut hi = 0u64;
        for _ in 0..128 {
            hi += 1;
            j.append(hi, 0, &payload).unwrap().wait().unwrap();
        }
        let mut lo = 0u64; // purged through
        let mut s = Vec::with_capacity(cfg.trunc_samples);
        for _ in 0..cfg.trunc_samples {
            for _ in 0..64 {
                hi += 1;
                j.append(hi, 0, &payload).unwrap().wait().unwrap();
            }
            let t = Instant::now();
            j.truncate_after(hi - 16).unwrap().wait().unwrap();
            hi -= 16;
            lo += 32;
            j.purge_before(lo).unwrap();
            s.push(t.elapsed().as_nanos() as f64);
        }
        m.insert("truncate_purge_p99_ns".into(), percentile(&mut s, 99.0));
    }

    m
}
