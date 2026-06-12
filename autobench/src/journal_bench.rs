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
    "append_consistent_p99_ns",
    "append_consistent_p99_ns_64b",
    "append_consistent_p99_ns_4k",
    "append_eventual_ack_p99_ns",
    "append_eventual_throughput",
    "replay_throughput_entries_s",
    "truncate_purge_p99_ns",
    "reopen_ms",
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
    // Callers bind `let (_d, j) = fresh(..)`; pattern bindings drop in reverse
    // declaration order, so `j` (the Journal, joining its writer thread) drops
    // before `_d` removes the directory.
    let dir = tempfile::Builder::new().prefix("jb").tempdir_in(root).unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.durability = durability;
    let j = Journal::open(cfg).unwrap();
    (dir, j)
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
        let mut seq = 0u64;
        let t = Instant::now();
        for _ in 0..cfg.group_rounds {
            let mut last = None;
            for _ in 0..BURST {
                seq += 1;
                last = Some(j.append(seq, 0, &payload).unwrap());
            }
            last.unwrap().wait().unwrap();
        }
        let total = (cfg.group_rounds as u64 * BURST) as f64;
        m.insert("group_commit_throughput".into(), total / t.elapsed().as_secs_f64());
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
