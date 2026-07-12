// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Single-writer (serial) commit benchmark with persistence.
//!
//! The serial counterpart to `multiwriter_persistence_bench`: one writer
//! commits TOTAL_COMMITS transactions back-to-back (`WriterMode::SingleWriter`,
//! no OCC, no thread overlap). Each transaction updates a single key. With no
//! concurrency there is nothing to overlap fsyncs against, so the Standalone
//! tiers pay their per-commit I/O cost in full.
//!
//! Configurations: inmemory, smr, standalone_consistent, standalone_consistent_inline,
//! standalone_consistent_inline_prealloc, standalone_consistent_coalesced_prealloc, standalone_eventual.
//!
//! `smr` (`Persistence::Smr`) is checkpoint-only: `commit()` does no per-commit
//! disk I/O (durability is delegated to an external consensus log), so its
//! write path should track the in-memory baseline and run far ahead of the
//! WAL-backed Standalone tiers.
//!
//! Measured (2026-06-18, Claude sandbox — noisy/virtualized, re-run on the real
//! bench host for authoritative numbers; 1 writer × 200 sequential commits):
//!
//! | config                                    | throughput     | vs smr           |
//! |-------------------------------------------|----------------|------------------|
//! | `inmemory`                                | ~1.11 Melem/s  | ~equal           |
//! | `smr`                                     | ~1.11 Melem/s  | baseline         |
//! | `standalone_eventual`                     | ~0.65 Melem/s  | smr ~1.7× faster |
//! | `standalone_consistent`                   | ~25.5 Kelem/s  | smr ~43× faster  |
//! | `standalone_consistent_inline`            | real-disk A/B  | NVMe (c6id, PerEntry): 130.8 µs/commit vs async 153.5 (1.17×) |
//! | `standalone_consistent_inline_prealloc`   | real-disk A/B  | NVMe (c6id): 40.6 µs/commit — 1.51× vs async-prealloc, 3.78× vs old default |
//! | `standalone_consistent_coalesced_prealloc`| real-disk A/B  | NVMe (c6id): 61.2 µs/commit (2.5× vs async-PerEntry). Sandbox noise ±2×. |
//!
//! Takeaway: serially, SMR is again indistinguishable from in-memory, and the
//! Consistent gap blows out to ~40×+ (vs ~6× under the multi-writer bench)
//! because a serial writer has no concurrency for three-phase commit to overlap
//! fsyncs against — each commit eats a full fsync.
//!
//! Do NOT compare absolute throughput across this bench and the multi-writer
//! one (serial inmemory ~1.1 Melem/s vs mw inmemory ~206 Kelem/s — ~5×). Both
//! run the identical update-one-key + commit operation; the gap is writer mode
//! and contention, not measurement artifact. The mw bench now uses a persistent
//! worker pool (no per-iteration thread spawn), and that change recovered only
//! ~16% — confirming spawn was a minor factor. Decomposition (in-memory,
//! 200 commits/iter): SingleWriter inline ~900 ns/commit (this bench),
//! MultiWriter inline ~1880 ns/commit (~2×, from the OCC commit path — intent
//! claim, commit lock, per-key merge, FIFO promote gate), and with 4 concurrent
//! writers the per-commit cost rises further from cross-core cacheline/lock
//! contention on shared commit state. So the gap is MultiWriter's OCC tax plus
//! contention. Compare SMR-vs-non-SMR ratios *within* each bench, where writer
//! mode and threading are held constant.

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ultima_db::{Persistence, Store, StoreConfig, WalWrite, WriterMode};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Record {
    value: u64,
}

const TOTAL_COMMITS: usize = 200;
const PRELOAD_ROWS: u64 = 1000;

fn make_store(persistence: Persistence, tmpdir: Option<&std::path::Path>) -> Store {
    let mut config = StoreConfig::builder()
        .num_snapshots_retained(2)
        .writer_mode(WriterMode::SingleWriter)
        .persistence(Persistence::None)
        .build();
    if let Some(dir) = tmpdir {
        config.persistence = match persistence {
            Persistence::Standalone {
                durability,
                wal_write,
                ..
            } => Persistence::standalone(dir.to_path_buf(), durability, wal_write),
            Persistence::Smr { .. } => Persistence::smr(dir.to_path_buf()),
            other => other,
        };
    } else {
        config.persistence = persistence;
    }

    let durable = !matches!(config.persistence, Persistence::None);
    let store = Store::new(config).unwrap();
    store.register_table::<Record>("data").unwrap();
    if durable {
        store.recover().unwrap();
    }
    // Preload so updates don't collide on auto-increment IDs.
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<Record>("data").unwrap();
        for i in 0..PRELOAD_ROWS {
            table.insert(Record { value: i }).unwrap();
        }
    }
    wtx.commit().unwrap();
    store
}

/// One writer, TOTAL_COMMITS sequential single-key updates.
fn run_serial_commits(store: &Store) {
    for i in 0..TOTAL_COMMITS {
        let key = (i as u64 % PRELOAD_ROWS) + 1;
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<Record>("data")
            .unwrap()
            .update(key, Record { value: key * 100 })
            .unwrap();
        wtx.commit().unwrap();
    }
}

fn bench_singlewriter_persistent(c: &mut Criterion) {
    let mut group = c.benchmark_group("sw_persistent");
    group.throughput(Throughput::Elements(TOTAL_COMMITS as u64));

    // In-memory baseline
    {
        let store = make_store(Persistence::None, None);
        group.bench_function("inmemory", |b| {
            b.iter(|| {
                run_serial_commits(&store);
                black_box(());
            });
        });
    }

    // SMR (checkpoint-only; no per-commit WAL)
    {
        let tmpdir = tempfile::tempdir_in(ultima_bench_workloads::ycsb::bench_disk_dir()).unwrap();
        let store = make_store(
            Persistence::smr(std::path::PathBuf::new()),
            Some(tmpdir.path()),
        );
        group.bench_function("smr", |b| {
            b.iter(|| {
                run_serial_commits(&store);
                black_box(());
            });
        });
    }

    // Standalone Consistent
    {
        let tmpdir = tempfile::tempdir_in(ultima_bench_workloads::ycsb::bench_disk_dir()).unwrap();
        let store = make_store(
            Persistence::standalone(
                std::path::PathBuf::new(),
                ultima_db::Durability::Consistent,
                WalWrite::PerEntry,
            ),
            Some(tmpdir.path()),
        );
        group.bench_function("standalone_consistent", |b| {
            b.iter(|| {
                run_serial_commits(&store);
                black_box(());
            });
        });
    }

    // Standalone Consistent + prealloc (A/B pair with standalone_consistent above)
    //
    // Identical durability/workload; only wal_write differs.  On real disk,
    // CoalescedPrealloc should convert per-fsync metadata-journal cost into a
    // pure data barrier (sync_data) for steady-state batches.
    //
    // NOTE: the real-disk delta must be measured on the bench host with the
    // worktree+shared-CARGO_TARGET_DIR A/B protocol (sandbox noise floors reach
    // ±2×).  Until that run is complete the feature stays opt-in/off.
    {
        let tmpdir = tempfile::tempdir_in(ultima_bench_workloads::ycsb::bench_disk_dir()).unwrap();
        let store = make_store(
            Persistence::standalone(
                std::path::PathBuf::new(),
                ultima_db::Durability::Consistent,
                WalWrite::CoalescedPrealloc,
            ),
            Some(tmpdir.path()),
        );
        group.bench_function("standalone_consistent_coalesced_prealloc", |b| {
            b.iter(|| {
                run_serial_commits(&store);
                black_box(());
            });
        });
    }

    // Standalone Consistent Inline (off-lock inline fsync, no bg-thread handoff)
    //
    // A/B pair with `standalone_consistent` above: identical durability guarantee,
    // identical workload; only the fsync mechanism differs.  The committing thread
    // performs the fsync itself (off-lock), eliminating the ~20–35 µs cross-thread
    // handoff that otherwise dominates serial durable-commit cost on fast disk.
    //
    // Measured on tmpfs (fsync ≈ 0): handoff ≈ 32 µs/commit removed → ~3 µs/commit.
    // Projected NVMe: ~72 µs → ~38 µs (~1.9×).  Real bench-host A/B still pending
    // (NVMe fleet was torn down).  See docs/tasks/task38_wal_inline_fsync.md.
    {
        let tmpdir = tempfile::tempdir_in(ultima_bench_workloads::ycsb::bench_disk_dir()).unwrap();
        let store = make_store(
            Persistence::standalone(
                std::path::PathBuf::new(),
                ultima_db::Durability::ConsistentInline,
                WalWrite::PerEntry,
            ),
            Some(tmpdir.path()),
        );
        group.bench_function("standalone_consistent_inline", |b| {
            b.iter(|| {
                run_serial_commits(&store);
                black_box(());
            });
        });
    }

    // Inline-fsync + preallocation (both optimizations stacked) — the fastest
    // durable config. Real NVMe (AWS c6id, 2026-06-21): 40.6 µs/commit vs
    // async-prealloc 61.2 µs (1.51×) and async-PerEntry 153.5 µs (3.78×).
    // See docs/tasks/task38_wal_inline_fsync.md §6.
    {
        let tmpdir = tempfile::tempdir_in(ultima_bench_workloads::ycsb::bench_disk_dir()).unwrap();
        let store = make_store(
            Persistence::standalone(
                std::path::PathBuf::new(),
                ultima_db::Durability::ConsistentInline,
                WalWrite::CoalescedPrealloc,
            ),
            Some(tmpdir.path()),
        );
        group.bench_function("standalone_consistent_inline_prealloc", |b| {
            b.iter(|| {
                run_serial_commits(&store);
                black_box(());
            });
        });
    }

    // Standalone Eventual
    {
        let tmpdir = tempfile::tempdir_in(ultima_bench_workloads::ycsb::bench_disk_dir()).unwrap();
        let store = make_store(
            Persistence::standalone(
                std::path::PathBuf::new(),
                ultima_db::Durability::Eventual,
                WalWrite::PerEntry,
            ),
            Some(tmpdir.path()),
        );
        group.bench_function("standalone_eventual", |b| {
            b.iter(|| {
                run_serial_commits(&store);
                black_box(());
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = sw_persistent;
    config = Criterion::default()
        .sample_size(30)
        .measurement_time(Duration::from_secs(15));
    targets = bench_singlewriter_persistent
}
criterion_main!(sw_persistent);
