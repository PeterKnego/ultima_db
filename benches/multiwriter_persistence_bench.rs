// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Multi-threaded writer benchmark with persistence.
//!
//! Measures the throughput benefit of three-phase commit: releasing the store
//! lock during WAL fsync so concurrent writers can overlap their I/O.
//!
//! N writer threads are spawned ONCE per configuration (a persistent worker
//! pool, as a real deployment would run them). The timed section only releases
//! the pool to commit a batch (COMMITS_PER_THREAD each) and waits for it to
//! finish — two barrier hand-offs — so the measurement reflects steady-state
//! commit throughput, not per-iteration `thread::spawn`/`join` cost. Throughput
//! is measured as commits/second.
//!
//! Configurations: inmemory, smr, standalone_consistent, standalone_eventual.
//!
//! `smr` (`Persistence::Smr`) is checkpoint-only: `commit()` does no per-commit
//! disk I/O (durability is delegated to an external consensus log), so its
//! write path should track the in-memory baseline and run far ahead of the
//! WAL-backed Standalone tiers.
//!
//! Measured (2026-06-18, Claude sandbox — noisy/virtualized, re-run on the real
//! bench host for authoritative numbers; 4 persistent writers × 50 commits):
//!
//! | config                  | throughput     | vs smr        |
//! |-------------------------|----------------|---------------|
//! | `inmemory`              | ~206 Kelem/s   | ~equal        |
//! | `smr`                   | ~207 Kelem/s   | baseline      |
//! | `standalone_eventual`   | ~135 Kelem/s   | smr ~1.5× faster |
//! | `standalone_consistent` | ~31 Kelem/s    | smr ~6.7× faster |
//!
//! Takeaway: SMR multi-writer throughput is indistinguishable from pure
//! in-memory — `commit()` never touches disk — while the WAL-backed tiers pay
//! for per-commit writes (Eventual: async fsync) and fsync stalls (Consistent).

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::JoinHandle;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ultima_db::{Persistence, Store, StoreConfig, WalWrite, WriterMode};

// ---------------------------------------------------------------------------
// Record type
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Record {
    value: u64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const NUM_THREADS: usize = 4;
const COMMITS_PER_THREAD: usize = 50;
const TOTAL_COMMITS: usize = NUM_THREADS * COMMITS_PER_THREAD;

fn make_store(persistence: Persistence, tmpdir: Option<&std::path::Path>) -> Store {
    let mut config = StoreConfig {
        num_snapshots_retained: 2,
        writer_mode: WriterMode::MultiWriter,
        persistence: Persistence::None,
        ..StoreConfig::default()
    };
    if let Some(dir) = tmpdir {
        config.persistence = match persistence {
            Persistence::Standalone { durability, .. } => Persistence::Standalone {
                dir: dir.to_path_buf(),
                durability,
                wal_write: WalWrite::PerEntry,
            },
            Persistence::Smr { .. } => Persistence::Smr {
                dir: dir.to_path_buf(),
            },
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
    // Each thread gets its own key range: thread i uses keys (i*1000+1)..=(i*1000+1000).
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<Record>("data").unwrap();
        for i in 0..(NUM_THREADS * 1000) as u64 {
            table.insert(Record { value: i }).unwrap();
        }
    }
    wtx.commit().unwrap();
    store
}

/// One transaction: update a single preloaded key, retrying on the (rare,
/// disjoint-key) conflict by blocking on the holder's waiter then rebasing.
fn commit_one(store: &Store, key: u64) {
    loop {
        let mut wtx = store.begin_write(None).unwrap();
        let res = wtx
            .open_table::<Record>("data")
            .unwrap()
            .update(key, Record { value: key * 100 });
        if let Err(ultima_db::Error::WriteConflict { wait_for, .. }) = res {
            drop(wtx);
            if let Some(w) = wait_for {
                w.wait();
            }
            continue;
        }
        res.unwrap();
        match wtx.commit() {
            Ok(_) => break,
            Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                if let Some(w) = wait_for {
                    w.wait();
                }
                continue;
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
}

/// A persistent pool of NUM_THREADS writer threads spawned once. `run_batch`
/// releases all workers to commit COMMITS_PER_THREAD transactions each (on
/// disjoint key ranges) and blocks until they finish, via a start/end barrier
/// pair that both the workers and the caller participate in. This keeps the
/// per-iteration cost to the commits plus two barrier hand-offs — no thread
/// creation in the timed path.
struct WorkerPool {
    start: Arc<Barrier>,
    end: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    fn new(store: &Store) -> Self {
        let start = Arc::new(Barrier::new(NUM_THREADS + 1));
        let end = Arc::new(Barrier::new(NUM_THREADS + 1));
        let stop = Arc::new(AtomicBool::new(false));

        let handles = (0..NUM_THREADS)
            .map(|t| {
                let store = store.clone();
                let start = Arc::clone(&start);
                let end = Arc::clone(&end);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    loop {
                        // Released by the caller's `run_batch` (or shutdown).
                        start.wait();
                        if stop.load(Ordering::Acquire) {
                            break;
                        }
                        for i in 0..COMMITS_PER_THREAD {
                            commit_one(&store, (t * 1000 + i + 1) as u64);
                        }
                        // Signal the caller that this batch is complete.
                        end.wait();
                    }
                })
            })
            .collect();

        WorkerPool {
            start,
            end,
            stop,
            handles,
        }
    }

    /// Release the pool for one batch and wait for completion (timed section).
    #[inline]
    fn run_batch(&self) {
        self.start.wait();
        self.end.wait();
    }

    fn shutdown(self) {
        self.stop.store(true, Ordering::Release);
        // Final release: workers observe `stop` and break (no matching `end`).
        self.start.wait();
        for h in self.handles {
            h.join().unwrap();
        }
    }
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_multiwriter_persistent(c: &mut Criterion) {
    let mut group = c.benchmark_group("mw_persistent");
    group.throughput(Throughput::Elements(TOTAL_COMMITS as u64));

    // In-memory baseline
    {
        let store = make_store(Persistence::None, None);
        let pool = WorkerPool::new(&store);
        group.bench_function("inmemory", |b| {
            b.iter(|| {
                pool.run_batch();
                black_box(());
            });
        });
        pool.shutdown();
    }

    // SMR (checkpoint-only; no per-commit WAL)
    {
        let tmpdir = tempfile::tempdir().unwrap();
        let store = make_store(
            Persistence::Smr {
                dir: std::path::PathBuf::new(),
            },
            Some(tmpdir.path()),
        );
        let pool = WorkerPool::new(&store);
        group.bench_function("smr", |b| {
            b.iter(|| {
                pool.run_batch();
                black_box(());
            });
        });
        pool.shutdown();
    }

    // Standalone Consistent
    {
        let tmpdir = tempfile::tempdir().unwrap();
        let store = make_store(
            Persistence::Standalone {
                dir: std::path::PathBuf::new(),
                durability: ultima_db::Durability::Consistent,
                wal_write: WalWrite::PerEntry,
            },
            Some(tmpdir.path()),
        );
        let pool = WorkerPool::new(&store);
        group.bench_function("standalone_consistent", |b| {
            b.iter(|| {
                pool.run_batch();
                black_box(());
            });
        });
        pool.shutdown();
    }

    // Standalone Eventual
    {
        let tmpdir = tempfile::tempdir().unwrap();
        let store = make_store(
            Persistence::Standalone {
                dir: std::path::PathBuf::new(),
                durability: ultima_db::Durability::Eventual,
                wal_write: WalWrite::PerEntry,
            },
            Some(tmpdir.path()),
        );
        let pool = WorkerPool::new(&store);
        group.bench_function("standalone_eventual", |b| {
            b.iter(|| {
                pool.run_batch();
                black_box(());
            });
        });
        pool.shutdown();
    }

    group.finish();
}

criterion_group! {
    name = mw_persistent;
    config = Criterion::default()
        .sample_size(30)
        .measurement_time(Duration::from_secs(15));
    targets = bench_multiwriter_persistent
}
criterion_main!(mw_persistent);
