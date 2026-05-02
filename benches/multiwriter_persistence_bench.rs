// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Multi-threaded writer benchmark with persistence.
//!
//! Measures the throughput benefit of three-phase commit: releasing the store
//! lock during WAL fsync so concurrent writers can overlap their I/O.
//!
//! Each iteration spawns N threads that each execute a write transaction
//! (insert a record, commit). Throughput is measured as commits/second.
//!
//! Configurations: inmemory, standalone_consistent, standalone_eventual.

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ultima_db::{Persistence, Store, StoreConfig, WriterMode};

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
            },
            other => other,
        };
    } else {
        config.persistence = persistence;
    }

    let store = Store::new(config).unwrap();
    store.register_table::<Record>("data").unwrap();
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

/// Run N threads, each committing COMMITS_PER_THREAD transactions.
/// Each transaction updates a single key in the thread's private range.
fn run_threaded_commits(store: &Store) {
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|t| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..COMMITS_PER_THREAD {
                    let key = (t * 1000 + i + 1) as u64;
                    let mut wtx = store.begin_write(None).unwrap();
                    wtx.open_table::<Record>("data")
                        .unwrap()
                        .update(key, Record { value: key * 100 })
                        .unwrap();
                    match wtx.commit() {
                        Ok(_) => {}
                        Err(ultima_db::Error::WriteConflict { .. }) => {
                            // Retry once on conflict (rare with disjoint keys).
                            let mut wtx = store.begin_write(None).unwrap();
                            wtx.open_table::<Record>("data")
                                .unwrap()
                                .update(key, Record { value: key * 100 })
                                .unwrap();
                            wtx.commit().unwrap();
                        }
                        Err(e) => panic!("unexpected error: {e}"),
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
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
        group.bench_function("inmemory", |b| {
            b.iter(|| {
                black_box(run_threaded_commits(&store));
            });
        });
    }

    // Standalone Consistent
    {
        let tmpdir = tempfile::tempdir().unwrap();
        let store = make_store(
            Persistence::Standalone {
                dir: std::path::PathBuf::new(),
                durability: ultima_db::Durability::Consistent,
            },
            Some(tmpdir.path()),
        );
        group.bench_function("standalone_consistent", |b| {
            b.iter(|| {
                black_box(run_threaded_commits(&store));
            });
        });
    }

    // Standalone Eventual
    {
        let tmpdir = tempfile::tempdir().unwrap();
        let store = make_store(
            Persistence::Standalone {
                dir: std::path::PathBuf::new(),
                durability: ultima_db::Durability::Eventual,
            },
            Some(tmpdir.path()),
        );
        group.bench_function("standalone_eventual", |b| {
            b.iter(|| {
                black_box(run_threaded_commits(&store));
            });
        });
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
