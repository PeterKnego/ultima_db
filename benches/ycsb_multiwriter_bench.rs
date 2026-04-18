#![allow(clippy::redundant_iter_cloned)]

//! YCSB-style benchmarks for multi-writer OCC, using real OS threads.
//!
//! `Store: Send + Sync`, so each thread holds a clone and opens its own
//! `WriteTx` locally. `WriteTx` itself is `!Send` and never crosses a thread.
//! Includes Ultima-specific scenarios (baseline, no-contention) plus the
//! shared multi-writer suite via `MultiWriterEngine`.

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::SeedableRng;
use ultima_db::{Store, StoreConfig, WriterMode};

#[path = "ycsb_common.rs"]
mod ycsb_common;
use ycsb_common::{
    bench_multiwriter_workloads, ycsb_criterion, BurstResult, MultiWriterEngine, YcsbRecord,
    ZipfianGenerator, MW_OPS_PER_WRITER, MW_WRITERS, NUM_RECORDS,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_store(mode: WriterMode) -> Store {
    let store = Store::new(StoreConfig {
        num_snapshots_retained: 2,
        auto_snapshot_gc: true,
        writer_mode: mode,
        ..StoreConfig::default()
    }).unwrap();
    // Preload table with NUM_RECORDS rows
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
        for i in 1..=NUM_RECORDS {
            table.insert(YcsbRecord::new(i)).unwrap();
        }
    }
    wtx.commit().unwrap();
    store
}

// ---------------------------------------------------------------------------
// MultiWriterEngine implementation for Ultima
// ---------------------------------------------------------------------------

struct UltimaMultiWriterEngine {
    store: Store,
}

impl UltimaMultiWriterEngine {
    fn new() -> Self {
        Self {
            store: make_store(WriterMode::MultiWriter),
        }
    }
}

impl MultiWriterEngine for UltimaMultiWriterEngine {
    fn name(&self) -> &str {
        "ultima"
    }

    fn execute_burst(&mut self, key_sets: &[Vec<u64>]) -> BurstResult {
        let barrier = Arc::new(Barrier::new(key_sets.len()));
        let handles: Vec<_> = key_sets
            .iter()
            .cloned()
            .map(|keys| {
                let store = self.store.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut conflicts = 0u64;
                    loop {
                        let mut wtx = store.begin_write(None).unwrap();
                        {
                            let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                            for &key in &keys {
                                let _ = table.update(key, YcsbRecord::new(key.wrapping_add(1)));
                            }
                        }
                        match wtx.commit() {
                            Ok(_) => return (1u64, conflicts),
                            Err(ultima_db::Error::WriteConflict { .. }) => {
                                conflicts += 1;
                                continue;
                            }
                            Err(e) => panic!("unexpected error: {e}"),
                        }
                    }
                })
            })
            .collect();

        let mut committed = 0u64;
        let mut conflicts = 0u64;
        for h in handles {
            let (c, x) = h.join().unwrap();
            committed += c;
            conflicts += x;
        }
        BurstResult { committed, conflicts }
    }

    fn verify_key(&self, key: u64) -> bool {
        let rtx = self.store.begin_read(None).unwrap();
        let table = rtx.open_table::<YcsbRecord>("ycsb").unwrap();
        table.get(key).is_some()
    }
}

// ---------------------------------------------------------------------------
// Ultima-specific benchmarks (not in the shared suite)
// ---------------------------------------------------------------------------

/// Baseline: SingleWriter mode, sequential commits (no OCC overhead)
fn bench_baseline_single_writer(c: &mut Criterion) {
    let store = make_store(WriterMode::SingleWriter);
    let zipf = ZipfianGenerator::new(NUM_RECORDS, 0.99);
    let mut rng = StdRng::seed_from_u64(100);

    let total_ops = (MW_WRITERS * MW_OPS_PER_WRITER) as u64;
    let mut group = c.benchmark_group("multiwriter");
    group.throughput(Throughput::Elements(total_ops));

    group.bench_function("baseline_single_writer", |b| {
        b.iter(|| {
            for _ in 0..MW_WRITERS {
                let mut wtx = store.begin_write(None).unwrap();
                {
                    let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                    for _ in 0..MW_OPS_PER_WRITER {
                        let key = zipf.next(&mut rng);
                        let _ = table.update(key, YcsbRecord::new(key.wrapping_add(1)));
                    }
                }
                wtx.commit().unwrap();
            }
            black_box(());
        });
    });
    group.finish();
}

/// No contention: MultiWriter, each writer operates on a different table
fn bench_no_contention(c: &mut Criterion) {
    let store = Store::new(StoreConfig {
        num_snapshots_retained: 2,
        auto_snapshot_gc: true,
        writer_mode: WriterMode::MultiWriter,
        ..StoreConfig::default()
    }).unwrap();
    // Preload each writer's table
    for w in 0..MW_WRITERS {
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut table = wtx
                .open_table::<YcsbRecord>(format!("ycsb_{w}").as_str())
                .unwrap();
            for i in 1..=NUM_RECORDS {
                table.insert(YcsbRecord::new(i)).unwrap();
            }
        }
        wtx.commit().unwrap();
    }

    let zipf = ZipfianGenerator::new(NUM_RECORDS, 0.99);
    let mut rng = StdRng::seed_from_u64(200);

    let total_ops = (MW_WRITERS * MW_OPS_PER_WRITER) as u64;
    let mut group = c.benchmark_group("multiwriter");
    group.throughput(Throughput::Elements(total_ops));

    group.bench_function("no_contention_diff_tables", |b| {
        b.iter_batched(
            || {
                // Generate per-thread key sequences deterministically in the setup
                // phase so RNG state stays off the hot path.
                (0..MW_WRITERS)
                    .map(|_| (0..MW_OPS_PER_WRITER).map(|_| zipf.next(&mut rng)).collect::<Vec<u64>>())
                    .collect::<Vec<_>>()
            },
            |key_sets| {
                let barrier = Arc::new(Barrier::new(MW_WRITERS));
                let handles: Vec<_> = key_sets
                    .into_iter()
                    .enumerate()
                    .map(|(w, keys)| {
                        let store = store.clone();
                        let barrier = Arc::clone(&barrier);
                        let table_name = format!("ycsb_{w}");
                        thread::spawn(move || {
                            barrier.wait();
                            let mut wtx = store.begin_write(None).unwrap();
                            {
                                let mut table =
                                    wtx.open_table::<YcsbRecord>(table_name.as_str()).unwrap();
                                for key in keys {
                                    let _ = table.update(key, YcsbRecord::new(key.wrapping_add(1)));
                                }
                            }
                            wtx.commit().unwrap();
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
                black_box(());
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Shared multi-writer suite
// ---------------------------------------------------------------------------

fn bench_multiwriter_suite(c: &mut Criterion) {
    let mut engine = UltimaMultiWriterEngine::new();
    bench_multiwriter_workloads(c, &mut engine);
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

fn ycsb_multiwriter_criterion() -> Criterion {
    ycsb_criterion()
}

criterion_group! {
    name = multiwriter;
    config = ycsb_multiwriter_criterion();
    targets =
        bench_baseline_single_writer,
        bench_no_contention,
        bench_multiwriter_suite
}
criterion_main!(multiwriter);
