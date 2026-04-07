//! YCSB-style benchmarks for multi-writer OCC.
//!
//! Since `WriteTx` is not `Send` (see `dyn Any` type-erasure limitation),
//! these benchmarks simulate concurrent writers on a single thread using a
//! burst pattern: open W writers, execute operations, commit in sequence,
//! retry on conflict.
//!
//! Includes Ultima-specific scenarios (baseline, no-contention) plus the
//! shared multi-writer suite via `MultiWriterEngine`.

use std::hint::black_box;

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
        require_explicit_version: false,
    });
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
        let mut committed = 0u64;
        let mut conflicts = 0u64;

        // Open all writers simultaneously (burst pattern)
        let mut writers: Vec<_> = (0..key_sets.len())
            .map(|_| Some(self.store.begin_write(None).unwrap()))
            .collect();

        // Each writer updates its keys
        for (w, wtx_opt) in writers.iter_mut().enumerate() {
            let wtx = wtx_opt.as_mut().unwrap();
            let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
            for &key in &key_sets[w] {
                let _ = table.update(key, YcsbRecord::new(key.wrapping_add(1)));
            }
        }

        // Commit in sequence — conflicts trigger retry with fresh tx
        for w in 0..key_sets.len() {
            let wtx = writers[w].take().unwrap();
            match wtx.commit() {
                Ok(_) => committed += 1,
                Err(ultima_db::Error::WriteConflict { .. }) => {
                    conflicts += 1;
                    // Retry with fresh transaction
                    let mut wtx = self.store.begin_write(None).unwrap();
                    {
                        let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                        for &key in &key_sets[w] {
                            let _ = table.update(key, YcsbRecord::new(key.wrapping_add(1)));
                        }
                    }
                    wtx.commit().unwrap();
                    committed += 1;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
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
        require_explicit_version: false,
    });
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
        b.iter(|| {
            let table_names: Vec<String> =
                (0..MW_WRITERS).map(|w| format!("ycsb_{w}")).collect();
            let mut writers: Vec<_> = (0..MW_WRITERS)
                .map(|_| store.begin_write(None).unwrap())
                .collect();

            for (w, wtx) in writers.iter_mut().enumerate() {
                let mut table = wtx
                    .open_table::<YcsbRecord>(table_names[w].as_str())
                    .unwrap();
                for _ in 0..MW_OPS_PER_WRITER {
                    let key = zipf.next(&mut rng);
                    let _ = table.update(key, YcsbRecord::new(key.wrapping_add(1)));
                }
            }

            for wtx in writers {
                wtx.commit().unwrap();
            }
            black_box(());
        });
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
