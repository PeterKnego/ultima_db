// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

#![allow(clippy::drop_non_drop, clippy::redundant_iter_cloned)]

//! Consolidated multi-writer scaling bench: one writer-count sweep, four
//! contention shapes. Replaces the former `disjoint_tables_bench`,
//! `smallbank_scaling_bench`, and the scaling group of the shared YCSB
//! multi-writer suite.
//!
//! Note: the two ycsb shapes generate their key sets inside the timed
//! iteration (matching the original scaling group), while disjoint/smallbank
//! draw from pre-generated pools — compare absolute throughput across shapes
//! with that in mind.
//!
//! Groups (criterion id `mw_scaling_<shape>/<writers>`):
//! - `ycsb_low`        — one shared table, Zipfian over 10K keys (in-memory)
//! - `ycsb_high`       — one shared table, hot keys 1..=10 (in-memory)
//! - `disjoint`        — one table per writer (Eventual persistence)
//! - `smallbank_high`  — 3-table txns on hot accounts (Eventual persistence)

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use ultima_bench_workloads::smallbank::{
    build_concurrent_store, gen_hot_ops, run_smallbank_burst,
};
use ultima_bench_workloads::ycsb::{NUM_RECORDS, YcsbRecord, ZipfianGenerator, ZIPFIAN_CONSTANT};
use ultima_db::{Store, StoreConfig, WriterMode};

const WRITER_COUNTS: &[usize] = &[1, 2, 4, 8, 16];
const MULTI_WRITER_COUNTS: &[usize] = &[2, 4, 8, 16];
const OPS_PER_WRITER: usize = 50;
const POOL_SIZE: usize = 64;

// ---------------------------------------------------------------------------
// Shape 1+2: YCSB shared table (ported from the shared suite's scaling group)
// ---------------------------------------------------------------------------

fn ycsb_store() -> Store {
    let store = Store::new(
        StoreConfig::builder()
            .num_snapshots_retained(2)
            .auto_snapshot_gc(true)
            .writer_mode(WriterMode::MultiWriter)
            .build(),
    )
    .unwrap();
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

/// One burst: each key set is one writer's updates on the shared table,
/// committed concurrently with retry-on-conflict. Returns total conflicts.
fn ycsb_burst(store: &Store, key_sets: &[Vec<u64>]) -> u64 {
    let barrier = Arc::new(Barrier::new(key_sets.len()));
    let handles: Vec<_> = key_sets
        .iter()
        .cloned()
        .map(|keys| {
            let store = store.clone();
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
                        Ok(_) => return conflicts,
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
    handles.into_iter().map(|h| h.join().unwrap()).sum()
}

fn bench_ycsb_shared(c: &mut Criterion, shape: &str, hot: bool, seed: u64) {
    let store = ycsb_store();
    let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
    let mut rng = StdRng::seed_from_u64(seed);

    let mut group = c.benchmark_group(format!("mw_scaling_{shape}"));
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(10));
    for &n in WRITER_COUNTS {
        let total_ops = (n * OPS_PER_WRITER) as u64;
        group.throughput(Throughput::Elements(total_ops));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &nw| {
            b.iter(|| {
                let key_sets: Vec<Vec<u64>> = (0..nw)
                    .map(|_| {
                        (0..OPS_PER_WRITER)
                            .map(|_| {
                                if hot {
                                    rng.random_range(1..=10u64)
                                } else {
                                    zipf.next(&mut rng)
                                }
                            })
                            .collect()
                    })
                    .collect();
                black_box(ycsb_burst(&store, &key_sets))
            });
        });
    }
    group.finish();
}

fn bench_ycsb_low(c: &mut Criterion) {
    bench_ycsb_shared(c, "ycsb_low", false, 500);
}

fn bench_ycsb_high(c: &mut Criterion) {
    bench_ycsb_shared(c, "ycsb_high", true, 501);
}

// ---------------------------------------------------------------------------
// Shape 3: disjoint tables (ported verbatim from disjoint_tables_bench.rs;
// functions renamed with a disjoint_ prefix)
// ---------------------------------------------------------------------------

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ItemState {
    value: u64,
}

const KEYS_PER_TABLE: u64 = 10_000;

fn disjoint_table_name(idx: usize) -> String {
    format!("shard_{idx:02}")
}

fn disjoint_build_store(max_writers: usize) -> (Store, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir_in(ultima_bench_workloads::ycsb::bench_disk_dir()).unwrap();
    let cfg = StoreConfig::builder()
        .num_snapshots_retained(2)
        .auto_snapshot_gc(true)
        .writer_mode(WriterMode::MultiWriter)
        .persistence(ultima_db::Persistence::standalone(
            tmpdir.path().to_path_buf(),
            ultima_db::Durability::Eventual,
            ultima_db::WalWrite::PerEntry,
        ))
        .build();
    let store = Store::new(cfg).unwrap();
    for i in 0..max_writers {
        store
            .register_table::<ItemState>(&disjoint_table_name(i))
            .unwrap();
    }
    let mut wtx = store.begin_write(None).unwrap();
    for i in 0..max_writers {
        let name = disjoint_table_name(i);
        let mut t = wtx.open_table::<ItemState>(name.as_str()).unwrap();
        let batch: Vec<ItemState> = (1..=KEYS_PER_TABLE).map(|v| ItemState { value: v }).collect();
        t.insert_batch(batch).unwrap();
    }
    wtx.commit().unwrap();
    (store, tmpdir)
}

fn disjoint_gen_ops(num_writers: usize, seed: u64) -> Vec<Vec<(u64, u64)>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..num_writers)
        .map(|_| {
            (0..OPS_PER_WRITER)
                .map(|_| {
                    let id = rng.random_range(1..=KEYS_PER_TABLE);
                    let new_value = KEYS_PER_TABLE + rng.random_range(1..=10_000_000);
                    (id, new_value)
                })
                .collect()
        })
        .collect()
}

fn disjoint_run_burst(store: &Store, table_names: &[String], op_sets: &[Vec<(u64, u64)>]) {
    let barrier = Arc::new(Barrier::new(op_sets.len()));
    let handles: Vec<_> = op_sets
        .iter()
        .zip(table_names)
        .map(|(ops, tname)| {
            let ops = ops.clone();
            let tname = tname.clone();
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                loop {
                    let mut wtx = store.begin_write(None).unwrap();
                    let write_res: Result<(), ultima_db::Error> = {
                        let mut t = wtx.open_table::<ItemState>(tname.as_str()).unwrap();
                        let mut res = Ok(());
                        for (id, new_value) in &ops {
                            if let Err(e) = t.update(*id, ItemState { value: *new_value }) {
                                res = Err(e);
                                break;
                            }
                        }
                        res
                    };
                    match write_res {
                        Ok(()) => {}
                        Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                            drop(wtx);
                            if let Some(w) = wait_for {
                                w.wait();
                            }
                            continue;
                        }
                        Err(e) => panic!("ops error: {e}"),
                    }
                    match wtx.commit() {
                        Ok(_) => return,
                        Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                            if let Some(w) = wait_for {
                                w.wait();
                            }
                            continue;
                        }
                        Err(e) => panic!("commit error: {e}"),
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

fn bench_disjoint(c: &mut Criterion) {
    let max_writers = *MULTI_WRITER_COUNTS.iter().max().unwrap();
    let (store, _tmp) = disjoint_build_store(max_writers);

    let mut group = c.benchmark_group("mw_scaling_disjoint");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(10));
    for &n in MULTI_WRITER_COUNTS {
        let total_ops = (n * OPS_PER_WRITER) as u64;
        group.throughput(Throughput::Elements(total_ops));
        let names: Vec<String> = (0..n).map(disjoint_table_name).collect();
        let pool: Vec<Vec<Vec<(u64, u64)>>> = (0..POOL_SIZE)
            .map(|i| disjoint_gen_ops(n, 1000 + i as u64))
            .collect();
        let cursor = AtomicUsize::new(0);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched_ref(
                || {
                    let idx = cursor.fetch_add(1, Ordering::Relaxed) % pool.len();
                    pool[idx].clone()
                },
                |op_sets| {
                    disjoint_run_burst(&store, &names, op_sets);
                    black_box(());
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Shape 4: SmallBank hot accounts (helpers live in bench_workloads::smallbank)
// ---------------------------------------------------------------------------

fn bench_smallbank_high(c: &mut Criterion) {
    let mut group = c.benchmark_group("mw_scaling_smallbank_high");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(10));
    for &n in MULTI_WRITER_COUNTS {
        let total_ops = (n * OPS_PER_WRITER) as u64;
        group.throughput(Throughput::Elements(total_ops));
        let pool: Vec<_> = (0..POOL_SIZE).map(|i| gen_hot_ops(n, 500 + i as u64)).collect();
        let (store, _tmp) = build_concurrent_store();
        let cursor = AtomicUsize::new(0);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched_ref(
                || {
                    let idx = cursor.fetch_add(1, Ordering::Relaxed) % pool.len();
                    pool[idx].clone()
                },
                |op_sets| {
                    run_smallbank_burst(&store, op_sets);
                    black_box(());
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    mw_scaling,
    bench_ycsb_low,
    bench_ycsb_high,
    bench_disjoint,
    bench_smallbank_high
);
criterion_main!(mw_scaling);
