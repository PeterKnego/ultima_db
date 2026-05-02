// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

#![allow(clippy::drop_non_drop, clippy::redundant_iter_cloned)]

//! Disjoint-table concurrent bench. N writers, each pinned to its *own*
//! table. Designed to showcase the sharded commit path: disjoint-table
//! writers should not serialize on commit.
//!
//! Contrast with `smallbank_scaling_bench`, where all writers touch the
//! same hot keys and benefit from early-fail intents but not from
//! commit-path sharding.
//!
//! Each writer performs UPDATES (no inserts) on its own table with a
//! shared schema `ItemState { value: u64 }`. The store is pre-seeded with
//! `KEYS_PER_TABLE` rows per table.

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::{Rng, RngExt, SeedableRng};
use rand::rngs::StdRng;
use ultima_db::{Store, StoreConfig, WriterMode};

const WRITER_COUNTS: &[usize] = &[2, 4, 8, 16];
const OPS_PER_WRITER: usize = 50;
const KEYS_PER_TABLE: u64 = 10_000;
const POOL_SIZE: usize = 64;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ItemState {
    value: u64,
}

fn table_name(idx: usize) -> String {
    format!("shard_{idx:02}")
}

/// Build a store with one table per potential writer (max WRITER_COUNTS)
/// and preload KEYS_PER_TABLE rows each.
fn build_store(max_writers: usize) -> (Store, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir().unwrap();
    let cfg = StoreConfig {
        num_snapshots_retained: 2,
        auto_snapshot_gc: true,
        writer_mode: WriterMode::MultiWriter,
        persistence: ultima_db::Persistence::Standalone {
            dir: tmpdir.path().to_path_buf(),
            durability: ultima_db::Durability::Eventual,
        },
        ..StoreConfig::default()
    };
    let store = Store::new(cfg).unwrap();

    for i in 0..max_writers {
        let name = table_name(i);
        store.register_table::<ItemState>(&name).unwrap();
    }

    let mut wtx = store.begin_write(None).unwrap();
    for i in 0..max_writers {
        let name = table_name(i);
        let mut t = wtx.open_table::<ItemState>(name.as_str()).unwrap();
        // No secondary index: primary-key `update` is all this bench
        // needs, and a unique index on `value` would conflict on repeat
        // updates of the same key with different values.
        let batch: Vec<ItemState> = (1..=KEYS_PER_TABLE)
            .map(|v| ItemState { value: v })
            .collect();
        t.insert_batch(batch).unwrap();
    }
    wtx.commit().unwrap();

    (store, tmpdir)
}

/// Generate N writers' op sequences, each a list of (id, new_value) pairs
/// updating random keys in that writer's own table.
fn gen_ops(num_writers: usize, seed: u64) -> Vec<Vec<(u64, u64)>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..num_writers)
        .map(|_| {
            (0..OPS_PER_WRITER)
                .map(|_| {
                    let id = rng.random_range(1..=KEYS_PER_TABLE);
                    // Offset to avoid unique-index collisions on `value`.
                    // Each writer modifies its own KEYS_PER_TABLE ids, so
                    // just bump to a value that won't collide with siblings.
                    let new_value = KEYS_PER_TABLE + rng.random_range(1..=10_000_000);
                    (id, new_value)
                })
                .collect()
        })
        .collect()
}

fn run_burst(store: &Store, table_names: &[String], op_sets: &[Vec<(u64, u64)>]) {
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
                            // Each writer rewrites `value` to a uniquely
                            // reserved range, so the unique index stays
                            // consistent even under repeated updates.
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
                            if let Some(w) = wait_for { w.wait(); }
                            continue;
                        }
                        Err(e) => panic!("ops error: {e}"),
                    }
                    match wtx.commit() {
                        Ok(_) => return,
                        Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                            if let Some(w) = wait_for { w.wait(); }
                            continue;
                        }
                        Err(e) => panic!("commit error: {e}"),
                    }
                }
            })
        })
        .collect();
    for h in handles { h.join().unwrap(); }
}

fn bench(c: &mut Criterion) {
    let max_writers = *WRITER_COUNTS.iter().max().unwrap();
    let (store, _tmp) = build_store(max_writers);

    let mut group = c.benchmark_group("disjoint_tables");
    group.sample_size(20).measurement_time(Duration::from_secs(10));

    for &n in WRITER_COUNTS {
        let total_ops = (n * OPS_PER_WRITER) as u64;
        group.throughput(Throughput::Elements(total_ops));

        let names: Vec<String> = (0..n).map(table_name).collect();
        let pool: Vec<Vec<Vec<(u64, u64)>>> = (0..POOL_SIZE)
            .map(|i| gen_ops(n, 1000 + i as u64))
            .collect();
        let cursor = std::sync::atomic::AtomicUsize::new(0);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched_ref(
                || {
                    let idx = cursor.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % pool.len();
                    pool[idx].clone()
                },
                |op_sets| {
                    black_box(run_burst(&store, &names, op_sets));
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(disjoint, bench);
criterion_main!(disjoint);
