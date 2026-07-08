// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! End-to-end benchmark for optimization #1 wired into `Table`.
//!
//! Drives the real `Store → WriteTx → Table` path (in-memory, no persistence),
//! so it measures the in-place `insert_mut` win as a user sees it, including the
//! copy-on-write branch that fires when a `Table` shares its data tree with an
//! already-committed snapshot.
//!
//! A/B protocol (criterion baselines):
//!   1. On the in-place branch:  cargo bench --bench table_insert_e2e_bench -- --save-baseline inplace
//!   2. Revert the 6 `Table` call sites to `self.data = self.data.insert(...)`.
//!   3. cargo bench --bench table_insert_e2e_bench -- --baseline inplace
//!
//! Run: `cargo bench --bench table_insert_e2e_bench`

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ultima_db::Store;

/// `Table::insert_batch` through a single write transaction on a fresh store.
/// The data tree is privately owned by the WriteTx, so in-place should win big.
fn bench_insert_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_insert_batch");
    for &n in &[10_000usize, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let store = Store::default();
                let mut wtx = store.begin_write(None).unwrap();
                let mut t = wtx.open_table::<u64>("t").unwrap();
                t.insert_batch((0..n as u64).collect()).unwrap();
                wtx.commit().unwrap();
                black_box(store)
            });
        });
    }
    group.finish();
}

/// Single-record `Table::insert` in a loop through one write transaction.
fn bench_single_insert_loop(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_single_insert_loop");
    for &n in &[10_000usize, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let store = Store::default();
                let mut wtx = store.begin_write(None).unwrap();
                let mut t = wtx.open_table::<u64>("t").unwrap();
                for i in 0..n as u64 {
                    t.insert(i).unwrap();
                }
                wtx.commit().unwrap();
                black_box(store)
            });
        });
    }
    group.finish();
}

/// Realistic incremental case: a store already holds `base` committed rows;
/// a *new* write transaction opens the table (sharing the committed snapshot's
/// data tree) and appends `delta` more. The first touch of each shared spine
/// node must CoW-clone; the rest of the appended growth is in place. This is the
/// path that dominates steady-state writes against a populated table.
fn bench_incremental_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_incremental_append");
    let base = 200_000u64;
    for &delta in &[10_000u64, 100_000] {
        group.throughput(Throughput::Elements(delta));
        // Pre-build a store with `base` committed rows once, outside timing.
        let seed = {
            let store = Store::default();
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<u64>("t").unwrap();
            t.insert_batch((0..base).collect()).unwrap();
            wtx.commit().unwrap();
            store
        };
        group.bench_with_input(BenchmarkId::from_parameter(delta), &delta, |b, &delta| {
            b.iter_batched(
                || seed.clone(), // O(1) Store clone; shares committed snapshot
                |store| {
                    let mut wtx = store.begin_write(None).unwrap();
                    let mut t = wtx.open_table::<u64>("t").unwrap();
                    t.insert_batch((base..base + delta).collect()).unwrap();
                    wtx.commit().unwrap();
                    black_box(store)
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_insert_batch,
    bench_single_insert_loop,
    bench_incremental_append
);
criterion_main!(benches);
