// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Prototype benchmark for optimization #1: in-place `BTree::insert_mut` vs the
//! immutable `BTree::insert`.
//!
//! Both build a privately-owned tree of N keys. The immutable path allocates a
//! fresh `Arc<BTreeNode>` for every node on the root→leaf path on *every* key;
//! `insert_mut` descends through `Arc::make_mut`, mutating nodes in place while
//! they are uniquely owned (the common case inside a batch / a single WriteTx).
//!
//! Three access patterns are measured because they stress the path-sharing
//! differently:
//!   * `ascending` — sequential ids (UltimaDB's dominant auto-increment case);
//!     successive inserts share almost the entire right spine.
//!   * `random` — scattered keys; less path overlap, more node splits.
//!   * `mixed_snapshot` — build, then take a snapshot, then keep inserting. This
//!     exercises the CoW branch of `make_mut`: the first insert after the
//!     snapshot must re-clone shared nodes, so this is the *pessimistic* case
//!     where in-place wins the least.
//!
//! Run: `cargo bench --bench btree_insert_mut_bench`
//!      `cargo bench --bench btree_insert_mut_bench -- --quick` for a smoke pass.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ultima_db::BTree;

/// Deterministic scatter (LCG) — no rng/Date dependency.
fn scatter(n: u64) -> Vec<u64> {
    let mut lcg = 0x2545F4914F6CDD1Du64;
    (0..n)
        .map(|_| {
            lcg = lcg
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            lcg
        })
        .collect()
}

fn bench_ascending(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_ascending");
    for &n in &[1_000u64, 100_000, 1_000_000] {
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("immutable", n), &n, |b, &n| {
            b.iter(|| {
                let mut t = BTree::<u64, u64>::new();
                for i in 0..n {
                    t = t.insert(i, i);
                }
                black_box(t)
            });
        });

        group.bench_with_input(BenchmarkId::new("in_place", n), &n, |b, &n| {
            b.iter(|| {
                let mut t = BTree::<u64, u64>::new();
                for i in 0..n {
                    t.insert_mut(i, i);
                }
                black_box(t)
            });
        });
    }
    group.finish();
}

fn bench_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_random");
    for &n in &[1_000u64, 100_000, 1_000_000] {
        group.throughput(Throughput::Elements(n));
        let keys = scatter(n);

        group.bench_with_input(BenchmarkId::new("immutable", n), &keys, |b, keys| {
            b.iter(|| {
                let mut t = BTree::<u64, u64>::new();
                for &k in keys {
                    t = t.insert(k, k);
                }
                black_box(t)
            });
        });

        group.bench_with_input(BenchmarkId::new("in_place", n), &keys, |b, keys| {
            b.iter(|| {
                let mut t = BTree::<u64, u64>::new();
                for &k in keys {
                    t.insert_mut(k, k);
                }
                black_box(t)
            });
        });
    }
    group.finish();
}

/// Pessimistic case: half the inserts happen *after* a live snapshot is taken,
/// so those inserts hit the CoW branch of `make_mut` and re-clone shared nodes.
/// In-place should still win (the snapshot only shares the path touched once),
/// but by a smaller margin — this guards against over-claiming the speedup.
fn bench_mixed_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_mixed_snapshot");
    for &n in &[100_000u64, 1_000_000] {
        group.throughput(Throughput::Elements(n));
        let half = n / 2;

        group.bench_with_input(BenchmarkId::new("immutable", n), &n, |b, &n| {
            b.iter(|| {
                let mut t = BTree::<u64, u64>::new();
                for i in 0..half {
                    t = t.insert(i, i);
                }
                let _snap = t.clone();
                for i in half..n {
                    t = t.insert(i, i);
                }
                black_box((t, _snap))
            });
        });

        group.bench_with_input(BenchmarkId::new("in_place", n), &n, |b, &n| {
            b.iter(|| {
                let mut t = BTree::<u64, u64>::new();
                for i in 0..half {
                    t.insert_mut(i, i);
                }
                let _snap = t.clone();
                for i in half..n {
                    t.insert_mut(i, i);
                }
                black_box((t, _snap))
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_ascending, bench_random, bench_mixed_snapshot);
criterion_main!(benches);
