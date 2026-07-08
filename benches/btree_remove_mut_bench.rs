// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! A/B benchmark for the in-place `BTree::remove_mut` (see
//! docs/superpowers/specs/2026-07-08-btree-remove-mut-design.md).
//!
//! Mirrors `btree_insert_mut_bench.rs`. Each arm builds a privately-owned tree
//! of N keys (untimed setup, via the fast in-place `insert_mut`) and then times
//! deleting **every** key. The immutable `remove` allocates a fresh
//! `Arc<BTreeNode>` for the whole root->leaf path on *every* delete; the
//! `remove_mut` arm descends through `Arc::make_mut`, mutating uniquely-owned
//! nodes in place.
//!
//! NOTE: both the `immutable` and `in_place` arms are active — `remove_mut`
//! has landed, so this file measures the A/B ratio directly.
//!
//! Run: `cargo bench --bench btree_remove_mut_bench`

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

/// Build a privately-owned tree of the given keys via the fast in-place insert.
fn build(keys: &[u64]) -> BTree<u64, u64> {
    let mut t = BTree::<u64, u64>::new();
    for &k in keys {
        t.insert_mut(k, k);
    }
    t
}

fn bench_delete_ascending(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_ascending");
    for &n in &[100_000u64, 1_000_000] {
        group.throughput(Throughput::Elements(n));
        let keys: Vec<u64> = (0..n).collect();

        group.bench_with_input(BenchmarkId::new("immutable", n), &keys, |b, keys| {
            b.iter_batched(
                || build(keys),
                |mut t| {
                    for k in keys {
                        t = t.remove(k).unwrap();
                    }
                    black_box(t)
                },
                criterion::BatchSize::PerIteration,
            );
        });

        group.bench_with_input(BenchmarkId::new("in_place", n), &keys, |b, keys| {
            b.iter_batched(
                || build(keys),
                |mut t| {
                    for k in keys {
                        t.remove_mut(k);
                    }
                    black_box(t)
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_delete_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_random");
    for &n in &[100_000u64, 1_000_000] {
        group.throughput(Throughput::Elements(n));
        let keys = scatter(n);

        group.bench_with_input(BenchmarkId::new("immutable", n), &keys, |b, keys| {
            b.iter_batched(
                || build(keys),
                |mut t| {
                    for k in keys {
                        t = t.remove(k).unwrap();
                    }
                    black_box(t)
                },
                criterion::BatchSize::PerIteration,
            );
        });

        group.bench_with_input(BenchmarkId::new("in_place", n), &keys, |b, keys| {
            b.iter_batched(
                || build(keys),
                |mut t| {
                    for k in keys {
                        t.remove_mut(k);
                    }
                    black_box(t)
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_secs(1))
        .measurement_time(std::time::Duration::from_secs(4))
}

criterion_group! {
    name = benches;
    config = config();
    targets = bench_delete_ascending, bench_delete_random
}
criterion_main!(benches);
