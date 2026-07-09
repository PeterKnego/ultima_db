// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Point-lookup micro-bench for the fanout (`T`) A/B sweep.
//!
//! `get` is read-only, so there is no immutable-vs-in-place arm here (unlike the
//! `insert_mut` / `remove_mut` benches). The single `get` arm exists to give the
//! fanout sweep its **read** column: reads favor a shallower tree (larger `T` →
//! fewer cache-missing node descents), the opposite pressure from delete, so the
//! read/insert-vs-delete tension is what picks the balance point. See
//! `scripts/fanout_ab.sh` and `docs/superpowers/specs/2026-07-08-btree-optimization-candidates.md` §2.
//!
//! The tree is built once per input (untimed, via the fast in-place `insert_mut`)
//! and every key is then looked up in the timed loop.
//!
//! Run: `cargo bench --bench btree_get_bench`

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ultima_db::BTree;

/// Deterministic scatter (LCG) — no rng/Date dependency. Matches the other
/// btree benches so the same key distribution is measured across the sweep.
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

fn bench_get_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_random");
    for &n in &[100_000u64, 1_000_000] {
        group.throughput(Throughput::Elements(n));
        let keys = scatter(n);

        // Built once, untimed — reads never mutate, so the tree is shared across
        // all iterations.
        let mut t = BTree::<u64, u64>::new();
        for &k in &keys {
            t.insert_mut(k, k);
        }

        group.bench_with_input(BenchmarkId::new("get", n), &keys, |b, keys| {
            b.iter(|| {
                for k in keys {
                    black_box(t.get(k));
                }
            });
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
    targets = bench_get_random
}
criterion_main!(benches);
