//! Distance kernel + end-to-end HNSW search benchmarks.
//!
//! Run with `cargo bench -p ultima-vector --bench distance`.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use ultima_db::{Store, StoreConfig};
use ultima_vector::distance::Distance;
use ultima_vector::{Cosine, CosineNormalized, DotProduct, HnswParams, L2, VectorCollection};

fn rand_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect()
}

fn random_unit_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    let mut v = rand_vec(rng, dim);
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in &mut v {
        *x /= norm;
    }
    v
}

fn bench_pairwise(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(0xBEEF);
    let mut group = c.benchmark_group("distance/pairwise");
    for &dim in &[128usize, 384, 768, 1536] {
        let a = rand_vec(&mut rng, dim);
        let b = rand_vec(&mut rng, dim);
        group.throughput(Throughput::Elements(dim as u64));

        group.bench_with_input(BenchmarkId::new("cosine", dim), &dim, |bencher, _| {
            bencher.iter(|| black_box(Cosine.distance(black_box(&a), black_box(&b))));
        });
        group.bench_with_input(BenchmarkId::new("l2", dim), &dim, |bencher, _| {
            bencher.iter(|| black_box(L2.distance(black_box(&a), black_box(&b))));
        });
        group.bench_with_input(BenchmarkId::new("dot", dim), &dim, |bencher, _| {
            bencher.iter(|| black_box(DotProduct.distance(black_box(&a), black_box(&b))));
        });
        group.bench_with_input(
            BenchmarkId::new("cosine_normalized", dim),
            &dim,
            |bencher, _| {
                bencher.iter(|| {
                    black_box(CosineNormalized.distance(black_box(&a), black_box(&b)))
                });
            },
        );
    }
    group.finish();
}

fn bench_distance_many(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(0xBA7E);
    let dim = 768;
    let n = 1024;
    let q = rand_vec(&mut rng, dim);
    let targets_storage: Vec<Vec<f32>> = (0..n).map(|_| rand_vec(&mut rng, dim)).collect();
    let targets: Vec<&[f32]> = targets_storage.iter().map(|v| v.as_slice()).collect();
    let mut out = vec![0.0f32; n];

    let mut group = c.benchmark_group("distance/many");
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("cosine_1024_dim768", |b| {
        b.iter(|| Cosine.distance_many(black_box(&q), black_box(&targets), black_box(&mut out)));
    });
    group.bench_function("l2_1024_dim768", |b| {
        b.iter(|| L2.distance_many(black_box(&q), black_box(&targets), black_box(&mut out)));
    });
    group.finish();
}

fn bench_hnsw_search(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(0xA77E);
    let dim = 768;
    let n = 10_000;

    let store = Store::new(StoreConfig::default()).unwrap();
    let coll: VectorCollection<u64, Cosine> = VectorCollection::open(
        store.clone(),
        "vectors",
        HnswParams::for_dim(dim),
        Cosine,
    )
    .unwrap();

    let items: Vec<(Vec<f32>, u64)> = (0..n)
        .map(|i| (random_unit_vec(&mut rng, dim), i as u64))
        .collect();
    coll.bulk_insert(items).unwrap();

    // Pre-build a pool of queries so the bench measures search, not RNG.
    let queries: Vec<Vec<f32>> = (0..256).map(|_| random_unit_vec(&mut rng, dim)).collect();
    let mut q_idx = 0usize;

    c.bench_function("hnsw/search_k10_dim768_n10k", |b| {
        b.iter(|| {
            let q = &queries[q_idx % queries.len()];
            q_idx = q_idx.wrapping_add(1);
            let hits = coll.search(black_box(q), 10, None, Some(64)).unwrap();
            black_box(hits);
        });
    });
}

criterion_group!(benches, bench_pairwise, bench_distance_many, bench_hnsw_search);
criterion_main!(benches);
