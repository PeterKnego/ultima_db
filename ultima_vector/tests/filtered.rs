// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Filtered ANN search: assert results stay within filter and recall holds
//! at varying selectivities, including the brute-force fallback regime.

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::RngExt;
use ultima_db::{Store, StoreConfig};
use ultima_vector::filter::RoaringTreemap;
use ultima_vector::{Cosine, HnswParams, VectorCollection};

const DIM: usize = 16;
const N: usize = 1_500;
const Q: usize = 100;
const K: usize = 10;

fn random_unit_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in &mut v {
        *x /= norm;
    }
    v
}

fn brute_force_top_k_in_bitmap(
    query: &[f32],
    vectors: &[(u64, Vec<f32>)],
    filter: &RoaringTreemap,
    k: usize,
) -> Vec<u64> {
    use std::cmp::Ordering;
    let mut scored: Vec<(u64, f32)> = vectors
        .iter()
        .filter(|(id, _)| filter.contains(*id))
        .map(|(id, v)| {
            let mut dot = 0.0f32;
            for i in 0..query.len() {
                dot += query[i] * v[i];
            }
            (*id, 1.0 - dot)
        })
        .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    scored.into_iter().take(k).map(|(id, _)| id).collect()
}

struct Setup {
    coll: VectorCollection<u64, Cosine>,
    vectors: Vec<(u64, Vec<f32>)>,
}

fn build() -> Setup {
    let mut rng = StdRng::seed_from_u64(0xF11);
    let store = Store::new(StoreConfig::default()).unwrap();
    let coll: VectorCollection<u64, Cosine> = VectorCollection::open(
        store.clone(),
        "vec",
        HnswParams::for_dim(DIM),
        Cosine,
    )
    .unwrap();
    let mut vectors: Vec<(u64, Vec<f32>)> = Vec::with_capacity(N);
    let mut tx = store.begin_write(None).unwrap();
    for i in 0..N {
        let v = random_unit_vec(&mut rng, DIM);
        let id = coll.upsert_in(&mut tx, v.clone(), i as u64).unwrap();
        vectors.push((id, v));
    }
    tx.commit().unwrap();
    Setup { coll, vectors }
}

#[test]
fn results_stay_inside_filter() {
    let Setup { coll, vectors } = build();
    let mut rng = StdRng::seed_from_u64(0x42);

    // Select a 30% random subset.
    let mut filter = RoaringTreemap::new();
    for (id, _) in &vectors {
        if rng.random_range(0.0f32..1.0) < 0.3 {
            filter.insert(*id);
        }
    }

    let q = random_unit_vec(&mut rng, DIM);
    let res = coll.search(&q, K, Some(&filter), Some(64)).unwrap();
    for (id, _) in &res {
        assert!(filter.contains(*id), "result id {id} not in filter");
    }
}

#[test]
fn brute_force_fallback_returns_exact_top_k() {
    let Setup { coll, vectors } = build();
    let mut rng = StdRng::seed_from_u64(0x99);

    // Tiny filter: well below BRUTE_FORCE_THRESHOLD (128). Forces fallback.
    let mut filter = RoaringTreemap::new();
    for (id, _) in vectors.iter().take(20) {
        filter.insert(*id);
    }

    let q = random_unit_vec(&mut rng, DIM);
    let hnsw_top: Vec<u64> = coll
        .search(&q, K, Some(&filter), Some(64))
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    let truth = brute_force_top_k_in_bitmap(&q, &vectors, &filter, K);

    // Brute-force fallback must match brute-force exactly.
    assert_eq!(hnsw_top, truth, "fallback didn't match brute force");
}

#[test]
fn recall_holds_at_50pct_selectivity() {
    let Setup { coll, vectors } = build();
    let mut rng = StdRng::seed_from_u64(0x77);

    // 50% filter.
    let mut filter = RoaringTreemap::new();
    for (id, _) in &vectors {
        if rng.random_range(0.0f32..1.0) < 0.5 {
            filter.insert(*id);
        }
    }

    let mut hits = 0usize;
    for _ in 0..Q {
        let q = random_unit_vec(&mut rng, DIM);
        let hnsw_top: Vec<u64> = coll
            .search(&q, K, Some(&filter), Some(128))
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let truth_set: std::collections::HashSet<u64> =
            brute_force_top_k_in_bitmap(&q, &vectors, &filter, K).into_iter().collect();
        for id in &hnsw_top {
            if truth_set.contains(id) {
                hits += 1;
            }
        }
    }
    let recall = hits as f64 / (Q * K) as f64;
    println!("recall@{K} (50% filter) = {recall:.4}");
    assert!(recall >= 0.85, "recall@{K} = {recall:.4} < 0.85");
}
