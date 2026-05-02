// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Recall integration test: assert HNSW top-K matches brute-force ground truth
//! on random L2-normalized vectors.

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::RngExt;
use ultima_db::{Store, StoreConfig};
use ultima_vector::{Cosine, HnswParams, VectorCollection};

const DIM: usize = 32;
const N: usize = 2_000;
const Q: usize = 200;
const K: usize = 10;
const RECALL_FLOOR: f64 = 0.95;

fn random_unit_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in &mut v {
        *x /= norm;
    }
    v
}

fn brute_force_top_k(
    query: &[f32],
    vectors: &[(u64, Vec<f32>)],
    k: usize,
) -> Vec<u64> {
    use std::cmp::Ordering;
    let mut scored: Vec<(u64, f32)> = vectors
        .iter()
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

#[test]
fn recall_at_10_meets_floor_on_random_vectors() {
    let mut rng = StdRng::seed_from_u64(0xBEEF);

    let store = Store::new(StoreConfig::default()).unwrap();
    let coll: VectorCollection<u64, Cosine> = VectorCollection::open(
        store,
        "vec",
        HnswParams::for_dim(DIM),
        Cosine,
    )
    .unwrap();

    // Build the index in a single write transaction for speed.
    let mut vectors: Vec<(u64, Vec<f32>)> = Vec::with_capacity(N);
    {
        let store = coll.store().clone();
        let mut tx = store.begin_write(None).unwrap();
        for i in 0..N {
            let v = random_unit_vec(&mut rng, DIM);
            let id = coll.upsert_in(&mut tx, v.clone(), i as u64).unwrap();
            vectors.push((id, v));
        }
        tx.commit().unwrap();
    }

    // Recall against brute force.
    let mut hits = 0usize;
    for _ in 0..Q {
        let q = random_unit_vec(&mut rng, DIM);
        let hnsw_top: Vec<u64> = coll
            .search(&q, K, None, Some(64))
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let truth: Vec<u64> = brute_force_top_k(&q, &vectors, K);
        let truth_set: std::collections::HashSet<u64> = truth.into_iter().collect();
        for id in &hnsw_top {
            if truth_set.contains(id) {
                hits += 1;
            }
        }
    }

    let recall = hits as f64 / (Q * K) as f64;
    println!("recall@{K} = {recall:.4}");
    assert!(
        recall >= RECALL_FLOOR,
        "recall@{K} = {recall:.4} < floor {RECALL_FLOOR}"
    );
}
