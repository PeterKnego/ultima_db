// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! End-to-end example: build a small in-memory index, search it.
//!
//!     cargo run --example quick_search -p ultima-vector

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::RngExt;
use ultima_db::{Store, StoreConfig};
use ultima_vector::{Cosine, HnswParams, VectorCollection};

fn random_unit_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in &mut v {
        *x /= norm;
    }
    v
}

fn main() {
    const DIM: usize = 16;
    const N: usize = 500;

    let store = Store::new(StoreConfig::default()).unwrap();
    let coll: VectorCollection<String, Cosine> =
        VectorCollection::open(store, "demo", HnswParams::for_dim(DIM), Cosine).unwrap();

    let mut rng = StdRng::seed_from_u64(0xCAFEBABE);
    let items: Vec<(Vec<f32>, String)> = (0..N)
        .map(|i| (random_unit_vec(&mut rng, DIM), format!("doc-{i}")))
        .collect();
    coll.bulk_insert(items).unwrap();
    println!("inserted {N} vectors of dim {DIM}");

    let query = random_unit_vec(&mut rng, DIM);
    let top = coll.search(&query, 5, None, None).unwrap();
    println!("top-5 nearest to a random query:");
    for (id, dist) in top {
        println!("  id={id:>4} dist={dist:.4}");
    }
}
