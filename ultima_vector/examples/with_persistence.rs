//! Persistence round-trip: build an index, checkpoint, drop the store,
//! reopen from the same directory, and search again.
//!
//! Requires the `persistence` feature:
//!
//!     cargo run --example with_persistence -p ultima-vector --features persistence

#![cfg(feature = "persistence")]

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::RngExt;
use ultima_db::{Durability, Persistence, Store, StoreConfig};
use ultima_vector::{Cosine, HnswParams, VectorCollection};

#[cfg(feature = "persistence")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Doc {
    title: String,
}

fn random_unit_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in &mut v {
        *x /= norm;
    }
    v
}

fn config(dir: &std::path::Path) -> StoreConfig {
    StoreConfig {
        persistence: Persistence::Standalone {
            dir: dir.to_path_buf(),
            durability: Durability::Consistent,
        },
        ..StoreConfig::default()
    }
}

fn main() {
    const DIM: usize = 8;

    let dir = tempfile::tempdir().unwrap();
    println!("persistence dir: {}", dir.path().display());

    let mut rng = StdRng::seed_from_u64(0xD15C);
    let docs: Vec<Doc> = (0..100).map(|i| Doc { title: format!("doc-{i}") }).collect();
    let vectors: Vec<Vec<f32>> = (0..100).map(|_| random_unit_vec(&mut rng, DIM)).collect();
    let query = vectors[0].clone();

    // Build the index, force a checkpoint, then let the store drop.
    let pre_top: Vec<u64> = {
        let store = Store::new(config(dir.path())).unwrap();
        // open() registers the data and entry tables with the store's
        // type registry — necessary before recover() can replay anything.
        let coll: VectorCollection<Doc, Cosine> =
            VectorCollection::open(store.clone(), "docs", HnswParams::for_dim(DIM), Cosine)
                .unwrap();
        store.recover().unwrap(); // no-op on fresh dir, but safe to call.

        let items: Vec<(Vec<f32>, Doc)> =
            vectors.iter().cloned().zip(docs.iter().cloned()).collect();
        coll.bulk_insert(items).unwrap();

        // Optional: force a checkpoint. Without it WAL alone reconstructs.
        store.checkpoint().unwrap();

        coll.search(&query, 5, None, None)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };
    println!("before close: top-5 = {pre_top:?}");

    // Reopen the same directory in a fresh Store.
    let post_top: Vec<u64> = {
        let store = Store::new(config(dir.path())).unwrap();
        let coll: VectorCollection<Doc, Cosine> =
            VectorCollection::open(store.clone(), "docs", HnswParams::for_dim(DIM), Cosine)
                .unwrap();
        store.recover().unwrap();
        coll.search(&query, 5, None, None)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };
    println!("after recover: top-5 = {post_top:?}");

    assert_eq!(pre_top, post_top, "search results disagree across restart");
    println!("ok — same results before and after restart");
}
