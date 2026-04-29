//! Persistence round-trip: build an HNSW index, close the store, reopen
//! from the WAL/checkpoint, and verify search results match.

#![cfg(feature = "persistence")]

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::RngExt;
use ultima_db::{Durability, Persistence, Store, StoreConfig};
use ultima_vector::{Cosine, HnswParams, VectorCollection};

const DIM: usize = 8;
const N: usize = 200;

fn random_unit_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in &mut v {
        *x /= norm;
    }
    v
}

fn standalone_config(dir: &std::path::Path) -> StoreConfig {
    StoreConfig {
        persistence: Persistence::Standalone {
            dir: dir.to_path_buf(),
            durability: Durability::Consistent,
        },
        ..StoreConfig::default()
    }
}

#[test]
fn round_trip_after_wal_replay_matches_pre_close_results() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path());

    let mut rng = StdRng::seed_from_u64(0xCAFE);
    let mut vectors: Vec<Vec<f32>> = (0..N).map(|_| random_unit_vec(&mut rng, DIM)).collect();
    // Use the first row as the query target.
    let query = vectors[0].clone();

    // Build phase.
    let pre_top_ids: Vec<u64> = {
        let store = Store::new(config.clone()).unwrap();
        let coll: VectorCollection<u64, Cosine> = VectorCollection::open(
            store.clone(),
            "vec",
            HnswParams::for_dim(DIM),
            Cosine,
        )
        .unwrap();
        // Nothing to recover on a fresh store, but call it to exercise the path.
        store.recover().unwrap();

        let items: Vec<(Vec<f32>, u64)> = vectors
            .drain(..)
            .enumerate()
            .map(|(i, v)| (v, i as u64))
            .collect();
        coll.bulk_insert(items).unwrap();

        // Force a checkpoint so the entire dataset is on disk before close.
        store.checkpoint().unwrap();

        coll.search(&query, 10, None, Some(64))
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }; // store dropped here

    // Recovery phase.
    let post_top_ids: Vec<u64> = {
        let store = Store::new(config).unwrap();
        let coll: VectorCollection<u64, Cosine> = VectorCollection::open(
            store.clone(),
            "vec",
            HnswParams::for_dim(DIM),
            Cosine,
        )
        .unwrap();
        store.recover().unwrap();

        coll.search(&query, 10, None, Some(64))
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };

    assert_eq!(pre_top_ids, post_top_ids, "post-recovery top-K must match pre-close");
}

#[test]
fn wal_only_recovery_works_without_checkpoint() {
    // Skip checkpoint; rely entirely on WAL replay to reconstruct the graph.
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path());

    let mut rng = StdRng::seed_from_u64(0x1234);
    let vectors: Vec<Vec<f32>> = (0..50).map(|_| random_unit_vec(&mut rng, DIM)).collect();
    let query = vectors[0].clone();

    let pre_top: Vec<u64> = {
        let store = Store::new(config.clone()).unwrap();
        let coll: VectorCollection<u64, Cosine> = VectorCollection::open(
            store.clone(),
            "vec",
            HnswParams::for_dim(DIM),
            Cosine,
        )
        .unwrap();
        store.recover().unwrap();
        let items: Vec<(Vec<f32>, u64)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (v.clone(), i as u64))
            .collect();
        coll.bulk_insert(items).unwrap();

        coll.search(&query, 5, None, None)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };

    let post_top: Vec<u64> = {
        let store = Store::new(config).unwrap();
        let coll: VectorCollection<u64, Cosine> = VectorCollection::open(
            store.clone(),
            "vec",
            HnswParams::for_dim(DIM),
            Cosine,
        )
        .unwrap();
        store.recover().unwrap();
        coll.search(&query, 5, None, None)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };

    assert_eq!(pre_top, post_top);
}
