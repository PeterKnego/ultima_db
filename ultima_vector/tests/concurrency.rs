//! Concurrency smoke: readers on snapshot of a partially-built collection
//! see internally consistent results while a writer keeps inserting.
//!
//! Uses `ultima_db::Readable` to call `open_table` polymorphically off the
//! cloned store. Each thread opens its own `ReadTx` against the cloned store.

use std::sync::Arc;
use std::thread;

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::RngExt;
use ultima_db::{Store, StoreConfig};
use ultima_vector::{Cosine, HnswParams, VectorCollection};

const DIM: usize = 8;
const SEED_N: usize = 200;
const ADD_N: usize = 200;
const READERS: usize = 4;

fn random_unit_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0f32..1.0)).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in &mut v {
        *x /= norm;
    }
    v
}

#[test]
fn readers_see_stable_snapshot_under_writer() {
    let store = Store::new(StoreConfig::default()).unwrap();
    let coll: VectorCollection<u64, Cosine> = VectorCollection::open(
        store.clone(),
        "vec",
        HnswParams::for_dim(DIM),
        Cosine,
    )
    .unwrap();

    // Seed.
    let mut rng = StdRng::seed_from_u64(0xACE);
    let seed_items: Vec<(Vec<f32>, u64)> = (0..SEED_N)
        .map(|i| (random_unit_vec(&mut rng, DIM), i as u64))
        .collect();
    coll.bulk_insert(seed_items).unwrap();
    let baseline_version = store.begin_read(None).unwrap().version();

    // Capture a query and the expected top-1 ids at the baseline snapshot.
    let queries: Vec<Vec<f32>> = (0..16).map(|_| random_unit_vec(&mut rng, DIM)).collect();
    let coll_arc = Arc::new(coll);

    let baseline_results: Vec<u64> = queries
        .iter()
        .map(|q| coll_arc.search(q, 1, None, None).unwrap()[0].0)
        .collect();

    // Spawn writer that adds more vectors after readers attach to baseline.
    let writer_store = store.clone();
    let writer_coll = Arc::clone(&coll_arc);
    let writer = thread::spawn(move || {
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        let items: Vec<(Vec<f32>, u64)> = (0..ADD_N)
            .map(|i| (random_unit_vec(&mut rng, DIM), (SEED_N + i) as u64))
            .collect();
        let mut tx = writer_store.begin_write(None).unwrap();
        for (e, m) in items {
            writer_coll.upsert_in(&mut tx, e, m).unwrap();
        }
        tx.commit().unwrap();
    });

    // Spawn readers. Each reader opens its OWN ReadTx pinned at baseline_version.
    let mut handles = Vec::new();
    for _ in 0..READERS {
        let store = store.clone();
        let coll = Arc::clone(&coll_arc);
        let queries = queries.clone();
        let baseline_results = baseline_results.clone();
        handles.push(thread::spawn(move || {
            let tx = store.begin_read(Some(baseline_version)).unwrap();
            for (q, expected) in queries.iter().zip(baseline_results.iter()) {
                let res = coll.search_in(&tx, q, 1, None, None).unwrap();
                assert_eq!(res[0].0, *expected, "snapshot read disagreed with baseline");
            }
        }));
    }

    writer.join().unwrap();
    for h in handles {
        h.join().unwrap();
    }
}
