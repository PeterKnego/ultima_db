//! Integration tests for VectorCollection restore.

use ultima_db::Store;
use ultima_vector::distance::Cosine;
use ultima_vector::hnsw::params::HnswParams;
use ultima_vector::row::{EntryPoint, VectorRow};
use ultima_vector::VectorCollection;

fn open_coll(store: Store) -> VectorCollection<u64, Cosine> {
    VectorCollection::open(store, "v", HnswParams::for_dim(4), Cosine).unwrap()
}

#[test]
fn restore_round_trip_preserves_search_results() {
    // Build a collection of small vectors via normal upsert.
    let store_a = Store::default();
    let coll_a = open_coll(store_a.clone());
    let queries: Vec<Vec<f32>> = vec![
        vec![1.0, 0.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0, 0.0],
        vec![0.0, 0.0, 1.0, 0.0],
        vec![0.5, 0.5, 0.0, 0.0],
        vec![0.0, 0.5, 0.5, 0.0],
    ];
    for (i, e) in queries.iter().enumerate() {
        coll_a.upsert(e.clone(), i as u64).unwrap();
    }
    let q = vec![0.9, 0.1, 0.0, 0.0];
    let expected = coll_a.search(&q, 3, None, None).unwrap();

    // Capture all VectorRows + the EntryPoint by reading the underlying tables.
    let rtx = store_a.begin_read(None).unwrap();
    let data_name = coll_a.data_table_name();
    let entry_name = coll_a.entry_table_name();
    let data_t = rtx
        .open_table::<VectorRow<u64>>(data_name.as_str())
        .unwrap();
    let entry_t = rtx
        .open_table::<EntryPoint>(entry_name.as_str())
        .unwrap();
    let rows: Vec<(u64, VectorRow<u64>)> =
        data_t.iter().map(|(id, r)| (id, r.clone())).collect();
    let ep = entry_t.get(1).cloned().unwrap_or_default();
    drop(rtx);

    // Restore into a fresh store + new collection.
    let store_b = Store::default();
    let coll_b = open_coll(store_b);
    coll_b.restore_vec(rows, ep).unwrap();

    // Same query → same top-k.
    let actual = coll_b.search(&q, 3, None, None).unwrap();
    let actual_ids: Vec<u64> = actual.iter().map(|(id, _)| *id).collect();
    let expected_ids: Vec<u64> = expected.iter().map(|(id, _)| *id).collect();
    assert_eq!(actual_ids, expected_ids);
}
