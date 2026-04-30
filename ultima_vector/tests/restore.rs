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

#[test]
fn restore_replace_drops_old_data() {
    let store = Store::default();
    let coll = open_coll(store.clone());

    // Seed with 3 vectors.
    for i in 0..3u64 {
        coll.upsert(vec![i as f32, 0.0, 0.0, 0.0], i).unwrap();
    }
    // Restore into the *same* collection with completely different content.
    let new_rows: Vec<(u64, VectorRow<u64>)> = vec![(
        100,
        VectorRow {
            embedding: vec![5.0, 0.0, 0.0, 0.0],
            meta: 100,
            hnsw: ultima_vector::row::HnswState::empty(0),
        },
    )];
    let ep = EntryPoint { node_id: Some(100), max_level: 0 };
    coll.restore_vec(new_rows, ep).unwrap();

    // Old IDs are gone.
    let rtx = store.begin_read(None).unwrap();
    let data_name = coll.data_table_name();
    let t = rtx
        .open_table::<VectorRow<u64>>(data_name.as_str())
        .unwrap();
    assert!(t.get(1).is_none());
    assert!(t.get(100).is_some());
    assert_eq!(t.len(), 1);
}

#[test]
fn restore_dim_mismatch_errors_before_install() {
    use ultima_vector::error::Error;

    let store = Store::default();
    let coll = open_coll(store.clone());
    let v_before = store.latest_version();

    let bad = vec![(
        1u64,
        VectorRow {
            embedding: vec![1.0, 0.0], // dim=2, but params.dim=4
            meta: 1,
            hnsw: ultima_vector::row::HnswState::empty(0),
        },
    )];
    let res = coll.restore_vec(bad, EntryPoint::default());
    assert!(matches!(
        res,
        Err(Error::DimMismatch { expected: 4, got: 2 })
    ));
    assert_eq!(
        store.latest_version(),
        v_before,
        "store must be unchanged on validation failure"
    );
}

#[test]
fn restore_duplicate_ids_errors() {
    use ultima_db::Error as DbError;
    use ultima_vector::error::Error;

    let store = Store::default();
    let coll = open_coll(store.clone());
    let v_before = store.latest_version();

    let dup: Vec<(u64, VectorRow<u64>)> = vec![
        (
            1,
            VectorRow {
                embedding: vec![1.0, 0.0, 0.0, 0.0],
                meta: 1,
                hnsw: ultima_vector::row::HnswState::empty(0),
            },
        ),
        (
            1,
            VectorRow {
                embedding: vec![2.0, 0.0, 0.0, 0.0],
                meta: 2,
                hnsw: ultima_vector::row::HnswState::empty(0),
            },
        ),
    ];
    let res = coll.restore_vec(dup, EntryPoint::default());
    assert!(matches!(
        res,
        Err(Error::Storage(DbError::InvalidBulkLoadInput(_))),
    ));
    assert_eq!(store.latest_version(), v_before);
}

#[test]
fn restore_empty_collection() {
    let store = Store::default();
    let coll = open_coll(store.clone());

    coll.restore_vec(vec![], EntryPoint::default()).unwrap();

    let rtx = store.begin_read(None).unwrap();
    let data_name = coll.data_table_name();
    let entry_name = coll.entry_table_name();
    let t = rtx
        .open_table::<VectorRow<u64>>(data_name.as_str())
        .unwrap();
    let e = rtx.open_table::<EntryPoint>(entry_name.as_str()).unwrap();
    assert_eq!(t.len(), 0);
    assert_eq!(e.get(1).cloned().unwrap_or_default().node_id, None);

    // Search returns nothing.
    let q = vec![1.0, 0.0, 0.0, 0.0];
    let results = coll.search(&q, 5, None, None).unwrap();
    assert!(results.is_empty());
}

#[test]
fn restore_concurrent_read_unaffected() {
    let store = Store::default();
    let coll = open_coll(store.clone());

    // Seed.
    for i in 0..3u64 {
        coll.upsert(vec![i as f32, 0.0, 0.0, 0.0], i).unwrap();
    }
    let v1 = store.latest_version();
    let rtx_pre = store.begin_read(Some(v1)).unwrap();

    // Restore into something completely different.
    let new_rows: Vec<(u64, VectorRow<u64>)> = vec![(
        50,
        VectorRow {
            embedding: vec![9.0, 0.0, 0.0, 0.0],
            meta: 50,
            hnsw: ultima_vector::row::HnswState::empty(0),
        },
    )];
    let ep = EntryPoint { node_id: Some(50), max_level: 0 };
    coll.restore_vec(new_rows, ep).unwrap();

    // Pre-restore reader still sees the seed.
    let data_name = coll.data_table_name();
    let t = rtx_pre
        .open_table::<VectorRow<u64>>(data_name.as_str())
        .unwrap();
    assert_eq!(t.len(), 3);
    assert!(t.get(50).is_none());
}
