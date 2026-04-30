//! Restore a vector collection from previously-built rows.
//!
//! Run: `cargo run -p ultima-vector --example bulk_restore`

use ultima_db::Store;
use ultima_vector::{VectorCollection, hnsw::params::HnswParams};
use ultima_vector::distance::Cosine;
use ultima_vector::row::{EntryPoint, VectorRow};

fn main() {
    // 1. Build a "source" collection the normal way.
    let store_a = Store::default();
    let coll_a: VectorCollection<u64, Cosine> =
        VectorCollection::open(store_a.clone(), "v", HnswParams::for_dim(8), Cosine).unwrap();

    let n = 1000;
    for i in 0..n as u64 {
        let mut e = vec![0.0_f32; 8];
        e[(i as usize) % 8] = 1.0 + (i as f32 * 0.001);
        coll_a.upsert(e, i).unwrap();
    }
    println!("Source collection built: {n} rows");

    // 2. Capture rows + entry point.
    let rtx = store_a.begin_read(None).unwrap();
    let data_name = coll_a.data_table_name();
    let entry_name = coll_a.entry_table_name();
    let data_t = rtx.open_table::<VectorRow<u64>>(data_name.as_str()).unwrap();
    let entry_t = rtx.open_table::<EntryPoint>(entry_name.as_str()).unwrap();
    let rows: Vec<(u64, VectorRow<u64>)> =
        data_t.iter().map(|(id, r)| (id, r.clone())).collect();
    let ep = entry_t.get(1).cloned().unwrap_or_default();
    drop(rtx);
    println!("Captured {} rows + entry-point id={:?}", rows.len(), ep.node_id);

    // 3. Restore into a fresh store + collection.
    let t0 = std::time::Instant::now();
    let store_b = Store::default();
    let coll_b: VectorCollection<u64, Cosine> =
        VectorCollection::open(store_b, "v", HnswParams::for_dim(8), Cosine).unwrap();
    coll_b.restore_vec(rows, ep).unwrap();
    println!("Restore took {:.2}ms", t0.elapsed().as_secs_f64() * 1000.0);

    // 4. Same query, same top-3 ids in both collections.
    let q = vec![1.0_f32, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    let a = coll_a.search(&q, 3, None, None).unwrap();
    let b = coll_b.search(&q, 3, None, None).unwrap();
    let a_ids: Vec<u64> = a.iter().map(|(i, _)| *i).collect();
    let b_ids: Vec<u64> = b.iter().map(|(i, _)| *i).collect();
    println!("Source top-3: {a_ids:?}");
    println!("Restored top-3: {b_ids:?}");
    assert_eq!(a_ids, b_ids);
    println!("Round-trip verified.");
}
