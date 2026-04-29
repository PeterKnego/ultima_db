//! Metadata filtering: define a secondary index on a metadata field,
//! resolve a predicate to a `RoaringTreemap`, and pass it to `search`.
//!
//!     cargo run --example filtered_search -p ultima-vector

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::RngExt;
use ultima_db::{IndexKind, Store, StoreConfig};
use ultima_vector::filter::{from_id_record_pairs, RoaringTreemap};
use ultima_vector::row::VectorRow;
use ultima_vector::{Cosine, HnswParams, VectorCollection};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
struct DocMeta {
    title: String,
    category: String,
}

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

    let store = Store::new(StoreConfig::default()).unwrap();
    let coll: VectorCollection<DocMeta, Cosine> =
        VectorCollection::open(store.clone(), "docs", HnswParams::for_dim(DIM), Cosine).unwrap();

    // Define a non-unique index on the `category` field of the metadata.
    // Indexes live on the underlying data table (named "<base>_data").
    {
        let mut tx = store.begin_write(None).unwrap();
        let mut data = tx
            .open_table::<VectorRow<DocMeta>>(coll.data_table_name().as_str())
            .unwrap();
        data.define_index::<String>("by_category", IndexKind::NonUnique, |row| {
            row.meta.category.clone()
        })
        .unwrap();
        drop(data);
        tx.commit().unwrap();
    }

    // Insert a mix of documents across two categories.
    let mut rng = StdRng::seed_from_u64(0xCAFE);
    let categories = ["news", "sports"];
    for i in 0..400 {
        let category = categories[i % 2].to_string();
        coll.upsert(
            random_unit_vec(&mut rng, DIM),
            DocMeta {
                title: format!("doc-{i}"),
                category,
            },
        )
        .unwrap();
    }

    // Resolve the filter predicate (`category == "sports"`) to a bitmap
    // by querying the secondary index.
    let filter: RoaringTreemap = {
        let tx = store.begin_read(None).unwrap();
        let data = tx
            .open_table::<VectorRow<DocMeta>>(coll.data_table_name().as_str())
            .unwrap();
        let pairs = data
            .get_by_index::<String>("by_category", &"sports".to_string())
            .unwrap();
        from_id_record_pairs(pairs)
    };
    println!("filter cardinality (sports docs): {}", filter.len());

    // Search restricted to the "sports" category.
    let query = random_unit_vec(&mut rng, DIM);
    let top = coll.search(&query, 5, Some(&filter), None).unwrap();
    println!("top-5 sports docs near random query:");
    for (id, dist) in top {
        let tx = store.begin_read(None).unwrap();
        let data = tx
            .open_table::<VectorRow<DocMeta>>(coll.data_table_name().as_str())
            .unwrap();
        let row = data.get(id).unwrap();
        println!(
            "  id={id:>4} dist={dist:.4} title={} category={}",
            row.meta.title, row.meta.category
        );
    }
}
