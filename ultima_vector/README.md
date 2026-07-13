# ultima_vector

HNSW approximate-nearest-neighbor search built on
[UltimaDB](https://crates.io/crates/ultima-db)'s MVCC tables. A
`VectorCollection` stores embeddings plus caller-defined metadata rows and
answers top-k similarity queries, optionally filtered on that metadata —
useful when you already have UltimaDB in the process and want vector search
without standing up a separate service.

## Quick example

```rust
use ultima_db::{Store, StoreConfig};
use ultima_vector::{Cosine, HnswParams, VectorCollection};

let store = Store::new(StoreConfig::default())?;
let coll: VectorCollection<String, Cosine> =
    VectorCollection::open(store, "docs", HnswParams::for_dim(3), Cosine)?;

coll.upsert(vec![0.1, 0.2, 0.3], "doc-a".to_string())?;
coll.upsert(vec![0.9, 0.1, 0.0], "doc-b".to_string())?;

let hits = coll.search(&[0.1, 0.2, 0.25], 1, None, None)?;
assert_eq!(hits[0].0, 1); // id of the closest match, "doc-a"
# Ok::<(), ultima_vector::Error>(())
```

More in [`examples/`](examples/): quick search, filtered search, bulk
restore, transactional composition, and persistence.

## Distance metrics

`Cosine`, `L2` (squared Euclidean), `DotProduct` (negated, so
smaller-is-closer holds uniformly), and the opt-in `CosineNormalized` fast
path for pre-normalized vectors. Kernels are SIMD-accelerated
(AVX-512 / AVX2 / NEON, selected at runtime via `pulp`) with a scalar
fallback. Inputs are validated at the collection boundary: dimension
mismatches and non-finite values (NaN/±Inf) are rejected on insert, update,
search, and restore.

## Feature flags

- `persistence` — persist collections through UltimaDB's WAL / checkpoint
  machinery (`ultima-db/persistence`); bulk restores go through the
  MVCC-consistent restore path.

## License

Apache-2.0, part of the [`ultima_db`](https://github.com/PeterKnego/ultima_db)
workspace. See [LICENSE](../LICENSE) and [NOTICE](../NOTICE).
