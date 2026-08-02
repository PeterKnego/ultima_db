# How to add vector search to your data

If you already run UltimaDB in-process and need top-k similarity search
over embeddings, use the companion crate `ultima-vector` — an HNSW index
stored in ordinary UltimaDB tables, so it shares the store's transactions,
snapshots, and (optionally) persistence.

## Add the dependency

```toml
[dependencies]
ultima-db = "0.3"
ultima-vector = "0.3"
```

If the collection must survive restarts, enable persistence on the vector
crate — its `persistence` feature forwards to `ultima-db/persistence`:

```toml
ultima-vector = { version = "0.3", features = ["persistence"] }
```

## Create a collection

A `VectorCollection<Meta, D>` pairs each embedding with a caller-defined
metadata payload `Meta` and a distance metric `D`, fixed at open time:

```rust
use ultima_db::Store;
use ultima_vector::{Cosine, HnswParams, VectorCollection};

let store = Store::default();
let coll: VectorCollection<String, Cosine> =
    VectorCollection::open(store.clone(), "docs", HnswParams::for_dim(768), Cosine)?;
```

Pick the metric your embeddings were trained for: `Cosine`, `L2` (squared
Euclidean), or `DotProduct` (negated so smaller-is-closer holds for all
metrics). If every vector you store *and* query is already unit-length,
`CosineNormalized` skips the norm computation — normalize inputs first
with the public helpers:

```rust
use ultima_vector::{normalize_in_place, normalize_many};

normalize_in_place(&mut query);
normalize_many(&mut embeddings);
```

## Insert and search

```rust
let id = coll.upsert(embedding, "doc-a".to_string())?;

// (id, distance) pairs, closest first.
let hits: Vec<(u64, f32)> = coll.search(&query, 10, None, None)?;
```

For many inserts, `bulk_insert` commits one transaction for the whole
batch; a failing row aborts and rolls back the entire batch. Both paths
validate input at the boundary: a wrong-length vector is rejected with
`Error::DimMismatch`, and NaN/±Inf values with `Error::NonFinite` — on
insert, update, search, and restore alike.

`upsert_in`, `delete_in`, and `search_in` take an existing transaction if
vector changes must commit atomically with your other table writes.

## Filter by metadata

`search` takes an optional `RoaringTreemap` of allowed ids. Resolve your
predicate to a bitmap however you like; the natural way is a secondary
index on the collection's data table (named `<collection>_data`, rows are
`VectorRow<Meta>`):

```rust
use ultima_db::IndexKind;
use ultima_vector::filter::from_id_record_pairs;
use ultima_vector::row::VectorRow;

// Once: index the metadata field.
let mut tx = store.begin_write(None)?;
let mut data = tx.open_table::<VectorRow<DocMeta>>(coll.data_table_name().as_str())?;
data.define_index::<String>("by_category", IndexKind::NonUnique, |row| {
    row.meta.category.clone()
})?;
drop(data);
tx.commit()?;

// Per query: predicate -> bitmap -> filtered search.
let filter = {
    let tx = store.begin_read(None)?;
    let data = tx.open_table::<VectorRow<DocMeta>>(coll.data_table_name().as_str())?;
    from_id_record_pairs(data.get_by_index::<String>("by_category", &"sports".to_string())?)
};
let hits = coll.search(&query, 10, Some(&filter), None)?;
```

See [How to find records by fields other than the primary
key](query-with-indexes.md) for the index API.

## Tune recall vs cost

Start from `HnswParams::for_dim(dim)` and override fields as needed. The
two levers that matter first: `ef_search_default` (or the per-query `ef`
argument to `search`) trades query latency for recall, and
`ef_construction`/`m` trade build time and memory for graph quality.

```rust
let mut params = HnswParams::for_dim(768);
params.ef_search_default = 100;

// Or per query, without reopening the collection:
let hits = coll.search(&query, 10, None, Some(200))?;
```

## Persist and restore

With the `persistence` feature, `VectorCollection::open` registers the
collection's backing tables with the store, so open the collection before
calling `Store::recover()` — then the usual WAL/checkpoint flow applies
(see [How to set up durable persistence](set-up-durable-persistence.md)).

To rebuild a collection from captured rows — a backup, or a snapshot
shipped from another node — use the atomic restore path instead of
re-inserting, which would rebuild the HNSW graph vector by vector:

```rust
use ultima_vector::row::{EntryPoint, VectorRow};

// Capture rows + entry point from the source collection's tables...
let rows: Vec<(u64, VectorRow<String>)> = /* data table contents */;
let entry: EntryPoint = /* entry table row */;

// ...and install them wholesale; readers on older snapshots are unaffected.
coll.restore_vec(rows, entry)?;
```

`restore_iter` does the same from any iterator. See
`examples/bulk_restore.rs` in the `ultima_vector` crate for the full
capture-and-restore round trip.
