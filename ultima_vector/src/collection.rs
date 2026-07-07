// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Public façade: `VectorCollection<Meta, D>` ties an UltimaDB `Store` to
//! an HNSW graph stored in two tables (`<name>_data`, `<name>_entry`).

use std::marker::PhantomData;

use roaring::RoaringTreemap;
use ultima_db::{ReadTx, Record, Store, WriteTx};

use crate::Distance;
use crate::error::{Error, Result};
use crate::hnsw::params::HnswParams;
use crate::hnsw::{insert as hnsw_insert, search as hnsw_search};
use crate::row::{EntryPoint, VectorRow};

pub struct VectorCollection<Meta, D> {
    store: Store,
    base_name: String,
    params: HnswParams,
    distance: D,
    _phantom: PhantomData<Meta>,
}

impl<Meta, D> VectorCollection<Meta, D>
where
    Meta: Record + Clone,
    D: Distance,
{
    /// Open or create a collection with the given parameters.
    ///
    /// Tables are created lazily on first write; opening a collection on a
    /// fresh store does not allocate any table state.
    pub fn open(store: Store, name: &str, params: HnswParams, distance: D) -> Result<Self> {
        if params.dim == 0 {
            return Err(Error::InvalidParams("dim must be > 0"));
        }
        if params.m < 2 {
            return Err(Error::InvalidParams("m must be >= 2"));
        }
        if params.m_max0 < params.m {
            return Err(Error::InvalidParams("m_max0 must be >= m"));
        }
        if params.ef_construction == 0 {
            return Err(Error::InvalidParams("ef_construction must be > 0"));
        }
        if params.ef_search_default == 0 {
            return Err(Error::InvalidParams("ef_search_default must be > 0"));
        }

        // When the persistence feature is on, both backing tables must be
        // registered with the store's type registry so the WAL/checkpoint
        // machinery knows how to (de)serialize their rows. Idempotent —
        // calling `open` repeatedly on the same store is fine.
        #[cfg(feature = "persistence")]
        {
            let data_name = format!("{name}_data");
            let entry_name = format!("{name}_entry");
            store.register_table::<VectorRow<Meta>>(&data_name)?;
            store.register_table::<EntryPoint>(&entry_name)?;
        }

        Ok(Self {
            store,
            base_name: name.to_string(),
            params,
            distance,
            _phantom: PhantomData,
        })
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn params(&self) -> &HnswParams {
        &self.params
    }

    /// Name of the underlying UltimaDB data table. Useful when defining
    /// secondary indexes on the metadata for filtered search.
    pub fn data_table_name(&self) -> String {
        format!("{}_data", self.base_name)
    }

    /// Name of the underlying entry-point singleton table.
    pub fn entry_table_name(&self) -> String {
        format!("{}_entry", self.base_name)
    }

    fn data_name(&self) -> String {
        self.data_table_name()
    }

    fn entry_name(&self) -> String {
        self.entry_table_name()
    }

    /// Insert a single vector in its own write transaction.
    pub fn upsert(&self, embedding: Vec<f32>, meta: Meta) -> Result<u64> {
        let mut tx = self.store.begin_write(None)?;
        let id = self.upsert_in(&mut tx, embedding, meta)?;
        tx.commit()?;
        Ok(id)
    }

    /// Insert a vector under an existing write transaction (composes with
    /// caller-driven txs).
    pub fn upsert_in(&self, tx: &mut WriteTx, embedding: Vec<f32>, meta: Meta) -> Result<u64> {
        // Per-call RNG seeded from system entropy via the thread-local RNG.
        // Tests that need determinism use the lower-level `hnsw::insert::insert`
        // directly with a seeded RNG.
        let mut rng = rand::rng();
        hnsw_insert::insert(
            tx,
            &self.data_name(),
            &self.entry_name(),
            &self.distance,
            embedding,
            meta,
            &self.params,
            &mut rng,
        )
    }

    /// Search top-K nearest neighbors. `ef` overrides
    /// `params.ef_search_default` if provided.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&RoaringTreemap>,
        ef: Option<usize>,
    ) -> Result<Vec<(u64, f32)>> {
        let tx = self.store.begin_read(None)?;
        self.search_in(&tx, query, k, filter, ef)
    }

    /// Insert many vectors in a single write transaction. Faster than
    /// looping over `upsert` because there's only one commit at the end.
    /// The graph is built incrementally across the items just like with
    /// `upsert_in` — atomicity is the only guarantee, not parallel build.
    pub fn bulk_insert<I>(&self, items: I) -> Result<Vec<u64>>
    where
        I: IntoIterator<Item = (Vec<f32>, Meta)>,
    {
        let mut tx = self.store.begin_write(None)?;
        let mut ids = Vec::new();
        for (e, m) in items {
            ids.push(self.upsert_in(&mut tx, e, m)?);
        }
        tx.commit()?;
        Ok(ids)
    }

    /// Atomically replace this collection's contents with the provided rows
    /// and entry point. Existing rows in both backing tables are dropped;
    /// pre-existing `ReadTx`s on prior snapshots continue to see the old
    /// state via MVCC.
    ///
    /// Validates per-row that `embedding.len() == params.dim` and
    /// `HnswState::layers_len() == level + 1`. Does not validate neighbor-id
    /// integrity — bad refs surface as `NodeNotFound` at search time.
    ///
    /// `HnswState`'s safe constructors enforce the layer-count invariant; the
    /// check guards against malformed deserialization (e.g. a corrupted
    /// backup file).
    ///
    /// Returns the new committed snapshot version.
    pub fn restore_iter<I>(&self, rows: I, entry_point: EntryPoint) -> Result<u64>
    where
        I: IntoIterator<Item = (u64, VectorRow<Meta>)>,
    {
        use ultima_db::{AddOptions, BulkLoadInput, BulkLoadOptions, BulkSource};

        let mut materialized: Vec<(u64, VectorRow<Meta>)> = Vec::new();
        for (id, row) in rows {
            if row.embedding.len() != self.params.dim {
                return Err(Error::DimMismatch {
                    expected: self.params.dim,
                    got: row.embedding.len(),
                });
            }
            let level = row.hnsw.level();
            let layers = row.hnsw.layers_len();
            if layers != usize::from(level) + 1 {
                return Err(Error::InvalidHnswState { id, level, layers });
            }
            materialized.push((id, row));
        }

        let data_input = BulkLoadInput::Replace(BulkSource::unsorted_vec(materialized));
        let entry_input = BulkLoadInput::Replace(BulkSource::sorted_vec(vec![(1, entry_point)]));

        let mut batch = self.store.bulk_load_batch();
        batch.add::<VectorRow<Meta>>(&self.data_table_name(), data_input, AddOptions::default())?;
        batch.add::<EntryPoint>(&self.entry_table_name(), entry_input, AddOptions::default())?;
        let v = batch.commit(BulkLoadOptions::default())?;
        Ok(v)
    }

    /// Convenience over [`restore_iter`](Self::restore_iter) for in-memory `Vec` input.
    pub fn restore_vec(
        &self,
        rows: Vec<(u64, VectorRow<Meta>)>,
        entry_point: EntryPoint,
    ) -> Result<u64> {
        self.restore_iter(rows, entry_point)
    }

    /// Delete a node by tombstoning it. Subsequent searches skip it.
    pub fn delete(&self, id: u64) -> Result<()> {
        let mut tx = self.store.begin_write(None)?;
        self.delete_in(&mut tx, id)?;
        tx.commit()?;
        Ok(())
    }

    pub fn delete_in(&self, tx: &mut WriteTx, id: u64) -> Result<()> {
        hnsw_insert::delete::<Meta>(tx, &self.data_name(), &self.entry_name(), id)
    }

    /// Replace the embedding at a stable id, preserving the metadata.
    pub fn update_embedding(&self, id: u64, embedding: Vec<f32>) -> Result<()> {
        let mut tx = self.store.begin_write(None)?;
        self.update_embedding_in(&mut tx, id, embedding)?;
        tx.commit()?;
        Ok(())
    }

    pub fn update_embedding_in(
        &self,
        tx: &mut WriteTx,
        id: u64,
        embedding: Vec<f32>,
    ) -> Result<()> {
        let mut rng = rand::rng();
        hnsw_insert::update_embedding::<Meta, D, _>(
            tx,
            &self.data_name(),
            &self.entry_name(),
            &self.distance,
            id,
            embedding,
            &self.params,
            &mut rng,
        )
    }

    pub fn search_in(
        &self,
        tx: &ReadTx,
        query: &[f32],
        k: usize,
        filter: Option<&RoaringTreemap>,
        ef: Option<usize>,
    ) -> Result<Vec<(u64, f32)>> {
        if query.len() != self.params.dim {
            return Err(Error::DimMismatch {
                expected: self.params.dim,
                got: query.len(),
            });
        }
        let ef = ef.unwrap_or(self.params.ef_search_default);
        hnsw_search::search::<Meta, D>(
            tx,
            &self.data_name(),
            &self.entry_name(),
            &self.distance,
            query,
            k,
            ef,
            filter,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Cosine;
    use crate::hnsw::params::HnswParams;
    use ultima_db::{Store, StoreConfig};

    #[test]
    fn search_on_empty_collection_returns_empty() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let res = coll.search(&[1.0, 0.0], 5, None, None).unwrap();
        assert!(res.is_empty());
    }

    #[test]
    fn first_insert_creates_entry_point() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let id = coll.upsert(vec![1.0, 0.0], 7u64).unwrap();
        assert_eq!(id, 1, "first auto-id is 1");

        let res = coll.search(&[1.0, 0.0], 5, None, None).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0, id);
        assert!((res[0].1 - 0.0).abs() < 1e-5);
    }

    #[test]
    fn search_ranks_by_distance() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let near = coll.upsert(vec![1.0, 0.0], 1).unwrap();
        let _far = coll.upsert(vec![0.0, 1.0], 2).unwrap();
        let mid = coll.upsert(vec![1.0, 0.5], 3).unwrap();

        let res = coll.search(&[1.0, 0.05], 3, None, None).unwrap();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0].0, near);
        assert_eq!(res[1].0, mid);
    }

    #[test]
    fn delete_removes_from_search_results() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let a = coll.upsert(vec![1.0, 0.0], 1).unwrap();
        let b = coll.upsert(vec![0.0, 1.0], 2).unwrap();
        let c = coll.upsert(vec![0.7, 0.7], 3).unwrap();

        coll.delete(a).unwrap();
        let res = coll.search(&[1.0, 0.0], 5, None, None).unwrap();
        let ids: Vec<u64> = res.iter().map(|(id, _)| *id).collect();
        assert!(
            !ids.contains(&a),
            "deleted id {a} should not appear in results"
        );
        assert!(ids.contains(&b));
        assert!(ids.contains(&c));
    }

    #[test]
    fn delete_is_idempotent() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let a = coll.upsert(vec![1.0, 0.0], 1).unwrap();
        coll.delete(a).unwrap();
        coll.delete(a).unwrap(); // second delete is a no-op
    }

    #[test]
    fn delete_unknown_id_errors() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        coll.upsert(vec![1.0, 0.0], 1).unwrap();
        let err = coll.delete(999).unwrap_err();
        assert!(matches!(err, crate::Error::NodeNotFound(999)));
    }

    #[test]
    fn delete_entry_point_picks_replacement() {
        // Insert two nodes; delete whichever is currently the entry point;
        // search must still work and return the remaining node.
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let a = coll.upsert(vec![1.0, 0.0], 1).unwrap();
        let b = coll.upsert(vec![0.0, 1.0], 2).unwrap();
        coll.delete(a).unwrap();
        let res = coll.search(&[0.0, 1.0], 5, None, None).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0, b);
    }

    #[test]
    fn update_embedding_changes_search_ranking_at_stable_id() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let id = coll.upsert(vec![1.0, 0.0], 42).unwrap();
        coll.upsert(vec![0.0, 1.0], 43).unwrap();
        coll.upsert(vec![1.0, 1.0], 44).unwrap();

        // Move id from (1,0) to (0,1).
        coll.update_embedding(id, vec![0.0, 1.0]).unwrap();

        // Query close to the new location — id should now appear near the top.
        let res = coll.search(&[0.0, 1.0], 1, None, None).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0, id, "id should match new embedding location");
    }

    /// Updating the embedding of the node that happens to be the HNSW entry
    /// point must keep the graph connected. The update resets the node's
    /// adjacency and rebuilds it — if the rebuild is skipped because the
    /// node *is* the entry point, every subsequent search greedy-descends
    /// from a node with no out-edges and returns only that node.
    #[test]
    fn update_embedding_of_entry_point_keeps_graph_connected() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store.clone(), "vec", HnswParams::for_dim(2), Cosine).unwrap();

        // A spread of vectors around the unit circle.
        let n = 21u64;
        for i in 0..n {
            let theta = (i as f32) * 0.25;
            coll.upsert(vec![theta.cos(), theta.sin()], i).unwrap();
        }

        // Identify the current entry point.
        let ep_id = {
            let rtx = store.begin_read(None).unwrap();
            let entry = rtx.open_table::<EntryPoint>("vec_entry").unwrap();
            entry.get(1).unwrap().node_id.unwrap()
        };

        // Move the entry point's embedding.
        coll.update_embedding(ep_id, vec![-1.0, 0.0]).unwrap();

        // The rest of the graph must still be reachable.
        let res = coll.search(&[1.0, 0.0], 10, None, None).unwrap();
        assert_eq!(
            res.len(),
            10,
            "graph disconnected after updating the entry point's embedding"
        );

        // And the moved node itself is findable at its new location.
        let res = coll.search(&[-1.0, 0.0], 1, None, None).unwrap();
        assert_eq!(res[0].0, ep_id, "moved entry point not found at new location");
    }

    /// Updating the sole node in a collection (necessarily the entry point)
    /// must keep it searchable — there is nothing to reconnect to.
    #[test]
    fn update_embedding_of_sole_node_stays_searchable() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let id = coll.upsert(vec![1.0, 0.0], 1).unwrap();
        coll.update_embedding(id, vec![0.0, 1.0]).unwrap();

        let res = coll.search(&[0.0, 1.0], 5, None, None).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0, id);
    }

    /// Updating the entry point when every other node is tombstoned behaves
    /// like the sole-node case.
    #[test]
    fn update_embedding_of_entry_point_with_only_tombstoned_others() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store.clone(), "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let a = coll.upsert(vec![1.0, 0.0], 1).unwrap();
        let b = coll.upsert(vec![0.0, 1.0], 2).unwrap();

        // Delete whichever is NOT the entry point, then update the EP.
        let ep_id = {
            let rtx = store.begin_read(None).unwrap();
            let entry = rtx.open_table::<EntryPoint>("vec_entry").unwrap();
            entry.get(1).unwrap().node_id.unwrap()
        };
        let other = if ep_id == a { b } else { a };
        coll.delete(other).unwrap();
        coll.update_embedding(ep_id, vec![0.7, 0.7]).unwrap();

        let res = coll.search(&[0.7, 0.7], 5, None, None).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0, ep_id);
    }

    /// A tombstoned node referenced as the entry point (a MultiWriter
    /// delete race or a stale restore can produce this) must never appear
    /// in search results — but the graph must still be reachable through
    /// its out-edges.
    #[test]
    fn search_excludes_tombstoned_entry_point_node() {
        use crate::row::HnswState;

        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();

        let mut dead = HnswState::empty(0);
        dead.set_neighbors(0, vec![2]);
        dead.tombstone();
        let mut live = HnswState::empty(0);
        live.set_neighbors(0, vec![1]);

        let rows = vec![
            (
                1,
                VectorRow {
                    embedding: vec![1.0, 0.0],
                    meta: 1u64,
                    hnsw: dead,
                },
            ),
            (
                2,
                VectorRow {
                    embedding: vec![0.0, 1.0],
                    meta: 2u64,
                    hnsw: live,
                },
            ),
        ];
        coll.restore_vec(
            rows,
            EntryPoint {
                node_id: Some(1),
                max_level: 0,
            },
        )
        .unwrap();

        // Query right at the tombstoned node's position — strongest pull.
        let res = coll.search(&[1.0, 0.0], 5, None, None).unwrap();
        let ids: Vec<u64> = res.iter().map(|r| r.0).collect();
        assert!(
            !ids.contains(&1),
            "tombstoned entry point returned from search: {ids:?}"
        );
        assert_eq!(ids, vec![2], "live nodes must stay reachable through it");
    }

    #[test]
    fn bulk_insert_returns_one_id_per_item() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let items = vec![
            (vec![1.0, 0.0], 1u64),
            (vec![0.0, 1.0], 2),
            (vec![1.0, 1.0], 3),
        ];
        let ids = coll.bulk_insert(items).unwrap();
        assert_eq!(ids.len(), 3);
        // Searching after bulk insert finds them.
        let res = coll.search(&[1.0, 0.0], 3, None, None).unwrap();
        assert_eq!(res.len(), 3);
    }

    #[test]
    fn bulk_insert_dim_mismatch_aborts_whole_batch() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let items = vec![
            (vec![1.0, 0.0], 1u64),
            (vec![1.0, 0.0, 0.0], 2), // wrong dim
        ];
        let err = coll.bulk_insert(items).unwrap_err();
        assert!(matches!(err, crate::Error::DimMismatch { .. }));
        // Tx was dropped without commit, so the first row is also gone.
        let res = coll.search(&[1.0, 0.0], 3, None, None).unwrap();
        assert!(res.is_empty(), "failed bulk_insert must roll back");
    }

    #[test]
    fn upsert_rejects_non_finite_embedding() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let err = coll.upsert(vec![1.0, bad], 7u64).unwrap_err();
            assert!(
                matches!(err, crate::Error::NonFinite { index: 1, .. }),
                "expected NonFinite at index 1, got {err:?}"
            );
        }
        // Nothing was inserted.
        let res = coll.search(&[1.0, 0.0], 5, None, None).unwrap();
        assert!(res.is_empty());
    }

    #[test]
    fn bulk_insert_non_finite_aborts_whole_batch() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let items = vec![
            (vec![1.0, 0.0], 1u64),
            (vec![f32::NAN, 0.0], 2), // poison row
        ];
        let err = coll.bulk_insert(items).unwrap_err();
        assert!(matches!(err, crate::Error::NonFinite { index: 0, .. }));
        // Tx was dropped without commit, so the first row is also gone.
        let res = coll.search(&[1.0, 0.0], 3, None, None).unwrap();
        assert!(res.is_empty(), "failed bulk_insert must roll back");
    }

    #[test]
    fn update_embedding_rejects_non_finite_and_keeps_old() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let id = coll.upsert(vec![1.0, 0.0], 42u64).unwrap();

        let err = coll
            .update_embedding(id, vec![f32::NEG_INFINITY, 0.0])
            .unwrap_err();
        assert!(matches!(err, crate::Error::NonFinite { index: 0, .. }));

        // Old embedding is intact and searchable at its old location.
        let res = coll.search(&[1.0, 0.0], 1, None, None).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0, id);
        assert!((res[0].1 - 0.0).abs() < 1e-5);
    }

    #[test]
    fn dim_mismatch_is_rejected() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(3), Cosine).unwrap();
        let err = coll.upsert(vec![1.0, 0.0], 7u64).unwrap_err();
        match err {
            crate::Error::DimMismatch { expected, got } => {
                assert_eq!(expected, 3);
                assert_eq!(got, 2);
            }
            other => panic!("expected DimMismatch, got {other:?}"),
        }
    }
}
