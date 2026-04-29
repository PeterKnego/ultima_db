# Task 22 — `ultima_vector`: HNSW vector search on UltimaDB

A sibling crate `ultima_vector` that implements approximate nearest-neighbor
(ANN) search using HNSW, layered on top of UltimaDB's existing primitives.
It exposes a `VectorCollection<Meta, D>` API for in-process Rust use at
thousands-to-low-millions scale.

## Context

This is the first sub-project of a larger "vector DB on UltimaDB"
investigation. The full feature set (HNSW, hybrid retrieval, REST/gRPC
server, multi-modal data) decomposes into roughly four independent
sub-projects; this task covers only HNSW. Hybrid retrieval, network layer,
and any deeper UltimaDB-internal persistence hooks are deliberately out of
scope and tracked separately.

The framing question that opened the work was *"what is UltimaDB missing
to build a vector DB?"* The exploration found that UltimaDB already had
the structurally important pieces — public `CustomIndex<R>` trait,
re-exported `BTree<K, V>`, MVCC + multi-writer OCC, persistence (WAL +
checkpoints), an existing BM25 inverted index in `src/fulltext.rs` — so
the gap was substantially smaller than the feature list implied. The
remaining major work is HNSW itself plus a pre-filter integration.

## Decisions

- **Build HNSW from scratch**, CoW-friendly. Wrapping an existing crate
  (`instant-distance`, `hnsw_rs`) was rejected because their internal
  `Vec` / `HashMap` graph representations defeat MVCC at the index layer
  — every `WriteTx::commit` would have to clone the whole graph.
- **Architecture: graph denormalized into the data row.** Each
  `VectorRow<Meta>` carries `embedding`, `meta`, and `HnswState` (level,
  tombstone bit, per-layer adjacency lists). A second, singleton table
  holds the entry-point reference (top-of-graph node id + max level).
  Both tables are ordinary UltimaDB tables — they get O(1) snapshot
  clone, MVCC, multi-writer OCC, and persistence "for free." This made
  the planned sub-project D (custom-index persistence hook) unnecessary
  for this work.
- **No `CustomIndex<R>` impl is used.** The HNSW algorithm runs in
  user-space (`VectorCollection`) reading and writing the table directly.
  `CustomIndex` is the right tool when an *implicit* invariant must be
  maintained on every mutation; here the user always goes through
  `VectorCollection`, so explicit orchestration is simpler.
- **Filter integration: pre-filter via `RoaringTreemap`.** Caller
  resolves their metadata filter through ordinary UltimaDB indexes
  (`define_index`, `index_range`) and passes a u64 bitmap of allowed
  ids. Selected by recall under selective filters; the alternative
  (post-filter over-fetch) collapses recall when the filter is sharp.
  `roaring::RoaringTreemap` (u64) is used rather than `RoaringBitmap`
  (u32) since UltimaDB ids are u64.
- **Brute-force fallback** below `BRUTE_FORCE_THRESHOLD = 128`. HNSW is
  bypassed entirely under highly selective filters; the bitmap's ids are
  scanned linearly. Avoids HNSW's well-known recall collapse for sharp
  filters at the cost of a single linear pass over a small set.
- **Distance is a compile-time generic** with a `Distance` trait and
  three impls: `Cosine`, `L2` (squared), `DotProduct` (negated, so
  smaller-is-closer holds uniformly across all metrics).
- **Update at stable id is "lazy."** `update_embedding` rewrites the row
  in place with a fresh `HnswState` and re-runs the connection
  algorithm. Stale back-references on existing nodes at layers above
  the new level are left alone — they degrade to dead ends during
  traversal but don't break correctness. Cheaper than walking the whole
  table to strip references.
- **Crate boundary: sibling crate `ultima_vector`** in the same
  workspace. The repo was converted from a single-crate package to a
  workspace with `ultima-db` as both root crate and member, plus
  `ultima_vector/` as a sibling.

## Code map

```
ultima_vector/
  src/
    lib.rs                  re-exports
    collection.rs           VectorCollection<Meta, D> public API
    distance.rs             Distance trait + Cosine, L2, DotProduct
    error.rs                Error / Result
    filter.rs               RoaringTreemap helpers
    row.rs                  VectorRow<Meta>, HnswState, EntryPoint
    hnsw/
      mod.rs                NodeAccess trait, Candidate
      params.rs             HnswParams (m, m_max0, ef_construction, ef_search_default, max_level)
      level.rs              LevelSampler — geometric in m, capped at max_level
      search.rs             search_layer, search, brute_force_filtered, BRUTE_FORCE_THRESHOLD
      insert.rs             insert, update_embedding, delete, connect_node, select_neighbors_heuristic
  tests/
    integration.rs          recall@10 ≥ 0.95 vs brute force on 2k random vectors
    filtered.rs             results-stay-in-filter, brute-force fallback exactness, recall@10 ≥ 0.85 at 50% filter
    persistence.rs          checkpoint round-trip + WAL-only recovery (gated on persistence feature)
    concurrency.rs          readers on snapshot stay consistent under concurrent writer
  examples/
    quick_search.rs         end-to-end demo
```

## Implementation notes

- **One TableWriter at a time per WriteTx.** `WriteTx::open_table` returns
  `TableWriter<'_, R>` whose lifetime borrows the WriteTx mutably. The
  HNSW algorithm therefore can't hold the data writer and entry writer
  open simultaneously. Insert is structured as three phases: read
  entry-point → do all data-table work in a single TableWriter scope →
  reopen entry table to update if needed.
- **Search starts by opening the entry table** to read the singleton
  entry-point row. On a fresh store this returns `Error::KeyNotFound` —
  the search converts that to an empty result rather than propagating.
- **HNSW algorithm follows Malkov & Yashunin (2018)**, Algorithms 2, 3, 4.
  Heuristic neighbor selection with `keep_pruned_connections = true`:
  accept a candidate iff it is closer to the query than to any
  already-selected neighbor; if `selected.len() < m` after the pass,
  fill from rejected candidates.
- **M-pruning on neighbor back-references.** When the new node is added
  to a neighbor's adjacency list and the list exceeds `m` (or `m_max0`
  at layer 0), the same heuristic re-selects the top `m` keepers
  anchored at the neighbor (not the query).
- **Persistence requires registering `Table<VectorRow<Meta>>` and
  `Table<EntryPoint>`** with the store's type registry. `VectorCollection::open`
  does this when the `persistence` feature is on. Registration is
  idempotent on same name + same type.
- **Entry-point repair on delete.** When the deleted node is the current
  entry point, the data table is scanned linearly to find the highest-
  level non-tombstoned node. Linear in table size; rare in practice
  (only triggered when the entry point itself is deleted).

## Verification

All gates from the spec passed:

- `recall_at_10_meets_floor_on_random_vectors` — recall ≥ 0.95 against
  brute-force ground truth on 2k vectors of dim 32.
- `results_stay_inside_filter` — pre-filter never leaks ids outside the
  bitmap.
- `brute_force_fallback_returns_exact_top_k` — fallback below
  `BRUTE_FORCE_THRESHOLD` returns identical top-K to brute force.
- `recall_holds_at_50pct_selectivity` — recall ≥ 0.85 with 50% filter.
- `round_trip_after_wal_replay_matches_pre_close_results` — checkpoint +
  WAL recovery yields identical search results.
- `wal_only_recovery_works_without_checkpoint` — WAL replay alone
  reconstructs the graph correctly.
- `readers_see_stable_snapshot_under_writer` — readers pinned at a
  baseline version see the same results before and after a concurrent
  writer commits new vectors.
- `cargo clippy --workspace --all-targets -- -D warnings` clean, with
  and without the `persistence` feature.

## Out of scope (deferred)

- Hybrid search / fusion of BM25 + HNSW.
- REST / gRPC server, dynamic per-collection schema.
- SIMD distance kernels (scalar f32 in v1; gate on benchmark numbers).
- Periodic graph compaction to reclaim tombstone space.
- IVF / PQ / quantization.
- Optimization: neighbor-update batching, vector-only sub-rows to
  reduce per-update Clone cost. Current per-insert cost includes Clone
  of full `VectorRow<Meta>` for every back-reference update.
