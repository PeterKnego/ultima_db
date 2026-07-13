# Changelog

## 0.1.1 — 2026-07-13

Metadata-only patch (`ultima-db`): crate description and README/crate-doc
opening repositioned to "high-performance transactional embedded database",
with the published YCSB comparison linked as evidence. No code changes.
`ultima-vector` stays at 0.1.0.

## 0.1.0 — 2026-07-13

First public release of `ultima-db` and `ultima-vector`.

### ultima-db

- MVCC snapshot store on a persistent copy-on-write B-tree (T=64):
  zero-copy historical reads, O(1) table clones, atomic multi-table commits.
- Typed tables with auto-increment ids, unique / non-unique / custom
  secondary indexes (incl. an optional BM25 full-text index), and atomic
  batch operations; sequential batch inserts take an O(batch + height)
  bulk-append fast path.
- Concurrent writers (opt-in `MultiWriter`) with key-level OCC; opt-in
  serializable snapshot isolation (SSI) for write-skew prevention.
- Opt-in durability (`persistence`): group-committed WAL with Eventual /
  Consistent / ConsistentInline tiers, preallocated-WAL and coalesced write
  modes, CRC-protected checkpoints, crash recovery, checkpoint-only SMR
  mode, and the `Persistence::standalone_fast` preset.
- Bulk load / restore (O(N) sorted builds, multi-table atomic installs)
  and a streaming snapshot wire format.

### ultima-vector

- HNSW approximate-nearest-neighbor search over UltimaDB tables with
  metadata filtering and MVCC-consistent restores.
- SIMD distance kernels (Cosine / L2 / DotProduct / CosineNormalized) with
  runtime AVX-512 / AVX2 / NEON dispatch; strict input validation
  (dimension and non-finite checks) at every collection boundary.

### Notes

- MSRV 1.88. Pre-1.0 API: minor releases may contain breaking changes.
- Known limitations: SSI does not yet validate read-only transactions or
  index-DDL backfills (documented in `docs/tasks/task21`); CJK full-text
  tokenization is unigram-incomplete (task43); `BTree::from_sorted` packs
  one tail leaf below the MIN_KEYS floor at exactly
  `m·(MAX_KEYS+1)³ + δ (δ < MIN_KEYS)` element counts — benign and
  self-healing (task51).
