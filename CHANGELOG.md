# Changelog

## Unreleased

### Changed

- `ReadTx` is now `Send + Sync` and `WriteTx` is now `Send` (still `!Sync`).
  The `PhantomData<*const ()>` marker that pinned both to their creating
  thread was removed after an audit found no thread affinity anywhere on the
  read or write path — it was a footgun guard, not a correctness requirement.
  A transaction can now be moved between threads and held across an `.await`.
  Additive, so no downstream code breaks. Two caveats the compiler no longer
  enforces: an open `WriteTx` holds the SingleWriter slot (or its MultiWriter
  intents), and `commit()` blocks — on an async runtime it belongs in
  `spawn_blocking`. See `docs/tasks/task55_send_audit.md`.

## 0.2.0 — 2026-07-30

**Heads-up: an on-disk format break is coming in 0.3.0.** Arbitrary primary
keys will change the WAL and checkpoint formats, and recovery will reject
files written by earlier versions. Do not build long-lived persisted data on
0.2.0 that you cannot rebuild.

### Breaking

- `Error` is now `#[non_exhaustive]`. Downstream `match` expressions over it
  must include a wildcard arm. This is the last time adding an error variant
  will be a breaking change.
- `Error::DuplicateTableOpen` added (see `open_tables2`/`open_tables3` below).

### Added (ultima-db)

- `Store::pin_version(Option<u64>) -> Result<VersionPin>` — a `Send + Sync +
  Clone` handle that keeps one snapshot alive across `gc()`, for handing a
  consistent point-in-time view to another thread. Note that pinning is not
  atomic with commit: `pin_version(Some(v))` can race auto-GC under concurrent
  committers, while `pin_version(None)` is race-free.
- `WriteTx::open_tables2` / `open_tables3` — open two or three tables in one
  call and hold their writers simultaneously, instead of one at a time.
  Returns `Error::DuplicateTableOpen` if a name is repeated.
- `fanout-t8` cargo feature — narrow B-tree fanout (T=8) for write-dominated
  deployments: roughly 1.8x the default's contended write throughput, at the
  cost of about 2x read-p99-under-load and 25% slower uncontended gets.

### Changed (ultima-db)

- B-tree nodes use inline fixed-capacity storage, making a copy-on-write node
  clone a single allocation; default fanout retuned from T=64 to T=32.
- `Store::gc()` is now O(evicted + pins) per run rather than O(retained), so a
  large `num_snapshots_retained` no longer costs per-commit time.
- `WriteTx::open_table` caches its per-table metrics handle and name, removing
  a registry lookup and an allocation from every repeat call.

### ultima-vector

- Version-only release. No source changes; republished so that its dependency
  requirement admits `ultima-db` 0.2.0.

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
