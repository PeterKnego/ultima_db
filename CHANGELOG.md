# Changelog

## 0.3.0 — 2026-07-31

Arbitrary primary keys: a table can now be keyed by `String`, `Vec<u8>`, any
integer width, or a tuple, instead of only an auto-incrementing `u64`. This is
the on-disk format break announced in 0.2.0 — **read the migration note below
before upgrading a store that has data on disk.**

### Migration from 0.2.x — required if you use `persistence`

There is no in-place upgrade, and **checkpointing on 0.2.x does not help**:
0.3.0 rejects pre-0.3.0 checkpoints as well as pre-0.3.0 WALs. The path is:

1. With the **0.2.x** binary, `Store::recover()` the existing persistence
   directory.
2. Read the rows out through a `ReadTx`.
3. Load them into a **0.3.0** store with `Store::bulk_load` /
   `Store::bulk_load_batch` — existing `u64`-keyed tables need no key change —
   and then `checkpoint()`.

(There is no `export` API; the steps above are the export.) Both rejection
messages state this path in full.

### Breaking — on-disk and wire formats

- **WAL entry format v2.** Each entry payload now opens with
  `[magic 0xFF][format 2]` and carries an encoded primary key instead of a
  `u64` id. A pre-0.3.0 WAL is refused **when the store is opened**, in all
  three `WalWrite` modes (`PerEntry`, `Coalesced`, `CoalescedPrealloc`) — not
  merely at `recover()`. This is deliberate: appending to a v1 WAL would let
  `commit()` acknowledge writes as durable that `recover()` could never read
  back, and the only remedy the later error could offer would destroy exactly
  those acknowledged commits.
- **Checkpoint table format v2.** The per-table payload header is two bytes,
  `[magic 0xFF][version 2]`, followed by explicit big-endian lengths for the
  auto-increment counter, each key and each record. Rejected at `recover()`
  with an error naming the table. (Two bytes rather than one because
  `bincode`'s standard config is a varint encoding: a v1 payload for a table
  that had taken exactly one insert begins with the literal byte `0x02`, so a
  bare version byte would have *silently misread* it as v2. `0xFF` is not a
  legal varint tag.)
- **Snapshot stream `FILE_FORMAT_V` 1 → 2.** Rows are
  `key_len(u32) | key | val_len(u32) | val`, and each table header carries the
  source table's primary-key type. This is a **live-replication** break as
  well as an on-disk one: a 0.2.0 SMR follower cannot install a 0.3.0 leader's
  snapshot, and a 0.3.0 follower cannot install a 0.2.0 leader's. Both
  directions reject cleanly rather than mis-parsing.

### Breaking — API

- `SnapshotStreamError::NonU64Key` is **removed**; `KeyTypeMismatch { table,
  stream, destination }` and `KeyTooLong { table, len, max }` are added. The
  key-type check is enforced on install because row keys are opaque bytes that
  several key types decode without complaint (the eight bytes of `1u64` are
  also a valid NUL-filled `String`), so a mismatched stream would otherwise
  install garbage keys silently.
- `SnapshotStreamError` is now `#[non_exhaustive]`. Downstream `match`
  expressions over it must include a wildcard arm. Done in the release that
  already breaks exhaustive matches over it, so it costs nothing extra now and
  makes future variants non-breaking.
- **`Error::WriteConflict.keys` now carries `PrimaryKey::hash64` digests, not
  row ids — including on `u64`-keyed tables.** This one is *silent*: code that
  logged or correlated those numbers still compiles and now emits different
  values. Treat the entries as opaque conflict identifiers; to recover the
  offending rows, re-read them on the retry. (Renaming the field to
  `key_digests`, which would turn this into a compile error, was considered
  and deferred — say so if you would prefer it before 1.0.)
- **Direct `Table` users**: `get`, `update`, `delete` and `contains` now take
  `&K` where they took a `u64` by value; `get_many`, `delete_batch` and
  `resolve` take `&[K]`; and `iter`, `range`, `first`, `last` yield
  `(&K, &R)` instead of `(u64, &R)`. The transaction handle layer
  (`TableReader`/`TableWriter`) masks this — it takes `impl Borrow<K>` and its
  `iter`/`range` still yield `(K, &R)` — so code going through `open_table` is
  unaffected.
- `snapshot_stream::codec::TableHeader` gained a public `key_type` field.
  Struct-literal construction of it must be updated.

### Added

- `Table<R, K = u64>`, generic over the primary key. The type parameter is
  defaulted, so every existing `Table<R>` mention keeps compiling.
- `PrimaryKey` trait (order-preserving `encode`/`decode`, a `hash64` conflict
  digest, and `ENCODED_LEN` for tuple framing), implemented for `u8`–`u128`,
  `i8`–`i128`, `String`, `Vec<u8>`, and 2- and 3-tuples. `AutoKey`, which
  gates auto-increment, is implemented only for `u64`.
- `WriteTx::open_table_keyed::<R, K>`, `ReadTx::open_table_keyed::<R, K>`,
  `Store::register_table_keyed::<R, K>`, `Store::bulk_load_keyed::<R, K>`.
  These are **additive**: `open_table`, `register_table`, `open_tables2` and
  `open_tables3` keep their exact signatures and stay `u64`-only, because Rust
  has no default type parameters on *functions* — widening them would make
  every existing `open_table::<R>(..)` turbofish an `E0107`. Keyed
  `open_tables2`/`open_tables3` are deferred until a caller needs them.
- Secondary indexes over any primary key: `ManagedIndex` storage is generic
  over the row key independently of the index key, and `CustomIndex<R, K =
  u64>` and the built-in BM25 `FullTextIndex` are widened the same way. This
  is additive — existing `CustomIndex` impls are unchanged.
- `BTree::range_prefix` — an O(log n + k) prefix scan on a `BTree<(A, B), V>`,
  which a `RangeBounds` cannot express without inventing minimum and maximum
  values for `B`.
- `examples/string_keyed_table.rs`.

### Notes

- A `String`-keyed table's `get` does **not** accept a `&str`: the standard
  library provides `String: Borrow<str>`, not `str: Borrow<String>`, so
  `t.get("alice@example.com")` does not compile — pass a `&String`. This is
  the inverse of `HashMap<String, _>::get` and is the most common surprise
  when moving a table off `u64` keys.
- Encoded keys are capped at 64 KiB (`MAX_ENCODED_KEY_LEN`), a bound the WAL
  and the snapshot wire format share by construction.
- The snapshot stream's key-type check compares `std::any::type_name`, which
  Rust does not promise stable across compiler versions. Same-binary SMR is
  unaffected; a cross-toolchain stream fails loudly rather than silently.
- Full design notes: `docs/tasks/task56_arbitrary_primary_keys.md`.

### ultima-vector

- Version-only release. No source changes; republished so that its dependency
  requirement admits `ultima-db` 0.3.0 (for `0.x` crates `^0.2.0` excludes
  `0.3.0`, so without this the vector crate would pin consumers to 0.2.x).

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

- Transactions are no longer pinned to the thread that opened them. `ReadTx`
  is now `Send + Sync` and `WriteTx` is now `Send` (it stays `!Sync`, so it
  can be moved between threads but never shared by two at once). The
  `PhantomData<*const ()>` marker that enforced the old restriction was
  removed after an audit found no thread affinity anywhere on the read or
  write path — it was a footgun guard, not a correctness requirement. This is
  additive, so no existing code breaks, and it makes the crate usable from
  async code, where a transaction may now be held across an `.await`.

  Three things the compiler no longer catches, all of which matter on an
  async runtime: an open `WriteTx` holds the SingleWriter slot (or, in
  MultiWriter, its intents), so parking one on a long `.await` stalls other
  writers; `commit()` blocks, on locks, on the promotion gate, and on the WAL
  fsync under `Durability::Consistent`; and *dropping* a `WriteTx` blocks
  too, since it takes the store write lock to release its writer slot and
  intents — which includes the implicit drop on an early return or a
  cancelled task. Run transaction work inside `spawn_blocking`. See
  `docs/tasks/task55_send_audit.md`.
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
