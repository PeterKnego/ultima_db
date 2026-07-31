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

**The 0.3.0 store must point at a fresh, empty directory** (or one from which
every `checkpoint_*.bin` and `wal.bin` has been deleted). Migrating in place
leaves the directory permanently unrecoverable: a fresh store restarts at
version 0, so its first checkpoint is `checkpoint_1.bin`, while recovery picks
the *highest*-versioned checkpoint file and `checkpoint()` only prunes files
below the one it just wrote. A leftover `checkpoint_500.bin` from 0.2.x
therefore outranks the migrated data forever, and every subsequent
`recover()` hard-errors on it with the new data unreachable.

(There is no `export` API; the steps above are the export.) Both rejection
messages state this path in full.

### Breaking — on-disk and wire formats

- **WAL entry format v2.** Each entry payload now opens with
  `[magic 0xFF][format 2]`, and every key-carrying op holds a
  `PrimaryKey::KEY_TYPE_ID` tag plus an encoded primary key instead of a
  `u64` id. A pre-0.3.0 WAL is refused **when the store is opened**, in all
  three `WalWrite` modes (`PerEntry`, `Coalesced`, `CoalescedPrealloc`) — not
  merely at `recover()`. This is deliberate: appending to a v1 WAL would let
  `commit()` acknowledge writes as durable that `recover()` could never read
  back, and the only remedy the later error could offer would destroy exactly
  those acknowledged commits.
- **Checkpoint table format v2.** The per-table payload header is
  `[magic 0xFF][version 2][key_type u32-be]`, followed by explicit big-endian
  lengths for the auto-increment counter, each key and each record. Rejected
  at `recover()` with an error naming the table. (Two header bytes rather than
  one because `bincode`'s standard config is a varint encoding: a v1 payload
  for a table that had taken exactly one insert begins with the literal byte
  `0x02`, so a bare version byte would have *silently misread* it as v2.
  `0xFF` is not a legal varint tag.)
- **Snapshot stream `FILE_FORMAT_V` 1 → 2.** Rows are
  `key_len(u32) | key | val_len(u32) | val`, and each table header carries the
  source table's primary-key type — as a `key_type_id` (checked) and a
  `key_type` name (diagnostic). This is a **live-replication** break as
  well as an on-disk one: a 0.2.0 SMR follower cannot install a 0.3.0 leader's
  snapshot, and a 0.3.0 follower cannot install a 0.2.0 leader's. Both
  directions reject cleanly rather than mis-parsing.
- **All three formats record the key type and validate it on read.** Encoded
  keys are opaque bytes that a *different* key type decodes without
  complaint — `u64` and `i64` encodings differ only in the sign bit, `String`
  and `Vec<u8>` are identical, and the eight bytes of `1u64` are valid UTF-8 —
  and because the encoding is order-preserving, the reinterpreted keys pass
  every ascending-order check downstream. Reopening a directory under a
  different `K` used to recover `Ok` with every key silently reinterpreted; it
  now fails with an error naming both key types.

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
- **`Error::WriteConflict.keys` is renamed `key_digests` and carries
  `PrimaryKey::hash64` digests, not row ids — including on `u64`-keyed
  tables.** The rename is deliberate: leaving the name alone would have made
  this the one break in the release that produces neither a compile error nor
  a runtime one (code that logged or correlated those numbers would keep
  compiling and quietly emit different values). As `key_digests` it is a
  compile error at every use site. Treat the entries as opaque conflict
  identifiers; to recover the offending rows, re-read them on the retry. The
  `Display` text changed to match (`key digests [..]`).
- **Direct `Table` users**: `get`, `update`, `delete` and `contains` now take
  `&K` where they took a `u64` by value; `get_many`, `delete_batch` and
  `resolve` take `&[K]`; and `iter`, `range`, `first`, `last` yield
  `(&K, &R)` instead of `(u64, &R)`. The transaction handle layer
  (`TableReader`/`TableWriter`) masks this — it takes `impl Borrow<K>` and its
  `iter`/`range` still yield `(K, &R)` — so code going through `open_table` is
  unaffected.
- `snapshot_stream::codec::TableHeader` gained public `key_type_id` and
  `key_type` fields. Struct-literal construction of it must be updated.
- `wal::WalOp::{Insert, Update, Delete}` gained a `key_type` field. Struct-literal
  construction must be updated for any code with the `persistence` feature enabled.

### Added

- `Table<R, K = u64>`, generic over the primary key. The type parameter is
  defaulted, so every existing `Table<R>` mention keeps compiling.
- `PrimaryKey` trait (order-preserving `encode`/`decode`, a `hash64` conflict
  digest, `ENCODED_LEN` for tuple framing, and `KEY_TYPE_ID`, the persisted
  key-type discriminant every format stamps and checks), implemented for
  `u8`–`u128`, `i8`–`i128`, `String`, `Vec<u8>`, and 2- and 3-tuples.
  `AutoKey`, which gates auto-increment, is implemented only for `u64`.
  **Third-party `PrimaryKey` impls must declare their own `KEY_TYPE_ID`**: ids
  `1..=63` are reserved for the built-in scalars, ids with the high bit set are
  produced by the tuple impls, and `0` is reserved — pick a fixed arbitrary
  value in `64..=0x7FFF_FFFF` and never change it once data exists.
- `Error::KeyTooLong { len, max, context }` — an encoded primary key over
  `MAX_ENCODED_KEY_LEN`. Returned by the mutation that produced it, so a key
  the persistence formats cannot carry never reaches `commit()`. (`Error` is
  `#[non_exhaustive]` as of 0.2.0, so this is additive.)
- `TableWriter::put(key, record)` — insert-or-replace at an explicit key, and
  the only way to write an explicitly-keyed table (there is no auto-increment
  for a key the store cannot generate). It also works on a `u64` table, where
  it differs from `insert_with_id` in two ways: it *replaces* an existing row
  rather than returning `DuplicateKey`, and it advances the id counter past
  `key` so a later `insert` cannot reissue it. Writing `u64::MAX` leaves the
  counter where it is — an auto-increment table never loses its counter.
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
- Encoded keys are capped at 64 KiB (`MAX_ENCODED_KEY_LEN`), a bound the WAL,
  the checkpoint and the snapshot wire format share by construction and
  enforce on **write** as well as on read. With a WAL sink configured,
  an over-long key is refused at the `put`/`update`/`delete` that produced it,
  so `commit()` never acknowledges a row that could not be read back. Without
  a WAL (under `Persistence::smr`), the check surfaces at `checkpoint()` time.
- The snapshot stream's key-type check compares `PrimaryKey::KEY_TYPE_ID`, not
  `std::any::type_name`: the id is declared by the key type, so it is stable
  across compiler versions and injective across crate versions in a way the
  name is not. The name still travels, for the error message.
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
