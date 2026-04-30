# Task 23 — Bulk Load

A fast path for ingesting large amounts of data into UltimaDB: initial
loads, full backup restores, and incremental delta application. Built
around three layered primitives — bottom-up `BTree::from_sorted`, fast
index rebuild from sorted data, and `Table::from_bulk` — exposed via a
single public entry point `Store::bulk_load`. The path skips the
per-row `WriteTx` machinery (no per-row WAL, no per-row index hooks),
builds a fresh table off-lock, and atomically installs it as a new
MVCC snapshot.

## Goal

Make large data ingestion (initial loads, full backup restores,
incremental delta apply) dramatically faster than the existing
`insert_batch`/`update_batch` path while remaining MVCC-safe and
atomic. A single API covers four use cases:

1. Initial load of an empty table (seeding, fresh dataset import).
2. Full backup restore — replace a table's contents wholesale.
3. Incremental backup apply — inserts + updates + deletes on top of
   current contents.
4. Large batch insert inside a normal `WriteTx` (convenience, not the
   primary path).

## Architecture

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Both explicit-ID and auto-ID inputs | Explicit-ID is required for restore (preserves cross-table references); auto-ID is the natural fit for fresh imports. |
| 2 | Top-level `Store::bulk_load` *and* `WriteTx`-scoped variant | Top-level is the real bulk path (off-lock bottom-up build). The `WriteTx` variant is convenience over `*_batch`; it does *not* get bottom-up semantics because the table participates in OCC merge. |
| 3 | Delta payload partitioned by op (`{ inserts, updates, deletes }`), not an ordered log | Lets us sort/dedupe each bucket and amortize index work. Caller adapts wire format on input. |
| 4 | Iterator-based core, `Vec` convenience wrappers; both sorted and unsorted entry points | Streaming for huge restores; explicit sortedness lets ordered backups take the fast path. |
| 5 | Checkpoint after load, prune WAL up to new version (no per-row WAL) | The whole point of bulk load is to skip 10M individual WAL writes. Durability ridge is the checkpoint. |
| 6 | Indexes built post-data via shared "build index from sorted (id, key) pairs" primitive | Same primitive serves bulk-load and `define_index` backfill. Single-threaded for v1. |
| 7 | All-or-nothing atomicity, in-memory build (~2× peak memory), no options | Restore is a single atomic event. Disk-backed staging deferred until the 2×-memory edge case actually bites. |

## API

```rust
// src/store.rs (top-level entry point)
impl Store {
    pub fn bulk_load<R: Record>(
        &self,
        table_name: &str,
        input: BulkLoadInput<R>,
        opts: BulkLoadOptions,
    ) -> Result<u64>;  // returns new committed version
}

// src/store.rs (WriteTx-scoped convenience over *_batch)
impl<'a, R: Record> TableWriter<'a, R> {
    pub fn bulk_load(&mut self, input: BulkLoadInput<R>) -> Result<()>;
}

// src/bulk_load.rs (public types)
pub enum BulkLoadInput<R> {
    Replace(BulkSource<R>),
    Delta(BulkDelta<R>),
}

pub enum BulkSource<R> {
    Sorted(Box<dyn Iterator<Item = (u64, R)> + Send>),
    Unsorted(Box<dyn Iterator<Item = (u64, R)> + Send>),
    AutoId(Box<dyn Iterator<Item = R> + Send>),
}

impl<R> BulkSource<R> {
    pub fn sorted_vec(v: Vec<(u64, R)>) -> Self;
    pub fn unsorted_vec(v: Vec<(u64, R)>) -> Self;
    pub fn auto_id_vec(v: Vec<R>) -> Self;
}

pub struct BulkDelta<R> {
    pub inserts: Vec<(u64, R)>,
    pub updates: Vec<(u64, R)>,
    pub deletes: Vec<u64>,
}

pub struct BulkLoadOptions {
    pub create_if_missing: bool,    // default true
    pub checkpoint_after: bool,     // default true
}
```

File pointers:

- Public types: `src/bulk_load.rs`.
- Top-level entry point: `src/store.rs:613` (`Store::bulk_load`).
- Internal install helper: `src/store.rs:711` (`Store::install_replaced_table`).
- WriteTx-scoped convenience: `src/store.rs:1488` (`TableWriter::bulk_load`).
- Public re-exports: `src/lib.rs:21` (`BulkDelta`, `BulkLoadInput`, `BulkLoadOptions`, `BulkSource`).

### Semantics

- **`Replace` on a table that exists**: the table's index *definitions*
  are preserved (via `Table::empty_index_defs`, `src/table.rs:174`);
  data and index *contents* are rebuilt from scratch. Existing
  `ReadTx`s on prior snapshots still see the old data via MVCC.
- **`Replace` on a missing table**: created if `create_if_missing`
  (default); else `Error::TableNotFound`. Indexes start empty; caller
  can `define_index` afterwards (which now uses the same fast
  primitive — see *Three primitives* below).
- **`Delta` on a missing table**: `Error::TableNotFound` regardless of
  `create_if_missing`. A delta has no schema or index info; it must be
  applied to an existing table.
- **`AutoId` source**: assigns IDs from `1` (Replace-only). Using
  `AutoId` with `Delta` is rejected as a usage error.
- **`next_id` handling**: `Replace` + explicit IDs → `max(id) + 1`;
  `Replace` + `AutoId` → `N + 1`; `Delta` → `max(existing_next_id,
  max(insert_ids) + 1)`.
- **Empty input**: `Replace` empty → table replaced with zero rows
  (data tree empty, indexes empty). `Delta` empty → no-op, no new
  snapshot version produced.

### Delta validation rules

A `BulkDelta` is validated before any mutation begins. Any failure
returns `Err` and changes nothing.

- **No ID overlap across buckets.** A given ID appears in at most one
  of `inserts`, `updates`, `deletes`. Overlap → `Error::InvalidArgument`.
  Callers wanting "delete then re-insert" semantics use `updates`
  instead.
- **No duplicate IDs within a bucket.** Adjacent equal IDs after sort →
  `Error::DuplicateKey`.
- **`updates` IDs must exist** in the current table. Missing →
  `Error::KeyNotFound`. (Matches today's `Table::update` behavior.)
- **`deletes` IDs must exist** in the current table. Missing →
  `Error::KeyNotFound`. *Stricter* than today's silent `delete` to
  surface backup/log inconsistencies.
- **`inserts` IDs must not exist** in the current table. Collision →
  `Error::DuplicateKey`.

Validation runs in two passes: bucket-internal checks (sort + dedup,
cross-bucket overlap), then existence checks against the captured base
data tree.

## Three primitives, layered

### 1. `BTree::from_sorted` — bottom-up tree build

`src/btree.rs:73` — `pub(crate) fn from_sorted<I: IntoIterator<Item = (K, Arc<V>)>>(iter: I) -> Self`.

Single pass over sorted input, O(N):

1. Maintain a stack of partially-built nodes — `Vec<LevelBuilder<K, V>>` —
   one per tree level.
2. **Leaf level (`level[0]`)**: accumulate `(K, Arc<V>)` entries until
   the leaf has `MAX_KEYS = 63` entries.
3. **Promote-on-fill**: when leaf is full and the next entry arrives,
   freeze the current leaf as `Arc<BTreeNode>`, push it as the next
   child of `level[1]`, promote the *next entry* to `level[1].entries`
   as the separator, and start a fresh empty `level[0]`.
4. Internal levels apply the same rule.
5. **Finish**: walk levels bottom to top. At each level, freeze the
   (possibly partial) builder and attach it as the rightmost child of
   the level above. The single remaining root becomes `BTree::root`.

**Tail handling.** Packing at `MAX_KEYS = 63` leaves a residue of `N
mod 63` entries in the last leaf, which can be smaller than
`MIN_KEYS = T - 1 = 31` and would violate the invariant. At finish
time, if any rightmost-path node has `< MIN_KEYS` entries, the builder
*redistributes* with its left sibling: pull entries from the
sibling's tail (and one separator down from the parent, replacing it
with the new boundary entry). Implemented as `redistribute_tail` at
`src/btree.rs:847`. The algorithm covers both the leaf-drain
(`MAX_KEYS + 1` rows) and the level-1-drain
(`MAX_KEYS * (MAX_KEYS + 1) + 1` rows) cases.

`from_sorted` debug-asserts strictly ascending input. Caller is
responsible for sort + dedup.

### 2. `IndexMaintainer::rebuild_from_sorted_data` — fast index backfill

`src/index.rs:44` — trait default impl falls back to per-row replay
(used for `CustomIndex`); fast specializations on `ManagedIndex`
(`src/index.rs:150` and `src/index.rs:228`).

For a fully-built data `BTree<u64, R>` and an index def:

1. Walk the data tree in key order (O(N)), extracting
   `(extracted_key, row_id)` pairs into `Vec<(K, u64)>`.
2. Sort:
   - **Unique** index: `sort_unstable_by(|a, b| a.0.cmp(&b.0))`; detect
     collisions during sort (adjacent equal keys →
     `Error::UniqueConstraintViolation`).
   - **Non-unique** index: composite ordering `(K, u64)`, naturally
     strict because `row_id` is unique.
3. Build the index B-tree via `BTree::from_sorted`.

Used by both bulk-load and `Table::define_index` (`src/table.rs:567`
no longer iterates row-by-row — it calls
`rebuild_from_sorted_data`). Single-threaded; each index is
independent and trivially parallelizable later.

### 3. `Table::from_bulk` — assemble the new table

`src/table.rs:150` — `pub(crate) fn from_bulk(sorted_rows: Vec<(u64,
Arc<R>)>, next_id: u64, index_defs: Vec<Box<dyn IndexMaintainer<R>>>)
-> Result<Self>`.

1. Build `BTree<u64, R>` via `from_sorted`.
2. For each index def: call `rebuild_from_sorted_data`, install into
   the table's `indexes` map.
3. Set `next_id`.

If any index build fails (unique violation), returns `Err`. Caller
drops the partial table; the original `Table` (if any) is unchanged.

## Concurrency, install, persistence

### Atomic install

`Store::bulk_load` (`src/store.rs:613`) is a specialized commit:

1. **Build phase (no locks):** sort/dedup, build `Table<R>` via
   `Table::from_bulk`. Captures the current `Snapshot` *only at start*
   to clone existing index definitions and (for `Delta`) the existing
   data tree. Other readers and writers run concurrently.
2. **Commit phase (under `inner.commit_lock`):** `Replace` swaps in
   the new table wholesale. `Delta` checks whether any committed
   writer modified the same table since our build started; if yes,
   returns `Error::WriteConflict` (caller retries — v1 chooses abort
   over retry-rebase for simplicity).
3. **Snapshot install:** clone the prior `Snapshot`'s table map, swap
   in the new table, install as `snapshots[new_version]`, bump
   `latest_version`. See `Store::install_replaced_table`
   (`src/store.rs:711`). Same tail as `WriteTx::commit`.

### Concurrency contract

- **Readers:** never blocked. `ReadTx`s started before install see
  pre-load state; `ReadTx`s started after see loaded state. Old data
  lives until those `ReadTx`s drop, then `gc()` reclaims.
- **SingleWriter mode:** build runs concurrently with at most one
  `WriteTx`; commit phase serializes on `commit_lock`.
- **MultiWriter mode:** `Replace` always wins at install (it is a
  wholesale replacement). For a writer that committed against the
  same table *before* our install, their commit is already in the
  snapshot map at a lower version — we don't undo it. For a writer
  attempting to commit *after* our install, our table is their merge
  base and per-key merge replays their dirty keys onto our loaded
  data — consistent with existing `MergeableTable::merge_keys_from`
  semantics.
- **`!Send`/`!Sync` of `WriteTx`:** unaffected — `bulk_load` is on
  `Store`, which is `Send + Sync + Clone`.

### Persistence

After install, if `BulkLoadOptions.checkpoint_after` is `true` and the
store is in a persistent mode (`Persistence::Standalone` or
`Persistence::Smr`):

- Call existing `Store::checkpoint()` → writes a checkpoint file for
  the new snapshot version *and* prunes the WAL automatically (the
  checkpoint method already handles WAL truncation; bulk_load doesn't
  call `prune_wal` separately).

For `Persistence::None`, `checkpoint_after` is silently a no-op.

Bulk load **never** writes per-row WAL entries. If `checkpoint_after =
false`, the loaded data is in memory only until the next regular
checkpoint; a crash in that window loses the load (documented
trade-off for "I'll batch many bulk loads, then checkpoint once at
the end" — covered by
`bulk_load_skip_checkpoint_loses_data_on_crash` in
`tests/persistence_integration.rs`).

## Atomicity and memory model

- Inputs funnel into a single `Vec<(u64, Arc<R>)>` before tree build —
  the index builder needs to walk it, and we don't want to walk the
  iterator twice.
- For `Sorted`: assert ascending, collect.
- For `Unsorted`: collect, `sort_unstable_by_key`, dedup-check.
- For `AutoId`: collect, assign IDs `1..=N`.
- For `Delta`: clone existing table's data into a `Vec<(u64,
  Arc<R>)>` via in-order walk; apply deletes (`HashSet<u64>` lookup,
  skip); apply updates (replace via `HashMap<u64, Arc<R>>`); merge
  inserts (sort-merge with the existing rows by ID); duplicate-ID
  check across inserts vs existing rows.

Peak memory: ~2× the loaded data during build (new tree alongside
whatever the prior snapshot holds). All-or-nothing: failure during
sort/dedup/index-build returns `Err` and the original snapshot is
untouched.

## Out of scope for v1

- Partial / resumable loads.
- Parallel index construction (single-threaded; trivially parallel
  per-index when needed).
- Disk-backed staging for loads larger than 2× RAM.
- A bulk-load-specific WAL op (durability is via checkpoint).
- Bulk load over a `CustomIndex<R>` definition: the trait does not
  yet expose a generic "make empty" hook, so
  `CustomIndex`'s `IndexMaintainer::empty_clone` panics. The Replace
  path consequently can't preserve a custom-index definition. A
  `CustomIndex::empty` requirement (or similar) would unlock it; not
  needed for the v1 use cases.

## Test pointers

- `BTree::from_sorted` invariants and small/medium/large equivalence:
  `src/btree.rs:1379` and following (empty, single, exact MAX_KEYS,
  MAX_KEYS+1, 1k, 100k, tail-underfull).
- Index rebuild matches incremental backfill (unique + non-unique +
  collision): `src/index.rs:704`, `src/index.rs:756`,
  `src/index.rs:802`.
- `Table::from_bulk` with indexes and collision error path: in
  `src/table.rs` test module (search `from_bulk_*`).
- Store-level integration tests: `tests/bulk_load.rs` —
  - `bulk_load_replace_on_empty_store_creates_table`
  - `bulk_load_replace_with_existing_data_rebuilds_indexes`
  - `bulk_load_replace_preserves_index_definitions`
  - `bulk_load_replace_auto_id_assigns_sequential_ids`
  - `bulk_load_replace_missing_table_without_create_errors`
  - `bulk_load_delta_applies_inserts_updates_deletes`
  - `bulk_load_delta_rejects_overlapping_ids_across_buckets`
  - `bulk_load_delta_missing_table_errors`
  - `write_tx_table_bulk_load_via_batch`
  - `bulk_load_replace_does_not_disturb_existing_read_tx`
- Persistence integration: `tests/persistence_integration.rs` —
  - `bulk_load_persists_via_checkpoint_and_recovers`
  - `bulk_load_skip_checkpoint_loses_data_on_crash`
- Benchmark: `benches/bulk_load_bench.rs` — `bulk_load` (sorted) vs
  `insert_batch` at 100k and 1M rows.
- Example: `examples/bulk_restore.rs` — full restore (10k rows) plus
  a delta (500 inserts, 1 update, 3 deletes).

## Implementation notes

What was actually built and how it diverges from the original spec:

- **Single-threaded index construction.** Each index rebuilds
  serially. The `rebuild_from_sorted_data` shape is trivially
  parallelizable (per-index independence) but parallelization is
  deferred until benchmarks show it matters.
- **No disk-backed staging.** Peak memory is ~2× the loaded data.
  Acceptable per Decision 7 above; a follow-up adds disk-backed
  staging if real-world restores exceed available RAM.
- **`CustomIndex` is not supported in `bulk_load` yet.**
  `IndexMaintainer::empty_clone` for `CustomIndex` (`src/index.rs:434`)
  panics with a pointer to this doc. The Replace path can't preserve a
  custom-index definition without a `CustomIndex::empty` hook (or
  similar). The v1 use cases — backups and restores of standard
  managed indexes — are fully covered.
- **`Persistence` enum names.** The original spec assumed `OffDisk |
  Standalone | Consistent`. The actual enum (`src/persistence.rs:50`)
  is `None | Standalone | Smr`. `checkpoint_after` is a no-op for
  `Persistence::None`.
- **`Store::checkpoint()` prunes the WAL.** The original spec called
  for an explicit `prune_wal` call after `checkpoint()`; in practice
  `Store::checkpoint()` already truncates the WAL up to the new
  checkpoint version, and `bulk_load` relies on that single call.
- **Phase-1 `redistribute_tail`.** The bottom-up builder's tail
  handling redistributes from the previously-frozen sibling. The
  algorithm is verified for both leaf-drain (MAX_KEYS+1 rows trigger
  underfull-leaf rebalance) and level-1-drain
  (MAX_KEYS*(MAX_KEYS+1)+1 rows trigger underfull-internal-node
  rebalance) cases — see the `from_sorted_*` tests at
  `src/btree.rs:1379`.
- **`WriteTx`-scoped variant uses the existing batch APIs** rather
  than the bottom-up build: it delegates `Replace` to
  `delete_batch` + `upsert`/`insert_batch`, and `Delta` to
  `delete_batch` → `update_batch` → per-insert upsert. This is
  intentional — the WriteTx variant participates in OCC merge and
  must remain compatible with per-key merging at commit. It is
  convenience, not the bulk path.
- **Performance.** Smoke-test (`cargo bench --bench bulk_load_bench
  -- --quick`) measures ~9× speedup at 100k rows and ~12× at 1M rows
  for sorted bulk_load vs insert_batch on a single string-valued
  column with no secondary indexes. Index-heavy workloads should see
  proportionally larger wins because index construction is the more
  expensive piece of the existing batch path.
