# Task 24 — Vector Restore

## Goal

Add a fast, atomic restore path to `ultima_vector`: install a previously-built
HNSW collection's rows + entry-point in one snapshot, skipping the per-item
graph construction that `bulk_insert` does today.

This task covers **Restore only** — replacing a collection's contents from
already-built `VectorRow`s. Bulk **construction** (build an HNSW graph from
raw embeddings, faster than per-item insertion) is a separate, larger feature
deferred to a future task.

Builds directly on `task23_bulk_load.md` (single-table `Store::bulk_load`).

## Architecture

The feature spans two crates:

1. **`ultima_db`** — generalize `Store::bulk_load` to a multi-table atomic
   batch. Restore needs to install both the data table and the entry-point
   singleton table in the same snapshot version; task23's API is single-table.
   The new primitive is `Store::bulk_load_batch()` returning a
   `BulkLoadBatch` builder.

2. **`ultima_vector`** — add `VectorCollection::restore_iter` /
   `restore_vec` that build the two `BulkLoadInput`s (one for the data
   table, one for the entry-point singleton) and commit them atomically via
   the new batch API.

The single-table `Store::bulk_load(name, input, opts)` from task23 stays as a
thin wrapper that constructs a one-element batch — see Implementation notes.

### Architectural decisions

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Restore and Build are separate features | Different cost profiles, different algorithms, different shipping risk. Restore is mostly plumbing; Build is algorithmic work. |
| 2 | Multi-table atomicity via `Store::bulk_load_batch` in `ultima_db` | Other features will want it (paired tables, denormalized views). Cleaner than vector-specific plumbing. |
| 3 | `Vec` + iterator entry points | `restore_iter` is the real API (handles 10M+ rows without OOM); `restore_vec` is a one-line convenience wrapper. |
| 4 | Replace-only semantics | Restore is by definition "this is the state now." Existing rows in either table are dropped; pre-existing `ReadTx`s see old state via MVCC. |
| 5 | Cheap-only validation (dim + HnswState shape per row) | Dim catches wrong-collection backups; HnswState shape catches malformed serialization. Skip neighbor-id integrity (O(N) memory) — bad refs surface as `NodeNotFound` at search time without corrupting the store. |
| 6 | Imperative `BulkLoadBatch` builder, explicit `commit` | Idiomatic Rust, consistent with existing `WriteTx` style. Each `add` runs the build off-lock immediately so caller controls memory peaks. Drop without `commit` = noop. |
| 7 | `Delta` is single-table only — `BulkLoadBatch::add` accepts `Replace` only | Multi-table delta semantics compound (per-table base versions, per-table existence checks); not needed for v1. Single-table delta path stays on `Store::bulk_load`. |
| 8 | No backup file format defined | Caller serializes however they want (bincode, parquet, custom). File format is its own design with versioning + compression + parallel-readability concerns. |

## `ultima_db` API

In `src/bulk_load.rs`:

```rust
/// Builder for an atomic multi-table bulk install. Build each table off-lock
/// via repeated `add`, then `commit` to install all of them in a single new
/// snapshot version.
///
/// Dropped without `commit` discards the built tables; the store is unchanged.
pub struct BulkLoadBatch<'s> { /* ... */ }

/// Per-table options for `BulkLoadBatch::add`.
pub struct AddOptions {
    /// If true and the target table doesn't exist, create it. Default: true.
    pub create_if_missing: bool,
}

impl<'s> BulkLoadBatch<'s> {
    /// Build a `Replace` input into a new table and stage it for install.
    /// Index *definitions* on the existing table (if any) are preserved;
    /// data + index *contents* are rebuilt from `input`.
    ///
    /// Validation (duplicate IDs, missing table) surfaces here, not at commit.
    /// `Delta` input is rejected with `Error::InvalidBulkLoadInput`.
    pub fn add<R: Record>(
        &mut self,
        name: &str,
        input: BulkLoadInput<R>,
        opts: AddOptions,
    ) -> Result<()>;

    /// Atomically install all staged tables, producing one new snapshot
    /// version. Optionally checkpoint + prune WAL.
    pub fn commit(self, opts: BulkLoadOptions) -> Result<u64>;
}
```

In `src/store.rs`:

```rust
impl Store {
    /// Begin a multi-table atomic bulk install. Captures the current
    /// `latest_version` as the base version — concurrent committers that
    /// advance the version before this batch's `commit` will trigger a
    /// `WriteConflict` at install time.
    pub fn bulk_load_batch(&self) -> BulkLoadBatch<'_>;
}
```

Concrete locations:

- `BulkLoadBatch`, `AddOptions`, `PendingTable`: `src/bulk_load.rs:294`+.
- `Store::bulk_load_batch()`: `src/store.rs:752`.
- `Store::bulk_load` thin wrapper: `src/store.rs:613`–`src/store.rs:626`.

### Semantics

- **`add`** runs the full materialize → validate → `Table::from_bulk` pipeline
  immediately. Errors surface here, not at `commit`. The built table is held
  as `Arc<dyn MergeableTable>` until `commit`.
- **Repeated `add`** for the same table name is allowed; last one wins.
- **`commit`** acquires `inner.write()`, checks `latest_version == base_version`
  (else `Error::WriteConflict`), clones the prior snapshot's table map, swaps
  in *all* staged tables in one update, bumps version, optionally checkpoints.
  Single lock acquisition.
- **Empty `commit`** (no `add` calls) is a no-op: returns `Ok(base_version)`
  without taking the lock, without bumping `latest_version`, and without
  checkpointing. Matches "no work, no version" intuition and avoids spurious
  empty snapshots.

## `ultima_vector` API

In `ultima_vector/src/collection.rs:173`:

```rust
impl<Meta, D> VectorCollection<Meta, D>
where Meta: Record + Clone, D: Distance,
{
    /// Atomically replace this collection's contents with the provided rows
    /// and entry point. Existing rows in both the data table and the entry-
    /// point table are dropped; pre-existing `ReadTx`s on prior snapshots
    /// continue to see the old state via MVCC.
    ///
    /// Validates per-row: `embedding.len() == params.dim` and
    /// `HnswState::layers_len() == level + 1`. Does not validate
    /// neighbor-id integrity — bad refs surface as `NodeNotFound` at
    /// search time.
    ///
    /// Returns the new committed snapshot version.
    pub fn restore_iter<I>(&self, rows: I, entry_point: EntryPoint) -> Result<u64>
    where I: IntoIterator<Item = (u64, VectorRow<Meta>)>;

    /// Convenience over `restore_iter` for in-memory `Vec` input.
    pub fn restore_vec(
        &self,
        rows: Vec<(u64, VectorRow<Meta>)>,
        entry_point: EntryPoint,
    ) -> Result<u64>;
}
```

`restore_iter` materializes + validates rows in one pass (O(N) loop, dim
check is O(1) per row, HnswState shape check is two field accesses), builds
two `BulkLoadInput::Replace` inputs (one for the data table, one for the
entry-point singleton at id=1 per the existing `write_entry_point`
convention), and installs them via `Store::bulk_load_batch().add(...).add(...).commit(...)`.

The entry-point row is a singleton at id=1 in a dedicated table.

### New error variants

In `ultima_vector/src/error.rs`:

- `Error::DimMismatch { expected, got }` — pre-existing.
- `Error::InvalidHnswState { id, level, layers }` — added for restore.
  Surfaces when a deserialized `HnswState`'s `layers.len()` doesn't match
  `level + 1`.

## Concurrency, install, persistence

### Concurrency

- **Readers**: never blocked. Pre-existing `ReadTx`s on prior snapshots see
  pre-restore state via MVCC; `ReadTx`s started after the install see
  restored state.
- **Writers (SingleWriter mode)**: build phase (`add` calls) runs concurrently
  with at most one `WriteTx`; commit phase serializes on `inner.write()`.
- **Writers (MultiWriter mode)**: same. The conflict check at `commit`
  (`latest_version == base_version`) means a concurrent `WriteTx` that
  committed *after* `bulk_load_batch()` was called will cause restore to
  abort with `WriteConflict`. Caller retries the whole restore. In practice
  restore is a quiesced operation; concurrent writers shouldn't be present.
- **`!Send`/`!Sync` of `WriteTx`** is unaffected — `BulkLoadBatch` borrows
  `&Store` (which is `Send + Sync + Clone`), not a `WriteTx`. Restore can
  be invoked from any thread holding a `Store` clone.

### Install

`BulkLoadBatch::commit` mirrors `Store::install_replaced_table` (now removed,
see Implementation notes) but for N tables instead of 1:

1. Acquire `inner.write()`.
2. Check `inner.latest_version == self.base_version`. If not, return
   `WriteConflict`.
3. Clone the prior snapshot's `tables: HashMap<String, Arc<dyn MergeableTable>>`.
4. For each `PendingTable`: insert into the cloned map (replacing any existing
   entry).
5. Bump `latest_version` and `next_version`.
6. Insert the new `Snapshot { version, tables }` into `snapshots`.
7. Run `auto_gc_if_enabled` if configured.
8. Drop the lock.
9. If `BulkLoadOptions.checkpoint_after`: call `Store::checkpoint()` (which
   prunes WAL).

### Persistence

Reuses the task23 path: `Store::checkpoint()` writes a checkpoint file for
the new version and prunes the WAL. No per-row WAL entries — the build phase
is in-memory, durability comes from the checkpoint.

For `Persistence::None`, `checkpoint_after` is a no-op.

## Atomicity

All-or-nothing in-memory build (consistent with task23). Peak memory: ~2× the
restored data during the build (the new tables alongside whatever the prior
snapshot held). For the entry-point table this is a single row — negligible.

If `add` fails (validation, dim mismatch, etc.), the batch is dropped and the
store is unchanged. If `commit` fails (`WriteConflict`), the staged tables
are also dropped — caller retries.

## Out of scope for v1

- **Bulk construction from raw embeddings.** Separate task, separate
  brainstorming round.
- **Backup file format.** Caller picks (bincode, parquet, etc.).
- **Multi-table delta.** `BulkLoadBatch::add` accepts `Replace` only;
  single-table delta stays on `Store::bulk_load`. Multi-table delta semantics
  compound (per-table base versions, per-table existence checks); not
  needed for v1.
- **Custom-index handling on the data table.** `VectorCollection`'s data
  table has no secondary indexes today. If users add them later, the existing
  `empty_index_defs` plumbing in `Table::from_bulk` (task23) handles it
  automatically — no restore-specific code needed.
- **Streaming-from-disk readers.** `restore_iter` accepts an iterator, but
  we don't ship a "read backup file → iterator" utility. Caller provides
  their own reader.
- **Parallel build of the data table.** Single-threaded per task23.
- **Restore-time HNSW invariant repair.** If a backup contains a stale
  entry-point reference (e.g., the entry-point node was tombstoned after the
  backup was taken), restore preserves the bug; caller is responsible for
  backup integrity.

## Test pointers

### `ultima_db` (batch API)

`tests/bulk_load.rs`:

- `bulk_load_batch_two_tables_install_atomically` — atomic multi-table install.
- `bulk_load_batch_drop_without_commit_is_noop` — drop semantics.
- `bulk_load_batch_conflict_detection` — `WriteConflict` on intervening commit.
- `bulk_load_batch_add_validates_input_eagerly` — duplicate IDs at `add`.
- `bulk_load_batch_create_if_missing_false_errors` — missing table behavior.
- `bulk_load_batch_rejects_delta` — `Delta` rejection.
- `bulk_load_batch_empty_commit_is_noop` — empty-commit fast path.
- Plus the 10 prior single-table tests, which continue to pass against the
  delegated implementation.

### `ultima_vector` (restore)

`ultima_vector/tests/restore.rs`:

- `restore_round_trip_preserves_search_results` — search returns same top-k
  before/after restore.
- `restore_replace_drops_old_data` — replacement semantics.
- `restore_dim_mismatch_errors_before_install` — dim validation, store
  unchanged.
- `restore_duplicate_ids_errors` — duplicate-ID rejection from `ultima_db`.
- `restore_empty_collection` — empty restore.
- `restore_concurrent_read_unaffected` — MVCC isolation.

`ultima_vector/tests/with_persistence.rs` (persistence feature):

- `restore_persists_via_checkpoint_and_recovers` — restore on a Standalone
  store, drop, reopen, search returns restored content.

### Example

`ultima_vector/examples/bulk_restore.rs` — round-trip example that builds
1000 rows via `upsert`, captures rows + entry point, restores into a fresh
store, and asserts top-3 search results match. Restore time printed.

## Implementation notes

What shipped, including divergences from the spec:

- **v1 ships single-table `Delta` only.** Multi-table delta is deferred —
  `BulkLoadBatch::add` accepts `Replace` only, returning
  `Error::InvalidBulkLoadInput("BulkLoadBatch supports Replace only — use Store::bulk_load for Delta")`
  on `Delta`.
- **`BulkLoadBatch::add` runs eagerly.** The full materialize → validate →
  `Table::from_bulk` build pipeline runs inside `add`, so validation errors
  (duplicate IDs, missing target table when `create_if_missing: false`,
  rejected `Delta`) surface at `add`, not at `commit`. This trades a tiny
  amount of "deferred error" elegance for stronger error locality.
- **Empty `commit` is a no-op.** With zero `add` calls, `commit` returns
  `Ok(base_version)` without acquiring the write lock or bumping
  `latest_version`. Matches "no work, no version" intuition and avoids
  spurious empty snapshots.
- **`Error::InvalidHnswState` is defense-in-depth.** It can't be triggered
  through safe public APIs because the `HnswState` constructors enforce the
  `layers.len() == level + 1` invariant. The variant exists so
  `restore_iter` can guard against malformed deserialization (a backup file
  edited by hand or corrupted in transit). Tests removed correspondingly —
  the only way to construct an inconsistent state is unsafe, which a
  product test wouldn't exercise.
- **`Store::install_replaced_table` was deleted in Phase B.** The single-
  table `Store::bulk_load` now constructs a one-element `BulkLoadBatch`,
  calls `add`, then `commit`, going through the same install path as the
  multi-table case. Net code size is smaller — one install path, not two.
