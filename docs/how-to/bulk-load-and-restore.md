# How to bulk-load, back up, and restore data

`Store::bulk_load` bypasses the transaction path: it builds a fresh B-tree
from sorted input in O(N), rebuilds secondary indexes, and installs the
result atomically as one new snapshot version. Use it for restores and
large ingests; use normal write transactions for everything else.

## Restore a full backup

Use `BulkLoadInput::Replace`. The table's previous contents are discarded;
index *definitions* are preserved and their data rebuilt over the new rows.

```rust
use ultima_db::{BulkLoadInput, BulkLoadOptions, BulkSource, Store};

let base: Vec<(u64, String)> = (1u64..=10_000).map(|i| (i, format!("user_{i}"))).collect();
let v1 = store.bulk_load::<String>(
    "users",
    BulkLoadInput::Replace(BulkSource::sorted_vec(base)),
    BulkLoadOptions::default(),
)?;
```

Pick the `BulkSource` that matches your input:

- `BulkSource::sorted_vec` / `Sorted` — strictly ascending `(id, row)` pairs,
  the fastest path. Duplicates and out-of-order ids are rejected before
  anything is installed.
- `BulkSource::unsorted_vec` / `Unsorted` — arbitrary order, sorted internally.
- `BulkSource::auto_id_vec` / `AutoId` — rows without ids; assigned `1..=N`.

The `Sorted`/`Unsorted` variants also accept any
`Box<dyn Iterator<Item = (u64, R)> + Send>`, so you can stream from a file
decoder instead of materializing a `Vec` yourself. For rows arriving as
`Result` items (e.g. off a wire), see `Store::bulk_load_stream`.

## Apply an incremental backup

Use `BulkLoadInput::Delta` with the three buckets. The whole delta is
validated up front (duplicate ids, inserts of existing ids, updates/deletes
of missing ids, cross-bucket overlap); any failure leaves the store
unchanged.

```rust
use ultima_db::BulkDelta;

let delta = BulkDelta::<String> {
    inserts: (10_001u64..=10_500).map(|i| (i, format!("user_{i}"))).collect(),
    updates: vec![(42, "user_42_renamed".into())],
    deletes: vec![100, 200, 300],
};
let v2 = store.bulk_load::<String>(
    "users",
    BulkLoadInput::Delta(delta),
    BulkLoadOptions::default(),
)?;
```

## Restore several tables atomically

If a backup spans multiple tables and readers must never see a half-restored
state, stage each table with `bulk_load_batch` and commit once — all tables
land in a single new snapshot version. `Replace` only; deltas stay on
`Store::bulk_load`.

```rust
use ultima_db::AddOptions;

let mut batch = store.bulk_load_batch();
batch.add::<String>(
    "strings",
    BulkLoadInput::Replace(BulkSource::sorted_vec(str_rows)),
    AddOptions::default(),
)?;
batch.add::<u64>(
    "u64s",
    BulkLoadInput::Replace(BulkSource::sorted_vec(u64_rows)),
    AddOptions::default(),
)?;
let version = batch.commit(BulkLoadOptions::default())?;
```

Each `add` builds its table immediately, off-lock, so input errors surface
per table rather than at commit. Dropping the batch without `commit`
discards everything. If a concurrent commit advanced the store past the
version captured at `bulk_load_batch()`, `commit` fails with
`Error::WriteConflict` — rebuild the batch and retry
(see [How to handle write conflicts](handle-write-conflicts.md)).

## Load tables with non-u64 keys

`bulk_load` is `u64`-only. For any other `PrimaryKey` type, use
`bulk_load_keyed` with rows in strictly ascending key order (anything else
is rejected with `Error::InvalidBulkLoadInput` before the store is touched):

```rust
let rows = vec![
    ("a@example.com".to_string(), "Alice".to_string()),
    ("b@example.com".to_string(), "Bob".to_string()),
];
store.bulk_load_keyed::<String, String>("emails", rows, None)?;
```

`None` for the options means `BulkLoadOptions::default()`. See
[How to use natural primary keys](use-natural-primary-keys.md) and the
[key encoding reference](../reference/key-encoding-and-formats.md).

## Do not skip the checkpoint on a persistent store

Bulk loads do not go through the WAL. `BulkLoadOptions::checkpoint_after`
defaults to `true`, which writes a checkpoint and prunes the WAL right after
the install — leave it that way on a Standalone store unless you checkpoint
yourself immediately after.

If you load with `checkpoint_after: false` on a Standalone store:

- A crash before the next checkpoint loses the load (recovery falls back to
  the pre-load state).
- Worse, if normal commits land *on top* of the un-checkpointed load, those
  WAL entries assume the post-load table and cannot be replayed. Recovery
  detects this and fails with `Error::BulkLoadNotCheckpointed` rather than
  recovering a state that mixes pre-load data with post-load commits. The
  only way forward is to redo the load from its source.

`checkpoint_after: false` is only appropriate when the store is in-memory
(`Persistence::None`, where it is a no-op anyway) or when you batch several
loads and call `store.checkpoint()` once at the end — with no commits in
between.

## Coordinate with concurrent writers

A bulk load behaves like a delete-and-recreate of the table:

- **SingleWriter:** the load is refused with `Error::WriterBusy` while any
  `WriteTx` is open. Drop or commit the writer, then retry the load.
- **MultiWriter:** the load proceeds; an in-flight transaction that wrote to
  the loaded table gets `Error::WriteConflict` at its commit (under
  `Serializable`, one that merely read it gets
  `Error::SerializationFailure`). Transactions on other tables are
  unaffected.
- In either mode, a commit that lands while the load is being built causes
  the install itself to fail with `Error::WriteConflict` — retry against the
  new state.

## Notes for large ingests

- The sorted `Replace` path is O(N) — no per-row tree descent. Pre-sort at
  the source if you can; `Unsorted` pays an extra `sort_unstable` over the
  whole input. `examples/bulk_load_10m.rs` times the variants at 10M rows
  (`cargo run --release --example bulk_load_10m`), including the
  `insert_batch` transaction path for comparison.
- Define secondary indexes on the (empty) table *before* the load; `Replace`
  preserves the definitions and rebuilds their data in the same pass. Each
  index adds roughly a proportional cost per row.
- Numbers and provenance live in the
  [performance reference](../reference/performance.md).
