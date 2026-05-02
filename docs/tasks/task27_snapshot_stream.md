# Task 27 — `snapshot_stream` and streaming bulk-load

**Status:** Shipped 2026-05-02 (merge of `feat/snapshot-stream` into `main`).
**Module:** `src/snapshot_stream/` in the `ultima-db` crate, gated on the `persistence` feature.

## Goal

Give the future openraft `RaftStateMachine` adapter (see [task26_journal.md](task26_journal.md) for its log-side companion) a streaming snapshot transport: a versioned wire format that can be sourced from a frozen `Arc<Snapshot>` (no blocking of writers) and atomically installed on a receiver. Plus a streaming variant of `bulk_load` for any restore use case.

This completes the `ultima_db` half of the openraft-readiness audit. Both halves (journal + snapshot stream) ship without an actual adapter; the adapter is downstream work.

## What landed

| Item | Location |
|---|---|
| Wire format codec | `src/snapshot_stream/codec.rs` |
| Build path (`SnapshotReader: impl Read`) | `src/snapshot_stream/build.rs` |
| Install path (`Store::install_snapshot_stream`) | `src/snapshot_stream/install.rs` |
| `Store::snapshot_stream`, `list_checkpoints`, `open_checkpoint_reader` | `src/store.rs` |
| `Store::bulk_load_stream` | `src/bulk_load.rs` |
| `MergeableTable::index_list`, `collect_serialized_rows` | `src/table.rs` |
| `TableRegistry::numeric_type_id`, `build_table_from_raw` | `src/registry.rs` |
| Integration tests (14) | `tests/snapshot_stream.rs` |
| Microbenchmarks | `benches/snapshot_throughput.rs` |

## Wire format

Self-describing, single-pass encode/decode. Every field is little-endian.

```
[file header]
    magic         8 bytes  b"ULTSNAP\0"
    format_ver    u16      = 1
    store_ver     u64      Snapshot version this came from
    table_count   u32

[per table, repeated table_count times]
    [table header]
        name_len       u16
        name           utf-8
        record_type_id u64    (registry hash key — best-effort mismatch hint)
        row_count      u64
        index_count    u16
        for each index:
            kind     u8       0 = Unique, 1 = NonUnique
            name_len u16
            name     utf-8
    [row stream, row_count rows]
        key      u64
        val_len  u32
        val      bincode bytes
    [table trailer]
        table_crc32  u32     CRC over this table's row stream

[file trailer]
    total_rows    u64
    total_crc32   u32        CRC over all bytes preceding the trailer
    bookend       8 bytes    b"ULTSNAP\0"
```

**Indexes are not shipped as key bytes** — they are rebuilt on the receiver from the row data using the same `bulk_load`-side index rebuild primitive. Trades a little CPU on receive for a meaningfully smaller wire payload.

**Tables iterate in deterministic sorted-by-name order** so the build is reproducible across replicas.

## Build path

```rust
impl Store {
    pub fn snapshot_stream(
        &self,
        version: Option<u64>,
    ) -> Result<SnapshotReader, SnapshotStreamError>;
}
```

`SnapshotReader` implements `std::io::Read`. State machine: `NotStarted → BetweenTables → InTable → Done`. Internal state holds the cursor + current table iterator + scratch buffer + running `crc32fast::Hasher`.

`Store::snapshot_stream` clones the `Arc<Snapshot>` and `Arc<TableRegistry>` under the read lock, then releases — subsequent `Read::read` calls hold no Store locks. MVCC guarantees the snapshot remains valid even as new writers commit (verified by the `snapshot_build_does_not_block_concurrent_writes` test).

`version=None` uses `latest_version`. A specific `version` returns whichever frozen snapshot the store still retains in memory.

## Install path

```rust
impl Store {
    pub fn install_snapshot_stream<R: Read>(
        &self,
        reader: R,
        opts: InstallOptions,
    ) -> Result<u64, SnapshotStreamError>;
}

pub struct InstallOptions {
    pub on_unknown_tables: OnUnknown,    // Drop | Keep | Error (default Drop)
    pub commit_version: Option<u64>,     // currently ignored (v1 limitation)
}
```

Algorithm:

1. Drain the reader into memory (v1 — streaming consumer is a future optimization noted in spec §6.4).
2. Decode file header. Validate magic + format_ver.
3. For each declared table:
   - Decode header.
   - Drain `row_count` rows accumulating raw `(u64, Vec<u8>)` pairs and updating both the table-CRC and the running total-CRC.
   - Validate the table_crc trailer. Mismatch → `Err(BadCrc { table: Some(name) })`.
   - If table is registered: dispatch through `TableRegistry::build_table_from_raw(name, raw_rows)` which uses a per-type closure registered at `Store::register_table` time to deserialize each row's bytes into typed `R` and build a `Table<R>` via `Table::from_bulk`. Generic-free at the call site.
   - If unknown: `OnUnknown::Drop` → skip; `OnUnknown::Keep` → not yet implemented; `OnUnknown::Error` → `Err(UnknownTable { ... })`.
4. Validate file trailer (total_rows, total_crc, bookend magic).
5. Atomic install via `Store::bulk_load_batch` — all tables installed in a single batch commit, single Arc swap. Either all-visible or no-effect. Crash mid-install drops in-memory build state; on-disk state is untouched until a subsequent `Store::checkpoint()`.

### `commit_version`

The plan specified `InstallOptions::commit_version: Option<u64>` for SMR deployments where the consensus log dictates the version. The v1 `BulkLoadBatch` API doesn't accept a version override, so the field is kept for future use and currently ignored. Documented as such.

## Streaming `bulk_load`

```rust
impl Store {
    pub fn bulk_load_stream<R, I>(
        &self,
        table_name: &str,
        sorted: I,
        row_count: u64,
    ) -> Result<u64>
    where
        R: Record,
        I: IntoIterator<Item = Result<(u64, R), Error>>;
}
```

Collects the iterator (propagating any iterator error), validates `row_count` and strict-monotonic key order, then delegates to the existing `Store::bulk_load(name, BulkLoadInput::Replace(BulkSource::sorted_vec(...)), BulkLoadOptions { checkpoint_after: false, .. })`. No duplicate logic.

## `Store::list_checkpoints` + `open_checkpoint_reader`

```rust
impl Store {
    pub fn list_checkpoints(&self) -> Result<Vec<u64>, Error>;
    pub fn open_checkpoint_reader(&self, version: u64)
        -> Result<SnapshotReader, SnapshotStreamError>;
}
```

`list_checkpoints` reads the persistence directory and parses `checkpoint_{version}.bin` filenames (the existing convention from `src/checkpoint.rs`). Empty under `Persistence::None`.

`open_checkpoint_reader` calls the existing `crate::checkpoint::load_checkpoint(&path, &registry)` and wraps the resulting `Snapshot` in a `SnapshotReader` — same wire-format emitter as `Store::snapshot_stream`, just sourced from disk instead of an in-memory snapshot.

**Note on retention:** the existing `Store::checkpoint()` calls `cleanup_old_checkpoints` after each write, so `list_checkpoints` typically returns a single version (the latest). Callers should treat the API as "query the available on-disk checkpoint(s)" without assuming history is preserved.

## Concurrency invariants

- **Build:** zero coordination needed. The source is `Arc<Snapshot>` — immutable. Writers continue uninterrupted. Verified by integration test.
- **Install:** uses the existing `BulkLoadBatch` commit-lock — one install at a time, no concurrent commits of any kind. Same guarantee as the existing `bulk_load`.

## Crash safety

| Phase | Crash window | Guarantee |
|---|---|---|
| Build | While `Read::read` is being drained | Wire bytes can be partial; receiver detects via length checks + magic + CRCs and refuses install. Source store is untouched. |
| Install before atomic swap | Mid-decode, mid-bulk-add | New snapshot is built entirely in memory before the single `Arc` swap. Crash drops in-memory state with no on-disk effect. |
| Install after swap, before checkpoint | Between commit and `Store::checkpoint()` | New snapshot is in memory but not yet persisted. On restart, the adapter re-receives from the leader (Raft handles this naturally). |

## Errors

```rust
pub enum SnapshotStreamError {
    Io(std::io::Error),
    BadMagic,
    BadFormatVersion(u16),
    Truncated,
    BadCrc { table: Option<String> },
    UnknownTable { name: String, type_id: u64 },
    BulkLoad(crate::Error),
}
```

All install-path failures are atomic — destination Store is left untouched on any error.

## Tests

14 integration tests in `tests/snapshot_stream.rs`:

- Magic-bytes header sanity
- Build → install full roundtrip
- Concurrent writes do not block build
- Truncated wire bytes fail install without partial state
- Corrupted bytes fail with `BadCrc`
- Unknown-table policy variants (`Drop` / `Error`)
- `list_checkpoints` returns versions
- `open_checkpoint_reader` produces a wire-format stream
- Empty store snapshot (zero tables)
- Multi-table snapshots
- Serialized row format roundtrip
- Atomic-failure leaves destination unchanged

Plus 3 `bulk_load_stream` unit tests (build from iterator, iterator-error propagation, count-mismatch error).

Microbenchmarks at 1 K / 10 K / 100 K rows for both build and install paths in `benches/snapshot_throughput.rs`.

## Deferred work

Tracked in design spec §12 (preserved at `docs/superpowers/specs/`).

- **Streaming install consumer.** v1 drains the reader into memory before parsing. A streaming consumer that builds tables incrementally is the next optimization.
- **`InstallOptions::commit_version`.** Wire-up to `BulkLoadBatch` once the latter exposes a version-override API.
- **`OnUnknown::Keep`.** Currently treated as Drop with a doc comment; needs preservation logic from the current snapshot.
- **Stable cross-build `record_type_id`.** Currently a `DefaultHasher::hash(&TypeId)` which is not stable across Rust builds. For cross-cluster safety the registry should adopt a user-supplied stable id at register time.

## Cross-references

- **Companion (log-side) work:** [task26_journal.md](task26_journal.md) — `ultima_journal` crate.
- **Future adapter:** the openraft `RaftStateMachine::build_snapshot` / `install_snapshot` / `get_current_snapshot` adapter. Plus the `_meta` table convention to write `(applied_log_id, last_membership)` atomically with state-machine data inside the same `WriteTx`.
