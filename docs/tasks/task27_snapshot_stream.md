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

**Indexes are not shipped as key bytes.** The wire format ships only index *names* and *kinds* (per-table-header `IndexDef`). The receiver's destination store must already have the matching `define_index` calls in place — the `KeyExtractor` closures live in the binary, not in the wire format. At install, the install path looks up the destination's existing table by name, clones its `empty_index_defs()`, and `Table::from_bulk` rebuilds them from the new rows via the existing `IndexMaintainer::rebuild_from_sorted_data` pass. Trades a little CPU on receive for a meaningfully smaller wire payload, with the constraint that index definitions must be code-resident on both sides.

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
    pub on_extra_tables: OnExtra,        // Keep | Drop (default Keep)
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
        I: Iterator<Item = Result<(u64, R)>>;
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
    RowCountMismatch { trailer: u64, actual: u64 },
    BulkLoad(crate::Error),    // #[from] crate::Error — preserves WriteConflict etc.
}
```

`BulkLoad` carries the underlying `crate::Error` rather than a stringified version, so the future Raft adapter can pattern-match on `crate::Error::WriteConflict` to retry without parsing strings.

`RowCountMismatch` catches a flipped-byte in the trailer's `total_rows` field. The trailer's `total_crc32` is finalized *before* `total_rows` is written (build-time ordering), so `total_rows` is cross-validated at install time against the sum of per-table `row_count`s in the headers (which *are* covered by `total_crc`).

All install-path failures are atomic — destination Store is left untouched on any error.

## Tests

16 integration tests in `tests/snapshot_stream.rs`:

- Magic-bytes header sanity.
- Build → install full roundtrip (single-table).
- Concurrent writes do not block build (frozen `Arc<Snapshot>` semantics).
- Truncated wire bytes fail install without partial state.
- Corrupted bytes fail with `BadCrc`.
- Unknown-table policy variants (`Drop` / `Error`).
- `list_checkpoints` returns versions.
- `open_checkpoint_reader` produces a wire-format stream.
- Secondary indexes survive a roundtrip (defines `by_email` on dst, installs, asserts `get_unique` works).
- Multi-table roundtrip (two distinct record types — `accounts` + `txs`).
- Plus several happy-path / format-validation cases.

Plus 3 `bulk_load_stream` unit tests (build from iterator, iterator-error propagation, count-mismatch error).

Microbenchmarks at 1 K / 10 K / 100 K rows for both build and install paths in `benches/snapshot_throughput.rs`.

## Hardening notes

The v1 install path treats the wire stream as untrusted input. Several hardening passes after the initial implementation:

- **Bounded pre-allocation.** `row_count` is capped against the remaining stream bytes (each row is ≥ 12 bytes on the wire) before `Vec::with_capacity`, so a corrupted or malicious `u64::MAX` can't abort the process via allocator failure before the CRC check runs. Same fix in `bulk_load_stream`.
- **Strict-ascending key check.** `build_from_raw_rows` validates strict-monotonic keys before calling `BTree::from_sorted` (which only `debug_assert!`s). Out-of-order or duplicate keys fail with `Error::Persistence` instead of silently corrupting the tree in release builds.
- **`next_id` overflow guard.** `last_id + 1` uses `checked_add`; a wire stream with `u64::MAX` as the last key surfaces as a clean error rather than a debug-build panic / release-build wrap-to-0.
- **Custom-index detection.** Before `Table::from_bulk` (which would `panic!` for `IndexKind::Custom`), the install path walks the destination's `index_list()` and returns `SnapshotStreamError::CustomIndexUnsupported { table, index }` if any custom index is present.
- **Atomic capture of `(snapshot, version)`.** Captured under one read lock to close a race where a concurrent committer between two reads could let `install_batch`'s OCC pass while `base_snapshot` reflected stale state.
- **`OnExtra` policy.** New `InstallOptions::on_extra_tables` controls whether tables present in the destination snapshot but absent from the wire stream survive the install. Default `Keep` preserves the v1 merge behaviour; `Drop` matches Raft `InstallSnapshot` exact-match semantics. Wired into `Store::install_batch_replace`, which filters the prev snapshot's tables by the stream's name set.
- **Error taxonomy.**
  - Invalid UTF-8 in the table-header decode reports `SnapshotStreamError::Malformed` (the bytes are present, they're just not valid UTF-8) rather than the misleading `Truncated`.
  - `Store::snapshot_stream(missing_version)` reports `SnapshotStreamError::VersionNotFound(v)` instead of wrapping a `crate::Error::VersionNotFound` inside `BulkLoad`.

## Deferred work

- **Streaming install consumer.** v1 drains the reader into memory before parsing. A streaming consumer that builds tables incrementally is the next optimization.
- **`InstallOptions::commit_version`.** The field is kept on `InstallOptions` for forward compatibility but is **currently ignored** (the v1 `BulkLoadBatch` API has no version-override hook). Will be wired up once `BulkLoadBatch` exposes one. Documented as ignored in the field's doc comment.
- **`OnUnknown::Keep`.** Currently treated identically to `Drop` (preserved-table logic out of scope for v1). Documented as such in the variant's doc comment.
- **Custom-index restore.** A `CustomIndex::empty()` (or similar) hook would let custom indexes survive install/bulk-load instead of being rejected. Out of scope for v1.
- **Stable cross-build `record_type_id`.** Currently `DefaultHasher::hash(&TypeId)` — stable within a process run, not across Rust builds. For cross-cluster safety the registry should adopt a user-supplied stable id at register time (e.g., a `&'static str` namespace). The install path tolerates a mismatched type_id when the table name is registered (it dispatches by name), so the field today is a best-effort hint, not a hard contract.

## Cross-references

- **Companion (log-side) work:** [task26_journal.md](task26_journal.md) — `ultima_journal` crate.
- **Future adapter:** the openraft `RaftStateMachine::build_snapshot` / `install_snapshot` / `get_current_snapshot` adapter. Plus the `_meta` table convention to write `(applied_log_id, last_membership)` atomically with state-machine data inside the same `WriteTx`.
