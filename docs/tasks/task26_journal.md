# Task 26 — `ultima_journal` crate

**Status:** Shipped 2026-05-02 (merge commit on `main`).
**Crate:** `ultima_journal/` (workspace sibling of `ultima_db`, `ultima_vector`).

## Goal

Provide the storage primitives a future openraft `RaftLogStorage` adapter will need: a runtime-agnostic segmented append journal and a small durable atomic-value primitive. Designed so the adapter is straightforward to write and high-performance on the steady-state hot paths (log append, replication reads, vote persistence).

The adapter itself lives in another project; this crate ships only the primitives. The companion `ultima_db::snapshot_stream` work (`RaftStateMachine`-side) is tracked separately under `docs/superpowers/plans/2026-05-02-snapshot-stream-implementation.md` and is not yet implemented.

## Architecture

`ultima_journal` has **no dependency on `ultima_db`** and `ultima_db` has no dependency on `ultima_journal` — they are peers, not stacked. The future Raft adapter depends on both.

Three modules, three responsibilities:

```
ultima_journal/src/
├── lib.rs           — module roots + re-exports
├── durability.rs    — Durability { Consistent, Eventual }
├── error.rs         — JournalError, StableValueError (manual Clone on JournalError so callbacks can be cloned)
├── notifier.rs      — Notifier (wait/on_complete) + Signal (producer)
├── journal/
│   ├── mod.rs       — Public Journal API
│   ├── segment.rs   — Segment file format, framing, recovery scan
│   └── writer.rs    — bg writer thread + group commit
└── stable_value.rs  — StableValue<T> (rotating two-slot durable atomic value)
```

## `Notifier`

Signalling primitive shared by `Journal::append`, `Journal::truncate_after`, and `StableValue::store`. Returned by mutating ops to report when a write reaches durable storage.

```rust
impl Notifier {
    fn done() -> Self;                         // already-resolved (Eventual mode)
    fn pending() -> (Signal, Notifier);
    fn is_done(&self) -> bool;
    fn wait(self) -> Result<(), JournalError>;
    fn on_complete<F>(self, f: F) where F: FnOnce(Result<(), JournalError>) + Send + 'static;
}
```

Two consumption shapes:
- **`wait()`** — blocking via condvar. Used by `save_vote` and any synchronous caller.
- **`on_complete(callback)`** — bg fsync thread invokes the callback inline after `sync_all()` (zero thread-hop). Maps directly onto openraft's `IOFlushed::io_completed(...)`.

Implementation: `Arc<Mutex<State>>` where `State` is `Pending(Vec<Callback>)` or `Done(Result<(), JournalError>)`. `Signal::complete(result)` swaps `Done`, drops the lock, then fires queued callbacks outside the critical section.

`JournalError` has a manual `Clone` impl (rebuilds the inner `io::Error` via `kind() + to_string()`) so each callback gets an independent owned copy.

## `Journal`

Segmented append-only log with caller-assigned `u64` sequence numbers, opaque `u64` per-record `meta` slot, group commit + async-friendly durability notification, fast truncate/purge.

### Public API

```rust
struct JournalConfig { dir: PathBuf, segment_size_bytes: u64, durability: Durability }
// JournalConfig::new(dir) defaults: segment_size_bytes = 64 MiB, Durability::Consistent.

impl Journal {
    fn open(config: JournalConfig) -> Result<Self, JournalError>;
    fn close(self) -> Result<(), JournalError>;

    // Caller-assigned, strictly monotonic seq.
    fn append(&self, seq: u64, meta: u64, payload: &[u8]) -> Result<Notifier, JournalError>;

    // O(1).
    fn first_seq(&self) -> Option<u64>;
    fn last_seq(&self) -> Option<u64>;

    // Reads.
    fn read(&self, seq: u64) -> Result<Option<(u64, Vec<u8>)>, JournalError>;
    fn read_range(&self, range: impl RangeBounds<u64>) -> Result<Vec<(u64, u64, Vec<u8>)>, JournalError>;
    fn iter_range(&self, range: impl RangeBounds<u64>) -> Result<impl Iterator<Item = Result<(u64, u64, Vec<u8>), JournalError>> + '_, JournalError>;

    // Mutations. Truncate is exact (mid-segment OK). Purge is segment-aligned.
    fn truncate_after(&self, keep_seq: u64) -> Result<Notifier, JournalError>;
    fn purge_before(&self, seq: u64) -> Result<(), JournalError>;
}
```

The opaque `meta: u64` slot lets the future Raft adapter store the leader's term per record without forcing a payload decode — `(seq, term)` lookups for `get_key_log_ids` cost a single record header read instead of a full bincode deserialize.

### On-disk format

**Segment file** `seg-{base_seq:020}.log`:

```
[segment header, fixed 32 bytes]
    magic         8 bytes  b"ULTJSEG\0"
    format_ver    u16
    base_seq      u64
    created_at    u64       (unix nanos; informational)
    header_crc32  u32       over bytes 0..26
    pad           2 bytes

[records, repeated]
    len           u32       length of (seq + meta + payload)
    seq           u64
    meta          u64
    payload       (len - 16) bytes
    record_crc32  u32       over (seq + meta + payload)
```

Length-prefix + per-record CRC32 (same framing pattern as the existing `wal.rs`). Sequence is stored *inside* the record, so corruption + recovery can verify monotonicity.

### Group commit

mpsc channel from appenders → single bg writer thread:

```
recv() first record  →  drain try_recv() into batch
                     →  write all records to file
                     →  single sync_all() (Consistent only)
                     →  for each request in batch: signal Notifier
```

`Durability::Eventual` short-circuits caller waits — `append()` returns an already-resolved Notifier; the bg thread fsyncs on a 50 ms timer to bound staleness.

The append path validates monotonicity under the `WriterState` mutex *and* sends to the channel under the same lock, so channel order = seq order regardless of appender concurrency.

### Crash safety

- **Append (Consistent):** Notifier reflects actual fsync completion. Records that hadn't fsynced are absent on recovery.
- **Append (Eventual):** caller is told "queued"; durability is best-effort up to the next periodic fsync.
- **`truncate_after`:** Two-phase sentinel protocol. (1) truncate segment to `new_end`, (2) append a sentinel record (`meta = u64::MAX`, `payload = b"ULTJTRUNC"`) and fsync — intent is now durable, (3) truncate back to `new_end` removing the sentinel, (4) unlink later segments, fsync dir. Crash between (2) and (3) is recoverable: `Journal::open` detects the sentinel and re-runs the cleanup.
- **`purge_before`:** Pure segment unlinks. Idempotent.
- **Recovery (`Journal::open`):** Three phases —
  - **Torn-tail repair:** for each segment, if scan sees `had_torn_tail`, truncate to `last_durable_offset` + fsync.
  - **Sentinel completion:** backwards-search for a sentinel record as the last record. If found, truncate the segment at the sentinel's offset, drop later segments.
  - **State compute:** derive `first_seq` / `last_seq` from final scan results.
- Recovery is idempotent — after a recovery pass, no sentinel remains on disk; reopening produces identical state.

### Concurrency

Single bg writer thread. Multiple appenders supported via the channel — the `WriterState` mutex serializes seq claims with channel submission so order is preserved.

Reads currently take the same mutex. The plan's deferred work (open questions §12) is to wrap segments in `Arc<RwLock<Vec<Arc<SegmentRef>>>>` with per-segment `Mutex<File>`, allowing concurrent reads against immutable sealed segments. The current Mutex-serialized model is correct and not deadlock-prone (verified by `concurrent_reads_during_appends` test); profile-driven optimization is left for future work.

### Sparse index

Each `SegmentFile` maintains an in-memory sparse `(seq, byte_offset)` index, one entry per ~64 KiB. Currently unused on the read path — `Journal::read` linear-scans the segment via `scan()`. The bench task notes this and points at `binary_search` over the index as the optimization site.

## `StableValue<T>`

Durable, atomic single-value slot for tiny payloads (Raft `vote`, `committed`, `last_purged`). Two-slot rotation with generation-counter tie-break.

```rust
struct StableValueConfig { path: PathBuf, durability: Durability, max_payload_bytes: u32 }
// max_payload_bytes default = 4096 - 17.

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> StableValue<T> {
    fn open(config: StableValueConfig) -> Result<Self, StableValueError>;
    fn load(&self) -> Result<Option<T>, StableValueError>;          // O(1) cached after open
    fn store(&self, value: &T) -> Result<Notifier, StableValueError>;
    fn clear(&self) -> Result<Notifier, StableValueError>;
    fn close(self) -> Result<(), StableValueError>;
}
```

### On-disk layout

```
[file header, fixed 32 bytes]
    magic           8 bytes  b"ULTSVAL\0"
    format_ver      u16
    slot_size       u32
    crc32           u32      over bytes 0..14

[slot 0]   gen: u64 | state: u8 | len: u32 | bytes (slot_size − 17) | crc32
[slot 1]   identical layout
```

`store` alternates slots; the higher-`gen` slot with valid CRC wins on `load`. Both slots invalid = unrecoverable (`StableValueError::Corrupted`); single bad slot = normal post-crash state, the other slot has the older-but-valid value.

Why two slots, not append-and-truncate: a single-slot file has a torn-write window where neither old nor new is readable. Two slots = always one valid value, regardless of crash timing.

`bincode 2` is used for value serialization (`bincode::serde::encode_to_vec(value, bincode::config::standard())`).

## Tests

44 in-crate tests:
- 42 unit tests across `notifier`, `journal::segment`, `journal`, `stable_value`.
- 2 integration tests (`tests/journal_integration.rs`, `tests/stable_value_integration.rs`).

Coverage highlights:
- Framing / CRC roundtrips.
- Segment scan with torn tail.
- Monotonic-seq enforcement.
- Segment rotation at threshold.
- Range reads spanning segments.
- Concurrent appenders (200 threads).
- Callback fires inline from bg fsync thread.
- Eventual mode returns already-done Notifier; periodic fsync timer bounds staleness.
- `truncate_after` single-segment + cross-segment.
- `purge_before` segment-aligned + active-segment protection.
- Recovery: torn tail truncation; in-flight truncate completion via sentinel.
- StableValue corrupt-slot fallback.

Microbenchmarks live in `ultima_journal/benches/append_throughput.rs`:
- `append_consistent` at 64/256/1024/4096-byte payloads
- `append_eventual` at 64/256/1024
- `iter_range` over 1000 records from a 10 000-record journal

## Deferred work

Tracked in design spec §12 (preserved at `docs/superpowers/specs/2026-05-02-ultima-raft-storage-readiness-design.md`):

- `Arc<RwLock<Vec<Arc<SegmentRef>>>>` refactor for lock-free concurrent reads against sealed segments.
- Sparse-index seek in `Journal::read` (currently linear scan).
- mmap for sealed segments (decide via benchmarks).
- Tunable defaults (segment size, Eventual fsync interval, StableValue slot size).

## Cross-references

- **Design spec (covers both this and the pending snapshot_stream work):** `docs/superpowers/specs/2026-05-02-ultima-raft-storage-readiness-design.md`.
- **Pending companion plan:** `docs/superpowers/plans/2026-05-02-snapshot-stream-implementation.md` — `ultima_db::snapshot_stream` for `RaftStateMachine` build/install paths.
- **Future adapter:** the openraft `RaftLogStorage` + `RaftStateMachine` adapter is a downstream project. It depends on this crate plus the snapshot_stream work, plus a `_meta` table convention in the state machine to track `(applied_log_id, last_membership)` atomically with data writes.
