# Design: Coalesced Batch Writes for `ultima_journal`

**Date:** 2026-06-01
**Status:** Draft for review
**Crate:** `ultima_journal` (`ultima_db/ultima_journal`)

## Problem

`ultima_journal` already has **group commit**: the background `writer_loop` drains
the mpsc channel into a `batch`, writes all records, and issues a **single fsync per
batch** (`src/journal/writer.rs`). The expensive fsync is already amortized across the
batch.

What it does **not** have is **write-coalescing**. `write_batch` loops over the batch
calling `SegmentFile::append_record` once per record, and each `append_record` does its
own `seek(EOF)` + `write_all` (`src/journal/segment.rs`). A 256-record batch therefore
issues **256 `seek` + 256 `write_all` syscalls**, then one fsync.

This is exactly the per-entry pattern that ultima_db's WAL improved in **task30/task31
(Coalesced WAL writes)**: collapsing N `write_all` calls into a single buffered
`write_all` per batch, keeping `sync_all` unchanged, measured **+24–35% Eventual batch
throughput** with identical durability semantics. The journal never adopted this.

### Relationship to the "latest WAL improvements"

| Improvement | ultima_db WAL | ultima_journal |
|-------------|---------------|----------------|
| One fsync per batch (group commit) | ✅ | ✅ |
| Durable-seq watermark (task28) | ✅ | ✅ (`SeqWatermark`) |
| Fsync failure poisoning (task29) | ✅ | ✅ |
| **Write-coalescing (task30/31)** | ✅ (`WalWrite::Coalesced`) | ❌ **← this design** |

This design brings the one missing piece — the write-coalescing half of group commit —
into the journal.

## Goals

- Collapse the per-batch write path from N `write_all` (+ N `seek`) to **one `write_all`
  per segment touched by the batch** (one in the common single-segment case).
- **Zero change** to: on-disk format, fsync/durability semantics, recovery, the
  `SeqWatermark`, the group-commit loop, and the public API.
- Quantify the gain with a **before/after benchmark** measured on one machine.

## Non-Goals

- Read-path optimization (sparse-index seek, lock-free reads) — deferred per
  `task26_journal.md` §Deferred work.
- A config option (`WalWrite`-style). Decided **always-on**: within-batch coalescing is
  strictly fewer syscalls with identical durability and on-disk bytes, and the journal's
  "batch" is an internal concept (unlike ultima_db, where `PerEntry` was a documented
  public default that had to be preserved). No public API change.
- Changing `Durability` modes or fsync cadence.

## Architecture

The hot path is `writer_loop → write_batch → SegmentFile::append_record`. The change is
entirely **below the fsync boundary**: only *how the already-formed batch reaches the
file* changes. The fsync, watermark publish, and signal fan-out are untouched.

### Component 1: `SegmentFile::append_records` (`segment.rs`)

```rust
/// Append many records with a single seek + write_all. Caller enforces
/// monotonic seq. Updates size and the sparse index per record using the
/// same 64 KiB-gap rule as `append_record`.
pub fn append_records(
    &mut self,
    records: &[(u64, u64, &[u8])], // (seq, meta, payload)
) -> Result<(), JournalError>
```

- Encodes every record into one `Vec<u8>` via the existing `encode_record`.
- One `self.file.seek(SeekFrom::Start(self.size))`, one `self.file.write_all(&buf)`.
- Advances `self.size` by the buffer length.
- Pushes sparse-index entries per record using the **identical** `SPARSE_INDEX_GAP`
  (64 KiB) rule as `append_record`, so the (currently unused) index stays byte-accurate
  for the future read optimization.
- `append_record` becomes a thin wrapper:
  `self.append_records(&[(seq, meta, payload)])` — existing call sites and tests
  unaffected.

> The per-write `seek(EOF)` is retained (now once per run instead of once per record).
> It is still required because `scan()` uses `self.file.try_clone()`, which on Unix
> shares the file offset; a concurrent scan can move the writer's position. Decoupling
> reads to drop the seek is out of scope (read-path work, deferred).

### Component 2: `write_batch` rotation grouping (`writer.rs`)

Walk the batch maintaining a **projected** current-segment size
(`on-disk size + buffered bytes`):

```
prev = st.last_seq
run: Vec<(seq, meta, &payload)> = []
for req in batch:
    validate monotonic(req.seq vs prev)         # else flush run, return Err
    validate size(total <= segment_size)         # else flush run, return Err
    projected = current_seg_size + bytes_in(run)
    if no segment yet OR projected >= segment_size:
        flush run -> current segment (append_records)
        rotate: create new segment (base_seq = req.seq)
        run = []
    run.push((req.seq, req.meta, &req.payload))
    prev = Some(req.seq)
flush run -> current segment
update st.first_seq / st.last_seq
return Ok(st.last_seq)   # batch high-water seq, unchanged contract
```

- Preserves today's exact rotation behavior: a segment overshoots `segment_size` by at
  most one record; the `total > segment_size` guard still guarantees one record fits a
  fresh segment.
- Common case (batch fits the active segment) = exactly **one** `append_records` call =
  one `write_all`.

## Error Handling

- Per-record **monotonic-seq** (`NonMonotonicSeq`) and **payload-size**
  (`PayloadTooLargeForSegment`) checks stay in the loop, in order, before a record joins
  a run — same errors as today.
- On a check failure, **flush the already-accumulated good prefix** before returning
  `Err`, preserving today's "earlier records in the batch are persisted" semantics and
  keeping `last_seq` consistent. (In practice these checks are redundant defense —
  `append()` validates at enqueue — so this path is rarely hit.)
- `write_batch` still returns the batch high-water seq; `writer_loop` fsyncs once and
  publishes the watermark exactly as now. **Durability and recovery are bit-for-bit
  identical**: same framed bytes, same single `sync_all`, same torn-tail handling.

## Trade-offs

- **One transient extra copy** of the batch bytes into the coalescing buffer, bounded by
  the channel backlog drained into the batch. This is the only cost; it is the same
  trade-off task31 accepted.

## Testing

### Correctness
- **Direct unit test** of `append_records` (deterministic): a multi-record slice →
  reopen → `scan()` verifies all records, payloads, and that `size`/offsets are
  byte-for-byte identical to a sequence of `append_record` calls.
- **Rotation test**: a coalesced run that crosses `segment_size` creates the correct
  segment files with correct `base_seq`.
- **Regression**: all existing `mod.rs` / `segment.rs` tests stay green (recovery, torn
  tail, `read_range`, purge, `truncate_after`) — these enforce the format-invariance
  guarantee.

### Benchmarking (baseline-first — required for a valid before/after)

The existing one-at-a-time benches never form batches, so a batched bench must be added
**and measured before** the implementation change, using criterion's baseline feature:

1. **Add** `bench_append_batched` to `benches/append_throughput.rs`: Eventual mode,
   burst of ~256 records per `iter` (submit all, wait the last notifier),
   `Throughput::Bytes`, sizes `[64, 256, 1024, 4096]`. Commit this as a standalone
   baseline artifact (against the current per-record code).
2. **Capture baseline:**
   `cargo bench --bench append_throughput -- --save-baseline before`
3. **Implement** coalescing (Components 1 + 2).
4. **Compare:**
   `cargo bench --bench append_throughput -- --baseline before`
   Criterion prints the per-size % change directly (expected: +24–35% Eventual batch
   band from task31).
5. **Record** the before/after table in the task doc (task32), same format as task31's
   "View 3", noting machine + filesystem (numbers are disk-dependent).
6. **Guard**: keep the existing `consistent` / `eventual` single-commit benches —
   coalescing must **not** regress single-commit latency (size-1 batches; task31
   confirmed this is a no-op there).

## Deliverables

- `src/journal/segment.rs`: `append_records`; `append_record` becomes a wrapper.
- `src/journal/writer.rs`: `write_batch` rotation grouping.
- `benches/append_throughput.rs`: `bench_append_batched`.
- `docs/tasks/task32_coalesced_journal_write.md`: canonical doc + before/after results.
- New unit tests in `segment.rs` (and rotation coverage).
- `cargo clippy -- -D warnings` clean; all existing tests green.
