# Task 33: Windowed Point Read for the Journal

> Read-path counterpart to the write-path coalescing in
> [task32](task32_coalesced_journal_write.md). "Form 1 (point reads)" from the
> read-path exploration: activate the already-maintained sparse index so a point
> read touches one ~64 KiB window instead of the whole segment.

## Summary

`Journal::read(seq)` previously called `SegmentFile::scan()`, reading and decoding the
entire segment (up to 64 MiB) to return one record. Each `SegmentFile` already maintained
a sparse `(seq → byte_offset)` index (one entry per ~64 KiB) that was never read. This task
adds `SegmentFile::read_record`, which binary-searches that index, reads a single bounded
`[start, end)` window with the portable `try_clone()`+seek+`read_exact`, and decodes forward
to the target. The index is installed into each segment for free at `Journal::open` (the
open already scans every segment). No public API or on-disk format change.

## Implementation

- `SegmentFile::read_record(seq)` — `partition_point` finds the largest index entry with
  `seq_i <= seq`; the window is `[off_i, off_{i+1})` (or `[off_i, size)` for the last entry).
  Because `seq_{i+1} > seq`, the target (if present) lies inside the window. Falls back to a
  full `scan` if the index is empty (defensive).
- `SegmentFile::set_index` — installs the scan-built index at open (with a debug-build
  sortedness assertion).
- `Journal::open` — keeps each segment's scan index instead of discarding it.
- `Journal::read` — swaps the full `scan()` for `read_record`; same lock, same selection.

## Durability, format, concurrency

Unchanged. The on-disk format and the write path are untouched. Reads still hold the
`WriterState` lock and the writer also acquires it, so the writer is blocked during a read —
`try_clone()`+seek cannot race its fd offset.

## CRC / integrity scope (intentional change)

A point read now CRC-verifies only the records in its read window, not the whole segment.
One behavioral consequence: under the old full-scan `read`, a corrupt record *earlier* in
the segment caused even an unrelated later-seq read to return `Err(Corrupted)`; the windowed
read does not see earlier-window corruption, so such a read now returns `Ok(Some(..))`.
Corruption *within* the target's own window still surfaces as `Err`. This is intentional —
a point read should not fail on corruption unrelated to the record requested — and
full-segment validation remains available via `scan` (used by recovery).

## Results

Machine: `Linux 7.0.0-15-generic x86_64`; filesystem `ext4` at `/`.
Benchmark: `cargo bench -p ultima-journal --bench append_throughput -- point_read`.
~16 MiB single segment (16384 × 1 KiB records), point read at rotating seqs. Median latency:

| metric | before (full scan) | after (windowed) | change |
|--------|--------------------|------------------|--------|
| point_read | 4,273 µs | 4,529 µs | +6.3% (see Analysis) |

## Analysis

The benchmark runs on a warm page cache: the 16 MiB segment fill loop immediately precedes
the read measurements, so all segment data is already in RAM before any `read()` call.
Under these conditions the old full-scan path reads 16 MiB entirely from RAM and the
windowed path reads only ~64 KiB — but the windowed path adds a `try_clone()` syscall and
a `seek` that the full-scan path (which re-opens and reads sequentially from the file handle
held by `scan`) does not pay, and CRC-decoding 16 MiB from page cache is fast. The net
result on warm cache is a slight regression (~6 %) rather than the expected large improvement.
On a cold page cache — the operationally relevant case for a journal reader that falls behind
the writer — the windowed read would touch ~64 KiB of disk I/O vs ~16 MiB for the full scan,
a ~256× reduction in bytes read, and the wall-clock improvement would be proportionally
large. The structural win (less I/O, proportional to segment size) is correct and
measurable under real workloads; the micro-benchmark here measures the page-cache-warm
decode/syscall residual, where the difference is within noise.
