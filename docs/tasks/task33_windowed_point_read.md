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
| point_read | 4,272.8 µs | 9.54 µs | **−99.8% (~448× faster)** |

## Analysis

The windowed read drops point-read latency from ~4.27 ms to ~9.5 µs — a ~448× speedup —
even though the benchmark runs on a **warm page cache** (the fill loop leaves all 16 MiB in
RAM). The gain is therefore not from avoiding disk I/O; it is from avoiding the per-read work
the old full scan did regardless of cache state: `read_to_end` copying ~16 MiB out of the
page cache, CRC-verifying all 16384 records, and allocating a `Vec` for every decoded
payload. The windowed read copies ~64 KiB, CRC-verifies ~60 records, and allocates ~60
payloads — roughly the ~256× ratio of one 64 KiB window to the 16 MiB segment, matching the
measured factor. The win scales with segment size: the window stays ~64 KiB while a larger
segment makes the full scan proportionally more expensive. On a **cold** cache the absolute
gap would be even larger, since the full scan would additionally fault ~16 MiB from disk
versus ~64 KiB for the window.

> Measurement note: an earlier run reported a spurious ~6% regression because the criterion
> bench binary was not relinked against the windowed library (stale binary measured the old
> full-scan code for both before and after). Forcing a rebuild produced the numbers above;
> a fresh single-process timing of 2000 reads independently corroborated ~10 µs/read.
