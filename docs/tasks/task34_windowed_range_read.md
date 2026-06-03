# Task 34: Windowed Range Read for the Journal

> Range-read counterpart to the point read in
> [task33](task33_windowed_point_read.md). "Form 1 (range variant)": segment
> pruning + coalesced span read.

## Summary

`Journal::read_range` previously `scan()`-ed every segment whole (up to 64 MiB each) and
filtered to the requested `[lo, hi]`, paying O(total journal size) even for a localized
range. This task makes it (1) prune segments whose `base_seq` range does not overlap
`[lo, hi]` (no I/O) and (2) for each overlapping segment, read only the index-bounded byte
span `[window(lo), window(hi))` via the new `SegmentFile::read_window`. The sparse index is
already installed at open (task33). No public API or on-disk format change.

## Implementation

- `SegmentFile::read_span(start, end)` — shared byte-window reader (`try_clone`+seek+
  `read_exact`), used by both `read_record` and `read_window`.
- `SegmentFile::read_window(lo, hi)` — `partition_point` finds the span covering `[lo, hi]`;
  reads it via `read_span`, decode-filters to `[lo, hi]`. Falls back to `scan` if the index
  is empty.
- `Journal::read_range` — prunes segments by `base_seq` boundaries, then concatenates each
  overlapping segment's `read_window`. `iter_range` is unchanged (eager wrapper, inherits
  the win).

## Durability, format, concurrency

Unchanged. Reads still hold the `WriterState` lock and the writer also acquires it, so the
writer is blocked during a read — `try_clone()`+seek cannot race its fd offset.

## CRC / integrity scope (intentional)

A *partial* range CRC-verifies only the records in the spans it reads, not whole segments —
the same intentional scope as `read_record`. A *full/unbounded* range reads and verifies
every segment, exactly as before. Full-segment validation remains in `scan` (recovery).

## Results

Machine: `Linux 7.0.0-15-generic x86_64`; filesystem `ext4` at `/`.
Benchmark: `cargo bench -p ultima-journal --bench append_throughput -- range_read`.
~16 MiB single segment (16384 × 1 KiB records), `read_range` of a 64-seq localized slice at
a rotating position. Median latency:

| metric | before (full scan) | after (windowed) | change |
|--------|--------------------|------------------|--------|
| range_read | 4,582.0 µs | 21.8 µs | **−99.5% (~210× faster)** |

A full-range `read_range(..)` is unchanged (each segment's window spans its whole content).

## Analysis

The windowed range read drops localized-range latency from ~4.58 ms to ~21.8 µs — a ~210×
speedup — on a warm page cache. As with the point-read win (task33, ~448×), the gain is
from avoiding the per-call work the old full scan did regardless of cache state: copying
~16 MiB out of the page cache, CRC-verifying all 16 384 records, and allocating a `Vec` for
every decoded payload. The windowed read covers the index spans that bound the 64-record
slice (typically 1–2 ~64 KiB index windows, i.e. ~128 KiB), so it copies and verifies
roughly 128× less data than the point-read window. The lower factor vs. the point read
(~210× vs. ~448×) is expected: a 64-record range may straddle two index windows and must
decode and allocate all 64 payloads, while a point read reads one window and stops after
finding a single record. Across a multi-segment journal, segment pruning additionally skips
non-overlapping segments entirely (zero I/O), compounding the benefit.
