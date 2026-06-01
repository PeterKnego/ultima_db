# Task 32: Coalesced Batch Writes for the Journal

> Brings the write-coalescing half of group commit (ultima_db's
> [task30](task30_wal_backends.md)/[task31](task31_coalesced_wal_write.md)) into
> `ultima_journal`. The journal already had one fsync per batch; it lacked the
> single-`write_all`-per-batch that task31 added to the store's WAL.

## Summary

`write_batch` previously issued one `seek` + one `write_all` per record, then a
single fsync for the batch. This task coalesces every record destined for the same
segment into one buffer and issues a single `write_all` per segment (one in the
common single-segment batch), keeping the single fsync, the framed on-disk format,
the `SeqWatermark`, and recovery bit-for-bit identical.

Always-on (no config flag): within-batch coalescing is strictly fewer syscalls with
no durability or format change, and the journal's "batch" is internal — unlike
ultima_db, there was no documented `PerEntry` default to preserve.

## Implementation

- `SegmentFile::append_records(&[(seq, meta, &payload)])` — encodes the run into one
  buffer (via a shared `encode_record_into` framing helper), one `seek` + one
  `write_all`, advances `size`, maintains the sparse index per record with the same
  64 KiB gap rule. `append_record` is now a thin wrapper.
- `write_batch` groups the batch into per-segment runs using a projected-size counter
  that mirrors the old `seg.size() >= segment_size` rotation rule, flushing each run
  via `append_records`. Monotonic-seq and payload-size guards are retained; on failure
  the accumulated good prefix is flushed before returning `Err` (matches prior
  per-record semantics).

## Durability & format

Unchanged. Same framed bytes, same single `sync_all` per batch, same torn-tail and
sentinel recovery. The change is entirely below the fsync boundary.

## Results

Machine: `Linux 7.0.0-15-generic x86_64`; filesystem `ext4` at `/`.
Benchmark: `cargo bench -p ultima-journal --bench append_throughput -- append_batched_eventual`.
Burst = 256 records/iter, Eventual durability. Medians:

### Eventual batch throughput (MiB/s, median) — before vs after

| Payload | before (per-record) | after (coalesced) | change |
|---------|-------------------|-------------------|--------|
| 64 B    | 29.4 MiB/s        | 57.8 MiB/s        | +97%   |
| 256 B   | 102.1 MiB/s       | 169.7 MiB/s       | +66%   |
| 1 KiB   | 311.5 MiB/s       | 484.0 MiB/s       | +55%   |
| 4 KiB   | 701.9 MiB/s       | 854.3 MiB/s       | +22%   |

### Guard: Consistent single-commit latency

No meaningful change: all four sizes landed within noise (p64: no change detected,
p256: within noise threshold, p1024: no change detected, p4096: +6.9% apparent but
with 7 outliers — within expected measurement noise). Coalescing is a no-op for
size-1 batches, consistent with task31's finding.

## Analysis

The measured gains substantially exceed task31's +24–35% band for Eventual batch
throughput, ranging from +22% at 4 KiB to +97% at 64 B. The gradient is expected:
small payloads are dominated by per-record syscall overhead, so eliminating N−1
`write_all` calls per 256-record batch yields the largest relative win. Large
payloads (4 KiB) are more bandwidth-bound, so the gain is smaller and closer to
task31's range. The journal's write path previously had no coalescing at all (unlike
the WAL which had already batched some operations), making the absolute improvement
larger than what task31 saw when it moved from PerEntry to Coalesced in the store.
