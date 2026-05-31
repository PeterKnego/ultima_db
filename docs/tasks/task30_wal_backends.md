# Task 30: WAL Write-Backend Comparison

> Builds on [task29_durability_failure_handling.md](task29_durability_failure_handling.md),
> which introduced the `WalSink` trait and `FileSink` production implementation.

## Summary

Implements and benchmarks four `WalSink` backends — `FileSink` (baseline), `BufferedFileSink`,
`MmapSink`, and `IoUringSink` — to measure the cost of durable WAL appends and determine
whether a faster write mechanism is worth adopting over the current `FileSink` baseline.

## Goal

The WAL's append-only I/O path is on the critical path for every durable commit. The existing
`FileSink` issues one `write_all` per entry (the framed record — header + body + CRC — is built
by `frame_entry` into a single buffer) plus `sync_all`. The question is: can
coalescing writes (buffered I/O), memory-mapped I/O, or kernel-bypass async I/O (io_uring) do
meaningfully better, and if so, at what complexity cost?

The benchmark measures three views per backend:
1. **Consistent single-commit latency** — one commit that blocks until fsynced.
2. **Eventual single-commit caller latency** — fire-and-forget hand-off to the WAL thread.
3. **Eventual batch throughput** — 256-entry batch drained to a single fsync (group commit).

## Architecture

All four sinks implement the existing `WalSink` trait (`append(&WalEntry)` + `sync()`) unchanged.
No new trait surface was added. Sink selection at runtime uses `WalSinkKind` via
`WalHandle::with_sink_kind`. `frame_entry` produces a byte-identical on-disk format
(`[len: u32][WalEntry bytes][crc32: u32]`) regardless of sink, so WAL files written by any
sink are readable by `read_wal`. A `len == 0` sentinel was added to `read_wal` to mark
end-of-log, which is necessary for `MmapSink`'s pre-sized zero tail.

**Sinks:**

- **`FileSink`** (baseline, `fswrite`): One `write_all` per entry (the framed record — header +
  body + CRC — is built by `frame_entry` into a single buffer) followed by `sync_all` per batch.
  Simple, no buffering, no unsafe.
- **`BufferedFileSink`** (`buffered`): `append` only pushes framed bytes into an in-memory
  `Vec<u8>` (zero syscalls). `sync` issues one `write_all` of the entire batch buffer, then
  `fdatasync`. For an N-entry batch this means N `write_all` calls (fswrite) become 1 (buffered),
  and `sync_all` is replaced by `fdatasync` (skips file-metadata sync). No unsafe.
- **`MmapSink`** (`mmap`, bench-only/experimental): Pre-sizes the WAL file to a fixed capacity
  and memory-maps it. `append` copies bytes in via `memcpy` (safe slice assignment) and
  advances a write cursor; `sync` calls `flush_range(0, write_head)`/`msync(MS_SYNC)` over
  the entire written prefix `[0, write_head)`.
  Requires `unsafe`. Not safe with `prune_wal`/checkpoint (truncation under a live mapping
  causes SIGBUS).
- **`IoUringSink`** (`iouring`, bench-only/experimental, Linux + `wal-iouring` feature):
  Issues a linked `Write` + `Fsync(DATASYNC)` pair through io_uring at explicit file offsets.
  The ring is created with depth 8; each `sync` submits exactly 2 SQEs — a linked `Write` +
  `Fsync(DATASYNC)` — and awaits both completions. Pipelining multiple batches concurrently
  (deeper effective queue depth) is future work. Requires `unsafe` and is Linux-only. Not safe
  with `prune_wal`/checkpoint (offset-based writes break after file truncation/rewrite).

## Results

Machine: Linux, ext4 (real disk, fsync non-trivial — confirmed by bench startup line
`[wal_bench] WAL dir = .../target/wal-bench | mount = / | fs = ext4`). Each Consistent/Eventual
single-commit benchmark uses 30 samples; the batch throughput group uses 20 samples.

### View 1: Consistent single-commit latency (µs, median)

One commit per iteration, blocks until fsynced.

| Size  | fswrite   | buffered  | mmap      | iouring   |
|-------|-----------|-----------|-----------|-----------|
| 1 KiB | 430.6 µs  | 557.1 µs  | 487.6 µs  | 669.9 µs  |
| 2 KiB | 430.5 µs  | 438.4 µs  | 676.5 µs  | 518.9 µs  |
| 4 KiB | 483.7 µs  | 627.3 µs  | 530.9 µs  | 701.8 µs  |
| 8 KiB | 573.5 µs  | 466.5 µs  | 673.5 µs  | 589.5 µs  |
| 16 KiB| 518.8 µs  | 673.6 µs  | 556.8 µs  | 714.4 µs  |

### View 2: Eventual single-commit caller latency (µs, median)

Fire-and-forget hand-off; fsync runs on the background thread and is off-clock.

| Size  | fswrite  | buffered  | mmap     | iouring  |
|-------|----------|-----------|----------|----------|
| 1 KiB | 1.37 µs  | 1.56 µs   | 1.50 µs  | 1.94 µs  |
| 2 KiB | 1.50 µs  | 1.47 µs   | 1.87 µs  | 2.07 µs  |
| 4 KiB | 1.57 µs  | 5.63 µs * | 1.91 µs  | 2.01 µs  |
| 8 KiB | 2.51 µs  | 2.23 µs   | 2.43 µs  | 2.40 µs  |
| 16 KiB| 3.08 µs  | 2.97 µs   | 3.01 µs  | 3.11 µs  |

\* The buffered/4KiB eventual single result (5.63 µs median) had 4 outliers and a very wide
confidence interval (1.86–15.26 µs); the distribution was bimodal and the median is not
representative. All other buffered entries behaved normally.

### View 3: Eventual batch throughput (MiB/s, median) — 256 entries, single drain

| Size  | fswrite    | buffered   | mmap       | iouring    |
|-------|------------|------------|------------|------------|
| 1 KiB | 95.5 MiB/s | 138.4 MiB/s| 121.4 MiB/s| 123.0 MiB/s|
| 2 KiB | 150.5 MiB/s| 192.5 MiB/s| 164.9 MiB/s| 185.5 MiB/s|
| 4 KiB | 180.7 MiB/s| 246.2 MiB/s| 207.6 MiB/s| 225.8 MiB/s|
| 8 KiB | 219.6 MiB/s| 282.7 MiB/s| 226.9 MiB/s| 276.3 MiB/s|
| 16 KiB| 234.1 MiB/s| 302.7 MiB/s| 269.2 MiB/s| 288.0 MiB/s|

## Analysis

### Consistent single-commit: fsync-floor dominates, no sink wins clearly

All four sinks fall in the 430–715 µs range, with no consistent ordering and overlapping
confidence intervals across sizes. The device-flush floor completely dominates; there is no
meaningful differentiation here. The buffered and iouring sinks show higher variance (wider
CI) in several cells, suggesting their overhead is less uniform than the simple fswrite path.
`fswrite` is the most consistent performer at this latency level. No sink can reduce the
fsync cost — the only way to improve Consistent latency is hardware or OS-level (NVMe, battery-
backed cache, etc.).

### Eventual single-commit caller latency: all sinks ~1–3 µs, no differentiation

Fire-and-forget hand-off is 1.4–3.1 µs for all sinks across all sizes (excluding the
bimodal buffered/4KiB outlier). This is pure "write to channel and return" cost; the actual
fsync is entirely off-clock. All sinks are within noise of each other. `fswrite` is
marginally fastest at small sizes (1.37 µs at 1KiB vs 1.56–1.94 µs for alternatives).

### Eventual batch throughput: buffered wins, others cluster below

This is where write-backend choice matters. The `BufferedFileSink` is the clear winner:

| Size  | buffered vs fswrite | iouring vs fswrite | mmap vs fswrite |
|-------|--------------------|--------------------|-----------------|
| 1 KiB | +45%               | +29%               | +27%            |
| 2 KiB | +28%               | +23%               | +10%            |
| 4 KiB | +36%               | +25%               | +15%            |
| 8 KiB | +29%               | +26%               | +3%             |
| 16 KiB| +29%               | +23%               | +15%            |

`BufferedFileSink` beats fswrite by 28–45% across all sizes. `IoUringSink` delivers a
competitive 23–29% win — very close to buffered at 8KiB and 16KiB (276 vs 283 MiB/s at
8KiB; 288 vs 303 MiB/s at 16KiB). `MmapSink` is in third place: its gains are real but
smaller (3–27%), and it underperforms buffered by 10–17% consistently despite the more
complex mmap machinery. The throughput win for buffered comes from two compounding factors: for an N-entry batch,
`FileSink` issues N `write_all` calls (one per entry) + one `sync_all`, while
`BufferedFileSink` coalesces the whole batch into 1 `write_all` + one `fdatasync`. The
mmap and io_uring sinks similarly batch their writes per `sync` call, reducing the number
of individual write operations the background thread performs before flushing.

## Recommendation

**Adopt `BufferedFileSink` as the new default production sink.**

It delivers 28–45% higher Eventual-mode batch throughput over `FileSink` with no unsafe
code, no platform restrictions, and no dependency on io_uring availability. Crucially, it
is safe with `prune_wal` and checkpoint (no live file mappings or offset assumptions to
invalidate). The Consistent-latency path is fsync-dominated and no sink helps there.

`IoUringSink` comes close to `BufferedFileSink` on throughput (within ~5% at larger sizes)
but adds: an `unsafe` block, a Linux-only platform lock, a `wal-iouring` feature flag, and
the prune-safety hazard. That complexity is not justified by the marginal gain in this
measurement. If queue depth > 1 were implemented (pipelining multiple entries per ring
submission), the picture could change — that is left as future work.

`MmapSink` finishes last among the three non-baseline sinks: it adds `unsafe`, a SIGBUS
hazard under prune/truncation, a pre-sizing policy question, and the `len==0` sentinel
complexity to `read_wal`, for a throughput gain that is consistently below both buffered
and iouring. It is not recommended for production.

## Caveats / Experimental Status

- **`MmapSink` and `IoUringSink` are experimental and bench-only.** They are not wired into
  `StoreConfig` or any public API surface. They exist only to provide a fair apples-to-apples
  comparison in the microbenchmark.
- **`MmapSink` is not safe with `prune_wal` or checkpoint.** Truncating or replacing the WAL
  file while a `mmap` mapping is live causes SIGBUS on the next access to the now-unmapped
  pages. Any production use would require a safe hand-off protocol (remap on rotation).
- **`IoUringSink` is not safe with `prune_wal` or checkpoint.** It tracks the write position
  as an explicit byte offset into the file. After a prune rewrite or checkpoint that replaces
  the WAL file, the offset is stale and writes land at the wrong position. Additionally, it
  is Linux-only and requires the `wal-iouring` feature flag.
- **`FileSink` and `BufferedFileSink` have no such restrictions.** They use the OS file
  position and are compatible with the existing prune, checkpoint, and recovery paths.
- **io_uring uses a ring of depth 8; each `sync` submits exactly 2 SQEs** — a linked
  `Write` + `Fsync(DATASYNC)` — and awaits both completions. Pipelining multiple batches
  concurrently (deeper effective queue depth) is future work that could improve iouring's
  throughput advantage.
- **Sink selection is not exposed via `StoreConfig`.** The `WalSinkKind` enum and
  `WalHandle::with_sink_kind` are internal (`#[cfg(feature = "bench-internals")]`). Exposing
  a sink selector in `StoreConfig` is a separate decision deferred until the production sink
  transition is made.
- **Bench numbers are on a single machine (Linux, ext4).** Results on NVMe vs spinning disk,
  or with a battery-backed write cache, will differ — particularly for Consistent-mode
  latency, which is entirely fsync-dominated.

## Tests

The four sinks share the WAL read/write test suite via the `WalSink` trait boundary. No
sink-specific behavioral tests were added; the bench itself exercises append + drain round-
trips. The `read_wal` `len==0` sentinel was covered by the existing `read_wal` round-trip
tests.

## References

- [task13_persistence.md](task13_persistence.md) — WAL file format and recovery.
- [task15_three_phase_consistent_persistence.md](task15_three_phase_consistent_persistence.md) — Consistent-mode fsync protocol.
- [task29_durability_failure_handling.md](task29_durability_failure_handling.md) — `WalSink` trait, `WalPoison`, `FileSink` production impl.
