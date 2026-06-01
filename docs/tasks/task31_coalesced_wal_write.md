# Task 31: Coalesced WAL Write Configuration Option

> Builds on [task30_wal_backends.md](task30_wal_backends.md), which measured four WAL sink
> backends and recommended adopting write-coalescing for batch throughput.

## Summary

Exposes `WalWrite::Coalesced` as the production-safe write-coalescing option for
`Persistence::Standalone`. It captures the batch-throughput win from task30's
write-coalescing measurement — 24–35% over the `PerEntry` baseline — using a single
`write_all` per batch and a full `sync_all` (no `fdatasync`), keeping the durability
guarantee identical to the existing default.

## Goal

Task 30 showed that its `BufferedFileSink` (the `buffered` column) lifted Eventual-mode
batch throughput by 28–45% over the per-entry baseline. That sink achieved the gain
through two compounding changes: write-coalescing AND switching
`sync_all` to `fdatasync`. The `fdatasync` half is an intentional metadata-sync skip —
reasonable on stable hardware, but a sharper durability trade-off than the existing
baseline, which is why it remains bench-only/experimental.

The goal of this task is narrower: expose write-coalescing alone, paired with the same
`sync_all` that `FileSink` already uses. This gives production users the throughput win
from batching writes without changing the fsync semantics.

## Public API

```rust
/// Selects how WAL entries are written to disk within each sync cycle.
/// Orthogonal to `Durability` (which controls *when* `commit()` returns).
pub enum WalWrite {
    /// One `write_all` per entry, followed by `sync_all` per batch (default).
    PerEntry,
    /// Coalesce all entries in the batch into one `write_all`, then `sync_all`.
    Coalesced,
}
```

`WalWrite` is re-exported at `ultima_db::WalWrite` (alongside `Durability`). It is a
field on `Persistence::Standalone`:

```rust
Persistence::Standalone {
    dir: PathBuf,
    durability: Durability,  // Consistent | Eventual
    wal_write: WalWrite,     // PerEntry | Coalesced  ← new field
}
```

**The two axes are orthogonal** — all four combinations are valid:

| `wal_write`  | `durability`   | Meaning                                           |
|--------------|----------------|---------------------------------------------------|
| `PerEntry`   | `Consistent`   | One write+fsync per commit, blocks until durable  |
| `PerEntry`   | `Eventual`     | One write per entry, fsync off-clock (default)    |
| `Coalesced`  | `Consistent`   | Batched write+fsync per commit, blocks            |
| `Coalesced`  | `Eventual`     | Batched write, fsync off-clock (recommended)      |

**Default is `PerEntry`**: behavior is unchanged for existing code. Adding `wal_write`
to `Persistence::Standalone` is a breaking change; existing struct literals must add
`wal_write: WalWrite::PerEntry` (or use `..` spread from a helper).

## Architecture

The underlying mechanism reuses `BufferedFileSink` from task30, parameterised by a
`datasync: bool` flag:

```
WalSinkKind::Coalesced    → BufferedFileSink { datasync: false }  → sync_all  (production)
WalSinkKind::BufferedFile → BufferedFileSink { datasync: true  }  → fdatasync (bench-only)
```

`Store::new` maps `wal_write: WalWrite::Coalesced` → `WalSinkKind::Coalesced` via
`WalHandle::with_sink_kind`. `WalWrite::PerEntry` maps to `WalSinkKind::FsWrite`
(existing `FileSink`), so no code path changes for the default.

`BufferedFileSink::sync` buffers all pending framed entries from `self.buf`, issues one
`write_all`, then calls `sync_all` (`datasync=false`) or `fdatasync` (`datasync=true`).
`FsWrite` (`FileSink`) still calls `write_all` per entry before `sync_all`.

Both `FileSink` and `BufferedFileSink` use the OS file position — no offset arithmetic,
no live mappings — so `Coalesced` is fully compatible with `prune_wal`, checkpoint, and
recovery, unlike the experimental `mmap` and `iouring` sinks.

## Results

Machine: Linux, ext4 (real disk — confirmed by bench startup line
`[wal_bench] WAL dir = .../target/wal-bench | mount = / | fs = ext4`).
Benchmark: `cargo bench --bench wal_bench --features "bench-internals wal-iouring"`.
Sample counts: 30 (consistent/eventual single), 20 (batch). All medians below.

### View 1: Consistent single-commit latency (µs, median)

One commit per iteration, blocks until fsynced. fsync-dominated; no sink wins clearly.

| Size   | fswrite  | coalesced | buffered | mmap     | iouring  |
|--------|----------|-----------|----------|----------|----------|
| 1 KiB  | 437.3 µs | 419.1 µs  | 413.1 µs | 543.2 µs | 578.2 µs |
| 2 KiB  | 431.3 µs | 468.3 µs  | 434.7 µs | 521.3 µs | 541.5 µs |
| 4 KiB  | 489.0 µs | 479.3 µs  | 454.8 µs | 566.3 µs | 588.8 µs |
| 8 KiB  | 509.7 µs | 476.1 µs  | 565.6 µs | 563.7 µs | 618.5 µs |
| 16 KiB | 512.3 µs | 491.3 µs  | 564.0 µs | 608.4 µs | 623.6 µs |

All five sinks fall in the 410–640 µs range with overlapping confidence intervals. The
device-flush floor dominates entirely; no write strategy can reduce the fsync cost.
`coalesced` and `fswrite` are indistinguishable from each other here, as expected.

### View 2: Eventual single-commit caller latency (µs, median)

Fire-and-forget hand-off; fsync runs on the background thread and is off-clock.

| Size   | fswrite | coalesced | buffered | mmap    | iouring |
|--------|---------|-----------|----------|---------|---------|
| 1 KiB  | 1.21 µs | 1.34 µs   | 1.46 µs  | 1.44 µs | 1.53 µs |
| 2 KiB  | 1.58 µs | 1.74 µs   | 1.49 µs  | 1.77 µs | 1.57 µs |
| 4 KiB  | 1.80 µs | 1.88 µs   | 1.60 µs  | 1.91 µs | 1.98 µs |
| 8 KiB  | 2.23 µs | 2.18 µs   | 2.05 µs  | 2.32 µs | 2.37 µs |
| 16 KiB | 3.01 µs | 3.15 µs   | 3.05 µs  | 3.11 µs | 3.19 µs |

All sinks fall in the 1.2–3.2 µs range — pure "copy to channel and return" cost. There
is no meaningful differentiation; all values are within noise of each other.

### View 3: Eventual batch throughput (MiB/s, median) — 256 entries, single drain

This is where write-backend choice matters. The key story is the `coalesced` column.

| Size   | fswrite     | coalesced   | buffered    | mmap        | iouring     |
|--------|-------------|-------------|-------------|-------------|-------------|
| 1 KiB  | 104.9 MiB/s | 138.8 MiB/s | 140.4 MiB/s | 124.8 MiB/s | 128.4 MiB/s |
| 2 KiB  | 151.6 MiB/s | 196.5 MiB/s | 190.0 MiB/s | 175.1 MiB/s | 186.5 MiB/s |
| 4 KiB  | 186.5 MiB/s | 252.2 MiB/s | 250.5 MiB/s | 222.2 MiB/s | 239.3 MiB/s |
| 8 KiB  | 214.0 MiB/s | 282.9 MiB/s | 279.9 MiB/s | 258.9 MiB/s | 272.8 MiB/s |
| 16 KiB | 245.5 MiB/s | 303.5 MiB/s | 303.8 MiB/s | 282.7 MiB/s | 298.8 MiB/s |

#### coalesced vs fswrite (write-coalescing gain alone)

| Size   | coalesced vs fswrite |
|--------|----------------------|
| 1 KiB  | +32%                 |
| 2 KiB  | +30%                 |
| 4 KiB  | +35%                 |
| 8 KiB  | +32%                 |
| 16 KiB | +24%                 |

#### coalesced vs buffered (cost of sync_all vs fdatasync)

| Size   | coalesced vs buffered |
|--------|-----------------------|
| 1 KiB  | −1.1%                 |
| 2 KiB  | +3.4%                 |
| 4 KiB  | +0.7%                 |
| 8 KiB  | +1.1%                 |
| 16 KiB | −0.1%                 |

## Analysis

### Consistent latency: unchanged, as expected

Both `coalesced` and `fswrite` land in the same 410–515 µs band. The fsync device-flush
floor dominates. Neither write-coalescing nor switching `sync_all` to `fdatasync`
provides any consistent-mode benefit. This is expected — the dominant cost is hardware,
not software.

### Eventual single: all sinks within noise

Fire-and-forget latency is 1.2–3.2 µs for all five sinks. The hand-off to the
background WAL thread is the only on-clock cost; the fsync is entirely off-clock. No
sink differentiates here.

### Eventual batch throughput: the key finding

`Coalesced` delivers **24–35% higher throughput than `fswrite`** across all record sizes,
purely from reducing N `write_all` calls per batch to 1. This is the write-coalescing
gain isolated from any fsync-semantics change.

The striking result is that **`coalesced` is statistically indistinguishable from
`buffered`** at every size (within ±3.4%, comfortably inside measurement noise). This
means swapping `sync_all` for `fdatasync` — the only difference between the two sinks —
contributes essentially zero additional throughput in this configuration. The write
syscall count reduction is the entire story; metadata sync latency is negligible
compared to the batch write time on this hardware. Accordingly, there is no measurable
throughput penalty for using `Coalesced` (full `sync_all`) over `buffered` (fdatasync).

`mmap` and `iouring` are below both `coalesced` and `buffered` in most cells, consistent
with task30's findings.

## Guidance: when to use `WalWrite::Coalesced`

Use `WalWrite::Coalesced` when:
- The workload commits many entries before each drain (batched ingestion, group-commit
  patterns, high-throughput Eventual mode).
- Throughput matters more than per-entry latency (already fire-and-forget in Eventual
  mode).
- You want the write-coalescing benefit with no change to fsync durability semantics.

Stick with `WalWrite::PerEntry` (default) when:
- The workload issues one commit at a time (OLTP, interactive, low concurrency).
- Simplicity and zero-change behavior are the priority.
- `Durability::Consistent` is in use — both sinks perform identically there.

`Coalesced` is safe with `prune_wal`, checkpoint, and recovery, just like the `PerEntry`
default. It uses the OS file position (no offset arithmetic, no file pre-sizing, no live
mappings), so file truncation and rotation work correctly.

### What stays bench-only / experimental

`fdatasync` (`BufferedFileSink { datasync: true }`, the task30 `buffered` column),
`MmapSink`, and `IoUringSink` are NOT exposed as `WalWrite` variants. They remain
accessible only through `WalSinkKind` under `#[cfg(feature = "bench-internals")]`:

- **`buffered` (fdatasync)**: Skips file-metadata sync. Marginally riskier durability
  posture and — as shown above — delivers no measurable throughput benefit over
  `Coalesced` (sync_all) in this benchmark. Remains available for future evaluation.
- **`mmap`**: Unsafe, pre-sizes the file, requires sentinel handling in `read_wal`, not
  safe with `prune_wal`/checkpoint (SIGBUS under truncation of a live mapping).
- **`iouring`**: Unsafe, Linux-only, offset-based writes break after file
  rotation/truncation, requires the `wal-iouring` feature flag.

## Tests

The `WalSink` trait test suite covers the `Coalesced` sink via the same round-trip path
as all other sinks. No new sink-specific behavioral tests are needed: the `append` →
`drain` → `read_wal` round-trip is identical to `FsWrite`, and `BufferedFileSink` was
already covered by the existing task30 bench infrastructure.

## References

- [task30_wal_backends.md](task30_wal_backends.md) — prior WAL-backend comparison; source
  of the `BufferedFileSink` implementation and the batch-throughput benchmarks that
  motivated this feature.
- [task29_durability_failure_handling.md](task29_durability_failure_handling.md) — `WalSink`
  trait, `WalPoison`, `FileSink` production baseline.
- [task15_three_phase_consistent_persistence.md](task15_three_phase_consistent_persistence.md)
  — Consistent-mode fsync protocol.
- [task13_persistence.md](task13_persistence.md) — WAL file format and recovery.
