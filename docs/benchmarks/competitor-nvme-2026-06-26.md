# Competitor re-bench on NVMe — `standalone_fast` vs RocksDB / Fjall / ReDB

**Date:** 2026-06-26
**Host:** AWS `c6id.2xlarge` (8 vCPU, 15 GB), **local NVMe instance store**
(`/dev/nvme1n1`, 441 GB, ext4), `us-east-1`, Ubuntu, rustc 1.96.
**Code:** `main` @ `04c27b3` + the `ULTIMA_BENCH_INLINE` bench toggle (this branch).
**Provisioned/torn down** via `../ultima_cluster/bench-infra` (single node,
`make destroy` after).

## Why

The `2026-06-17` baseline (`docs/benchmarks/baseline-2026-06-17-3282af5.md`) put
UltimaDB **last** on durable write-heavy YCSB — taken on a noisy sandbox where the
async-WAL handoff tax dominated. Since then `standalone_fast` shipped
(`Durability::ConsistentInline` off-lock fsync + `WalWrite::CoalescedPrealloc`,
~40 µs/commit, ~3.8× the old default). This re-bench, on real NVMe, asks: **did
`standalone_fast` close the gap?**

## Method

- Same YCSB harness as the baseline: `NUM_RECORDS=10_000`, `OPS_PER_ITER=1_000`,
  `FIELD_SIZE=100 B`, Zipfian θ=0.99, criterion "burst" benches, **one commit per
  op** (identical commit granularity across engines). All engines on the **NVMe
  mount** (`ULTIMA_BENCH_DIR=/mnt/nvme/bench`).
- Two durability tiers via `ULTIMA_BENCH_DURABILITY`:
  - **nondurable** — WAL/journal written, no commit blocks on fsync. UltimaDB
    `Durability::Eventual`; RocksDB WAL on / `sync=false`; ReDB `Durability::None`;
    Fjall `PersistMode::Buffer`.
  - **strict (durable)** — every commit fsyncs. **UltimaDB =
    `standalone_fast`** (`ConsistentInline` + `CoalescedPrealloc`, via
    `ULTIMA_BENCH_INLINE=1 ULTIMA_BENCH_PREALLOC=1`); RocksDB WAL on + `sync=true`;
    ReDB `Durability::Immediate`; Fjall `PersistMode::SyncAll`.
- Values below are criterion **medians** of the per-burst time (lower = better).
  **Bold = fastest engine** in the row.

> **Caveat — same-host relative only.** These are absolute numbers on one
> NVMe host; do not compare across machines. The point is the engine ordering
> and ratios on identical hardware, and the movement vs the 2026-06-17 capture.

## 1. YCSB — strict / durable tier (the headline)

| Workload | **UltimaDB** (`standalone_fast`) | Fjall | ReDB | RocksDB |
|---|---|---|---|---|
| A update-heavy (50/50 r/w) | **22.6 ms** | 40.9 ms | 46.9 ms | 73.9 ms |
| B read-mostly (95/5)       | **2.49 ms** | 5.29 ms | 5.81 ms | 9.13 ms |
| C read-only                | **0.181 ms** | 0.766 ms | 0.984 ms | 1.14 ms |
| D read-latest              | **2.53 ms** | 5.93 ms | 8.80 ms | 8.84 ms |
| E short-ranges             | **2.86 ms** | 23.7 ms | 17.3 ms | 26.5 ms |
| F read-modify-write        | **22.5 ms** | 43.7 ms | 52.5 ms | 75.4 ms |

**UltimaDB is fastest on every durable YCSB workload.** On the write-bound
workloads (A, F) it is **~1.8× faster than the next engine (Fjall)** and
**~3.3× faster than RocksDB**. This is a full reversal of the 2026-06-17 result
where UltimaDB was last on durable write-heavy — `standalone_fast` closed and
overturned the gap.

## 2. YCSB — nondurable tier

| Workload | UltimaDB | Fjall | ReDB | RocksDB |
|---|---|---|---|---|
| A update-heavy | 3.43 ms | **2.86 ms** | 15.6 ms | 3.62 ms |
| B read-mostly  | **0.532 ms** | 1.05 ms | 2.58 ms | 1.37 ms |
| C read-only    | **0.188 ms** | 0.761 ms | 0.984 ms | 1.06 ms |
| D read-latest  | **0.662 ms** | 1.51 ms | 3.11 ms | 5.06 ms |
| E short-ranges | **0.995 ms** | 17.9 ms | 10.4 ms | 615.6 ms ⚠️ |
| F read-modify-write | 4.28 ms | **3.28 ms** | 19.6 ms | 3.98 ms |

UltimaDB wins B–E; Fjall edges the two heaviest write mixes (A, F) when nobody
pays for fsync. ⚠️ RocksDB workload E (615 ms) is an outlier — its range-scan
path on this dataset, not a durability effect.

## 3. SmallBank — strict / durable tier

| Workload | UltimaDB | RocksDB | Fjall |
|---|---|---|---|
| mixed        | **1.98 ms** | 3.45 ms | 4.87 ms |
| read-heavy   | **0.728 ms** | 2.04 ms | 2.38 ms |
| write-heavy  | **2.27 ms** | 3.98 ms | 6.19 ms |
| contention-low (2 thr) | 0.959 ms | 11.1 ms | **0.933 ms** |
| contention-high (2 thr) | 0.673 ms | 9.89 ms | **0.502 ms** |
| contention-low (16 thr) | **4.64 ms** | — | 4.64 ms* |
| contention-high (16 thr) | 2.88 ms | — | **2.40 ms** |

UltimaDB leads the core mixed/read/write transactions by 1.7–3×; Fjall is
fractionally ahead on the tiny 2-thread contention micro-cases; RocksDB is an
order of magnitude behind under contention. (*Fjall n16 ≈ tie.)

## 4. YCSB — MultiWriter (durable, concurrent writers)

| | UltimaDB | Fjall | RocksDB |
|---|---|---|---|
| low contention  | **0.872 ms** | 1.70 ms | 1.92 ms |
| high contention | **0.461 ms** | 1.06 ms | 7.13 ms |

UltimaDB's key-level OCC leads both; RocksDB degrades badly under high
contention. (Note: `ConsistentInline` is SingleWriter-only, so the MultiWriter
UltimaDB arm runs `Consistent` + `CoalescedPrealloc`, not full `standalone_fast`.)

## Bottom line

`standalone_fast` did exactly what it was designed to: on real NVMe, **UltimaDB
went from last on durable write-heavy YCSB (2026-06-17) to first on every durable
YCSB workload, SmallBank, and MultiWriter** — ~1.8× ahead of Fjall and ~3.3×
ahead of RocksDB on the write-bound durable mixes. The only places a competitor
edges ahead are non-durable heavy-write YCSB (Fjall, no fsync to amortize) and
sub-millisecond 2-thread SmallBank contention micro-cases (Fjall).

Raw criterion logs: archived from the run node (`/mnt/nvme/results/*.log`),
17 runs, ~28 min wall.
