# Performance reference

Current head-to-head numbers for UltimaDB against RocksDB, Fjall, and ReDB on
the YCSB workloads. Full run records, including earlier runs and non-YCSB
workloads, are archived in [`docs/benchmarks/`](../benchmarks/).

## Provenance

- Run 2026-07-13, AWS local-NVMe host (c6id.2xlarge class: 8 vCPU, 15.7 GB),
  kernel 6.17.0-1019-aws, rustc 1.97.0, ultima_db git `e5ed2dc`
  (pre-0.3.0 code; the 0.3.0 keyed-table release did not change these paths
  for `u64`-keyed tables).
- One measurement = criterion median time for a burst of 1,000 YCSB
  operations, in milliseconds; each operation is its own transaction.
- UltimaDB durable arm = `Persistence::standalone_fast`
  (`ConsistentInline` + `CoalescedPrealloc`); durable-tier writes fsync per
  commit.
- Numbers from different hosts or runs are not comparable in absolute terms;
  only same-run orderings and ratios are meaningful. Single-row ratio moves
  ≤ ~15 % between runs are within combined noise. See
  [Reading our benchmark numbers](../explanation/reading-our-benchmarks.md)
  for the methodology.

## Durable tier (fsync per commit)

Milliseconds per 1,000-op burst; lower is better. Bold = fastest.

| Workload | UltimaDB | Fjall | ReDB | RocksDB | UltimaDB vs best competitor |
|---|--:|--:|--:|--:|---|
| A update-heavy | **23.6** | 42.0 | 45.7 | 75.7 | 1.78× faster (Fjall) |
| B read-mostly | **2.52** | 5.00 | 5.64 | 9.27 | 1.99× faster (Fjall) |
| C read-only | **0.172** | 0.719 | 1.03 | 1.17 | 4.19× faster (Fjall) |
| D read-latest | **2.59** | 5.69 | 8.42 | 9.19 | 2.20× faster (Fjall) |
| E short-ranges | **2.98** | 41.5 | 16.1 | 30.4 | 5.40× faster (ReDB) |
| F read-modify-write | **23.8** | 42.9 | 51.3 | 77.4 | 1.80× faster (Fjall) |

UltimaDB is fastest on all six durable workloads.

## Eventual-durability tier

`Durability::Eventual`: the WAL is still written to disk; fsync happens
asynchronously and commit does not block on it. This matches the competitors'
no-fsync write path.

| Workload | UltimaDB | Fjall | ReDB | RocksDB | Fastest |
|---|--:|--:|--:|--:|---|
| A update-heavy | 4.20 | **2.93** | 15.5 | 3.55 | Fjall (UltimaDB 1.43× behind) |
| B read-mostly | **0.594** | 0.941 | 2.59 | 1.36 | UltimaDB, 1.59× |
| C read-only | **0.176** | 0.707 | 1.01 | 1.13 | UltimaDB, 4.01× |
| D read-latest | **0.781** | 1.45 | 3.05 | 5.12 | UltimaDB, 1.85× |
| E short-ranges | **1.06** | 16.8 | 10.1 | 600 ⚠ | UltimaDB, 9.58× (vs ReDB) |
| F read-modify-write | 4.70 | **3.39** | 19.3 | 4.17 | Fjall (UltimaDB 1.39× behind) |

Fjall leads the two heaviest write mixes (A, F) in this tier; UltimaDB leads
B–E. The RocksDB E outlier (600 ms) is a known range-scan-path artifact,
reproduced across runs.

## Known caveats

- Scope is YCSB only. SmallBank and MultiWriter comparison numbers were last
  measured 2026-06-26 (`docs/benchmarks/competitor-nvme-2026-06-26.md`).
- Throughput equivalents: ops/sec = 1,000,000 ÷ ms (e.g. durable C: 0.172 ms
  → 5.81 M ops/sec).
