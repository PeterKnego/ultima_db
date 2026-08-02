# Performance reference

Current head-to-head numbers for UltimaDB against RocksDB, Fjall, and ReDB on
the YCSB workloads. Full run records, including earlier runs and non-YCSB
workloads, are archived in [`docs/benchmarks/`](../benchmarks/).

## Provenance

- Run 2026-08-02, AWS local-NVMe host (c6id.2xlarge class: 8 vCPU, 15.7 GB),
  rustc 1.97.1, ultima_db git `f8caa46` (`src/` identical to `main` at
  0.3.0). Full record:
  [`competitor-nvme-2026-08-02.md`](../benchmarks/competitor-nvme-2026-08-02.md).
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
| A update-heavy | **22.9** | 39.8 | 48.3 | 73.0 | 1.73× faster (Fjall) |
| B read-mostly | **2.50** | 4.80 | 5.50 | 8.40 | 1.92× faster (Fjall) |
| C read-only | **0.176** | 0.671 | 0.974 | 1.01 | 3.83× faster (Fjall) |
| D read-latest | **2.50** | 5.20 | 8.40 | 8.60 | 2.10× faster (Fjall) |
| E short-ranges | **2.90** | 21.6 | 15.7 | 26.3 | 5.35× faster (ReDB) |
| F read-modify-write | **22.6** | 40.8 | 57.7 | 74.4 | 1.80× faster (Fjall) |

UltimaDB is fastest on all six durable workloads.

## Eventual-durability tier

`Durability::Eventual`: the WAL is still written to disk; fsync happens
asynchronously and commit does not block on it. This matches the competitors'
no-fsync write path.

| Workload | UltimaDB | Fjall | ReDB | RocksDB | Fastest |
|---|--:|--:|--:|--:|---|
| A update-heavy | 3.70 | **2.70** | 15.1 | 3.30 | Fjall (UltimaDB 1.38× behind) |
| B read-mostly | **0.532** | 0.897 | 2.48 | 1.26 | UltimaDB, 1.69× |
| C read-only | **0.180** | 0.703 | 0.973 | 1.05 | UltimaDB, 3.91× |
| D read-latest | **0.701** | 1.43 | 3.00 | 4.67 | UltimaDB, 2.04× |
| E short-ranges | **1.11** | 16.5 | 10.1 | 564 ⚠ | UltimaDB, 9.09× (vs ReDB) |
| F read-modify-write | 4.90 | **3.00** | 18.9 | 3.80 | Fjall (UltimaDB 1.60× behind) |

Fjall leads the two heaviest write mixes (A, F) in this tier; UltimaDB leads
B–E. The RocksDB E outlier (564 ms) is a known range-scan-path artifact,
reproduced across runs.

## Known caveats

- Scope is YCSB only. SmallBank and MultiWriter comparison numbers were last
  measured 2026-06-26 (`docs/benchmarks/competitor-nvme-2026-06-26.md`).
- Throughput equivalents: ops/sec = 1,000,000 ÷ ms (e.g. durable C: 0.176 ms
  → 5.68 M ops/sec).
