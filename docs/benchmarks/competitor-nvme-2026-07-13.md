# Competitor YCSB re-bench on AWS NVMe — 2026-07-13

Re-run of the head-to-head after the post-June engine work landed: in-place B-tree
insert (`insert_mut`, task48), in-place delete + rebalance (`remove_mut`, task50),
and the fanout bump T=32→64 (PR #11). Prior doc: `competitor-nvme-2026-06-26.md`.

## Provenance

- 2026-07-13T07:00:41Z, `make bench-oneshot TARGET=competitor` (bench-infra), results
  `bench-infra/bench-out/dist/20260713T072102Z/`.
- AWS local-NVMe host, 8 vCPU / 15.7 GB (c6id.2xlarge class), kernel 6.17.0-1019-aws,
  rustc 1.97.0, ultima_db git `e5ed2dc`.
- UltimaDB strict arm = `standalone_fast` (`ConsistentInline` + `CoalescedPrealloc`),
  same as June. Metric = criterion median time for one burst of **1,000 YCSB ops**,
  in **milliseconds** (ms; lower is better). Throughput is that inverted:
  `ops/sec = 1,000 ops ÷ (ms × 0.001 s) = 1,000,000 ÷ ms` (higher is better) — the
  `1,000,000` being 1,000 ops/burst × 1,000 ms/s. Each op is its own transaction;
  strict-tier writes fsync per commit.
- **Scope: YCSB only.** The bench-infra `competitor` target runs `make
  bench/ycsb/compare`; the June doc's SmallBank and MultiWriter sections were a wider
  one-off script and were not re-run — those June numbers remain the latest.

## YCSB — strict / durable tier

| Workload | **UltimaDB** | Fjall | ReDB | RocksDB | UltimaDB vs best competitor |
|---|--:|--:|--:|--:|---|
| A update-heavy | **23.6** | 42.0 | 45.7 | 75.7 | 1.78× faster (Fjall) |
| B read-mostly | **2.52** | 5.00 | 5.64 | 9.27 | 1.99× faster (Fjall) |
| C read-only | **0.172** | 0.719 | 1.03 | 1.17 | 4.19× faster (Fjall) |
| D read-latest | **2.59** | 5.69 | 8.42 | 9.19 | 2.20× faster (Fjall) |
| E short-ranges | **2.98** | 41.5 | 16.1 | 30.4 | 5.40× faster (ReDB) |
| F read-modify-write | **23.8** | 42.9 | 51.3 | 77.4 | 1.80× faster (Fjall) |

UltimaDB fastest on **all six durable workloads** — unchanged from June.

Same data as throughput (**ops/sec = 1,000,000 ÷ ms**, higher is better):

| Workload | **UltimaDB** | Fjall | ReDB | RocksDB |
|---|--:|--:|--:|--:|
| A update-heavy | **42,400** | 23,800 | 21,900 | 13,200 |
| B read-mostly | **397,000** | 200,000 | 177,000 | 108,000 |
| C read-only | **5,810,000** | 1,390,000 | 971,000 | 855,000 |
| D read-latest | **386,000** | 176,000 | 119,000 | 109,000 |
| E short-ranges | **336,000** | 24,100 | 62,100 | 32,900 |
| F read-modify-write | **42,000** | 23,300 | 19,500 | 12,900 |

## YCSB — nondurable tier

| Workload | UltimaDB | Fjall | ReDB | RocksDB | Fastest |
|---|--:|--:|--:|--:|---|
| A update-heavy | 4.20 | **2.93** | 15.5 | 3.55 | Fjall (UltimaDB 1.43× behind) |
| B read-mostly | **0.594** | 0.941 | 2.59 | 1.36 | UltimaDB, 1.59× |
| C read-only | **0.176** | 0.707 | 1.01 | 1.13 | UltimaDB, 4.01× |
| D read-latest | **0.781** | 1.45 | 3.05 | 5.12 | UltimaDB, 1.85× |
| E short-ranges | **1.06** | 16.8 | 10.1 | 600 ⚠ | UltimaDB, 9.58× (vs ReDB) |
| F read-modify-write | 4.70 | **3.39** | 19.3 | 4.17 | Fjall (UltimaDB 1.39× behind) |

Same pattern as June: Fjall edges the heaviest non-durable write mixes (A/F, where its
LSM write path shines with no fsync to amortize); UltimaDB wins B–E.

Same data as throughput (**ops/sec = 1,000,000 ÷ ms**, higher is better):

| Workload | UltimaDB | Fjall | ReDB | RocksDB |
|---|--:|--:|--:|--:|
| A update-heavy | 238,000 | **341,000** | 64,500 | 282,000 |
| B read-mostly | **1,680,000** | 1,060,000 | 386,000 | 735,000 |
| C read-only | **5,680,000** | 1,410,000 | 990,000 | 885,000 |
| D read-latest | **1,280,000** | 690,000 | 328,000 | 195,000 |
| E short-ranges | **943,000** | 59,500 | 99,000 | 1,670 ⚠ |
| F read-modify-write | 213,000 | **295,000** | 51,800 | 240,000 |

## vs June 2026-06-26

Per the bench methodology, only **same-host ratios and orderings** are comparable
across runs (this is a different physical instance; absolute cross-run deltas are
noise). On that basis:

- **No ranking changed on any row of either tier.** UltimaDB holds all 6 durable
  workloads; Fjall holds nondurable A/F.
- Ratios moved modestly, mostly compressing: strict B 2.13→1.99×, D 2.34→2.20×,
  E 6.05→5.40×, F 1.94→1.80×; nondurable B 1.97→1.59×, D 2.28→1.85×, E 10.5→9.6×.
  Nondurable A widened against us (1.20×→1.43× behind Fjall). Read-only C is flat
  (4.2×/4.0×).
- **The post-June engine wins were not expected to move YCSB, and didn't.**
  `insert_mut`/`remove_mut` pay off when many writes land in one transaction's
  privately-owned tree; YCSB here commits per op, which is exactly the
  first-touch-CoW regime where the in-place paths ≈ the immutable ones. The T=64
  read gain is real but small relative to the harness overhead per burst. The batch
  win shows up in the bulk-load comparison (task49) and the upcoming
  `insert_batch` fast path (task51), not here. This run's value is confirming **no
  regression** from T=64 + in-place rebalance under the competitor workloads.

## Anomalies / caveats

- RocksDB nondurable-E remains a known outlier (600 ms; June 616 ms) — range-scan
  path artifact, not new.
- Fjall's strict-E worsened ~75% vs its own June number (23.7→41.5 ms), flipping
  Fjall/RocksDB in 3rd/4th on that row. Doesn't affect the leader or the
  best-competitor ratio (ReDB is 2nd both times); noise vs real Fjall regression is
  indistinguishable from one run.
- Nondurable A/D drifted high for UltimaDB even as ratios (the valid signal) stayed
  in the same regime; treat any single-row ratio move ≤ ~15% as within combined
  noise (host floor ±2.5–9% per arm).
