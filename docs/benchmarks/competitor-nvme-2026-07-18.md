# Competitor YCSB on AWS NVMe — 2026-07-18 (post-PR#14 re-bench)

Re-run of the YCSB competitor matrix after PR#14 (FixedVec inline nodes +
fanout T=64→32 default) to check for competitor-facing movement. Method,
metric, and harness identical to `competitor-nvme-2026-07-13.md` (criterion
median ms per 1,000-op burst; ops/sec = 1,000,000 ÷ ms; each op its own
transaction; 10k records, Zipfian).

## Provenance

- 2026-07-18, bench-infra c6id.2xlarge local NVMe, `make bench/competitor`;
  results `bench-infra/bench-out/dist/20260718T174646Z/`; ultima_db git
  `0ae4974` (main, post-PR#14). Same run window also recorded NVMe autobench
  baselines (`dist/20260718T172602Z/autobench-baselines/`, reference only —
  the committed `autobench/baselines/*.json` are sandbox-recorded for the
  local gate).
- Different host instance than 2026-07-13: compare **vs-competitor ratios**,
  not cross-day absolutes.

## YCSB — strict / durable tier (fsync per commit)

| Workload | **UltimaDB** | Fjall | ReDB | RocksDB | UltimaDB vs best competitor |
|---|--:|--:|--:|--:|---|
| A update-heavy | **23.5 ms** | 44.2 | 46.9 | 80.4 | 1.88× faster (Fjall) |
| B read-mostly | **2.6 ms** | 5.6 | 5.8 | 10.2 | 2.17× faster (Fjall) |
| C read-only | **179 µs** | 797 | 1026 | 1504 | 4.45× faster (Fjall) |
| D read-latest | **2.5 ms** | 6.2 | 8.9 | 10.2 | 2.47× faster (Fjall) |
| E short-ranges | **2.9 ms** | 48.1 | 17.9 | 32.7 | 6.22× faster (ReDB) |
| F read-modify-write | **23.6 ms** | 44.3 | 51.7 | 82.0 | 1.88× faster (Fjall) |

UltimaDB fastest on **all six durable workloads** — margins equal to or
slightly better than 2026-07-13 (was 1.78–5.40×, now 1.88–6.22×).

## YCSB — eventual-durability tier (WAL written, async fsync)

| Workload | UltimaDB | Fjall | ReDB | RocksDB | Fastest |
|---|--:|--:|--:|--:|---|
| A update-heavy | 4.5 ms | **3.1** | 16.4 | 3.9 | Fjall (UltimaDB 1.44× behind) |
| B read-mostly | **596 µs** | 1141 | 2774 | 1678 | UltimaDB, 1.92× |
| C read-only | **179 µs** | 861 | 1033 | 1603 | UltimaDB, 4.82× |
| D read-latest | **697 µs** | 1540 | 3223 | 4842 | UltimaDB, 2.21× |
| E short-ranges | **1.04 ms** | 19.4 | 10.9 | 296.9 | UltimaDB, 10.5× (vs ReDB) |
| F read-modify-write | 5.1 ms | **3.6** | 20.8 | 4.6 | Fjall (UltimaDB 1.40× behind) |

Same 4-of-6 pattern as 2026-07-13 (Fjall's no-fsync LSM path edges the pure
write mixes A/F by the same ~1.4×; UltimaDB wins B–E, with D ~11% faster
than July 13 in absolute terms despite the host change).

## Reading

**PR#14 moved nothing here — by design.** The YCSB harness is small
(10k rows, cache-resident, height ~2 at either fanout) and its write cost is
dominated by the per-commit WAL path, not tree-clone work. The retune's wins
live in the SMR/contended regime
(`smr-fanout-fixedvec-nvme-2026-07-18.md`: 1.6×/2.85× contended writes) and
arrived with **zero competitor-facing regression** — the durable-tier sweep
and eventual-tier margins are intact.

## Reproduce

```bash
cd bench-infra && make bench-oneshot TARGET=competitor
```
