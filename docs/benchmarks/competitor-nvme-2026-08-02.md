# Competitor YCSB on AWS NVMe — 2026-08-02 (post-0.3.0 re-bench)

Re-run of the YCSB competitor matrix after the 0.2.0/0.3.0 primary-key
generalization (task56: `Table<R, K>`, WAL entry format v2 with encoded key
bytes, hashed write-sets, registry format v2) plus the dirty-map handle cache
(#19) and the task53 gc rework. Method, metric, and harness identical to
`competitor-nvme-2026-07-18.md` (criterion median ms per 1,000-op burst;
ops/sec = 1,000,000 ÷ ms; each op its own transaction; 10k records, Zipfian).

## Provenance

- 2026-08-02, bench-infra c6id.2xlarge local NVMe, `make bench-oneshot
  TARGET=competitor`; results `bench-infra/bench-out/dist/20260802T120445Z/`;
  ultima_db git `f8caa46` (formal/tla-s0-gate — `src/` identical to main at
  0.3.0; the branch adds formal-verification tooling only). rustc 1.97.1.
- Different host instance than 2026-07-18: compare **vs-competitor ratios**,
  not cross-day absolutes.

## YCSB — strict / durable tier (fsync per commit)

| Workload | **UltimaDB** | Fjall | ReDB | RocksDB | UltimaDB vs best competitor |
|---|--:|--:|--:|--:|---|
| A update-heavy | **22.9 ms** | 39.8 | 48.3 | 73.0 | 1.73× faster (Fjall) |
| B read-mostly | **2.5 ms** | 4.8 | 5.5 | 8.4 | 1.92× faster (Fjall) |
| C read-only | **176 µs** | 671 | 974 | 1012 | 3.83× faster (Fjall) |
| D read-latest | **2.5 ms** | 5.2 | 8.4 | 8.6 | 2.10× faster (Fjall) |
| E short-ranges | **2.9 ms** | 21.6 | 15.7 | 26.3 | 5.35× faster (ReDB) |
| F read-modify-write | **22.6 ms** | 40.8 | 57.7 | 74.4 | 1.80× faster (Fjall) |

UltimaDB fastest on **all six durable workloads**, same sweep as 2026-07-13
and 2026-07-18. Margins 1.73–5.35× (2026-07-18: 1.88–6.22×); the compression
comes from the competitors running faster on this host instance — UltimaDB's
own medians are within ±4% of the 2026-07-18 cells on every workload.

## YCSB — eventual-durability tier (WAL written, async fsync)

| Workload | UltimaDB | Fjall | ReDB | RocksDB | Fastest |
|---|--:|--:|--:|--:|---|
| A update-heavy | 3.7 ms | **2.7** | 15.1 | 3.3 | Fjall (UltimaDB 1.38× behind) |
| B read-mostly | **532 µs** | 897 | 2482 | 1258 | UltimaDB, 1.69× |
| C read-only | **180 µs** | 703 | 973 | 1046 | UltimaDB, 3.91× |
| D read-latest | **701 µs** | 1430 | 3001 | 4671 | UltimaDB, 2.04× |
| E short-ranges | **1.11 ms** | 16.5 | 10.1 | 563.6 | UltimaDB, 9.09× (vs ReDB) |
| F read-modify-write | 4.9 ms | **3.0** | 18.9 | 3.8 | Fjall (UltimaDB 1.60× behind) |

Same 4-of-6 pattern as both prior runs: Fjall's no-fsync LSM path edges the
pure write mixes A/F by ~1.4–1.6×; UltimaDB wins B–E. (RocksDB's E outlier,
563 ms, recurs from 2026-07-18's 297 ms — its nondurable range-scan path, not
a harness artifact.)

## Reading

**The 0.3.0 key generalization moved nothing competitor-facing — by
design.** Entry format v2 puts encoded key bytes on the WAL path and the
write-set now carries `hash64` digests, but for `u64` keys the encoding is a
fixed 8-byte big-endian write and the digest is a cheap mix — and the burst
commit cost stays fsync-dominated. UltimaDB's absolute medians match the
2026-07-18 run within the same-host noise band on all twelve cells (strict A
23.5→22.9 ms, C 179→176 µs, F 23.6→22.6 ms; eventual B 596→532 µs,
D 697→701 µs), so the durable-tier sweep and the eventual-tier win/loss
pattern are intact. The ratio movement vs 2026-07-18 is competitor-side and
within cross-instance variance.

## Reproduce

```bash
cd bench-infra && make bench-oneshot TARGET=competitor
```
