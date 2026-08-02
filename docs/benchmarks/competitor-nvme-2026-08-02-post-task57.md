# Competitor YCSB on AWS NVMe — 2026-08-02 (post-task57 re-bench)

Same-day re-run of the competitor matrix after task57 (WAL frame-at-commit +
buffer pooling, `docs/tasks/task57_wal_buffer_pooling.md`). Method, metric,
and harness identical to `competitor-nvme-2026-08-02.md` (criterion median ms
per 1,000-op burst; each op its own transaction; 10k records, Zipfian).
Shipped config only — glibc, no allocator swap.

## Provenance

- 2026-08-02, bench-infra c6id.2xlarge local NVMe, `make bench-oneshot
  TARGET=competitor`; results `bench-infra/bench-out/dist/20260802T203110Z/`;
  ultima_db git `a02a94b` (branch `perf/wal-frame-at-commit`).
- Different host instance than the morning pre-task57 run: compare
  **vs-competitor ratios**, not cross-run absolutes.

## YCSB — eventual-durability tier (the tier task57 targets)

| Workload | UltimaDB | Fjall | ReDB | RocksDB | Fastest |
|---|--:|--:|--:|--:|---|
| A update-heavy | 3.61 ms | **2.80** | 15.33 | 3.47 | Fjall (UltimaDB 1.29× behind) |
| B read-mostly | **535 µs** | 929 | 2520 | 1300 | UltimaDB, 1.74× |
| C read-only | **178 µs** | 722 | 977 | 951 | UltimaDB, 4.05× |
| D read-latest | **570 µs** | 1440 | 3020 | 4880 | UltimaDB, 2.52× |
| E short-ranges | **1.02 ms** | 17.0 | 9.97 | 616.4 | UltimaDB, 9.81× (vs ReDB) |
| F read-modify-write | 3.77 ms | **3.14** | 19.27 | 3.94 | Fjall (UltimaDB 1.20× behind) |

**The write-mix gap vs Fjall narrowed substantially**: A 1.38× → **1.29×**
behind, F 1.60× → **1.20×** behind (morning pre-task57 doc). UltimaDB is now
also ahead of RocksDB on F (3.77 vs 3.94), and the win margins on B/D/E all
widened (D 2.04×→2.52×). Same 4-of-6 pattern; the residual A/F deficit is
Fjall's no-fsync LSM write (memtable insert) vs our per-commit MVCC
path-clone — the write-overlay design is the lever aimed at that
(~2.1 µs/op MVCC tax per the decomposition doc).

Embedder note: with a mimalloc global allocator (application-level opt-in,
`bench-mimalloc` shows the shape), the task57 fleet A/B measured UltimaDB's
A/F cells at 2.9/2.5 ms — reading ~even with (A) and ahead of (F) Fjall's
cells here, but cross-host, so it stays out of this table.

## YCSB — strict / durable tier (fsync per commit)

| Workload | **UltimaDB** | Fjall | ReDB | RocksDB | UltimaDB vs best competitor |
|---|--:|--:|--:|--:|---|
| A update-heavy | **22.8 ms** | 40.0 | 46.3 | 74.5 | 1.76× faster (Fjall) |
| B read-mostly | **2.5 ms** | 4.9 | 5.7 | 9.2 | 1.97× faster (Fjall) |
| C read-only | **178 µs** | 670 | 989 | 1130 | 3.77× faster (Fjall) |
| D read-latest | **2.5 ms** | 5.5 | 8.5 | 9.2 | 2.22× faster (Fjall) |
| E short-ranges | **2.9 ms** | 21.8 | 16.0 | 25.5 | 5.43× faster (ReDB) |
| F read-modify-write | **22.7 ms** | 41.5 | 51.5 | 75.4 | 1.83× faster (Fjall) |

Strict-tier sweep intact — fastest on all six, margins in line with the
pre-task57 run (fsync dominates; task57 changes nothing material here, and
regressed nothing).

## Reproduce

```bash
cd bench-infra && make bench-oneshot TARGET=competitor
```
