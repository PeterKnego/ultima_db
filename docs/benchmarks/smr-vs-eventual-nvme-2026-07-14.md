# SMR vs eventual-durability YCSB on AWS NVMe — 2026-07-14

Single-writer A/B of UltimaDB's two write paths on the same host: **checkpoint-only
SMR** (`Persistence::smr` — no per-commit WAL) vs **eventual-durability Standalone**
(`Persistence::standalone(Eventual, Coalesced)` — WAL written, async fsync). Isolates
the cost of the per-commit WAL machinery. Motivated by the question of how much the
WAL write path costs when durability comes from an external consensus log instead.

## Provenance

- 2026-07-14T09:18:40Z, `make bench-oneshot TARGET=smr-ycsb` (bench-infra), results
  `bench-infra/bench-out/dist/20260714T092301Z/`.
- AWS local-NVMe host, 8 vCPU / 15.7 GB (c6id class), kernel 6.17.0-1019-aws,
  rustc 1.97.0, ultima_db git `874ebec`.
- Metric = criterion **median** time for one burst of **1,000 YCSB ops**, single
  writer, each op its own transaction. Dataset 10k records, Zipfian(0.99), 100-byte
  fields. Same harness as the competitor YCSB doc; only the UltimaDB engine runs here
  (no competitor has an SMR equivalent).
- The two arms differ **only** in persistence mode:
  - **eventual** = `standalone(dir, Durability::Eventual, WalWrite::Coalesced)` — every
    commit serializes its write set and hands it to the WAL background thread; fsync is
    async, so the committer does not block on it, but the WAL write work + handoff are
    on the hot path and the bg thread does real NVMe I/O.
  - **SMR** = `smr(dir)` — no WAL at all; a commit does only the in-memory copy-on-write
    update + snapshot install. Durability is assumed to come from an external log;
    on-disk state advances only at `checkpoint()` (batched, off the commit path — and at
    1,000-op bursts no checkpoint is triggered, so this is SMR's steady-state write cost).

## Results (median, lower = better)

| Workload | eventual | SMR | SMR speedup | throughput (eventual → SMR) |
|---|--:|--:|--:|--:|
| A update-heavy (50% write) | 3.90 ms | 2.55 ms | **1.53× (−35%)** | 256k → **393k ops/s** |
| F read-modify-write | 4.55 ms | 3.08 ms | **1.48× (−32%)** | 220k → **325k ops/s** |
| B read-mostly (5% write) | 575 µs | 420 µs | 1.37× (−27%) | 1.74M → 2.38M ops/s |
| D read-latest | 768 µs | 627 µs | 1.23× (−18%) | 1.30M → 1.60M ops/s |
| E short-ranges (scans) | 1.08 ms | 952 µs | 1.13× (−12%) | 928k → 1.05M ops/s |
| C read-only (0% write) | 169 µs | 171 µs | 0.99× (tied) | 5.90M → 5.85M ops/s |

(throughput = 1,000,000 ÷ ms; see `docs/benchmarks/competitor-nvme-2026-07-13.md`
for the ms↔ops/sec derivation.)

## What it shows

The speedup tracks the write fraction, exactly as the commit-path structure predicts:

- **Write-heavy A/F: ~32–35% faster** under SMR. These are the rows that most exercise
  the per-commit path SMR strips: the ~100-byte record's bincode serialization for the
  WAL, `WalOp`/`WalEntry` construction, the handoff to the WAL background thread, and
  that thread's NVMe write + async fsync.
- **Read-only C: statistically tied** (169 vs 171 µs, within noise) — reads never touch
  the WAL, so with zero writes the two modes are identical. This is the control that
  validates the comparison.
- **Mixed B/D/E: 12–27%**, scaling with write rate. Part is the per-write serialize
  savings; part is that even a 5%-write workload spins up the WAL background thread,
  whose wakeups/lock/epoch traffic perturb the single benchmark thread — SMR has no such
  thread. C is flat precisely because zero writes leave that thread idle.

Per-op decomposition (using C's ~169 ns/read to net out the reads in B): eventual-B's
50 writes cost ≈ (575 − 950×0.169) µs ⁄ 50 ≈ **8.3 µs/write**, SMR-B's ≈ **5.2 µs/write**
— i.e. SMR shaves ~3 µs off each commit, entirely on the write path, reads unchanged.

## The tradeoff (not a free speedup)

SMR keeps **no local WAL**: a crash loses everything since the last checkpoint. It is
only correct where an external consensus log (Raft/Paxos) owns durability and replays
missing commits on recovery — see `docs/tasks/task13_persistence.md` (SMR mode) and
`docs/tasks/task12_smr_determinism.md` (deterministic apply). For a standalone store,
eventual-durability Standalone is the floor; SMR is not a drop-in swap for it. Against
this run's own numbers, SMR buys back roughly the write-path cost that a replicated
deployment would otherwise pay twice (consensus log + local WAL).

## Reproduce

```bash
# local (magnitude only — sandbox noise ±2x; not publishable):
make bench/smr-ycsb ULTIMA_BENCH_DIR=$HOME/bench-disk   # real disk, not tmpfs

# authoritative (AWS local-NVMe, billable — always tears down):
cd bench-infra && make bench-oneshot TARGET=smr-ycsb
```

Both run `ycsb_bench` twice (`ULTIMA_BENCH_DURABILITY=nondurable` for the eventual arm,
`ULTIMA_BENCH_SMR=1` for the SMR arm) and `critcmp` the two saved baselines.
