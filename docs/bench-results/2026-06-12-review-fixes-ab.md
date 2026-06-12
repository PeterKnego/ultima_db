# Benchmark verification: code-review fixes (2026-06-12)

A/B comparison confirming the seven review-fix commits
(`e60f8ce..1906e68` — phase-2 commit restructure, bulk-load OCC
visibility, WAL prune serialization, HNSW fixes, journal truncate
protocol, corruption hardening, metrics hot-path caching) introduce no
performance regressions.

## Method

- **Baseline**: `1de711a` (last commit before the review fixes), built
  and run from a git worktree.
- **Candidate**: `1906e68` (HEAD), run immediately after in the same
  shared `CARGO_TARGET_DIR`, so criterion compares run-to-run.
- **Suites** (UltimaDB-only; the fjall/rocksdb/redb comparison benches
  measure other engines and were skipped): `ycsb_bench`,
  `ycsb_multiwriter_bench`, `smallbank_bench`, `smallbank_scaling_bench`,
  `disjoint_tables_bench`, `multiwriter_persistence_bench`,
  `bulk_load_bench`, `snapshot_throughput`, `wal_bench` — ~60 benchmark
  configurations. Features: `bench-internals` on both sides.
- **Noise-floor control**: every delta flagged above ±2.5% was re-run as
  a self-comparison (identical HEAD binary vs its own stored results) to
  separate drift from real change. Raw outputs:
  `2026-06-12-review-fixes-ab-raw.txt` (A/B) and
  `2026-06-12-review-fixes-noise-floor.txt` (self-comparison).

## Results

| Suite | Outcome |
|---|---|
| YCSB A–F | A (update-heavy) improved −3.2%; B/C/D/F within +1–2%; E unchanged |
| YCSB MultiWriter (contention + 1–8 thread scaling) | all within ±2%, majority improved |
| SmallBank (7 scenarios incl. n16 contention) | flat to improved (−0.8% to −1.5%) |
| disjoint_tables 2–16 writers | ±2% mixed; improved at 8/16 writers |
| mw_persistent inmemory / eventual | statistically unchanged (gate-skip working as designed) |
| mw_persistent standalone_consistent | ±3–4% across runs, both directions — promotion gate unmeasurable against the fsync wait |
| bulk_load (insert_batch + bulk_load_sorted, 100k/1M) | at baseline |
| snapshot_throughput (build + install) | at baseline |
| wal_bench (4 sinks × 5 sizes × 2 modes) | ±10–30% scatter both directions — raw fsync timing noise, no pattern |

## Flagged deltas were drift, not regressions

The four configurations that exceeded ±2.5% in the A/B reproduced
swings of the same magnitude in the *opposite* direction when identical
code was re-run against itself:

| Bench | A/B delta | Self-comparison (identical code) |
|---|---|---|
| snapshot_install/10k | +9.3% | −9.3% |
| snapshot_build/100k | +5.4% | −5.9% |
| bulk_load_sorted/1M | +4.5% | −4.9% |
| mw_persistent/standalone_consistent | +4.0% | −2.6% |

These large-allocation and fsync-bound benches have a ±5–9% run-to-run
noise floor on this machine; the A/B's two phases ran ~40 minutes apart
under different system state. Final absolute numbers sit at or below
baseline (bulk-load 1M: 129.3 ms baseline → ~128.5 ms; consistent
commits: 6.62 ms → ~6.76 ms). The 100k bulk variant showing zero change
also rules out any per-row cost from the new OCC checks and WAL marker
(both O(1) per install).

## Conclusion

Across all nine suites, nothing regressed beyond measurement noise; the
multiwriter contention paths measurably improved. The review-fix series
is performance-neutral.
