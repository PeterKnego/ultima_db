# Write-overlay fleet ship gate — 2026-08-04 (PASSED at cap 32)

The task58 ship gate defined in `docs/tasks/task58_write_overlay.md`:
eventual-tier YCSB A must beat Fjall on the same host under glibc, with C/E
guardrails unchanged. Run on one c6id.2xlarge (local NVMe, idle; results
`bench-infra/bench-out/dist/20260804T185027Z-overlay-gate/` with the run
script); branch `2f88e64` (PR #25), main `19d7844` (the branch's base),
interleaved ×2, shipped config (glibc, no allocator swap).

## Decision cells (criterion medians, both reps shown)

| Arm | YCSB A | YCSB F |
|---|--:|--:|
| main | 3.56 / 2.89 ms | 3.41 / 2.65 ms |
| branch, cap 128 (spec default) | 2.86 / 2.66 ms | 2.49 / 2.45 ms |
| **branch, cap 32** | **2.42 / 2.40 ms** | **2.21 / 2.20 ms** |
| branch, cap 16 | 2.43 / 2.41 ms | 2.24 / 2.24 ms |
| Fjall | 2.73 / 2.69 ms | 2.96 / 2.97 ms |

**Gate verdict: PASS at cap 32 (and 16).** A: 2.41 ms vs Fjall's 2.71 —
UltimaDB ahead 1.12×; the first time the last Fjall-held eventual cell
flips in the shipped config. F: 2.20 vs 2.97 — 1.35× ahead. At the spec's
original cap 128 the A cell only ties Fjall (2.76 vs 2.71) — the final
review's cost-model correction (per-entry Arc-clone, linear in cap, so
smaller caps win) was the decisive call. **The default is now 32**
(`src/overlay.rs::OVERLAY_CAP`), flipped in this branch.

(main's rep-1 A/F cells ran first after boot and read high — its rep-2
values are the fair baseline; the branch beats either.)

## Guardrails (branch cap 128 vs main — worst case for read probes)

| Cell | main | branch | Δ |
|---|--:|--:|--:|
| C read-only | 173.5 / 174.1 µs | 178.9 / 180.0 µs | **+3.2%** (budget ≤5% under writes) |
| E short-ranges | 965 / 973 µs | 909 / 909 µs | **−6%** (improved) |

C's probe cost is the true worst case here: the preload leaves the overlay
near-full, so every C read pays the binary-search probe. Within budget.
E improves outright.

## Write-tail side signal (same-binary smr-apply A/B, cap 128 vs cap 0, ×3)

- `apply_sw_batch_throughput`: 417–440k vs 313–328k — **+31–36%**
- `read_p99_under_load_ns`: 1882–1928 vs 2526–2613 — **−26% (better)**
- `apply_p99_ns`: 14.9–16.6k vs 12.2–15.7k — **+10–21% worse** (the
  1-in-cap flush spike; milder on NVMe than the sandbox's +31–124%)
- `checkpoint_ms`: flat (the sandbox's +19% did not reproduce — environmental)

At cap 32 the flush spike is ~4× smaller and ~4× more frequent (3.1% of
commits, so flush cost fully occupies the p99 quantile) — the e2e cells
above already price that in and cap 32 still wins.

## Reproduce

`nvme_overlay_gate.sh` archived alongside the results in the dist dir.
