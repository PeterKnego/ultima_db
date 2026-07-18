# Shared-vals node block A/B on AWS NVMe (iter8 validation) — 2026-07-18

Same-host isolation of the shared-vals change (`e429531`: keys inline,
values behind one shared `Arc<FixedVec<Arc<V>>>` per node) against its
immediate parent (`2213381`: FixedVec, per-value Arcs inline). Both arms
T-swept; both regimes.

## Provenance

- 2026-07-18 (second window, ~35 min), bench-infra c6id.2xlarge local NVMe,
  one up/destroy cycle; arms rsynced sequentially (A = branch HEAD `02e9369`,
  B = detached `2213381` with sweep-narrowing-only uncommitted edits).
- Results `bench-infra/bench-out/dist/20260718T{163520,164542,164646,165123}Z/`
  (dist dirs accumulate the remote results dir; timestamps map A-smr, A-fanout,
  B-smr, B-fanout).
- 7 runs/arm medians (smr-ab); criterion (fanout). Distinct host instance from
  the morning run — do not compare absolutes across the two 2026-07-18 docs.

## Contended (`smr-ab`, 100k rows)

apply_sw_batch_throughput (ops/s ↑) / read_p99_under_load (ns ↓):

| T | pre-iter8 | with iter8 | write Δ | read Δ |
|--:|--:|--:|--:|--:|
| 8 | 415,692 / 2,041 | 524,040 / 2,163 | **+26%** | +6% |
| 32 | 232,791 / 853 | 339,455 / 2,618 | **+46%** | **3.1× worse** |

(with-iter8 arm also ran T=16: 435,598 / 2,289 and T=64: 220,988 / 2,667.)

## Uncontended (`fanout`, 1M random keys, @T=32 absolutes, ms ↓)

| op | pre-iter8 | with iter8 |
|---|--:|--:|
| get | 499.5 | 623.5 (+25%) |
| insert | 807.6 | 860.2 (+6%) |
| remove | 672.1 | 846.7 (+26%) |

## Reading

Shared vals removes ~12-15 per-value Arc bumps from every interior-node CoW
clone — a pure win for contended writes, growing with node width (+26% @T=8,
+46% @T=32). But the values now live one pointer away from the node:

- At T=8 nodes were already ~2 cache lines, so reads barely notice (+6%).
- At T=32 the indirection destroys exactly the co-location that made
  FixedVec's contended-read p99 win (853 → 2,618 ns) and taxes every
  uncontended op double-digit.

**Verdict: SMR-specialist only.** Correct for a write-dominated T=8 SMR
build; wrong for a balanced T=32 default. cfg-gating an entire node layout
would mean maintaining two copies of every B-tree algorithm — rejected on
simplicity grounds.

## Consequence for the merge plan

- Merge candidate for main: **FixedVec + T=32 default + T=8 cargo feature,
  WITHOUT shared vals** (revert `e429531` when cutting the candidate).
- The shared-vals commit stays on the autoresearch branch as a measured
  finding: if a dedicated SMR-only build/crate flavor ever exists, it's a
  ready +26% write win there.
- Combined contended-write gains vs original main (T=64+Vec, same-host
  morning ratios): T=8+FixedVec ≈ 2.85×; +iter8 ≈ 3.6×.
