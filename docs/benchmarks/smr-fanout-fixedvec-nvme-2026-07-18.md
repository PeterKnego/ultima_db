# Fanout × node-layout A/B on AWS NVMe (SMR branch validation) — 2026-07-18

Same-host, same-day validation of the `autoresearch/smr-apply-jul17` branch
changes (fanout T=64→8 + `FixedVec` inline node storage) against `main`
(T=64, Vec-based nodes), across two regimes:

- **Contended** (`bench/smr-ab`): SMR-apply batch throughput + point-read p99
  under concurrent write load — the perf-gate regime the branch optimized.
- **Uncontended** (`bench/fanout`): bulk get/insert/remove @ 1M random keys —
  the regime that originally chose T=64.

## Provenance

- 2026-07-18, bench-infra single AWS c6id.2xlarge (8 vCPU, local NVMe),
  one `make up` … `make destroy` window (~40 min), tree rsynced per arm.
- Branch arm: git `bd9053e` (T=8 + FixedVec; sweep sed-patches `const T`).
  Main arm: git `a4af298` + uncommitted `T_SWEEP` narrowing only.
- Results: `bench-infra/bench-out/dist/20260718T{092056,093256,093442,094309}Z/`.
- 7 runs/arm (smr-ab, medians, spreads ≤±1%); criterion (fanout).

## Contended regime (`smr-ab`, 100k rows)

apply_sw_batch_throughput (ops/s, higher better) / read_p99_under_load (ns, lower better):

| T | Vec (main) | FixedVec (branch) |
|--:|--:|--:|
| 8 | 405,300 / 2,653 | **414,493** / 2,187 |
| 16 | — | 313,973 / 2,206 |
| 32 | 244,532 / 2,782 | 232,878 / **1,016** |
| 64 | 145,316 / 1,159 | 134,985 / 915 |

- **Write side tracks fanout, not layout**: T=8 ≈ 2.8× the shipped
  T=64+Vec throughput (405–414k vs 145k). Layout is ±2–7% here.
- **Read-under-load tracks layout**: FixedVec cuts read p99 at every T
  (−18% @8, −63% @32, −21% @64) — key/value co-location beats the extra
  pointer chase under concurrent CoW churn.
- The 2026-07-17 sandbox loop's "read p99 improves at small T" was a noisy-host
  artifact: on quiet hardware small T costs reads (height), full stop.

## Uncontended regime (`fanout`, 1M random keys, ms lower better)

| T | get Vec / FixedVec | insert Vec / FixedVec | remove Vec / FixedVec |
|--:|--:|--:|--:|
| 8 | 758 / 752 | 1292 / 1017 | 1167 / 860 |
| 32 | 541 / 602 | 1017 / 942 | 816 / 819 |
| 64 | **433** / 650 | 997 / 1139 | 800 / 884 |

(Vec absolutes derived from each arm's printed T=32 base × its normalized
ratios; branch full sweep also ran T=16 (get 1.08, insert 0.98 vs T=32) and
T=128 — the T=128 row was bogus and exposed a real bug: `FixedVec`'s u8
length wraps at capacity 256, silently corrupting the tree for T>127. Now a
compile-time guard (branch `2213381`).)

- Uncontended reads still favor big-T+Vec: shipped main's get (433 ms) beats
  every branch config; branch-T=8 gets are +74% vs that.
- **Layout × fanout interact**: FixedVec helps writes at small T (insert/remove
  −21–26% @T=8) but hurts everything at T=64 (fixed ~2KB node memcpy/cache
  footprint vs Vec's exact-size blocks). Neither layout dominates.

## Conclusions

1. **For the SMR/contended target, the branch config (T=8+FixedVec) is
   validated**: 2.85× shipped write throughput; reads-under-load −18% vs
   same-T Vec, though still 1.9× worse than shipped T=64+Vec.
2. **FixedVec+T=32 is the balanced candidate for a general default**: 1.60×
   shipped write throughput *and* better read-p99-under-load than shipped
   (1016 vs 1159 ns), at +11% uncontended get vs T=32+Vec (+39% vs shipped
   T=64+Vec's best-case get).
3. No config dominates all regimes; pick per deployment: SMR-heavy → T=8,
   mixed/general → T=32+FixedVec, read-dominated uncontended → status quo.
4. Sandbox loop numbers (2026-07-17 TSV) directionally right on writes,
   wrong on reads; treat only this doc's numbers as authoritative.

## Reproduce

```bash
cd bench-infra && make up
make bench/smr-ab && make bench/fanout      # arm 1: current checkout
# git checkout <other-arm> && repeat
make destroy && make status                  # verify teardown
```
