# task52 — FixedVec inline node storage + fanout retune (T=32 default, `fanout-t8`)

Outcome of the 2026-07-17/18 `smr-apply` autobench campaign
(`autoresearch/smr-apply-jul17`) plus two AWS-NVMe validation runs. Cuts the
SMR/contended write path's dominant cost — CoW path-clone work — and retunes
fanout around the new clone economics.

## What shipped

1. **`FixedVec` inline node storage** (`src/btree.rs`). `BTreeNode`'s two
   `Vec`s (entries, children) became inline fixed-capacity slot arrays
   (`[Option<E>; N]` + `u8` len; safe code, no new deps). A CoW node clone is
   now **one allocation** (the Arc block) instead of three, and entries are
   co-located with the node — which is also where the contended-read win
   comes from. A compile-time guard rejects capacities that don't fit the
   `u8` len (i.e. T > 127; a T=128 sweep arm exposed silent len wraparound).
2. **Fanout retune**: `const T` = **32 default** (was 64), **8 behind the
   `fanout-t8` cargo feature**. Rationale below; feature is additive and
   affects only the const.

## Why

The SMR apply path (one `WriteTx` per consensus entry, `Persistence::Smr`,
no WAL) commits by cloning the root-to-leaf path — retained snapshots plus
live readers mean `Arc::make_mut` never fires there. Autobench established
the median commit cost is that clone work (allocs, memcpy, Arc-refcount
bumps), not locks and not the read traversal.

Authoritative same-host NVMe numbers
(`docs/benchmarks/smr-fanout-fixedvec-nvme-2026-07-18.md`):

| config | contended writes | contended read p99 | uncontended get @1M |
|---|--:|--:|--:|
| old main (T=64 + Vec) | 1.0× | 1159 ns | 433 ms |
| **new default (T=32 + FixedVec)** | **1.60×** | **1016 ns** | 602 ms |
| `fanout-t8` (T=8 + FixedVec) | **2.85×** | 2187 ns | 752 ms |

- The default strictly improves both contended axes over the old main and
  concedes only uncontended-get peak.
- T=8 is the write specialist: fanout dominates the write side (~width per
  cloned level), but costs ~2× on reads under load — opt-in only.

## Measured negative results (recorded so they aren't retried)

- **Shared per-node vals block** (keys inline, values behind one `Arc`):
  +26–46% contended writes but 3.1× contended-read regression at T=32 and
  +6–26% on all uncontended ops (`docs/benchmarks/shared-vals-nvme-2026-07-18.md`).
  Reverted from this candidate; lives on the autoresearch branch as an
  SMR-specialist finding.
- **Background reclaim thread** for snapshot frees: 3.1× apply-p99
  regression — cross-thread decrements on hot near-root Arcs ping-pong
  cachelines; frees must stay serial on the committer.
- T=4 breaks `extend_from_sorted` (underfull non-root nodes) — latent
  small-T builder bug, open follow-up.
- Commit critical-section shrink and fused get+replace traversal: washes
  (path is not lock-bound; read walk is cheap).

## Follow-ups

- ~~Formal model re-sync~~ **done**: re-instantiated at T=32 (all 10
  theorems axiom-clean; drift gate passes). The kernel stays a Vec-based
  behavioral model of `FixedVec` — boundary documented in `formal/README.md`.
  Remaining: the `fanout-t8` config (T=8) is not separately instantiated;
  the durable fix is a **T-parametric development** (`2 ≤ T ≤ 127`, upper
  bound from the u8-len guard). Feasibility gate: charon/aeneas const-generic
  support; fallback is a second concrete instantiation.
- Re-record autobench baselines (`make perf/baseline`) post-merge; committed
  baselines predate both changes.
- Fix `extend_from_sorted` small-T underfull bug.
- Gate B harness rot: `run-iter` drives `uc_autobench`, which no longer
  exists in the rewritten `ultima_cluster` (uc2). Local runs gate on
  torture + Gate A only until repaired.

## Provenance

Campaign record: `autobench/tasks/smr-apply/results.tsv` rows dated
2026-07-17/18 (same-day medians, sandbox host; cumulative sandbox apply_p99
13118 → 3104 ns for the shipped subset). Authoritative numbers: the two
2026-07-18 NVMe docs above (bench-infra, c6id.2xlarge, one host per run).
