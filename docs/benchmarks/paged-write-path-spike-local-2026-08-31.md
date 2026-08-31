# Spike: paged write path under memory pressure (local, 2026-08-31)

**Question:** the fs-paged matrix (feat/paged-btree 407f955) showed ultima-paged
15-25x behind ReDB on YCSB A/F inside a memory-capped cgroup. Why, and what
fixes it?

**Environment:** standalone NVMe server (YMTC PC411-1TB; swap 8G on the same
NVMe — NOT the old ±2x Claude sandbox; swap faults measure ~60us). Every cell:
5M rows insert-loaded ("built by writes"), zipf 0.99, cgroup memory.max =
198 MiB (the fs-paged run's calibrated footprint/4), paged budget 64 MiB,
Eventual durability, 500k-op cap / 60s timeout, `compare_benches`
`paging_matrix` with the spike instrumentation (commit 00c0e3d: PagedStats
run-phase deltas; knobs ee0769a/ea2a165). Data: `data-paged-write-path-spike-2026-08-31.jsonl`.

## Finding chain (each probe designed to kill the previous hypothesis)

- **F1 — CONFIRMED BUG, fixed (7558715):** `resident_leaf_bytes` accounting
  asymmetry. Creation (`Child::resident_new`) credits only `dirty_bytes`;
  the demote debit is unconditional → the i64 counter negative-saturates at
  the first demote-heavy checkpoint; the `.max(0)` clamp hides it; the
  `due_mem` trigger and the fault-in budget wake go permanently silent.
  Evidence: base-A ran 60s under pressure with `checkpointer_runs=0`.
  Fix: checkpoint-time reconciliation from the exact per-table walk.
  Effect on throughput: ~none (the thrash was never resident data leaves —
  see F6) — but the budget trigger is correctness for bounded-memory
  deployments, and post-fix the estimate exposed the real culprit.
- **F2 — demotion cannot reclaim a touched working set:** forced 1-5s
  checkpoints demoted 0-41 leaves (second-chance protects every leaf touched
  since the last pass; scrambled-zipf re-touches within the tick) while
  adding 15-16k written pages of overhead each → int5/int1 cells were WORSE
  than base (815/758 vs 980 ops/s).
- **F3 — leaf granularity is a large lever:** fanout-t8 build = 3156 vs 980
  ops/s (3.2x), majflt 15.8 → 3.5/op. FNV-scrambled zipf puts ~1 hot key per
  leaf; a T=32 leaf pins ~4-8 KB (node + value Arcs) per hot key.
- **F4 — allocator arena retention: REFUTED** as primary. `malloc_trim(0)`
  after load freed ~1 GiB of load-churn garbage (RSS 1722 → 652 MiB) but
  changed run throughput not at all: the remaining 652 MiB is LIVE.
- **F5 — snapshot-retention pinning: REFUTED** as the holder.
  `--snapshots-retained=2` and `=1` both left 652 MiB live and throughput
  unchanged. (The mechanism is real — an older snapshot CoW-shares the
  pre-demotion leaves, `install_paged_tables` swaps only the target
  version's table map — it just isn't what holds the 652 MiB.)
- **F6 — HEAP FRAGMENTATION: CONFIRMED, the dominant term.** The ~47 MiB of
  surviving resident leaves + their values (per the F1-fixed estimate) are
  smeared at low density across the ~650 MB load-era heap: insert-order CoW
  churn interleaves survivors with freed sub-page chunks, which defeats
  malloc_trim's madvise. The cgroup swaps ~650 MB of sparse pages; every op
  touching a survivor drags mostly-dead pages back through swap
  (15.8 majflt/op on writes; reads barely allocate → 0.3).
  **Discriminator:** `--restart` (drop + recover from pages.bin = compact
  heap, same process, same cgroup):

  | cell | run1 ops/s | run2 (compact) | gain |
  |---|---|---|---|
  | restart-A (T=32) | 974 | 3,727 | 3.8x |
  | restartt8-A (T=8) | 2,999 | 12,907 | 4.3x |

  Compounded (T=8 + compact heap): **13.2x over baseline, within ~1.5x of
  ReDB's 18.8k** at a tighter budget than the fs-paged run. Corroborated by
  the task16 acceptance JSON (A.json: 1531 → 4647 ops/s, 256 MiB cgroup).

## Cell tables (198 MiB cgroup unless noted)

spike1 (pre-fix): base-A 980 ops/s, majflt 15.8, ckpt 0 | int5-A 815/28.9/4 |
int1-A 758/32.0/5 | budget160-A 985/15.8/0 | t8-A 3156/3.5/0 | base-C 33.7k/0.30/0
spike2 (F1 fixed): sanity 361k ckpt=2 resident 15MiB (no cgroup) |
fix-A 1067/15.8/0 resident_end 50MiB | fixt8-A 3246/3.5/0
spike3 (trim): trim-A 1008/15.4 — trim left 652 MiB live
spike4 (ret=2): 1015/15.8 — unchanged
spike5 (ret=1): 1035/15.7 — unchanged
spike6 (restart): table above

## Recommendations (ranked)

1. **Graduate F1** (7558715) with a unit test (negative-saturation repro:
   insert-load → checkpoint → assert due_mem fires on refault pressure) and
   a formal-cite re-anchor (store.rs line shifts break check-cites.py).
2. **Leaf-arena allocation (design work, biggest durable win):** allocate a
   leaf's node + its values from a per-leaf (or per-epoch) arena so demote
   frees whole pages — removes F6 at the root and fixes the NODE_BYTES
   undercount (budget would track real bytes). Candidate for the stage-3
   spec alongside in-place eviction.
3. **Allocator lever now:** recommend/bench mimalloc for paged deployments
   (task57 already measured -26/-32% on eventual writes; fragmentation
   behavior differs). Cheap NVMe A/B.
4. **fanout-t8** is the shipped lever for write-heavy paged deployments
   (3.2x here, consistent with task52).
5. **Document the ingest-then-serve pattern:** after a large insert-load,
   reopen (drop + recover) — 2-4s at 5M rows — for a compact heap; 3.8-4.3x.
6. **F2 follow-up (stage 3):** demote policy needs a pressure override —
   second-chance alone cannot reclaim a continuously-touched set.

## Open

- The single >=4.29s op (u32-saturated `max_us`) in every A cell including
  t8: present with ckpt_runs=0, so not checkpoint-induced. Candidates:
  first gc/drop of load-era memory under swap, cgroup direct-reclaim stall.
  Also: raise the harness latency counter above u32 ns.
- Local numbers: single NVMe box, n=1 per cell — ordering/mechanism
  evidence, not publishable ratios (bench-infra NVMe run for those).
