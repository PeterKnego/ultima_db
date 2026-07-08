# B-tree optimization candidates — analysis & payoffs

**Date:** 2026-07-08
**Status:** Analysis / backlog (informs future specs)
**Context:** Follows the in-place `insert_mut` work (`docs/tasks/task48_btree_insert_mut.md`),
which cut insert/update from `O(height)` allocations per key to near-zero on privately-owned
trees (~20–27× e2e). This doc surveys what's left, grounded in the actual `src/btree.rs`
structure and the numbers already measured (the `insert_mut` micro A/B and the
`from_sorted` vs `insert_batch` gap in `docs/tasks/task49_bulk_load_bench.md`).

**Focus dimensions (chosen):** single-thread **write** (insert/update/**delete**) and
**read/scan** throughput. Memory footprint and MultiWriter/OCC commit throughput are out of
scope for this pass.

## Current structure (facts the ranking is built on)

- `BTreeNode { entries: Vec<(K, Arc<V>)>, children: Vec<Arc<BTreeNode<K,V>>> }`, `T = 32`,
  `MAX_KEYS = 63`. Values behind `Arc<V>`; children behind `Arc<BTreeNode>`.
- **Insert/update:** `insert_mut` (done) descends via `Arc::make_mut`, mutating
  uniquely-owned nodes in place. This path is already optimized.
- **Delete:** `remove` → `delete_from_node` still **clones `entries`+`children` at every
  level of the root→leaf path on every call** (btree.rs:705, 724–725, 745–746, 770,
  783–784), plus `remove_leftmost` and the rebalancers. This is the immutable-CoW cost
  `insert_mut` removed for inserts — still fully present for deletes.
- **Read (`get`):** cache-friendly `binary_search_by` recursion, ~`log_32(N)` deep (≈4
  levels at 1M keys).
- **Scan (`BTreeRange`):** already lean — an explicit `(&node, idx)` stack with `Copy`
  frames, **zero allocation per item**, yielding `(&K, &V)` borrows. Little headroom here.

## Candidates, ranked by payoff-per-effort

### Tier 1 — clear, grounded win

**1. `remove_mut` (in-place delete).** Symmetric twin of `insert_mut`: descend via
`Arc::make_mut`, mutate uniquely-owned nodes in place, CoW-clone only snapshot-shared ones;
wire into `Table::delete`/`delete_batch`. The per-level `entries.clone()+children.clone()`
is paid on *every* delete regardless of rebalancing, so this is a guaranteed win.
- **Payoff:** high on delete-heavy batches in one txn — same cost structure as the insert
  win, so expect roughly the `insert_mut` range (~8× random … ~20× sequential). ~1× for
  one-delete-per-txn (CoW-first-touch, same as insert).
- **Effort:** medium-high — rebalance (rotate/merge/root-collapse) in place is fiddlier than
  insert's split (touches parent + two siblings). The always-paid descent is the easy,
  high-value part; in-place rebalance is the rarer, trickier part.
- **Cost:** correctness-critical; **re-fires the formal drift guard** (the remove path *is*
  verified — the heavier proofs). Resolve as with `insert_mut`: mirror in `formal/kernel`
  (functional `*self = self.remove(key)` equivalent + differential test) or ACK.
- **→ Specced in `2026-07-08-btree-remove-mut-design.md`.**

### Tier 2 — cheap experiment + solid ROI

**2. Fanout (`T` / `MAX_KEYS`) tuning.** `T = 32` is a fixed choice. Larger `T` → shallower
tree (fewer cache misses per descent) at the cost of bigger per-node search/clone. **Near-
zero effort** — change one const, A/B the existing benches.
- **✅ MEASURED (2026-07-08, dev sandbox, random 1M keys; sweep `T ∈ {8,16,32,64,128}` via a
  scratch probe over get / `insert_mut` / `remove_mut`).** Times normalized to the current
  **T=32** (lower = faster):

  | T | MAX_KEYS | get | insert | remove |
  |--:|--:|--:|--:|--:|
  | 8 | 15 | 1.48 | 1.35 | 1.10 |
  | 16 | 31 | 1.23 | 1.03 | **0.91** |
  | **32** | **63** | **1.00** | **1.00** | **1.00** |
  | 64 | 127 | 0.99 | 0.91 | 1.31 |
  | 128 | 255 | **0.87** | **0.85** | 1.89 |

  (absolute @T=32: get 182 ms, insert 311 ms, remove 925 ms.)
- **Finding:** a monotonic read/insert-vs-delete tension. Reads and inserts favor **larger** `T`
  (shallower tree; gains diminish past T=32 and are modest — the get gain at T=128 is within
  ~1 CI). Deletes favor **smaller** `T` and degrade sharply at large `T` (**1.9× slower at
  T=128**) because bigger nodes mean bigger `Vec` shifts on removal and, critically, bigger
  **sibling clones during rebalancing** (the in-place rebalance micro-opt — see `task50` §6).
- **Conclusion: keep `T = 32`** — it is the balance point; no free lunch from re-tuning a mixed
  workload. `T=64` is worth considering *only* for read/insert-heavy + delete-light deployments
  (~9% faster insert, flat get; 31% slower delete). Not a default change.
- **Reorders the backlog:** the delete-at-high-fanout cliff makes the in-place rebalance
  sibling-clone micro-opt (documented in `task50` §6) a **prerequisite** for any future fanout
  increase —
  fixing it would flatten the delete regression and could shift the optimum `T` upward,
  capturing the read/insert gains without the delete penalty.

**3. Auto-increment bulk-append fast path in `insert_batch`.** The bulk-load bench showed
`Store::bulk_load` (from_sorted) beats `insert_batch` ~2× even with `insert_mut`. For
sequential auto-increment ids (Table's default), inserts always land in the rightmost leaf;
a batch could fill leaves directly instead of re-descending per key.
- **Payoff:** up to ~2× on the dominant sequential `insert_batch` path (grounded by the
  measured from_sorted↔insert_batch gap). **Effort:** medium.

### Tier 3 — profile-gated, marginal (read side)

**4. Branchless / SIMD-linear `find_pos` (u64-key specialization).** `binary_search_by` over
≤63 entries has branch mispredicts; a branchless search or 8-wide SIMD compare (u64 keys
only) could speed node search. **Payoff:** ~10–30% on a point-lookup *microbench*, less
end-to-end; only worth it if a flamegraph shows node search dominating. **Effort:** medium
(generic `K` complicates; likely a u64 specialization). **5. `get` recursion→loop, descent
prefetch** — micro (<5%), profile-gated.

### Tier 4 — big, invasive, defer

**6. Struct-of-arrays node layout** (separate `keys: Vec<K>` / `vals: Vec<Arc<V>>` so binary
search touches only keys → denser cache lines). Could help read on large trees, but it's a
full rewrite touching every path **and** a formal-kernel re-verify. Defer unless profiling
justifies.

## Cross-cutting recommendation

Run `make bench/flamegraph` (→ `target/criterion/*/profile/flamegraph.svg`) **before**
committing to Tier 3/4. The read structures are already lean, so those payoffs are
speculative until a profile confirms where time actually goes. Tier 1 (`remove_mut`) and
Tier 2 (fanout A/B) do not need a profile first — they're grounded in the measured insert
win and the from_sorted gap.

## Recommended order

1. ✅ `remove_mut` (Tier 1) — shipped (`task50`).
2. ✅ Fanout A/B experiment (Tier 2.2) — done; `T=32` confirmed near-optimal, and gated the
   in-place rebalance micro-opt as the prerequisite for any fanout increase (see #2 above).
3. In-place rebalance sibling-clone micro-opt (`task50` §6) — now the top open item: unblocks
   both delete-heavy random workloads and a future fanout bump.
4. Bulk-append fast path (Tier 2.3) — if the sequential-insert path matters.
5. Flamegraph, then reconsider Tier 3/4 with data.
