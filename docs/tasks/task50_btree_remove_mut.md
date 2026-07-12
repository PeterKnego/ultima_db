# task50: In-place B-tree delete (`BTree::remove_mut`)

**Status:** Implemented and validated. The symmetric twin of `insert_mut` (task48).
Correctness proven by 5 dedicated unit tests (equivalence to the immutable `remove` +
snapshot-isolation preservation, incl. isolation under merge) plus a concurrent-reader
isolation integration test; the full existing suite (387 lib + 82 `store_integration`) passes
unchanged; clippy clean; the formal drift guard passes (kernel mirror + differential test).
A/B benchmarked: **~26× faster** on ascending and **~9.8× faster** on random delete-heavy
batches at the raw B-tree layer — see §5. (Both figures include the in-place rebalancing
follow-up, §5.1; the initial descent-only version was ~18×/~3.7×.)

**Related:**
- `benches/btree_remove_mut_bench.rs` (micro A/B: immutable `remove` vs in-place `remove_mut`, ascending + random)
- `docs/tasks/task48_btree_insert_mut.md` (the insert twin this mirrors)
- `docs/superpowers/specs/2026-07-08-btree-remove-mut-design.md` (design spec)
- `docs/superpowers/specs/2026-07-08-btree-optimization-candidates.md` (the optimization backlog this is Tier 1 of)
- `docs/superpowers/plans/2026-07-08-btree-remove-mut.md` (implementation plan)
- `docs/tasks/task46_formal_btree_verification.md` (the immutable `remove` this mirrors)

## 1. Motivation

`insert_mut` removed the per-key spine-reallocation cost from inserts/updates by descending
through `Arc::make_mut` — cloning a node only when it is still shared with an older snapshot,
and otherwise mutating it in place. Deletes were left on the immutable path: `remove` →
`delete_from_node` **clones `entries` and `children` at every level of the root→leaf path on
every call**, plus `remove_leftmost` and the rebalancers. A `WriteTx` deleting many rows from
a privately-owned data tree rebuilt a spine nothing else referenced on each delete — exactly
the waste `insert_mut` eliminated for the insert side.

The measured immutable baseline confirmed the opportunity: deleting every key one at a time
cost **3.12 s** (ascending) / **3.66 s** (random) at 1M keys — essentially the same as the
pre-`insert_mut` immutable *insert* (3.36 s / 3.51 s), i.e. delete paid the identical per-key
spine clone.

## 2. Design

`remove_mut(&mut self, key: &K) -> bool` mirrors the recursive immutable delete, but each
level takes `&mut Arc<BTreeNode>` and opens it with `Arc::make_mut` (the sole copy-on-write
point) before mutating in place. Returns `true` iff the key was present; decrements `self.len`
iff it removed something.

New code in `src/btree.rs`:

- **`DeleteOutcome { NotFound, Removed { underfull: bool } }`** — the outcome type. The mutated
  node flows back through the caller's `&mut Arc<BTreeNode>`, so only the found/underfull flags
  propagate up.
- **`delete_from_node_mut(node: &mut Arc<BTreeNode>, key) -> DeleteOutcome`** — `Arc::make_mut`
  the node, then: leaf → `entries.remove(i)`; internal-with-key → pull the in-order successor
  in place via `remove_leftmost_mut` and overwrite the entry; internal-descend → recurse into
  the child. On child underflow, rebalance.
- **`remove_leftmost_mut(node: &mut Arc<BTreeNode>) -> ((K, Arc<V>), bool)`** — in-place
  minimum-key extraction, fixing underfull children on the way up.
- **`BTree::remove_mut`** — calls `delete_from_node_mut(&mut self.root, key)`; on `Removed`
  decrements `len` and applies the existing **root-collapse** (an empty internal root with one
  child drops a level — the child Arc is moved up, no clone).

**Key simplification — the rebalance helpers are reused verbatim.** `fix_underfull_child`,
`rotate_right`, `rotate_left`, `merge_with_left`, and `merge_with_right` already take
`&mut Vec<..>` of the *parent's* entries/children and mutate them in place. Because
`delete_from_node_mut` hands them the `make_mut`'d parent's real `entries`/`children`, they
work unchanged — **no in-place `*_mut` rebalance variants were needed.** The guaranteed win
comes entirely from not cloning the parent's Vecs on the descent (paid on every delete);
rebalancing only fires on `MIN_KEYS` underflow (rare, amortized).

Snapshot isolation is preserved by construction, exactly as for `insert_mut`: `make_mut`
clones any node still shared with a snapshot before touching it, so the snapshot keeps its
original Arcs.

## 3. Correctness

Unit tests in `src/btree.rs` (mirror of the `insert_mut` set):

- `remove_mut_matches_remove_scrambled` — interleaved LCG insert/remove churn (present + absent
  keys, forcing rotate/merge/collapse); the in-place tree stays structurally identical (full
  `dump`) to one built with the immutable `remove`, and `len` tracks `std::collections::BTreeMap`
  throughout.
- `remove_mut_preserves_snapshot_isolation` — snapshot a tree, then delete half its keys in
  place; the snapshot is completely unaffected while the live tree reflects every deletion.
- `remove_mut_chained_snapshots_independent` — repeated snapshot-then-delete cycles; each
  chained snapshot retains what it saw at capture time.
- `remove_mut_absent_key_is_noop` — deleting a missing key returns `false` and leaves the tree
  unchanged.

Integration test in `tests/store_integration.rs`:

- `uncommitted_remove_mut_invisible_to_concurrent_reader` — a barrier-ordered reader thread,
  released only after the writer performs its uncommitted in-place deletes, sees all N rows
  (deletes invisible); the committed deletions become visible only to a fresh read afterward.

Formal drift guard: `BTree::remove_mut` is mirrored in `formal/kernel/src/lib.rs` (modeled
functionally as `*self = self.remove(key)` — the `Arc::make_mut` clone-vs-mutate distinction
is invisible in the uniquely-owned `Box` model) with a differential test
`remove_mut_matches_remove_and_std_btreemap` pinning `remove_mut ≡ remove ≡ std::BTreeMap`.
`formal/scripts/check-drift.sh` passes.

## 4. `Table` wiring

- `Table::delete` keeps its `get_arc` presence check + index maintenance, then calls
  `self.data.remove_mut(&id)` (presence already guaranteed, so it returns `true` — guarded by
  `debug_assert!`).
- `Table::delete_batch` switches its per-id loop to `remove_mut`; atomic rollback is
  **unaffected** — `snapshot()` clones the root Arc first, so the first `remove_mut` CoW-clones
  and leaves the captured tree intact for `restore()` on a mid-batch failure (identical
  reasoning to `insert_batch`).

## 5. Benchmarks

`benches/btree_remove_mut_bench.rs`: each arm builds a privately-owned tree of N keys (untimed
setup, via `insert_mut`) and times deleting **every** key. Immutable arm rebuilds the spine per
delete; in-place arm descends via `Arc::make_mut`. Dev sandbox, criterion, `sample_size = 10`.

Current numbers (with the in-place rebalancing of §5.1):

| workload | immutable `remove` | in-place `remove_mut` | speedup |
|---|--:|--:|--:|
| delete ascending, 100K | 249.3 ms | 8.6 ms | **~29×** |
| delete ascending, 1M | 3.06 s | 117.3 ms | **26.1×** |
| delete random, 100K | 263.0 ms | 14.0 ms | **~19×** |
| delete random, 1M | 3.44 s | 350.9 ms | **9.8×** |

The **ratios are the deliverable** — A/B cancels common-mode host noise; the absolute numbers
are sandbox-relative. Random delete is still a smaller win than ascending (9.8× vs 26×) because
scattered deletion order triggers far more rebalancing and has worse cache locality — but the
gap is now driven by rebalance *frequency* and cache behaviour, not allocation (§5.1 removed the
per-rebalance sibling clone).

### 5.1 Follow-up: in-place rebalancing (2026-07-08)

The initial `remove_mut` reused the existing `rotate_*`/`merge_*` helpers verbatim, which
rebuilt the two involved sibling nodes by **cloning** (`left.entries.clone()` + `Arc::new(...)`).
On every underflow that cost two full node allocations — the dominant remaining cost on
random-delete workloads (which rebalance constantly). The follow-up rewrote those helpers to
mutate the siblings **in place** via `Arc::make_mut` (rotate) and `Arc::try_unwrap` (merge's
`absorb` helper), with `slice::split_at_mut` to hold `&mut` to two adjacent children at once.

The helpers are shared by both delete paths, and the CoW rewrite is correct for both:
- **`remove_mut` on a private tree:** siblings are uniquely owned → `make_mut` mutates in place,
  `try_unwrap` moves the merged node's contents. The win.
- **Snapshotted tree / immutable `remove`:** siblings are shared → `make_mut` clones and
  `try_unwrap` fails to a clone — byte-identical to the previous behaviour; snapshots preserved.

Measured effect on the `in_place` arm (same host, before → after): ascending 1M 169.6 → 117.3 ms
(−31%), random 1M 994.6 → 350.9 ms (**−65%, ~2.8× faster**); random 100K 47.4 → 14.0 ms (−70%).
The immutable arm was unaffected (−2%/−6%, noise). Snapshot safety of the shared-sibling path is
pinned by `remove_mut_merge_under_snapshot_preserves_isolation` (deletes a 3000-key contiguous
prefix while a snapshot holds the merged siblings) and `remove_mut_preserves_snapshot_isolation`.

## 6. Notes / limitations

- **Eager `Arc::make_mut` before presence is known (accepted trade-off).** `delete_from_node_mut`
  opens each node with `make_mut` before the key's presence in that subtree is known, so
  `remove_mut(&absent_key)` on a *snapshotted* tree clones the search path needlessly. This is
  a conscious trade-off, not an oversight: correctness and snapshot isolation are unaffected;
  the **hot path has zero waste** (present-key deletes mutate every path node anyway, and all
  `Table` callers `get`-check before deleting, per the CLAUDE.md check-before-delete convention,
  so `Table` never hits the absent path); and the obvious "`get`-first guard" would *regress* the
  hot path by adding a full traversal per delete (and would triple `Table`'s traversal count).
  A single-pass in-place delete fundamentally cannot avoid make-mut-before-descend on the
  internal-miss case without a presence pre-check.
- **Rebalance sibling clone — DONE (§5.1).** The `rotate_*`/`merge_*` helpers now mutate the two
  involved sibling nodes in place (`Arc::make_mut` / `Arc::try_unwrap` + `split_at_mut`) instead
  of cloning them. This was the main reason the random-delete win was smaller than ascending;
  eliminating it gave random-delete a further ~2.8× (see §5.1). The fanout experiment
  (`docs/superpowers/specs/2026-07-08-btree-optimization-candidates.md` §Tier 2.2) had flagged
  this as the prerequisite for any future `T` increase, since the old sibling clone made large
  fanout degrade delete badly. With the clone gone, the fanout sweep was re-run on the AWS NVMe
  bench host (2026-07-09, `scripts/fanout_ab.sh` / `make bench/fanout`, new `benches/btree_get_bench.rs`
  read column): the delete cliff is gone (0.81 @T=64 / 0.80 @T=128 vs the old 1.31 / 1.89) and the
  optimum shifted up — **T=64 dominates T=32 on all three axes** (get −18%, insert −8%, remove −19%).
  Full table + provenance in the candidates doc §2; the recommendation is to bump the default to
  `T=64`.
- **Contended-workload cross-check (SMR-apply + read-p99-under-load).** The fanout win above was
  measured on *uncontended* bulk ops. The autobench perf gate (`make perf/check`) exercises the
  opposite regime — SMR apply of SmallBank batches plus reads run *concurrently* with writes, so
  `Arc::make_mut` frequently CoW-clones shared nodes. Bench-host A/B (`scripts/smr_apply_ab.sh` /
  `make bench/smr-ab`, 15 runs/arm, AWS NVMe 2026-07-09) at T ∈ {32,48,64}:
  - **apply_sw_batch_throughput** (tight, monotonic, non-overlapping arms): T=48 **0.81×**, T=64
    **0.78×** — a real **−22%** SMR-apply throughput regression at T=64 (bigger nodes → bigger
    memmove/CoW per applied batch). Does *not* recover at T=48.
  - **read_p99_under_load_ns**: T=64 **0.38×** (**−62%**, tight [715–958] vs T=32 [2194–2367]) — a
    large, consistent tail-latency *improvement* on real hardware. (An earlier sandbox run showed
    the opposite sign — a VM artifact; the bench host is authoritative.)
  - **T=48 is dominated** — takes almost the full apply hit while its p99 is *bimodal* (four runs
    ~1000 ns, eleven ~2300 ns; median ≈ T=32) and it keeps only ~half the bulk get win. No useful
    middle ground; the metrics are non-monotonic in `T`.
  - **Decision: keep `T=64`.** The −22% SMR-apply cost is accepted for the broad wins (get −28%,
    read p99 −62%, bulk faster). Revisit only for an SMR/Raft-apply-bound deployment (see the
    "configurable fanout" note below / candidates doc). Autobench baseline should be re-recorded at
    T=64 on the gate's machine (the committed baseline predates the bump; `make perf/check` also
    fails on machine-mismatch independent of `T`).
- **Default bumped to `T=64` (commit `7f6c3fb`).** One-const change; `cargo test` + `clippy -D
  warnings` clean with and without the `persistence` feature. Correctness is unaffected — the
  delete/insert algorithm, rebalance semantics, and the `MIN_KEYS = T-1` / `MAX_KEYS = 2T-1`
  relations are all unchanged and generic in `T`.
- **Formal model re-instantiated at `T=64` — drift closed.** The Lean model in `formal/` was
  re-instantiated to match production: bumped `formal/kernel/src/lib.rs` `T` to 64, re-extracted the
  translation (charon → aeneas, toolchain via `formal/scripts/fetch-toolchain.sh`), and updated the
  proof constants that scale with `T` — `MAX_KEYS` 63→127, `MIN_KEYS` 31→63, split median/counts
  (`mid=64`, pre-split `2T=128`, child counts `T+1=65`), the `NodeInv` arity bound and `MinArity`
  balance invariant, and the underfull thresholds. All parametric (the split/merge relationships —
  `merge-fits = 2(T−1)+1 = 2T−1 = MAX_KEYS`, split median at index `T` — are preserved at any `T`).
  Verified: `lake build` clean (1719 jobs), the differential kernel test passes, the generated
  `BtreeKernel.lean` has **zero axioms**, and all eight top-level theorems
  (`BTree.{insert,remove}_{inv,get,frame}`, `remove_total`, `remove_spec`) depend on only the three
  standard Lean axioms (`propext`, `Classical.choice`, `Quot.sound`). Because both `src/btree.rs` and
  `formal/` changed, `formal/drift-check` now passes normally — **no `[skip-formal-drift]` needed**.
- **Immutable `remove` is retained** — `remove_mut` is strictly additive. The delete algorithm,
  `MIN_KEYS`, and rebalance semantics are unchanged; only the allocation discipline differs,
  which the persistent structure makes observationally identical.
- The CoW / snapshot-isolation guarantee is the load-bearing property and is not formally
  modellable (Arc refcounts are out of Aeneas scope) — it rests on the same `make_mut` reasoning
  as `insert_mut` plus the concurrent-reader integration test.
