# task50: In-place B-tree delete (`BTree::remove_mut`)

**Status:** Implemented and validated. The symmetric twin of `insert_mut` (task48).
Correctness proven by 4 dedicated unit tests (equivalence to the immutable `remove` +
snapshot-isolation preservation) plus a concurrent-reader isolation integration test; the
full existing suite (386 lib + 82 `store_integration`) passes unchanged; clippy clean; the
formal drift guard passes (kernel mirror + differential test). A/B benchmarked:
**~18× faster** on ascending and **~3.7× faster** on random delete-heavy batches at the raw
B-tree layer — see §5.

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

| workload | immutable `remove` | in-place `remove_mut` | speedup |
|---|--:|--:|--:|
| delete ascending, 100K | 249.3 ms | 14.9 ms | **16.8×** |
| delete ascending, 1M | 3.12 s | 169.6 ms | **18.4×** |
| delete random, 100K | 263.0 ms | 47.4 ms | **5.5×** |
| delete random, 1M | 3.66 s | 994.6 ms | **3.7×** |

The **ratios are the deliverable** — A/B cancels common-mode host noise; the absolute numbers
are sandbox-relative. The immutable baseline reproduced across two runs (~3.66 s random @1M),
so the cross-run random ratio is trustworthy.

Random delete is a smaller win than ascending (~3.7× vs ~18×) because scattered deletion order
triggers far more rebalancing (rotations/merges), and the rebalance path still **clones the two
involved sibling nodes** (see §6) — that allocation is not eliminated by the in-place descent.
This mirrors `insert_mut`'s own ascending-vs-random asymmetry (~21× vs ~8×), where splits are
rarer than delete's rebalancing.

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
- **Rebalance sibling clone (future micro-opt).** The reused `rotate_*`/`merge_*` helpers still
  rebuild the two involved sibling nodes by cloning. This fires only on `MIN_KEYS` underflow, so
  it is amortized-rare, but it is the main reason the random-delete win is smaller than
  ascending. In-place sibling rebalancing (via `split_at_mut` + `make_mut` on the siblings) is a
  plausible follow-up if delete-heavy random workloads show up in profiles.
- **Immutable `remove` is retained** — `remove_mut` is strictly additive. The delete algorithm,
  `MIN_KEYS`, and rebalance semantics are unchanged; only the allocation discipline differs,
  which the persistent structure makes observationally identical.
- The CoW / snapshot-isolation guarantee is the load-bearing property and is not formally
  modellable (Arc refcounts are out of Aeneas scope) — it rests on the same `make_mut` reasoning
  as `insert_mut` plus the concurrent-reader integration test.
