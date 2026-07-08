# In-place B-tree delete (`BTree::remove_mut`) — design spec

**Date:** 2026-07-08
**Status:** Approved (design), ready for implementation plan
**Related:** `docs/tasks/task48_btree_insert_mut.md` (the insert twin this mirrors),
`docs/superpowers/specs/2026-07-08-btree-optimization-candidates.md` (Tier 1),
`formal/kernel/src/lib.rs` + `formal/scripts/check-drift.sh` (the remove path is formally
verified; this change re-fires the drift guard).

---

## 1. Motivation

`insert_mut` removed the per-key spine-reallocation cost from inserts/updates. Deletes still
pay it in full: `remove` → `delete_from_node` **clones `entries` and `children` at every
level of the root→leaf path on every call** (`src/btree.rs:705, 724–725, 745–746, 770,
783–784`), and `remove_leftmost` and the rebalancers do the same. A `WriteTx` that deletes
many rows from a privately-owned data tree re-rebuilds a spine nothing else references on
each delete — exactly the waste `insert_mut` eliminated for the insert side.

`remove_mut` is the symmetric fix: descend through `Arc::make_mut`, mutating uniquely-owned
nodes in place and cloning only nodes still shared with an older snapshot. Snapshot
isolation is preserved by construction, identically to `insert_mut`.

**Measured baseline (dev sandbox, 2026-07-08, `benches/btree_remove_mut_bench.rs`, immutable
arm — delete every key from a privately-owned tree):** delete-ascending 247 ms @100K /
**3.11 s @1M**; delete-random 261 ms @100K / **3.66 s @1M**. That is ~the same cost as the
pre-`insert_mut` immutable insert (277 ms / 3.36 s ascending; 241 ms / 3.51 s random) —
confirming delete pays the identical per-key spine-reallocation. `insert_mut` cut those to
160 ms (ascending) / 450 ms (random) @1M, so `remove_mut` should land in the same ~8–20×
range. A/B ratios cancel host noise; absolutes are sandbox-relative.

## 2. Scope

- **In:** new in-place delete on `BTree` and its recursive helpers; wire into
  `Table::delete` and `Table::delete_batch`; unit + integration tests; formal-kernel mirror.
- **Out:** the delete *algorithm* is unchanged (same successor-replacement, same
  rotate/merge/collapse rebalancing, same `MIN_KEYS`) — only the allocation discipline
  changes. The immutable `remove` stays as the CoW-returning API. No fanout/layout changes.

## 3. Public API

```rust
impl<K: Ord + Clone, V> BTree<K, V> {
    /// In-place variant of [`remove`](Self::remove). Removes `key`, mutating
    /// `self`. Returns `true` if the key was present (and removed), `false` if
    /// absent (self unchanged). Copy-on-write preserved: nodes shared with an
    /// older snapshot are cloned before mutation.
    pub fn remove_mut(&mut self, key: &K) -> bool;
}
```

Returns `bool` (present?) rather than `Result`, matching the check-before-delete convention
(callers already `get` first) and letting callers update state without a sentinel error.
`self.len` is decremented iff `true`.

## 4. Internal design

Mirror of the existing recursive delete, but each level takes `&mut Arc<BTreeNode>` and
opens it with `Arc::make_mut` (the sole CoW point), then mutates in place.

**Outcome type** (analogue of the insert path's `InsertOutcome`; the node flows back through
the `&mut Arc`, so only flags return):

```rust
enum DeleteOutcome { NotFound, Removed { underfull: bool } }
```

**`delete_from_node_mut(node: &mut Arc<BTreeNode<K,V>>, key) -> DeleteOutcome`:**
- `let n = Arc::make_mut(node);` — clones iff shared, else mutates in place.
- Leaf: `binary_search_by`; on hit `n.entries.remove(i)` in place, report
  `Removed { underfull: n.entries.len() < MIN_KEYS }`; on miss `NotFound`.
- Internal, key present (`Ok(i)`): `remove_leftmost_mut(&mut n.children[i+1])` to pull the
  in-order successor in place; overwrite `n.entries[i]`; if the successor subtree underfull,
  `fix_underfull_child_mut(n, i+1)`; report underfull by `n.entries.len()`.
- Internal, key in subtree (`Err(ci)`): recurse `delete_from_node_mut(&mut n.children[ci], key)`;
  on `Removed{underfull}` and underfull, `fix_underfull_child_mut(n, ci)`; propagate
  `NotFound` unchanged; report underfull by `n.entries.len()`.

**`remove_leftmost_mut(node: &mut Arc<BTreeNode<K,V>>) -> ((K, Arc<V>), bool)`** — same
make_mut descent to the leftmost leaf, `entries.remove(0)` in place, fix underfull children
on the way up.

**`remove_mut` (root handling):** call `delete_from_node_mut(&mut self.root, key)`; on
`Removed`, decrement `self.len`; then the existing **root-collapse**: if the root is now an
empty internal node with one child, replace `self.root` with that child
(`let child = std::mem::take(...)`-style; the child Arc is moved up, no clone). Return
whether removed.

**In-place rebalancing.** The existing `fix_underfull_child` / `rotate_right` /
`rotate_left` / `merge_with_left` / `merge_with_right` already take `&mut Vec<..>` of the
*parent's* entries/children and rebuild the two involved sibling nodes by **cloning** them
(`left.entries[..].to_vec()`, `right.entries.clone()`, etc.). The `*_mut` versions operate
on `n.entries`/`n.children` in place and mutate the two sibling nodes via `Arc::make_mut`
instead of cloning. The borrow subtlety — needing `&mut` to two adjacent `children[i-1]` and
`children[i]` at once — is resolved with `slice::split_at_mut` (or take-make_mut-reput);
name the technique in the plan.

> **Frequency note for planning:** the make_mut *descent* is paid on every delete and is the
> guaranteed win; rebalancing fires only on `MIN_KEYS` underflow (rare, amortized). The plan
> may land the in-place descent first (biggest win, simplest) and the in-place rebalancers
> as a second task, keeping each independently testable. Both are in scope.

## 5. Correctness

Snapshot isolation is preserved by construction (`make_mut` clones shared nodes before any
mutation), exactly as for `insert_mut`. Pinned by unit tests in `src/btree.rs` mirroring the
`insert_mut` set:

- `remove_mut_matches_remove_scrambled` — interleaved LCG insert/remove churn (mixing
  present & absent keys, forcing rotate/merge/collapse); the in-place tree stays structurally
  identical (full `range` dump) to one built with the immutable `remove`, and `len` matches
  `std::collections::BTreeMap` throughout.
- `remove_mut_preserves_snapshot_isolation` — build a tree, snapshot it (O(1) clone), then
  delete many keys via `remove_mut`; the snapshot is completely unaffected (values, `len`,
  absent keys as captured); the live tree reflects every deletion.
- `remove_mut_chained_snapshots_independent` — repeated snapshot-then-delete cycles; each
  chained snapshot retains what it saw at capture time.
- Absent-key path: `remove_mut` on a missing key returns `false` and leaves the tree
  (and any snapshot) untouched.

Integration test in `tests/store_integration.rs` mirroring
`uncommitted_insert_mut_invisible_to_concurrent_reader`: a barrier-ordered reader thread
proves an uncommitted writer's in-place **deletes** are invisible to a concurrent read and
become visible only at commit.

## 6. `Table` wiring

- `Table::delete` (`src/table.rs:368`): keep the `get_arc` presence check + index
  maintenance; replace `self.data = self.data.remove(&id)?` with `self.data.remove_mut(&id)`
  (the presence check already guarantees it returns `true`).
- `Table::delete_batch` (`src/table.rs:506`): the per-id loop switches to `remove_mut`;
  atomic rollback is **unaffected** — `snapshot()` clones the root Arc first, so the first
  `remove_mut` CoW-clones and leaves the captured tree intact for `restore()` on failure
  (identical reasoning to `insert_batch`).

## 7. Formal drift guard

`src/btree.rs`'s remove path is mirrored in `formal/kernel/src/lib.rs` and machine-checked,
so this change re-fires the drift guard. Resolve it the same way as `insert_mut` (see the
commit `formal(kernel): mirror insert_mut`): add `BTree::remove_mut` to the kernel modeled as
`*self = self.remove(key)` (the `Arc::make_mut` clone-vs-mutate distinction is invisible in
the functional `Box` model, so the faithful mirror is the in-place assignment of `remove`'s
result), plus a differential test pinning `remove_mut ≡ remove ≡ std::BTreeMap` across
interleaved churn. `make test/formal-kernel` must pass; the CoW/isolation property stays out
of Aeneas scope (covered by the integration test). Confirm the drift guard passes locally
(`BASE=origin/main formal/scripts/check-drift.sh`).

## 8. Verification

- `cargo test --lib btree` (new `remove_mut_*` tests) + full lib + `store_integration`.
- `cargo clippy --all-targets` clean (the gated lint).
- `make test/formal-kernel` (kernel differential test incl. the new `remove_mut` case).
- A/B the win: extend `benches/btree_insert_mut_bench.rs` (or a sibling) with a delete-heavy
  group — immutable `remove` vs `remove_mut` on ascending/random churn — to confirm the
  expected ~8–20× and record it in a `task50`-style doc on completion.

## 9. Non-goals / notes

- No API removed; immutable `remove` stays. `remove_mut` is strictly additive.
- Delete algorithm, `MIN_KEYS`, and rebalance semantics unchanged — only allocation
  discipline differs, which the persistent structure makes observationally identical.
- The CoW/snapshot-isolation guarantee is the load-bearing property and is *not* formally
  modellable (Arc refcounts are out of Aeneas scope) — it rests on the same `make_mut`
  reasoning as `insert_mut` plus the concurrent-reader integration test.
