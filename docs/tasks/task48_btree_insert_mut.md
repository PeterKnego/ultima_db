# task48: In-place B-tree insert (`BTree::insert_mut` / `insert_arc_mut`)

**Status:** Implemented and validated. Correctness proven by 4 dedicated unit tests
(equivalence to the immutable path + snapshot-isolation preservation); the full
existing suite (382 lib + 80 `store_integration`) passes unchanged; clippy clean.
Benchmarked: **12–19× faster** on the auto-increment (ascending-id) insert path — see §5.
**Related:**
- `benches/btree_insert_mut_bench.rs` (micro A/B: immutable vs in-place, 3 access patterns)
- `benches/table_insert_e2e_bench.rs` (end-to-end through `Store → WriteTx → Table`)
- `docs/tasks/task46_formal_btree_verification.md` (the immutable `insert`/`remove` this mirrors)

---

## 1. Motivation

The persistent (copy-on-write) `BTree::insert` allocates a **fresh `Arc<BTreeNode>` for
every node on the root→leaf path, on every single call**. It has to: it cannot know
whether any node on that path is still shared with an older snapshot, so it conservatively
rebuilds the whole spine and returns a new `BTree`.

That is the correct default for the persistent API. But the dominant write pattern in
UltimaDB does **not** need it: a `WriteTx` lazily clones a table's data tree once (O(1)
Arc bump) and then applies *all* of that transaction's inserts to a tree it now privately
owns. Every insert after the first re-rebuilds a spine that nothing else references —
pure allocation + refcount churn. The auto-increment case (`Table` assigns sequential
ids) is the worst offender: successive keys share almost the entire right spine, so the
immutable path re-clones ~`height` nodes per key that it could have mutated in place.

## 2. Design

Two new methods on `BTree<K, V>`, in-place counterparts to `insert` / `insert_arc`:

```rust
pub fn insert_mut(&mut self, key: K, val: V);              // wraps val in Arc, delegates
pub fn insert_arc_mut(&mut self, key: K, val_arc: Arc<V>); // reuses an existing Arc<V>
```

They descend the tree through **`Arc::make_mut`** (`insert_into_node_mut`). `make_mut`
clones a node **iff** it is actually shared (`strong_count > 1`) and otherwise hands back
a unique `&mut` to mutate in place. This is exactly copy-on-write, applied one node at a
time on the descent:

- A node still visible to an older snapshot → cloned before mutation (isolation preserved).
- A node uniquely owned by this tree (e.g. one just created by an earlier insert in the
  same batch) → mutated directly (the win).

So a batch of inserts into a privately-owned tree drops from `O(height)` allocations +
refcount traffic **per key** to near-zero once successive keys share path nodes; only the
first touch of a shared spine node after a snapshot pays the CoW clone.

**Supporting pieces:**

- **`enum InsertOutcome<K,V>`** — the in-place analogue of `InsertResult`. It does *not*
  carry the mutated node (that flows back through the `&mut Arc<BTreeNode>` the caller
  passed); only the promoted `median` + new `right` sibling on a split, plus the
  insert-vs-replace flag, propagate up.
- **`maybe_split_mut`** — splits an overflowed node in place using `split_off`/`pop` so the
  **left half reuses the node's existing `Vec` allocation**; only the right sibling
  allocates. (The immutable `maybe_split` allocates three fresh Vecs via `to_vec`.) The
  split point (`mid = len/2` at `MAX_KEYS+1 == 64`) matches the immutable path exactly, so
  both produce identically-shaped trees.
- **Root split** is handled in `insert_arc_mut`: the mutated `self.root` is the left half,
  lifted under a fresh root alongside the promoted median.
- **Manual `Clone for BTreeNode<K,V>`** bounded on `K: Clone` only. `#[derive(Clone)]`
  would add a spurious `V: Clone` bound; values live behind `Arc<V>` and children behind
  `Arc<BTreeNode>`, so cloning a node clones only the keys and bumps refcounts — `V` is
  never cloned. This is what makes `Arc::make_mut` usable here without imposing `V: Clone`
  on callers.

The immutable `insert` / `insert_arc` are unchanged and remain the API for callers that
need a new tree.

## 3. Correctness

Copy-on-write / snapshot isolation is the load-bearing property, and it is preserved by
construction (`make_mut` clones shared nodes). Four unit tests in `src/btree.rs` pin it:

- `insert_mut_matches_insert_ascending` — 10 000 sequential keys (multi-level, many
  splits); the in-place tree is structurally identical to the immutable one.
- `insert_mut_matches_insert_scrambled_with_replaces` — LCG-scrambled order with repeated
  overwrites of existing keys (the `Ok(pos)` replace path); still identical.
- `insert_mut_preserves_snapshot_isolation` — take a snapshot, then overwrite every key
  and grow the live tree; the snapshot is **completely unaffected** (values, `len`,
  absent keys all as captured).
- `insert_mut_chained_snapshots_independent` — 50 snapshot-then-mutate cycles; each
  chained snapshot retains the value it saw at capture time (structural sharing across
  versions).

## 4. `Table` wiring

The six `Table` insert call sites that mutate a `&mut self`-owned data tree switched from
`self.data = self.data.insert(...)` to `self.data.insert_mut(...)` (and `insert_arc` →
`insert_arc_mut`): `insert`, `try_insert`, `upsert_arc`, `insert_batch`, `update_batch`,
and the recovery/`insert_with_id` path.

**Batch atomic rollback is unaffected.** `insert_batch` / `update_batch` capture state via
`snapshot()` (which does `self.data.clone()` — an O(1) Arc bump) *before* mutating. That
clone raises the root's `strong_count`, so the first `insert_mut` CoW-clones instead of
mutating in place, leaving the captured tree intact for `restore()` on failure. Same
guarantee as before, same O(1) cost.

## 5. Benchmarks

`benches/btree_insert_mut_bench.rs`, `insert_ascending` group (build a privately-owned
tree of N sequential keys), quick pass on the dev sandbox:

| N (ascending) | immutable | in_place | speedup |
|--------------:|----------:|---------:|--------:|
| 1,000         | 1.03 ms   | 81 µs    | ~12.6×  |
| 100,000       | 276 ms    | 15.6 ms  | ~17.7×  |
| 1,000,000     | 3.34 s    | 173 ms   | ~19.3×  |

The bench also covers two harder cases (present in the harness; re-run on a quiet host for
publishable numbers):
- `insert_random` — scattered keys: less path overlap, more splits, so a smaller (but
  still large) win.
- `insert_mixed_snapshot` — half the inserts happen *after* a live snapshot is taken, so
  those hit the CoW branch of `make_mut`. This is the **pessimistic** guard against
  over-claiming: in-place still wins, but by the smallest margin.

`benches/table_insert_e2e_bench.rs` drives the real `Store → WriteTx → Table` path
(`e2e_insert_batch`, `e2e_single_insert_loop`, `e2e_incremental_append`) so the win can be
measured as a user sees it, including the CoW-on-first-touch cost against a populated
table. Its header documents the A/B protocol (save `--baseline inplace`, revert the six
call sites, re-run) for regression tracking.

## 6. Notes / limitations

- The win is proportional to how much successive inserts share path nodes and to how
  privately the tree is owned. Sequential ids (UltimaDB's default) are best case; a tree
  freshly forked from a snapshot pays one CoW clone per shared spine node on first touch,
  then runs in place.
- No API removed: the immutable persistent `insert`/`insert_arc` stay as the CoW-returning
  API. `insert_mut` is strictly additive.
- `remove` was left on the immutable path (not in the measured hot loop for this task); an
  in-place `remove_mut` is a plausible follow-up if delete-heavy batches show up in
  profiles.
