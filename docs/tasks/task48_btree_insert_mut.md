# task48: In-place B-tree insert (`BTree::insert_mut` / `insert_arc_mut`)

**Status:** Implemented and validated. Correctness proven by 4 dedicated unit tests
(equivalence to the immutable path + snapshot-isolation preservation); the full
existing suite (382 lib + 80 `store_integration`) passes unchanged; clippy clean.
A/B benchmarked: **~20–27× faster** end-to-end (`Store → WriteTx → Table`), **8–25× faster**
at the raw B-tree layer across ascending/random/snapshot-mixed patterns — see §5.
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

**Methodology.** Every number below is an **A/B ratio** — `immutable` and `in_place`
timed against each other. Both micro benches are within a single criterion process
(`insert` and `insert_mut` back to back); the e2e bench uses the revert protocol (save the
in-place run as baseline `inplace`, revert the six `Table` call sites, re-run against the
baseline). Because both arms run under the same host contention, noisy-neighbour variance
is *common-mode* and cancels in the ratio — so the ratios are trustworthy even on a shared
CI/sandbox host (dev sandbox here). **Absolute** timings are sandbox-relative and not
publishable; re-run `make bench/save` on a quiet NVMe host (e.g. the c6id bench host) for
those. A dedicated bench host was *not* required for the A/B conclusion: the deltas
(8–27×) dwarf the ~±2× sandbox absolute-noise floor, and the ratio cancels it regardless.
Criterion settings: `--warm-up-time 1 --measurement-time 3 --sample-size 10`.

**Micro — `benches/btree_insert_mut_bench.rs`** (build a privately-owned tree of N keys):

| pattern          | N       | immutable | in_place | speedup |
|------------------|--------:|----------:|---------:|--------:|
| ascending        | 1,000   | 1.029 ms  | 79.8 µs  | 12.9×   |
| ascending        | 100,000 | 277.3 ms  | 12.76 ms | 21.7×   |
| ascending        | 1,000,000 | 3.362 s | 160.1 ms | 21.0×   |
| random           | 1,000   | 991.8 µs  | 95.4 µs  | 10.4×   |
| random           | 100,000 | 241.1 ms  | 16.82 ms | 14.3×   |
| random           | 1,000,000 | 3.506 s | 450.5 ms | 7.8×    |
| mixed_snapshot   | 100,000 | 273.9 ms  | 10.80 ms | 25.4×   |
| mixed_snapshot   | 1,000,000 | 3.325 s | 142.3 ms | 23.4×   |

- `random` (scattered keys) is the **smallest** win — less path overlap, more splits — yet
  still 7.8–14.3×: in-place avoids the per-key full-spine reallocation regardless of order.
- `mixed_snapshot` takes a live snapshot mid-build, so half the inserts hit the CoW branch
  of `make_mut`. It was included as a **pessimistic** guard against over-claiming; in
  practice it still wins ~23–25× because only the *first* touch of each shared spine node
  re-clones — the rest of the appended growth is in place.

**End-to-end — `benches/table_insert_e2e_bench.rs`** (real `Store → WriteTx → Table`,
in-memory), immutable vs in_place via the revert protocol:

| bench                | N       | immutable | in_place | speedup |
|----------------------|--------:|----------:|---------:|--------:|
| insert_batch         | 10,000  | 21.44 ms  | 809 µs   | 26.5×   |
| insert_batch         | 100,000 | 317.0 ms  | 12.77 ms | 24.8×   |
| single_insert_loop   | 10,000  | 21.22 ms  | 1.073 ms | 19.8×   |
| single_insert_loop   | 100,000 | 289.1 ms  | 14.10 ms | 20.5×   |
| incremental_append   | 10,000  | 37.19 ms  | 1.613 ms | 23.1×   |
| incremental_append   | 100,000 | 387.3 ms  | 14.60 ms | 26.5×   |

The e2e win (~20–27×) is if anything *larger* than the raw btree micro, because at the
transaction level the immutable path's per-key spine-realloc dominates while commit is
cheap (fast-path single Arc swap). `incremental_append` is the realistic steady-state case
— append into a table already holding 200 K committed rows, so the writer's cloned data
tree shares the committed snapshot's spine and pays the CoW-on-first-touch cost — and it
still runs **26.5×** faster. `single_insert_loop` (one `Table::insert` at a time, not
batched) confirms the win is not an artifact of batching.

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
