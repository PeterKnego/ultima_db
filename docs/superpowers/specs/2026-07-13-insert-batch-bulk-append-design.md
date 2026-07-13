# Bulk-append fast path for `Table::insert_batch` — design

**Date:** 2026-07-13
**Status:** Approved design (approach chosen by Peter from A/B/C)
**Context:** Tier 2.3 of `docs/superpowers/specs/2026-07-08-btree-optimization-candidates.md` —
the last unstarted item of the ranked B-tree backlog. Task doc will be
`docs/tasks/task51_insert_batch_bulk_append.md`.

## Problem

`Table::insert_batch` phase 1 inserts each record with `BTree::insert_mut`: a full
root→leaf descent with per-node binary search for every key, and — because the keys are
strictly ascending — every leaf split leaves the left half at ~50% occupancy.
`BTree::from_sorted` (`BulkBuilder`) builds the same data O(N) with leaves packed to
`MAX_KEYS`. Task49 measured the resulting gap: `ub_sorted` beats `ub_batch` ~2× at 1 M
rows, even with `insert_mut`.

The enabling invariant: batch ids are `next_id, next_id+1, …`, and **every existing key
in the data tree is `< next_id`** (auto-increment ids; `merge_keys_from` maxes `next_id`
across concurrent writers; `bulk_load` rebuilds `next_id` past the loaded max; `upsert_arc`
never writes above the source writer's `next_id`). A batch is therefore a pure append past
the current max key — the tree never needs a general sorted-merge, only an append.

## Chosen approach: right-spine-seeded bulk append (option A)

Rejected alternatives: (B) `from_sorted` only when the table is empty — trivial but only
helps the first batch, and `Store::bulk_load` already covers that; (C) a rightmost-descent
`append_max_mut` per key — keeps per-key descent and sparse leaves, ~half the win.

### New: `BulkBuilder::seed_from_spine(tree: &BTree<K, V>) -> BulkBuilder<K, V>`

Reconstruct the builder state *as if it had just consumed the existing tree's entries*,
by unzipping the right spine (root→leaf, tree levels numbered leaf = 0):

- For the spine node at level `i > 0` (internal): `levels[i].entries` = clone of the
  node's `entries`; `levels[i].children` = clone of `children[..len-1]` — all but the
  rightmost child, whose slot stays open. This matches the builder's mid-build invariant
  (`children.len() == entries.len()`; the open slot is filled later by the carry in
  `finish()` or by `attach_child`). The excluded rightmost child *is* the next spine
  level, unzipped in the same way.
- For the spine leaf (level 0): `levels[0].entries` = clone of its `entries`; no children.
- `len` = `tree.len()`; `last_key` = the spine leaf's last key (the tree max).
- Off-spine children are attached as shared `Arc`s (refcount bump only). Cost:
  O(height × MAX_KEYS) entry/Arc clones per batch — negligible.
- Empty tree ⇒ equivalent to `BulkBuilder::new()`.

Snapshot isolation needs no `make_mut` here: the spine nodes are *read*, and `finish()`
builds **fresh** replacement spine nodes; the original tree (and any older snapshot
holding it) is untouched.

**Audit required during implementation:** `redistribute_tail` (and any helper that
mutates a previously-frozen sibling, e.g. via `Arc` unwrap) currently only ever sees
nodes freshly built by `from_sorted`, so it may assume exclusive ownership. With seeding,
a frozen sibling can be an original-tree node shared with a live snapshot — every such
mutation point must go through `Arc::make_mut` (clone-when-shared). Add a regression test
that would fail if it didn't (snapshot taken before an append whose tail-redistribute
touches an original node).

### New: `BTree::extend_from_sorted(&mut self, iter)` (`pub(crate)`)

Requires every input key strictly ascending and strictly greater than the current max
(debug_assert, same style as `from_sorted`). Implementation:
`seed_from_spine(self)` → `push` each `(K, Arc<V>)` → `*self = builder.finish()`.

### Wiring: `Table::insert_batch` phase 1

Replace the per-record `insert_mut` loop with one guarded call:

- Compute `start_id = self.next_id`; release-mode check `self.data.max_key() < start_id`
  (new trivial `max_key()` — rightmost descent, O(height)).
- Invariant holds (always, per above): `self.data.extend_from_sorted((start_id..).zip(records.into_iter().map(Arc::new)))`,
  then bump `next_id` and build `ids`.
- Invariant violated (defensive, should be unreachable): fall back to the existing
  per-key `insert_mut` loop. No panic — the fast path is an optimization, not a
  semantic change.

Phase 2 (index maintenance, rollback via snapshot-and-restore) is unchanged. No public
API change; no commit-path / `MergeableTable` change; `WriteTx` semantics identical.

## Formal drift

Any `src/btree.rs` change fires the drift guard. Follow the task48/task50 pattern:
mirror `extend_from_sorted` in `formal/kernel` as the functional equivalent (a fold of
the verified `insert`) and extend the differential test so the kernel and production
disagree loudly if the append path diverges. No `[skip-formal-drift]`.

## Tests

- **Equivalence** (mapping + `len`, not node-identity — packing legitimately differs):
  `extend_from_sorted` == repeated `insert_mut` for: empty tree; tree ending in an
  underfull tail leaf; tree ending exactly at `MAX_KEYS`; multi-level (≥3) spines;
  single-entry batches; a 100 k fuzz alternating random-sized batches with point
  inserts/removes between batches.
- **Invariants:** reuse the existing node-invariant checkers on the result of every
  case above (arity bounds, uniform depth, key order).
- **Snapshot isolation:** clone the tree (simulating an older snapshot), append, verify
  the clone is bit-for-bit unaffected — including a case sized to force
  `redistribute_tail` into an original (shared) node.
- **Fallback:** a table whose data tree contains a key ≥ `next_id` (built via test-only
  access) takes the slow path and stays correct.
- **Table level:** existing `insert_batch` tests must pass unchanged (ids, rollback on
  index-constraint violation, empty batch).

## Benchmarks

Add a batch-append arm to `benches/btree_insert_mut_bench.rs` (raw layer) and
`benches/table_insert_e2e_bench.rs` (Store→WriteTx→Table). Local runs are a magnitude
smoke test only (per bench methodology; the expected ~2× clears sandbox noise). The
authoritative number comes from the task49 harness on the next bench-infra run — not a
gate for merging this.

## Success criteria

1. All existing tests pass unchanged; new tests above pass; clippy clean.
2. Formal drift guard passes without a skip marker.
3. Local A/B shows `insert_batch` (sequential ids) approaching `from_sorted` at 100 k–1 M
   (gap shrinks from ~2× to ≲1.2×), and no regression on small batches.

## Process notes

Implement on a feature branch in an isolated worktree. Consolidate into
`docs/tasks/task51_insert_batch_bulk_append.md` before merge, keeping this spec and the
implementation plan per CLAUDE.md.
