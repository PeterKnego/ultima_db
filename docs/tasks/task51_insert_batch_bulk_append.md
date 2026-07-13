# task51: `insert_batch` bulk-append fast path (`BTree::extend_from_sorted`)

**Status:** Implemented and validated. `Table::insert_batch` now builds its batch
through a right-spine-seeded `BulkBuilder` instead of a per-key `insert_mut` loop,
since batch ids are always a contiguous append past the table's current max key.
Correctness proven by an equivalence suite (mapping + `len` vs per-key `insert_mut`),
a snapshot-isolation suite (incl. a per-round-snapshot fuzz), a `Table`-level
equivalence + fallback test, and a formal-kernel differential test; the full existing
suite passes unchanged; clippy clean. **Along the way, this task's fuzzing exposed and
fixed two pre-existing latent bugs in `BulkBuilder::finish()` that shipped on `main`**
— see §3, which matters beyond this task: it fixes `Store::bulk_load` for anyone
restoring data at specific cascade-aligned row counts. Sandbox bench smoke shows
`insert_batch` within the target ~1.2× of `bulk_load_sorted` at 100k+ (previously ~2×)
— see §6 (non-authoritative; authoritative number deferred to a `bench-infra` run).

**Related:**
- `docs/superpowers/specs/2026-07-13-insert-batch-bulk-append-design.md` (design spec, main checkout)
- `docs/superpowers/plans/2026-07-13-insert-batch-bulk-append.md` (implementation plan, main checkout)
- `docs/tasks/task48_btree_insert_mut.md` (the in-place insert this reuses on the fallback path and on `pending_reinsert`)
- `docs/tasks/task49_bulk_load_bench.md` (the bench harness authoritative numbers will be re-recorded through)
- `docs/tasks/task50_btree_remove_mut.md` (the rebalance primitives `fix_right_spine_tail` reuses; also this doc's template)
- `docs/tasks/task23_bulk_load.md` (`Store::bulk_load`, whose `from_sorted` path shares the fixed `finish()` machinery)
- `.superpowers/sdd/task-3-report.md` (this branch's debugging narrative for §3, worktree-local)

---

## 1. Motivation

`Table::insert_batch` inserted every record in a batch one key at a time via
`BTree::insert_mut`. Since task48, `insert_mut` already avoids the immutable path's
per-key spine reallocation (it descends through `Arc::make_mut`, cloning a node only
if still shared), so a large batch into a privately-owned data tree is cheap relative
to the old immutable baseline — but it is still `O(batch × height)` traversal work:
every one of the `n` records in a batch walks the tree from the root, even though the
whole batch is a single contiguous run of new auto-increment ids.

`Store::bulk_load` / `BTree::from_sorted` already has an `O(n)` bulk-build path
(`BulkBuilder`) for exactly this shape of workload — sorted, no existing structure to
merge with. The gap task51 closes: `Table::insert_batch`'s ids are *always*
`next_id..next_id + n`, and every existing key in the table's data tree is `< next_id`
(the auto-increment invariant: `merge_keys_from` maxes `next_id` across writers on
commit, and `bulk_load` rebuilds `next_id` past the loaded max). A batch insert is
therefore never a "merge sorted input into an arbitrary tree" problem — it is always
a pure **append** past the current maximum key, which is the one case `BulkBuilder`
can be *seeded* for instead of started from scratch: reconstruct its levels from the
existing tree's right spine, then keep pushing.

## 2. Design

### `BulkBuilder::seed_from_spine(tree: &BTree<K, V>) -> BulkBuilder<K, V>`

Reconstructs builder state as if it had just consumed `tree`'s entries, by unzipping
the tree's **right spine** (the root→leaf path through each level's last child):

- Walk root→leaf, collecting each spine node into a `Vec` (root first).
- Builder levels are leaf-first (level 0 = leaf), so the spine is consumed in
  reverse. For an internal spine node at builder level `i > 0`: `entries` = a clone
  of the node's entries; `children` = a clone of all but the *last* child — that
  slot stays open, matching the builder's mid-build invariant
  (`children.len() == entries.len()`, with the rightmost slot filled later by
  `attach_child` or `finish()`'s carry). The excluded rightmost child is exactly the
  next spine level down, unzipped the same way.
- The spine leaf becomes `levels[0]` (entries only, no children).
- `len = tree.len()`; `last_key` = the spine leaf's last key (`push`'s ascending-order
  debug_assert then also implicitly enforces the append-past-max invariant).
- Empty tree ⇒ `BulkBuilder::new()`.

Cost is `O(height × MAX_KEYS)` — clones of the entries/Arc-bumps of the children
sitting on the spine, negligible next to the batch itself. `BTree::extend_from_sorted`
is then just `seed_from_spine(self)` → `push` every `(K, Arc<V>)` in the iterator →
`*self = builder.finish()`, reusing `finish()` unchanged (see §3 for what changed
inside `finish()` to make it handle seeded — not just from-scratch — state).

### Why this is snapshot-safe: the two-part `redistribute_tail` argument

`seed_from_spine` only *reads* the existing tree (clones entries, bumps child Arcs);
`finish()` builds entirely fresh replacement nodes. So a tree with a live snapshot
sharing its spine is untouched by the read — the risk is entirely inside `finish()`'s
rebalancing, specifically `redistribute_tail`, which pops a "sibling" node off a
builder level to borrow entries from an underfull tail node. Under `from_sorted`,
that popped node was always freshly built by this same call (never shared). Under
`extend_from_sorted`, a seeded level's popped sibling *could*, in principle, be an
original tree node still shared with an older snapshot.

The doc comment on `seed_from_spine` (`src/btree.rs`) states the two-part reason this
is still safe without needing an `Arc::make_mut` in `redistribute_tail` itself:

1. `redistribute_tail` copies the popped sibling (`to_vec()`) and constructs a fresh
   `Arc::new(BTreeNode { .. })` — it never mutates *through* the popped Arc. Whatever
   the popped node's origin, the original Arc (and anything holding it) is
   unaffected.
2. `redistribute_tail` can only ever pop a **freshly-frozen full** node in the first
   place: a level that is underfull at `finish()` time with a parent sibling present
   must have frozen *during this build* (freezing only happens at `MAX_KEYS`, and a
   never-frozen seeded level keeps its original `>= MIN_KEYS` entries and only grows
   from there — it can't be the level `redistribute_tail` fires on). So the "popped
   sibling could be an untouched seeded node" case this task worried about at spec
   time cannot actually arise: point 2 means the popped node was always born inside
   this `finish()` call, and point 1 means even if it hadn't been, mutation wouldn't
   reach the original Arc.

This is why the spec's planned "construct a case where `redistribute_tail` touches an
original (shared) node" regression test turned out to be **unreachable** — see §4 for
what replaced it.

### `Table::insert_batch` wiring

Phase 1 of `insert_batch` computes `start_id = self.next_id`, then checks
`self.data.max_key().is_none_or(|k| *k < start_id)`:

- **Invariant holds** (the expected case, always true in practice): take the fast
  path — `self.data.extend_from_sorted(records enumerated as (start_id + i, Arc::new(record)))`,
  then bump `next_id` by the batch length.
- **Invariant violated** (defensive; unreachable given `next_id`'s bookkeeping, but a
  violated invariant must degrade gracefully rather than corrupt the packed builder):
  fall back to the original per-key `insert_mut` loop, one id at a time. No panic —
  the fast path is purely an optimization, never a semantic change. `Table` tests
  exercise this branch directly (see §4) by corrupting `next_id` below the max key.

Phase 2 (index maintenance) and atomic rollback (snapshot-and-restore) are unchanged
— both operate on the resulting `ids`/table state identically regardless of which
phase-1 path built it.

## 3. Pre-existing `BulkBuilder::finish()` bugs found and fixed (matters beyond task51)

The equivalence/fuzz testing this task added (`extend_from_sorted_fuzz_alternating_batches_and_removes`)
surfaced **two independent, pre-existing bugs in `BulkBuilder::finish()`** that shipped
on `main` from earlier bulk-load work (the task23 bulk-load work) — **not specific to seeding**. Both
are reachable from a plain `BTree::from_sorted` call — and therefore from
`Store::bulk_load` — at specific input sizes the pre-existing test suite never
happened to hit. `extend_from_sorted`'s seeded levels start already near `MAX_KEYS`
rather than at 0, so the same tail shapes came up organically at realistic batch
sizes, which is how the fuzz test tripped over them; a targeted binary search then
confirmed both are also reachable from a bare `from_sorted(n)` at exact multiples of
`MAX_KEYS + 1` (= 128 at `T = 64`), the smallest failing case being
`n = (MAX_KEYS + 1)^2 = 16384`.

**Impact if unfixed:** `BTree::from_sorted` (and therefore `Store::bulk_load`, the
full-restore/incremental-delta fast path documented in task23) panics on a
`debug_assert` in debug builds, and **silently builds a structurally wrong tree in
release builds**, at these cascade-aligned sizes.

### Bug 1 — invalid `redistribute_tail` on a truly-empty internal level

At `n = (MAX_KEYS+1)^2` exactly, the second-from-top internal level ends the build
completely drained (0 entries, 0 children — its content had already frozen and
promoted further up). The old condition `has_parent_sibling && entries.len() < MIN_KEYS`
still fired `redistribute_tail` on it (`0 < MIN_KEYS` is true), but a **leaf** with 0
entries is safe to redistribute (no children invariant to violate) while a genuinely
empty **internal** level is not: the popped sibling's `entries + 1 separator` gets
re-split across zero children of its own, landing one child short of
`entries.len() + 1`.

Fix: a new `is_degenerate_empty_internal` guard
(`!is_leaf_level && entries.is_empty() && children.is_empty()`) excludes this case from
the redistribute condition. Leaves remain exempt from the guard — a fully-drained leaf
still *must* redistribute when it has a parent sibling (it's the parent's only way to
get a valid rightmost child), and a leaf's `children` is always empty regardless, so
the merge never touches children there.

### Bug 2 — an "unclosed" level with no carry to close it

Once Bug 1's guard prevented the doomed redistribute, the next problem surfaced: a
level whose reserved final child slot (`attach_child`'s `children.len() == entries.len()`
mid-build invariant) never got filled because input ran out mid-cascade before
anything arrived from below to close it — reachable both from `extend_from_sorted`'s
fuzz test (an intermediate level, seeded near-full, froze early in a batch and was
never refilled) and from a plain `from_sorted` at exact cascade boundaries
(`(MAX_KEYS+1)^2`, `2*(MAX_KEYS+1)^2`). This tripped the
`debug_assert_eq!(children.len(), entries.len() + 1)` internal-arity check.

The level's dangling last entry is real data (the single largest key promoted so far)
with no matching right-hand child. Fix: pop it into a `pending_reinsert: Vec<(K, Arc<V>)>`
and re-insert each one via the ordinary, already-tested `insert_arc_mut` (task48) after
the tree is otherwise built, adjusting `len` so it isn't double-counted. If popping
empties the **topmost** level down to 0 entries / 1 child, that level collapses to its
child directly — mirroring the existing root-collapse pattern in `remove`/`remove_mut`
— restricted to the topmost level only, because collapsing an intermediate level would
desync that branch's leaf depth from the rest of the tree.

Separately, also added `fix_right_spine_tail`: a post-pass that walks the finished
tree's right spine and repairs any node still underfull after the per-level tail
rebalance, using the same `fix_underfull_child`/rotate/merge machinery `remove_mut`
(task50) already uses on real sibling pointers — needed because `redistribute_tail`
operates on builder-level bookkeeping that can lose track of a leaf's true sibling
once an intermediate level has already frozen and reset.

### Known accepted residual limitation (benign, documented, pinned)

A **nested** cascade (`m * (MAX_KEYS + 1)^3 + delta` for any multiple `m >= 1` and
`1 <= delta < MIN_KEYS`; ~2M entries at `T = 64` for the smallest `m = 1`) can still
leave one *intermediate* (non-root) node with 0 entries / 1 child after the
topmost-only collapse restriction above — structurally well-formed (no arity or depth
corruption; reads, writes, and ordering are unaffected) but failing the `MIN_KEYS`
floor `check_invariants` enforces. This is a **packing floor** limitation, not a
correctness bug: a delete touching that leaf self-heals it back above `MIN_KEYS` via
the existing rebalance machinery. Out of scope to fully close — it requires input on
the order of 2 million exactly-aligned entries, far beyond anything realistic
`insert_batch`/`bulk_load` usage reaches, or the fuzz coverage this task added.
Documented with doc-comment caveats on `BTree::from_sorted` and `Store::bulk_load`,
and pinned by a permanent regression test,
`from_sorted_nested_cascade_two_million_benign`, which builds the exact boundary size,
confirms full read/order correctness *without* calling `check_invariants` (the
documented deviation, not a bug), then deletes the top `2 * MAX_KEYS` keys and asserts
`check_invariants` now passes (the self-heal claim). The two *fixed* boundaries (one-
and two-level cascades) are separately pinned by `from_sorted_exact_cascade_boundary`.

**Final-review addendum:** a related but distinct debug-assert family — `n = m *
(MAX_KEYS + 1)^3 + j * (MAX_KEYS + 1)^2` for any multiple `m >= 1` and
`1 <= j < MIN_KEYS` (smallest failing case `128^3 + 128^2 = 2_113_536` at `T = 64`) —
was found during final review of this task and **fixed** (not merely documented): an
internal level could reach `finish()` "unclosed" (`children.len() == entries.len()`, a
dangling separator with no right child) *and* underfull with a parent sibling
present, tripping `redistribute_tail`'s own arity `debug_assert_eq!` because its split
math assumes a closed level. The fix pops the dangling separator into
`pending_reinsert` before redistributing (the same `finish()` machinery as Bug 2
above), verified to hold across the whole `j` range, and pinned by
`from_sorted_unclosed_level_debug_assert_family`. With this fix in place, the only
residual limitation described in this section is the benign, non-panicking packing
deviation above.

## 4. Correctness

Unit tests in `src/btree.rs`:

- `max_key_empty_and_single`, `max_key_multi_level` — the new `BTree::max_key()`
  accessor `Table::insert_batch` uses to guard the fast path.
- `check_invariants_accepts_existing_constructors`, `check_invariants_catches_bad_len`
  — a new structural invariant checker (arity bounds with root exempt from `MIN_KEYS`,
  uniform leaf depth, `children == entries + 1`, per-node and global key order, `len`
  consistency) used throughout the rest of the suite below.
- `extend_from_sorted_empty_tree_and_empty_batch`, `extend_from_sorted_tail_shapes` —
  equivalence (mapping + `len`, not node-identity — packing legitimately differs from
  per-key `insert_mut`) across empty tree/batch, underfull tail leaf, exactly-full
  root-leaf, one-past-full, and multi-level base trees, crossed with several batch
  sizes.
- `from_sorted_exact_cascade_boundary` — regression test for the two *fixed* §3
  boundaries (one- and two-level cascades: `(MAX_KEYS+1)^2` and `2*(MAX_KEYS+1)^2`,
  each ±1 and +`MIN_KEYS`).
- `from_sorted_nested_cascade_two_million_benign` — pins the §3 residual limitation
  (full read/order correctness at the boundary size without `check_invariants`
  pre-delete; `check_invariants` passes post-delete, proving self-heal).
- `extend_from_sorted_fuzz_alternating_batches_and_removes` — the test that found both
  §3 bugs: 60 rounds of random-sized appends via `extend_from_sorted`, interleaved
  with random removes via `remove_mut`, checked at every round against a parallel
  per-key `insert_mut`/`remove_mut` tree and `check_invariants`.
- `extend_from_sorted_preserves_snapshots` — a single constructed snapshot-then-append
  case (`MAX_KEYS^2` base, 10 000-entry append): the pre-append clone is bit-for-bit
  unaffected.
- `extend_from_sorted_fuzz_snapshot_per_round` — **this is the test that replaced the
  design spec's planned single-case "shared-sibling redistribute regression test"**
  (spec §Tests: "a case sized to force `redistribute_tail` into an original (shared)
  node"). As derived in §2, that specific case is unreachable by construction:
  `redistribute_tail` only ever pops a sibling that was frozen *during the current
  `finish()` call* (never an original seeded node), and even in principle it copies
  rather than mutates through the popped Arc. Rather than write a test for an
  impossible precondition, this test instead snapshots the tree before **every** append
  round (40 rounds, small batches sized to maximize tail-redistribute hits) and
  verifies each snapshot is unaffected afterward — a strictly stronger, exhaustive
  version of the same property the spec's test was trying to pin, without depending on
  hand-constructing a reachable failure shape.

`Table`-level tests in `src/table.rs`:

- `insert_batch_bulk_path_matches_per_key_semantics` — interleaves single inserts and
  batches of varying size across 20 rounds; ids stay sequential and every record is
  retrievable, exercising repeated re-seeding of a growing tree.
- `insert_batch_falls_back_when_next_id_behind_max_key` — corrupts `next_id` below the
  max key (unreachable in production; the guard exists exactly for this) and confirms
  the fallback path preserves legacy per-key replace-on-collision semantics.

Formal drift guard: `formal/kernel/src/lib.rs` gained
`BTree::extend_from_sorted(&mut self, items: &[(u64, u64)])`, modeled as a fold of the
verified `insert_mut` over the batch (the fold **is** the spec; the Lean proofs cover
each individual `insert`). A new differential test,
`extend_from_sorted_matches_insert_and_std_btreemap`, checks the kernel mirror, a
parallel per-key kernel tree, and `std::BTreeMap` agree over 50 rounds of random-sized
ascending appends. Because both `src/btree.rs` and `formal/` changed in this branch,
`formal/scripts/check-drift.sh` passes normally — **no `[skip-formal-drift]` marker
needed**.

## 5. What changed (files)

- `src/btree.rs` — `BTree::max_key()`; `BulkBuilder::seed_from_spine`;
  `BTree::extend_from_sorted`; the `finish()` fixes and `fix_right_spine_tail`
  post-pass from §3; the structural `check_invariants` test helper; all unit tests
  listed in §4.
- `src/table.rs` — `Table::insert_batch` phase 1 rewired to the guarded fast
  path/fallback described in §2; the two `Table`-level tests from §4.
- `src/store.rs` — doc caveat on `Store::bulk_load` for the §3 residual limitation.
- `formal/kernel/src/lib.rs` — `extend_from_sorted` mirror + differential test (§4).
- `formal/kernel/Cargo.toml` — declares an explicit empty `[workspace]` table, making
  the (already cargo-workspace-excluded) kernel crate its own workspace root
  explicitly. Needed only when developing from a worktree nested inside the main
  checkout (`.claude/worktrees/*`) — cargo's ancestor directory walk otherwise escapes
  past the worktree root and binds to the outer checkout's workspace, failing
  `cargo test --manifest-path formal/kernel/...` from inside the worktree. Inert on a
  normal (non-nested) checkout, where the kernel crate is already its own
  workspace-of-one either way.
- `benches/bulk_load_bench.rs` — unchanged; its existing `bulk_vs_insert_batch` group
  is the harness §6's numbers come from (no new bench file — the spec's originally
  planned `benches/btree_insert_mut_bench.rs` batch arm was dropped as YAGNI:
  `extend_from_sorted` is `pub(crate)` so a standalone bench can't call it directly,
  and this harness already measures the exact end-to-end gap).
- `docs/tasks/task51_insert_batch_bulk_append.md` — this document.
- `CLAUDE.md` — one line noting the fast path in the `Table<R>` bullet.

## 6. Bench (sandbox smoke — non-authoritative)

`cargo bench --bench bulk_load_bench -- --quick` (`bulk_vs_insert_batch` group,
dev sandbox, criterion quick mode):

| n | insert_batch | bulk_load_sorted | ratio (insert_batch / bulk_load_sorted) |
|---|---:|---:|---:|
| 100,000 | 8.20 ms | 12.55 ms | **0.65×** (insert_batch faster) |
| 1,000,000 | 134.7 ms | 129.3 ms | **1.04×** |

Both points land at or well inside the ~1.2× target this task set out to reach (the
gap was ~2× before this task, per the design spec's motivation). Per project bench
methodology, **this is a single-run sandbox smoke test — magnitude-only, not a
publishable A/B** (same-machine noise floor on the Claude sandbox runs ±2×; these two
numbers are not treated as "insert_batch is now proven Nx faster/slower"). The
authoritative, repeatable version of this comparison should be re-recorded on the
`bench-infra` NVMe host through the existing `task49_bulk_load_bench.md` harness
(`make bench/bulk-load/compare`) — not run as part of this task.

## 7. Notes / limitations

- The fast path's correctness depends entirely on the auto-increment invariant
  (`next_id` bookkeeping never lets an existing key reach `>= next_id`). The
  `max_key()` guard makes this load-bearing rather than merely assumed: if it is ever
  violated, `insert_batch` degrades to the always-correct per-key loop instead of
  producing a corrupt tree.
  - Ordering constant deferred: `Vec` allocation for the `ids` result
    (`(start_id..start_id + n).collect()`) happens on both paths identically, so the
    fast/fallback choice affects only the data-tree build, not `ids`/index-maintenance
    behavior.
- `update_batch`/`delete_batch` are unchanged — they mutate existing (non-append) keys,
  which is exactly the case `extend_from_sorted` cannot help with; only `insert_batch`
  qualifies.
- See §3 for the accepted residual packing-floor limitation at ~2M-entry nested
  cascades — pre-existing in `from_sorted`/`bulk_load`, unrelated to `insert_batch`'s
  correctness, and out of scope to fully close here.
