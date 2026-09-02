# Task 64: Paged leaf value blocks + memory-honest budget

Branch: `feat/paged-value-blocks`, off `main` @ `485aac2`. Binding spec:
`docs/superpowers/specs/2026-08-31-paged-leaf-arena-memory-honesty-design.md`
(the "stage-3 memory-honesty slice" of the paged B-tree work — see
`docs/tasks/task63_paged_btree.md` §7 for the write-path spike this
consolidates). Design-history record: the plan
(`docs/superpowers/plans/2026-08-31-paged-leaf-value-blocks.md`) and the
SDD ledger (`.superpowers/sdd/2026-08-31-paged-leaf-value-blocks/`, 11
implementer tasks + this one) is kept locally only — `.superpowers/` is
gitignored — so this doc and the plan are the committed record per the
Feature Development Workflow.

## 1. Problem this closes

The fs-paged comparison matrix (task63) showed paged UltimaDB 15-25x
behind ReDB on pressured write workloads. The write-path spike
decomposed the gap into three structural findings, all fixed here rather
than tuned per-host:

- **F6 (local-dominant): heap fragmentation.** Every value was its own
  heap `Arc<V>` referenced from a leaf entry
  (`Entries<K,V> = FixedVec<(K, Arc<V>)>`). Insert-era CoW churn smeared
  surviving leaves+values at low density across the whole load-era heap;
  sub-page holes defeated `malloc_trim`.
- **Pins (NVMe-dominant): retained snapshots were invisible to the
  budget.** Older retained snapshots CoW-share pre-demotion leaves; the
  resident counter debited at demote while the bytes stayed live. On the
  NVMe bench host this was a 56x throughput lever (237 vs 13,343 ops/s,
  `num_snapshots_retained` 10 vs 1).
  **CORRECTED 2026-09-02 (§13.4): that 56x compared workload A at
  retention 10 against workload C at retention 1 — the lever script ran
  read-only C. On C, retention 10 vs 1 was 13,664 vs 13,343. No valid
  measurement on any host shows retention as a write-workload lever;
  the mechanism stands (Task 9, §6), the magnitude claim does not.**
- **F2: second-chance demotion couldn't reclaim a continuously-touched
  set** (0-41 of ~80k leaves evicted under forced checkpoints), and
  `NODE_BYTES = size_of::<BTreeNode>()` excluded value bytes entirely —
  the budget was neither honest nor enforceable.

## 2. Representation: `Value<V>` and block leaves

```rust
/// Some = shared heap value (inner nodes, non-paged trees — byte-
/// identical to today via the NonNull niche, 8B). None = "in-block": the
/// value lives at this entry's own position in the node's value block.
pub(crate) struct Value<V>(Option<Arc<V>>);

pub(crate) type Entries<K, V> = FixedVec<(K, Value<V>), { MAX_KEYS + 1 }>;

pub(crate) struct BTreeNode<K, V> {
    entries: Entries<K, V>,
    children: Children<K, V>,
    /// Some only on paged data-tree leaves ("block leaves"). +8B/node.
    block: Option<Box<[V]>>,
}
```

- **Invariant I-A (all-or-nothing):** a node's entries are all-Arc or
  all-in-block, never mixed — enforced by construction (the only
  block-leaf constructors are `NodeCodec::decode` and leaf CoW rebuilds)
  and by a debug assert on every block-leaf build, plus a store-level
  representation assert (added in Task 6, once the `_mut` mutation path
  itself started keeping blocks — the Task 5 assert alone lived at the
  `BTree` level because the `_mut` family still materialized at that
  point).
- **Invariant I-B:** block leaves exist only in trees whose `clone_fn`
  is `Some` — i.e. only paged-attached data trees. Inner nodes and every
  index tree stay all-Arc (always resident, ~1.6% of entries at T=32;
  they neither demote nor fragment against the budget).
- Reads (`get -> Option<&R>`) are unchanged: zero-copy, one branch per
  leaf (not per entry), through the Arc or into the block.
- **Wire format unchanged in both directions.** Encode serializes
  identically from either representation; decode chooses the
  representation by whether the tree is a paged data tree — no format
  version, no migration, task62 compat rules hold trivially. Proven by
  fixture, not just argued: `tests/fixtures/paged_prechange/` was
  written by the pre-block-decode code (commit `9331ea5`, before
  `1c7663e` turned decode into blocks) and is a committed regression
  fixture — `tests/paged_format_compat.rs` opens it with the current
  code and verifies content, and a golden-bytes assertion pins that
  encode output is unchanged.

## 3. `clone_value` / `register_table_paged` plumbing

The tree gained `clone_fn: Option<fn(&V) -> V>`, set to `V::clone` only
at paged attach where `R: Clone` is statically known (the same
erased-fn pattern index extractors already use) — no `V: Clone` bound on
any `BTree` method; non-paged trees never observe the field.
`NodeSource::clone_value` (Task 2, `src/child.rs`) is the boundary that
calls it: `make_mut`'s block-leaf CoW path clones a shared block leaf
through the source rather than through `Arc::make_mut`, which is what
lets a plain `Clone` (no `unsafe`, no aliasing) stand in for the old
refcount-bump-only CoW.

**Public surface (additive):** `Store::register_table_paged::<R: Record
+ Clone>(name)` (and `register_table_paged_keyed` for non-`u64` keys) —
the required registration for tables of a paged store; it degrades to
plain registration on a non-paged store. Plain `register_table` on a
paged store returns `Error::PagedNeedsClone { table }` naming the fix.
Implemented as a shared `register_table_impl::<R, K>(name, clone: Option<fn(&R) -> R>)`
body (`src/store.rs`) gated by an up-front `PagedNeedsClone` check under
the same write lock as the rest of registration — paged-ness is fixed at
`Store::new`, so there is no race to protect against. This was judged
free of real-world breakage: paged mode was unreleased at the time
(0.3.0 predates it).

Task 3's migration sweep touched every paged test call site
(`tests/paged_*.rs`, `compare_benches/src/bin/paging_matrix.rs`) plus
two extra sites the brief didn't enumerate, found empirically.

## 4. The one-pass mutation discipline

**Mutations build the new block in one allocation pass — never
clone-then-mutate:** update = rebuild with slot `i` replaced (the
incoming value moves in; the displaced value drops, never clones);
insert = a len+1 rebuild; split = one fresh block per half
(`rebuild_block_leaf`, `split_block`/`split_block_mut`, `src/btree.rs`,
via the `LeafEdit<K, V>` enum). Values arriving as `Arc<R>`
(`upsert_arc` commit-merge, overlay flush) clone out of the Arc at the
boundary, not before it.

**Cost, stated honestly (spec §4):** leaf CoW goes from 63 refcount
bumps (T=32) to a block rebuild — memcpy-scale for flat records,
compounding the standing `fanout-t8` recommendation for write-heavy
paged deployments; records with heap fields pay a real per-element
clone.

**Test approach — the clone-count instrument.** A clone-then-mutate
rewrite of any block-leaf mutation path would still produce correct
values and correct representation; only a per-call counter would
notice. `MockDisk::cloned` (an `AtomicU64` bumped inside the test
double's `clone_value`) is that instrument: `immutable_replace_keeps_the_leaf_block_backed`
asserts a 40-entry leaf's `insert` over an existing key clones exactly
39 values (n-1, not n); `immutable_insert_...` asserts exactly n on a
new-key insert; `immutable_delete_...` asserts n-1 on a delete;
`immutable_insert_splits_a_block_leaf_into_two_block_leaves` asserts
`split_block` itself adds **zero** clones beyond the preceding rebuild's
n carries (the split drains one already-built block into two halves by
move). This is a **pinned property**, standing in `src/btree.rs`'s
`block_leaves` test module (also the Miri-gated module, see §9) — not a
one-off measurement. Task 5's implementer found the property untested
by the original scope (review Important-1) and added it as a
first-class regression guard; Task 7's review caught a related but
distinct doc overclaim (delete's *removed-value* cost: `Table::delete`
cannot move a value out of a CoW-shared block, so it clones it into the
returned `Arc<R>` — one clone per delete on paged tables, on top of the
n-1 block-rebuild clones; corrected from an initial "exactly one clone"
claim to the accurate two-branch cost model, pinned with `CountingRow`).

## 5. The accounting unit: `leaf_bytes`, everywhere

Before this task, `dirty_bytes`/`resident_leaf_bytes` credited the flat
`NODE_BYTES = size_of::<BTreeNode>()` for every leaf, block or not — an
undercount by construction once leaves carry a value block. **`leaf_bytes()`**
(`src/child.rs`) is now the single accounting primitive used everywhere
a leaf's memory cost is estimated:

```rust
let bytes = if node.block.is_some() { node.leaf_bytes() } else { Self::NODE_BYTES };
```

- **Credit** (fault-in, `NodeCodec::decode`, `src/pagecodec.rs`): builds
  the block directly (one `Box<[V]>`, not n `Arc::new` calls) and
  credits `NODE_BYTES + n * size_of::<V>()`.
- **Debit** (demotion): computed from the dropped leaf itself, so it is
  symmetric with the credit by construction — Task 8 replaced the old
  flat-`NODE_BYTES` debit with the same `leaf_bytes()` call.
- **Reconcile** (`Store::checkpoint_impl_paged`'s F1 walk,
  `resident_leaf_estimate`): upgraded to count block bytes rather than
  flat node bytes.
- **`dirty_bytes`** (write-path CoW credit): also upgraded to
  `NODE_BYTES + block bytes` — this was flagged mid-plan (Task 5 review
  ⚠️1) as an orphaned spec §4 requirement no task explicitly owned, and
  folded into Task 8's scope rather than left to drift.

**Documented limit:** heap bytes *inside* `V` are not counted — exact
for the flat small-row target this spec optimizes for, an undercount for
heap-carrying records (spec §5, out of scope; unchanged from before this
task).

## 6. Pin-aware reconciliation + `pinned_leaf_bytes`

The checkpoint-end reconcile now walks the data trees of **every**
retained snapshot, newest-first, deduping by leaf pointer identity
(`Child::resident_ptr()` — a pure identity peek, never faults, never
marks the accessed bit — feeding `BTree::resident_leaf_bytes_dedup` /
`MergeableTable::paged_resident_leaf_bytes_dedup`, the same `&dyn Any`
type-erasure pattern `merge_keys_from` already uses for a
key-type-independent `HashSet<*const ()>`). The latest snapshot's own
deduped total is `resident_leaf_bytes`; the dedup total across every
retained snapshot minus `resident` is the new stat
**`pinned_leaf_bytes`** — resident bytes reachable *only* from a
non-latest snapshot, i.e. un-evictable by `demote_pass` as it exists
today (it only ever demotes the latest snapshot's tables). CoW sharing
keeps the multi-root walk cheap in practice (mostly retracing identical
pointers).

**Empirical pin steady-states (Task 9's investigation, load-bearing for
Task 11's design):** the brief's suggested test scenario — "commit a few
more times so the old pins age out" — **does not work**, and this was
proven empirically (deterministic debug traces), not assumed:

- Any write that touches an on-disk (already-demoted) leaf shared with
  its own base snapshot faults that leaf in for *both* snapshots
  momentarily (they share the same `Child`), then the write's own CoW
  splits them apart — permanently orphaning a resident copy in whichever
  snapshot the write was built from. Not specific to repeated-key
  writes: this happens for *any* write to *any* previously-demoted leaf
  whose immediate predecessor snapshot is still retained.
- With `num_snapshots_retained(N)`, this is a **steady state**, not a
  transient: every further write creates one new pin at the same moment
  the oldest one ages out of the window. `pinned_leaf_bytes` holds at
  `(N-1) * avg_leaf_bytes` — a *different* set of leaves each round —
  for as long as writes continue. `Store::gc()` does not help either:
  with exactly `N` snapshots present, `gc_inner`'s fast path has nothing
  legitimately outside the retention window to evict.
- **Spread-key oscillation above the floor:** the reviewer's empirical
  correction (Task 9 review, carried into Task 11) found pinned bytes
  are **not** bounded by a clean `(N-1)` floor under spread-key (as
  opposed to same-key) write patterns — they oscillate at roughly
  4-5x that unit. Task 11's enforcement trigger was written to use
  **measured** `pinned_leaf_bytes`, never a retention-derived formula,
  specifically because of this.
- What *does* reach `0`: retention no longer keeping a diverged snapshot
  alive at all (proven with a separate `num_snapshots_retained(1)` store
  plus an explicit post-write `gc()`).

This reframes the pin phenomenon from "an adversarial corner case" to
"the write-path steady state under any retention > 1" — directly
informing why an enforcement arm (§8) exists at all: ordinary write
traffic cannot self-resolve it.

## 7. Hard-cap clock eviction and `MAX_DEMOTE_CYCLES = 8`

Same trigger and placement as the pre-task64 second-chance pass (inside
the checkpointer tick, after `write_dirty`, `MIN_INTER_RUN` = 200ms), but
within one invocation the pass now **cycles**: sweep (clear accessed
bits, evict leaves already clear), re-check the counter, sweep again —
leaves cleared on the previous cycle evict unless re-touched during the
pass. Deterministic order of weapons in the tick: (1) clock cycles over
evictable leaves; (2) if still over budget and pins are the excess, §8's
retention shrink, then `gc()`, then the next reconcile settles.

**The hard cap and why it exists.** The original termination argument
("each cycle either reclaims bytes or, within two cycles, proves the
remainder un-evictable") is sound *single-threaded* but silently assumed
nothing re-credits resident bytes mid-pass. Concurrent readers do:
every data-leaf fault-in credits `resident_leaf_bytes`, and the pass
holds no lock excluding that. Task 10's review **reproduced an
unbounded loop under concurrency: 62,000 cycles in 6 seconds**, only
terminating when the workload itself stopped — driven by two clauses
that can both stay false forever: an un-evictable `Residency::Resident`
floor already above budget (the pass skips that table wholesale, so
nothing ever debits it), and concurrent fault-ins re-crediting faster
than one thread's cycles can evict. `Store::MAX_DEMOTE_CYCLES: u64 = 8`
(`src/store.rs`) is the fix: a hard clock-cycle cap per pass invocation
(convergence in the single-threaded case needs 2; the extra headroom is
for multi-table bit-clearing staggering), observable via the
`clock_cycles` stat, with a documented guarantee — a demote pass exits
within 8 cycles **always**, no exceptions — traded against
earliest-possible-eviction slack under the pathological concurrent case.

## 7a. Decide/apply demote batches (2026-09-02 follow-up to §14.1)

The pre-fix batch read `latest_version`, walked `paged_demote` off-lock,
and installed the demoted clone into *that* version. §14.1's trace showed
that under a per-op commit stream every batch loses: a swap-bound walk
of 0.3-4.5 s sees hundreds to thousands of commits, so the install
targets a version that is gc'd (`None`, nothing counted) or stale
(counted but unreachable from any reader — silent counter drift).

Now a batch is two halves (`src/btree.rs` `plan_demote`/`apply_demote`,
`src/table.rs` `paged_demote_plan`/`paged_demote_apply`, `src/store.rs`
`demote_pass_inner` + `demote_apply_chunk`):

- **Decide (off-lock):** walk at most `demote_batch` leaf-parents after
  the cursor touching only slot atomics — `is_loaded`, `page_id`,
  `take_accessed` — no `make_mut`, no leaf deref. Output: key-ordered
  `DemoteChunk { from, to, ids: HashSet<PageId> }` of at most
  `Store::DEMOTE_APPLY_CHUNK` (64) parents each, plus the next cursor.
- **Apply (under `inner.write()`, one chunk per acquisition):** re-read
  the *current* latest, evict every loaded leaf whose page id is in the
  chunk under a parent whose max key lies in `(from, to]`, and re-publish
  at that version. The descent is pruned to the chunk's key range by the
  inner nodes' separators; only a parent with at least one hit is
  `make_mut`'d. The superseded table/snapshot `Arc`s leave the lock
  scope before they drop, so leaf frees (which touch possibly swapped
  memory) never stall a commit. Bytes are still `leaf_bytes()` of the
  leaf actually dropped (credit/debit symmetry, §5).

A plan made against version *v* applying to a later version is sound
because it is only a set of page ids in a key range: a leaf split,
merged, rewritten (new id), re-faulted dirty, or already demoted since
is simply not matched. `install_paged_tables` is untouched and still
serves the checkpoint. The single-shot `demote_leaves`/`paged_demote`
became plan-then-apply on one version and is the tree/table-level
regression surface (the table trait method is now `cfg(test)`).

Regression test: `store::tests::demote_pass_lands_on_current_latest_when_captured_version_is_evicted_mid_batch`
(the former `..._dropped_install_does_not_bump_stats`, whose assertions
pinned the lose-the-race behaviour as correct and were inverted): the
race hook commits, checkpoints and gc's the planned version between
decide and apply; the pass must still demote, count exactly what
landed, and the live latest's dedup walk must shrink. Watched fail
before the change (`demoted=0`), passes after.

**Post-fix sanity cell** (same host, config and 60 s window as §13.3's
A/zipf retention-1 cell; n=1, local, ordering only):

| metric | pre-fix | post-fix |
|---|---|---|
| `resident_leaf_bytes_end` (budget 64 MiB) | 174.8 MB | **58.9 MB** |
| `leaves_demoted` during the run | 0 | **24,742** |
| `checkpointer_runs` during the run | 2 | 4 |
| run1 ops/s (timed out both) | 1,263 | 895 |
| run2 ops/s after restart | 2,195 | 5,371 |
| pf/op (DB) | 0.374 | 0.442 |

The budget is honoured at run end for the first time in a pressured
write cell. Run1 throughput is lower — evicting under pressure costs
re-faults the old pass never paid because it never evicted — and that
is inside the local noise band besides; the run2 gain is not a claim
either. §14.1(c) is untouched by this: the ~41 MB `dirty_bytes_end`
here is the un-demotable dirty set, and it is only the four checkpoints
in the window (vs two) that kept the total under budget.

## 7b. Dirty trigger scaled to the budget (2026-09-02, §14.1(c))

`PagedOptionsBuilder::build` now resolves `checkpoint_dirty_bytes` to
`min(budget / 2, 256 MiB)` (floor 1) when `memory_budget_bytes` is set
and the trigger was not set explicitly (`src/persistence.rs`; the
builder records `dirty_explicit`). An explicit value always wins, even
above the budget, in either call order; a hand-edited
`PagedOptions::default()` is not scaled. Why half: the dirty set is
un-demotable until a checkpoint writes it (§14.1(c)), so with the old
flat 256 MiB trigger a 64 MiB budget could sit under a 41 MB dirty set
that no demote pass can touch — the cap was only ever enforceable over
the clean remainder. Bounding dirty at half the budget leaves the other
half for the demote pass to enforce. Cost: more frequent checkpoints
under a small budget with a write-heavy workload — the memory-wins
principle already ruled for the budget (spec §9.5), and a checkpoint
only writes the pages that are dirty.

Tests: `persistence::tests::dirty_trigger_scales_to_half_the_budget_unless_set_explicitly`
(resolution rules) and
`tests/paged_checkpointer.rs::memory_budget_scales_the_dirty_trigger_when_unset`
(6 MiB budget, interval disabled, ~4.1 MB dirtied while resident stays
under budget — only the scaled trigger can fire; watched fail unscaled
with dirty 4,118,048 / resident 4,063,552 and no checkpoint).

Same-host sanity cell as §7a's (A/zipf, 60 s, 198 MiB cgroup, 64 MiB
budget → 32 MiB trigger, retention 1; n=1, ordering only):

| metric | §7a (flat 256 MiB trigger) | §7b (32 MiB trigger) |
|---|---|---|
| `dirty_bytes_end` | 41.2 MB | **32.7 MB** |
| `resident_leaf_bytes_end` (budget 64 MiB) | 58.9 MB | **52.2 MB** |
| `checkpointer_runs` during the run | 4 | 5 |
| `leaves_demoted` during the run | 24,742 | 23,736 |
| run1 / run2 ops/s | 895 / 5,371 | 856 / 5,624 |

The dirty set now ends just under its trigger, one more checkpoint fits
the window, and throughput is unchanged inside noise.

## 7c. Pre-pass reconcile and a residency assertion in the gate (2026-09-02, §14.1(c) remainder)

Two follow-ups to the same diagnosis, both landed 2026-09-02.

**The first checkpoint's demote pass no longer trusts a never-credited
counter.** `Child::resident_new` credits only `dirty_bytes`; a leaf
*built* in memory never credits `resident_leaf_bytes` (only fault-ins
do), and the exact-walk re-base ran only *after* the demote pass. So on
a store's very first checkpoint the pass's exit check read 0 whatever
the tree held: if cycle 1 was a pure second-chance sweep (every leaf
accessed since it was built — a put-loop load, or any scan), it demoted
nothing, saw `0 <= budget`, and quit with the tree fully resident until
the *next* checkpoint. `checkpoint_impl_paged` phase 3 now calls
`reconcile_paged_stats` before `demote_pass` as well as after (line-neutral
edit, no cite re-anchoring), so the first pass cycles like every later
one. The bounded remainder of the pre-first-checkpoint gap is the
checkpointer's `due_mem` trigger, which reads the same counter: before
the first checkpoint it is structurally silent, and §7b's scaled dirty
trigger is what fires that checkpoint (every built leaf is dirty).

Test: `tests/paged_demotion.rs::first_checkpoint_demote_pass_cycles_past_a_never_credited_counter`
(64 KiB budget, 20,000 rows, a full point-read scan to arm every accessed
bit, dirty trigger explicit and huge so the background checkpointer
cannot take the first checkpoint; watched fail with the reconcile
reverted: `clock_cycles=1`, then passes with `clock_cycles >= 2`,
`resident_leaf_bytes_est <= budget`, `forced_evictions > 0`).

**`make paging/check` now asserts residency.** §14.1 called the gate
blind: it asserted DB faults/op, OOM count and recover time, never
resident bytes against `paged_budget_bytes`, so the zero-demotion
budget miss passed every assertion. `paging_check.py` adds, per cell:
`checkpointer_runs >= 1` (before the first checkpoint the counter reads
0 and any ratio would pass vacuously), `leaves_demoted > 0` (the direct
witness for passes that never land), and a **trough** assertion: the
harness now samples `resident_leaf_bytes_est` and the checkpointer run
count into the 5 s trajectory (`paging_matrix.rs::Trajectory`, paged
engine only), and the minimum over samples taken after the first
checkpointer run of the run phase must be `<= 1.25 x budget`. The
series max and mean are printed as disclosure, not gated.

Why the trough and not the end value: the end value was tried first and
is a phase sample of a saw-tooth. Two runs of the same C/uniform cell
on trees that differ only by the pre-pass reconcile ended at **1.44x**
and **4.20x** budget (96.5 MB vs 281.9 MB) with every other metric
within noise — 416k vs 438k ops, pf/op 0.62 vs 0.64, swap-ins 1.48M
vs 1.51M, 5 checkpointer runs each, 244k vs 235k leaves demoted. The
282 MB reading exceeds the process RSS (233 MB): the tree really held
that many leaf bytes, with the 256 MiB cgroup pushing them to swap —
exactly the failure the budget exists to prevent, and exactly why the
overshoot is disclosed rather than hidden behind a loose bound. What
the two runs agree on is the mechanism: 5 ticks in 60 s is a ~12 s
period against a uniform read stream faulting in ~18 MB/s, and the
tick is long because (§14 item 5, unverified) `apply_demote` derefs
every evicted leaf under the write lock. The trough witnesses that
passes land and pull the tree under budget; the max/mean measure how
far the workload runs past it while a tick is in flight. Watched fail:
the diagnosis-era A cell (ratio 3.28, `leaves_demoted=0`) fails
`leaves_demoted > 0`; a report without counter samples fails the trough
assertion outright (not SKIP — the checker's reason to exist is not
passing vacuously).

## 7d. `Store::drop` waits for an in-flight background checkpoint (2026-09-02, corruption fix)

**Symptom.** The first `make paging/check` run after §7c's gate assertions
panicked in the A/zipf cell's *restart* phase, reading a data leaf right
after `recover()`:

```
cannot load page 639974952 of rows: checkpoint corrupted: page 639974952: unknown page kind 0
```

Other runs gave kinds 169, 68 and 8 at nearby offsets. Reproduction (A
cell, 5M rows, 64 MiB budget, 256 MiB cgroup, `--restart`): **4 of 4**
runs on the decide/apply trees (ab76e46 and the working tree), **0 of 2**
on 2bd84b9, the commit before demote passes started landing under a
commit stream.

**Diagnosis.** An env-gated trace (temporary, reverted) at root write,
recovery and `PageFile::set_cursor` showed every root self-consistent —
the recovered root named `file_end = 612,546,280`, its largest referenced
id was `612,542,748`, none past the end, cursor re-set to `file_end` — and
then run 2 faulted an id *27 MB past that*, an offset only the recovered
store's own checkpoints could have written, and found zeros. Two writers
on one page file. The library's own `Checkpointer` doc describes how:
the worker thread upgrades its `Weak` to a strong
`Arc<RwLock<StoreInner>>` for the duration of every tick, so when the
application drops its last `Store` mid-tick, the *worker's* later drop
of that `Arc` runs `StoreInner`'s destructor on the worker itself, and
`Checkpointer::drop`'s self-join guard has no choice but to detach.
`Store::drop` had already returned; the harness's `restart()` (and any
in-process reopen) then opened a second `Store` on the same directory
while the first was still appending pages, writing a root and punching
dead ranges. The guard was written as a deadlock avoidance; it was also a
correctness hole, reachable whenever a tick is in flight at drop time —
which the decide/apply split made routine (5 ticks per 60 s cell, each
swap-bound for seconds).

**Fix.** `CheckpointGate` (`src/store.rs`): the worker holds a strong
reference only under the gate lock or with `in_flight` set (set and
cleared under the lock, cleared only after the reference is gone, via an
`InFlightGuard` that covers every exit path); the per-tick transient
`Store` *moves* the upgraded `Arc` so the worker's count is exactly one.
`impl Drop for Store` takes the lock, reads `Arc::strong_count - in_flight`,
and if that says it is the last application handle it sets `stop` and
waits for `in_flight` to clear before letting its own `Arc` go — so the
destructor always runs on an application thread, where the join is a
real join. The worker's transient handle skips this by thread id. A
`Store` clone that is not the last handle pays one uncontended mutex
acquisition on drop and nothing else. Handles held by a `VersionPin` or
an open transaction count as application handles (no wait); that leaves
today's behaviour for those — the destructor runs wherever the last of
them drops — but a `Store` drop itself can no longer return while a
checkpoint it started is still writing.

**Evidence.** `tests/paged_checkpointer.rs::dropping_the_last_handle_waits_for_an_in_flight_background_checkpoint`:
1M rows, attach, dirty every leaf in one commit, wait until
`pages_written` shows the tick mid-write (file length is no witness —
`pages.bin` is preallocated in large chunks), drop, assert the second
root already exists when `drop` returns and the root set is unchanged
500 ms later, reopen and verify rows. Watched fail before the fix:
`left: 1, right: 2` — drop returned with the checkpoint still running.
Fixed-tree A-cell reproduction sweep and the full gate: see the table
below (§7d-evidence, filled from the run).

| run | tree | A/zipf restart | note |
|---|---|---|---|
| 1-2 | working tree (gate + reconcile, no drop fix) | **panic** ×2 | kinds 68, 8 |
| 1-2 | ab76e46 | **panic** ×2 | kinds 169, 0 |
| 1-2 | 2bd84b9 (before decide/apply) | clean ×2 | demotes never landed, ticks rarely in flight at drop |
| 1-3 | working tree + `CheckpointGate` | **clean ×3** | |

`make paging/check` on the fixed tree (256 MiB cgroup, 64 MiB budget,
60 s, n=1): all 12 assertions pass. Per-cell series of the sampled
resident counter over the budget, after the first checkpoint of the run
phase:

| cell | `checkpointer_runs` | `leaves_demoted` | trough | max | mean | `recover_secs` |
|---|---|---|---|---|---|---|
| C/uniform (read-only) | 5 | 246,036 | **0.01** | 5.09 | 2.13 | 0.14 (was 0.7-1.2 racing the detached checkpoint) |
| A/zipf | 6 | 31,562 | **0.69** | 1.11 | 1.01 | 0.15 |

A/zipf now sits on its budget (mean 1.01, max 1.11). C/uniform is a
saw-tooth between ~0 and 5x: each pass evicts essentially *everything*
quiet (trough 0.01, not "down to budget" — the cycle-level exit check
runs only after a whole-tree sweep), and the uniform read stream then
refaults ~18 MB/s until the next tick lands seconds later. Both halves
of that are §14 item 5.

## 8. Adaptive retention shrink (default ON) and the pin-while-latest orphan hazard

**Enforcement arm (spec §5, Peter's ruling: on by default).** When the
reconciled counter stays over budget and `pinned_leaf_bytes` is the
excess, the checkpointer directs `gc` to shrink retention below
`num_snapshots_retained`, down to a floor of the latest version plus
every explicit `VersionPin` (a user-held pin is always honored).
`PagedOptions::shrink_retention_under_pressure` (default `true`,
`#[non_exhaustive]` builder setter) is the opt-out. Rationale: setting a
memory budget is a declaration that memory wins over history depth —
the NVMe data misses that by 56x with the naive default. *(2026-09-02:
the 56x is withdrawn, §13.4 — the default-on ruling now rests on the
principle alone and is listed in §14 for re-ruling.)* Implemented
trigger deviates from the plan's literal decomposed wording
("`resident > budget AND pinned >= excess`", which essentially never
fires in the scenario the feature exists for) to a **total**
`(resident + pinned) > budget` trigger — ruled in because post-capped-pass
`resident` is usually already under budget while pins are the excess,
and spec §5's "reconciled counter over budget" reads naturally as the
honest total.

**Named, documented-not-fixed hazard (task60-F1 convention): pin-while-latest orphan.**
A pre-existing hazard since task63, made *reachable* by default-on
shrink with a tiny budget: `Store::pin_version` can be called while its
target version is still `latest_version`. A subsequent `checkpoint()`'s
per-table `install_paged_tables` unconditionally rebuilds a *new*
`Arc<Snapshot>` for that same version number — the live `VersionPin`
still holds the *old* `Arc`, now a second, divergent allocation from
`inner.snapshots`'s map entry. If retention shrink later collects the
map's entry (the pin is invisible to the reconcile — it only walks
`inner.snapshots`, not live `VersionPin` handles), `begin_read` for that
version starts failing even though the `VersionPin` is still alive and
still holding real memory. **Reproduced deterministically** and shipped
as an `#[ignore]`d regression test,
`paged_shrink_orphans_latest_version_pin`
(`tests/paged_accounting.rs`), per the task60-F1 convention: red there
means "the documented hazard still reproduces, not a new regression."
The full fix (a pin registry, or re-publish pin-awareness) is a **named
follow-up**, out of this task's scope — the review-ruled response was
honest documentation on three surfaces (`Store::pin_version`,
`VersionPin`, `PagedOptions::shrink_retention_under_pressure`) plus the
deterministic repro, not a design fix.

## 9. Budget honesty limits (soft counter, not real memory)

The hard cap governs the **soft counter**, not true residency — stated
explicitly rather than implied:

- **Heap-inside-`V` is uncounted** (§5, §6 above) — an inherent
  undercount for records with heap fields, by design (arena-of-arenas is
  out of scope, deferred to a future Approach-2 spec if NVMe data ever
  justifies it).
- **A stale `ReadTx` snapshot's leaves are invisible to both counters.**
  `install_paged_tables` replaces the `Arc<Snapshot>` at the *same*
  version on every demote-pass install; a `ReadTx` opened before that
  install keeps its old leaves alive, but the reconcile only ever walks
  `inner.snapshots` (the current map), never a caller's already-checked-out
  `Arc`. Task 10's review measured this directly (Probe C): a demote
  pass exiting "under budget" — 36 KB counted against a 64 KB budget —
  with **~813 KB of leaves genuinely resident** behind one stale reader.
  Pre-existing since Task 4/9's accounting design, inherited rather than
  introduced by Task 10; recorded here as an explicit, permanent
  limitation of the counter's contract, not a bug to fix in this task.

## 10. API surface (additive, spec §7)

- `Store::register_table_paged` / `register_table_paged_keyed`.
- `Error::PagedNeedsClone { table: String }`.
- `PagedOptions::shrink_retention_under_pressure(bool)` builder setter
  (default `true`).
- `PagedStatsSnapshot::pinned_leaf_bytes: u64` (struct now
  `#[non_exhaustive]`); `PagedStats::pinned_leaf_bytes: AtomicU64`
  (`src/pagecodec.rs`) — a plain unsigned exact-walk total (never a
  decrement-based running estimate, so it can never go negative, unlike
  the signed `resident_leaf_bytes` estimate).
- New metrics gauges: `ultima.paged.pinned_leaf_bytes`, plus the Task 10
  clock-cycle/forced-eviction counters.
- `get -> Option<&R>` unchanged, zero-copy. `delete -> Result<Arc<R>>`
  clones the removed value out of a CoW-shared block (one clone per
  delete on paged tables — corrected cost model, §4). `upsert_arc` /
  overlay flush clone out of the incoming `Arc` into the new block.
  **Bulk ingest pays zero clones**: `bulk_load` / `from_sorted` / the
  task51 `BulkBuilder` own their values and build blocks by move.
  Persisted secondary indexes stay unaffected — never block-backed.

## 11. Testing and verification

- **Equivalence oracle** (proptest, `tests/paged_block_leaves.rs`):
  identical op sequences against a paged block-leaf table and an
  in-memory Arc table, contents compared after every batch and across
  checkpoint->recover cycles.
- **Representation invariants** I-A/I-B: debug asserts plus a
  `walk_representation` tree-walk test helper after mixed workloads
  (`src/btree.rs`'s `block_leaves` test module).
- **Accounting oracle** (`src/store.rs`,
  `store::tests::pin_aware_reconcile::oracle`, a same-crate unit test —
  needed direct access to `resident_leaf_bytes_dedup` /
  `paged_resident_leaf_bytes_dedup` an integration test can't reach):
  drives 1-25 random insert/update/checkpoint/gc ops, then independently
  re-walks every retained snapshot **oldest-first** with a fresh dedup
  set (production walks newest-first) and asserts `resident + pinned ==`
  that independently-computed total — proving the deduped total is
  order-independent, not an artifact of one walk direction. Made
  mutant-proof in fix round 1 after the original oracle proved
  tautological (a mutation disabling dedup entirely still passed the
  whole suite) — a point test,
  `pinned_leaf_bytes_is_zero_for_a_fully_shared_snapshot`, closed the
  gap.
- **Hard cap:** deterministic over-budget continuously-touched
  workloads converge under budget within the 8-cycle cap; a bounded
  "runaway shape" test pins the cap itself under concurrent fault-in
  pressure (Task 10 fix round 1).
- **Boundaries:** block-leaf `delete` correctness (pinned with
  `CountingRow`), MultiWriter disjoint-key merge on block leaves + the
  task59 43-cell race matrix re-run clean, bulk-load move path.
- **Format compat:** `tests/fixtures/paged_prechange/` (written by
  pre-change code) opens and verifies under post-change code;
  golden-bytes assertion pins encode output unchanged.
- **No new `unsafe`** — `Value<V>` is a newtype over `Option<Arc<V>>`,
  the block is `Box<[V]>` — rides the existing Miri Stacked-Borrows and
  Tree-Borrows gates; block-leaf tests joined the Miri-run set
  (`cargo +nightly miri test -p ultima-db --lib btree::tests::block_leaves child::`,
  both models) with `cfg(miri)` case caps on the proptests.

## 12. Gate sweep (this task, 2026-09-01)

All green, in order, each before the next:

1. `cargo test --features persistence` (full workspace, incl. doctests):
   34 `test result:` blocks, all `ok`, 0 `FAILED`, 0 `error[`, 1141 lib
   tests passed total (plus integration + doctests), exit 0.
2. `cargo clippy --all-targets --features persistence -- -D warnings` and
   with `--features persistence,metrics` — both clean, exit 0.
3. Miri, `cargo +nightly miri test -p ultima-db --lib -- btree::tests::block_leaves child::`,
   both Stacked Borrows and Tree Borrows (`MIRIFLAGS=-Zmiri-tree-borrows`):
   41/41 passed, exit 0, both models. (Both runs reported an identical
   "finished in 148.32s" — Miri deterministic-timing artifact, not a
   fabricated rerun; judged by test lists + exit codes per the isolation
   note in `docs/tasks/`-adjacent memory, not by wall time.)
4. `make formal/cite-check`: re-anchored the 19 stale anchors this
   branch accumulated (16 pre-existing from an earlier `store.rs`
   insertion + 3 from Task 9's `PagedStatsSnapshot` growth) — see
   commit `cf4fc1a`. Re-anchored by reading, not by uniform offset: two
   contiguous regions (the `recover()` body and the
   `commit_single_writer`/`commit_multi_writer` bodies) turned out to be
   byte-identical to their pre-branch content, just shifted by a
   constant *local* offset (+419 and +468 lines respectively, verified
   with a `diff` of the old and new ranges before trusting it); two
   other single-line anchors (`begin_write`'s signature and its
   `active_writer_count` check) shifted by a different, independently
   verified offset (+58). Updated `cite-anchors.tsv` and every prose
   occurrence — including bare-colon continuation cites
   (`:6171`/`:6234`/`:6288`/`:6305` etc.) — across `README.md`,
   `RESULTS.md`, `WalCrash.tla` (preserving its column-boxed comment
   alignment, all edited lines re-verified at the fixed 78-char width),
   and the mutation/mode `.cfg` files. Result: "all anchors verified —
   ok" (66/66).
   `make formal/tla-smoke`: `S0Smoke` no error (expected), `S0Canary`
   still discriminates (TLC exit 12, expected) — the canary was not
   silently defanged by the re-anchor.
5. `make paging/check`: PASS, no threshold re-record needed — see
   §13 for the numbers; block decode did not move either shape gate.
6. Oracle soak: `PROPTEST_CASES=512 cargo test --features persistence --test paged_block_leaves -- oracle`,
   once, as the pre-merge soak — see §13.
7. Local sanity lever cell (`compare_benches/scripts/fs_paged_levers.sh`,
   `LIMIT=207642624`) — glibc-T=32 cell, **local, sanity only, not a
   perf claim** — see §13 for the number and its comparison to the
   2026-08-31 NVMe baseline (13,343 ops/s / 0.852 majflt/op).

## 13. Gate results (fill-in from this task's run)

Everything in this section was run 2026-09-02 at HEAD `cf4fc1a` (docs
uncommitted at run time) on the local box — the *same host* as the
2026-08-31 write-path spike (YMTC PC411-1TB NVMe, 32 vCPU, 60 GiB, 8 GiB
swapfile), so same-host comparisons against
`docs/benchmarks/paged-write-path-spike-local-2026-08-31.md` are legitimate
as *ordering*; every cell is n=1 and **not a perf claim**. This box's
NVMe is slow (Peter, 2026-09-02), so nothing here is comparable to the
bench host's absolutes — only same-host A/B against the spike's cells
and against the other engines' local cells. Raw logs and
JSON were kept in the session scratchpad only; the gate's own JSON is
`target/paging-check/{C,A}.json`.

### 13.1 `make paging/check` — PASS, all six assertions *(2026-09-02: twelve since §7c — three residency assertions per cell; numbers in §7c and §7d)*

```
[paging/check] C/uniform: ops=360448 timed_out=True ops_per_sec=5885 pf_per_op=0.5526 majflt_per_op=4.640 recover_secs=0.911
[paging/check] A/zipf:    ops=108032 timed_out=True ops_per_sec=1799 pf_per_op=0.3320 majflt_per_op=16.219 recover_secs=3.468
PASS x6 (cg_events_oom == 0 both cells; C pf_per_op <= 1.5; A pf_per_op <= 3.0; recover_secs <= 5.0 both cells)
```

Against the task63 Task-16 run of the same gate (2026-08-31, same
thresholds, `docs/tasks/task63_paged_btree.md` "Acceptance"):

| cell | metric | task63 (pre-blocks) | this run (blocks) |
|---|---|---|---|
| C/uniform | ops/s | 17,113 (500k, cap) | 5,885 (360k, **timed out**) |
| C/uniform | pf/op (DB) | 0.156 | 0.553 |
| C/uniform | majflt/op (OS) | 0.753 | 4.640 |
| A/zipf | ops/s | 1,304 (78k, timed out) | 1,799 (108k, timed out) |
| A/zipf | pf/op (DB) | 0.370 | 0.332 |
| A/zipf | majflt/op (OS) | 11.98 | 16.22 |

No threshold was re-recorded: both cells stay inside the gate. But the
C-cell shape moved, and in the direction the spec predicts rather than
noise: the DB-level fault rate is a counter, not a timing, and it rose
3.5x because **the same nominal 64 MiB budget now holds fewer rows** —
value bytes are counted (§5) where before they lived outside
`NODE_BYTES` and were effectively free. A deployment that tuned
`memory_budget_bytes` against the pre-task64 undercount gets a smaller
real resident set after upgrading and must raise the budget to keep the
same residency. This is the honest-budget trade, not a regression to
chase, but it is user-visible and belongs in the changelog entry.

### 13.2 Oracle soak — PASS

`PROPTEST_CASES=512 cargo test --features persistence --test paged_block_leaves -- oracle`:
3 passed, 0 failed (`paged_blocks_equal_in_memory_oracle` plus the two
recovered-tree oracle tests), exit 0.

### 13.3 Local sanity lever cells (same host as the spike, sanity only)

All cells: 5M rows insert-loaded, zipf, cgroup `memory.max` = 198 MiB
(`LIMIT=207642624`), paged budget 64 MiB, Eventual, glibc, T=32,
`--restart`, 500k ops / 60 s. "run1" = fresh from ingest, "run2" = after
drop + `recover()`.

| cell | run1 ops/s | majflt/op | pf/op | run2 ops/s | resident_leaf_bytes_end | leaves_demoted (run) | checkpointer_runs (run) |
|---|---|---|---|---|---|---|---|
| C/zipf, retention 1 (the Task 12 brief's cell) | 9,320 (500k, cap) | 3.104 | 0.305 | 10,088 | n/a | n/a | n/a |
| A/zipf, retention 1 | 1,263 (76k, timed out) | 23.6 | 0.374 | 2,195 | 174.8 MB | **0** | 2 |
| A/zipf, retention 10 | 1,243 (76k, timed out) | 23.3 | 0.374 | 2,221 | 174.8 MB | **0** | 2 |

Same-host pre-task64 reference points from the spike JSON
(`data-paged-write-path-spike-2026-08-31.jsonl`): `base-C` 33,671 ops/s
at 0.30 majflt/op; `ret1-A` 1,035 at 15.7; `restart-A` 974 -> 3,727 on
run2.

What these say, stated as observations (n=1, ordering only):

1. **The write-path gap did not close locally.** A/zipf run1 is ~1.25k
   ops/s post-blocks vs ~1.0k pre-blocks — inside noise — and the
   restart (compact-heap) gain that identified F6 shrank from 3.8x to
   1.7x rather than vanishing. Whatever fragmentation the block
   representation removed, the pressured write cell on this host is
   still ~15x behind the ReDB number the spike quoted (18.8k). The
   spec's §8.7 NVMe validation remains the only thing that can settle
   the F6 claim, and this local result lowers the prior that it will.
2. **Retention is not a lever on workload A here** — 1,263 vs 1,243
   ops/s at retention 1 vs 10, byte-identical `pf_per_op` and
   `resident_leaf_bytes_end`. This agrees with the spike's own F5
   probe (`ret1-A` 1,035 vs `base-A` 980) and **contradicts the 56x
   figure this task's §1 and the spec's §1 were built on** — see 13.4.
3. **The budget did not hold during either pressured run.** Both A
   cells finished at 174.8 MB `resident_leaf_bytes_end` against a 64 MiB
   budget with `leaves_demoted = 0` across the run phase, despite two
   checkpointer runs that wrote ~18.5k pages each; the gate's C cell
   finished at 93.0 MB against the same budget after one run that
   demoted 152k leaves and then never ran again in the window. The
   fault-in wake (`PageCodec` credit path, `wake_checkpointer` when
   `resident >= budget`) and the mem trigger (`due_mem`) both exist in
   the code; why they produced 1-2 runs and zero demotions in 60 s under
   swap pressure is **undiagnosed**. `make paging/check` cannot see this
   — none of its six assertions compare `resident_leaf_bytes_end` to
   the budget. Until this is understood, §7's "a demote pass exits
   within 8 cycles, always" is true but the surrounding claim that
   `memory_budget_bytes` is a *hard cap* is only demonstrated for the
   deterministic unit tests, not for a pressured 60 s workload.
4. **Reads under pressure got slower with the honest budget.** C/zipf at
   retention 1: 33.7k -> 9.3k ops/s, 0.30 -> 3.10 majflt/op. Pre-task64
   the F1 bug left the counter at 0, so nothing ever demoted and the
   kernel's own LRU kept the hot zipf leaves in RAM; now 152k leaves are
   demoted at tighten and every hot-leaf fault-in allocates a fresh
   block. Mechanism is a hypothesis; the number is same-host.

### 13.4 Correction to the evidence base (found during this sweep)

`compare_benches/scripts/fs_paged_levers.sh` passed no `--workload`, so
every "lever" cell in `docs/benchmarks/fs-paged-nvme-2026-08-31.md` ran
`paging_matrix`'s **default workload C (read-only)**, not the pressured
A the doc's headline names. The matrix's own C/zipf/eventual cell at the
default retention 10 ran 13,664 ops/s (0.859 majflt/op); the lever's
C/zipf cell at retention 1 ran 13,343 (0.852). Retention did nothing on
C. The "237 vs 13,343 (~56x)" comparison is workload A at retention 10
against workload C at retention 1, and "29.4k, 2.2x ReDB on the same
cells" compares T=8 on C against ReDB's A cell (ReDB's C cell was 28.4k,
so T=8 is ~parity on C). Corrections are now stamped in that doc, in
`docs/tasks/task63_paged_btree.md` §7's guidance, and in §1/§8 of this
doc; the script now defaults to `--workload=A` (override with
`WORKLOAD=`). The spec is left as written (design history) — its §1
"Pins (NVMe dominant)" bullet is the sentence this correction retracts.

Consequence for what shipped: the pin *mechanism* is real and was
independently proven by Task 9 (§6 — steady-state pins under any
retention > 1), and `pinned_leaf_bytes` is correct accounting either
way. What is withdrawn is the *magnitude* argument for making retention
shrink **on by default** (§8): no valid measurement on any host shows
retention as a throughput lever on a write workload. Whether the
default stays on is a ruling for Peter, recorded in §14 — the code was
not changed here.

## 14. Open pre-publish obligation

The spec's decisive validation (§8.7) — an NVMe bench-host lever re-run
confirming out-of-box A/eventual moves from ~237 ops/s toward
lever-class numbers (T=8 floor: 29.4k) now that F6 is fixed structurally
— is **explicitly out of scope for this task**. It requires
`bench-infra/`'s billable AWS provisioning, which needs its own separate,
explicit authorization under the cloud-fleet policy (see CLAUDE.md,
"Benchmarking & Performance Testing"). Recorded here as the standing
open obligation before any published perf claim about this feature:
**do not claim a validated perf win until that run has happened.**

Added by the Task 12 sweep (2026-09-02, details in §13):

1. **Budget not enforced under pressured 60 s workloads — DIAGNOSED
   2026-09-02; cause (a) FIXED the same day (decide/apply split, §7a),
   (b) mostly removed by the same change, (c) bounded the same day by
   scaling the dirty trigger to the budget (§7b) — the floor itself is
   inherent and stays documented.** Both local A cells ended at 174.8 MB resident
   vs a 64 MiB budget with zero demotions during the run; the gate's C
   cell ended at 93 MB. Root cause from an env-gated trace of the demote
   pass and checkpointer tick (temporary instrumentation, reverted; 30 s
   A/zipf and C/zipf cells, 198 MiB cgroup, retention 1, same host as
   §13.3), three compounding causes:

   - **(a) Every demote batch loses the install race under a commit
     stream.** `demote_pass_inner` reads `latest_version`, walks
     `paged_demote` off-lock, then `install_paged_tables(version, ..)`
     into *that* version. The harness commits once per write op
     (~600 commits/s). Traced batches walked 272-2113 ms each and by
     install time `latest_now` was 200-3000 versions past `version`;
     under retention 1 the version was already gc'd, so all four
     in-run batches returned `None` (`landed=false`) — nothing counted,
     nothing freed from the live snapshot. Under retention N > 1 the
     same batch *lands in the stale version* (`install_paged_tables`
     only checks presence, not latest-ness), so it is counted and
     `resident_leaf_bytes` is debited while the live latest keeps every
     leaf — a silent counter drift, and the reason the retention-10 A
     cell also showed zero: at 600 commits/s a 300 ms walk is already
     >10 versions behind. The demote doc's "the next batch reads
     `latest_version` fresh ... so the pass converges regardless"
     assumes batches are faster than commits; under memory pressure
     they are three orders of magnitude slower.
   - **(b) The walk is 0.3-4.5 s per 1024 leaf-parents under swap**
     (C: seven batches, median 1.56 s, max 4.53 s; A: median 0.74 s).
     `demote_leaves` `make_mut`s every parent on the path (a fresh
     ~1.5 KB node per leaf-parent), and calls `load_quiet(src).leaf_bytes()`
     on every demotable leaf — one swap fault per leaf just to measure
     it. At 5M rows / T=32 one batch is ~45% of the tree. The first tick
     (also the *first checkpoint ever*: 87k pages written, load never
     tripped the 256 MiB dirty trigger) took 20 s of A's 30 s window and
     did not finish inside C's; that is why the 60 s C cell logged one
     run and the A cells two. `MIN_INTER_RUN` is irrelevant at this
     scale.
   - **(c) Dirty leaves are never demotable, and the pre-first-checkpoint
     counter is bogus** *(the counter half fixed 2026-09-02 by reconciling
     before the pass, §7c)*. A leaf modified since the last checkpoint has
     no page id, so `paged_demote` skips it; in-run batches found 9-27
     demotable leaves per 1024 parents because the hot zipf set is
     dirty until the next 20 s checkpoint writes it. And because load
     never credits `resident_leaf_bytes` for created leaves (the task63
     F1 gap, reconciled only at checkpoint end), the very first pass
     debited 478 MB from a 67 MB counter, read `-360,631,696`, decided
     it was under budget after two batches, and quit — the end-of-tick
     reconcile then reported 128.8 MB real.

   **Why the gate is blind:** `paging_check.py` asserts DB faults/op,
   OOM count, and recover time — never `resident_leaf_bytes_end` against
   the budget. *(Fixed 2026-09-02, §7c: four residency assertions.)*

   **Fix (shipped, see §7a):** split decide from apply. Walk off-lock to *choose* (leaf-parent key,
   child page ids) with no `make_mut` and no leaf deref (measure bytes
   from the slot's cached length or accept `NODE_BYTES + n*size_of::<V>`
   from the parent's entry count); then under `inner.write()` re-read
   latest, re-apply to the *current* latest's parents (inner levels are
   resident, so this is cheap), install at the current version, and
   drop the superseded table outside the lock. That makes (a) land
   every time and removes most of (b); (c)'s dirty-leaf floor is
   inherent — under a sustained write workload the enforceable budget
   is `budget` only *between* checkpoints, so either the checkpointer
   must run far more often under pressure (dirty trigger scaled to the
   budget, not a fixed 256 MiB) or the doc must say the cap excludes
   the dirty set. The existing
   `demote_pass_race_hook_dropped_install_does_not_bump_stats` test
   pins today's lose-the-race behavior as correct and would have to
   flip to "re-applies and lands".
2. **Re-rule `shrink_retention_under_pressure` default.** Peter's
   2026-08-31 ruling (spec §9.5) was made on the 56x figure, which §13.4
   retracts. Keep-on is defensible on principle (budget declares memory
   wins over history); the evidence for it as a lever is gone.
3. **F6 fix unconfirmed locally.** Same-host A/zipf run1 moved ~1.0k ->
   ~1.25k ops/s (noise) and the compact-heap restart gain shrank 3.8x ->
   1.7x but did not vanish. The NVMe run above is still the decisive
   test, but expect it to need the write-path spike's methodology
   (per-phase PagedStats deltas) rather than a bare lever cell.
4. **Honest budget shrinks effective residency at a fixed
   `memory_budget_bytes`** (C/uniform pf/op 0.156 -> 0.553 at 64 MiB).
   Changelog entry needed: paged mode has none at all under Unreleased
   yet, for task63 or task64.
5. **Read-only saw-tooth between ~0 and 5x budget** (§7d table: C/uniform
   trough 0.01, max 5.09, mean 2.13; A/zipf holds at mean 1.01). Two
   halves: (i) a pass evicts every quiet leaf in its sweep instead of
   stopping at the budget — `demote_pass_inner` checks `resident <=
   budget` once per *cycle*, after a whole-tree sweep, so the cheap fix
   is to check per applied chunk; (ii) the tick period is seconds, so
   the workload refaults far past the budget before the next pass lands. The budget is enforced only
   when a demote pass lands, and between the `due_mem` wake and the pass
   landing the workload keeps faulting in. Unverified hypothesis for
   the tick cost: `apply_demote` derefs every evicted leaf
   (`load_quiet(src).leaf_bytes()`) under `inner.write()` to debit its
   bytes — one swap fault per leaf, serialized against all readers.
   Candidate fix: measure in the off-lock plan (carry `PageId -> bytes`
   in `DemoteChunk`; a leaf with a page id is content-immutable, so the
   size cannot drift between plan and apply), leaving the apply to
   touch slot atomics and parent clones only. Needs an env-gated
   per-tick timing trace to confirm before implementing; then tighten
   `RESIDENT_TROUGH_MAX` in `paging_check.py` (and start gating the
   series max) to whatever the measured floor supports.
6. **`Store::drop` gate residual (§7d).** A last handle held by a
   `VersionPin` or an open transaction is not a `Store`, so its drop does
   not go through the gate; the destructor then runs wherever that last
   reference drops, worker included. Not reachable by the harness or any
   test today; a `Store::close()`-style explicit wait would close it.

## 15. Files touched (feature-wide, Tasks 1-12)

`src/btree.rs`, `src/child.rs`, `src/table.rs`, `src/store.rs`,
`src/pagecodec.rs`, `src/registry.rs`, `src/error.rs`, `src/index.rs`,
`src/metrics.rs`, `src/persistence.rs` — plus
`tests/paged_block_leaves.rs`, `tests/paged_accounting.rs`,
`tests/paged_format_compat.rs`, `tests/paged_write_after_recover.rs`,
`tests/fixtures/paged_prechange/` (new), and the migration sweep across
every other `tests/paged_*.rs` file and
`compare_benches/src/bin/paging_matrix.rs`. Formal-verification
re-anchor: `formal/tla/wal/{cite-anchors.tsv,README.md,RESULTS.md,WalCrash.tla}`
and its `mutations/`/`modes/`/root `.cfg` files.
