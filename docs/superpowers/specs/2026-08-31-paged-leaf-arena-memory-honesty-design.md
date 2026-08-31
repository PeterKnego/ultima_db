# Paged leaf value blocks + memory-honest budget (stage-3 memory-honesty slice)

Date: 2026-08-31. Status: approved section-by-section by Peter (slice scope,
inline values, `R: Clone` mechanism, hard-cap eviction, adaptive retention
shrink on-by-default, API/compat, testing) — this document is the binding
spec for the implementation plan.

## 1. Problem and evidence

The fs-paged comparison matrix (task63's bench tier) showed paged UltimaDB
15-25x behind ReDB on pressured write workloads. The write-path spike
(`docs/benchmarks/paged-write-path-spike-local-2026-08-31.md`, branch
history now on main `00c0e3d..ac91b19`) decomposed the gap; the NVMe
bench-host run (`docs/benchmarks/fs-paged-nvme-2026-08-31.md`) validated
which levers replicate. Three structural findings drive this spec:

- **F6 (local dominant): heap fragmentation.** Every value is its own heap
  `Arc<V>` referenced from leaf entries (`Entries<K,V> =
  FixedVec<(K, Arc<V>)>`, src/btree.rs:384). Insert-era CoW churn smears
  surviving leaves+values at low density across the whole load-era heap;
  sub-page holes defeat `malloc_trim`; a bounded-memory deployment pays
  page-granularity swap rent on the scatter.
- **Pins (NVMe dominant): retained snapshots are invisible to the budget.**
  Older retained snapshots CoW-share pre-demotion leaves; the resident
  counter debits at demote while the bytes stay live. On the bench host
  this is a 56x throughput lever (237 vs 13,343 ops/s, retention 10 vs 1).
- **F2: second-chance demotion cannot reclaim a continuously-touched set**
  (0-41 of ~80k leaves evicted under forced checkpoints), and
  `NODE_BYTES = size_of::<BTreeNode>()` (src/child.rs:305) excludes value
  bytes entirely — the budget is neither honest nor enforceable.

Cross-box caveat, stated because it shaped the design: the dominant
mechanism differed by box (fragmentation locally, pins on the AWS kernel).
Both are fixed structurally here rather than tuned per-host.

## 2. Scope

**In (this spec):** per-leaf value blocks for paged data leaves; `R: Clone`
plumbing via an additive registration entry point; exact resident/pinned
accounting including retained snapshots; hard-cap clock eviction; adaptive
retention shrink under budget pressure (on by default).

**Out (deliberately):** the read-robustness slice (Result-returning reads,
version pinning ergonomics, in-place eviction) — its own future spec; full
single-allocation arena leaves (Approach 2 — an evolution of this design if
NVMe data ever says the second allocation matters); heap bytes *inside* `V`
(documented undercount, §4); fanout stamping in the root record (existing
separate follow-up).

## 3. Leaf representation and read path (§1, approved)

```rust
/// Newtype over Option<Arc<V>>: Some = shared heap value (inner nodes,
/// non-paged trees — byte-identical to today via the NonNull niche, 8B);
/// None = "in-block": the value lives at this entry's own position in the
/// node's value block. No index is stored; blocks are in entry order.
pub(crate) struct Value<V>(Option<Arc<V>>);

pub(crate) type Entries<K, V> = FixedVec<(K, Value<V>), { MAX_KEYS + 1 }>;

pub(crate) struct BTreeNode<K, V> {
    entries: Entries<K, V>,
    children: Children<K, V>,
    /// Some only on paged data-tree leaves ("block leaves"). +8B per node.
    block: Option<Box<[V]>>,
}
```

- **Invariant I-A (all-or-nothing):** a node's entries are all-Arc or
  all-in-block; never mixed. Enforced by construction (the only block-leaf
  constructors are `NodeCodec::decode` and leaf CoW rebuilds) and by a
  debug assert on every block-leaf build.
- **Invariant I-B:** block leaves exist only in trees whose `clone_fn`
  (§4) is `Some` — i.e., only paged-attached data trees. Inner nodes and
  every index tree stay all-Arc (always resident: ~1.6% of entries at
  T=32; they neither demote nor fragment against the budget).
- Reads: `get -> Option<&R>` borrows through the Arc or into the block —
  same signature, zero-copy, one branch per leaf (not per entry).
- **Wire format unchanged in both directions.** Encode serializes values
  identically from either representation; decode chooses the
  representation by whether the tree is a paged data tree. No format
  version, no migration; task62 compatibility rules hold trivially.

## 4. CoW write path and `R: Clone` plumbing (§2, approved)

- The tree gains `clone_fn: Option<fn(&V) -> V>`, set to `V::clone` at
  paged attach where `R: Clone` is statically known (same erased-fn
  pattern as index extractors). No `V: Clone` bound on any `BTree` method;
  non-paged trees never observe the field.
- **Public surface (additive):** `register_table_paged::<R: Record +
  Clone>(name)` — the required registration for tables of a paged store;
  it also works (as plain registration) on non-paged stores. On a paged
  store, plain `register_table` returns the new `Error::PagedNeedsClone`
  naming the fix. Free of real-world breakage: paged mode is unreleased
  (0.3.0 predates it). The alternative (folding `Clone` into `Record`
  under `persistence`) was rejected as bound-creep onto SMR/row-format
  users.
- **Mutations build the new block in one allocation pass** — never
  clone-then-mutate: update = rebuild with slot i replaced (incoming value
  moves in; displaced value drops, not clones); insert = len+1 rebuild;
  split = one fresh block per half. Values arriving as `Arc<R>`
  (`upsert_arc` commit-merge, overlay flush) clone out of the Arc at the
  boundary.
- **Cost, stated honestly:** leaf CoW goes from 63 refcount bumps to a
  block rebuild — memcpy-scale for flat records (~4 KB at T=32, ~500 B at
  T=8, compounding the standing fanout-t8 recommendation for write-heavy
  paged deployments); records with heap fields pay a real per-element
  clone. `dirty_bytes` credits `NODE_BYTES + block bytes`.

## 5. Fault-in, demotion, and pin-aware accounting (§3, approved)

- Fault-in (`NodeCodec::decode`, today src/pagecodec.rs:263) builds the
  block directly — one `Box<[V]>` instead of n `Arc::new` calls — and
  credits `NODE_BYTES + n * size_of::<V>()`. **Documented limit:** heap
  bytes inside `V` are not counted (exact for the flat small-row target;
  an undercount for heap-carrying records).
- Demotion frees exactly two allocations per leaf; the debit is computed
  from the dropped leaf itself — symmetric with the credit by
  construction. The F1 checkpoint reconciliation (shipped `7558715`)
  remains the safety net, with `resident_leaf_estimate` upgraded to count
  block bytes.
- **Pin-aware reconciliation:** the checkpoint-end reconcile walks the
  data trees of ALL retained snapshots with pointer-identity dedup (the
  `BTree::diff` ptr-walk trick), counting each unique resident leaf once.
  New stat `pinned_leaf_bytes` = resident bytes reachable only from
  non-latest snapshots (un-evictable by demote). CoW sharing keeps the
  multi-root walk cheap (mostly retraced identical pointers).
- **Enforcement arm — adaptive retention shrink, ON by default (Peter's
  ruling):** when the reconciled counter stays over budget and
  `pinned_leaf_bytes` is the excess, the checkpointer directs gc to shrink
  retention below `num_snapshots_retained`, to a floor of the latest
  version plus every explicit `VersionPin` (user holds always honored).
  Opt-out knob on `PagedOptions` (`#[non_exhaustive]`, builder setter).
  Rationale: configuring a memory budget is a declaration that memory
  wins over history depth; the NVMe data shows defaults otherwise miss by
  56x. Prominent doc callout as the one deliberate behavior choice.

## 6. Hard-cap clock eviction (§4, approved)

- Same trigger and placement: inside the checkpointer tick, after
  `write_dirty` (everything clean, hence demotable); summoned between
  interval ticks by the fault-in budget wake; `MIN_INTER_RUN` (200 ms)
  bounds spikes to fault-rate x 200 ms.
- Within one invocation the pass **cycles**: sweep (clear accessed bits,
  evict already-clear leaves), re-check the counter, sweep again — leaves
  cleared last cycle evict unless re-touched during the pass. Clock hand =
  the existing resume cursor, wrapping. Stop when under budget or nothing
  evictable remains (pinned-only or `Residency::Resident` — both exempt as
  today). Termination: each cycle reclaims bytes or proves the remainder
  un-evictable.
- Deterministic order of weapons in the tick: (1) clock cycles over
  evictable leaves; (2) if still over and pins are the excess, §5's
  retention shrink, then gc, then the next reconcile settles.
- **Behavioral contract:** under sustained over-budget, reads degrade to
  pages.bin re-fault speed (~60-100 us on NVMe) instead of swap-thrash or
  OOM — the budget is a guarantee, not a goal. New stats: clock cycles per
  pass; forced evictions (evicted-while-recently-touched).

## 7. API boundaries and compatibility (§5, approved)

- `get -> Option<&R>`: unchanged, zero-copy.
- `delete -> Result<Arc<R>>` (src/store.rs:4679 and `Table::delete`): the
  removed value cannot be moved out of a CoW-shared block — cloned into
  the returned Arc; one clone per delete on paged tables.
- `upsert_arc` / overlay flush: clone out of the incoming Arc into the new
  block; merge semantics unchanged. The task59 MultiWriter race matrix
  re-runs as regression.
- **Bulk ingest pays zero clones:** `bulk_load` / `from_sorted` / the
  task51 `BulkBuilder` own their values and build blocks by move.
- Persisted secondary indexes: never block-backed; unchanged end to end.
- Additive API: `register_table_paged`, `Error::PagedNeedsClone`, the
  retention-shrink opt-out, new stats fields; `PagedStatsSnapshot` becomes
  `#[non_exhaustive]` in the same change.

## 8. Testing and verification (§6, approved)

No new `unsafe` (newtype over `Option<Arc<V>>`; `Box<[V]>`) — rides the
existing Miri SB+TB gates; block-leaf tests join the Miri set with
`cfg(miri)` case caps.

1. Equivalence oracle: proptest driving identical op sequences against a
   paged block-leaf table and an in-memory Arc table; identical contents
   after every batch and across checkpoint->recover cycles (extends
   `checkpoint_chain_equivalence`).
2. Representation invariants I-A/I-B: debug asserts + a tree-walk test
   after mixed workloads.
3. Accounting: proptest counter == exact dedup walk after arbitrary
   op/checkpoint/gc sequences; `pinned_leaf_bytes` vs a reference walk;
   the F1 regression test remains.
4. Hard cap: deterministic over-budget continuously-touched workload
   converges under budget in one pass with forced evictions > 0;
   pinned-excess triggers retention shrink; explicit `VersionPin` is never
   dropped (extends task53 pin tests).
5. Boundaries: block-leaf `delete` value correctness; MultiWriter
   disjoint-key merge on block leaves + task59 matrix re-run; bulk-load
   move path.
6. Format compat: committed fixture directory written by pre-change code
   opens and verifies post-change (task62 discipline); golden-bytes
   assertion that encode output is unchanged.
7. Perf: `make paging/check` stays green (thresholds re-recorded if shape
   moves); decisive validation = one NVMe lever re-run post-implementation
   — out-of-box A/eventual must move from ~237 toward lever-class numbers
   (T=8 floor: 29.4k) — under the cloud-fleet policy (per-run approval +
   verified teardown).

## 9. Decision log (Peter, 2026-08-31)

1. Slice: memory-honesty first (arena + pressure override + budget
   honesty); read-robustness slice separate.
2. Value contract: inline values for paged leaves (over refcounted shared
   arenas and copy-forward).
3. Copy mechanism: `R: Clone` bound via additive registration (over serde
   roundtrip and raw-bytes leaves).
4. Eviction: hard cap — clock cycles may evict recently-touched leaves;
   budget is a guarantee.
5. Retention shrink under pressure: on by default.
