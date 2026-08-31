# task63: Paged B-tree (stages 1+2) — config, metrics, and the consolidated record

**Status:** Implemented (Tasks 1–14) and accepted (Task 16, `make paging/check` — all six
assertions PASS, local/shape-only; see the "Acceptance (Task 16, local, shape only)" section at
the end of this doc). §7 below is unchanged from Task 14 and still describes itself as
"unmeasured" — that referred to the sandbox regression gates that Task 15 filled in
("Measured (Task 15 gates, local)" section) and the Task 16 acceptance run now also fills in;
the published NVMe-rig numbers (spec follow-on 7) remain the only thing still open.
**Related:** `docs/superpowers/specs/2026-08-30-paged-btree-stage-1-2-design.md` (design spec,
binding authority), `docs/superpowers/plans/2026-08-30-paged-btree-stage-1-2.md` (16-task
implementation plan, 4 phases), `.superpowers/sdd/2026-08-30-paged-btree-stage-1-2/progress.md`
(the SDD review ledger this doc summarizes — not pasted verbatim, see §6).

---

## 1. What this feature is

A data set bigger than RAM stays reachable without falling back to a full-row checkpoint that
(de)serializes every row on every checkpoint. Opt in per `Persistence::standalone(..)`/
`Persistence::smr(..)` via `.paged(PagedOptions::builder()....build())`: table B-trees page their
nodes against an on-disk store (`pages.bin`) instead. Data-tree inner levels and every index tree
stay fully resident; data leaves default `Lazy` and demote back to disk under a memory budget
(`Store::set_residency` overrides per table). A background checkpointer thread drives
`Store::checkpoint()` on dirty-bytes/memory-budget/interval triggers, with no application call
required. See `src/lib.rs`'s crate doc and `CLAUDE.md`'s Persistence bullet for the user-facing
summary; this doc is the design-decision and review record.

## 2. Architectural decisions

- **The page file *is* the checkpoint format.** `pages.bin` (append-only, preallocated in
  `prealloc_chunk_bytes` chunks — `src/pagefile.rs`) holds every node; a `checkpoint_{v}.root`
  record (`src/checkpoint.rs`'s `CheckpointKind::Paged`) is the commit point naming each table's
  root page id plus the version's dead-page list. A paged root is always self-contained — never
  part of a delta chain the way row-format checkpoints can be — so `StoreConfig::checkpoint_chain_max`
  is inert in paged mode (`Store::checkpoint_impl` branches to `checkpoint_impl_paged` before the
  knob is ever read; `tests/paged_config.rs::checkpoint_chain_max_is_inert_in_paged_mode` pins
  this).
- **Panic-on-fault for LAZY reads; `Err` for EAGER loads (revised, final-review wave I-1).** A
  corrupt or unreadable on-disk page found during a workload-triggered fault-in (`Child::load`/
  `load_quiet`, a leaf a read actually touches) still **panics** rather than returning `Err` — a
  `Result`-returning fault-in would push the decision of "what does a caller do with a corrupt
  B-tree node mid-traversal" onto every read call site in the crate, and there is no
  `Result`-returning path through the B-tree's existing `&V`-returning API to propagate one
  anyway. But `recover()`'s and index-attach's EAGER loads — `Table::from_paged_entry`'s inner-
  level fault-in and `UniqueStorage`/`NonUniqueStorage::from_root_page`'s full-tree fault-in,
  which run once, up front, with a `Result`-returning signature already in hand — return `Err`
  instead: `Child::try_load` (sharing `fault_in`'s body) backs `BTree::try_load_inner_levels`/
  `try_load_all`, and a corrupt page there fails `recover()`/`define_persisted_index` cleanly
  rather than crashing the process on startup. The infallible `load_inner_levels`/`load_all`
  remain for paths that run *after* a paged store has already recovered successfully (index
  `paged_reachable_ids`'s residency re-assertion), where the invariant is already known to hold.
- **Demotion is a same-version re-publish, not a new commit.** `Store::install_paged_tables`
  swaps a table's `Arc` into the *current* `latest_version`'s snapshot — no WAL entry, no
  write-set bookkeeping, no version bump. `Store::demote_pass`, `Store::checkpoint_impl_paged`'s
  attach/write loop, and `Store::set_residency` all funnel through this one mechanism (Task 8's
  `install_table_clone` generalized by Task 10). `set_residency` additionally holds
  `checkpoint_lock` for its whole body so a `Resident` request can never be silently undone by a
  concurrently running demote batch (Task 10 fix round).
- **Retention-gated punch.** A checkpoint's dead-page diff (pages the new root no longer
  reaches, versus the *previous* successful paged checkpoint) is recorded in the new root but not
  reclaimed yet — `hole-punch`ed only once the root that named a range as dead is itself deleted
  by `retained_checkpoints` rotation (Task 8's forward-risk M8, closed by Task 11's
  `punch_after`/`pending_punch` bookkeeping). Punching earlier would risk reclaiming space an
  older, still-retained root's readers can still reach.
- **Dirty-bytes credits node creation.** `PagedStats::dirty_bytes` must be credited not only when
  a *clean* (already-paged) node is CoW'd, but also when a brand-new node is created — otherwise,
  with the (very common) default `memory_budget_bytes: None`, every trigger the background
  checkpointer thread checks is structurally false on a store that has never yet had a node
  survive one checkpoint, and the checkpointer ships inert (Task 12's review Critical, §5).
- **`checkpoint_interval` defaults to `Some(60s)`**, not `None` — a backstop so no configuration
  leaves every checkpointer trigger false at once (`checkpoint_dirty_bytes` alone can go unmet for
  a long time on a low-write workload, and `memory_budget_bytes` defaults to `None`).
  `PagedOptionsBuilder::checkpoint_interval_disabled()` opts back out. See spec §8 and the plan's
  Task 8 interface note for the same table, updated in this task per the Controller amendment.

## 3. What shipped, by task

| Task | Shipped |
|---|---|
| 1 | `Child<K,V>` slot (atomic `meta` word: page id or `NO_PAGE` + accessed bit; atomic pointer set once by CAS on fault-in) and the `NodeSource` trait. Clone copies on-disk slots without touching resident children — the sibling-refcount fix (§5). |
| 2 | Threaded `Child` slots and an optional `NodeSource` through `BTree` — descent, mutation, and `BTreeRange` all switch to slot-aware traversal with no change to the public read API. |
| 2b | `FixedVec<E,N>` storage changed from `[Option<E>; N]` to `[MaybeUninit<E>; N]` + length — closes the niche-loss regression Task 2's review caught (§5); `Child` back to 16 B, inner nodes 1040→1048 B. |
| 3 | Tree primitives: `height` (now cached, no leaf fault needed to learn tree depth), `write_dirty`, `demote_leaves` (second-chance eviction), `changed_page_ids`, `from_root_page`, `load_inner_levels`. |
| 4 | `PageFile` — preallocated, append-only page store with CRC framing, prefetch reads, and hole-punch reclaim primitives. |
| 5 | `NodeCodec` (data/index page (de)serialization) and `PagedSource` (fault/dirty-byte counters wired to `PagedStats`). |
| 6 | The `.root` record format and `checkpoint_{v}.root` discovery/cleanup in `src/checkpoint.rs`, sharing framing conventions with row-format checkpoints but its own namespace. |
| 7 | Paged hooks on `MergeableTable`/`IndexMaintainer` and the `Table` implementation: `paged_write`, `paged_demote`, changed-pages diffing. |
| 8 | `PagedOptions`, `Persistence::paged`, `PagedState`, and the paged checkpoint path (phases 1–2: dirty walk + root record, no demotion yet). |
| 9 | Recovery from a paged root, the legacy-directory upgrade path, and the config-mismatch refusal (`PagedFormatRequired`). |
| 10 | Demotion install (phase 3) as a same-version re-publish, and the resident-leaf-bytes memory estimate. |
| 11 | Dead-list and hole-punch reclaim (phase 4), with mutation-testing crash points proving the punch step's failure modes. |
| 12 | The background checkpointer thread: dirty-bytes, memory-budget, and interval triggers driving `Store::checkpoint()` with no application call. |
| 13 | Index attach after recovery — `TableWriter::define_persisted_index` semantics, full residency for index trees on attach. |
| 14 | This task: config/docs/metrics completeness pass (below). |

## 4. Task 14: what this pass adds

- **Public surface**: `PagedOptions`, `PagedOptionsBuilder` (now re-exported — previously reachable
  only via `ultima_db::persistence::PagedOptionsBuilder`), `Residency`, `IndexDef`,
  `PagedStatsSnapshot` all export coherently from `src/lib.rs`; a landing-page bullet added to the
  crate doc's "Where to start" section.
- **Metrics** (`--features metrics`): `PagedStatsSnapshot`'s fields mirror into the `metrics` crate
  as gauges (`ultima.paged.*`, `src/metrics.rs`'s `emit_paged_stats`), not counters — the fields
  are already-cumulative totals read from `PagedStats`'s atomics at a point in time, not deltas
  since the last call, and this function fires from more than one place, so counter increments
  would double-count on every call after the first. Two emission points, chosen as the cheapest
  sound spots rather than the hot per-fault/per-dirty paths inside the checkpoint loop: the end of
  `Store::checkpoint_impl_paged` (the write-path half — fires once per completed checkpoint,
  covering a store nobody polls `paged_stats()` on) and `Store::paged_stats()` itself (the
  read-path half — covering a caller that polls without the store ever completing a checkpoint,
  e.g. reading `page_faults` from a `memory_budget_bytes`-only store with `checkpoint_interval`
  disabled). Both share one `PagedStatsSnapshot::from_stats` constructor so they read the same
  fields the same way.
- **`Store::set_residency` docs**: already covered the blocking-under-checkpoint behavior in full
  from Task 10 (holds `checkpoint_lock` for its whole body, documented rationale about a demote
  batch racing a `Resident` request) — verified, not thin; one stray doubled word fixed.
- **String cleanup** (Task 13 deferred minor): four line-wrap artifacts with literal multi-space
  runs collapsed to single spaces via proper `\`-continuation — `src/pagecodec.rs` (raw walker's
  two `CheckpointCorrupted` messages), `src/table.rs` (the changed-pages walk-failure `eprintln!`),
  `tests/paged_index.rs` (an assertion message). `src/btree.rs`'s `load_all` doc caller list
  updated: it now also lists the two `paged_reachable_ids` structural-invariant call sites Task 13
  added, not just the `from_root_page` constructors from Task 6/9.
- **`tests/paged_config.rs`** (new, 4 tests): `checkpoint_chain_max` set alongside `.paged(..)`
  (`Store::new` succeeds, every checkpoint is a `.root`, never a `.bin`); `.paged(..)` composing
  with `Persistence::standalone_fast` end-to-end; MultiWriter with four threads writing disjoint
  key ranges concurrently, each also driving `Store::checkpoint()` periodically so checkpoints
  land mid-write, with every row present after a fresh recovery; `Store::bulk_load` on a paged
  store, checkpoint, recover, rows present (closes a coverage gap Task 8's minors list left open
  generically, and the first place bulk_load is exercised against a paged store at all).

## 5. Headline review findings

Pulled from the SDD ledger's review rounds — see
`.superpowers/sdd/2026-08-30-paged-btree-stage-1-2/progress.md` for the full record; this is a
summary of the four findings load-bearing enough to shape the design, not a restatement of it.

- **The sibling-refcount fix** is the feature's core CoW property, not an incidental optimization:
  `Child::clone` (spec §4's table) copies `meta` and, only if the child is resident
  (`node` non-null), bumps that one `Arc`'s strong count — an on-disk sibling is never touched.
  Before this, a naive port of the existing `Arc<BTreeNode>`-per-child design would have paid a
  refcount bump across a node's *entire* fan-out on every CoW clone of its parent, even for
  children that were never faulted in — exactly the cost the paging baseline measured and this
  design exists to remove (plan, "Architecture").
- **`load_arc`'s UB was in the plan's own reference snippet**, not introduced by an implementer
  deviating from it. Task 1's review caught a pointer derived from `&BTreeNode` and then written
  back as a refcount source — Miri-proven undefined behavior under both Stacked and Tree Borrows.
  The ruling replaced it with an `AtomicPtr`-derived pointer and corrected the plan file in the
  same fix commit, on the general principle that Miri is the arbiter regardless of which document
  a defect originated in.
- **The `FixedVec` niche loss** was a 3× memory regression hiding behind a "2×" plan estimate:
  `[Option<Child>; N]` storage gives `Option<Child>` no niche to exploit, so each slot cost 24 B
  instead of the intended 16 B, and `Children` came out at 1560 B rather than the ~1040 B the plan
  assumed. Caught in Task 2's review, it earned an inserted Task 2b (not folded into Task 2's fix
  round) specifically to change `FixedVec<E,N>`'s backing storage to `[MaybeUninit<E>; N]` + a
  length field, landing `Child` back at 16 B and inner nodes at 1048 B — Miri-verified across all
  four Stacked/Tree-Borrows × deterministic/oracle combinations the controller re-ran directly
  (self-reported implementer Miri output was explicitly not trusted as evidence of record).
- **The inert-trigger catch**: Task 12's review found that with the *original* default
  configuration (`memory_budget_bytes: None`, `checkpoint_interval: None`), the background
  checkpointer thread's three trigger conditions were all structurally false for a store whose
  nodes had never yet survived one checkpoint — `dirty_bytes` was credited only on a *clean*-node
  CoW, never on node creation, so a freshly written, never-checkpointed store could accumulate
  unbounded dirty state with the checkpointer thread never once firing. The fix threads a credit
  into every node-creation site (`Child::resident_new` calling `note_dirty`, 20 sites, 2 documented
  exemptions) and separately changes `checkpoint_interval`'s default to `Some(60s)` (§2) as a
  backstop independent of the dirty-bytes fix — two layers closing the same class of gap from
  different angles.

## 6. Deferred minors

Every task's review round left a short list of non-blocking findings explicitly deferred rather
than folded into that task's fix round — the full ledger entries (search
`.superpowers/sdd/2026-08-30-paged-btree-stage-1-2/progress.md` for `minor (deferred)`) are the
record; summarized by theme rather than reproduced line-for-line:

- **Doc/comment drift**: several off-by-one or overstated doc sentences (`NO_PAGE`'s doc,
  `same_node`'s doc contradicting its own test, a module doc overstating a lifetime guarantee, a
  stale bulk-load-stats comment) — none affect behavior, all are candidates for a future doc pass.
  The four line-wrap string artifacts and the `load_all` caller-list drift in this category were
  the ones this task was explicitly asked to close (§4).
- **Test coverage gaps, not correctness bugs**: several structural checks are asserted by
  `debug_assert` or documented invariant rather than a dedicated test (`Child`'s `Send`/`Sync`
  bounds, `FixedVec::push`'s precondition, a punch-retry double-count edge case, a self-join-guard
  race), plus a handful of paged-mode configuration combinations nobody had exercised yet before
  this task's `tests/paged_config.rs` (indexed table, multi-table, non-`u64` key, retention
  pruning under sustained churn, `bulk_load`, `checkpoint_chain_max` interaction).
- **Cost/inefficiency, not incorrectness**: `demote_leaves` re-walking below its cursor each pass
  instead of pruning by separator, `changed_page_ids` faulting both inner spines per checkpoint,
  and `O(N)` per-table lock trips folded into a single critical section by a later fix round
  (Task 8's I2) but not re-measured since.
- **Narrow-scope leaks, not silent-corruption risk**: a dropped table's pending-index pages, a
  same-version re-checkpoint dropping a dead list, and a partial multi-range punch retry that can
  double-count `dead_pages_punched` on retry — all either idempotent, stats-only, or bounded to
  "reclaim doesn't happen," never to a live page being punched or data being lost.
- **Style-only**: a missing `pub(crate)` on one module declaration, an `allow(too_many_arguments)`,
  a couple of unused-but-documented accessor methods.
- **`fanout-t8` cross-build portability (final-review wave, I-2(b))**: paged mode composes with
  `fanout-t8` *within a build* — a `pages.bin` written under one fanout setting is not portable
  across the flag. Reading a T=32-written page file with a `fanout-t8` build (or vice versa) is
  undiagnosed today: `NodeCodec::decode`'s entry-count bound-check (I-2(a)) turns an
  over-capacity node into a clean `CheckpointCorrupted` `Err` rather than a `FixedVec::push`
  panic, but nothing yet detects the *mismatch itself* up front or names it in the error. Stamping
  the build's fanout (`T`) in the root record so `recover()` can refuse a fanout mismatch by name
  is a planned follow-up, not yet scheduled.

None of these block Task 15/16 — they are recorded here as the place a future cleanup pass would
start, per this task's brief.

## 7. Measured numbers

**Unmeasured until Task 16.** Task 15 (regression gates on the in-memory path) and Task 16
(`make paging/check` — bigger-than-RAM acceptance, built by writes) are the tasks that produce
real numbers; this feature's memory/perf claims (the `Child` slot sizing in §5, the sibling-clone
cost avoided) are structural/Miri-verified, not benchmarked, as of this task. `docs/tasks/task63_paged_btree.md`
(this file) is the place those numbers land once Task 16 runs — see that task's brief for the
exact `cg_events_oom`/`majflt_per_op`/`recover_secs` gates.

## 8. Config surface reference

`PagedOptions` (builder, `#[non_exhaustive]`) — see `src/persistence.rs`:

| knob | default | purpose |
|---|---|---|
| `memory_budget_bytes` | `None` | soft cap on resident data-leaf bytes; `None` disables demotion entirely |
| `checkpoint_dirty_bytes` | 256 MiB | volume trigger for the background checkpointer |
| `checkpoint_interval` | `Some(60s)` | time trigger / backstop (§2) — `checkpoint_interval_disabled()` opts out |
| `demote_batch` | 1024 parents | lock hold per demotion batch |
| `page_prefetch_bytes` | 4 KiB | first `pread` on fault-in |
| `prealloc_chunk_bytes` | 16 MiB | zero-fill grow-ahead quantum |
| `retained_checkpoints` | 2 | `.root` files kept; gates hole-punch eligibility |

`Residency`: `Resident` (eager, never demoted) or `Lazy` (demotable). Data-tree inner levels and
every index tree are always `Resident`; data leaves default `Lazy`, overridable per table via
`Store::set_residency`.

`PagedStatsSnapshot` fields (`src/store.rs`), all mirrored to `metrics` gauges under §4:
`page_faults`, `data_page_faults`, `index_page_faults`, `pages_written`, `leaves_demoted`,
`dirty_bytes`, `resident_leaf_bytes_est`, `checkpointer_runs`, `dead_pages_punched`, and
`dead_pages_dropped` (added after the spec's original list, during Task 11's fix round, for the
out-of-bounds-range defense — included here for completeness).

## 9. Testing

| Suite | Covers |
|---|---|
| `tests/paged_checkpoint.rs` | Phases 1–2: dirty write + root record, MultiWriter no-op settling |
| `tests/paged_recovery.rs` | Recovery from a paged root, legacy-directory upgrade, format refusal |
| `tests/paged_demotion.rs` | Phase 3: demotion install, second-chance accessed-bit eviction |
| `tests/paged_reclaim.rs` | Phase 4: dead-list computation, hole-punch, retention gating |
| `tests/paged_fault_crash_root.rs`, `tests/paged_fault_crash_punch.rs` | Mutation-testing crash points around root-write and punch |
| `tests/paged_checkpointer.rs` | Background checkpointer thread triggers (dirty-bytes, memory-budget, interval) |
| `tests/paged_index.rs` | Persisted secondary indexes: attach after recovery, dead-page accounting for a dropped table's pending index |
| `tests/checkpoint_chain_equivalence.rs` | Paged vs. row-format equivalence matrix (task 9's third config variant) |
| `tests/paged_config.rs` (this task) | `checkpoint_chain_max` inertness, `standalone_fast` composition, MultiWriter + interleaved checkpoints, `bulk_load` on a paged store |
| `src/child.rs`, `src/pagecodec.rs`, `src/pagefile.rs`, `src/btree.rs` unit tests + Miri | `Child` slot semantics, page codec round-trips, page-file bounds, twin-tree demotion proptests |

## 10. Files changed (feature-wide, Tasks 1–14)

`src/child.rs` (new), `src/pagefile.rs` (new), `src/pagecodec.rs` (new), `src/btree.rs`,
`src/table.rs`, `src/store.rs`, `src/index.rs`, `src/persistence.rs`, `src/checkpoint.rs`,
`src/registry.rs`, `src/metrics.rs`, `src/lib.rs`, `src/error.rs`, `src/mutation.rs` — plus the
`tests/paged_*.rs` suite (§9), `CLAUDE.md`, `README.md`, and this file. Base `2a74c3d` (plan
commit) through `f17aa88` (Task 13 head) for Tasks 1–13; Task 14's own commit follows this file.

## Measured (Task 15 gates, local)

Regression gates run 2026-08-30/31 at HEAD `53ab598` (branch `feat/paged-btree`), on the Claude
sandbox host — not the NVMe bench host. Every number below is **local, ±2×, sanity only** per
the repo's bench A/B methodology; none of it is a "faster/slower" conclusion and none of it
re-records a committed baseline.

### `make perf/check` — FAIL (environmental, not a code regression call)

- `smr-apply-microbench`: **PASS** — "perf check OK (10 metrics within tolerance)".
- `mw-commit-microbench`: **FAIL** — 2 of 7 gated metrics outside tolerance:
  - `mw_scaling_8x`: 26103.4 vs baseline 13316.2 (+96.0%)
  - `mw_scaling_efficiency`: 0.7 vs baseline 0.4 (+60.8%)
  - The other 5 metrics on this task (`mw_commit_p99_ns`, `mw_commit_throughput`,
    `mw_conflict_rate`, `mw_disjoint_throughput`, `read_p99_under_load_ns`) were within tolerance.
- The baseline file (`autobench/baselines/multiwriter-commit.json`) carries its own note, verbatim:
  "NVMe-host values: `make perf/check` on the noisy virtualized sandbox WILL fail by design
  (different host shape, not a regression) — re-record locally with `make perf/baseline` if you
  need a sandbox gate." Baseline was recorded 2026-07-26 on an AWS c6id.2xlarge NVMe host at
  `b48295e`, unrelated to this feature branch. Per task rules, this FAIL is reported as-is; no
  baseline was re-recorded and no code was changed to chase it. Re-run on the NVMe bench host
  (`bench-infra/`) is the only way to get a trustworthy verdict on this cell.

### `cargo bench --bench btree_get_bench` / `btree_insert_mut_bench` — HEAD `53ab598` vs base `2a74c3d`, local, ±2×, sanity only

Base built in a worktree (`git worktree add .../base-wt 2a74c3d`) against the same shared
`CARGO_TARGET_DIR`; criterion's own baseline mechanism (HEAD run saved first, base run compared
against it) cross-checks the manually-computed deltas below and agrees in direction throughout.

**`btree_get_bench`** (random-key `get`, median times):

| bench | HEAD `53ab598` | base `2a74c3d` | delta (base→HEAD) |
|---|---|---|---|
| get_random/get/100000 | 3.6272 ms (27.57 Melem/s) | 4.2364 ms (23.61 Melem/s) | HEAD ~14% faster |
| get_random/get/1000000 | 99.965 ms (10.00 Melem/s) | 112.67 ms (8.88 Melem/s) | HEAD ~11% faster (criterion: not statistically significant, p=0.11, high sample variance/outliers on both sides) |

**`btree_insert_mut_bench`** (median times; `immutable` = CoW `insert`, `in_place` = mutable
fast-path append):

| bench | HEAD `53ab598` | base `2a74c3d` | delta (base→HEAD) |
|---|---|---|---|
| insert_ascending/immutable/1000 | 874.08 µs | 864.42 µs | HEAD ~1% slower |
| insert_ascending/immutable/100000 | 224.15 ms | 211.72 ms | HEAD ~6% slower |
| insert_ascending/immutable/1000000 | 2.7518 s | 2.5860 s | HEAD ~6% slower |
| insert_ascending/in_place/1000 | 55.632 µs | 61.288 µs | HEAD ~9% faster |
| insert_ascending/in_place/100000 | 8.1611 ms | 8.6384 ms | HEAD ~6% faster |
| insert_ascending/in_place/1000000 | 98.859 ms | 103.97 ms | HEAD ~5% faster |
| insert_random/immutable/1000 | 856.29 µs | 884.99 µs | HEAD ~3% faster |
| insert_random/immutable/100000 | 200.56 ms | 195.24 ms | HEAD ~3% slower |
| insert_random/immutable/1000000 | 3.0024 s | 2.8542 s | HEAD ~5% slower |
| insert_random/in_place/1000 | 65.310 µs | 130.56 µs | HEAD ~50% faster |
| insert_random/in_place/100000 | 10.769 ms | 17.270 ms | HEAD ~38% faster |
| insert_random/in_place/1000000 | 230.92 ms | 294.09 ms | HEAD ~21% faster |
| insert_mixed_snapshot/immutable/100000 | 224.35 ms | 212.04 ms | HEAD ~6% slower |
| insert_mixed_snapshot/immutable/1000000 | 2.7496 s | 2.5909 s | HEAD ~6% slower |
| insert_mixed_snapshot/in_place/100000 | 7.5229 ms | 8.1925 ms | HEAD ~8% faster |
| insert_mixed_snapshot/in_place/1000000 | 96.398 ms | 100.59 ms | HEAD ~4% faster |

Shape observed (local, sanity only): the `in_place` mutable fast-path is consistently faster at
HEAD than at the pre-feature base across every size — the `insert_random/in_place` column lands
in the +21%..+50% range, consistent with the brief's "Task 2b recovered +21-28%" signal at the
100k/1M sizes. The `immutable`/CoW path shows the opposite shape at 100k/1M sizes — consistently
~3-6% slower at HEAD than base, plausibly the `Child` indirection/page-bookkeeping overhead the
paged-btree feature adds to the CoW path; at the 1000-row size the immutable delta is small and
sign-mixed. None of this is a "regression" claim — it is a same-host, same-run-order shape
comparison at ±2× sandbox noise, offered as a sanity check, not a verdict.

### `formal/scripts/check-cites.py` and `formal/scripts/check-drift.sh` — both PASS

- `check-cites.py`: "cite-check: all anchors verified — ok." (66 distinct `src/*.rs` anchors, 66
  manifest rows, cited sources `src/persistence.rs`, `src/store.rs`, `src/wal.rs`). No re-anchoring
  was needed.
- `check-drift.sh`: "formal drift-check: src/btree.rs src/persistence.rs src/store.rs src/wal.rs
  and formal/ both changed — ok."

### `make consistency/elle` — PASS (java available)

All three history classes (point, scan-ratio 0.5, predicate-ratio 0.5/4 buckets) passed under
both SI and Serializable isolation: known-bad write-skew fixture correctly rejected under
serializable / accepted under SI; SI histories satisfy snapshot-isolation with anomalies ⊆
{G2-item} (write skew only, nothing worse); SSI histories satisfy serializable with no anomaly
types. "elle consistency check passed" printed three times (once per history class).

### Full suite — all PASS

| suite | result |
|---|---|
| `cargo test` | ok, 0 failed across all ~30 unit+integration binaries (677 lib unit tests + all integration suites) |
| `cargo test --features persistence` | ok, 0 failed — identical binary/test-count set to plain `cargo test`, because workspace-wide feature unification already activates `ultima-db/persistence` for the plain run (`bench_workloads`, `compare_benches`, and `autobench` all depend on `ultima-db` with `features=["persistence"]`, and `cargo test` at the workspace root builds/tests all members together) |
| `cargo test --features persistence,mutation-testing` | ok, 0 failed — 682 lib unit tests (5 more than the base runs) plus the mutation-testing-only integration tests activate |
| `cargo test --features persistence,metrics` | ok, 0 failed |
| `cargo test -p ultima-vector` | ok, 0 failed (60 unit + doctest + integration suites, including `results_stay_inside_filter`) |
| `cargo clippy --all-targets --all-features -- -D warnings` | clean, zero warnings/errors — `--all-features` (including `wal-iouring`) built and checked without conflict, so no fallback to the four documented configs was needed |
| `cargo bench --no-run -p compare-benches` | compiles clean — all YCSB/SmallBank/paging_matrix bench binaries built |

## Acceptance (Task 16, local, shape only)

`make paging/check` (root Makefile): the `ultima-paged` `Engine` impl in
`compare_benches/src/bin/paging_matrix.rs` (`Persistence::standalone(dir, Eventual, Coalesced)`
`.paged(PagedOptions::builder().memory_budget_bytes(64 MiB).build())`, table built by
`--load=insert` — batched `insert_batch` in 10k-row write transactions, not `bulk_load`'s
`from_sorted`) run against 5,000,000 rows inside a `systemd-run --user --scope` cgroup v2 memory
scope at `memory.max` = 256 MiB (`scripts/paging_matrix.sh LIMIT=268435456`) — a deliberately
different knob from the store's own 64 MiB `--paged-budget`: the cgroup ceiling has to cover
everything the process needs beyond paged data leaves (inner nodes, every index tree, WAL/
checkpoint buffers, the harness itself), not just the thing `memory_budget_bytes` demotes. Two
cells, each run with `--restart` (drop the store, reopen, `Store::recover()`, run the same
workload again), `--ops=500000 --timeout-secs=60`. Run 2026-08-31 on the Claude sandbox host (not
the NVMe bench host) — `compare_benches/target/bench-scratch` on the real ext4 root disk (not
`/tmp`, which is tmpfs on this host — see the guardrail in this repo's bench methodology notes),
verified via `bench_disk_dir`'s own fstype assertion. Per repo convention this is a **shape gate**,
not a published perf number; the NVMe rig run (spec follow-on 7) is what produces those.

Two summary lines (stderr, first run of each cell):

```
[ultima-paged] C/uniform ratio=LIMIT=268435456 ops=500000 (cap) 17113 ops/s p50=57.7us p99=271.0us p999=1304.8us majflt/op=0.753 minflt=284112 pswpin=712187 pswpout=183232 rss_end=193MiB psi_some=6370ms drop=0.00s (majflt 21)
[ultima-paged] A/zipf ratio=LIMIT=268435456 ops=78336 (timeout) 1304 ops/s p50=83.5us p99=30973.4us p999=61081.0us majflt/op=11.976 minflt=82368 pswpin=983806 pswpout=974247 rss_end=257MiB psi_some=13048ms drop=0.00s (majflt 5)
```

(The A/zipf cell hit its 60s `--timeout-secs` cap at 78,336/500,000 ops under real swap
thrashing — 50% writes at a hot-key zipfian distribution inside a 256 MiB cgroup over a ~1.7 GiB
unconstrained working set. `timed_out=true` in the JSON; the checker's assertions still hold over
whatever ops actually completed, per the brief.)

**Disclosed substitution (fix round 1, controller-ruled):** the task16 plan specified a
size-scaled `recover_secs` bound — "5x the first checkpoint's inner-level count x 0.1 ms" — that
is unimplementable as written: "inner-level count" is defined nowhere in the plan, and no such
field exists in the paging_matrix report JSON to compute it from. The controller ruled that the
flat `<= 5.0s` bound below stands as a coarse LOCAL shape gate in place of the unimplementable
formula; the NVMe-host rerun (spec follow-on 7) is what sets real, published bounds.

Checker verdicts (`compare_benches/scripts/paging_check.py`, thresholds from the task16 brief):

```
[paging/check] C/uniform: engine=ultima-paged load=insert rows=5000000 ops=500000 timed_out=False ops_per_sec=17113 pf_per_op=0.156058 majflt_per_op=0.753 recover_secs=0.140189035
[paging/check] A/zipf:    engine=ultima-paged load=insert rows=5000000 ops=78336 timed_out=True ops_per_sec=1304 pf_per_op=0.37037888071895425 majflt_per_op=11.976 recover_secs=2.120307247
[paging/check] PASS: C/uniform cg_events_oom == 0 — cg_events_oom=0
[paging/check] PASS: A/zipf cg_events_oom == 0 — cg_events_oom=0
[paging/check] PASS: C-cell pf_per_op <= 1.5 — pf_per_op=0.156058
[paging/check] PASS: A-cell pf_per_op <= 3.0 — pf_per_op=0.37037888071895425
[paging/check] PASS: C/uniform recover_secs <= 5.0 — recover_secs=0.140189035
[paging/check] PASS: A/zipf recover_secs <= 5.0 — recover_secs=2.120307247
[paging/check] ALL ASSERTIONS PASSED
```

All six assertions **PASS**. Notable shape findings, not conclusions (sandbox, ±2×, single run,
tmpfs-adjacent host — see the repo's bench A/B methodology notes):

- `pf_per_op` — the DB-level counter (`PagedStats::data_page_faults / ops`, a positioned `pread`
  against `pages.bin`, not an OS major fault) — stayed well under both thresholds (0.156 vs 1.5
  for C; 0.370 vs 3.0 for A) even though the *OS-level* `majflt_per_op` was heavy (0.75 and 12.0
  respectively) under the tight 256 MiB cgroup. The two numbers measure different things: OS
  major faults include WAL/checkpoint-buffer and general allocator page-ins under real memory
  pressure, not just paged B-tree data-leaf faults.
- `recover_secs` came in at 0.14s (C) and 2.12s (A) against the 5.0s gate — both comfortably
  under, though the A-cell's is ~15× the C-cell's, consistent with recovery replaying more WAL
  entries after a write-heavy (50%), partially-completed (timed-out) run.
- `cg_events_oom == 0` on both cells: the 256 MiB/64 MiB split held under real memory pressure
  (`cg_pgmajfault` and `pswpin`/`pswpout` both far from zero) without the scope being OOM-killed.

Files: `compare_benches/src/bin/paging_matrix.rs` (`UltimaPagedEngine`, `Engine::restart`/
`paged_data_faults`, `run_workload` extraction, `--paged-budget`/`--restart` args), `Makefile`
(`paging/check` target), `compare_benches/scripts/paging_check.py` (new checker),
`compare_benches/scripts/paging_matrix_run.sh` (`ultima-paged` engine awareness for the general
sweep script — `PAGED_BUDGET` env var, `engine_arg`/`extra_args_for`).

No test failures were encountered at any point in this task; nothing was patched.

## 7. Performance under memory pressure — the cheap stack (2026-08-31 spike follow-up)

The fs-paged comparison matrix (paged UltimaDB vs RocksDB/Fjall/ReDB, every engine under one
cgroup `memory.max`) exposed a 15-25× write-workload gap vs ReDB at 5M rows / 198 MiB. The
spike (`docs/benchmarks/paged-write-path-spike-local-2026-08-31.md`, branch
`spike/paged-write-path`) decomposed it; the dominant term is **heap fragmentation** (F6):
insert-era CoW churn smears the surviving resident leaves + values across the whole load-era
heap at low density, and a bounded-memory deployment then pays page-granularity swap rent on
the scatter. It was *not* the paging machinery — the F1-fixed estimate proved resident data
leaves sat *under* budget throughout.

**Shipped from the spike:**

- **F1 fix** — `resident_leaf_bytes` reconciliation at the end of every budget-configured
  checkpoint (`checkpoint_impl_paged`). Pre-fix, node *creation* never credited the counter
  while the demote debit was unconditional, so a store built by writes negative-saturated it;
  the `.max(0)` clamp then reported 0 forever and the `due_mem` trigger + fault-in budget wake
  were structurally dead. Regression test:
  `paged_demotion::refaulted_leaves_raise_resident_estimate_after_full_demotion` (verified to
  fail with the fix reverted).
- **Deployment guidance** (measured, single NVMe box, ordering-grade evidence):
  - **mimalloc** as the global allocator for paged deployments: +29% (T=32) to **2.1×** (T=8)
    on pressured writes, consistent with task57's eventual-tier findings.
  - **`fanout-t8`** for write-heavy paged deployments: 3.2× alone (leaf-granular residency —
    scrambled-zipf pins ~1 hot key per leaf, so T=32 leaves overcommit any budget).
  - **Ingest-then-serve reopen**: after a large insert-load, drop + `recover()` (~2-5 s at 5M
    rows) rebuilds a compact heap: 3.8-4.3× on the subsequent workload. Combined stack
    (t8 + mimalloc + reopen): 24.9k ops/s on pressured zipf A — above ReDB's 18.8k from the
    same matrix at a tighter budget.

**Deferred to the stage-3 spec** (with in-place eviction / pinning / `Result` reads):

- **Leaf-arena allocation** — allocate a leaf's node + values from one arena so demotion frees
  whole pages: kills F6 structurally (no reopen needed), and makes `memory_budget_bytes`
  count real bytes (today `NODE_BYTES` excludes the per-entry `Arc<V>` value allocations, so
  the budget under-counts true leaf cost). Bends the value-sharing contract (`Arc<V>` across
  leaf CoW generations) for paged tables — needs its own design pass.
- **Demote pressure override** — second-chance alone reclaims ~nothing from a continuously
  touched leaf set (forced 1-5 s checkpoints demoted 0-41 of ~80k leaves); under sustained
  over-budget pressure the pass needs an eviction mode that ignores the accessed bit.
- **Open observation**: one ≥4.29 s op (u32-saturated `max_us`) per pressured write cell,
  present with zero checkpointer runs — suspect first-gc drop of load-era memory under swap or
  cgroup direct-reclaim; also raise the harness latency counter above u32 ns.
