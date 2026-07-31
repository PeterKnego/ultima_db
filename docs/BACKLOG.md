# Backlog

Directions and loose ends deliberately set aside, with enough context to pick
them up cold. The canonical per-feature record stays `docs/tasks/taskXX_*.md`;
this file is only for work *not* started.

Last reviewed: 2026-07-30.

## Direction — apply-path performance

Set aside 2026-07-30 in favour of the adoption program
(`docs/superpowers/specs/2026-07-30-adoption-program-design.md`). The campaign
that preceded it is recorded in task52 (FixedVec + fanout), task53
(`VersionPin` + O(evicted) gc), and task54 (multi-table writer).

**Where it stands.** Against a flat/chunked-CoW store on the SMR apply path
(the `hi-perf-cmp` LOB workload), UltimaDB costs ~30–50× batched and ~75–175×
unbatched. That is down from an apparent ~1,000×, ~90% of which turned out to
be a snapshot-retention artifact rather than engine cost.

**Why the residual gap exists,** heaviest term first:

1. **Where the CoW bill lands.** A chunked-CoW store pays copy-on-write per
   *snapshot capture*; UltimaDB pays it per *transaction*, because every commit
   is a snapshot. Node clone cost scales with fanout `T`.
2. **O(log n) descent versus O(1) slot addressing.** The workload's keys are
   dense integers, which a flat store exploits directly; `Table` cannot assume
   density.
3. **Per-value `Arc` allocation** on every insert/update, plus record clones on
   read. Flat stores allocate nothing on the write path.

Term 1 is already largely addressed (`fanout-t8`, batching). Terms 2 and 3 are
irreducible while every commit is a version — attacking them means changing
that premise or specialising around it.

**Candidate attacks:**

- Eliminate the per-value `Arc` for small or `Copy` records (inline storage
  instead of `Arc<V>`).
- A dense-integer-key fast path. **`Table<R, K = u64>` has landed** (0.3.0,
  `docs/tasks/task56_arbitrary_primary_keys.md`), so the specialisation hook
  now exists: `K` is a real type parameter carrying `PrimaryKey`
  (`ENCODED_LEN`, `encode`, `hash64`) plus the `AutoKey` sub-trait that only
  `u64` implements. `AutoKey` is already the gate for `insert` and the
  bulk-append path, so it is the natural bound for a dense-slot layout — an
  `impl<R> Table<R, u64>` block, or a new `DenseKey: AutoKey` marker, can
  carry a slot-addressed representation without touching the generic path.
  Still a performance change, and still unmeasured.
- Deeper batch amortisation beyond the current `insert_batch` bulk-append path.

**Measurement.** Same-host A/B on the `hi-perf-cmp` fleet harness; the sandbox
is direction-only. Recorded lesson: probes that shrink or reorder the config
*overstate* results — only a same-config, same-fleet A/B toggling one variable
is trustworthy. See `docs/benchmarks/` and the bench-infra guardrails in
`CLAUDE.md` (real billable AWS; explicit authorisation, always `make destroy`).

**Explored and rejected** (do not re-attempt without new information): shared
per-node values block (3.1× contended-read regression); background reclaim
thread for snapshot frees (3.1× apply-p99 regression — cross-thread Arc
decrements ping-pong cachelines); commit critical-section shrink and fused
get+replace traversal (both washes).

## Direction — 1.0 hardening

Set aside 2026-07-30. Correctness and robustness work for a stable release.

- **SSI gaps** (task21 "v1 limitations"). `ReadTx` is never validated. Index
  DDL is invisible to SSI/OCC — task41 made a conflicting DDL *fail* rather
  than silently drop, but the backfill read is still not read-set tracked and
  DDL still generates no conflicts for other writers.
- **WAL fault-injection testing.** Recovery is tested for well-formed inputs
  and tail tolerance; there is no systematic torn-write / partial-fsync /
  truncation injection suite.
- **T-parametric formal development.** The Lean kernel is instantiated at T=32
  only, so the `fanout-t8` configuration ships unverified. The durable fix is a
  parametric development (`2 ≤ T ≤ 127`, upper bound from the u8-len guard);
  feasibility gates on charon/aeneas const-generic support, with a second
  concrete instantiation as the fallback. See task52 follow-ups.
- **`extend_from_sorted` underfull bug at T=4.** Latent small-T builder bug;
  produces underfull non-root nodes. Not reachable from any shipped
  configuration (T=8 and T=32 are the only ones), but it is a real bug.
- **Gate B harness rot.** `autobench`'s `run-iter` drives `uc_autobench`, which
  no longer exists in the rewritten `ultima_cluster` (uc2). Local runs gate on
  torture + Gate A only until repaired.
- **CJK tokenization.** task43 made the full-text tokenizer Unicode-aware, but
  CJK still under-matches: no spaces, and ideographs are alphanumeric, so a run
  becomes one token. Needs a unigram/n-gram filter. Also: NFD combining marks
  are treated as boundaries (NFC input recommended).

## Smaller loose ends

- **YCSB workloads D/E** grow the dataset across iterations, so absolute
  numbers drift between runs. Cross-engine comparisons stay fair (all engines
  see the same drift). Fix only if absolute D/E numbers are ever published.
- **`hi-perf-cmp`** (sibling repo): post-restore `UltimaBook::current_version()`
  is 1 where it was previously 2. Matters only if a caller resumes writes into
  a restored store.
- **`hi-perf-cmp`** (sibling repo): no `apply_batch <= warmup` validation, so a
  large-batch sweep silently runs zero warmup batches — fix before any
  `SMRC_APPLY_BATCH` sweep. Empty-book (hwm=0) restore is untested.
