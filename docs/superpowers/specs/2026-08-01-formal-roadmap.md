# UltimaDB — formal verification roadmap (post-B-tree)

**Date:** 2026-08-01
**Status:** RECOMMENDATION — not started. Dispatch brief for future sessions;
each numbered task below is self-contained enough to pick up cold.
**Origin:** a cross-repo assessment of the formal efforts (this repo's
`formal/` arc vs `ultima_cluster`'s Lean + Veil arcs). Verdict on the existing
work: the B-tree verification (`formal/`) is **complete for its scoped
surface** (insert/remove/get functional correctness at T=32, sorry-free,
drift-guarded in CI) and costs ~nothing to maintain. This doc records what is
*worth proving next*, ranked, with the context a fresh session needs.

**Ranking lens** (derived from the cluster arcs' measured outcomes): bug yield
comes from **code-tight models of new, concurrent, or crash-facing surfaces**.
Four real shipped bugs came from theorem-proving over faithful models; zero
came from abstraction-first bounded checking of already-fixed surfaces. Prefer
the siege engine (Lean/Aeneas) for lasting guarantees, a scout to find shallow
counterexamples first.

**Tool portfolio (decided 2026-08-01, shared with ultima_cluster — full
rationale in `../ultima_cluster/docs/superpowers/specs/2026-08-01-uc2-formal-roadmap.md`
§"Tool portfolio"):** deliberately minimal — **Lean (incl. Aeneas)** as the
prover and sole record, **TLA+/TLC** as the ONE model-checking scout
(Veil is retired permanently; Specula is at most a gated harness over the
TLA+ slot), and **loom** for weak-memory interleavings. kani is dropped
(covered by loom + proptest/differential habits). A tool's real cost is the
standing overhead — toolchain rot, model drift, DSL tuition — so new tools
enter only through a gated spike with an exit-cheap clause.

---

## Ground state (verified 2026-08-01)

- `formal/` proves: `BTree.{insert,remove}_{inv,get,frame}`, `remove_total`,
  `remove_spec`, `remove_minkeys`, `remove_balanced_spec` over an
  Aeneas-translated kernel (`formal/kernel/` → `formal/proofs/BtreeKernel.lean`,
  generated, never hand-edit). All sorry-free, axioms = standard trio only.
  See `formal/README.md` (authoritative: toolchain pins, regen commands,
  ground rules) and `formal/WRITEUP.md` (methodology narrative).
- Enforcement: `make formal/drift-check` on every PR
  (`.github/workflows/formal.yml`); weekly `lean` CI job rebuilds proofs.
- Documented gaps (from `formal/README.md`): range iterators; T-parametric
  fanout (only T=32 instantiated; T=8 `fanout-t8` is unverified — follow-up
  tracked in `docs/tasks/task52_btree_fixedvec_fanout.md`); FixedVec
  representation layer (kernel models nodes as `Vec`); everything
  concurrent/IO (store, OCC, WAL — **out of Aeneas scope by design**).

---

## Task F-DB-1 — `PrimaryKey` order-preserving encoding (Aeneas + Lean)

**Priority: HIGH — best assurance-per-effort in the repo. Estimated ~1 session**
given the existing `formal/` infrastructure.

**Why.** `src/primary_key.rs` (~780 lines, pure safe Rust — ideal Aeneas
shape) implements order-preserving `encode`/`decode`: bytewise order of the
encoding must equal `Ord` order, and tuple framing relies on fixed-width
elements being self-delimiting while variable-length elements (`String`,
`Vec<u8>`) are escaped (`0x00 → 0x00,0xFF`) and terminated (`0x00,0x01`) —
length prefixes are deliberately NOT used because they don't preserve order.
Three load-bearing consumers silently corrupt if this is wrong:

1. WAL replay ordering (recovery applies ops keyed by encoded order),
2. `BTree::from_sorted` (assumes input sorted by `Ord` == encoded order),
3. secondary-index range scans (`BTree::range_prefix` over `(IK, K)`
   composite keys).

Escape-sequence comparators are a classic off-by-one-byte bug class, and the
tuple/escaping interaction (does an escaped `0x00` inside the first element
compare correctly against a terminator in a shorter key?) is exactly the kind
of corner a differential test samples but a proof covers.

**Approach.**
- Port `encode`/`decode` for the interesting impls (`u64`/`i64` as the
  fixed-width representatives with the sign-bit flip, `String`/`Vec<u8>` for
  escaping, and the 2-/3-tuple combinators) into a new Aeneas kernel crate,
  same pattern as `formal/kernel/` (excluded from workspace, Charon owns the
  build, differential test against the real impl as the behavioral anchor).
- Theorems: `encode_strict_mono : a < b ↔ encode a <lex encode b` (the ↔
  matters — antisymmetry gives injectivity for free) and
  `decode_encode : decode (encode a) = ok a`. For tuples, prove the framing
  lemma first: no encoding of one element is a strict prefix of another's
  in a way that breaks lexicographic comparison at the tuple boundary
  (this is where the self-delimiting/terminator argument lives).
- Follow `formal/README.md` toolchain pins EXACTLY (Charon must come from the
  Aeneas `charon-pin`; same-day nightlies mismatch). Zero `axiom`s in the
  generated Lean (an axiom = an unmodeled std function leaked in).
- Extend the drift guard: changes to `src/primary_key.rs` must trip
  `make formal/drift-check` the same way `src/btree.rs` changes do.

**Exit criteria.** Sorry-free `encode_strict_mono` + `decode_encode` for the
listed impls; `#print axioms` = standard trio; drift guard extended; README
table updated. **Non-goal:** `hash64` (an OCC digest — collisions are
tolerated by design, nothing to prove).

**Reference for context:** `docs/tasks/task56_arbitrary_primary_keys.md`.

---

## Task F-DB-2 — WAL / recovery crash-safety protocol model

**Priority: HIGH — the biggest uncovered data-loss surface in the repo.**
This is the db's equivalent of consensus safety: the failure class is
*acked-write loss / un-acked-write visibility*, the same class as the
cluster's Findings #5/#6b. Multi-session; treat like the cluster's Phase-2
protocol arc, with a re-gate after the model lands.

**Why.** ~4,600 lines across `src/wal.rs` (3,618), `src/checkpoint.rs` (786),
`src/persistence.rs` (193) implement the three-phase Consistent pipeline
(`docs/tasks/task15_three_phase_consistent_persistence.md`), including:

- commit → WAL-submit → fsync-park → **FIFO `PromoteGate`** snapshot
  promotion, with the invariant that a parked commit can never be erased by a
  later commit forking past it;
- version finalization under the commit lock (auto versions bumped past
  `max(latest_version, last_submitted_version)` → unique + strictly monotonic
  in submission order, even with parked commits);
- three `WalWrite` modes — the `CoalescedPrealloc` tail-tolerant recovery scan
  (`docs/tasks/task37_wal_preallocation.md`) is the subtlest: recovery must
  distinguish a torn tail from pre-zeroed space;
- checkpoint/prune interaction: prune runs on the WAL bg thread between
  batches via tmp+rename; `cleanup_old_checkpoints` must never delete
  checkpoints newer than the one being kept;
- `WalOp::BulkLoad` markers making recovery fail cleanly
  (`BulkLoadNotCheckpointed`) if commits follow an un-checkpointed load.

Empirical coverage exists (`tests/corruption_recovery.rs`,
`tests/persistence_integration.rs`, `tests/wal_preallocation.rs`, autobench
torture floors) but **crash-at-every-intermediate-point is exactly what
testing enumerates poorly and a model enumerates exhaustively**. Note: the
box's tmpfs `/tmp` makes real-disk durability testing awkward (see the
repo-wide no-/tmp rule); a model needs no disk at all.

**Approach — scout first, then prove (this ordering is a hard lesson from the
cluster's Veil arc; read `../ultima_cluster/proofs-veil/spike-ledger.md`
"toolchain lessons" sections before building any explicit-state model).**

1. **Model:** state = (durable WAL byte-prefix, checkpoint set, parked-commit
   FIFO, promoted snapshot chain, acked set). Steps = submit / write /
   fsync-ack / promote / checkpoint / prune / **crash** (truncate WAL to any
   prefix ≤ durable, drop all volatile state) / recover. Model each of the
   three `WalWrite` modes' durability step distinctly — `CoalescedPrealloc`'s
   crash step must allow a torn batch *and* stale pre-zeroed space after it.
2. **Scout (recommended): TLA+/TLC** — the portfolio's designated checker,
   and this is its home turf (fsync/crash/recover state machines are the
   canonical industrial TLA+ use). Expect a 300–500-line spec with
   crash-at-any-prefix as a one-line disjunct; TLC runs in minutes at this
   state-space size. Hunt the two safety properties below at small bounds
   before proving anything. Adjudicate every counterexample against the
   Rust before "fixing" the model — the cluster arc's counterexamples were
   model artifacts ~11 times before they were ever real, and each
   adjudication is itself a fidelity audit of the Rust. The debugged TLA+
   model is then the porting source for step 3's Lean model.
3. **Prove (Lean, hand model — NOT Aeneas; concurrency + IO are out of its
   scope):** `recovery_sound` — after any crash, recovery yields exactly a
   prefix of submission order containing every acked commit
   (`Consistent`/`ConsistentInline` ack ⇒ survives) and no partially-applied
   commit; `promotion_faithful` — the promoted snapshot chain equals WAL
   submission order (the PromoteGate FIFO invariant); plus `Eventual`-mode's
   weaker contract stated honestly (acked-but-lost is PERMITTED, prefix
   property still required).

**Exit criteria.** The two theorems sorry-free over a model whose fidelity is
anchored by a conformance-style differential test (drive the real store with
a scripted crash harness — `kill`points or a fault-injecting `File` shim —
and diff surviving-state against the model's prediction; the cluster's
`Conform/` rig in `../ultima_cluster/proofs/` is the pattern). A gate doc
recording scope, abstraction obligations, and honest boundaries.

**Pitfalls.** (1) Don't model fsync as atomic-per-file if the code relies on
`fdatasync` semantics — the prealloc mode's whole point is metadata-free
syncs. (2) The WAL bg thread's prune-between-batches is a concurrency seam;
model it as an interleaved step, not a sub-step of checkpoint. (3) Keep the
model on the *protocol* altitude — bytes-in-files is too low, "the WAL is a
list of ops" is too high to see torn tails.

---

## Task F-DB-3 — complete the B-tree verified surface

**Priority: MEDIUM — background-quality work inside existing machinery.**
Three independent sub-tasks, each picks up the `formal/` framework as-is:

- **(a) `from_sorted` / `BulkBuilder`.** Prove: sorted-strictly-ascending
  input ⇒ output tree satisfies the full invariant (NodeInv ∧ Aligned ∧
  HeightInv ∧ MIN_KEYS where applicable) and `get k = some v` iff `(k,v)` in
  input. Consumers: `Store::bulk_load`, index `rebuild_from_sorted_data`,
  recovery. The right-spine-seeded incremental `BulkBuilder` (task51) is the
  interesting half. Port to the kernel first, differential-test, regenerate.
- **(b) Range iterators.** `range`/`range_prefix` return exactly the in-order
  keys in bounds. The `RemoveFlatten.lean` in-order flatten characterization
  is the natural foundation — an iterator theorem is "iterator output =
  filtered flatten", plus `range_prefix`'s no-min/max-needed trick over
  composite keys (which also backstops F-DB-1's consumer #3).
- **(c) T-parametric development.** Generalize the instantiation to
  `2 ≤ T ≤ 127`, closing the task52 gap (T=8 currently unverified). Per
  `formal/README.md` this is mostly by-role-constant discipline; expect
  arithmetic-lemma churn in `MinKeysPreserve.lean` (the "arithmetic-heavy
  direction" per the README).
- **(d, small) FixedVec side-proof.** The kernel models node storage as `Vec`;
  the documented representation gap (slot bookkeeping, `u8` length) is
  covered only by unit tests + a compile-time `T ≤ 127` guard. A standalone
  Aeneas proof that `FixedVec`'s ops are Vec-equivalent on the initialized
  prefix (kani is out per the portfolio decision; if a proof is too costly,
  exhaustive small-capacity differential tests are the fallback). Discharges
  the last caveat in the README's task52 note.

---

## Deprioritized — recorded so the reasoning isn't re-litigated

- **MultiWriter OCC / SI / SSI commit protocol.** Tempting (one nice theorem:
  `hash64`-digest conflict detection produces spurious conflicts, never
  missed ones), but the Elle harness (`make consistency/elle`, task45) with
  mutation testing (task47) already gives this surface strong empirical
  coverage *with proven teeth* — the rare place where the testing story is
  genuinely good. Revisit only after F-DB-2, whose store-model substrate it
  would share.
- **HNSW / `ultima_vector`.** Approximate by contract; no crisp spec to
  prove. Differential + property testing is the right tool.
- **SIMD distance kernels.** Differential testing against the scalar path
  covers it; formalizing float SIMD is poor ROI.
- **`snapshot_stream` wire format.** A round-trip proof is possible but the
  format is stable and shared with the cluster; do it only if a format
  change is ever proposed.

---

## Standing constraints (apply to every task above)

- **Durability tests never under `/tmp`** — it's RAM tmpfs on the dev box;
  use real disk (`/home/claude/...`) or, better, prove at the model level.
- Aeneas/Charon/Lean pins: follow `formal/README.md` §Toolchain pins exactly;
  never downgrade the repo's Lean pin to chase a research tool (precedent:
  the cluster's Phase-1.5 exit).
- Every kernel change: differential test green BEFORE regenerating; zero
  `axiom`s in generated Lean; no `sorry` committed; `#print axioms` = trio.
- Finished work consolidates into `docs/tasks/taskXX_*.md` per the repo's
  Feature Development Workflow; this roadmap is the picking list, not the
  record.
