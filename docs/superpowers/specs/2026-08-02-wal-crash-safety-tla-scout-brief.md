# WAL crash-safety TLA+ scout — dispatch brief (F-DB-2, scout phase)

**Date:** 2026-08-02
**Status:** RECOMMENDATION — not started. Dispatch brief for a future
session; self-contained. Assumes a full checkout.
**Parent task:** F-DB-2 in
`docs/superpowers/specs/2026-08-01-formal-roadmap.md` — this brief is its
**scout phase only**. The Lean proof phase is a separate follow-on brief,
written after this scout lands; the debugged TLA+ spec is its porting source.
**Tool:** TLA+/TLC — the portfolio's ONE model-checking scout (portfolio
decision 2026-08-01, rationale in the cluster roadmap's "Tool portfolio"
section). Veil is retired; do not revive it for this. TLC needs Java, which
is already a dev dependency here (the Elle harness).
**Decision being recommended:** a **time-boxed ~2-session scout** that
models the Standalone persistence pipeline (three-phase Consistent commit,
the WAL sink modes, crash, recovery, checkpoint/prune) in TLA+, calibrates
the model by re-breaking known-fixed bugs, then checks the safety battery
below. Any violation surviving adjudication against the Rust is a real
finding; a clean, calibrated pass de-risks and shapes the Lean proof.

---

## 1. Why this surface, and why a checker first

The failure class is **acked-write loss / phantom durability** — the same
class as the cluster's Findings #5/#6b. ~4,600 lines implement it
(`src/wal.rs` 3,618; `src/checkpoint.rs` 786; `src/persistence.rs` 193; plus
the commit path in `src/store.rs`). Empirical coverage is decent
(`tests/corruption_recovery.rs`, `tests/persistence_integration.rs`,
`tests/wal_preallocation.rs`, `tests/wal_inline_fsync.rs`, autobench torture
floors) but **crash-at-every-intermediate-point is what testing samples and
a checker enumerates**. History says the hazard is real: task15's promotion
rework exists because three reproducible lost-update interleavings shipped
and were found by hand (see §5 — they are this scout's calibration targets).
A model also needs no disk, sidestepping the box's tmpfs constraint on
durability testing.

Crash/fsync/recover state machines are TLA+'s canonical industrial use;
expect a 300–500-line spec and TLC runs in minutes at the bounds in §6.

## 2. Ground truth to model (verified against the checkout 2026-08-02)

Read before modeling: `docs/tasks/task15_three_phase_consistent_persistence.md`
(the design record — especially "Three-phase commit protocol", "Why this is
safe", "Promotion ordering (lost-update fix)"),
`docs/tasks/task37_wal_preallocation.md` (§4 "The three invariants", §6
prune P2, §7 tail-tolerant recovery), `docs/tasks/task38_wal_inline_fsync.md`
(§4 off-lock inline mechanism, §5 recovery), and `CLAUDE.md` §Persistence.

Mechanisms the model must carry:

- **Three-phase commit** (`WriteTx::commit`, task15): Phase 1 PREPARE under
  the write lock (finalize version — MultiWriter bumps past
  `max(latest_version, last_submitted_version)`, allocates from
  `next_version`; submit WAL entry to the bg thread, **no fsync**; record
  OCC write set; take a `PromoteGate` ticket) → Phase 2 SYNC lockless (park
  on `SyncWaiter` until the bg thread fsyncs the entry; MultiWriter also
  waits for ticket turn) → Phase 3 PROMOTE (re-acquire lock, **fork from
  the latest at promote time**, insert snapshot, advance `latest_version`).
  The load-bearing rule: *`latest_version` strictly advances at every
  promotion, and every promotion forks from the latest at promote time.*
  SingleWriter holds the writer slot through the fsync wait; a commit whose
  fsync fails advances the gate WITHOUT promoting. `Eventual` /
  `Persistence::None` collapse phases 2–3 under one continuous lock hold
  (`commit_may_park = false` skips the gate).
- **Durability modes**: `Consistent` (bg-thread fsync, ack after durable),
  `ConsistentInline` (task38: committing thread fsyncs off-lock;
  SingleWriter only), `Eventual` (ack before durable — acked-lost is
  PERMITTED, the prefix property is not).
- **Sink/write modes** (`WalSinkKind`, `src/wal.rs:816`): model the three
  production-relevant ones — `FsWrite` (per-entry write + `sync_all`),
  `Coalesced` (one write per batch + `sync_all`), `CoalescedPrealloc`
  (positioned writes into pre-zeroed `wal.bin`; steady-state `sync_data`;
  extend = zero-fill by chunks + **`sync_all` before use**; task37's three
  invariants: `write_head ≤ capacity ≤ physical_len`, records in
  `[0, write_head)`, durable zeros in `[write_head, capacity)`;
  `write_head` reconstructed by scan on open, never persisted). Bench-only
  sinks (`BufferedFile`, `Mmap`, `IoUring`) are OUT of scope.
- **Recovery** (`Store::recover`, `src/store.rs:974`): `find_latest_checkpoint`
  → `load_checkpoint` → `scan_wal(path, tail_tolerant)` → replay entries
  with version > checkpoint version. `tail_tolerant = true` iff
  `CoalescedPrealloc`: first CRC-bad/undecodable frame = end-of-log, stop
  at last good offset, no error. Strict mode: CRC mismatch =
  `WalCorrupted`. A `WalOp::BulkLoad` marker with commits after it and no
  covering checkpoint must fail recovery cleanly
  (`BulkLoadNotCheckpointed`).
- **Checkpoint + prune**: `checkpoint()` writes a checkpoint then prunes the
  WAL up to its version; `prune_wal` is tmp+rename (P2: tmp pre-zeroed to
  live+chunk) — crash leaves whole-old or whole-new, executed by the WAL bg
  thread between batches (an interleaved step, never racing appends);
  `cleanup_old_checkpoints` never deletes checkpoints newer than the kept
  one.

## 3. The model

**State** (suggested variables; rename freely, keep the altitude):
`walDurable` (sequence of frames known durable), `walBuffered` (frames
written but not yet covered by a durability barrier), `prealloc` state
(`writeHead`, `capacity`, `metaDurable` — whether the current physical size
is `sync_all`-covered), `checkpoints` (set of ⟨version⟩, plus at most one
tmp), `parked` (submitted commits with ticket order), `latestVersion`,
`lastSubmitted`, `nextVersion`, `promoted` (the snapshot chain), `acked`
(per-commit ack status), `crashed/recovered` phase flags, and the config
constants `Durability`, `SinkKind`, `WriterMode`.

**Crash semantics — the part worth being careful about.** On `Crash`:
volatile state resets; every frame in `walBuffered` independently becomes
absent, **torn** (present-but-CRC-bad — model as a flag, don't model bytes),
or present (per-frame nondeterministic choice); frames in `walDurable`
survive. For `CoalescedPrealloc`, additionally: if `¬metaDurable`, bytes
beyond the last `sync_all`-covered size may vanish wholesale (frames written
after an un-synced extend are lost even if "written"). Model `sync_data` as
a barrier over frame *data* only and `sync_all` as data + metadata — the
distinction IS the prealloc extend bug surface. `Rename` (prune, checkpoint
publish) is atomic. Fsync failure is a legal transition (the commit errors,
gate advances without promotion — task15).

**Properties** (initial battery; refine freely, record refinements):
- **S1 `RecoverySound`** — after any crash+recover: recovered state = the
  replay of a *prefix of submission order*; contains every
  `Consistent`/`ConsistentInline`-**acked** commit; contains no partial
  commit; recovered versions strictly monotone.
- **S2 `PromotionFaithful`** — the promoted chain (live, pre-crash) is
  exactly submission order; `latestVersion` strictly advances per promotion;
  no snapshot is ever replaced at the same version.
- **S3 `PruneCheckpointSafe`** — prune never drops a frame with version >
  the kept checkpoint's; a crash mid-prune leaves whole-old or whole-new;
  `cleanup_old_checkpoints` never removes a newer checkpoint; recovery after
  any crash mid-`checkpoint()` still satisfies S1.
- **S4 `EventualHonest`** — under `Eventual`: acked-lost is permitted, but
  the recovered state is STILL a submission-order prefix (no reordering, no
  partial commit).
- **S5 `TailTolerance`** — under `CoalescedPrealloc`, a torn tail never
  aborts recovery (stops at last good frame); under strict sinks a torn
  tail... check what the model says and adjudicate against
  `scan_wal` — if strict mode can see a torn *tail* (not just mid-WAL
  corruption) after a legal crash, that is a finding to adjudicate, not a
  spec bug to paper over.
- **L1 (liveness, cheap in TLA+, first liveness property in this repo)** —
  under fairness: every submitted commit eventually promotes or errors, and
  the store never wedges with the gate held (the failed-fsync
  gate-advance rule is what this checks).
- **`BulkLoadGuard`** — commits after an un-checkpointed `BulkLoad` marker
  ⇒ recovery refuses (models the `BulkLoadNotCheckpointed` contract).

## 4. Non-goals (record them in the spec header)

SMR mode (consensus owns durability); MultiWriter OCC *validation*
semantics (model write-set visibility only as far as S2 needs; Elle covers
OCC); random mid-WAL corruption (crash truncation/tearing only — the CRC
catches mid-WAL corruption and `tests/corruption_recovery.rs` covers it);
byte-level framing; snapshot content (commits are opaque ⟨version, table⟩
pairs); the bench-only sinks; GC.

## 5. Calibration — the gate before any verdict is trusted

The model has teeth only if it can re-find bugs that actually shipped.
task15's "Promotion ordering (lost-update fix)" documents **three real
pre-fix interleavings**; each becomes a model mutation that MUST produce a
violation (S1 or S2), checked one at a time:

1. **M1 — early slot release:** decrement the writer slot / build the
   snapshot in phase 1, pre-fsync (pre-fix SingleWriter). Expected: second
   writer forks past the parked commit; one commit silently erased though
   both acked.
2. **M2 — no promotion gate:** promote in completion order, not ticket
   order (pre-fix MultiWriter). Expected: disjoint-table commits erase each
   other wholesale.
3. **M3 — version bump vs `latest_version` only** (not
   `max(latest, lastSubmitted)`). Expected: duplicate versions; second
   `insert` replaces the first.
4. **M4 — strict scan on prealloc** (`tail_tolerant = false` with
   `CoalescedPrealloc`). Expected: a legal torn tail aborts recovery
   (S5/L1 violation) — this is task37 §7's exact subtlety.
5. **M5 — extend without `sync_all`:** write frames beyond old capacity
   with only `sync_data`. Expected: crash loses "durable" frames (S1).

Plus the standing anti-vacuity discipline (imported verbatim from the Veil
arc, `../ultima_cluster/proofs-veil/spike-ledger.md`, sessions 4–5b):
**every green run is paired with a vacuity canary run first** (e.g. assert
`¬∃ recovered commit` — must be violated; assert `¬∃ crash-then-recover
behavior reaching promotion` — must be violated), and a green verdict is
read only after confirming the run wasn't voided (TLC parse/semantic errors)
and wasn't depth/state-bounded when exhaustiveness is claimed. A CE is a
question, not an answer: **every violation is adjudicated against the Rust
(cite `src/wal.rs`/`src/store.rs` lines) before the model is changed.** The
cluster arc hit 11+ model artifacts before anything real; expect the same
ratio and budget for it.

## 6. Sizing, sequencing, gates

Bounds that cover every calibration bug with room to spare: ≤ 4 commits,
≤ 2 tables, ≤ 1 checkpoint + 1 prune, ≤ 1 crash per behavior, the three
sink kinds × three durability modes as separate TLC configs (symmetry on
table identity). If TLC exceeds minutes at these bounds, the model is too
low-altitude — lift it rather than buying compute.

- **S0 (gate, ≤ 1 hour): toolchain.** `tla2tools.jar` + Java (present via
  Elle), run a trivial spec. **TLC's state/metadata dir and the spec's
  working dir go on real disk (`/home/claude/...`), NEVER `/tmp`** (RAM
  tmpfs, no swap); bound the JVM heap (`-Xmx`, Elle-harness precedent).
  Exit cheap on any wall.
- **S1 (session 1): model + calibration.** Steady-state pipeline, crash,
  recovery; then M1–M5, one at a time, each must violate. **Re-gate here:**
  if ≥ 2 mutations fail to violate, the model is not discriminating — stop
  and reassess rather than proceeding to a hunt that would produce
  meaningless greens.
- **S2 (session 2): the battery.** Full S1–S5 + L1 + BulkLoadGuard across
  the mode matrix, unmutated. Adjudicate any violation against the Rust.
  Close with the results memo.
- **Timebox: 2 sessions + the S0 hour.** A third session only on a
  user-approved re-gate (precedent: the Veil session-1 re-gate pattern).

## 7. Deliverables & exit criteria

- Spec + configs under **`formal/tla/wal/`** (new; sibling of the Lean
  `formal/proofs` tree, outside the cargo workspace) with a README stating
  scope, non-goals, abstraction obligations (fsync-as-barrier,
  frame-granular tearing, atomic rename, `sync_data`/`sync_all` split), and
  how to run each config.
- **`formal/tla/wal/RESULTS.md`** — the scout memo: per-config verdicts
  with state counts, the M1–M5 calibration table, vacuity-canary results,
  and every adjudication (artifact vs real, with Rust line cites). Bounded
  verdicts are labeled as bounded; this memo is a scout record, **not** a
  proof — consolidation into a `docs/tasks/` doc happens only when F-DB-2
  (including the Lean phase) completes.
- Any real finding: filed + fixed per repo convention before the scout is
  declared done, with a directed regression test.
- Exit states: **CLEAN-CALIBRATED** (all mutations violate, battery green →
  proceed to the Lean-proof brief, porting this model), **FINDING** (real
  violation adjudicated → fix first, then re-run), or **NOT-DISCRIMINATING**
  (S1 re-gate failed → banked honestly with the stuck point named; the Lean
  phase then starts from the task15/37/38 docs instead).
