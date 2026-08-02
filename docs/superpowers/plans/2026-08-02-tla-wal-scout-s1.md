# F-DB-2 / S1: TLA+ Model of the WAL Crash-Safety Pipeline + Calibration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a TLA+ model of UltimaDB's Standalone persistence pipeline and prove it discriminating, by showing it re-finds five bugs that actually shipped.

**Architecture:** One spec (`WalCrash.tla`) over the three-phase Consistent commit, the three production sink modes, crash, and recovery — plus per-mode TLC configs and five mutation configs. The model is bounded small (≤4 commits, ≤2 tables, ≤1 crash) and lifted rather than made faithful: fsync is a barrier, tearing is a per-frame flag, rename is atomic, commits are opaque `⟨version, table⟩` pairs.

**Tech Stack:** TLA+ / TLC 2.19 (vendored at `tools/tla/tla2tools-1.7.4.jar`), Java 21.

## Scope

This plan is **S1 of the scout only**: the model and its calibration. The full safety battery across the mode matrix (S1–S5, L1, `BulkLoadGuard`) is S2, and the Lean proof phase is a separate brief written after the scout lands. Brief: `docs/superpowers/specs/2026-08-02-wal-crash-safety-tla-scout-brief.md`.

**A note on sizing, stated plainly.** The brief times S1 at one session. This plan is six tasks with a review gate on each, which is a heavier process than the brief assumed and will take longer than one sitting. That is a deliberate trade — the calibration result is the entire value of the scout, and a mis-calibrated model that produces confident greens is worse than no model. If the timebox matters more than the gating, execute Tasks 1–3 and 6 and treat 4–5 as one combined task.

## Global Constraints

- **`/tmp` is tmpfs on this box** (7.7 GB, RAM-backed, no swap). TLC's `-metadir` and any scratch output go under `$HOME` — `make formal/tla-smoke` already encodes this; follow it.
- **Bound the JVM heap.** `-XX:+UseSerialGC -Xmx2g`, same discipline as the Elle harness.
- **Every green verdict is paired with a vacuity canary that must go red first.** A checker that only reports success is indistinguishable from a broken one. This is the S0 gate's own pattern (`formal/tla/wal/S0Canary.tla`) at full scale.
- **A counterexample is a question, not an answer.** Every violation is adjudicated against the Rust with file:line cites *before* the model is changed. The brief's sibling arc hit 11+ model artifacts before anything real; expect that ratio.
- **Bounds:** ≤ 4 commits, ≤ 2 tables, ≤ 1 checkpoint + 1 prune, ≤ 1 crash per behavior. If TLC exceeds minutes at these bounds, the model is too low-altitude — lift it rather than buying compute.
- **Out of scope, record in the spec header:** SMR mode, MultiWriter OCC validation semantics, random mid-WAL corruption, byte-level framing, snapshot content, bench-only sinks (`BufferedFile`, `Mmap`, `IoUring`), GC.
- Do NOT run `cargo fmt`; never `perl -pi` on non-ASCII.
- Nothing in this plan touches `src/`. If a real finding emerges, it is filed and fixed per repo convention *before* the scout is declared done — but that is a separate change, not part of a task here.

## Ground truth — read before modeling

`docs/tasks/task15_three_phase_consistent_persistence.md` (especially "Three-phase commit protocol", "Why this is safe", "Promotion ordering (lost-update fix)"), `docs/tasks/task37_wal_preallocation.md` (§4 three invariants, §6 prune P2, §7 tail-tolerant recovery), `docs/tasks/task38_wal_inline_fsync.md` (§4, §5), and `CLAUDE.md` §Persistence.

**The load-bearing rule the whole model exists to check**, from task15: *`latest_version` strictly advances at every promotion, and every promotion forks from the latest at promote time.*

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `formal/tla/wal/WalCrash.tla` | The single spec: state, commit phases, sinks, crash, recovery | 1–3 |
| `formal/tla/wal/WalCrash.cfg` | Baseline config (Consistent × FsWrite) | 1 |
| `formal/tla/wal/Vacuity.cfg` | Canary configs asserting reachability negations | 1–3 |
| `formal/tla/wal/modes/*.cfg` | One config per sink × durability combination | 3 |
| `formal/tla/wal/mutations/M1..M5.cfg` | Calibration mutations, each selected by a constant | 4–5 |
| `formal/tla/wal/RESULTS.md` | Scout memo: verdicts, state counts, calibration table, adjudications | 6 |
| `Makefile` | `formal/tla-model`, `formal/tla-calibrate` targets | 1, 4, 6 |

**Mutations are config-selected, not separate spec copies.** A `MUTATION` constant (`"NONE"`, `"M1"`, …) gates the mutated behavior inside the one spec. Five forked `.tla` files would drift from the baseline the moment the baseline changes, and the calibration would silently stop testing the real model.

---

### Task 1: Steady-state commit pipeline, no crash

**Files:**
- Create: `formal/tla/wal/WalCrash.tla`, `formal/tla/wal/WalCrash.cfg`, `formal/tla/wal/Vacuity.cfg`
- Modify: `Makefile`

**Interfaces:**
- Produces: state variables `walDurable`, `walBuffered`, `parked`, `promoted`, `latestVersion`, `lastSubmitted`, `nextVersion`, `acked`, `crashed`; actions `Submit`, `Fsync`, `Promote`; constants `MaxCommits`, `Tables`, `Durability`, `SinkKind`, `WriterMode`, `MUTATION`; invariant `PromotionFaithful`.
- Consumes: nothing.

**What to model.** Three-phase commit with no crash yet. Phase 1 PREPARE: finalize a version (MultiWriter bumps past `max(latestVersion, lastSubmitted)`, allocating from `nextVersion`), append the entry to `walBuffered`, take a `PromoteGate` ticket. Phase 2 SYNC: park until the entry is covered by a durability barrier. Phase 3 PROMOTE: fork from the latest *at promote time*, insert, advance `latestVersion`.

`PromotionFaithful` (the brief's S2): the promoted chain is exactly submission order, `latestVersion` strictly advances per promotion, and no snapshot is ever replaced at the same version.

- [ ] **Step 1: Write the vacuity canary first**

Before the model verifies anything, prove the state space is non-trivial. Create `formal/tla/wal/Vacuity.cfg`:

```
SPECIFICATION Spec
INVARIANT NoCommitEverPromotes
CHECK_DEADLOCK FALSE
CONSTANTS
  MaxCommits = 2
  Tables = {t1, t2}
  Durability = "Consistent"
  SinkKind = "FsWrite"
  WriterMode = "SingleWriter"
  MUTATION = "NONE"
```

with `NoCommitEverPromotes == promoted = <<>>` in the spec. **This invariant must be VIOLATED** — if TLC reports it holds, nothing ever promotes and every later green is vacuous.

- [ ] **Step 2: Run the canary and confirm it goes red**

Run: `cd formal/tla/wal && java -XX:+UseSerialGC -Xmx2g -cp ../../../tools/tla/tla2tools-1.7.4.jar tlc2.TLC -metadir $HOME/tlc-states -workers 2 -config Vacuity.cfg WalCrash.tla`
Expected: `Invariant NoCommitEverPromotes is violated`, with a trace reaching a promotion. If it instead reports success, the model is inert — fix that before writing another line.

- [ ] **Step 3: Add `PromotionFaithful` and verify the baseline**

`WalCrash.cfg` is the same constants with `INVARIANT PromotionFaithful`.

Run the same command with `-config WalCrash.cfg`.
Expected: "Model checking completed. No error has been found." Record the state count — later tasks compare against it, and a sudden collapse in state count is how a model silently stops exploring.

- [ ] **Step 4: Add the Makefile target**

```make
formal/tla-model:
	@mkdir -p $(TLC_METADIR)
	@cd formal/tla/wal && $(TLC) -config Vacuity.cfg WalCrash.tla > /dev/null 2>&1 \
	  && { echo "vacuity canary FAILED — nothing promotes; every green below is meaningless"; exit 1; } \
	  || echo "vacuity canary: violated (expected)"
	@cd formal/tla/wal && $(TLC) -config WalCrash.cfg WalCrash.tla > /dev/null \
	  && echo "WalCrash baseline: no error (expected)" \
	  || { echo "WalCrash baseline FAILED"; exit 1; }
```

Add `formal/tla-model` to `.PHONY`.

- [ ] **Step 5: Run the target and commit**

Run: `make formal/tla-model`
Expected: canary violated, baseline clean.

```bash
git add formal/tla/wal Makefile
git commit -m "formal(tla): steady-state commit pipeline + promotion invariant

Three-phase commit with no crash yet. The vacuity canary runs first and must
go red: a model where nothing promotes verifies everything."
```

---

### Task 2: Crash and recovery

**Files:**
- Modify: `formal/tla/wal/WalCrash.tla`, `formal/tla/wal/Vacuity.cfg`

**Interfaces:**
- Consumes: Task 1's state and actions.
- Produces: new state variables `crashed`, `recovered`, `checkpointVersion`, `walAfterCrash` (the post-crash log the recovery scan reads); actions `Crash`, `Recover`; helpers `SurvivingFrames`, `Replay`, `TailTolerant`; invariant `RecoverySound`; canary `NoCrashThenRecover`. (Task 1 declares `crashed` as a flag; this task adds `recovered` alongside it so the two-phase crash→recover sequence is expressible, and `checkpointVersion` so replay has a floor.)

**Crash semantics — the part worth being careful about.** On `Crash`: volatile state resets; every frame in `walBuffered` independently becomes absent, **torn** (present-but-CRC-bad — a flag, not bytes), or present; frames in `walDurable` survive. Recovery is `find_latest_checkpoint` → `load_checkpoint` → `scan_wal` → replay entries with version > checkpoint version.

`RecoverySound` (the brief's S1): after any crash+recover, the recovered state is the replay of a **prefix of submission order**; it contains every `Consistent`/`ConsistentInline`-acked commit; it contains no partial commit; recovered versions are strictly monotone.

- [ ] **Step 1: Add the crash-reachability canary first**

Add `NoCrashThenRecover == ~(crashed /\ recovered)` and a `Vacuity.cfg` variant selecting it. **Must be VIOLATED** — otherwise the crash path is unreachable and `RecoverySound` is checked over zero crash behaviors, which is precisely the shape of a meaningless green.

- [ ] **Step 2: Run it and confirm red**

Expected: violated, with a trace containing a crash and a recovery.

- [ ] **Step 3: Model `Crash` and `Recover`**

Per the semantics above. Keep tearing a per-frame nondeterministic choice — three-way (absent / torn / present) for buffered frames, survive for durable ones. The shape:

```tla
\* Each buffered frame independently survives, tears, or vanishes.
\* Durable frames always survive. Volatile state resets.
Crash ==
  /\ ~crashed
  /\ \E outcome \in [1..Len(walBuffered) -> {"absent", "torn", "present"}] :
       walAfterCrash' = walDurable \o SurvivingFrames(walBuffered, outcome)
  /\ crashed'  = TRUE
  /\ parked'   = <<>>          \* parked commits are volatile — their acks are not
  /\ UNCHANGED <<acked, checkpointVersion>>

\* Recovery replays from the checkpoint floor, stopping where scan_wal stops.
Recover ==
  /\ crashed /\ ~recovered
  /\ recovered'  = TRUE
  /\ promoted'   = Replay(walAfterCrash, checkpointVersion, TailTolerant(SinkKind))
  /\ UNCHANGED <<crashed, acked>>
```

`TailTolerant(SinkKind) == SinkKind = "CoalescedPrealloc"` mirrors `Store::recover`, which passes `tail_tolerant = true` only for that sink. `Replay` stops at the first torn frame under tail-tolerant mode and yields an error state under strict mode — that asymmetry is what M4 attacks in Task 5.

Note `acked` deliberately survives the crash: it records what the *caller* was told, and the whole point of `RecoverySound` is comparing that against what actually came back.

- [ ] **Step 4: Add `RecoverySound` and verify**

Run the baseline config with `INVARIANT PromotionFaithful RecoverySound`.
Expected: clean. **If it violates, do not change the model yet** — adjudicate against `src/store.rs`'s `recover` and `src/wal.rs`'s `scan_wal` first, and record the adjudication. A violation here is either a real finding or a model artifact, and telling them apart is the work.

- [ ] **Step 5: Confirm the state count grew**

Compare against Task 1's recorded count. Crash and recovery should multiply the state space substantially. A count that barely moved means the crash action is under-enabled.

- [ ] **Step 6: Commit**

```bash
git add formal/tla/wal
git commit -m "formal(tla): crash and recovery; RecoverySound

Per-frame nondeterministic tearing; recovery as replay of a submission-order
prefix. The crash-reachability canary gates it — RecoverySound over zero
crash behaviors would verify trivially."
```

---

### Task 3: The sink modes and the durability matrix

**Files:**
- Modify: `formal/tla/wal/WalCrash.tla`
- Create: `formal/tla/wal/modes/*.cfg` (one per combination)

**Interfaces:**
- Consumes: Tasks 1–2.
- Produces: sink behavior for `FsWrite`, `Coalesced`, `CoalescedPrealloc`; prealloc state `writeHead`, `capacity`, `metaDurable`; new constant `ChunkSize` (the prealloc extend granularity — 2 is sufficient at these bounds); actions `SyncData`, `SyncAll`, `Extend`; invariants `EventualHonest` and `PreallocInvariant` (task37 §4's three invariants, stated separately so a model bug surfaces as an invariant break rather than a confusing downstream trace).

**The distinction that matters.** Model `sync_data` as a barrier over frame *data* only and `sync_all` as data + metadata. That difference **is** the prealloc extend bug surface (M5). For `CoalescedPrealloc`: steady-state uses `sync_data`; extend zero-fills by chunks and must `sync_all` *before use*; on crash with `¬metaDurable`, bytes beyond the last `sync_all`-covered size may vanish wholesale — frames written after an un-synced extend are lost even if "written".

`EventualHonest` (the brief's S4): under `Eventual`, acked-lost is *permitted*, but the recovered state is still a submission-order prefix — no reordering, no partial commit.

- [ ] **Step 1: Add the prealloc-extend reachability canary**

`NoExtendBeforeSync == ~(writeHead > capacity)` — or whichever shape makes "an extend happened" reachable in your encoding. **Must be violated**, or M5 has nothing to bite on in Task 5.

- [ ] **Step 2: Run it and confirm red**

- [ ] **Step 3: Model the three sinks and the `sync_data`/`sync_all` split**

The distinction to encode:

```tla
\* sync_data covers frame DATA already written within the durable size.
\* sync_all covers data AND metadata (the physical file size).
SyncData ==
  /\ walDurable' = walDurable \o walBuffered
  /\ walBuffered' = <<>>
  /\ UNCHANGED metaDurable          \* an extend is still unsynced

SyncAll ==
  /\ walDurable' = walDurable \o walBuffered
  /\ walBuffered' = <<>>
  /\ metaDurable' = TRUE            \* physical size is now covered

\* Prealloc: extending past capacity zero-fills, and must SyncAll before use.
Extend ==
  /\ SinkKind = "CoalescedPrealloc"
  /\ writeHead + 1 > capacity
  /\ capacity'    = capacity + ChunkSize
  /\ metaDurable' = FALSE           \* until a SyncAll covers it
```

The crash rule that makes M5 bite: when `¬metaDurable`, frames written beyond the last `sync_all`-covered size vanish **wholesale** on crash, even if a `sync_data` covered their bytes. Task37 §4's three invariants (`write_head ≤ capacity ≤ physical_len`; records in `[0, write_head)`; durable zeros in `[write_head, capacity)`) are worth stating as a separate `PreallocInvariant` so a model bug shows up as an invariant break rather than a confusing downstream trace.

- [ ] **Step 4: Write one config per combination**

`Consistent × {FsWrite, Coalesced, CoalescedPrealloc}`, `ConsistentInline × {FsWrite, CoalescedPrealloc}` (SingleWriter only — `Store::new` rejects `ConsistentInline` under MultiWriter), `Eventual × FsWrite`. Symmetry on table identity.

- [ ] **Step 5: Run every config; record state counts per config**

Expected: all clean. Any violation is adjudicated against the Rust before the model changes — cite `src/wal.rs` lines for the sink behavior you believe is being contradicted.

- [ ] **Step 6: Commit**

```bash
git add formal/tla/wal
git commit -m "formal(tla): three sink modes; sync_data vs sync_all split

The barrier distinction is the prealloc extend bug surface, so it is modeled
explicitly rather than collapsed into one fsync action."
```

---

### Task 4: Calibration M1–M3 — the task15 interleavings

**Files:**
- Modify: `formal/tla/wal/WalCrash.tla`
- Create: `formal/tla/wal/mutations/M1.cfg`, `M2.cfg`, `M3.cfg`

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces: `MUTATION`-gated behavior for M1–M3.

**These three are not hypothetical.** `docs/tasks/task15_three_phase_consistent_persistence.md:81-101` documents them as *reproducible failure modes that shipped*, from an implementation that assembled the snapshot and decremented `active_writer_count` in phase 1, before the fsync wait:

1. **M1 — early slot release.** `begin_write` gated on `active_writer_count`, already decremented, so a second writer was admitted, forked from a latest lacking the parked commit, and whichever promoted at the higher version silently erased the other. Both `commit()` calls returned `Ok`; the WAL held both, so recovery diverged from live state. **Must violate `PromotionFaithful` or `RecoverySound`.**
2. **M2 — no promotion gate.** Promote in completion order rather than ticket order. Per-table locks don't overlap for disjoint tables, so writer B's fork ran while A's snapshot was unpromoted, and B's higher-versioned snapshot lacked A's table wholesale. **Must violate.**
3. **M3 — version bump against `latestVersion` only**, not `max(latestVersion, lastSubmitted)`. `latestVersion` lags while commits are parked, so two writers get the same version and the second insert silently replaces the first. **Must violate.**

- [ ] **Step 1: Gate M1 behind the `MUTATION` constant and run it**

Expected: **violated.** Read the counterexample trace and confirm it matches the documented mechanism — a second writer admitted while the first is parked, forking from a stale latest. A violation for a *different* reason means the mutation isn't reproducing the historical bug and the calibration doesn't count.

- [ ] **Step 2: Record the trace summary in your report**

Which invariant, at what depth, and the mechanism in one sentence.

- [ ] **Step 3: Repeat for M2**

Expected: violated, trace showing disjoint-table commits erasing each other.

- [ ] **Step 4: Repeat for M3**

Expected: violated, trace showing two commits at the same version.

- [ ] **Step 5: Re-run the unmutated baseline**

Expected: still clean. A mutation left accidentally enabled would make every later result meaningless.

- [ ] **Step 6: Commit**

```bash
git add formal/tla/wal
git commit -m "formal(tla): calibration M1-M3 (the task15 lost-update interleavings)

Each mutation reproduces a failure mode that actually shipped and must
produce a violation. Mutations are constant-gated inside the one spec so
they cannot drift from the baseline."
```

---

### Task 5: Calibration M4–M5 — the task37 prealloc subtleties

**Files:**
- Modify: `formal/tla/wal/WalCrash.tla`
- Create: `formal/tla/wal/mutations/M4.cfg`, `M5.cfg`

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces: `MUTATION`-gated behavior for M4–M5.

4. **M4 — strict scan on prealloc.** `tail_tolerant = false` with `CoalescedPrealloc`. Expected: a *legal* torn tail aborts recovery — task37 §7's exact subtlety. **Must violate `TailTolerance` or the liveness property.**
5. **M5 — extend without `sync_all`.** Write frames beyond old capacity with only `sync_data`. Expected: a crash loses frames the system considered durable. **Must violate `RecoverySound`.**

- [ ] **Step 1: Add `TailTolerance` (the brief's S5)**

Under `CoalescedPrealloc`, a torn tail never aborts recovery — it stops at the last good frame. Note the brief's caution: under *strict* sinks, check what the model says and adjudicate against `scan_wal` rather than assuming. If strict mode can see a torn *tail* (not just mid-WAL corruption) after a legal crash, that is a finding to adjudicate, not a spec bug to paper over.

- [ ] **Step 2: Gate M4 and run it**

Expected: violated.

- [ ] **Step 3: Gate M5 and run it**

Expected: violated, with a trace where a frame written after an un-synced extend is lost across a crash despite being treated as durable.

- [ ] **Step 4: Re-run the unmutated baseline across every mode config**

Expected: all clean.

- [ ] **Step 5: Count how many of M1–M5 violated**

**This is the S1 re-gate.** If two or more mutations fail to violate, the model is not discriminating: **stop, and report NOT-DISCRIMINATING with the stuck point named.** Do not proceed to the S2 battery — a hunt over a non-discriminating model produces confident greens that mean nothing, which is worse than no model at all. If exactly one fails, report it, adjudicate why, and let the plan owner decide.

- [ ] **Step 6: Commit**

```bash
git add formal/tla/wal
git commit -m "formal(tla): calibration M4-M5 (prealloc tail tolerance, unsynced extend)

Completes the calibration battery. The S1 re-gate is counted here: fewer
than four of five mutations violating means the model is not discriminating
and the scout stops rather than producing meaningless greens."
```

---

### Task 5b: M6 — calibrate `RecoverySound` clause (c)

**Inserted 2026-08-02, after the S1 re-gate was counted and reported.** The re-gate came back 5 of 5 with matching mechanisms. In counting it, Task 5 found that `RecoverySound`'s clause (c) — the recovered state replays no torn frame — is **uncalibrated**: none of M1–M5 exercises it, so its greens rest on nothing. Task 5 deliberately did *not* close it, because adding a sixth mutation unilaterally would have changed what "5 of 5" meant after the count had already gone to the plan owner. Peter has now authorised closing it. **The re-gate is re-counted as M1–M6.**

**Files:**
- Modify: `formal/tla/wal/WalCrash.tla` (a `MUTATION = "M6"` arm on `ScanLen`, around `:654-667`)
- Create: `formal/tla/wal/mutations/M6.cfg`
- Modify: `Makefile` (`TLA_MODES` entry), `formal/tla/wal/README.md`

**Interfaces:**
- Consumes: everything from Tasks 1–5.
- Produces: `MUTATION = "M6"` gating a tolerant `ScanLen` that accepts non-`absent` frames — i.e. a scan that keeps going *past* a torn frame instead of stopping at it.

**The mechanism.** Real `scan_wal` (`src/wal.rs:574-607`) is a sequential offset walk that stops at the first frame it cannot accept. Clause (c) asserts recovery therefore never replays a torn frame. M6 removes that stop, so a torn frame is replayed as though good — the corruption-passes-CRC failure `scan_wal`'s `break`/`return Err` exists to prevent.

**The price is already measured**, so this is a confirmation rather than a search: a clause-(c) mutation is red on `RecoverySound` at **depth 9, 461/188 states on `modes/ConsistentPrealloc.cfg`** (905/368 on `WalCrashPrealloc.cfg`, which differs only by `SYMMETRY`), about one second. If your numbers differ materially, say so rather than adjusting to match.

**Held to Task 4's standard: a violation is not enough.** The counterexample must show the documented mechanism — a `torn` frame appearing in the recovered `promoted` chain. A red for any other reason does not count, and must be reported as such rather than credited.

- [ ] **Step 1: Add the `M6` arm and its config**

Gate the tolerant `ScanLen` behind `MUTATION = "M6"` so it is an identity at every other value. Point `mutations/M6.cfg` at `RecoverySound`, on the prealloc shape at `MaxCommits = 2`.

- [ ] **Step 2: Run it and read the trace**

Expected: `RecoverySound` violated, exit 12, depth 9. Confirm the recovered `promoted` chain contains a frame whose `walAfterCrash` entry is `torn` — that is the mechanism. Record the state count.

- [ ] **Step 3: Confirm the same-bound control is clean**

The matching `MUTATION = "NONE"` config must come back exit 0, so the red is attributable to M6 alone.

- [ ] **Step 4: Confirm M6 is inert everywhere else**

Run the four baselines with `MUTATION = "M6"` unset and confirm they are bit-identical: SingleWriter 147/depth 11, MultiWriter 651/depth 10, CoalescedPrealloc 559/depth 11, ConsistentPrealloc3 14,934/depth 14.

- [ ] **Step 5: Re-count the re-gate as M1–M6**

State the tally with the matching-vs-merely-violating split, exactly as Task 5 did. **Do not adjust any earlier classification** — M1 and M4 match on their primary config; M2, M3 and M5 match only on clause-focused secondaries.

- [ ] **Step 6: Wire it into the gate and commit**

`TLA_MODES` entry with the exact expected exit code, README's calibration table updated, and the "Not yet done" paragraph that priced this follow-up replaced with the result.

```bash
git add formal/tla/wal Makefile
git commit -m "formal(tla): M6 — calibrate RecoverySound clause (c)

Clause (c) had no mutation exercising it, so its greens rested on nothing.
M6 removes scan_wal's stop-at-first-bad-frame, replaying a torn frame as
though good. Re-gate re-counted as M1-M6."
```

---

### Task 6: The scout memo

**Files:**
- Create: `formal/tla/wal/RESULTS.md`
- Modify: `formal/tla/wal/README.md`, `Makefile`

**Interfaces:**
- Consumes: everything above.
- Produces: the S1 record, and `make formal/tla-calibrate`.

- [ ] **Step 1: Write `RESULTS.md`**

Per the brief's §7 it must contain: per-config verdicts **with state counts**; the M1–M5 calibration table (mutation → invariant violated → depth → one-sentence mechanism, or "did not violate" with analysis); vacuity-canary results for every canary; and every adjudication made along the way (model artifact vs real finding, with Rust file:line cites).

**Label bounded verdicts as bounded.** This memo is a scout record, not a proof. Consolidation into a `docs/tasks/` doc happens only when F-DB-2 completes, including the Lean phase.

- [ ] **Step 2: Add `make formal/tla-calibrate`**

Runs all five mutations and asserts each violates — the standing guard that the model stays discriminating as it evolves. Same shape as `formal/tla-smoke`'s canary assertion: the target fails if a mutation stops violating.

- [ ] **Step 3: Update `formal/tla/wal/README.md`**

Replace the "Not yet done" section with what S1 delivered, what S2 still owes (the full S1–S5/L1/`BulkLoadGuard` battery across the mode matrix), and the abstraction obligations the model rests on: fsync-as-barrier, frame-granular tearing, atomic rename, the `sync_data`/`sync_all` split, opaque commits.

- [ ] **Step 4: Run every gate**

```bash
make formal/tla-smoke
make formal/tla-model
make formal/tla-calibrate
```

- [ ] **Step 5: Commit**

```bash
git add formal/tla/wal Makefile
git commit -m "formal(tla): S1 scout memo + calibration guard

RESULTS.md records per-config verdicts with state counts, the M1-M5
calibration table, canary results, and every adjudication. Bounded verdicts
are labeled bounded — this is a scout record, not a proof."
```

---

## Exit states

Per the brief's §7, S1 ends in exactly one of:

- **CLEAN-CALIBRATED** — all five mutations violate, baseline green across the mode matrix. Proceed to S2 (the full battery).
- **FINDING** — a real violation survives adjudication against the Rust. File and fix it per repo convention, with a directed regression test, *before* the scout continues.
- **NOT-DISCRIMINATING** — the S1 re-gate failed. Bank it honestly with the stuck point named; the Lean phase then starts from the task15/37/38 docs rather than from a model nobody should trust.

## Risks

- **Adjudication is the real cost, not TLC.** Every counterexample is a question about whether the model or the Rust is wrong, against ~4,600 lines. The sibling arc hit 11+ model artifacts before anything real; budget for that ratio rather than treating the first violation as a finding.
- **A model too low-altitude blows up.** If TLC exceeds minutes at the stated bounds, lift the model — do not raise the bounds or buy compute.
- **The temptation at the re-gate.** If four mutations violate and one doesn't, the pull will be to tweak the model until it does. That is fitting the model to the answer. Adjudicate why it didn't, record it, and let the plan owner rule.
