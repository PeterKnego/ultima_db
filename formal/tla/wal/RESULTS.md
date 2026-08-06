# S1 scout memo — TLA+ model of the WAL crash-safety pipeline

**Phase:** F-DB-2, scout phase, stage S1. Tasks 1–6.
**Date:** 2026-08-02.
**Brief:** `docs/superpowers/specs/2026-08-02-wal-crash-safety-tla-scout-brief.md`.
**Plan:** `docs/superpowers/plans/2026-08-02-tla-wal-scout-s1.md`.
**Model:** `WalCrash.tla` + `modes/`, `mutations/`. How to run: `README.md`.

**This is a scout record, not a proof.** Every verdict below is bounded — at
`MaxCommits = 2` for 35 of the 43 configs that bind it and 3 for the other 8,
two tables, at most one crash, and no operation after recovery. Consolidation into a `docs/tasks/` doc happens only
when F-DB-2 completes, including the Lean phase. Nothing here should be cited
as "the WAL is verified".

---

## 1. Verdict

**CLEAN-CALIBRATED, with one honest asterisk on the calibration and two
open conjuncts.** All seven mutations violate; all seven match their
documented mechanism; the baseline battery is green across the mode matrix;
and all 34 gate entries hit their exact expected exit code — which for **24 of
the 34 means expected to *fail*** (13 canaries and 11 mutations, each asserted
at exit 12). Only 10 entries are clean verifications: 9 mode baselines and 1
calibration control. "34/34" is a statement about the gate discriminating, not
about 34 things being proved. §7 has the breakdown.

**The asterisk.** "7 of 7 match" is true but not uniform, and flattening it
to "7/7 clean" would misrepresent it:

- **M1, M4, M6, M7 match on their primary config.** The shallowest
  counterexample TLC reports *is* the documented mechanism. (`M4Abort.cfg` is
  supplementary rather than load-bearing for the match: it pins M4's *harm* —
  the durably-acked commit a strict scan makes unreachable — on top of the
  mechanism `M4.cfg` already shows.)
- **M2, M3, M5 match only on a clause-focused secondary config** —
  `M2Fork.cfg` (depth 11), `M3Dup.cfg` (depth 12), `M5Strand.cfg` (depth 16),
  all three at `MaxCommits = 3`. On their primary configs these three do
  violate, but on something other than the documented symptom: for M2 and M3 a
  shallower *consequence* of the break, and for M5 the *mechanism* rather than
  the *harm*. TLC halts at the first counterexample, and the symptom needs a
  third commit and a deeper trace. This split was audited twice and is
  unchanged.

That distinction is the difference between "the model catches this bug" and
"the model catches this bug *the way the bug actually presented*". Both are
true here, but only because three extra configs were built to make the second
one true, and those three configs are now the single point of failure for
that claim (§7 gates them mechanically).

### Did it earn its keep?

**Qualified yes, and the qualification is where the decision actually lives.**

*What was bought.* Three findings about the shipping system (§3), one of which
is reachable on the `#[default]` `WalWrite` and is now a checked, re-verified
owed property rather than tribal knowledge. A calibrated model that
demonstrably re-finds all **seven injected faults** it was aimed at — of which
three (M1–M3) re-create bugs that actually shipped, two (M4–M5) re-create
documented task37 subtleties, one (M6) removes a protection the scan really
carries, and one (M7) is clause-targeted rather than code-derived (§6). A
precise, written
statement of the five things the durability story assumes about the world
(README, "Abstraction obligations") — which did not exist in any form before.
And a gate that cannot quietly pass: exact exit codes, paired vacuity canaries,
and now a manifest that fails the build if the calibration evidence is deleted
or silently re-bounded.

*What it cost.* Eight tasks (1, 2, 3, 4, 5, 5b, 5c, 6), of which **four —
exactly half — were spent on calibration alone** (Tasks 4, 5, 5b, 5c), and two
of those four (5b, 5c) were inserted mid-phase only because a clause turned out
to be checked-by-everything and falsified-by-nothing *after* it had already
been declared covered. That ratio is the honest headline: **adjudication and
calibration, not model-checking, is where the time goes.** TLC itself runs the
whole battery in under a minute.

*The uncomfortable part.* **No baseline invariant ever went red.** The findings
came from reading the Rust closely enough to model it, not from a
counterexample. A reasonable person can read that two ways — "the pipeline is
in good shape, and the model confirms it" or "we paid for a checker and got
value from the code review it forced" — and both are defensible on this
evidence. What is *not* defensible is citing the greens as assurance without
§2 attached.

*Judgement.* Proceed to S2, but fund it as **an adjudication exercise with a
checker attached**, not as a model-checking exercise. Budget the calibration
cost explicitly — on this evidence it is the majority of the work — and expect
the deliverable to be findings and precise assumptions rather than red
invariants. The item most likely to produce an actual counterexample is the
one S1 could not reach at all: a `Checkpoint` action, with prune and crash
interleaving (§9.2).

### Was there a real finding?

**Yes — three, and none of them came from a TLC counterexample against a
baseline invariant.** They came from adjudicating the model against the Rust
(§3). The baselines themselves never went red. That is worth stating plainly,
because it is the honest shape of this result: *the checker's green was not
the product; the act of writing the model precisely enough to check was.*

---

## 2. What S1 did **not** establish

Read this section before quoting any green above it.

**The bounds are small, and a green at a small bound is not a proof.**
Of the 45 `.cfg` files, 43 bind `MaxCommits` (`S0Smoke.cfg`/`S0Canary.cfg` are
a separate spec and bind none), and of those **35 are at 2 and 8 are at 3**.
The eight, in full: `modes/ConsistentPrealloc3.cfg`, its three canaries
(`ConsistentPrealloc3Canary`, `ConsistentPrealloc3LiveLogCanary`,
`ConsistentPrealloc3ChunkCanary`), `mutations/CalibrationControl3.cfg`, and the
`M2Fork`/`M3Dup`/`M5Strand` trio. **Every other bound in this memo — including
all three committed baselines and every mutation's primary config — is 2.**
Two tables. At most **one** crash per behaviour. **No operation after
recovery** — every steady-state action requires `~crashed` and `Recover`
requires `~recovered`. So a whole class of questions is simply outside the
state space, including *"does a restarted store re-issue versions safely?"*,
*"what happens on crash-during-recovery?"*, and *"crash, recover, crash
again"*. None of these were checked and none should be assumed.

**Two conjuncts are checked by everything and falsified by nothing.**
Calibration here is at **clause** granularity, not conjunct granularity.
`RecoverySound` clause (a) is itself a conjunction of four things; M7
falsifies its `cid` and `tbl` conjuncts and **not** its `ver`-match or its
`Len(promoted) <= Len(submitted)` length bound. Those two may be
unfalsifiable by construction at this bound — the way `TailTolerance` clause
2 demonstrably is — but nobody has done the work to decide which, so they
remain greens with nothing behind them. **A prior round of this work
published a blanket all-clear on clause (a) and had to retract it**; the
README says so in two places on purpose and this memo will not undo it.

**The clause table is not conjunct coverage.** Do not read "all four clauses
of `RecoverySound` are calibrated" as "`RecoverySound` is calibrated".

**Five abstraction obligations are assumed, not modelled** (README,
"Abstraction obligations"): fsync as a total, atomic, honest barrier;
frame-granular, CRC-detectable tearing; atomic durable rename; the real
`sync_data`/`sync_all` size-durability split; and opaque commits. The third
is the weakest — S1 has no `Checkpoint` action, so nothing here would notice
if rename atomicity were false. The fifth bounds what "correct recovery" even
means in this model: **the right commit identities, in the right order, into
the right tables — not the right rows.** A replay applying the correct commit
with the wrong contents is invisible by construction.

**There is no fsync-failure action at all.** task15's "a commit whose fsync
fails advances the gate without promoting" has no home here: a crash destroys
the whole gate, whereas a failed fsync must remove *one* ticket and let the
FIFO proceed. Every bug whose trigger is a barrier that reported success
without providing one is outside this model. L1 (liveness) is inexpressible
until that action exists.

**Checkpoint and prune are absent.** `checkpointVersion` is carried and
honoured as the replay floor (`src/store.rs:1083-1086`), but no action moves
it off 0, so no config exercises a non-zero floor — and checkpoint/prune/crash
interleaving is exactly where this class of bug historically bites.

**Not in CI.** `make formal/tla-model` is not wired into any GitHub workflow.
Today it runs when a human runs it.

**S1 covered one of the brief's five property groups.** S2 still owes the
full S1–S5/L1/`BulkLoadGuard` battery across the mode matrix.

---

## 3. Engineering findings about the real system

These are findings about UltimaDB, not about the model. They are the most
valuable output of the phase.

### F1 — A torn tail costs a strict-scan store its whole log, including durable acked commits

**Reachable on 2 of the 3 `WalWrite` variants — including the `#[default]`
one — under both durable tiers.** Not an exotic corner.

Precisely, because "the default Standalone configuration" is not a thing the
API has: `WalWrite::PerEntry` is genuinely `#[default]`
(`src/persistence.rs:70-75`), but `Durability` derives no `Default` and has no
`impl Default` (`src/persistence.rs:44-46`), and `Persistence::standalone`
takes **both** explicitly (`src/persistence.rs:147-151`). So there is no
single configuration to call "the default". The true scope is wider than that
phrasing anyway: `tail_tolerant` is passed for `CoalescedPrealloc` and nothing
else, so F1 applies to `PerEntry` **and** `Coalesced` — 2 of the 3 `WalWrite`
variants — and the "durable acked" harm lands under either durable tier,
`Consistent` or `ConsistentInline`. The `#[default]` `WalWrite` is one of the
two affected.

`scan_wal` treats a CRC mismatch as end-of-log only when `tail_tolerant`,
which `Store::recover` passes for `CoalescedPrealloc` and nothing else
(`src/store.rs:1073-1078`). Every other sink gets `Err(WalCorrupted)`
(`src/wal.rs:705-707`), which `recover` propagates with `?`
(`src/store.rs:1079`) **before applying any entry** — so frames the scan had
already accepted, at offsets *before* the tear, are discarded with it. A
full-length-but-CRC-bad tail is physically ordinary on an appending sink.

**Status: committed as a checked owed property.**
`StrictScanErrLosesDurableAck` (`StrictScanErr.cfg`) must report **violated**,
depth 9, and `make formal/tla-model` asserts exit 12 for it exactly like a
canary. So "we know about this" is *re-checked every run* rather than
remembered. It is deliberately **not** a `RecoverySound` clause: `recover()`
returned `Err`, so there is no recovered state to predicate over, and folding
it in would convert a safety claim into an availability one.

Nobody has yet decided whether the behaviour should change. **Before deleting
the property, note that `mutations/M4Abort.cfg` uses it as M4's target** —
under M4 a prealloc store inherits this gap, which is how M4's harm gets a
witness at all. Resolving F1 and deleting the property costs M4 that witness.
Re-home the harm first.

### F2 — `preallocate_to` is not idempotent under ENOSPC interruption

> **Adjudicated 2026-08-03** ([#23](https://github.com/PeterKnego/ultima_db/issues/23)): real, **low severity** — a robustness and invariant-integrity gap, not a durability hole. In-process the post-error state is retry-safe (`capacity` not advanced, `buf` not cleared), and across a restart `capacity` is re-derived from `metadata().len()`, so it is self-correcting. What genuinely breaks is task37 §4 invariant 2, leaving safety to rest on the `fdatasync` semantics preallocation exists to be independent of, and costing the metadata-free fsync until the next clean extend. Fixed in `1e5d2b7` by rolling the size back to the last durable `capacity` on a failed extend. The analysis below stands as written — it was accurate about the mechanism.

`preallocate_to` (`src/wal.rs:628-667`) exists to establish one invariant:
*the new size is durable before any record is written into the region*
(task37 §4 invariant 2). It does that by physically zero-filling `[from, to)`
and then `sync_all`. **That invariant does not hold on its own error path.**
If the zero-fill is interrupted — ENOSPC is the realistic trigger — the
`write_all` error propagates out of `preallocate_to` *before* its `sync_all`,
and out of `PreallocFileSink::sync` before `self.capacity = new_cap`
(`1e5d2b7^ src/wal.rs:1130-1136` — cited at the pre-fix revision, because the
described error path no longer exists: the same block is `src/wal.rs:1207-1228`
today and now rolls the size back with `set_len` + `sync_all` before returning
the error). The file is then physically longer, the extension was never
`sync_all`'d, and in-memory `capacity` still holds the old value.

The consequence is on the *next open*, not on the retry:
`PreallocFileSink::open` adopts `capacity` from `metadata().len()`
(`src/wal.rs:1192-1193`) — i.e. it adopts the partially-extended,
never-`sync_all`'d size as though it were durable, and steady-state writes
into it are covered by `sync_data` only. That is precisely the shape M5 was
built to model (task37 §4 invariant 2), reached **without any mutation**.

**This is a code-reading finding, not a TLC counterexample, and the
distinction matters.** The model has no partial-extend action and no ENOSPC:
`Extend` either happens or does not. TLC neither found this nor could have.
It surfaced because writing `SlotSafe` and `syncedCapacity` forced the
question "what exactly makes a size durable, and when is it not?" to be
answered precisely.

*This paragraph read "**Unadjudicated as to severity** — no fix is proposed
here and none should be inferred" when written, which was true then. It was
adjudicated low severity and fixed on 2026-08-03; see the banner above. The
analysis is preserved because it was accurate about the mechanism, but the
disposition it states is not.*

### F3 — Two independent decisions about scan tolerance for the same file

> **Adjudicated 2026-08-03** ([#24](https://github.com/PeterKnego/ultima_db/issues/24)): real, **low severity** — a latent maintenance hazard with no reachable divergence, since both decisions derive 1:1 from the same `wal_write` field. Worth fixing because the likely future divergence is asymmetric: a new presizing sink added without touching recovery's `matches!` would make a benign torn tail a hard error costing the caller the whole durable log (F1's shape, from the other direction). Fixed in `5df6d23` — both call sites route through `WalSinkKind::tail_tolerant()`, exhaustive with no wildcard, so a new sink is a compile error until its tolerance is stated. Note the analysis below missed a *third* policy already in the tree: `MmapSink::open` presizes and takes its write head from `metadata().len()` without scanning at all (bench-only, outside the production contract).

`PreallocFileSink::open` scans **tolerantly, unconditionally** —
`scan_wal(&path, true)` at `5df6d23^ src/wal.rs:1119`, to reconstruct
`write_head`. Cited at the pre-fix revision: the hardcoded `true` is gone, and
that call is `src/wal.rs:1192` today, routed through
`WalSinkKind::CoalescedPrealloc.tail_tolerant()`.
`Store::recover` decided tolerance **separately**, from the configured
`WalWrite` — that call site is `src/store.rs:1073-1078` today and now derives
from the same `sink_kind().tail_tolerant()`, so the two decisions are no longer
independent.

Today the two agree, because the only sink whose `open` this is happens to be
the only sink for which `recover` passes `true`. The coupling is a
coincidence of the current configuration surface, not an enforced invariant:
the tolerance policy for one WAL file is encoded in two places, in two
different modules, deriving from two different things (a hardcoded `true` vs.
a config match). A future sink, or a future config combination, breaks the
agreement silently — and the failure mode is a `write_head` reconstructed
under a different corruption policy than the one recovery is about to apply.

**Also a code-reading finding**, and also unadjudicated. Recorded because it
is cheap to fix now and expensive to discover later.

---

## 4. Per-config verdicts, with state counts

Clean runs are exhaustive (0 states left on queue) and their counts are exact
and reproducible. **Red runs carry no state count**: TLC halts at the first
counterexample, so under the gate's `-workers 2` the totals vary run to run —
widely. `M1.cfg` came back 221, 246, 249, 251 and 256 across five runs against
a deterministic 238 at `-workers 1`. **The trace DEPTH is stable** and is what
this memo records for red rows.

### Baselines (Tasks 1–2)

| Config | Writer mode | Expected | Actual |
|---|---|---|---|
| `WalCrash.cfg` | SingleWriter | no error | clean, **147 distinct**, depth 11 |
| `WalCrashMW.cfg` | MultiWriter | no error | clean, **651 distinct**, depth 10 |
| `WalCrashPrealloc.cfg` | MW + prealloc | no error | clean, **559 distinct**, depth 11 |
| `StrictScanErr.cfg` | owed property (F1) | **violated** | violated, exit 12, depth 9 |

**SingleWriter is not the interesting config**, and its low count is the
reason. The writer slot is held from `begin_write` through promotion, so
`Len(parked) + Cardinality(begun) <= 1` is an invariant of it: nothing is ever
parked while another writer proceeds, the FIFO never holds two tickets,
`Fsync`'s batch-prefix nondeterminism never fires, and the version bump is
dead code. All four are reachable only under `WalCrashMW.cfg`. Both are
checked; MultiWriter is what exercises the protocol.

### The `Durability × WalWrite` matrix (Task 3)

| Config | Expected | Actual |
|---|---|---|
| `modes/ConsistentFsWrite.cfg` | no error | clean, **327**, depth 10 |
| `modes/ConsistentCoalesced.cfg` | no error | clean, **327**, depth 10 |
| `modes/ConsistentPrealloc.cfg` | no error | clean, **281**, depth 11 |
| `modes/ConsistentPreallocScanErrCheck.cfg` | no error | clean, **281**, depth 11 |
| `modes/ConsistentPrealloc3.cfg` (`MaxCommits = 3`) | no error | clean, **14,934**, depth 14 |
| `modes/InlineFsWrite.cfg` | no error | clean, **75**, depth 11 |
| `modes/InlinePrealloc.cfg` | no error | clean, **77**, depth 12 |
| `modes/EventualFsWrite.cfg` | no error | clean, **221**, depth 7 |
| `modes/ConsistentAckKeptCheck.cfg` | no error | clean, **327**, depth 10 |
| `mutations/CalibrationControl3.cfg` | no error | clean, **27,843** |

Two of those are results rather than bookkeeping, and both were *checked*
rather than asserted:

- **`FsWrite` and `Coalesced` are identical at 327/10, exactly.** Both are
  `O_APPEND` sinks that `sync_all` every batch and are scanned strictly, so
  they agree on every variable this model carries. They differ only in *when*
  bytes reach the file, which is invisible because the crash rule already
  gives every unbarriered frame an independent {present, torn, absent}
  outcome — a superset of both "some per-entry writes landed" and "a byte
  prefix of one big write landed".
- **`ConsistentInline` equals `Consistent` under SingleWriter** — 75/11 and
  77/12, confirmed by rerunning with only that one constant changed. Inline
  fsync changes *who* issues the barrier, not *what* it covers, and it is
  SingleWriter-only, where at most one frame is ever buffered.

**Why a third baseline exists.** `FsWrite` and `Coalesced` are both strict
scans, so under them `ScanIsTolerant` is constantly false and the tolerant
half of `scan_wal` is unreachable. Measured, not assumed: a scratch
reachability assertion that the branch is never taken **holds** under
`WalCrash.cfg` and `WalCrashMW.cfg` and is **violated** under
`WalCrashPrealloc.cfg`. Without the third config, `RecoverySound` would have
been green over dead code there.

**Why a `MaxCommits = 3` config exists.** At 2 there is exactly one `Extend`
per behaviour, always from `capacity = 0` on an empty log — so the
*production* shape (records already durable, next batch overruns, zero-fill a
**suffix** of an existing file, chunk-boundary crossing) never happens.
Measured: `NoExtendFromLiveLog` and `NoSecondChunk` are both **exit 0**
(unreachable) at `MaxCommits = 2` and both **exit 12** at 3.

---

## 5. Vacuity canaries — all of them

A model in which nothing ever promotes satisfies every safety property below
it, silently. Every baseline is paired with at least one canary that runs
**first** and must go red. All are asserted on TLC exit code **12**
specifically, never "nonzero" — 150 (parse error) and 151 (undefined
invariant) are also nonzero, so `|| echo "violated (expected)"` would report
success for a *typo in the invariant name*: the one gate whose entire purpose
is that it cannot lie, quietly lying.

| Canary config | Invariant asserted reachable | Result |
|---|---|---|
| `S0Canary.tla` | `AckedIsDurable` (toolchain gate) | violated, exit 12 |
| `Vacuity.cfg` | `NoCommitEverPromotes` | violated, exit 12 |
| `VacuityMW.cfg` | `NoCommitEverPromotes` | violated, exit 12 |
| `VacuityCrash.cfg` | `NoCrashThenRecover` | violated, exit 12 |
| `VacuityCrashMW.cfg` | `NoCrashThenRecover` | violated, exit 12 |
| `VacuityCrashPrealloc.cfg` | `NoCrashThenRecover` | violated, exit 12 |
| `modes/ConsistentFsWriteCanary.cfg` | `NoCrashThenRecover` | violated, exit 12 |
| `modes/ConsistentCoalescedCanary.cfg` | `NoCrashThenRecover` | violated, exit 12 |
| `modes/ConsistentPreallocCanary.cfg` | `NoCrashThenRecover` | violated, exit 12 |
| `modes/ConsistentPreallocExtendCanary.cfg` | `NoPreallocExtend` | violated, exit 12 |
| `modes/ConsistentPreallocTornTailCanary.cfg` | `NoTornTailTruncation` | violated, exit 12 |
| `modes/ConsistentPrealloc3Canary.cfg` | `NoCrashThenRecover` | violated, exit 12 |
| `modes/ConsistentPrealloc3LiveLogCanary.cfg` | `NoExtendFromLiveLog` | violated, exit 12 |
| `modes/ConsistentPrealloc3ChunkCanary.cfg` | `NoSecondChunk` | violated, exit 12 |
| `modes/InlineFsWriteCanary.cfg` | `NoCrashThenRecover` | violated, exit 12 |
| `modes/InlinePreallocCanary.cfg` | `NoCrashThenRecover` | violated, exit 12 |
| `modes/InlinePreallocExtendCanary.cfg` | `NoPreallocExtend` | violated, exit 12 |
| `modes/EventualFsWriteCanary.cfg` | `NoCrashThenRecover` | violated, exit 12 |
| `modes/EventualFsWriteLossCanary.cfg` | `NoEventualAckLoss` | violated, exit 12 |

Three of these are doing more work than "something happens":

- **`NoTornTailTruncation`** demands specifically that recovery truncated at a
  **torn** frame. An *absent* frame is a clean end-of-log in both scan modes
  (`src/wal.rs:692-697`, unconditional `break`), so reaching only that case
  would prove nothing about tolerance.
- **`NoEventualAckLoss`** (red) is paired with
  `modes/ConsistentAckKeptCheck.cfg`, which runs the **same invariant** with
  one constant changed and must come back **clean**. That pair is what shows
  the two durability tiers genuinely differ in this model, rather than the red
  being an artifact of the crash rule.
- **`NoExtendFromLiveLog` / `NoSecondChunk`** are the measurement behind "why
  a `MaxCommits = 3` config" above.

---

## 6. Calibration — the M1–M7 re-gate

A model that verifies clean but cannot re-find bugs that really shipped
produces confident greens that mean nothing. `mutations/` re-runs a committed
baseline with the `MUTATION` constant flipped. **Mutations are constant-gated
inside the one spec and are never forked `.tla` copies** — a forked copy
drifts from the baseline and silently stops testing the real model.

**M1–M6 are code-derived**: each removes a protection the codebase actually
carries, or re-creates a failure mode it documents. **M7 is clause-targeted**,
and the distinction is kept deliberately: no shipped bug ever permuted a
replayed row's identity. M7 exists because `RecoverySound` clause (a) was
checked by every config and falsified by none, and a clause with no falsifying
mutation is a green with nothing behind it.

### Re-gate result: 7 of 7 violate; 7 of 7 match — see the provenance split below

| Config | Baseline mutated | Result | Depth | Mechanism in one sentence |
|---|---|---|---|---|
| `M1.cfg` | `WalCrash.cfg` (SW) | `PromotionFaithful` | 7 | `WriterSlotFree` drops its `parked = <<>>` conjunct — the pre-fix code decremented `active_writer_count` in phase 1, so `begin_write` admitted a second writer while the first was parked in the fsync wait. |
| `M2.cfg` | `WalCrashMW.cfg` | `PromotionFaithful` | 7 | `GateApplies` goes false: no `PromoteGate`, so commits promote in *completion* order rather than ticket order. |
| **`M2Fork.cfg`** | — (`MaxCommits = 3`) | `ForkFromPromotePredecessor` | 11 | M2's documented **symptom**: the disjoint-table erasure. |
| `M3.cfg` | `WalCrashMW.cfg` | `PromotionFaithful` | 7 | The version bump reverts to the pre-fix form verbatim (`e60f8ce^`): compare against `latest_version` alone **and** allocate `latest_version + 1`. |
| **`M3Dup.cfg`** | — (`MaxCommits = 3`) | `NoDupLive` | 12 | M3's documented **symptom**: two writers bumped to the same version, the second `snapshots.insert(v, ..)` silently replacing the first. |
| `M4.cfg` | `WalCrashPrealloc.cfg` | `TailTolerance` | 9 | `ScanIsTolerant` loses its `CoalescedPrealloc` arm (`src/store.rs:1073-1078`), so a preallocated WAL is scanned strictly and a legal torn tail aborts recovery — task37 §7. |
| `M4Abort.cfg` | `modes/ConsistentPrealloc.cfg` | `StrictScanErrLosesDurableAck` | 10 | M4's **harm**: a durably-acked commit made unreachable because a later frame tore. |
| `M5.cfg` | `WalCrashPrealloc.cfg` | `PreallocInvariant` | 5 | `SyncData` loses its `metaDurable` guard — a batch written into a freshly extended region under a bare `fdatasync`, i.e. `preallocate_to`'s `sync_all` (`src/wal.rs:665`) never ran before the positioned write at `:1245`. task37 §4 invariant 2. |
| **`M5Strand.cfg`** | `modes/ConsistentPrealloc3.cfg` | `NoAckLossAfterLiveExtend` | 16 | M5's **harm** rather than its mechanism: an acked commit lost behind an un-synced *live-log* extend. |
| `M6.cfg` | `modes/ConsistentPrealloc.cfg` | `RecoverySound` clause (c) | 9 | `ScanLen` loses the stop at a CRC-bad frame (`src/wal.rs:701-708`), keeping only the end-of-log stop — corruption passes CRC, half a commit record lands in the store, and recovery reports success. |
| `M7.cfg` | `modes/ConsistentPrealloc.cfg` | `RecoverySound` clause (a) | 9 | `Replay` swaps the `cid`/`tbl` identity of chain positions 1 and 2, leaving `ver`, `sub`, `forkedFrom` as computed — the store restarts with the right versions, the right fork chain, and the wrong rows in them. |

Controls, all clean at the same bound as the mutation they pair with —
because "violated" means nothing without a `MUTATION = "NONE"` run proving the
**bound** is not what went red:

| Control | Controls for | Result |
|---|---|---|
| `mutations/CalibrationControl3.cfg` | `M2Fork`, `M3Dup` | clean, 27,843 |
| `modes/ConsistentPreallocScanErrCheck.cfg` | `M4Abort` | clean, 281 |
| `modes/ConsistentPrealloc3.cfg` | `M5Strand` | clean, 14,934 |
| `modes/ConsistentPrealloc.cfg` | `M6`, `M7` | clean, 281, depth 11 |

**Read the depth, not the invariant name.** Where two invariants are first
violated at the same depth, which one TLC reports is decided by declaration
order in the `.cfg`. `mutations/M3.cfg` has **two** — `PromotionFaithful` and
`RecoverySound` clause (d) — both first violated at depth 7, confirmed
separately one invariant at a time. Read the invariant name as *a* property
the mutation breaks, not *the* one.

### The `RecoverySound` clause × mutation matrix

Established by a **44-run matrix**: `RecoverySound` split into four separate
invariants, each run alone against every mutation config. Independently
reproduced.

| Clause | Falsified by | Depth |
|---|---|---|
| (a) recovered state is a prefix of submission order (cid, version, table) | `M7.cfg` **only** | 9 |
| (b) every `Consistent`/`ConsistentInline`-acked commit survives | `M5.cfg`, `M5Strand.cfg` | 8 |
| (c) no replayed torn frame | `M6.cfg` **only** | 9 |
| (d) strictly monotone recovered versions | `M3.cfg`, `M3Dup.cfg` | 7 |

**This is clause coverage, not conjunct coverage** — see §2. Clause (a)'s
`ver`-match and `Len(promoted) <= Len(submitted)` conjuncts remain falsified
by nothing.

### Reproducibility

A mutation row's **state count and witness trace are not reproducible; only
the depth is.** TLC aborts the level it is on when an invariant fails, so how
many states the other worker had already generated — and which of that level's
violating states it reported — is a race. `-workers 1` is deterministic:
`M6.cfg` is 461/188 and `M7.cfg` is 460/187 every time. Published `-workers 2`
numbers are **illustrations, not bounds**. The depth was identical across
every run, worker count and witness.

---

## 7. The gate

```bash
make formal/tla-smoke      # S0 toolchain gate
make formal/tla-model      # baselines + owed property; runs tla-manifest first, tla-modes last
make formal/tla-calibrate  # standing guard: mutations still violate, controls still clean
```

**Result: `make formal/tla-model` green — 34/34 `TLA_MODES` entries at their
exact expected exit codes**, plus the manifest check, the baselines and the
owed property. `make formal/tla-calibrate` green — 15/15.

**What "34/34" is and is not.** It is not 34 successful verifications. The
majority of the table is *expected to go red*:

| `TLA_MODES` entries | Count | Expected exit |
|---|---|---|
| Vacuity/reachability canaries | 13 | **12** (violated) |
| Mutations (M1–M7 + `M2Fork`, `M3Dup`, `M4Abort`, `M5Strand`) | 11 | **12** (violated) |
| Mode baselines | 9 | 0 (clean) |
| Calibration control (`CalibrationControl3`) | 1 | 0 (clean) |
| **Total** | **34** | — |

So **24 of 34 entries pass by failing**, and only 10 are clean verifications.
A green gate here means "the model still discriminates and the baselines still
hold", not "34 properties were proved". (`StrictScanErr.cfg`, the owed
property, is a twelfth expected-red run but lives in `formal/tla-model`
directly rather than in `TLA_MODES`.)

Every entry carries its expected exit code **per config** rather than inferring
it from the filename, so a rename cannot silently reclassify a config. Measured
codes on tla2tools 1.7.4 / TLC 2.19: **0** clean, **10** assumption false,
**12** invariant violated, **150** parse error, **151** invariant undefined.
Verified by injecting a typo'd invariant name into `modes/ConsistentFsWrite.cfg`
— TLC exits 151 and the target fails.

Exit code 10 earns its place: `ConsistentInline` under MultiWriter is a store
`Store::new` **rejects** (task38). Before the `ASSUME` in `WalCrash.tla`, such a
config model-checked cleanly at exit 0 over a configuration no user can
construct. It now exits 10, which fails whatever the table expects.

### The calibration manifest (new in Task 6)

`M2Fork.cfg`, `M3Dup.cfg` and `M5Strand.cfg` carry the **only** evidence that
M2, M3 and M5 reproduce their documented symptoms. Until now the only thing
protecting them was a prose comment above `TLA_MODES`, and **prose cannot fail
a build** — `M2.cfg` and `M3.cfg` keep passing perfectly well without them;
they just stop matching the shipped bug, which is the entire claim the
calibration makes.

`make formal/tla-manifest` pins five things per calibration config: the file
exists, the evidence-carrying `INVARIANT` is still declared, the `MUTATION`
constant is unchanged, the **`MaxCommits` bound** is unchanged, and the config
is still listed in `TLA_MODES` at the same exit code. It needs no TLC and runs
sub-second, so it is wired into `formal/tla-model` — the guard rides on the
target people already run.

Re-bounding is the failure this exists for. `M2Fork`/`M3Dup`/`M5Strand` at
`MaxCommits = 2` would stay **green** while checking a state space too small
to reach the symptom. A green is not a signal there; only the bound is. This
was confirmed rather than argued: re-bounded to `MaxCommits = 2`, `M3Dup.cfg`
exits **0** — 999 states, 739 distinct, depth 11, clean — so the config that
carries M3's only symptom evidence would sit in the battery reporting success
over a state space that cannot reach the duplicate. Nothing but the bound
check catches that. (Measured independently during Task 6 review.)

**All five checks were verified by breaking them, one at a time** — a gate
whose failure path has never fired is not a gate:

| Injected break | `make` result |
|---|---|
| delete `mutations/M2Fork.cfg` | rc=2, "is missing" |
| `M3Dup.cfg` re-bound `MaxCommits` 3 → 2 | rc=2, "is no longer MaxCommits = 3" |
| `M2Fork.cfg` target invariant swapped | rc=2, "no longer declares INVARIANT ForkFromPromotePredecessor" |
| `M5Strand.cfg` `MUTATION` flipped `"M5"` → `"M4"` | rc=2, "is no longer MUTATION = \"M5\"" |
| `mutations/M3Dup.cfg` row dropped from `TLA_MODES` | rc=2, "is not in TLA_MODES at exit 12" |

#### What the manifest still does not catch

Two residual holes, named here so the guard is not read as stronger than it is.
Both are limits of *config-level* checking, not oversights in the rows.

- **Nothing asserts which invariant TLC actually reported.** The manifest
  checks that the evidence-carrying `INVARIANT` is still *declared*
  (`Makefile:274-277`); `formal/tla-modes` checks only the exit code. So
  **adding** a second `INVARIANT` line to one of the single-invariant
  calibration configs (`M2Fork`, `M3Dup`, `M5Strand`, `M4Abort`) passes both:
  the target is still declared, and the run is still exit 12 — but the red may
  now be coming from the *added* invariant, and "this is where M2's symptom
  reproduces" quietly stops being guarded. Closing it needs the reported
  invariant name parsed out of TLC's output, which is a different and more
  brittle kind of check than anything here does today. §6's warning that TLC's
  choice among same-depth violations is decided by `.cfg` declaration order is
  the same fact from the other side.
- **`M6.cfg` and `M7.cfg` pin a weaker property than the other rows.** Their
  manifest invariant is `RecoverySound`, which nearly every baseline config
  also declares; the evidence they actually carry is a specific **clause** of
  it — (c) and (a) respectively — and no config-level check can express a
  clause. So for those two rows, "the evidence-carrying `INVARIANT`" is a
  weaker guarantee than it is for the four symptom-pinning configs, where the
  named invariant is unique to that config's purpose.

---

## 8. Adjudications

Every counterexample and every surprising number is a question about whether
the model or the Rust is wrong. This is the full list, classified.

### Real findings about the Rust

| # | Finding | Cites | Disposition |
|---|---|---|---|
| A1 | Torn tail loses durable acked commits on strict scan — 2 of the 3 `WalWrite` variants, incl. the `#[default]` one, under either durable tier (§3 F1) | `src/store.rs:1073-1078`, `:1079`; `src/wal.rs:705-707` | **F1** — committed as checked owed property `StrictScanErrLosesDurableAck` |
| A2 | `preallocate_to` not idempotent under ENOSPC; never-synced size adopted on next open | `src/wal.rs:628-667`, `:1192-1193`, `:1207-1228`; the error path as described is `1e5d2b7^ src/wal.rs:1130-1136` | **F2** — code-reading finding, outside the model's state space; adjudicated low severity and **fixed in `1e5d2b7`** (see F2) |
| A3 | Scan tolerance decided independently in two modules | `5df6d23^ src/wal.rs:1119` vs `src/store.rs:1073-1078` (both route through `WalSinkKind::tail_tolerant()` today) | **F3** — code-reading finding; adjudicated low severity and **fixed in `5df6d23`** (see F3) |

### Model artifacts and methodology corrections

| # | Adjudication | Resolution |
|---|---|---|
| B1 | The brief's suggested prealloc canary `~(writeHead > capacity)` is the same *family of claim* as `PreallocInvariant`'s load-bearing clause — it would report failure exactly when the model is **correct** | Reachability canary is `NoPreallocExtend` instead |
| B2 | `TailTolerance` clause 2 ("stops at the last good frame, not before it") has **no independent detection power**: `Replay` *is* the accepted prefix by construction. An earlier draft justified it by pointing at the strict path throwing the whole log away — that justification was **wrong**, because the strict path always sets `recoverErr`, which clause 1 already catches | Kept as future-proofing, and **labelled as such out loud** rather than counted as a check doing work |
| B3 | `RecoverySound` clause (a) was asserted to be covered by "M1/M2, the ordering mutations". **False.** `RecoverySound` is clean on both: their break lands on `PromotionFaithful`, a claim about the **live promotion chain**, whereas clause (a) is about the **recovered prefix** after a crash. Different property, different variable, no overlap | Retracted; hole closed in Task 5c by M7. The retraction is preserved in README "Closed calibration holes" because *how* it went unnoticed is the reusable lesson |
| B4 | A bare order reversal for M7 would also descend the versions, reddening `PromoteOrderIsSubmitOrder`, `ForkFromPromotePredecessor` and clause (d) at the same depth — the red would not be clause (a)'s alone | M7 permutes identity at **monotone** versions instead |
| B5 | Post-`Recover`, `promoted` is the **replay sequence, not the Rust's snapshot chain** — recovery installs exactly one snapshot, at `latest_version` (`src/store.rs:1206-1212`) | Documented as a caution in `WalCrash.tla`; a property like "every acked version is *readable* after recovery" must not be built on it |
| B6 | `ConsistentInline` under MultiWriter model-checked cleanly (exit 0) over a store `Store::new` rejects (task38) | `ASSUME` added; now exit 10, which fails every table entry |
| B7 | Mutating only M3's *comparison* cannot produce the documented duplicate; both halves of the pre-fix form are the bug | M3 reverts the version bump verbatim to `e60f8ce^` |
| B8 | M2/M3/M5's shallowest counterexample is a shallower **consequence** (M5: the mechanism rather than the harm), not the documented symptom | Four clause-focused secondary configs, each with a same-bound control — and now the §7 manifest |
| B9 | `NoAckLossAfterLiveExtend` is **implied by `RecoverySound`**, which `ConsistentPrealloc3.cfg` already checks, so it adds no detection power there | No detection power claimed; listed only so control and mutation rows name the *same invariant at the same bound* |
| B10 | `WalCrashPrealloc.cfg` moved 651 → 559 when Task 3 made the sink real. The net −92 hides two much larger opposite effects | Decomposed and measured: sink layer alone **+484** (1135 with `SlotSafe` neutered to `TRUE`), frontier cut **−576**. The components are the evidence, not the net. Removed states are crash outcomes no filesystem can produce — bytes cannot land past a file's durable end |
| B11 | `TailTolerance` unscoped is **false** for the strict sinks — and the model is right | Scoped to `Prealloc` deliberately. Asserting it unscoped would re-report F1 under a name claiming a promise no append-mode sink ever made, and make the property falsifiable by *sink choice* instead of by M4 |
| B12 | `ForkFromPromotePredecessor` is a **tautology below three promotions** (promotion 1 forks from 0; promotion 2 forks from exactly `promoted[1].ver`), and is satisfied by construction over a post-`Recover` chain | Fires only on a crash-free live chain of ≥3 promotions; `M2Fork.cfg` is the only committed config that reaches it |
| B13 | `M3Dup.cfg` checks `NoDupLive`, **not** `NoDuplicateVersion` — after a crash a duplicate in `promoted` is a *WAL* duplicate, not task15's "second `snapshots.insert` replaced the first" | `NoDupLive` switches off once `crashed`, so its counterexample is the live chain |
| B14 | `RecoverySound`'s acked-containment clause is gated on `DurableAck`, so under `Eventual` it is **silently switched off** — a reader would take the green for a promise the store does not make | `EventualHonest` states the promise `Eventual` *does* make, with the red/clean canary pair `EventualFsWriteLossCanary` + `ConsistentAckKeptCheck` |

---

## 9. What S2 owes

1. **The rest of the battery.** S1 covered one of the brief's property groups.
   The full S1–S5/L1/`BulkLoadGuard` set across the mode matrix is outstanding.
2. **A `Checkpoint` action**, which drags in WAL pruning (`src/wal.rs:744`,
   and the preallocating prune at `:788-823`). This is where
   checkpoint/prune/crash interleavings bite, and it is currently untouched.
   Note it will break `PreallocInvariant`'s `writeHead = Len(walDurable)`
   clause **on purpose** — a useful tripwire for whoever adds it.
3. **A per-ticket `Fsync` outcome**, without which fsync *failure* and all of
   L1 (liveness) stay inexpressible.
4. **Lift "no operation after recovery"**, which unlocks "does a restarted
   store re-issue versions safely?" and makes `write_head` reconstruction
   (task37 §4 invariant 3) meaningful — currently declared but inert; the
   reconstruction would be `writeHead' = ScanLen(walAfterCrash)` in `Recover`.
5. **Decide the two unfalsified conjuncts** of clause (a): unfalsifiable by
   construction, or an uncalibrated hole? Until then they are greens with
   nothing behind them.
6. **A mutation that installs the wrong table set at a correct cid.** M7
   *permutes* identity between positions; it does not apply a commit's rows to
   another table while keeping every cid, version and fork source right. If
   S3/S5 adds one, `M2`'s per-table last-writer map becomes necessary.
7. **Adjudicate F2 and F3** and decide F1's disposition — three findings
   currently recorded, none dispositioned.
8. ~~**Consider wiring the gate into CI.**~~ **Done** in the S1 follow-up: the
   `tla` job in `.github/workflows/formal.yml` runs `make formal/tla-smoke &&
   make formal/tla-model` on any PR touching `formal/tla/**`, `tools/tla/**` or
   the `Makefile`. What S2 still owes here is the *deep* tier — `make
   formal/tla-calibrate` and the M1–M7 re-gate are still manual.
9. **Close the manifest's two residual holes** (§7, "What the manifest still
   does not catch"): assert *which* invariant TLC reported, so that **adding**
   an invariant to a single-invariant calibration config cannot silently move
   the red off the target; and find a way to pin `M6`/`M7` to the
   `RecoverySound` **clause** they actually calibrate rather than to the whole
   conjunction. Both need something the current config-level checks cannot
   express — the first means parsing TLC's output, the second means the clauses
   becoming named invariants in their own right in those two configs. Neither
   is urgent; both are the difference between a guard that constrains the
   evidence and one that constrains only its container.
10. **Replace the raw line-range cites with anchored ones.** Every fidelity
   claim in `WalCrash.tla`, this file, `README.md` and the `.cfg` headers
   points at `src/*.rs:LINE`. Those numbers rot on *any* commit that moves
   lines, semantics or not: task57 (`perf(wal): frame entries at commit`)
   landed between the S1 work starting and merging, and shifted **every**
   `src/wal.rs` cite by 88–92 lines — 118 occurrences across 15 files, each of
   which had to be re-checked against the source by hand. The `store.rs` and
   `persistence.rs` cites survived only because that commit happened not to
   touch them. S2 should consider an anchor form instead — a function or item
   name plus one distinctive line of its body, e.g. `` src/wal.rs
   `PreallocFileSink::sync` — "Steady-state barrier: size unchanged" `` —
   which a grep can re-locate and a script could even verify. The drift guard
   (`formal/scripts/check-drift.sh`, extended in the S1 follow-up to watch
   `src/wal.rs`, `src/store.rs` and `src/persistence.rs`) now catches this at
   PR time rather than at the next reader, which **bounds** the damage to one
   PR's worth of rework; it does not eliminate the rework.
