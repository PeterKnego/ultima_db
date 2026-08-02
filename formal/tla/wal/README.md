# TLA+ WAL crash-safety scout

**Status: S0 gate + S1 Tasks 1–5** (steady-state commit pipeline, crash and
recovery, the three production WAL sinks, and the full M1–M5 calibration
battery). `S0Smoke`/`S0Canary` are the
toolchain gate: proof that TLC runs here, checks invariants, and — the part that
matters — *reports a violation when there is one*. `WalCrash.tla` is the model
proper, covering the three-phase Consistent commit, a crash with per-frame
tearing, `Store::recover`, and the `sync_data`/`sync_all` barrier split that
distinguishes `CoalescedPrealloc` from the two append-mode sinks.

Brief: `docs/superpowers/specs/2026-08-02-wal-crash-safety-tla-scout-brief.md`.
Roadmap: `docs/superpowers/specs/2026-08-01-formal-roadmap.md` (task F-DB-2).

## Toolchain

| Component | Version | Where |
|---|---|---|
| TLA+ tools (TLC) | 1.7.4 / TLC 2.19 | `tools/tla/tla2tools-1.7.4.jar` (vendored) |
| Java | 21 (Temurin) | already a dev dependency via the Elle harness |

The jar is committed, version-named, following the precedent set by
`tools/elle-cli/elle-cli-0.1.9-standalone.jar`. sha256:

```
936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
```

## Running

```bash
make formal/tla-smoke   # S0 toolchain gate
make formal/tla-model   # S1 model: baselines + the durability matrix (runs tla-modes)
make formal/tla-modes   # just the Durability x WalWrite matrix (modes/*.cfg)
```

Whole gate: ~22 s.

Two constraints are not optional on this box, both learned the hard way and
both encoded in the Makefile target:

- **TLC's metadata directory must not be on `/tmp`.** `/tmp` here is tmpfs
  (7.7 GB, RAM-backed, no swap), so a real model-checking run would consume
  memory it looks like it is not consuming, and die opaquely. The target
  writes state to `$(HOME)/tlc-states`.
- **The JVM heap is bounded** (`-Xmx2g`, `-XX:+UseSerialGC`), same discipline
  as the Elle harness. Unbounded TLC on a shared box is how you lose the box.

## What the gate proves

`S0Smoke.tla` is a deliberately tiny durability-shaped spec: a frame is
written, then either fsynced or lost to a crash. Its invariant is the one the
real scout exists to check — `acked <= durable`, i.e. anything acknowledged
durable survives a crash.

`S0Canary.tla` is the same spec with `Fsync` mutated to acknowledge *without*
advancing durability — the phantom-durability bug in one line.

| Spec | Expected | Actual |
|---|---|---|
| `S0Smoke` | no error, exit 0 | "Model checking completed. No error has been found." 9 distinct states |
| `S0Canary` | invariant violated, exit 12 | "Invariant AckedIsDurable is violated." |

The canary is the point. A model checker that only ever reports success is
indistinguishable from one that is broken, and the brief's calibration
discipline (M1–M5, §5) is this same idea at full scale: every green verdict is
only meaningful once the model has been shown to go red for a bug that really
existed. Keep the canary passing-as-failing; if it ever reports success, the
gate is lying.

## The model: `WalCrash.tla`

Tasks 1–2 of the S1 plan. Steady-state three-phase commit — `Begin` (allocate a
candidate version), `Submit` (phase 1 PREPARE: finalize the version against
`max(latest, lastSubmitted)`, buffer the WAL entry, take a `PromoteGate`
ticket), `Fsync` (the background thread's batch barrier, covering a prefix of
the buffered log), `Promote` (phase 3: FIFO head only, forks from the latest
*at promote time*). Under `Eventual` the lock is never released, so `Submit`
promotes inline (`commit_may_park = false`).

The invariant is task15's load-bearing sentence — *`latest_version` strictly
advances at every promotion, and every promotion forks from the latest at
promote time* — as `PromotionFaithful`.

The version bump and the `PromoteGate` FIFO are gated on
`WriterMode = "MultiWriter"`, because in the Rust they exist only in
`commit_multi_writer` (`src/store.rs:3992`, `:4050`/`:4104`).
`commit_single_writer` (`:3737-3843`) has neither — holding the writer slot
through the fsync wait is its *only* protection. Modelling them
unconditionally would hand SingleWriter protections the code lacks, and would
mask M1.

### Crash and recovery (Task 2)

`Crash` fires at most once per behaviour. Volatile state is destroyed; frames
already covered by a durability barrier survive intact; every merely-buffered
frame independently lands whole, lands **torn** (present, CRC-bad), or never
lands. Tearing is per-frame and **positional** — an absent frame is a hole at
its own byte offset, not a removal that slides later frames forward, because
`scan_wal` walks offsets in order and `break`s at the first record it cannot
accept (`src/wal.rs:574-607`). `Recover` is `Store::recover`
(`src/store.rs:984`): install the checkpoint, scan, replay entries past the
checkpoint version. `tail_tolerant` is true for `CoalescedPrealloc` only
(`src/store.rs:1017-1022`), and that is the whole of `SinkKind`'s influence.

`RecoverySound` is S1: after a successful crash+recover, the recovered state is
the replay of a **prefix of submission order** (matching on cid, version *and
table*), contains every `Consistent`/`ConsistentInline`-acked commit, replays no
torn frame, and has strictly monotone versions. A strict-mode scan error is
excluded — see "Not yet done", where it has its own named invariant.

Two things to know before building on this. The post-`Recover` value of
`promoted` is the **replay sequence, not the Rust's snapshot chain**: recovery
installs exactly one snapshot, at `latest_version` (`src/store.rs:1150-1156`),
so a property like "every acked version is *readable* after recovery" must not
be built on it. And the bound is **≤1 crash *and no operation after
recovery*** — every steady-state action requires `~crashed` and `Recover`
requires `~recovered`, so "does a restarted store re-issue versions safely?" is
outside this state space.

| Config | Expected | Actual |
|---|---|---|
| `Vacuity.cfg` | **violated** | violated (exit 12) |
| `WalCrash.cfg` — SingleWriter | no error | no error, **147 distinct**, depth 11 |
| `VacuityMW.cfg` | **violated** | violated (exit 12) |
| `WalCrashMW.cfg` — MultiWriter | no error | no error, **651 distinct**, depth 10 |
| `VacuityCrash.cfg` | **violated** | violated (exit 12) |
| `VacuityCrashMW.cfg` | **violated** | violated (exit 12) |
| `VacuityCrashPrealloc.cfg` | **violated** | violated (exit 12) |
| `WalCrashPrealloc.cfg` — MW + prealloc | no error | no error, **559 distinct**, depth 11 |
| `StrictScanErr.cfg` — owed property | **violated** | violated (exit 12), depth 9 |

Canary rows carry no state count on purpose: TLC halts at the first violation,
so their totals are partial and vary run to run with worker interleaving. Only
the clean runs are exhaustive (0 states left on queue) and reproducible.

`WalCrashPrealloc.cfg` moved from Task 2's 651/depth 10 to 559/depth 11 when
Task 3 made the sink real. The net −92 hides two much larger, opposite effects,
and the components are the evidence, not the net:

| | distinct | what changed |
|---|---|---|
| Task 2 | 651 | inert sink |
| Task 3, `SlotSafe` neutered to `TRUE` | **1135** | the sink layer alone adds **+484** |
| Task 3, as committed | **559** | the frontier cut removes **−576** |

Measured by editing `SlotSafe` to `TRUE` and rerunning `WalCrashPrealloc.cfg`
(still clean, 1135/depth 11). So `syncedCapacity` gates 576 of 1135 states —
just over half — and `Extend`/`SyncData`/`SyncAll` between them nearly doubled
the space before the cut. A sink contributing +484 states is not inert; the cut
removing 576 is not cosmetic.

The removed states are crash outcomes Task 2 enumerated as distinct but which no
filesystem can produce: bytes cannot land past a file's durable end. Depth rises
because `Extend` is a step. The tolerant-truncation branch survived: measured,
see the Task 3 report (assertion R11).

**Why a third baseline.** `FsWrite` and `Coalesced` are both *strict* scans, so
under them `TailTolerant` is constantly false and the tolerant half of
`scan_wal` — recovery truncating at a torn tail and carrying on, the branch M4
attacks — is unreachable. Measured, not assumed: a scratch reachability
assertion asserting that branch is never taken **holds** under `WalCrash.cfg`
and `WalCrashMW.cfg`, and is violated under `WalCrashPrealloc.cfg`. Without the
third config, `RecoverySound` would be green over dead code there.

**SingleWriter is strictly serial by construction and is not the interesting
config.** Because the writer slot is held from `begin_write` through
promotion, `Len(parked) + Cardinality(begun) <= 1` is an invariant of it: no
commit is ever parked while another writer proceeds, the FIFO never holds two
tickets, `Fsync`'s batch-prefix nondeterminism never fires (at most one frame
is ever buffered), and the version bump is dead code. All four are reachable
only under `WalCrashMW.cfg`. Both are checked; treat 169, not 49, as the
tripwire that the model is still exploring.

### The sink modes and the durability matrix (Task 3)

Task 1–2 had one `Fsync` action. Task 3 splits it by **what the barrier
covers**, because that difference is the entire preallocation bug surface.

| Action | Gloss |
|---|---|
| `SyncData` | `fdatasync` — a barrier over frame *data* inside an already-durable file size. Reached by `PreallocFileSink` and nothing else in production (`src/wal.rs:1050-1051`), and only in steady state (`metaDurable` guard). |
| `SyncAll` | `fsync` — data **and** metadata, so the physical file size becomes durable. Every append-mode sink uses it for every batch (`src/wal.rs:912-916`, `:966-970`) because every append changes the size; `PreallocFileSink` reaches it only as `preallocate_to`'s sync (`src/wal.rs:549`), i.e. only on an extend. |
| `Extend` | The batch overruns `capacity`, so grow by whole chunks of physically written zeros (`src/wal.rs:1038-1044`, `:531-551`) and drop `metaDurable` until a `SyncAll` covers the new size. |

`sync_data` does **not** make a new file *size* durable. So bytes past the last
`sync_all`-covered size can vanish **wholesale** on a crash — including frames a
`sync_data` "covered", because the filesystem may still present the old size and
those offsets simply are not there. `syncedCapacity` is that frontier and
`SlotSafe` is the crash rule. This is why `preallocate_to` ends in `sync_all`
*before* any record is written into the new region, and it is what M5 attacks in
Task 5.

`PreallocInvariant` is task37 §4 stated on its own, so a sink-layer model bug
surfaces as an invariant break rather than a confusing downstream
`RecoverySound` trace. Its load-bearing clause is `writeHead ≤ syncedCapacity`
— barrier discipline. At `MUTATION = "NONE"` that is an **invariant**, not a
reachable state; the brief's suggested canary shape `~(writeHead > capacity)` is
the same family of claim and would have reported failure exactly when the model
is correct, so the reachability canary is `NoPreallocExtend` instead (see the
Task 3 report, "Adjudication").

`modes/` is the `Durability × WalWrite` matrix, one config per combination the
Standalone pipeline actually offers, each with at least one paired canary at
exit 12. `SYMMETRY TableSymmetry` is declared here and deliberately *not* on the
Task 1–2 baselines, so their committed counts stay comparable across tasks.

| Config | Expected | Actual |
|---|---|---|
| `modes/ConsistentFsWrite.cfg` | no error | **327**, depth 10 |
| `modes/ConsistentCoalesced.cfg` | no error | **327**, depth 10 |
| `modes/ConsistentPrealloc.cfg` | no error | **281**, depth 11 |
| `modes/ConsistentPreallocScanErrCheck.cfg` | no error | **281**, depth 11 |
| `modes/ConsistentPrealloc3.cfg` (`MaxCommits = 3`) | no error | **14934**, depth 14 |
| `modes/InlineFsWrite.cfg` | no error | **75**, depth 11 |
| `modes/InlinePrealloc.cfg` | no error | **77**, depth 12 |
| `modes/EventualFsWrite.cfg` | no error | **221**, depth 7 |
| `modes/ConsistentAckKeptCheck.cfg` | no error | **327**, depth 10 |
| `modes/*Canary.cfg` (13) | **violated** | all violated (exit 12) |

**Why a `MaxCommits = 3` config.** Every other config uses `MaxCommits = 2`,
and at that bound there is exactly one `Extend` per behaviour, always from
`capacity = 0` on an empty log. So the *production* shape — records already
durable, the next batch overruns the region, `preallocate_to` zero-fills a
**suffix** of an existing file (`src/wal.rs:536-551` with `from != 0`) and
`need.div_ceil(chunk)*chunk` (`:1041`) crosses a chunk boundary — never happens.
Measured, not assumed: `NoExtendFromLiveLog` and `NoSecondChunk` are both
**exit 0** (unreachable) at `MaxCommits = 2` and both **exit 12** at 3. That is
also the region M5's *already-durable-and-acked* failure lives in. 14934 states,
depth 14, ~1.4 s — inside the plan's ≤4-commit bound and effectively free.

Two of those numbers are results, not bookkeeping:

- **`FsWrite` and `Coalesced` are identical — 327/10, exactly.** They still do
  not diverge, but now for a stated reason rather than an unexamined one: both
  are `O_APPEND` sinks that `sync_all` every batch and are scanned strictly, so
  they agree on every variable this model has. They differ only in *when* bytes
  reach the file (per entry at append vs. one `write_all` per batch inside
  `sync`), and that is invisible because the crash rule already gives every
  unbarriered frame an independent {present, torn, absent} outcome — a superset
  of both "some of the per-entry writes landed" and "a byte prefix of one big
  write landed".
- **`ConsistentInline` equals `Consistent` under SingleWriter** — 75/11 and
  77/12 respectively, confirmed by rerunning both configs with only that one
  constant changed. Inline fsync changes *who* issues the barrier (the
  committing thread, off the store lock) not *what* it covers, and it is
  SingleWriter-only, where at most one frame is ever buffered, so the batching
  it removes is already degenerate. Modelled as equal, and then checked.

`EventualFsWrite.cfg` carries `EventualHonest` as well as `RecoverySound`.
`RecoverySound`'s acked-containment clause is gated on `DurableAck`, so under
`Eventual` it is silently switched off — a reader who missed the gate would take
the green for a promise the store does not make. `EventualHonest` states the
promise it *does* make: whatever survives is a submission-order prefix, nothing
reordered, nothing partial. Its vacuity guard is
`modes/EventualFsWriteLossCanary.cfg` (`NoEventualAckLoss` must go **red** — an
acked commit really is lost), paired with `modes/ConsistentAckKeptCheck.cfg`,
which runs the *same invariant* with one constant changed and must come back
**clean**. That pair is what shows the two durability tiers genuinely differ in
this model instead of the red being an artifact of the crash rule.

The canary is not decoration. A model in which nothing ever promotes satisfies
every safety property below it, silently. Each baseline is paired with a
canary that runs first; `make formal/tla-model` enforces the ordering, and
asserts TLC exit code **12** specifically rather than "nonzero" — 150 (parse
error) and 151 (undefined invariant) are also nonzero, so a typo in an
invariant name would otherwise print "violated (expected)" having checked
nothing. The `modes/` loop writes its expected exit code down *per config*
(`TLA_MODES` in the Makefile) rather than inferring it from the filename, so a
rename cannot silently reclassify a config. Verified by injecting a typo'd
invariant name into `modes/ConsistentFsWrite.cfg`: TLC exits 151 and the target
fails.

Measured TLC exit codes on tla2tools 1.7.4 / TLC 2.19: **0** clean, **10**
assumption false, **12** invariant violated, **150** parse error, **151**
invariant undefined. 10 matters because `ConsistentInline` under MultiWriter is
a store `Store::new` rejects (task38); before the `ASSUME` in `WalCrash.tla` it
model-checked cleanly at exit 0 over a configuration no user can construct.
Now it exits 10, which fails whatever the table expects.

## Not yet done

**A torn tail costs a strict-scan store its whole log — including durable,
acked commits.** `scan_wal` treats a CRC mismatch as end-of-log only when
`tail_tolerant`, which `Store::recover` passes for `CoalescedPrealloc` and
nothing else (`src/store.rs:1017-1022`). Every other sink gets
`Err(WalCorrupted)` (`src/wal.rs:589-591`), which `recover` propagates with `?`
(`src/store.rs:1023`) *before applying any entry* — so frames the scan had
already accepted, at offsets before the tear, are discarded too. This is
reachable on the **default** Standalone configuration (`Consistent` +
`WalWrite::PerEntry`), not an exotic one, and a full-length-but-CRC-bad tail is
physically ordinary on an appending sink. It is deliberately **not** a
`RecoverySound` clause: `recover()` returned `Err`, so there is no recovered
state to predicate over, and folding it in would turn a safety claim into an
availability one. Nobody has yet decided whether the behaviour should change,
so until someone does it is checked as a **known gap** —
`StrictScanErrLosesDurableAck` (`StrictScanErr.cfg`) must report **violated**,
and `make formal/tla-model` asserts exit 12 for it like a canary. A green there
means either the strict error path stopped being reachable or the behaviour
changed; in the second case, write the real property and delete this one.

Fsync *failure* — task15's "a commit whose fsync fails advances the gate
without promoting". Crash did **not** give it a home: a crash removes a parked
commit by destroying the whole gate, whereas a failed fsync must remove *one*
ticket and let the rest of the FIFO proceed. That needs a per-ticket outcome on
`Fsync`. L1 (liveness) stays inexpressible until then.

Checkpoint and prune — `checkpointVersion` is carried and `Recover` honours it
as the replay floor (`src/store.rs:1027-1030`), but no action moves it off 0, so
no committed config exercises a non-zero floor. A `Checkpoint` action also drags
in WAL pruning (`src/wal.rs:628`), which is where checkpoint/prune/crash
interleavings would actually bite.

`write_head` **reconstruction on open** (task37 §4 invariant 3) is declared but
inert. `PreallocFileSink::open` rebuilds the head with a *tolerant* `scan_wal`
and takes `capacity` from `metadata().len()` (`src/wal.rs:1023-1024`); there is
no persisted head pointer to corrupt. `Crash` sets `writeHead` to 0 and
`Recover` leaves it there, because the bound is "no operation after recovery",
so nothing would consume a reconstructed head. When S2 lifts that bound the
reconstruction is `writeHead' = ScanLen(walAfterCrash)` in `Recover`.

**WAL prune**, including the preallocating prune (`src/wal.rs:672-707`, task37
§6 strategy P2), which resets `write_head`/`capacity` from a pre-sized
tmp+rename. It is reachable only from `Checkpoint`, which this model does not
have, so no action moves `writeHead` backwards and `writeHead = Len(walDurable)`
is currently a *checked* `PreallocInvariant` clause rather than an assumption. A
`Checkpoint` action would break that clause, on purpose — which makes it a
useful tripwire for whoever adds one.

Also pending: the remaining S3 properties. S5 (`TailTolerance`) and the M4–M5
battery landed in Task 5, below.

## Calibration: the mutations (Tasks 4–5)

A model that verifies clean but cannot re-find bugs that actually shipped
produces confident greens that mean nothing. `mutations/` re-runs a committed
baseline with the `MUTATION` constant flipped, re-creating each of the three
lost-update interleavings task15 documents as *reproducible failure modes*
(`docs/tasks/task15_three_phase_consistent_persistence.md:81-101`) plus the two
preallocation subtleties task37 is built around (§4 invariant 2, §7). They are
gated in `TLA_MODES` at exit **12** exactly like the canaries.

| Config | Baseline it mutates | Expected | Actual |
|---|---|---|---|
| `mutations/M1.cfg` | `WalCrash.cfg` (SingleWriter) | **violated** | `PromotionFaithful`, depth 7 |
| `mutations/M2.cfg` | `WalCrashMW.cfg` (MultiWriter) | **violated** | `PromotionFaithful`, depth 7 |
| `mutations/M2Fork.cfg` | — (`MaxCommits = 3`) | **violated** | `ForkFromPromotePredecessor`, depth 11 |
| `mutations/M3.cfg` | `WalCrashMW.cfg` | **violated** | `PromotionFaithful`, depth 7 |
| `mutations/M3Dup.cfg` | — (`MaxCommits = 3`) | **violated** | `NoDupLive`, depth 12 |
| `mutations/M4.cfg` | `WalCrashPrealloc.cfg` | **violated** | `TailTolerance`, depth 9 |
| `mutations/M4Abort.cfg` | `modes/ConsistentPrealloc.cfg` | **violated** | `StrictScanErrLosesDurableAck`, depth 10 |
| `mutations/M5.cfg` | `WalCrashPrealloc.cfg` | **violated** | `PreallocInvariant`, depth 5 |
| `mutations/M5Strand.cfg` | `modes/ConsistentPrealloc3.cfg` | **violated** | `NoAckLossAfterLiveExtend`, depth 16 |
| `mutations/CalibrationControl3.cfg` | control for M2Fork/M3Dup | no error | clean, 27843 states |
| `modes/ConsistentPreallocScanErrCheck.cfg` | control for M4Abort | no error | clean, 281 states |
| `modes/ConsistentPrealloc3.cfg` | control for M5Strand | no error | clean, 14934 states |

- **M1** — `WriterSlotFree` drops its `parked = <<>>` conjunct: the pre-fix
  code decremented `active_writer_count` in phase 1, so `begin_write` admitted
  a second writer while the first was parked in the fsync wait.
- **M2** — `GateApplies` goes false: no `PromoteGate`, so a commit promotes in
  *completion* order rather than ticket order.
- **M3** — the version bump reverts to the pre-fix form verbatim (`e60f8ce^`):
  compare against `latest_version` alone **and** allocate `latest_version + 1`.
  Both halves are the bug; see the Task 4 report for why mutating only the
  comparison cannot produce the documented duplicate.
- **M4** — `TailTolerant` loses its `CoalescedPrealloc` arm: the tolerance
  selection at `src/store.rs:1017-1022` goes away and a preallocated WAL is
  scanned *strictly*. task37 §7 is the whole reason that arm exists —
  preallocation puts a partially-written record in front of durable zeros, so a
  torn tail *looks* like a complete frame whose CRC fails, and the pre-task37
  rule aborts recovery for it.
- **M5** — `SyncData` loses its `metaDurable` guard: the batch is written into a
  freshly extended region under a bare `fdatasync`, i.e. `preallocate_to`'s
  `sync_all` (`src/wal.rs:549`) never ran before the positioned write at
  `:1046`. task37 §4 invariant 2 — "new size must be durable before use".

**Which config carries which mechanism.** `M1.cfg` … `M5.cfg` prove each
mutation is *caught*; the "Actual" column above is the shallowest
counterexample, which for M2, M3 and M5 is a shallower **consequence** (or, for
M5, the mechanism rather than the harm) of the break. The documented symptoms
are carried entirely by the four clause-focused configs: `M2Fork.cfg` witnesses
M2's disjoint-table erasure, `M3Dup.cfg` M3's duplicate `snapshots.insert`,
`M4Abort.cfg` the durably-acked commit that a strict scan makes unreachable,
and `M5Strand.cfg` the acked commit lost behind an un-synced *live-log* extend.
Deleting one, or re-bounding it, silently removes that evidence while the gate
stays green. Each has a same-bound, same-shape `MUTATION = "NONE"` control:
`CalibrationControl3.cfg`, `modes/ConsistentPreallocScanErrCheck.cfg`,
`modes/ConsistentPrealloc3.cfg`.

`PromotionFaithful` is a conjunction of four *named* clauses
(`PromoteOrderIsSubmitOrder`, `LatestStrictlyAdvances`,
`ForkFromPromotePredecessor`, `NoDuplicateVersion`) so a mutation can be
checked against one of them in isolation. TLC stops at the shallowest
counterexample, and the shallowest break is not necessarily the historical
bug's documented *symptom* — M2's disjoint-table erasure and M3's duplicate
insert both need `MaxCommits = 3` and a deeper trace than the ordering break
that masks them at 2. That is what `M2Fork.cfg` and `M3Dup.cfg` pin, and why
they carry a shared control at the same bound.

Two corners are worth knowing before touching those two configs:

- **`ForkFromPromotePredecessor` is a tautology below three promotions.**
  `Promote` sets `forkedFrom := latestVersion`, and `latestVersion` is the
  running max over promoted versions, so promotion 1 forks from 0 and promotion
  2 forks from exactly `promoted[1].ver` — divergence *requires* a third
  promotion. It is also satisfied by construction over a post-`Recover` chain,
  since `Replay` defines `forkedFrom` as `live[i-1].ver`. So the clause can fire
  only on a **crash-free live chain of ≥ 3 promotions**, and `M2Fork.cfg` is the
  only committed config that reaches it.
- **`M3Dup.cfg` checks `NoDupLive`, not `NoDuplicateVersion`.** After a crash
  `promoted` is the replay sequence rather than the snapshot chain, so a
  duplicate found there is a WAL duplicate — a real consequence of the same bug,
  but not task15's "the second `snapshots.insert(v, ..)` silently replaced the
  first". `NoDupLive` switches off once `crashed`, so its counterexample is the
  live chain: depth 12, `crashed = FALSE`, `acked = {1,2,3}`, two live snapshots
  at version 3.

## `TailTolerance` — S5, and what it says about the strict sinks (Task 5)

task37 §7: under `CoalescedPrealloc` a torn **tail** is end-of-log, not an
error. `TailTolerance` is that sentence, in two clauses that are different
claims:

1. recovery never **aborts** — `scan_wal` breaks out on an undecodable frame
   (`src/wal.rs:586-588`) instead of returning `Err(WalCorrupted)`
   (`:589-591`), so the store opens;
2. it stops at the **last good frame, not before it** — every frame in the
   maximal present-prefix of the on-disk log is replayed. Without clause 2 a
   "tolerant" scan that threw the whole log away would satisfy the property,
   which is not hypothetical: it is what the strict path actually does.

Its vacuity guard is `modes/ConsistentPreallocTornTailCanary.cfg`
(`NoTornTailTruncation` must go **red**), which demands specifically that
recovery truncated at a **torn** frame. An *absent* frame is a clean
end-of-log in both scan modes (`src/wal.rs:576-581`, unconditional `break`), so
reaching only that case would prove nothing about tolerance. Task 2 measured
the same fact with a scratch assertion and wrote it down in prose; prose is not
a gate, and this is the same measurement as a committed config.

**The property is scoped to `Prealloc`, and that scope is a finding rather than
a convenience.** Asked what the model says about the strict sinks, it says the
unscoped property is **false** for them — and the model is right. task37 §7
notes that the tolerant path "cannot distinguish a torn tail from tail
corruption"; the strict path has the same blindness pointing the other way and
treats both as fatal, so a legal torn tail after an ordinary crash *does* abort
recovery under `FsWrite`/`Coalesced`. That is not new and it is not papered
over: it is exactly `StrictScanErrLosesDurableAck`, already committed as an
**owed property**, already red on the *default* Standalone configuration
(`StrictScanErr.cfg`). Asserting `TailTolerance` unscoped would re-report that
known gap under a name claiming a promise no append-mode sink ever made, and
would make the property falsifiable by the sink choice instead of by M4.

The `M4Abort.cfg` / `ConsistentPreallocScanErrCheck.cfg` pair is where the two
meet: the *same* invariant, the *same* bound, one constant apart. Under
`MUTATION = "NONE"` the prealloc store is **clean** — the tolerant scan means
`recover()` never returns `Err`, so the default config's availability gap does
not exist on this sink. Under M4 it is **violated** at depth 10, with `cid 1`
durable, CRC-clean and `Consistent`-acked, and unreachable because a *later*
frame tore. Tail tolerance is what buys the immunity; remove it and the
immunity goes too.
