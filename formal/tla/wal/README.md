# TLA+ WAL crash-safety scout

**Status: S0 gate + S1 complete, Tasks 1–6** (steady-state commit pipeline,
crash and recovery, the three production WAL sinks, and the full M1–M7
calibration battery). **The S1 exit verdict and everything it did and did not
establish is in [`RESULTS.md`](RESULTS.md)** — read that first if you are
deciding whether to fund S2. `S0Smoke`/`S0Canary` are the
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
make formal/tla-smoke     # S0 toolchain gate
make formal/tla-model     # S1 model: baselines + the durability matrix (runs tla-manifest, then tla-modes)
make formal/tla-modes     # just the Durability x WalWrite matrix (modes/*.cfg)
make formal/tla-calibrate # standing guard: every mutation still violates, every control still clean
make formal/tla-manifest  # structural half of the above, no TLC, sub-second
```

`make formal/tla-model` is ~50 s on this box (it was ~22 s before Tasks 5b/5c
added M6 and M7; the figure had gone stale and was re-measured in Task 6).
`formal/tla-calibrate` is ~18–22 s and overlaps `tla-modes` by design — it is
the target to run when the question is specifically "is the model still
discriminating?". The spread is real and is the point: three runs here gave
17.7 / 18.2 / 18.9 s and an independent run gave 22.1 s. Both figures are
wall-clock on a noisy shared box — treat them as orders of magnitude, not
benchmarks, and never as a regression signal.

`formal/tla-manifest` is the mechanical guard on the calibration battery. Four
things carry a mutation's evidence — the config file, the invariant it
declares, its `MUTATION` constant and its `MaxCommits` bound — plus its
presence in `TLA_MODES`. Prose used to be the only thing protecting them, and
prose cannot fail a build: `M2.cfg` and `M3.cfg` keep passing perfectly well
without `M2Fork.cfg` and `M3Dup.cfg`, which are the *only* configs where M2 and
M3 reproduce their documented symptoms. Re-bounding is the quiet one — those
configs at `MaxCommits = 2` stay **green** while checking a state space too
small to reach the symptom. All five checks were verified by breaking them one
at a time; see `RESULTS.md`, "The gate".

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
discipline (M1–M7, §5) is this same idea at full scale: every green verdict is
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
accept (`src/wal.rs:662-695`). `Recover` is `Store::recover`
(`src/store.rs:984`): install the checkpoint, scan, replay entries past the
checkpoint version. `tail_tolerant` is true for `CoalescedPrealloc` only
(`src/store.rs:1017-1022`), and that is the whole of `SinkKind`'s influence.

`RecoverySound` is S1: after a successful crash+recover, the recovered state is
the replay of a **prefix of submission order** (matching on cid, version *and
table*), contains every `Consistent`/`ConsistentInline`-acked commit, replays no
torn frame, and has strictly monotone versions. A strict-mode scan error is
excluded — see "Not yet done", where it has its own named invariant.

**All four of its clauses are now calibrated**, each by a named mutation that
falsifies that clause *and no other clause* — measured by splitting
`RecoverySound` into four invariants and running each alone against every
mutation config, not argued:

| Clause | Falsified by | Depth |
|---|---|---|
| (a) prefix of submission order (cid, version, table) | `M7.cfg` **only** | 9 |
| (b) every `Consistent`/`ConsistentInline`-acked commit survives | `M5.cfg`, `M5Strand.cfg` | 8 |
| (c) no replayed torn frame | `M6.cfg` **only** | 9 |
| (d) strictly monotone recovered versions | `M3.cfg`, `M3Dup.cfg` | 7 |

Clause (a) was the last one open — it was checked by every config and falsified
by none until Task 5c, and this README asserted M1/M2 covered it, which was
false; see "Not yet done" for what that error was and how it was closed.

**Calibrated at CLAUSE granularity, and that is not the same as conjunct
granularity.** Clause (a) is itself a conjunction of four things, and M7 does
not reach all of them: isolated and run alone under `M7.cfg`, the **`cid`**
match is red (depth 9) and the **`tbl`** match is red (depth 9), while the
**`ver`** match and the **`Len(promoted) <= Len(submitted)`** length bound both
come back clean at the control's own 569/281. Since full clause (a) runs clean
to exhaustion under the other ten mutation configs, its weaker conjuncts do
too — so those two are still *checked by everything and falsified by nothing*,
which is the same species of hole Task 5c just closed one conjunct over. They
may well be **unfalsifiable by construction at this bound**, the way
`TailTolerance` clause 2 is — this spec's discipline is to say so out loud when
a clause holds by construction rather than by evidence (see `TailTolerance`'s
own comment in `WalCrash.tla`), and nobody has yet done the work to decide
which of the two these are. Do not read the table above as conjunct coverage.

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
under them `ScanIsTolerant` is constantly false and the tolerant half of
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
only under `WalCrashMW.cfg`. Both are checked; MultiWriter is the config to
watch for "is the model still exploring?", and its committed count is the
**651** in the baseline table above (SingleWriter's is 147). An earlier
revision of this sentence quoted 169 and 49 — Task 1/2 figures that the
committed baselines superseded, and which contradicted the table two hundred
lines up.

### The sink modes and the durability matrix (Task 3)

Task 1–2 had one `Fsync` action. Task 3 splits it by **what the barrier
covers**, because that difference is the entire preallocation bug surface.

| Action | Gloss |
|---|---|
| `SyncData` | `fdatasync` — a barrier over frame *data* inside an already-durable file size. Reached by `PreallocFileSink` and nothing else in production (`src/wal.rs:1142-1143`), and only in steady state (`metaDurable` guard). |
| `SyncAll` | `fsync` — data **and** metadata, so the physical file size becomes durable. Every append-mode sink uses it for every batch (`src/wal.rs:1004-1008`, `:1058-1062`) because every append changes the size; `PreallocFileSink` reaches it only as `preallocate_to`'s sync (`src/wal.rs:637`), i.e. only on an extend. |
| `Extend` | The batch overruns `capacity`, so grow by whole chunks of physically written zeros (`src/wal.rs:1130-1136`, `:619-639`) and drop `metaDurable` until a `SyncAll` covers the new size. |

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
**suffix** of an existing file (`src/wal.rs:624-639` with `from != 0`) and
`need.div_ceil(chunk)*chunk` (`:1133`) crosses a chunk boundary — never happens.
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

## Abstraction obligations

Every green in this model is conditional on five assumptions about the world
below it. They are not modelled, they are *assumed*, and each is a place where
a real failure would be invisible here. Anyone porting this to Lean, or reading
a green as reassurance, is taking these on:

1. **fsync is a barrier — total, atomic, and honest.** `SyncData`/`SyncAll` are
   single steps that make a set of frames durable instantly. Real barriers can
   fail partway, and on Linux a failed writeback can be reported once and then
   forgotten while the page is dropped (the "fsyncgate" class). This model has
   **no fsync-failure action at all** — see "Open behaviour and model gaps".
   So the model cannot see any bug whose trigger is a barrier that reported
   success without providing one.
2. **Tearing is frame-granular and CRC-detectable.** A merely-buffered frame
   independently lands whole, lands torn, or is absent. Real tearing is
   sector-granular; a frame spanning sectors is modelled as "present, CRC-bad",
   which is what a sector-boundary tear produces *given that the CRC catches
   it*. A tear that survives CRC32 is outside the state space. The CRC is 32
   bits (`crc32fast`), so that probability is small, not zero.
3. **Rename is atomic, and `sync_dir` makes it durable.** The preallocating
   prune (`src/wal.rs:760-795`) rebuilds the WAL through tmp+rename. S1 has no
   `Checkpoint` action, so this obligation is **inherited without being
   exercised** — the weakest of the five, because nothing here would notice if
   it were false.
4. **The `sync_data`/`sync_all` split is real.** `sync_data` makes frame bytes
   durable *within an already-durable file size*; only `sync_all` makes a new
   size durable. `syncedCapacity` is that frontier and `SlotSafe` is the crash
   rule built on it. On a filesystem where `fdatasync` also persists size
   growth the model is merely conservative; on one where `sync_all` does not,
   it is wrong, and M5's whole mechanism is mis-stated.
5. **Commits are opaque.** A commit is a `<version, table>` pair
   (`WalCrash.tla:45`, `:213`). Rows, keys, index maintenance and merge
   semantics are all invisible. "The recovered state is correct" therefore
   means *the right commit identities, in the right order, into the right
   tables* — **not** the right contents. A replay that applies the correct
   commit with the wrong rows in it is outside this model by construction.

## Not yet done

**Open work only.** This section used to mix two different things: work
that is genuinely open, and calibration holes that had since been CLOSED
but whose history is worth keeping. A reader scanning for "what is left"
had to read two paragraphs headlined CLOSED to find out they were not it.
They are now separated. Closed holes, with the narrative of how each was
found and shut, are in **"Closed calibration holes"** below. What is still
open splits in two: behaviour and model gaps first, then the one
calibration hole that is only *half* closed.

### Open behaviour and model gaps

**A torn tail costs a strict-scan store its whole log — including durable,
acked commits.** `scan_wal` treats a CRC mismatch as end-of-log only when
`tail_tolerant`, which `Store::recover` passes for `CoalescedPrealloc` and
nothing else (`src/store.rs:1017-1022`). Every other sink gets
`Err(WalCorrupted)` (`src/wal.rs:677-679`), which `recover` propagates with `?`
(`src/store.rs:1023`) *before applying any entry* — so frames the scan had
already accepted, at offsets before the tear, are discarded too. This is
reachable on **2 of the 3 `WalWrite` variants — `PerEntry` (the `#[default]`
one) and `Coalesced` — under either durable tier**, not an exotic corner.
(Earlier drafts said "the default Standalone configuration". There is no such
thing: `Durability` has no `Default` and `Persistence::standalone` takes both
values explicitly. `RESULTS.md` §3 F1 has the precise statement and the
cites.) A full-length-but-CRC-bad tail is
physically ordinary on an appending sink. It is deliberately **not** a
`RecoverySound` clause: `recover()` returned `Err`, so there is no recovered
state to predicate over, and folding it in would turn a safety claim into an
availability one. Nobody has yet decided whether the behaviour should change,
so until someone does it is checked as a **known gap** —
`StrictScanErrLosesDurableAck` (`StrictScanErr.cfg`) must report **violated**,
and `make formal/tla-model` asserts exit 12 for it like a canary. A green there
means either the strict error path stopped being reachable or the behaviour
changed; in the second case, write the real property and delete this one.

**Before deleting it, note what else depends on it.** `mutations/M4Abort.cfg`
uses this same invariant as its target — under M4 a *prealloc* store inherits
the gap, which is how M4's harm (a durably-acked commit made unreachable by a
later frame tearing) gets a witness at all. Resolving the gap and deleting the
property therefore costs M4 that witness, and `M4.cfg`'s abort would be the
only evidence left. Re-home the harm before you delete.

Fsync *failure* — task15's "a commit whose fsync fails advances the gate
without promoting". Crash did **not** give it a home: a crash removes a parked
commit by destroying the whole gate, whereas a failed fsync must remove *one*
ticket and let the rest of the FIFO proceed. That needs a per-ticket outcome on
`Fsync`. L1 (liveness) stays inexpressible until then.

Checkpoint and prune — `checkpointVersion` is carried and `Recover` honours it
as the replay floor (`src/store.rs:1027-1030`), but no action moves it off 0, so
no committed config exercises a non-zero floor. A `Checkpoint` action also drags
in WAL pruning (`src/wal.rs:716`), which is where checkpoint/prune/crash
interleavings would actually bite.

`write_head` **reconstruction on open** (task37 §4 invariant 3) is declared but
inert. `PreallocFileSink::open` rebuilds the head with a *tolerant* `scan_wal`
and takes `capacity` from `metadata().len()` (`src/wal.rs:1115-1116`); there is
no persisted head pointer to corrupt. `Crash` sets `writeHead` to 0 and
`Recover` leaves it there, because the bound is "no operation after recovery",
so nothing would consume a reconstructed head. When S2 lifts that bound the
reconstruction is `writeHead' = ScanLen(walAfterCrash)` in `Recover`.

**WAL prune**, including the preallocating prune (`src/wal.rs:760-795`, task37
§6 strategy P2), which resets `write_head`/`capacity` from a pre-sized
tmp+rename. It is reachable only from `Checkpoint`, which this model does not
have, so no action moves `writeHead` backwards and `writeHead = Len(walDurable)`
is currently a *checked* `PreallocInvariant` clause rather than an assumption. A
`Checkpoint` action would break that clause, on purpose — which makes it a
useful tripwire for whoever adds one.

Also pending: the remaining S3 properties. S5 (`TailTolerance`) and the M4–M5
battery landed in Task 5, below; M6 in Task 5b, M7 in Task 5c.

### Calibration holes still open

**Calibration at CLAUSE granularity is not calibration at CONJUNCT
granularity, and the gap between them is open.** Clause (a)'s `ver`-match
and `Len(promoted) <= Len(submitted)` conjuncts are *checked by everything
and falsified by nothing* — the same species of hole Task 5c closed one
conjunct over. They may be unfalsifiable by construction at this bound, the
way `TailTolerance` clause 2 is, and nobody has done the work to decide
which; until someone does, neither the clause table above nor anything in
`RESULTS.md` should be read as a blanket all-clear. The full statement is
in "Calibration: the mutations" as well as here, deliberately: a prior
round of this work published exactly that all-clear and had to retract it.

**Table identity: half-closed by M7, and the residual is a different claim.**
Clause (a) matches on cid, version *and* table, and until Task 5c no mutation
moved a table at all — every erasure M1–M6 produces is visible through version
and fork-source alone (Task 4 §3 has the ruling and the evidence that the
fork-source proxy is as strong as the table statement *for those* mutations).
M7 does move it: run with **only** the `tbl` conjunct of clause (a), `M7.cfg`
is violated at depth 9, on a witness where version 1's row is recovered into
`t2` and version 2's into `t1` while the versions stay monotone. So the `tbl`
conjunct is no longer a conjunct nothing can falsify — and since this paragraph
is already at conjunct granularity, the rest of that audit belongs here too: M7
falsifies clause (a)'s `cid` and `tbl` conjuncts **only**, and its
**`ver`-match and length-prefix conjuncts
remain checked but unfalsified**, possibly unfalsifiable by construction at this
bound (§`RecoverySound` above, and compare `TailTolerance` clause 2, which the
spec documents as holding by construction and says so out loud). What is
**still open** on table identity specifically is
narrower than it was: M7 *permutes* identity between two positions, it does not
install the **wrong table set at a correct cid**, so a mutation that kept every
cid, version and fork source right and simply applied a commit's rows to
another table would still be invisible here. If S3/S5 adds one, `M2`'s
per-table last-writer map becomes necessary after all.

## Closed calibration holes

Both of these were, at the time they were written, entries in "Not yet
done" — a clause of `RecoverySound` that every config checked and no
mutation could falsify. Each is now shut by a named mutation. They are kept
in full rather than deleted because *how* a checked-by-everything clause
goes unnoticed is the reusable lesson, and in clause (a)'s case this README
asserted the hole was already covered when it was not.

**`RecoverySound` clause (c) — "replays no torn frame" — was checked but
uncalibrated. CLOSED in Task 5b by `mutations/M6.cfg`.** Clauses (b) and (d)
already had a mutation that breaks them (M5 the acked-containment, M3 the
version monotonicity) — and clause (a), this paragraph said at the time, was
covered by "M1/M2 the ordering", which was **wrong**; see the next paragraph.
Clause (c) had **none**,
because M4 is task37 §7's *other* direction — a strict scan that refuses the
whole log, not a tolerant one that replays past the tear — and nothing in
M1–M5 replayed a torn frame either. M6 deletes `ScanLen`'s stop at a
CRC-bad frame (`src/wal.rs:673-680`) and the clause goes red at depth 9,
exactly as this paragraph priced it before the mutation existed. See the
calibration table below.

**`RecoverySound` clause (a) — the recovered state is the replay of a *prefix
of submission order* — was checked but uncalibrated: the clause-(c) hole again,
one clause over, and closing (c) did not close it. CLOSED in Task 5c by
`mutations/M7.cfg`.** It had been **exit 0 under all ten mutation configs** —
M1, M2, M2Fork, M3, M3Dup, M4, M4Abort, M5, M5Strand, M6 — run alone as its own
invariant.

The natural assumption is that M1 and M2 covered it, since they are *the*
ordering mutations. **They do not, and this README said they did until Task
5b.** `RecoverySound` as a whole is clean on `M1.cfg` and `M2.cfg`: their break
lands on `PromotionFaithful`, which is a claim about the **live promotion
chain**, whereas clause (a) is a claim about the **recovered prefix** after a
crash. Different property, different variable, no overlap.

M7 swaps the `cid`/`tbl` identity of two positions of the replayed chain and
leaves `ver`, `sub` and `forkedFrom` exactly as `Replay` computed them, so the
recovered store comes back with the right versions, the right fork chain and
the wrong rows in them — clause (a) red at depth 9 on
`modes/ConsistentPrealloc.cfg`, with (b), (c), (d) and `PromotionFaithful` all
still green at the control's own state count.

**Not a bare order reversal, deliberately.** Reversing the replayed list also
descends the versions, so `PromoteOrderIsSubmitOrder`,
`ForkFromPromotePredecessor` and clause (d) go red at the *same* depth, and on
the Task 4 standard that would be a red for the neighbouring properties as much
as for this clause — the "merely violating" outcome M2 and M3 land in on their
primary configs. What clause (a) uniquely owns is a replay that is mis-ordered
or mis-tabled at *monotone* versions, and that is what M7 pins.

## Calibration: the mutations (Tasks 4–5c)

A model that verifies clean but cannot re-find bugs that actually shipped
produces confident greens that mean nothing. `mutations/` re-runs a committed
baseline with the `MUTATION` constant flipped. **M1–M6 are code-derived**: each
removes a protection the codebase actually carries, or re-creates a failure mode
it documents — the three lost-update interleavings task15 records as
*reproducible failure modes*
(`docs/tasks/task15_three_phase_consistent_persistence.md:81-101`), the two
preallocation subtleties task37 is built around (§4 invariant 2, §7), and the
scan's stop-at-first-bad-frame (`src/wal.rs:673-680`). **M7 is
clause-targeted**, and the distinction is worth keeping. No shipped bug ever
permuted a replayed row's identity; M7 exists because `RecoverySound` clause (a)
was checked by every config and falsified by none, and a clause with no
falsifying mutation is a green with nothing behind it. It is faithful to what
the code *would* do wrong (`Store::recover` applying commit 2's row where
commit 1's belongs), not to something it once did. All of them are gated in
`TLA_MODES` at exit **12** exactly like the canaries.

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
| `mutations/M6.cfg` | `modes/ConsistentPrealloc.cfg` | **violated** | `RecoverySound` clause (c), depth 9 |
| `mutations/M7.cfg` | `modes/ConsistentPrealloc.cfg` | **violated** | `RecoverySound` clause (a), depth 9 |
| `mutations/CalibrationControl3.cfg` | control for M2Fork/M3Dup | no error | clean, 27843 states |
| `modes/ConsistentPreallocScanErrCheck.cfg` | control for M4Abort | no error | clean, 281 states |
| `modes/ConsistentPrealloc3.cfg` | control for M5Strand | no error | clean, 14934 states † |
| `modes/ConsistentPrealloc.cfg` | control for M6 **and M7** | no error | clean, 281 states, depth 11 |

- **M1** — `WriterSlotFree` drops its `parked = <<>>` conjunct: the pre-fix
  code decremented `active_writer_count` in phase 1, so `begin_write` admitted
  a second writer while the first was parked in the fsync wait.
- **M2** — `GateApplies` goes false: no `PromoteGate`, so a commit promotes in
  *completion* order rather than ticket order.
- **M3** — the version bump reverts to the pre-fix form verbatim (`e60f8ce^`):
  compare against `latest_version` alone **and** allocate `latest_version + 1`.
  Both halves are the bug; see the Task 4 report for why mutating only the
  comparison cannot produce the documented duplicate.
- **M4** — `ScanIsTolerant` loses its `CoalescedPrealloc` arm: the tolerance
  selection at `src/store.rs:1017-1022` goes away and a preallocated WAL is
  scanned *strictly*. task37 §7 is the whole reason that arm exists —
  preallocation puts a partially-written record in front of durable zeros, so a
  torn tail *looks* like a complete frame whose CRC fails, and the pre-task37
  rule aborts recovery for it.
- **M5** — `SyncData` loses its `metaDurable` guard: the batch is written into a
  freshly extended region under a bare `fdatasync`, i.e. `preallocate_to`'s
  `sync_all` (`src/wal.rs:637`) never ran before the positioned write at
  `:1138`. task37 §4 invariant 2 — "new size must be durable before use".
- **M6** — `ScanLen` loses the stop at a CRC-bad frame, keeping only the
  end-of-log stop (zero len-prefix / short tail, `src/wal.rs:664-669`). Real
  `scan_wal` walks offsets in order and halts at the first frame it cannot
  accept — `break` under `tail_tolerant` (`:674-676`), `return Err` without it
  (`:677-679`) — so a torn frame is never replayed. M6 takes it as good and
  keeps going: corruption passes CRC, half a commit record lands in the store,
  and recovery reports success. That is `RecoverySound` clause (c), and M6 is
  the only mutation that touches it. Carried on a **tolerant** sink on purpose:
  under `CoalescedPrealloc` the abort arm was never taken anyway, so the only
  observable change is the replayed tear rather than a suppressed
  `ScanFails`.
- **M7** — `Replay` swaps the `cid`/`tbl` identity of chain positions 1 and 2,
  leaving `ver`, `sub` and `forkedFrom` exactly as it computed them. Real
  recovery takes identity from the frame itself: `scan_wal` returns records in
  offset order and `Store::recover` applies each to the table its own entry
  names (`src/store.rs:1027-1030`), so position *i* carries submission *i*'s
  row. M7 applies commit 2's row where commit 1's belongs — the store restarts
  with the right versions, the right fork chain and the wrong rows in them.
  That is `RecoverySound` clause (a), and M7 is the only mutation that touches
  it. **Not** an order reversal: reversing drags the versions down with it and
  would redden `PromoteOrderIsSubmitOrder`, `ForkFromPromotePredecessor` and
  clause (d) at the same depth, so the red would not be clause (a)'s alone.
  Guarded on a chain of length ≥ 2 — a permutation of one element is not a
  permutation, so at `MaxCommits = 1` M7 is the identity.

**Which config carries which mechanism.** `M1.cfg` … `M5.cfg` prove each
mutation is *caught*; the "Actual" column above is the shallowest
counterexample, which for M2, M3 and M5 is a shallower **consequence** (or, for
M5, the mechanism rather than the harm) of the break. **Three** mutations
therefore depend on a clause-focused secondary config for their documented
symptom, and exactly three: `M2Fork.cfg` witnesses M2's disjoint-table erasure,
`M3Dup.cfg` M3's duplicate `snapshots.insert`, and `M5Strand.cfg` the acked
commit lost behind an un-synced *live-log* extend.

**`M4Abort.cfg` is a fourth clause-focused config but not a fourth such
dependency, and the difference matters.** `M4.cfg`'s own shallowest
counterexample is `TailTolerance` at depth 9 — task37 §7's documented mechanism,
on its primary config. `M4Abort.cfg` adds M4's *harm* on top of that (the
durably-acked commit a strict scan makes unreachable); it does not rescue a
match that would otherwise be missing. So M4 belongs with M1/M6/M7 —
matching on its primary — not with M2/M3/M5. Deleting or re-bounding any of the
four still silently removes evidence while the gate stays green, which is why
all four are in the manifest. Each has a same-bound, same-shape `MUTATION = "NONE"` control:
`CalibrationControl3.cfg`, `modes/ConsistentPreallocScanErrCheck.cfg`,
`modes/ConsistentPrealloc3.cfg`. `M6.cfg` and `M7.cfg` need no extra config on
either count: each one's shallowest counterexample already *is* the documented
mechanism (a `torn` frame standing in the recovered `promoted` chain; a
recovered chain whose rows are permuted against submission order), and both
controls are `modes/ConsistentPrealloc.cfg`, the committed mode config they
mutate.

**A mutation row's state count and its counterexample trace are not
reproducible. Only the DEPTH is.** TLC aborts the level it is on when an
invariant fails, so how many states the other worker had already generated —
and which of that level's violating states it reported — is a race. Under the
gate's `-workers 2`, repeat runs of `M6.cfg` return different state counts
(observed spread roughly 435–474 generated; that is an illustration, **not** a
bound — do not treat any published range as one) and different witness traces
for the same violation. `-workers 1` is deterministic: `M6.cfg` is 461/188 and
`M7.cfg` is 460/187 every time (`M7.cfg` was observed at 427/167, 469/194 and
471/195 under `-workers 2` — again an illustration, not a bound). The **depth**
is guaranteed by the breadth-first search and was
identical across every run, worker count and witness.

The **invariant name is not** guaranteed, even though it happens to be stable
for most rows. Where two invariants are first violated at the same depth, which
one TLC reports is decided by declaration order in the `.cfg`:
`mutations/M3.cfg` has **two** — `PromotionFaithful` and `RecoverySound`'s
clause (d) — both first violated at depth 7 (confirmed separately, one
invariant at a time). Compare mutation rows on the depth; read the invariant
name as *a* property the mutation breaks, not *the* one. The clean controls
explore exhaustively and their counts *are* exact, which is why the four
committed baselines reproduce byte-for-byte.

† `NoAckLossAfterLiveExtend` is **implied by `RecoverySound`**, which that
config already checks, so listing it there adds no detection power and none is
claimed. It is listed so the control row and `M5Strand.cfg`'s row name the
*same invariant at the same bound* — a reader comparing them should not have to
derive the implication to see that the comparison is like-for-like.

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
   (`src/wal.rs:674-676`) instead of returning `Err(WalCorrupted)`
   (`:677-679`), so the store opens;
2. it stops at the **last good frame, not before it** — every frame in the
   maximal present-prefix of the on-disk log is replayed.

**Clause 2 has no independent detection power today.** `Recover` sets
`promoted = Replay(walAfterCrash, …)` and `Replay` *is* the accepted prefix by
construction, so clause 2 holds in every state clause 1 admits and cannot fail
alone. An earlier draft justified it by pointing at the strict path throwing the
whole log away; that justification was wrong, because the strict path always
sets `recoverErr`, which clause 1 already catches. It is kept as a claim staked
against a mutation nobody has written yet — anything that returns *successfully*
while replaying less than the accepted prefix (a truncating replay, a floor bug,
a mis-filtering `Checkpoint` action) lands there and nowhere else. Read it as
future-proofing, not as a check currently doing work.

Its vacuity guard is `modes/ConsistentPreallocTornTailCanary.cfg`
(`NoTornTailTruncation` must go **red**), which demands specifically that
recovery truncated at a **torn** frame. An *absent* frame is a clean
end-of-log in both scan modes (`src/wal.rs:664-669`, unconditional `break`), so
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
**owed property**, already red on a configuration built from the `#[default]`
`WalWrite` (`StrictScanErr.cfg`; scope stated precisely in `RESULTS.md` §3 F1). Asserting `TailTolerance` unscoped would re-report that
known gap under a name claiming a promise no append-mode sink ever made, and
would make the property falsifiable by the sink choice instead of by M4.

The `M4Abort.cfg` / `ConsistentPreallocScanErrCheck.cfg` pair is where the two
meet: the *same* invariant, the *same* bound, one constant apart. Under
`MUTATION = "NONE"` the prealloc store is **clean** — the tolerant scan means
`recover()` never returns `Err`, so the strict sinks' availability gap does
not exist on this sink. Under M4 it is **violated** at depth 10, with `cid 1`
durable, CRC-clean and `Consistent`-acked, and unreachable because a *later*
frame tore. Tail tolerance is what buys the immunity; remove it and the
immunity goes too.
