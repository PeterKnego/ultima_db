# TLA+ WAL crash-safety scout

**Status: S0 gate + S1 Tasks 1–2** (steady-state commit pipeline, plus crash
and recovery). `S0Smoke`/`S0Canary` are the toolchain gate: proof that TLC runs
here, checks invariants, and — the part that matters — *reports a violation when
there is one*. `WalCrash.tla` is the model proper, covering the three-phase
Consistent commit, a crash with per-frame tearing, and `Store::recover`.

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
make formal/tla-model   # S1 model: three baselines, each after a canary that must go red
```

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
the replay of a **prefix of submission order**, contains every
`Consistent`/`ConsistentInline`-acked commit, replays no torn frame, and has
strictly monotone versions. A strict-mode scan error is excluded — that is a
*loud* failure (`recover()` returns `Err`, the store never opens), not the
silent acked-write loss S1 is about.

| Config | Expected | Actual |
|---|---|---|
| `Vacuity.cfg` | **violated** | violated (exit 12), 14 distinct, depth 5 |
| `VacuityCrash.cfg` | **violated** | violated (exit 12) |
| `WalCrash.cfg` — SingleWriter | no error | no error, **147 distinct**, depth 11 |
| `VacuityMW.cfg` | **violated** | violated (exit 12), 59 distinct, depth 5 |
| `VacuityCrashMW.cfg` | **violated** | violated (exit 12) |
| `WalCrashMW.cfg` — MultiWriter | no error | no error, **651 distinct**, depth 10 |
| `VacuityCrashPrealloc.cfg` | **violated** | violated (exit 12) |
| `WalCrashPrealloc.cfg` — MW + prealloc | no error | no error, **651 distinct**, depth 10 |

Constants otherwise: `MaxCommits = 2`, `Tables = {t1, t2}`, `Consistent`,
`MUTATION = "NONE"`; `SinkKind = FsWrite` except in the prealloc config.
Task 1's no-crash counts were 49 (SingleWriter) and 169 (MultiWriter); crash
and recovery multiply them 3.0× and 3.9×. All three baselines are exhaustive
(0 states left on queue).

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

The canary is not decoration. A model in which nothing ever promotes satisfies
every safety property below it, silently. Each baseline is paired with a
canary that runs first; `make formal/tla-model` enforces the ordering, and
asserts TLC exit code **12** specifically rather than "nonzero" — 150 (parse
error) and 151 (undefined invariant) are also nonzero, so a typo in an
invariant name would otherwise print "violated (expected)" having checked
nothing.

## Not yet done

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

`FsWrite` vs `Coalesced` still do not diverge: they differ in write
granularity, not in sync granularity or in what the recovery scan does. Also
pending: the S3/S4/S5 properties and the M2–M5 battery.

M1 is already gated on `MUTATION` (one spec, never forked `.tla` copies) — it
was needed to prove the SingleWriter path is no longer over-protected. It has
no committed config yet; run it with:

```bash
cd formal/tla/wal && sed 's/MUTATION = "NONE"/MUTATION = "M1"/' WalCrash.cfg > _m1.cfg
java -XX:+UseSerialGC -Xmx2g -cp ../../../tools/tla/tla2tools-1.7.4.jar tlc2.TLC \
  -metadir $HOME/tlc-states -workers 2 -config _m1.cfg WalCrash.tla; rm _m1.cfg
```

It violates `PromotionFaithful` (exit 12). M2–M5 and the mutation configs are
still to come.
