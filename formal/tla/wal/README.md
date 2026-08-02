# TLA+ WAL crash-safety scout

**Status: S0 gate + S1 Task 1** (steady-state commit pipeline, no crash yet).
`S0Smoke`/`S0Canary` are the toolchain gate: proof that TLC runs here, checks
invariants, and — the part that matters — *reports a violation when there is
one*. `WalCrash.tla` is the model proper, so far covering the three-phase
Consistent commit at steady state.

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
make formal/tla-model   # S1 model: vacuity canary (must go red) then baseline
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

Task 1 of the S1 plan. Steady-state three-phase commit — `Begin` (allocate a
candidate version), `Submit` (phase 1 PREPARE: finalize the version against
`max(latest, lastSubmitted)`, buffer the WAL entry, take a `PromoteGate`
ticket), `Fsync` (the background thread's batch barrier, covering a prefix of
the buffered log), `Promote` (phase 3: FIFO head only, forks from the latest
*at promote time*). Under `Eventual` the lock is never released, so `Submit`
promotes inline (`commit_may_park = false`).

The invariant is task15's load-bearing sentence — *`latest_version` strictly
advances at every promotion, and every promotion forks from the latest at
promote time* — as `PromotionFaithful`.

| Config | Expected | Actual |
|---|---|---|
| `Vacuity.cfg` (`NoCommitEverPromotes`) | **violated** | violated, 15 states, depth 5 |
| `WalCrash.cfg` (`TypeOK`, `PromotionFaithful`) | no error | no error, **49 distinct states**, depth 9 |

Baseline constants: `MaxCommits = 2`, `Tables = {t1, t2}`, `Consistent`,
`FsWrite`, `SingleWriter`, `MUTATION = "NONE"`.

The canary is not decoration. A model in which nothing ever promotes satisfies
every safety property below it, silently. Run `Vacuity.cfg` first, every time;
`make formal/tla-model` enforces the ordering and fails if the canary *passes*.

## Not yet done

Crash and recovery (Task 2) — `crashed` is declared and pinned `FALSE` so
Task 2 adds an action, not a variable, to every config. `SinkKind` is carried
but not yet behaviour-differentiating: `FsWrite`/`Coalesced`/`CoalescedPrealloc`
only diverge under a crash (`sync_data` vs `sync_all`, torn tails). Also
pending: fsync *failure* (the "advance the gate without promoting" rule),
checkpoint/prune, the S1/S3/S4/S5/L1 battery, and the M1–M5 calibration
mutations (gated on `MUTATION`, one spec, never forked `.tla` copies).
