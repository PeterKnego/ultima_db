# TLA+ WAL crash-safety scout — S0 toolchain gate

**Status: S0 only.** The model itself (F-DB-2's scout phase) is not started.
This directory currently holds just the toolchain gate: proof that TLC runs
here, checks invariants, and — the part that matters — *reports a violation
when there is one*.

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
make formal/tla-smoke
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

## Not yet done

Everything else. The model of the real pipeline — WAL frames, `sync_data` vs
`sync_all`, the three-phase Consistent commit, the promotion ticket queue,
checkpoint/prune, crash and recovery — plus the S1–S5/L1 safety battery and
the M1–M5 calibration mutations, is S1 and S2 of the brief.
