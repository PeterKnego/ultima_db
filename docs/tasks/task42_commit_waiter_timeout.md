# Task 42: CommitWaiter Bounded Wait

**Origin:** 2026-06 deep-review deferred backlog — "`CommitWaiter::wait()` has
no timeout; intent-queue liveness relies on a documented convention (drop tx
before waiting)."

(Note: two prior features both took the number task41 — the index-DDL conflict
fix and the Elle consistency harness. This work is numbered task42.)

## What changed

- New `CommitWaiter::wait_timeout(Duration) -> bool`: `true` = the conflicting
  MultiWriter writer committed/aborted (safe to rebase and retry), `false` =
  timed out (holder still live). Backed by `IntentWaiter::wait_timeout_inner`.
- Existing unbounded `wait()` is unchanged and still correct under the
  drop-before-wait convention.

## Design

- Deadline computed once (`Instant::now() + timeout`); the wait loops on
  `parking_lot::Condvar::wait_until(deadline)`. A spurious wakeup re-waits
  against the same deadline, so total wall-time stays bounded by `timeout`
  regardless of spurious wakes. On timeout the latched `done` flag is returned,
  so a signal landing exactly at the deadline still reports `true`.
- Caller-driven: no `StoreConfig` field. The `Error::WriteConflict { wait_for,
  .. }` handler already carries the `CommitWaiter`; the caller picks the bound.
  A `false` return should surface as a recoverable error / give-up, not a spin.

## Semantics

- `wait_timeout` is the safe default when a caller cannot guarantee it dropped
  its own `WriteTx` before waiting; unbounded `wait()` remains available for
  callers that follow the convention and want to block until resolution.
- Unchanged: intent-queue release path, SingleWriter (no intents), retry loops
  in `examples/` (still use unbounded `wait()`).

Design history: `docs/superpowers/specs/2026-07-07-commit-waiter-timeout-design.md`,
`docs/superpowers/plans/2026-07-07-commit-waiter-timeout.md`.
