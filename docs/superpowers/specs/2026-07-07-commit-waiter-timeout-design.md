# CommitWaiter bounded wait — design

**Date:** 2026-07-07
**Status:** approved (brainstormed with Peter)
**Origin:** 2026-06 deep-review deferred backlog — "`CommitWaiter::wait()` has
no timeout; intent-queue liveness relies on a documented convention (drop tx
before waiting)."
**Task number:** task42 (task41 is taken twice already — index-DDL conflict and
the Elle consistency harness; numbering collision noted, not resolved here).

## Problem

In `WriterMode::MultiWriter`, an early-fail conflict returns
`Error::WriteConflict { wait_for: Some(CommitWaiter), .. }`. The caller's retry
loop calls `CommitWaiter::wait()` (`src/intents.rs`), which blocks indefinitely
on a `parking_lot::Condvar` until the conflicting writer commits or aborts.

Liveness depends on an unenforced convention: the waiting writer must drop its
own `WriteTx` (releasing its intents) *before* waiting. A caller that waits
while still holding intents the holder needs can deadlock, and a wedged holder
hangs every waiter forever. There is no way to bound the wait.

## Decision (with Peter, 2026-07-07)

- **Add `CommitWaiter::wait_timeout(Duration) -> bool`** alongside the existing
  unbounded `wait()`. Additive, zero breakage: existing examples/tests keep
  calling `wait()`. `true` = the holder finished (safe to retry); `false` =
  timed out (holder still live).
- **Caller-driven only.** No `StoreConfig` field, no change to `wait()`'s
  meaning. The `WriteConflict` handler already hands the caller the
  `CommitWaiter`; the caller picks the bound.

## Design

### API (`src/intents.rs`)

```rust
impl CommitWaiter {
    /// Block until the conflicting writer commits or aborts, or until
    /// `timeout` elapses. Returns `true` if the writer finished (safe to
    /// rebase and retry), `false` on timeout — in which case the holder is
    /// still live: you likely violated the "drop your WriteTx before waiting"
    /// convention, or the holder is wedged. Returns immediately if the writer
    /// already finished.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        self.0.wait_timeout_inner(timeout)
    }
}
```

`IntentWaiter` gains the underlying method:

```rust
pub(crate) fn wait_timeout_inner(&self, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let mut d = self.done.lock();
    while !*d {
        if self.cvar.wait_until(&mut d, deadline).timed_out() {
            return *d;
        }
    }
    true
}
```

### Spurious-wakeup correctness

The deadline is computed once, *before* the loop. A spurious wake re-waits
against the same `deadline` rather than restarting the full `timeout`, so total
wall-time is bounded even under repeated spurious wakeups. On timeout the method
returns the latched `*d`, so a wake coinciding with the deadline still reports
`true` when the signal actually landed. `parking_lot`'s `wait_until` takes an
`Instant` and returns `WaitTimeoutResult` (`.timed_out()`).

### Unchanged

- `wait()` stays byte-for-byte the same (unbounded). Still correct *only* under
  the drop-before-wait convention — documented, not enforced.
- Intent-queue release path, `try_acquire`, `release_all_for`, retry loops in
  `examples/` (existing unbounded `.wait()` remains valid), SingleWriter mode.

### Docs

Module doc in `intents.rs` gains a retry-with-timeout snippet: on `false`,
surface a "timed out waiting for conflicting writer" error / give up rather than
spin. Note that unbounded `wait()` is convention-dependent and `wait_timeout`
is the safe default when the caller can't guarantee it dropped its tx.

## Testing

Unit tests in `src/intents.rs`:

1. **Signals before deadline → `true` promptly.** Spawn a thread that sleeps
   briefly then `signal_done()`; `wait_timeout(long)` returns `true`, elapsed
   well under the timeout.
2. **Never signaled → `false`, bounded.** `wait_timeout(short)` on an unsignaled
   waiter returns `false`; assert elapsed ≥ timeout and < a generous ceiling
   (proves it neither returns early nor hangs).
3. **Signal during wait wakes early → `true`.** Holder thread signals partway
   through a long timeout; waiter returns `true`, elapsed ≈ signal delay.
4. **Already-signaled → immediate `true`.** `signal_done()` first, then
   `wait_timeout(Duration::ZERO)` (or tiny) returns `true` without blocking.

Timing tests use generous margins to stay robust on the noisy sandbox:
"short" = 50ms, ceiling = a few seconds.

## Docs / task record

Canonical feature doc: `docs/tasks/task42_commit_waiter_timeout.md`. Update the
task21 / deferred-review references that call this out as an open convention.
