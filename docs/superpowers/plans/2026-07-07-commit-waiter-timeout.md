# CommitWaiter Bounded Wait (task42) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `CommitWaiter::wait_timeout(Duration) -> bool` so a MultiWriter conflict retry can bound its wait instead of hanging forever on a wedged holder or a violated drop-before-wait convention.

**Architecture:** `IntentWaiter` gains `wait_timeout_inner(Duration) -> bool` using `parking_lot::Condvar::wait_until` against a deadline computed once (spurious-wake safe). `CommitWaiter::wait_timeout` is a thin public wrapper. Existing unbounded `wait()` is untouched.

**Tech Stack:** Rust, root crate `ultima_db`, `parking_lot` 0.12. All changes in `src/intents.rs`.

**Spec:** `docs/superpowers/specs/2026-07-07-commit-waiter-timeout-design.md`

## Global Constraints

- Branch: `fix/commit-waiter-timeout` (already created).
- `cargo clippy --workspace --all-targets -- -D warnings` must pass at the end.
- `wait()` stays byte-for-byte unchanged.
- Timing tests use generous margins for the noisy sandbox: short timeout = 50ms, upper ceiling = a few seconds.
- Return semantics: `true` = writer finished (safe to retry), `false` = timed out.

---

### Task 1: `wait_timeout_inner` on `IntentWaiter` + `wait_timeout` on `CommitWaiter`

**Files:**
- Modify: `src/intents.rs` — imports (line 24-26), `IntentWaiter` impl (after `wait()` ~line 60), `CommitWaiter` impl (after `wait()` ~line 78)
- Test: `src/intents.rs` tests module (add one if none exists)

**Interfaces:**
- Produces: `IntentWaiter::wait_timeout_inner(&self, timeout: Duration) -> bool` (pub(crate)); `CommitWaiter::wait_timeout(&self, timeout: Duration) -> bool` (public). `true` = signaled, `false` = timed out.

- [ ] **Step 1: Write the failing tests.** Check whether `src/intents.rs` already has a `#[cfg(test)] mod tests`. If not, append one at end of file; if it does, add these into it. Tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn wait_timeout_returns_true_when_signaled_before_deadline() {
        let w = IntentWaiter::new();
        let w2 = Arc::clone(&w);
        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            w2.signal_done();
        });
        let start = Instant::now();
        let signaled = w.wait_timeout_inner(Duration::from_secs(5));
        assert!(signaled, "should observe the signal");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "should return promptly after signal, took {:?}",
            start.elapsed()
        );
        h.join().unwrap();
    }

    #[test]
    fn wait_timeout_returns_false_on_timeout_and_is_bounded() {
        let w = IntentWaiter::new();
        let start = Instant::now();
        let signaled = w.wait_timeout_inner(Duration::from_millis(50));
        let elapsed = start.elapsed();
        assert!(!signaled, "unsignaled waiter must time out");
        assert!(
            elapsed >= Duration::from_millis(50),
            "must not return before the timeout, took {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "must not hang, took {elapsed:?}"
        );
    }

    #[test]
    fn wait_timeout_wakes_early_when_signaled_mid_wait() {
        let w = IntentWaiter::new();
        let w2 = Arc::clone(&w);
        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            w2.signal_done();
        });
        let start = Instant::now();
        let signaled = w.wait_timeout_inner(Duration::from_secs(10));
        assert!(signaled);
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "woke on signal, not deadline: {:?}",
            start.elapsed()
        );
        h.join().unwrap();
    }

    #[test]
    fn wait_timeout_already_signaled_returns_immediately() {
        let w = IntentWaiter::new();
        w.signal_done();
        let start = Instant::now();
        let signaled = w.wait_timeout_inner(Duration::from_millis(0));
        assert!(signaled, "already-signaled waiter returns true");
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn commit_waiter_wait_timeout_wraps_inner() {
        let w = IntentWaiter::new();
        let cw = CommitWaiter(Arc::clone(&w));
        assert!(!cw.wait_timeout(Duration::from_millis(20)), "not signaled → false");
        w.signal_done();
        assert!(cw.wait_timeout(Duration::from_millis(20)), "signaled → true");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail (compile error — method missing)**

Run: `cargo test --lib intents::tests`
Expected: FAIL to compile — `no method named wait_timeout_inner` / `wait_timeout`.

- [ ] **Step 3: Add the `Instant`/`Duration` import.** In `src/intents.rs`, change:

```rust
use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::{Condvar, Mutex};
```

to:

```rust
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::{Condvar, Mutex};
```

- [ ] **Step 4: Add `wait_timeout_inner` to `IntentWaiter`.** Directly after the existing `wait()` method (the one ending `}` at ~line 60), add:

```rust
    /// Block until signaled or until `timeout` elapses. Returns `true` if
    /// the writer signaled done (safe to retry), `false` on timeout.
    ///
    /// The deadline is computed once so a spurious wakeup re-waits against
    /// the same instant rather than restarting the full timeout — total
    /// wall-time stays bounded by `timeout` regardless of spurious wakes.
    pub(crate) fn wait_timeout_inner(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut d = self.done.lock();
        while !*d {
            if self.cvar.wait_until(&mut d, deadline).timed_out() {
                // On deadline, return the latched flag: a signal that lands
                // exactly at the deadline still reports true.
                return *d;
            }
        }
        true
    }
```

- [ ] **Step 5: Add `wait_timeout` to `CommitWaiter`.** In the `impl CommitWaiter` block, after the existing `wait()` method, add:

```rust
    /// Block until the conflicting writer commits or aborts, or until
    /// `timeout` elapses. Returns `true` if the writer finished (safe to
    /// rebase and retry), `false` on timeout — in which case the holder is
    /// still live: you likely violated the "drop your WriteTx before
    /// waiting" convention, or the holder is wedged. Returns immediately if
    /// the writer already finished.
    pub fn wait_timeout(&self, timeout: std::time::Duration) -> bool {
        self.0.wait_timeout_inner(timeout)
    }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --lib intents::tests`
Expected: 5 passed.

- [ ] **Step 7: Commit**

```bash
git add src/intents.rs
git commit -m "feat(intents): CommitWaiter::wait_timeout for bounded conflict waits (task42)"
```

---

### Task 2: Module doc retry-with-timeout pattern

**Files:**
- Modify: `src/intents.rs` module doc (the `//!` block at the top, ~lines 4-22)

**Interfaces:**
- Consumes: `CommitWaiter::wait_timeout` (Task 1). Documentation only.

- [ ] **Step 1: Extend the module doc.** After the existing paragraph that ends "...deadlock is impossible." (the one describing the drop-before-wait bail), add a new paragraph:

```rust
//! `CommitWaiter::wait()` blocks unbounded — correct only under the
//! drop-before-wait convention above. When a caller can't guarantee it
//! released its own intents first (or wants to survive a wedged holder),
//! use `CommitWaiter::wait_timeout(Duration)` instead: it returns `true`
//! when the holder finished (rebase and retry) and `false` on timeout, so
//! a violated convention surfaces as a recoverable error rather than a
//! hang. Sketch:
//!
//! ```ignore
//! loop {
//!     match do_write(&store) {
//!         Ok(v) => break v,
//!         Err(Error::WriteConflict { wait_for: Some(w), .. }) => {
//!             // WriteTx already dropped by `do_write` returning Err.
//!             if !w.wait_timeout(Duration::from_secs(5)) {
//!                 return Err(/* gave up: conflicting writer still live */);
//!             }
//!             // holder finished — retry against a fresh base
//!         }
//!         Err(Error::WriteConflict { wait_for: None, .. }) => { /* retry */ }
//!         Err(e) => return Err(e),
//!     }
//! }
//! ```
```

- [ ] **Step 2: Verify the doc compiles** (the snippet is `ignore`d, but the crate must still build):

Run: `cargo build && cargo doc --no-deps -p ultima-db 2>&1 | tail -3`
Expected: builds clean, no doc warnings.

- [ ] **Step 3: Commit**

```bash
git add src/intents.rs
git commit -m "docs(intents): retry-with-timeout pattern for CommitWaiter"
```

---

### Task 3: Feature doc + full verification

**Files:**
- Create: `docs/tasks/task42_commit_waiter_timeout.md`
- Modify: `docs/tasks/task21_serializable_isolation.md` (if it references the unbounded-wait convention as open) — check first

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the feature doc** — `docs/tasks/task42_commit_waiter_timeout.md`:

```markdown
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
```

- [ ] **Step 2: Check task21 for an open-convention reference.**

Run: `grep -n "wait()\|CommitWaiter\|no timeout\|convention" docs/tasks/task21_serializable_isolation.md`
Expected: if a "v1 limitations" bullet calls out the unbounded wait as open work, update it to point at task42; if there is no such reference, skip (do not invent one). If a match is found, edit that bullet to append: `Bounded via CommitWaiter::wait_timeout since task42.`

- [ ] **Step 3: Full verification**

Run: `cargo test && cargo test -p ultima-vector && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all pass, zero warnings.

- [ ] **Step 4: Commit**

```bash
git add docs/tasks/task42_commit_waiter_timeout.md docs/tasks/task21_serializable_isolation.md
git commit -m "docs: task42 CommitWaiter bounded wait feature doc"
```
