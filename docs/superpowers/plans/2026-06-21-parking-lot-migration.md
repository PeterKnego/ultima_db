# parking_lot Lock Migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace every `std::sync::{Mutex, RwLock, Condvar}` in the `ultima_db` crate with `parking_lot` equivalents so a panic while a lock is held no longer poisons it and bricks the store.

**Architecture:** Strictly mechanical primitive swap, leaf-to-core (`metrics.rs` → `intents.rs` → `wal.rs` → `store.rs`), one commit per file, green at every step. Three transform shapes only: guard de-`Result`, condvar wait-loop rewrite, and replacing one `unsafe transmute` owned-guard with `parking_lot::ArcMutexGuard`. The hardest file (`store.rs`) lands last and is proven by a new poison-cascade regression test.

**Tech Stack:** Rust, `parking_lot = "0.12"` (feature `arc_lock`), existing `dashmap`, criterion benches, `cargo test` / `cargo clippy`.

**Design spec:** `docs/superpowers/specs/2026-06-21-parking-lot-migration-design.md`

## Global Constraints

- **No behavioral change** to commit ordering, durability, isolation, or the three-phase consistent-persistence machinery. Primitive swap only.
- **`WalPoison` is untouched.** The WAL's intentional fail-stop (`WalPoison::poison` / `inner.wal_poison.check()`) must survive byte-for-byte. Remove *accidental* mutex poisoning, not the *intentional* WAL poison gate.
- **No condvar timeouts.** `IntentWaiter`/`CommitWaiter` and `PromoteGate` keep unbounded wait semantics. (Separate backlog item.)
- **`DashMap` stays.** Only the `Arc<Mutex<()>>` *values* inside `table_locks` migrate, not the map.
- **`Arc`, `atomic::*`, `Once` stay** — only `Mutex`, `RwLock`, `Condvar` migrate.
- **Each file green before commit:** `cargo test`, `cargo test --test store_integration`, `cargo clippy -- -D warnings`.
- Branch: `feat/parking-lot-migration` (already created; spec already committed there).

---

## The three canonical transforms (reference for every task)

**T1 — Guard de-`Result`** (parking_lot guards aren't `LockResult`):
```rust
// before                          // after
x.lock().unwrap()              →   x.lock()
self.inner.read().unwrap()     →   self.inner.read()
self.inner.write().unwrap()    →   self.inner.write()
```

**T2 — Condvar wait-loop** (parking_lot `Condvar::wait` takes `&mut MutexGuard`, returns `()`, never poisons):
```rust
// before (std)                    // after (parking_lot)
let mut g = m.lock().unwrap();     let mut g = m.lock();
while !ready(&g) {                 while !ready(&g) {
    g = cv.wait(g).unwrap();           cv.wait(&mut g);
}                                  }
```
`notify_one` / `notify_all` are unchanged.

**T3 — Owned table-lock guard** (`store.rs` only; removes `unsafe`): see Task 5.

---

## Task 1: Add dependency + migrate `metrics.rs`

`metrics.rs` uses only `RwLock` (no condvars) — the simplest file, establishes the dependency and the T1 pattern.

**Files:**
- Modify: `Cargo.toml` (`[dependencies]`)
- Modify: `src/metrics.rs` (line 10 import; `.read().unwrap()`/`.write().unwrap()` at lines ~151, 231, 234, 243, 245, 293, 295, 309, 311, 329)

**Interfaces:**
- Consumes: nothing.
- Produces: `parking_lot` available crate-wide; `metrics.rs` types (`StoreMetrics`, `TableMetrics`, `IndexMetrics`) keep identical public method signatures — only guard internals change.

- [ ] **Step 1: Add the dependency**

In `Cargo.toml` under `[dependencies]`, add:
```toml
parking_lot = { version = "0.12", features = ["arc_lock"] }
```
(`arc_lock` is needed later by Task 5; add it now so the dep line is final.)

- [ ] **Step 2: Confirm baseline green**

Run: `cargo test --lib metrics 2>&1 | tail -5`
Expected: existing metrics tests PASS (establishes the green baseline before refactor).

- [ ] **Step 3: Swap the import**

In `src/metrics.rs` line 10:
```rust
// before
use std::sync::RwLock;
// after
use parking_lot::RwLock;
```

- [ ] **Step 4: Apply T1 to every guard site in the file**

Remove `.unwrap()` from every `.read()` / `.write()` call (lines ~151, 231, 234, 243, 245, 293, 295, 309, 311, 329). Example at line ~231:
```rust
// before
if let Some(t) = self.tables.read().unwrap().get(name) {
// after
if let Some(t) = self.tables.read().get(name) {
```
Sweep the whole file; no `.read().unwrap()` / `.write().unwrap()` may remain.

- [ ] **Step 5: Verify no std lock import lingers**

Run: `grep -n "std::sync::\(Mutex\|RwLock\|Condvar\)" src/metrics.rs`
Expected: no output.

- [ ] **Step 6: Build, test, lint**

Run: `cargo test --lib metrics && cargo clippy -- -D warnings 2>&1 | tail -5`
Expected: tests PASS, clippy clean (zero warnings).

- [ ] **Step 7: Commit**
```bash
git add Cargo.toml Cargo.lock src/metrics.rs
git commit -m "refactor(metrics): migrate RwLock to parking_lot (no poison)"
```

---

## Task 2: Migrate `intents.rs`

`IntentWaiter` = `Mutex<bool>` + `Condvar` — one T2 wait-loop and a couple of T1 sites.

**Files:**
- Modify: `src/intents.rs` (import line 25; `IntentWaiter::signal_done` ~line 50, `wait` ~line 56, `is_signaled` ~line 64)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `IntentWaiter` / `CommitWaiter` keep identical signatures (`new`, `signal_done`, `wait`, `is_signaled`); only guard internals change.

- [ ] **Step 1: Confirm baseline green**

Run: `cargo test --lib intents 2>&1 | tail -5`
Expected: existing intents tests PASS.

- [ ] **Step 2: Swap the import**

In `src/intents.rs` line 25:
```rust
// before
use std::sync::{Arc, Condvar, Mutex};
// after
use std::sync::Arc;
use parking_lot::{Condvar, Mutex};
```

- [ ] **Step 3: Apply T1 to `signal_done` and `is_signaled`**
```rust
// signal_done (before → after)
let mut d = self.done.lock().unwrap();   →   let mut d = self.done.lock();
// is_signaled (before → after)
*self.done.lock().unwrap()               →   *self.done.lock()
```

- [ ] **Step 4: Apply T2 to `wait`**
```rust
// before
pub(crate) fn wait(&self) {
    let mut d = self.done.lock().unwrap();
    while !*d {
        d = self.cvar.wait(d).unwrap();
    }
}
// after
pub(crate) fn wait(&self) {
    let mut d = self.done.lock();
    while !*d {
        self.cvar.wait(&mut d);
    }
}
```

- [ ] **Step 5: Verify no std lock import lingers**

Run: `grep -n "std::sync::\(Mutex\|RwLock\|Condvar\)\|Condvar, Mutex\b" src/intents.rs | grep -v parking_lot`
Expected: no `std::sync` Mutex/RwLock/Condvar references.

- [ ] **Step 6: Build, test, lint**

Run: `cargo test --lib intents && cargo clippy -- -D warnings 2>&1 | tail -5`
Expected: tests PASS, clippy clean.

- [ ] **Step 7: Commit**
```bash
git add src/intents.rs
git commit -m "refactor(intents): migrate IntentWaiter Mutex/Condvar to parking_lot"
```

---

## Task 3: Migrate `wal.rs`

Multiple lock-bearing structs: `WalSyncState` (`Mutex<()>` + `Condvar`, ~line 491), `WalDurability` (`Mutex<DurabilityWaiters>` + `Condvar`, ~line 1046), the inline-sink `Arc<Mutex<Box<dyn WalSink>>>`, and the per-batch `sync_state` condvar (~line 1644). `WalPoison` is a separate atomic-based mechanism — **do not touch it**.

**Files:**
- Modify: `src/wal.rs` (imports + all `Mutex`/`Condvar`/`RwLock` guard sites; wait-loops at ~535, 1145; publish/notify sites at ~1085, 1102, 1116, 1518, 1540, 1661, 1690, 1699)

**Interfaces:**
- Consumes: `parking_lot` (Task 1).
- Produces: `WalSyncState`, `WalDurability`, `SyncWaiter`, `WalHandle` keep identical signatures; `WalPoison` API and semantics unchanged.

- [ ] **Step 1: Confirm baseline green**

Run: `cargo test --lib wal --features persistence 2>&1 | tail -8`
Expected: existing WAL tests PASS.

- [ ] **Step 2: Add the import**

At the top of `src/wal.rs` add `use parking_lot::{Condvar, Mutex};` (keep `std::sync::Arc` and `std::sync::atomic::*`). Where structs spell out `std::sync::Mutex` / `std::sync::Condvar` inline (e.g. `WalSyncState`, `WalDurability`, `SyncWaiter::InlineSync`'s `Arc<std::sync::Mutex<Box<dyn WalSink>>>`), change those paths to the bare `Mutex` / `Condvar`.

- [ ] **Step 3: Apply T1 to every `.lock()/.read()/.write().unwrap()`**

Sweep all sites. Examples:
```rust
// inline sink (SyncWaiter::wait, ~line 533)
let mut s = sink.lock().unwrap();   →   let mut s = sink.lock();
// publish (~1084)
let mut w = self.inner.lock().unwrap();   →   let mut w = self.inner.lock();
```

- [ ] **Step 4: Apply T2 to the two wait-loops**

`WalSyncState` waiter (`SyncWaiter::wait`, ~line 535):
```rust
// before
let mut guard = state.mu.lock().unwrap();
loop {
    if state.fsynced_epoch.load(Ordering::Acquire) >= epoch { return Ok(()); }
    if poison.is_poisoned() { return Err(poison.error()); }
    guard = state.condvar.wait(guard).unwrap();
}
// after
let mut guard = state.mu.lock();
loop {
    if state.fsynced_epoch.load(Ordering::Acquire) >= epoch { return Ok(()); }
    if poison.is_poisoned() { return Err(poison.error()); }
    state.condvar.wait(&mut guard);
}
```
`WalDurability::wait` (~line 1145):
```rust
// before
let mut guard = self.inner.lock().unwrap();
loop {
    if self.durable_version.load(Ordering::Acquire) >= version { return Ok(()); }
    if let Some((ev, msg)) = &guard.last_error && *ev >= version {
        return Err(Error::Persistence(msg.clone()));
    }
    if self.closed.load(Ordering::Acquire) {
        return Err(Error::Persistence("WAL closed before version became durable".into()));
    }
    guard = self.condvar.wait(guard).unwrap();
}
// after — identical except:
    let mut guard = self.inner.lock();
    // ...
    self.condvar.wait(&mut guard);
```

- [ ] **Step 5: Confirm `WalPoison` was NOT modified**

Run: `git diff src/wal.rs | grep -i poison`
Expected: no lines touching `WalPoison`'s struct/impl (it uses `AtomicBool`, not the migrated locks). If any appear, revert just those.

- [ ] **Step 6: Verify no std lock lingers**

Run: `grep -n "std::sync::\(Mutex\|RwLock\|Condvar\)" src/wal.rs`
Expected: no output.

- [ ] **Step 7: Build, test, lint (with persistence feature)**

Run: `cargo test --lib wal --features persistence && cargo clippy --features persistence -- -D warnings 2>&1 | tail -5`
Expected: tests PASS, clippy clean.

- [ ] **Step 8: Commit**
```bash
git add src/wal.rs
git commit -m "refactor(wal): migrate Mutex/Condvar to parking_lot (WalPoison untouched)"
```

---

## Task 4: Migrate `store.rs` core locks (proven by poison-cascade test)

The core: `inner: RwLock<StoreInner>`, `checkpoint_lock: Mutex<()>`, and `PromoteGate` (`Mutex<u64>` + `Condvar`). This task is **test-first**: write the poison-cascade regression test against the still-`std` store (red), then migrate (green). The `TableLockGuards` `unsafe` block is deferred to Task 5.

**Files:**
- Modify: `src/store.rs` (import line 7; `PromoteGate` ~185–222; `inner`/`checkpoint_lock` guard sites — ~57 of them; new `#[cfg(test)]` seam + test)

**Interfaces:**
- Consumes: `parking_lot` (Task 1).
- Produces: a `#[cfg(test)] fn panic_with_inner_write_held(&self)` seam on `Store`; all `Store` public methods keep identical signatures. `MutexGuard`/`RwLock*Guard` type names imported from `parking_lot` for the still-`std` `TableLockGuards` (Task 5 finishes that).

- [ ] **Step 1: Write the failing poison-cascade test**

Add to the `#[cfg(test)]` module in `src/store.rs`. First the seam (place near `Store`'s impl, gated):
```rust
#[cfg(test)]
impl Store {
    /// Test-only: take the `inner` write lock and panic while holding it,
    /// to exercise the lock's panic-while-held behavior.
    fn panic_with_inner_write_held(&self) {
        let _guard = self.inner.write();
        panic!("injected panic while holding inner write lock");
    }
}
```
Then the test:
```rust
#[test]
fn panic_holding_inner_lock_does_not_brick_store() {
    let store = Store::default();
    // A panic while `inner` is held must NOT poison the lock.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.panic_with_inner_write_held();
    }));
    assert!(result.is_err(), "the injected panic should have unwound");

    // The store must still serve traffic. On std these `.read()/.write().unwrap()`
    // calls panic (PoisonError); on parking_lot they succeed.
    let _read = store.begin_read(None).expect("begin_read after panic");
    let mut wtx = store.begin_write(None).expect("begin_write after panic");
    wtx.commit().expect("commit after panic");
}
```

- [ ] **Step 2: Run the test — verify it FAILS on the still-`std` store**

Run: `cargo test --lib panic_holding_inner_lock_does_not_brick_store 2>&1 | tail -15`
Expected: FAIL — the post-panic `begin_read`/`begin_write` panic with a poisoned-lock error (`PoisonError` / "poisoned"). This proves the test actually detects the cascade.

- [ ] **Step 3: Swap the import**

In `src/store.rs` line 7:
```rust
// before
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
// after
use std::sync::Arc;
use parking_lot::{Mutex, MutexGuard, RwLock};
```

- [ ] **Step 4: Migrate `PromoteGate` (T1 + T2)**

```rust
struct PromoteGate {
    turn: Mutex<u64>,
    cv: Condvar,            // was std::sync::Condvar
}
// new(): Condvar::new()   (was std::sync::Condvar::new())
// is_turn:     *self.turn.lock() == ticket
// current_turn:*self.turn.lock()
// wait_turn:
fn wait_turn(&self, ticket: u64) {
    let mut turn = self.turn.lock();
    while *turn != ticket {
        self.cv.wait(&mut turn);
    }
}
// advance:
fn advance(&self) {
    let mut turn = self.turn.lock();
    *turn += 1;
    self.cv.notify_all();
}
```
Add `Condvar` to the `parking_lot` import in Step 3 if not already present (`use parking_lot::{Condvar, Mutex, MutexGuard, RwLock};`).

- [ ] **Step 5: Apply T1 to every `inner` / `checkpoint_lock` site**

Sweep all ~57 `inner.read().unwrap()` / `inner.write().unwrap()` and `checkpoint_lock.lock().unwrap()` sites; drop the `.unwrap()`. Example (line ~466):
```rust
let mut inner = self.inner.write().unwrap();   →   let mut inner = self.inner.write();
```
Leave the `TableLockGuards` `unsafe transmute` block (~1408–1445) as-is for now — it will still compile because `MutexGuard`/`Mutex` now resolve to `parking_lot`, and `transmute` is type-agnostic. (Task 5 replaces it.)

- [ ] **Step 6: Re-entrant read-lock scan (parking_lot is not recursive)**

Run: `grep -n "inner.read()\|inner.write()" src/store.rs`
Inspect each read path (`begin_read`, `begin_write`, `gc`, `latest_version`) for a second `inner.read()` taken while a first read guard from the same call is still in scope. parking_lot `RwLock` can deadlock on recursive reads if a writer parks between them.
Expected: none found (each method takes one guard and uses it). If one IS found, restructure to a single acquisition (hoist the second read into the first critical section) and note it in the commit message.

- [ ] **Step 7: Run the poison-cascade test — verify it now PASSES**

Run: `cargo test --lib panic_holding_inner_lock_does_not_brick_store 2>&1 | tail -8`
Expected: PASS — `begin_read`/`begin_write`/`commit` all succeed after the injected panic.

- [ ] **Step 8: Verify no std lock lingers (except the Task-5 transmute) and full suite green**

Run: `grep -n "std::sync::\(Mutex\|RwLock\|Condvar\)" src/store.rs`
Expected: no output.
Run: `cargo test && cargo test --test store_integration && cargo clippy -- -D warnings 2>&1 | tail -8`
Expected: all PASS, clippy clean.

- [ ] **Step 9: Commit**
```bash
git add src/store.rs
git commit -m "refactor(store): migrate inner/checkpoint/PromoteGate to parking_lot

Adds panic_holding_inner_lock_does_not_brick_store regression test
(red on std, green on parking_lot). TableLockGuards unsafe block
replaced in the next commit."
```

---

## Task 5: Replace `TableLockGuards` `unsafe transmute` with `ArcMutexGuard`

The owned table-lock guards (`store.rs` ~1390–1447) currently transmute borrowed `MutexGuard`s to `'static` inside `ManuallyDrop`. `parking_lot::Mutex::lock_arc()` returns an owned `ArcMutexGuard` — no transmute, no `ManuallyDrop`, no `unsafe`.

**Files:**
- Modify: `src/store.rs` (`table_locks` field type ~319; `TableLockGuards`/`LockSlot`/`acquire`/`Drop` ~1390–1447)

**Interfaces:**
- Consumes: `parking_lot` `arc_lock` feature (Task 1 added it).
- Produces: `TableLockGuards` keeps `empty()` / `acquire(arcs: Vec<Arc<Mutex<()>>>)`; the stored guard type changes to `ArcMutexGuard<parking_lot::RawMutex, ()>`.

- [ ] **Step 1: Confirm baseline green (concurrent-writer tests)**

Run: `cargo test --lib multi 2>&1 | tail -8`
Expected: existing MultiWriter/concurrent tests PASS.

- [ ] **Step 2: Rewrite `LockSlot` + `acquire` to use `lock_arc()`**

```rust
use parking_lot::ArcMutexGuard;
use parking_lot::lock_api::RawMutex as _; // if needed; otherwise parking_lot::RawMutex

struct TableLockGuards {
    // ArcMutexGuard owns its Arc and is 'static by construction — no
    // ManuallyDrop, no transmute. Dropped in reverse order by Vec::drop.
    guards: Vec<ArcMutexGuard<parking_lot::RawMutex, ()>>,
}

impl TableLockGuards {
    fn empty() -> Self {
        Self { guards: Vec::new() }
    }

    fn acquire(arcs: Vec<Arc<Mutex<()>>>) -> Self {
        let guards = arcs.into_iter().map(|arc| arc.lock_arc()).collect();
        Self { guards }
    }
}
```

- [ ] **Step 3: Delete the custom `Drop` and `LockSlot`**

`ArcMutexGuard`'s own `Drop` releases the lock and the `Arc`; `Vec` drops elements in order. Remove the entire `impl Drop for TableLockGuards`, the `LockSlot` struct, and the now-unused `ManuallyDrop` usage. Update the doc-comment to describe the `ArcMutexGuard` approach (drop the "`'static` bookkeeping trick" / "Safety relies on…" paragraphs).

- [ ] **Step 4: Verify no `unsafe` and no `transmute`/`ManuallyDrop` remain in the file**

Run: `grep -n "unsafe\|transmute\|ManuallyDrop" src/store.rs`
Expected: no output (if `unsafe` is used elsewhere in store.rs unrelated to this, confirm those are pre-existing and not the table-lock block — there should be none from this machinery).

- [ ] **Step 5: Build, test, lint**

Run: `cargo test && cargo test --test store_integration && cargo clippy -- -D warnings 2>&1 | tail -8`
Expected: all PASS, clippy clean. Concurrent-writer tests (`cargo test --lib multi`) specifically must pass — they exercise canonical-order lock acquisition.

- [ ] **Step 6: Commit**
```bash
git add src/store.rs
git commit -m "refactor(store): replace TableLockGuards transmute with ArcMutexGuard

Removes the unsafe 'static-lifetime transmute + ManuallyDrop; parking_lot
lock_arc() yields an owned, 'static guard directly."
```

---

## Task 6: Whole-crate verification + feature doc

Final gates per spec §6, plus the canonical per-feature doc (CLAUDE.md workflow).

**Files:**
- Create: `docs/tasks/task39_parking_lot_migration.md` (confirm the next free task number with `ls docs/tasks/`; adjust if 39 is taken)

**Interfaces:**
- Consumes: all prior tasks.
- Produces: none (verification + docs).

- [ ] **Step 1: Confirm no std lock primitives remain crate-wide**

Run: `grep -rn "std::sync::\(Mutex\|RwLock\|Condvar\)" src/`
Expected: no output.

- [ ] **Step 2: Full default-feature gate**

Run: `cargo test && cargo test --test store_integration && cargo clippy -- -D warnings 2>&1 | tail -8`
Expected: all PASS, zero warnings.

- [ ] **Step 3: Persistence-feature gate**

Run: `cargo test --features persistence && cargo clippy --features persistence -- -D warnings 2>&1 | tail -8`
Expected: all PASS, zero warnings.

- [ ] **Step 4: Workspace gate (per project memory — root `cargo test` misses member crates)**

Run: `cargo test -p ultima-vector && cargo build --workspace 2>&1 | tail -8`
Expected: PASS / clean build. No lock types cross the crate boundary, so no cross-crate breakage is expected — confirm, don't assume.

- [ ] **Step 5: Perf gate**

Run: `make perf/check 2>&1 | tail -20`
Expected: baselines within noise. Per the bench-A/B memory, the sandbox noise floor is wide (±2× on the Claude sandbox); if a regression shows, re-run on the bench host before treating it as real. The expected delta is small (single-digit-% on uncontended lock ops). Record the result.

- [ ] **Step 6: Write the feature doc**

Create `docs/tasks/task39_parking_lot_migration.md` capturing: motivation (panic-poison cascade bricks the store), the three transforms, the `ArcMutexGuard` unsafe-removal, the two parking_lot behavioral differences (no recursive reads — scan result from Task 4 Step 6; writer-preferring fairness — perf-gate result from Step 5), the poison-cascade regression test, and the non-goals (timeouts, WalPoison, DashMap). Reference the spec at `docs/superpowers/specs/2026-06-21-parking-lot-migration-design.md`.

- [ ] **Step 7: Commit**
```bash
git add docs/tasks/task39_parking_lot_migration.md
git commit -m "docs(task39): parking_lot migration — decisions, behavioral diffs, perf result"
```

- [ ] **Step 8: Hand off**

Migration complete on `feat/parking-lot-migration`. Surface the perf-gate number and the re-entrant-read scan result to the user, then offer to open a PR (per `finishing-a-development-branch`).

---

## Self-Review

**Spec coverage** (spec §-by-§):
- §1 goal / non-goals → Global Constraints + Task scoping (WalPoison untouched: Task 3 Step 5; timeouts excluded; DashMap kept).
- §2 scope inventory → Tasks 1–5, leaf-to-core, one file each.
- §3 three transforms → "canonical transforms" block + applied per task (T1/T2 everywhere; T3 in Task 5).
- §4.1 recursive reads → Task 4 Step 6. §4.2 fairness → Task 6 Step 5. §4.3 dependency → Task 1 Step 1.
- §5 execution strategy → task order + one-commit-per-file.
- §6 testing → Task 4 (red-then-green headline test), Tasks 1–5 (existing suites), Task 6 (workspace + perf).
- §7 definition of done → Task 6 gates + feature doc.

All spec sections map to a task. No gaps.

**Placeholder scan:** No TBD/TODO/"handle edge cases"; every code step shows real before/after pulled from the current source. Task number `39` flagged as "confirm with `ls docs/tasks/`" (genuinely assigned at write time, not a placeholder).

**Type consistency:** `inner.read()/.write()`, `Condvar::wait(&mut guard)`, `ArcMutexGuard<parking_lot::RawMutex, ()>`, `TableLockGuards::{empty, acquire}`, and the seam `panic_with_inner_write_held` / test `panic_holding_inner_lock_does_not_brick_store` are named identically wherever referenced across tasks.
