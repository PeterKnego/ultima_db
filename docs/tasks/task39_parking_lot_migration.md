# Task 39: `parking_lot` Lock Migration

**Date:** 2026-06-21
**Branch:** `feat/parking-lot-migration`
**Spec:** `docs/superpowers/specs/2026-06-21-parking-lot-migration-design.md`
**Plan:** `docs/superpowers/plans/2026-06-21-parking-lot-migration.md`

---

## Motivation

`std::sync::Mutex` and `RwLock` have lock *poisoning*: if a thread panics while holding a
lock, every subsequent `.lock()/.read()/.write().unwrap()` on that guard also panics with a
`PoisonError`. In UltimaDB this creates a cascade: a single transient panic on the commit
path (e.g., an OOM or an assertion inside `inner.write()`) poisons `inner`, and every
subsequent `begin_read`, `begin_write`, or `commit` call panics too — the store is
permanently bricked until the process restarts.

`parking_lot` locks have no poisoning: a panic-while-held unwinds the stack, drops the
guard, and releases the lock cleanly. The next acquirer proceeds normally.

Secondary wins (real, not the motivation):
- Cheaper uncontended fast path and single-word lock footprint on the hot `inner` lock.
- Removal of one `unsafe { transmute }` block (the owned table-lock guard, see §Transform 3).

**Origin:** Deferred from the 2026-06-11 deep review: *"Mutex-poisoning cascade: any panic
in commit poisons `inner` and bricks the store; fix implies a `parking_lot` migration —
invasive."* Also referenced in the WAL durability next-steps backlog as the
robustness+perf "twofer."

---

## Scope

Four files migrated, leaf-to-core (one commit per file, green at every step):

| Order | File | ~Sites | What's there |
|------:|------|-------:|--------------|
| 1 | `src/metrics.rs` | 12 | Plain `RwLock` counters. Leaf — no condvars. |
| 2 | `src/intents.rs` | 7 | `CommitWaiter`: `Mutex<bool>` + `Condvar`. One wait-loop. |
| 3 | `src/wal.rs` | 27 | `Mutex`/`Condvar` in durability waiter + `sync_state`. |
| 4 | `src/store.rs` | 79 | Core: `inner: RwLock<StoreInner>`, `checkpoint_lock: Mutex<()>`, `PromoteGate` (`Mutex<u64>` + `Condvar`), `TableLockGuards` owned-guard machinery. |

Total: ~90 `.unwrap()` poison points removed crate-wide.

Dependency added to `Cargo.toml`:
```toml
parking_lot = { version = "0.12", features = ["arc_lock"] }
```
(`arc_lock` is required for `lock_arc()` in Transform 3.)

---

## The Three Mechanical Transforms

Every change is one of these three shapes. Nothing else changed.

### T1 — Guard de-`Result`

parking_lot guards are returned directly, not wrapped in `LockResult`. Applied at ~90
sites across the four files.

```rust
// before (std)
let g = x.lock().unwrap();
let r = self.inner.read().unwrap();
let mut w = self.inner.write().unwrap();

// after (parking_lot)
let g = x.lock();
let r = self.inner.read();
let mut w = self.inner.write();
```

### T2 — Condvar wait-loop

parking_lot's `Condvar::wait` takes `&mut MutexGuard` (does not consume/return it) and
cannot poison. Applied in `intents.rs` (`CommitWaiter`), `wal.rs` (durability waiter +
`sync_state`), and `store.rs` (`PromoteGate`).

```rust
// before (std)
let mut guard = self.state.lock().unwrap();
while !ready(&guard) {
    guard = self.cv.wait(guard).unwrap();
}

// after (parking_lot)
let mut guard = self.state.lock();
while !ready(&guard) {
    self.cv.wait(&mut guard);
}
```

`notify_one` / `notify_all` are unchanged.

### T3 — Owned table-lock guard (removes `unsafe`)

`store.rs` held an owned, lock-set-lifetime guard by transmuting a borrowed `MutexGuard`
to `'static` inside `ManuallyDrop`:

```rust
// before (std + unsafe)
let guard: MutexGuard<'_, ()> = arc.lock().unwrap();
let guard: MutexGuard<'static, ()> = unsafe { std::mem::transmute(guard) };
// stored as ManuallyDrop<MutexGuard<'static, ()>> alongside the Arc
```

`parking_lot`'s `arc_lock` feature provides `Mutex::lock_arc() -> ArcMutexGuard<RawMutex, ()>`,
which genuinely owns its `Arc` and is `'static` by construction:

```rust
// after (parking_lot — no lifetime workaround needed)
let guard: ArcMutexGuard<parking_lot::RawMutex, ()> = arc.lock_arc();
// stored directly in Vec<ArcMutexGuard<...>>
```

The `TableLockGuards` sorted-acquisition (canonical key order to prevent deadlock) and
`Vec` drop ordering are unchanged. This transform deleted the file's `unsafe` block, the
`LockSlot` struct, the custom `impl Drop for TableLockGuards`, and the `ManuallyDrop`
usage.

---

## The Two Real Behavioral Differences

parking_lot is nearly a drop-in, but two semantic differences received explicit
verification, not assumptions.

### 4.1 No recursive/re-entrant read locks — scan result: none found

parking_lot's `RwLock` is not recursive: a thread holding a read guard that then takes a
second read guard can deadlock if a writer is parked between the two acquisitions. `std`
tolerates this; parking_lot may not.

**Scan performed (Task 4 Step 6):** `grep -n "inner.read()\|inner.write()" src/store.rs`
inspected every read path (`begin_read`, `begin_write`, `gc`, `latest_version`, and all
commit machinery). Result: **no re-entrant reads found.** Every method takes one guard and
uses it through a single critical section; no path holds an `inner` read guard and then
re-acquires `inner.read()` while the first is still in scope. No restructuring was needed.

### 4.2 Writer-preferring fairness — perf gate: PASS

parking_lot's `RwLock` favors writers; `std`'s policy is unspecified (often
reader-preferring). This changes scheduling under heavy read contention but not correctness.

**Perf gate result (`make perf/check` on sandbox, 2026-06-21):**
- `smr-apply-microbench`: perf check OK (10 metrics within tolerance)
- `mw-commit-microbench`: perf check OK (7 metrics within tolerance)

Both checks passed within baseline tolerance. Note: the sandbox noise floor is wide (±2×
is normal per project bench-A/B methodology); a bench-host re-run should be done before
treating any future swing as a real regression. The migration is a robustness fix; the
expected perf delta is small (single-digit-percent on uncontended lock ops).

---

## Headline Regression Test

The poison-cascade regression test (`panic_holding_inner_lock_does_not_brick_store` in
`src/store.rs`) was written red-then-green to prove the fix actually works:

1. A test-only seam `panic_with_inner_write_held` takes `inner.write()` and panics while
   holding it.
2. The test wraps the seam in `std::panic::catch_unwind`.
3. After the unwound panic, `begin_read(None)`, `begin_write(None)`, and `commit()` are
   asserted to succeed.

**On `main` (std):** the test *fails* — the post-panic `begin_read`/`begin_write` panics
with a `PoisonError`, confirming the cascade was real.

**After migration (parking_lot):** the test *passes* — the store serves traffic normally
after the injected panic.

---

## Non-Goals (Explicitly Out of Scope)

- **No condvar timeouts.** `CommitWaiter` (`intents.rs`) and `PromoteGate` (`store.rs`)
  keep their current unbounded wait semantics. The "`CommitWaiter::wait()` has no timeout"
  backlog item stays a separate task, even though parking_lot's `Condvar::wait_for` would
  make it cheap. This migration is strictly mechanical.

- **`WalPoison` untouched.** The WAL's deliberate fail-stop after an fsync failure
  (`wal.rs`, `WalPoison::poison` / `inner.wal_poison.check()`) is an intentional mechanism
  and survived byte-for-byte. This migration removes *accidental* mutex poisoning, not the
  *intentional* WAL poison gate. These are different mechanisms.

- **`DashMap` kept.** `table_locks: DashMap<String, Arc<Mutex<()>>>` keeps `DashMap` as
  its concurrent map. Only the `Arc<Mutex<()>>` values inside it migrated to
  `parking_lot::Mutex`. The `table_locks` unbounded-growth item remains a separate backlog
  task.

- **No behavioral change** to commit ordering, durability semantics, isolation levels, or
  the three-phase consistent-persistence machinery.

---

## Verification Gates (Task 6)

All gates run on `feat/parking-lot-migration` after all five commits:

| Gate | Command | Result |
|------|---------|--------|
| No std locks remain | `grep -rn "std::sync::{Mutex,RwLock,Condvar}" src/` | No output |
| Default-feature tests | `cargo test` | All passed |
| Integration tests | `cargo test --test store_integration` | 80 passed |
| Default clippy | `cargo clippy -- -D warnings` | Clean (0 warnings) |
| Persistence tests | `cargo test --features persistence` | All passed |
| Persistence clippy | `cargo clippy --features persistence -- -D warnings` | Clean (0 warnings) |
| Workspace member | `cargo test -p ultima-vector` | 6 passed |
| Workspace build | `cargo build --workspace` | Pre-existing failure in `ultima-vector` (serde bounds on `VectorRow`) — confirmed present on `main` before this migration; not introduced here |
| Perf gate | `make perf/check` | PASS (smr-apply: 10/10, mw-commit: 7/7) |
| No unsafe/transmute in store.rs | `grep -n "unsafe\|transmute\|ManuallyDrop" src/store.rs` | No output |

**Workspace build note:** `cargo build --workspace` fails with serde trait-bound errors on
`VectorRow<Meta>` in `ultima-vector`. This failure exists on `main` before any parking_lot
changes and is unrelated to this migration. The per-crate check `cargo test -p ultima-vector`
passes independently (it does not enable the persistence feature).

---

## Commits

```
cc74d3f refactor(metrics): migrate RwLock to parking_lot (no poison)
db05877 refactor(intents): migrate IntentWaiter Mutex/Condvar to parking_lot
e718aed refactor(wal): migrate Mutex/Condvar to parking_lot (WalPoison untouched)
37790df refactor(store): migrate inner/checkpoint/PromoteGate to parking_lot
ed150f9 refactor(store): replace TableLockGuards transmute with ArcMutexGuard
```
