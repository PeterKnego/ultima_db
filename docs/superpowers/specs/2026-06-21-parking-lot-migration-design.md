# Design: `parking_lot` lock migration

**Date:** 2026-06-21
**Status:** Approved design — ready for implementation plan
**Backlog origin:** "Deferred review items" (2026-06-11 deep review) — *Mutex-poisoning
cascade: any panic in commit poisons `inner` and bricks the store; fix implies a
`parking_lot` migration — invasive.* Also referenced in the WAL durability next-steps
backlog as the robustness+perf "twofer."

---

## 1. Goal & non-goals

### Goal

Replace every `std::sync::{Mutex, RwLock, Condvar}` in the `ultima_db` crate with the
`parking_lot` equivalent, so that **a panic while a lock is held no longer poisons the
lock**. Today a single panic on the commit path poisons `inner` and every subsequent
`.unwrap()` on that lock panics too — one transient bug permanently bricks the store.
parking_lot locks have no poisoning: a panic-while-held releases the lock cleanly and
the next acquirer proceeds.

Secondary wins (real but not the motivation):

- Cheaper uncontended fast path and a single-word footprint on the hot `inner` lock.
- Removal of one `unsafe { transmute }` block (the owned table-lock guard, see §4.3).

### Non-goals (explicitly out of scope)

- **No condvar timeouts.** `CommitWaiter` (`intents.rs`) and `PromoteGate` (`store.rs`)
  keep their current unbounded wait semantics. The "`CommitWaiter::wait()` has no
  timeout" backlog item stays a separate task, even though parking_lot's `Condvar`
  would make `wait_for` cheap. This migration is strictly mechanical.
- **No change to `WalPoison`.** The WAL's deliberate fail-stop after an fsync failure
  (`wal.rs`, `WalPoison::poison` / `inner.wal_poison.check()`) is an intentional
  mechanism and must survive byte-for-byte. We remove *accidental* mutex poisoning,
  not the *intentional* WAL poison gate. These are different mechanisms; do not
  conflate them.
- **No `DashMap` replacement.** `table_locks: DashMap<String, Arc<Mutex<()>>>` keeps
  `DashMap` as its concurrent map. Only the `Arc<Mutex<()>>` *values* inside it migrate
  to `parking_lot::Mutex`. (The `table_locks` unbounded-growth item is a separate
  backlog task and is not addressed here.)
- **No behavioral change** to commit ordering, durability semantics, isolation levels,
  or the three-phase consistent-persistence machinery. This is a primitive swap.

---

## 2. Scope inventory (what changes, by file)

Migrate leaf-to-core so the trickiest file lands last on a fully-migrated base
(Approach A, §5). Lock-bearing line counts are approximate (grep of
`Mutex<|RwLock<|Condvar|.lock()|.read()|.write()`):

| Order | File | ~sites | What's there |
|------:|------|-------:|--------------|
| 1 | `metrics.rs` | 12 | Plain `Mutex` counters. Leaf — no condvars, simplest. |
| 2 | `intents.rs` | 7 | `CommitWaiter`: `Mutex<bool>` + `Condvar`. One wait-loop rewrite. |
| 3 | `wal.rs` | 27 | `Mutex`/`Condvar` in the durability waiter + `sync_state` (≈ lines 491, 1046, 1644). Most condvar sites. `WalPoison` untouched. |
| 4 | `store.rs` | 79 | Core: `inner: RwLock<StoreInner>`, `checkpoint_lock: Mutex<()>`, `PromoteGate` (`Mutex<u64>` + `Condvar`, ≈ line 185), `TableLockSet` owned-guard machinery (≈ lines 1408–1426). |

Total ≈ 90 `.unwrap()` poison points across the crate.

---

## 3. The three mechanical transforms

Every change is one of these three shapes. Nothing else should change.

### 3.1 Guard de-`Result`

parking_lot guards are returned directly, not wrapped in `LockResult`.

```rust
// before
let g = x.lock().unwrap();
let r = self.inner.read().unwrap();
let mut w = self.inner.write().unwrap();
// after
let g = x.lock();
let r = self.inner.read();
let mut w = self.inner.write();
```

~90 sites. Mechanical and low-risk — the `.unwrap()` simply disappears.

### 3.2 Condvar wait-loop

parking_lot's `Condvar::wait` takes `&mut MutexGuard`, does not consume/return it, and
cannot poison. The surrounding predicate loop is unchanged.

```rust
// before  (std)
let mut guard = self.state.lock().unwrap();
while !ready(&guard) {
    guard = self.cv.wait(guard).unwrap();
}
// after  (parking_lot)
let mut guard = self.state.lock();
while !ready(&guard) {
    self.cv.wait(&mut guard);
}
```

`notify_one`/`notify_all` are unchanged. Sites: `intents.rs` (CommitWaiter),
`wal.rs` (durability waiter + `sync_state`), `store.rs` (PromoteGate).

### 3.3 Owned table-lock guard (removes `unsafe`)

`store.rs:~1408–1426` currently holds an owned, lock-set-lifetime guard by transmuting
a borrowed `MutexGuard` to `'static` and storing it in `ManuallyDrop`:

```rust
// before
let guard: MutexGuard<'_, ()> = arc.lock().unwrap();
let guard: MutexGuard<'static, ()> = unsafe { std::mem::transmute(guard) };
// stored as ManuallyDrop<MutexGuard<'static, ()>> alongside the Arc
```

parking_lot's `arc_lock` feature provides `Mutex::lock_arc() -> ArcMutexGuard<RawMutex, ()>`,
which genuinely owns its `Arc` and is `'static` by construction:

```rust
// after
let guard: ArcMutexGuard<parking_lot::RawMutex, ()> = arc.lock_arc();
// stored directly — no ManuallyDrop, no transmute, no unsafe
```

The `TableLockSet` sorted-acquisition (deadlock-avoidance by acquiring table locks in a
canonical order) and `Drop` ordering are unchanged — only the storage of each owned
guard changes. This deletes the file's `unsafe` block.

---

## 4. Risks & the two real behavioral differences

parking_lot is a near-drop-in, but two semantic differences are real and get explicit
verification steps, not assumptions.

### 4.1 No recursive/re-entrant read locks  *(must verify)*

parking_lot's `RwLock` is **not** recursive: a thread holding a read guard that then
takes a *second* read guard can deadlock if a writer is parked between the two
acquisitions. std tolerates this; parking_lot may not.

**Action:** before migrating `store.rs`, scan the read paths (`begin_read`,
`begin_write`, `gc`, `latest_version`, and anything reading `inner` while a guard is
live) for a re-entrant `inner.read()` taken while another `inner` read guard is still
in scope. If any exist, restructure to a single acquisition (pass the guard down, or
hoist the second read into the first critical section). Document any found in the plan.

### 4.2 Writer-preferring fairness  *(note + measure)*

parking_lot's `RwLock` favors writers; std's policy is unspecified (often
reader-preferring). This changes scheduling under heavy read contention but **not
correctness**.

**Action:** rely on the existing concurrency/integration tests for correctness and a
`make perf/check` run to confirm no throughput regression on the baselines. No code
change expected.

### 4.3 Dependency

Add to `Cargo.toml`:

```toml
parking_lot = { version = "0.12", features = ["arc_lock"] }
```

`arc_lock` is required for `lock_arc()` (§3.3). No other features needed.

---

## 5. Execution strategy (Approach A — incremental, file-by-file)

Migrate **leaf-to-core**, one commit per file, green at every step:

1. `Cargo.toml` dependency + `metrics.rs`
2. `intents.rs`
3. `wal.rs`
4. `store.rs` (includes the re-entrant-read scan §4.1 and the `unsafe` removal §3.3)

After **each** file: `cargo test`, `cargo test --test store_integration`, and
`cargo clippy -- -D warnings` must be green. The leaf-to-core order means the hardest
file (`store.rs`) lands on a base where everything beneath it is already migrated and
verified. The migration is bisectable if a regression appears.

Rejected alternatives:

- **Big-bang single commit** — large diff, can't run tests until it all compiles, a
  single condvar-semantics mistake is hard to isolate.
- **Wrapper newtype shim** — extra indirection for no benefit; parking_lot's API is
  already close to std. Over-engineering (YAGNI).

---

## 6. Testing & acceptance

### 6.1 Headline regression test (proves the fix)

Add a `#[cfg(test)]`-only seam that forces a panic **while `inner` is held**, wrapped in
`std::panic::catch_unwind`, then asserts the store still serves traffic:

```text
1. Build a Store.
2. catch_unwind(|| { /* take inner write lock, then panic! while held */ });
3. assert begin_read(None)  succeeds (no PoisonError panic).
4. assert begin_write(None) + a trivial commit succeeds.
```

Acceptance: this test **fails on `main`** (std poisons `inner`; step 3/4 panic on
`.unwrap()`) and **passes after** the migration. We confirm it red-then-green — a test
that only ever passes proves nothing.

### 6.2 Existing suite

Green after each file:

- `cargo test`
- `cargo test --test store_integration`
- `cargo clippy -- -D warnings`

### 6.3 Workspace gate (per project memory)

Root `cargo test` misses member crates. After the migration, run `cargo test -p
ultima-vector` and a workspace build. No lock types cross the crate boundary (the
`ultima_journal`/`ultima_cluster` lock work is independent), so no cross-crate breakage
is expected — confirm, don't assume.

### 6.4 Perf gate

`make perf/check` to confirm the hot-`inner` change and writer-preferring fairness don't
regress committed baselines. The expected perf delta is small (single-digit-percent on
uncontended lock ops); the migration is a robustness fix with a perf bonus, not a perf
optimization.

---

## 7. Definition of done

- [ ] `parking_lot` (with `arc_lock`) added; no `std::sync::{Mutex, RwLock, Condvar}`
      remain in `metrics.rs`, `intents.rs`, `wal.rs`, `store.rs`. (`Arc`, `atomic`,
      `Once`, etc. stay.)
- [ ] The `unsafe { transmute }` owned-guard block in `store.rs` is gone, replaced by
      `ArcMutexGuard`.
- [ ] `WalPoison` fail-stop behavior unchanged and still tested.
- [ ] §4.1 re-entrant-read scan completed; findings recorded.
- [ ] Headline poison-cascade test confirmed red on `main`, green after.
- [ ] `cargo test`, `store_integration`, `clippy -D warnings`, `cargo test -p
      ultima-vector`, workspace build, and `make perf/check` all green.
- [ ] One commit per file (leaf-to-core), each independently green.
- [ ] `docs/tasks/taskXX_parking_lot_migration.md` written per the CLAUDE.md feature
      workflow; this spec retained as design history.
