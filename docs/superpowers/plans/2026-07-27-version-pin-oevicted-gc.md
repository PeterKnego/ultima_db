# VersionPin API + O(evicted) Snapshot GC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `Send`-able `Store::pin_version` → `VersionPin` handle that keeps one snapshot version alive across GC, and rewrite `gc_inner` from two O(retained) scans to an O(evicted + pins) prefix walk.

**Architecture:** A `VersionPin` is an opaque struct holding a strong `Arc<Snapshot>` — GC already keeps any snapshot with `Arc::strong_count > 1` (the mechanism behind `ReadTx` retention), so pins need zero new bookkeeping and `Drop` is a plain Arc drop. The GC rewrite exploits that the evictable set is exactly the oldest `len − retain_count` keys of the sorted `BTreeMap`, so it visits only that prefix instead of scanning every entry. Spec: `docs/superpowers/specs/2026-07-27-version-pin-oevicted-gc-design.md`.

**Tech Stack:** Rust, single crate (`ultima-db`), no new dependencies. Everything lands in `src/store.rs` + one re-export in `src/lib.rs`.

## Global Constraints

- `cargo clippy -- -D warnings` must pass (CI-gated; zero warnings).
- Do NOT run `cargo fmt` — the repo has rustfmt-version drift and no fmt gate; match the surrounding style by hand.
- GC semantics must be byte-for-byte behaviorally identical: keep the newest `retain_count` keys (latest always kept, even at `num_snapshots_retained: 0`) plus any snapshot with `Arc::strong_count > 1`.
- Metrics behavior unchanged: `inc_gc_run()` on every gc run (including the fast path), `inc_snapshots_collected(n)` only when `n > 0`.
- Tests live in the existing `mod tests` inside `src/store.rs` and may use the in-crate `StoreConfig { field, ..StoreConfig::default() }` literal pattern (the `#[non_exhaustive]` attribute only restricts external crates) — this matches the surrounding GC tests at `src/store.rs:4134-4229`.
- Known flake (not ours): `store::tests::mock_wal_incremental_flush` intermittently fails in full runs — re-run it in isolation before treating a failure as a regression.
- Work on branch `feat/version-pin-oevicted-gc` (already created; spec committed).

---

### Task 1: O(evicted) `gc_inner` rewrite

**Files:**
- Modify: `src/store.rs:1695-1725` (`fn gc_inner`)
- Test: `src/store.rs` `mod tests` (add one test after `gc_retains_snapshots_with_active_readers_beyond_n`, ~line 4210)

**Interfaces:**
- Consumes: `StoreInner { snapshots: BTreeMap<u64, Arc<Snapshot>>, config, metrics }`, test helpers `store.snapshot_count()`, `store.has_snapshot(v)`, `store.gc()`.
- Produces: `gc_inner` with identical externally observable semantics — Task 2's pin tests rely on the `Arc::strong_count > 1` keep-rule surviving this rewrite.

- [ ] **Step 1: Write the characterization test**

This test passes against the OLD implementation too — that is the point. It pins the one geometry the rewrite could get wrong (a referenced snapshot in the *middle* of the evictable prefix, `retain_count > 1`) so the refactor is guarded from both sides.

Add to `mod tests` in `src/store.rs`, directly after `gc_retains_snapshots_with_active_readers_beyond_n` (~line 4210):

```rust
#[test]
fn gc_prefix_skips_referenced_snapshot_mid_window() {
    let store = Store::new(StoreConfig {
        num_snapshots_retained: 2,
        auto_snapshot_gc: false,
        ..StoreConfig::default()
    })
    .unwrap();
    // Commit v1..v5. Snapshots: 0,1,2,3,4,5. Latest is 5.
    for _ in 0..5 {
        store.begin_write(None).unwrap().commit().unwrap();
    }
    assert_eq!(store.snapshot_count(), 6);

    // Hold a reader on v2 — inside the evictable prefix {0,1,2,3}, not at
    // either end of it.
    let rtx2 = store.begin_read(Some(2)).unwrap();

    store.gc();
    // Kept: 4,5 (newest N=2) and 2 (referenced). Dropped: 0,1,3.
    assert_eq!(store.snapshot_count(), 3);
    assert!(store.has_snapshot(2));
    assert!(store.has_snapshot(4));
    assert!(store.has_snapshot(5));
    assert!(!store.has_snapshot(0));
    assert!(!store.has_snapshot(1));
    assert!(!store.has_snapshot(3));

    drop(rtx2);
    store.gc();
    // v2's reference is gone; only the window {4,5} remains.
    assert_eq!(store.snapshot_count(), 2);
    assert!(store.has_snapshot(4));
    assert!(store.has_snapshot(5));
}
```

- [ ] **Step 2: Run the new test plus the existing GC tests — all must PASS against the old code**

Run: `cargo test store::tests::gc_ -- --nocapture`
Expected: PASS (5 tests: `gc_removes_old_snapshots_except_latest_and_active_rtx`, `gc_retains_n_most_recent_snapshots`, `gc_retains_snapshots_with_active_readers_beyond_n`, `gc_zero_retained_keeps_only_latest_and_active`, `gc_prefix_skips_referenced_snapshot_mid_window`). If the new test fails here, the test is wrong — fix it before touching `gc_inner`.

- [ ] **Step 3: Rewrite `gc_inner`**

Replace the body of `fn gc_inner` at `src/store.rs:1695-1725` with:

```rust
fn gc_inner(inner: &mut StoreInner) {
    // The N most recent versions to retain unconditionally.
    // latest_version is always kept (even if num_snapshots_retained is 0).
    let retain_count = inner.config.num_snapshots_retained.max(1);

    // Fast path: nothing to collect.
    let len = inner.snapshots.len();
    if len <= retain_count {
        inner.metrics.inc_gc_run();
        return;
    }

    // Only the oldest `len - retain_count` entries lie outside the
    // newest-retain_count window (BTreeMap iterates in ascending key order),
    // so visit exactly those instead of scanning the whole map — O(evictable)
    // per run, not O(retained). Snapshots with outstanding references
    // (a ReadTx or VersionPin holds the Arc) are kept regardless of age.
    let evictable = len - retain_count;
    let doomed: Vec<u64> = inner
        .snapshots
        .iter()
        .take(evictable)
        .filter(|(_, snapshot)| Arc::strong_count(snapshot) == 1)
        .map(|(&v, _)| v)
        .collect();
    for v in &doomed {
        inner.snapshots.remove(v);
    }
    inner.metrics.inc_gc_run();
    if !doomed.is_empty() {
        inner.metrics.inc_snapshots_collected(doomed.len() as u64);
    }
}
```

Note: the comment mentions `VersionPin` which Task 2 introduces; that is fine — it is a comment, and the tasks land on the same branch.

- [ ] **Step 4: Run the GC tests again — all must still PASS**

Run: `cargo test store::tests::gc_ -- --nocapture`
Expected: PASS (same 5 tests). Also run the auto-GC path: `cargo test store::tests::auto_gc_on_commit` — expected PASS.

- [ ] **Step 5: Run the full store test module and clippy**

Run: `cargo test store::tests` then `cargo clippy -- -D warnings`
Expected: all PASS, zero clippy warnings. (If `mock_wal_incremental_flush` fails, re-run it alone before concluding anything.)

- [ ] **Step 6: Commit**

```bash
git add src/store.rs
git commit -m "perf(store): make gc_inner O(evicted) instead of O(retained)

gc_inner computed the retention cutoff with keys().nth_back() and then
did a full BTreeMap::retain — two O(len) scans on every commit under
auto-GC. The evictable set is exactly the oldest len - retain_count
keys, so walk only that prefix. Same kept-set semantics (newest N keys
plus any snapshot with outstanding references); steady-state auto-GC now
visits ~1 entry per commit regardless of num_snapshots_retained."
```

---

### Task 2: `VersionPin` + `Store::pin_version`

**Files:**
- Modify: `src/store.rs` — new `VersionPin` type (place it directly above the `ReadTx` section marker at ~line 1733, after the `gc_inner` function) and a new `pin_version` method on `impl Store` (directly after `begin_read`, ~line 571)
- Modify: `src/lib.rs:87` — add `VersionPin` to the `pub use store::{...}` list
- Test: `src/store.rs` `mod tests` (add after the GC tests, ~line 4229)

**Interfaces:**
- Consumes: `Snapshot` (`pub(crate)`, has `version: u64` field), `StoreInner.snapshots: BTreeMap<u64, Arc<Snapshot>>`, `Error::VersionNotFound(u64)`, `Result<T>` alias from `crate::error`. Relies on Task 1's keep-rule: `gc_inner` retains any snapshot with `Arc::strong_count > 1`.
- Produces: `pub struct VersionPin` (`Clone + Debug + Send + Sync`) with `pub fn version(&self) -> u64`; `pub fn pin_version(&self, version: Option<u64>) -> Result<VersionPin>` on `Store`; re-exported as `ultima_db::VersionPin`. Task 3 documents these.

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `src/store.rs`, after `gc_zero_retained_keeps_only_latest_and_active` (~line 4229):

```rust
#[test]
fn pinned_version_survives_gc_past_retention_window() {
    let store = Store::new(StoreConfig {
        num_snapshots_retained: 1,
        auto_snapshot_gc: false,
        ..StoreConfig::default()
    })
    .unwrap();
    store.begin_write(None).unwrap().commit().unwrap(); // v1
    let pin = store.pin_version(Some(1)).unwrap();
    store.begin_write(None).unwrap().commit().unwrap(); // v2
    store.begin_write(None).unwrap().commit().unwrap(); // v3

    store.gc();
    // Kept: 3 (latest, window N=1) and 1 (pinned). Dropped: 0, 2.
    assert_eq!(pin.version(), 1);
    assert!(store.has_snapshot(1));
    assert!(store.has_snapshot(3));
    assert!(!store.has_snapshot(0));
    assert!(!store.has_snapshot(2));
    // The pinned version is still readable.
    assert!(store.begin_read(Some(1)).is_ok());
}

#[test]
fn dropping_last_pin_makes_version_collectable() {
    let store = Store::new(StoreConfig {
        num_snapshots_retained: 1,
        auto_snapshot_gc: false,
        ..StoreConfig::default()
    })
    .unwrap();
    store.begin_write(None).unwrap().commit().unwrap(); // v1
    let pin = store.pin_version(Some(1)).unwrap();
    store.begin_write(None).unwrap().commit().unwrap(); // v2

    store.gc();
    assert!(store.has_snapshot(1));

    drop(pin);
    store.gc();
    assert!(!store.has_snapshot(1));
}

#[test]
fn cloned_pin_keeps_version_alive() {
    let store = Store::new(StoreConfig {
        num_snapshots_retained: 1,
        auto_snapshot_gc: false,
        ..StoreConfig::default()
    })
    .unwrap();
    store.begin_write(None).unwrap().commit().unwrap(); // v1
    let pin = store.pin_version(Some(1)).unwrap();
    let pin2 = pin.clone();
    store.begin_write(None).unwrap().commit().unwrap(); // v2

    drop(pin);
    store.gc();
    assert!(store.has_snapshot(1), "clone still holds the pin");
    assert_eq!(pin2.version(), 1);

    drop(pin2);
    store.gc();
    assert!(!store.has_snapshot(1));
}

#[test]
fn pin_version_missing_errors() {
    let store = Store::default();
    assert!(matches!(
        store.pin_version(Some(99)),
        Err(Error::VersionNotFound(99))
    ));
}

#[test]
fn pin_version_none_pins_latest() {
    let store = Store::default();
    store.begin_write(None).unwrap().commit().unwrap(); // v1
    let pin = store.pin_version(None).unwrap();
    assert_eq!(pin.version(), 1);
}

#[test]
fn pin_crosses_threads_smr_handoff() {
    // The motivating pattern: writer pins a capture version, hands the pin
    // to a serializer thread, and keeps committing with auto-GC on and a
    // minimal retention window. The serializer's begin_read must succeed.
    let store = Store::new(StoreConfig {
        num_snapshots_retained: 1,
        ..StoreConfig::default() // auto_snapshot_gc: true
    })
    .unwrap();
    store.begin_write(None).unwrap().commit().unwrap(); // v1
    let pin = store.pin_version(Some(1)).unwrap();

    // Commit far past the retention window; auto-GC runs on every commit.
    for _ in 0..64 {
        store.begin_write(None).unwrap().commit().unwrap();
    }

    let store2 = store.clone();
    let seen = std::thread::spawn(move || {
        // `pin` moved into this thread — compile-time proof VersionPin: Send.
        let rtx = store2.begin_read(Some(pin.version())).unwrap();
        drop(rtx);
        pin.version()
    })
    .join()
    .unwrap();
    assert_eq!(seen, 1);

    // Pin dropped with the thread; the version is collectable now.
    store.gc();
    assert!(!store.has_snapshot(1));
}
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `cargo test store::tests::pin -- --nocapture`
Expected: COMPILE ERROR — `no method named pin_version found for struct Store`.

- [ ] **Step 3: Implement `VersionPin` and `Store::pin_version`**

(a) Add the method to `impl Store` in `src/store.rs`, directly after `begin_read` (after ~line 571):

```rust
    /// Pin `version` (latest if `None`) so [`Store::gc`] — including
    /// per-commit auto-GC — cannot collect it while the returned
    /// [`VersionPin`] (or any clone of it) is alive.
    ///
    /// Returns [`Error::VersionNotFound`] if the requested version does not
    /// exist.
    ///
    /// Unlike [`ReadTx`], a [`VersionPin`] is `Send`, so it can travel to
    /// another thread. This closes the SMR snapshot-handoff race without
    /// inflating [`StoreConfig::num_snapshots_retained`]: the writer pins the
    /// capture version *before* publishing its number, and the serializer
    /// thread opens its own read transaction on arrival.
    ///
    /// # Examples
    ///
    /// ```
    /// use ultima_db::Store;
    ///
    /// let store = Store::default();
    /// store.begin_write(None).unwrap().commit().unwrap();
    ///
    /// // Writer side: pin the capture version, then hand it off.
    /// let pin = store.pin_version(Some(store.latest_version())).unwrap();
    ///
    /// let serializer = std::thread::spawn({
    ///     let store = store.clone();
    ///     move || {
    ///         // While `pin` is alive this cannot fail with VersionNotFound,
    ///         // no matter how far the writer has committed past it.
    ///         let rtx = store.begin_read(Some(pin.version())).unwrap();
    ///         // ... stream the snapshot from `rtx`, then drop both ...
    ///         drop(rtx);
    ///     }
    /// });
    /// serializer.join().unwrap();
    /// ```
    pub fn pin_version(&self, version: Option<u64>) -> Result<VersionPin> {
        let inner = self.inner.read();
        let v = version.unwrap_or(inner.latest_version);
        let snapshot = inner
            .snapshots
            .get(&v)
            .ok_or(Error::VersionNotFound(v))?
            .clone();
        Ok(VersionPin { snapshot })
    }
```

(b) Add the type in `src/store.rs`, directly above the `ReadTx` section-divider comment (`// ReadTx — snapshot-isolated read transaction`, ~line 1733 after Task 1):

```rust
// ---------------------------------------------------------------------------
// VersionPin — a Send-able handle that keeps one version alive across GC
// ---------------------------------------------------------------------------

/// Keeps one snapshot version alive across [`Store::gc`] runs.
///
/// Created by [`Store::pin_version`]. Holds a strong reference to the
/// snapshot, which is the same mechanism GC uses to protect versions held by
/// an active [`ReadTx`] — a pinned version is never collected, regardless of
/// [`StoreConfig::num_snapshots_retained`]. Dropping the last pin (and any
/// clones) makes the version collectable again.
///
/// `VersionPin` is `Send + Sync + Clone`, unlike [`ReadTx`]: use it to hand a
/// version across threads, then open a [`ReadTx`] on the receiving thread via
/// [`Store::begin_read`]`(Some(pin.version()))`.
#[derive(Clone)]
pub struct VersionPin {
    snapshot: Arc<Snapshot>,
}

impl VersionPin {
    /// The pinned version number.
    pub fn version(&self) -> u64 {
        self.snapshot.version
    }
}

impl std::fmt::Debug for VersionPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionPin")
            .field("version", &self.snapshot.version)
            .finish()
    }
}
```

(c) In `src/lib.rs:87`, extend the store re-export:

```rust
pub use store::{IsolationLevel, Readable, Store, StoreConfig, VersionPin, WriterMode};
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `cargo test store::tests::pin -- --nocapture` and `cargo test store::tests::gc_`
Expected: all PASS (6 new pin tests + 5 gc tests).

- [ ] **Step 5: Run doctests, full tests, clippy**

Run: `cargo test --doc store` (the `pin_version` example is a doctest), then `cargo test`, then `cargo clippy -- -D warnings`
Expected: all PASS, zero warnings. (Known flake: `mock_wal_incremental_flush` — re-run alone if it fails.)

- [ ] **Step 6: Commit**

```bash
git add src/store.rs src/lib.rs
git commit -m "feat(store): Send-able VersionPin via Store::pin_version

ReadTx is !Send, so the SMR snapshot handoff (writer publishes a version
number, serializer thread begin_reads it) had no way to pin the version
across the thread boundary — external users compensated with huge
num_snapshots_retained values, paying gc_inner's per-commit scan.
VersionPin holds a strong Arc<Snapshot>, the same reference-count keep
rule gc already honors: zero new GC bookkeeping, Drop is an Arc drop."
```

---

### Task 3: Documentation — task doc, GC doc comments, CLAUDE.md touch-up

**Files:**
- Create: `docs/tasks/task53_version_pin_gc.md`
- Modify: `src/store.rs:126-128` (`num_snapshots_retained` field doc), `src/store.rs:672-675` (`Store::gc` doc)
- Modify: `CLAUDE.md` (the `Store` bullet in the Architecture section — one sentence)

**Interfaces:**
- Consumes: `VersionPin` / `pin_version` from Task 2, `gc_inner` behavior from Task 1.
- Produces: canonical per-feature doc; no code changes.

- [ ] **Step 1: Update the two doc comments in `src/store.rs`**

At `src/store.rs:126-128`, extend the `num_snapshots_retained` field doc (keep the existing two lines, add the third):

```rust
    /// How many most-recent snapshots to retain during [`Store::gc()`]. Default: 10.
    /// The latest snapshot is always retained regardless of this value.
    /// To keep a specific older version alive, prefer [`Store::pin_version`]
    /// over a large retention window — pins are O(1); retention is a window.
    pub num_snapshots_retained: usize,
```

At `src/store.rs:672-675`, extend the `Store::gc` doc — replace:

```rust
    /// Garbage collect old snapshots that are no longer referenced by any [`ReadTx`].
    /// Always keeps the `num_snapshots_retained` most recent snapshots, plus any
    /// snapshot held by an active [`ReadTx`]. The latest snapshot is always kept
    /// even if `num_snapshots_retained` is 0.
```

with:

```rust
    /// Garbage collect old snapshots that are no longer referenced.
    /// Always keeps the `num_snapshots_retained` most recent snapshots, plus any
    /// snapshot held by an active [`ReadTx`] or [`VersionPin`]. The latest
    /// snapshot is always kept even if `num_snapshots_retained` is 0.
    /// Cost is O(evictable + pinned), not O(retained): only versions older than
    /// the retention window are visited.
```

- [ ] **Step 2: Write `docs/tasks/task53_version_pin_gc.md`**

```markdown
# Task 53: VersionPin API + O(evicted) snapshot GC

## Motivation

An external benchmark (hi-perf-cmp smr-collections, 2026-07-27, rev b48295e)
drove the store in its SMR pattern and hit ~104–113 µs per applied command —
~90% of which was a workaround interacting with a GC cost model:

- `ReadTx` is `!Send`, so the SMR snapshot handoff (writer publishes version
  `v`; a serializer thread `begin_read`s it) had a race window in which
  auto-GC could collect `v`. The harness compensated with
  `num_snapshots_retained(16384)`.
- `gc_inner` ran two O(retained) scans (`keys().nth_back()` for the cutoff +
  full `BTreeMap::retain`) on every commit. At 16k retained snapshots that
  dominated the commit path: a same-shape A/B measured 5.9 µs/op at
  `retained=10` vs 71.8 µs/op at `retained=16384` (sandbox, direction-only).

SMR needs O(1) live versions (latest + the capture version being serialized),
expressed as pins — not a deep retention window.

## Design

**`Store::pin_version(Option<u64>) -> Result<VersionPin>`** (latest if
`None`; `Error::VersionNotFound` otherwise, mirroring `begin_read`).
`VersionPin` is `Clone + Debug + Send + Sync`, exposes `version() -> u64`,
and holds a strong `Arc<Snapshot>` — the same reference-count rule
(`Arc::strong_count > 1`) `gc_inner` has always used to protect versions held
by an active `ReadTx`. Consequences:

- Zero new GC bookkeeping; `Drop` is a plain Arc drop (no store lock,
  panic-safe).
- While a pin lives, its version is guaranteed present in the snapshot map,
  so `begin_read(Some(pin.version()))` on any thread succeeds. There is
  deliberately no `VersionPin::begin_read` — one `ReadTx` construction path.

SMR handoff pattern:

    writer:      commit(v) → pin_version(Some(v)) → send (v, pin)
    serializer:  recv → begin_read(Some(v)) → drop pin → stream

**`gc_inner` is now O(evicted + pins) per run.** The kept set is unchanged
(newest `retain_count` keys, latest always kept, plus referenced snapshots),
but instead of computing a cutoff via `keys().nth_back(retain_count - 1)` and
running a full `retain`, it walks only the oldest `len - retain_count`
entries (the `BTreeMap` prefix below the old cutoff) and removes the
unreferenced ones. Steady-state auto-GC visits ~1 entry per commit regardless
of `num_snapshots_retained`. Long-lived pins older than the window stay in
the prefix and are re-visited each run — O(#pins).

## Testing

- Characterization test `gc_prefix_skips_referenced_snapshot_mid_window`
  (passes against both old and new `gc_inner`) guards the rewrite; the four
  pre-existing gc tests pin the retention/latest/reference semantics.
- Six pin tests cover: survival past the retention window, collectability
  after the last drop, clone semantics, `VersionNotFound`, `None` = latest,
  and the cross-thread SMR handoff under per-commit auto-GC (also the
  compile-time `Send` proof).

## Design history

`docs/superpowers/specs/2026-07-27-version-pin-oevicted-gc-design.md`
```

- [ ] **Step 3: Update `CLAUDE.md`**

In the Architecture section's `Store` bullet (the one describing `begin_read`/`begin_write`/`gc()`), extend the sentence listing what `Store` provides — change:

```
Provides `begin_read(Option<u64>)`, `begin_write(Option<u64>)`, and `gc()`.
```

to:

```
Provides `begin_read(Option<u64>)`, `begin_write(Option<u64>)`, `pin_version(Option<u64>)` (a `Send`-able `VersionPin` that keeps one version alive across `gc()` — the SMR snapshot-handoff primitive, task53), and `gc()` (O(evicted + pins) per run, not O(retained)).
```

- [ ] **Step 4: Verify docs build and full gates**

Run: `cargo doc --no-deps 2>&1 | grep -i warning` (expect no output), then `cargo test`, `cargo test -p ultima-vector`, `cargo clippy -- -D warnings`
Expected: all PASS, zero warnings. (`ultima-vector` doesn't touch the store, but the workspace-verification checklist says root `cargo test` misses member crates — run it to be safe.)

- [ ] **Step 5: Commit**

```bash
git add docs/tasks/task53_version_pin_gc.md src/store.rs CLAUDE.md
git commit -m "docs: task53 — VersionPin + O(evicted) gc"
```
