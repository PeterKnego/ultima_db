# VersionPin API + O(evicted) snapshot GC — design

Date: 2026-07-27
Status: approved (brainstormed in-session; see motivation below)

## Motivation

The hi-perf-cmp `smr-collections` benchmark (run 20260727, ultima_db pinned at
b48295e) drove ultima_db in its SMR pattern and reported ~104–113 µs per
applied command. Investigation showed ~90% of that is a harness workaround
interacting with a Store cost model, not intrinsic engine cost:

1. **No `Send`-able way to pin a version.** The SMR snapshot handoff (writer
   publishes version `v` to a serializer thread, which `begin_read`s it) has a
   race: between the writer handing off the number `v` and the serializer's
   `begin_read(Some(v))`, auto-GC may collect `v`. `ReadTx` is `!Send`, so the
   pin cannot travel with the message. The harness compensated with
   `num_snapshots_retained(16384)`.
2. **`gc_inner` is O(retained) per commit.** It computes the retention cutoff
   with `keys().nth_back(retain_count - 1)` (walks ~all keys) and then does a
   full `BTreeMap::retain` (walks all entries). With 16,384 retained snapshots
   and auto-GC on every commit, that is two ~16k-entry scans per commit.
   A sandbox A/B (direction-only) of the same 3-table op shape measured
   5.9 µs/op at `retained=10` vs 71.8 µs/op at `retained=16384` — a 12×
   spread far above the sandbox noise floor.

SMR needs O(1) live versions (latest + any capture version being serialized),
not a deep retention window. This feature makes the correct pattern
expressible (`VersionPin`) and makes large retention configs stop paying a
per-commit scan tax (O(evicted) GC).

## Design

### 1. `VersionPin` + `Store::pin_version`

A pin is an opaque handle holding a strong `Arc<Snapshot>` reference:

```rust
pub struct VersionPin {
    snapshot: Arc<Snapshot>,   // Snapshot stays pub(crate); handle is opaque
}

impl Store {
    /// Pin `version` (latest if `None`) so [`Store::gc`] cannot collect it
    /// while the pin (or any clone of it) is alive.
    /// Returns [`Error::VersionNotFound`] if the version does not exist.
    pub fn pin_version(&self, version: Option<u64>) -> Result<VersionPin>;
}

impl VersionPin {
    /// The pinned version number.
    pub fn version(&self) -> u64;
}
```

- `Clone + Debug + Send + Sync`. `Send`/`Sync` are automatic: `Snapshot` is
  `{ u64, BTreeMap<String, Arc<dyn MergeableTable>> }` and
  `MergeableTable: Send + Sync`.
- **No new GC bookkeeping.** `gc_inner` already keeps any snapshot whose
  `Arc::strong_count > 1` (the mechanism behind `ReadTx` retention); a pin is
  simply another strong reference. `Drop` is a plain `Arc` drop — no store
  lock, panic-safe.
- **No `VersionPin::begin_read`.** While a pin lives, its version is
  guaranteed present in `StoreInner::snapshots`, so
  `store.begin_read(Some(pin.version()))` on the receiving thread always
  succeeds. One `ReadTx` construction path is enough.
- Rejected alternative: a pin-count side table in `StoreInner`. More
  machinery, and `Drop` would need the store lock.

Intended SMR handoff pattern (goes in the doc example):

```text
writer:      commit(v)  →  pin = store.pin_version(Some(v))  →  send (v, pin)
serializer:  recv       →  rtx = store.begin_read(Some(v))   →  drop pin, stream rtx
```

With this, `num_snapshots_retained` can stay at its default.

### 2. `gc_inner`: O(evicted + pins) per run

The kept set is unchanged: the newest `retain_count` keys (latest always kept
via the existing `max(1)`) union any snapshot with outstanding references.
The implementation changes from cutoff-computation + full `retain` to visiting
only the evictable prefix:

```rust
// after the existing `len <= retain_count` fast path:
let evictable = len - retain_count;
let doomed: Vec<u64> = inner
    .snapshots
    .iter()
    .take(evictable)                                   // oldest keys, ascending
    .filter(|(_, snap)| Arc::strong_count(snap) == 1)  // unreferenced only
    .map(|(&v, _)| v)
    .collect();
for v in &doomed {
    inner.snapshots.remove(v);
}
```

- The oldest `len − retain_count` keys are exactly the keys below the old
  cutoff (`BTreeMap` iterates in ascending key order), so the semantics are
  identical to the current `retain(|v, s| v >= cutoff || strong_count > 1)`.
- Steady-state auto-GC visits ~1 entry per commit regardless of
  `num_snapshots_retained`. Long-lived pins/`ReadTx`s older than the window
  remain in the prefix and are re-visited each run — O(#pins), acceptable.
- Metrics behavior unchanged: `inc_gc_run` every run,
  `inc_snapshots_collected(doomed.len())` when non-empty.
- All four `gc_inner` call sites (`Store::gc`, post-commit auto-GC, and the
  two internal sites) benefit unchanged.

### Doc updates

- `Store::gc` / `StoreConfig::num_snapshots_retained` doc comments: extend the
  "held by an active ReadTx" sentence with "or `VersionPin`".
- `docs/tasks/task53_version_pin_gc.md` consolidates the feature per the
  repo's feature workflow; this spec is committed as design history.

## Error handling

- `pin_version(Some(missing))` → `Error::VersionNotFound(v)` (same as
  `begin_read`).
- `pin_version(None)` on a fresh store pins version 0 (the empty root
  snapshot), mirroring `begin_read(None)`.

## Testing

Existing GC semantics tests must pass untouched (they pin current behavior):
`gc_removes_old_snapshots_except_latest_and_active_rtx`,
`gc_zero_retained_keeps_only_latest_and_active`.

New tests (written first, TDD):

1. Pinned version survives `gc()` after committing past the retention window,
   and is still readable via `begin_read(Some(v))`.
2. Dropping the last pin makes the version collectable by the next `gc()`.
3. A clone of a pin keeps the version alive after the original is dropped.
4. `pin_version(Some(missing))` returns `VersionNotFound`.
5. `pin_version(None)` pins the latest version.
6. SMR handoff: pin moved into another thread (compile-time `Send` proof)
   while the main thread commits past the retention window; receiving thread
   `begin_read`s the pinned version successfully.
7. GC still evicts correctly with `num_snapshots_retained` larger than 1
   (exercises the `take(evictable)` prefix logic with a mix of pinned and
   unpinned old versions).

Gates: `cargo test`, `cargo clippy -- -D warnings`, member-crate tests per the
workspace verification checklist.

## Out of scope

- `VersionPin::begin_read` (YAGNI; `store.begin_read(Some(pin.version()))`).
- Any change to retention defaults or auto-GC policy.
- The hi-perf-cmp harness patch (separate repo; will be suggested upstream).
