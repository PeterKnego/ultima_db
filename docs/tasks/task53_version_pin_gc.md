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
  (passes against both old and new `gc_inner`) guards the rewrite; the five
  pre-existing gc tests pin the retention/latest/reference semantics.
- Seven pin tests cover: survival past the retention window, collectability
  after the last drop, clone semantics, `VersionNotFound`, `None` = latest,
  `None` on a fresh store pinning version 0, and the cross-thread SMR
  handoff under per-commit auto-GC (also the compile-time `Send` proof).

## Design history

`docs/superpowers/specs/2026-07-27-version-pin-oevicted-gc-design.md`
