# Task 58: Write overlay — bounded mini-memtable for SingleWriter commits

**Status:** implemented (Tasks 1–5), perf-cell + docs (this task), fleet A/B ship gate outstanding (manual, out of scope for this doc).

## Problem

`docs/benchmarks/ycsb-eventual-write-decomposition-2026-08-02.md` decomposed
the eventual-tier YCSB update op on an NVMe host: total 7528 ns/op, of which
the **MVCC path-clone tax is 1975 ns (26%)** — every commit pays an
O(height×T) copy-on-write clone chain on the B-tree because the writer's
tree shares nodes with the latest snapshot. That doc's own follow-up list
names the write overlay ("mini-memtable") as "the structural attack on the
remaining MVCC tax" once the WAL-side allocator fix (task57-era) landed.

The design doc (`docs/superpowers/specs/2026-08-03-write-overlay-design.md`)
frames the ship goal precisely: flip the eventual-tier YCSB A cell so
**UltimaDB beats Fjall on A, same host, glibc, shipped config**, with
quiet-store reads (C, E) unchanged and reads-under-writes within ≤5%.

## Design summary

The overlay converts the per-transaction node-clone tax into a bounded
small memcpy per transaction plus one *batched* tree pass every
`OVERLAY_CAP` writes (128 by default; batching was measured at ~⅓ the
per-op tree cost in the task52-era data).

**Data structure** (`src/overlay.rs`):

```rust
struct Overlay<R, K> {
    entries: Arc<Vec<(K, OverlayOp<R>)>>, // sorted by K, len <= OVERLAY_CAP
    len_delta: i64,                        // merged len = data.len() + len_delta
}
enum OverlayOp<R> {
    Put { rec: Arc<R>, tree_resident: bool },
    Tombstone,
}
```

`Table<R, K>` carries an `Overlay` plus an `overlay_disabled` flag;
`TableSnapshot` (batch-rollback capture) mirrors both fields and restores
them wholesale on rollback.

Cost model: after a commit shares `entries` with the latest snapshot, the
next write's `Arc::make_mut` copies ≤ `OVERLAY_CAP` entries (≈2 KB at cap
128, ~100 ns) instead of cloning tree nodes. This bounded copy replacing
the unbounded-fanout node-clone chain is the entire trick.

**Write path** — `insert`/`put`/`update`/`delete` on an enabled, non-full
table: a merged `get` (overlay-then-tree) decides existence and the exact
`len_delta` adjustment, then the row lands as `Put`/`Tombstone` in
`entries` via `make_mut` + binary-search insert. WAL emission in
`TableWriter` is unchanged — the logical `WalOp` is produced exactly as
today, regardless of where the row lands.

**The delete asymmetry** (the one non-obvious invariant): deleting a row
that exists **only in the overlay** — i.e. inserted since the last flush,
so the tree has no key to shadow — removes the overlay entry outright
(`Overlay::remove_entry`) instead of writing a tombstone. Tombstones
therefore always shadow a real tree row, which is what makes flush's
`remove_mut` infallible: every `Tombstone` in `entries` at flush time is
guaranteed to hit an existing tree key. `Overlay::set_tombstone` carries a
`debug_assert!` on exactly this precondition, and `set_put`/`set_tombstone`/
`remove_entry` implement the full `len_delta` transition table (tested
directly in `src/overlay.rs`'s `len_delta_follows_the_transition_table`).

**Flush** — triggered by a full overlay or by `define_index`/
`define_custom_index`. The writer already owns its table clone, so flush
replays the ≤ `OVERLAY_CAP` sorted entries into `data` in one batched pass
(`Put` → `insert_mut`, `Tombstone` → `remove_mut`), then resets `entries`
to a fresh empty `Vec` and rebases `len_delta` to 0. Sorted order gives the
tree its best-case locality; flush on owned data is infallible by
construction, so there is no partial-flush error path.

**Read paths** — one merged choke point. `get` does a binary-search probe
of the overlay first (`Put` returns the record, `Tombstone` returns
`None`), falling through to the tree only on a miss. All multi-row paths —
`iter`, `range`, scans, checkpoint serialization, snapshot streaming — flow
through `overlay::MergedIter`, a two-pointer merge of the sorted overlay
slice and the tree iterator; overlay wins ties, tombstones swallow the
tree's entry. `len()` = `data.len() + len_delta`. The design doc calls out
the correctness linchpin explicitly: **no direct `self.data.iter()`/range
may exist outside this choke point** — the property test below is what
guards it.

## V1 scope and auto-disable rules

- **SingleWriter stores only.** MultiWriter stores never enable overlays;
  the OCC merge path (`merge_keys_from`/`upsert_arc`) is untouched and
  carries a `debug_assert!(overlay.is_empty())` tripwire. The per-store cap
  is computed once at `Store::new`: `OVERLAY_CAP` (env-overridable via
  `ULTIMA_OVERLAY_CAP`, read once) for `WriterMode::SingleWriter`, `0` for
  `MultiWriter`.
- **Non-indexed tables only.** `define_index`/`define_custom_index` flush
  the overlay and permanently set `overlay_disabled` before touching
  `self.data` — closing a Task-3 review finding that `get`/
  `merged_get_arc` honor a nonempty overlay regardless of index state, so
  buffered rows must be flushed before an indexed table's DDL and its
  update/delete else-branches start bypassing the overlay.
- No public API, no config surface. `OVERLAY_CAP` is an internal
  `pub(crate)` const (128), env-overridable only for bench tuning
  (`ULTIMA_OVERLAY_CAP`, task57 precedent — read once at `Store::new`, `0`
  disables).
- **Durability/recovery/composition:** the overlay is volatile
  acceleration only. WAL ops are logical and unchanged; recovery replays
  through the normal table API and simply rebuilds an overlay from
  scratch. Checkpoints and snapshot streaming serialize the merged view —
  no on-disk format changes anywhere. `bulk_load` installs fresh tables
  (empty overlay). Composes with SMR mode, every `Durability`/`WalWrite`
  combo, and `fanout-t8` without special cases.
- Rejected alternatives (recorded in the design doc): per-commit frozen
  delta *chains* (read-probe depth grows between flushes, violating the
  read budget); in-place commit with snapshot-on-demand (changes MVCC
  retention semantics — `begin_read(Some(v))` and `num_snapshots_retained`
  promise materialized versions).

## Implementation notes (review-driven fixes during Tasks 3–5)

- **`put`/`upsert_arc` decoupling (T3 fix, commit `8194404`).** The
  original plan routed `put` through `upsert_arc`, which is also the
  MultiWriter commit-merge helper (`merge_keys_from`) and therefore must
  stay a direct, flush-first tree write. That would have made every `put`
  bypass the overlay. Coordinator-confirmed resolution: `insert`/`put`/
  `update`/`delete` all buffer into the overlay directly when
  `overlay_write_ready()`; `put` advances the `AutoKey` counter
  (`key.advance_auto_counter`) before landing the row in the overlay, so
  an explicitly-keyed `put` still protects a later auto-`insert` from
  reissuing the id even though the row hasn't touched the tree yet.
  `upsert_arc` remains the MultiWriter merge/replay helper's flush-first,
  direct-tree path — unchanged in shape, now doubling as `put`'s fallback
  for overlay-disabled tables (disabled cap, or an indexed table).
- **`bulk_load` `data_ref()` fix (T5, commit `f922c11`).** `Store::bulk_load`'s
  Delta path read a live snapshot table's `data_ref()` directly to
  materialize the delta against the base; `data_ref()` `debug_assert!`s the
  overlay is empty, which the T5 wiring broke — an ordinary SingleWriter
  commit can now leave rows buffered in the latest committed snapshot.
  Fix: clone the base table (O(1), BTree root + overlay `Arc` bumps) and
  `flush_overlay()` that clone before materializing the delta against it,
  leaving the installed snapshot itself untouched.
- **`set_overlay_cap` flush-on-any-change (T5 review fix, commit
  `e9f21d1`).** The initial guard only flushed when the new cap couldn't
  hold what was already buffered (`cap < entries.len()`), so a cap change
  to a value the entries still fit under (e.g. 128 → 64 with 10 buffered)
  silently dropped them along with their `len_delta` instead of flushing.
  Unreachable today (cap is a per-store constant, so it changes at most
  once per `Table` lifetime, on the `open_table` right after `Store::new`)
  but violated the function's "never lose entries" contract. Fixed to
  flush whenever the overlay is nonempty *and* the cap is actually
  changing; a same-cap re-open (the common `open_table` path across
  transactions) still preserves buffered entries as a no-op.
- **`last()` no longer drains the whole tree with a nonempty overlay** (T2
  review fix, commit `27de890`) — folded into the merged-iterator choke
  point described above rather than a special-cased tree-only path.

## Test inventory

- **Property test (centerpiece):** `overlay_table_is_observationally_identical_to_plain_table`
  (`src/table.rs`) — an overlay table and a plain table driven by identical
  random op sequences (insert/put/update/delete/get/range/iter/len) across
  8 seeds × caps `{1, 2, 3, 8}` (forcing flushes at every boundary
  alignment) must be observationally identical at every step. This is the
  single test guarding the merge/tombstone/`len_delta` surface.
- `src/overlay.rs` unit tests: sorted-insert/replace-in-place, the full
  `len_delta` transition table, CoW-clone-doesn't-leak, cap-0-disabled,
  `take_entries` reset.
- `src/table.rs`: `put_lands_in_overlay_and_advances_the_counter`,
  `set_overlay_cap_change_flushes_instead_of_dropping`, plus the existing
  batch-rollback and DDL suites exercising the overlay through
  `TableSnapshot` capture/restore and `define_index`/`define_custom_index`
  flush-and-disable.
- `src/store.rs`: SingleWriter-enable / MultiWriter-never-enables wiring,
  DDL flush+disable at the store level, MVCC visibility (old `ReadTx` sees
  its frozen overlay; new sees new state), recovery and checkpoint
  round-trips with a nonempty overlay at the cut point, and the
  `bulk_load` Delta-path fix above.

## Local direction numbers (sandbox, direction-only — do not draw a perf conclusion from these)

`cargo run --release --features persistence --example perf_decomp`, cell
(7) added: same `store_eventual_update` loop with `ULTIMA_OVERLAY_CAP=0`
set before the second store is constructed (the cap is read once at
`Store::new`), isolating the overlay's effect as
`store_eventual_no_overlay − store_eventual_update`.

Five back-to-back runs on the loaded local sandbox. `examples/perf_decomp.rs`'s
own header comment states the applicable noise band for this cell type:
*"Direction-only on the sandbox; single-threaded cells resolve to ±3–17%"*
(consistent with `docs/benchmarks/ycsb-eventual-write-decomposition-2026-08-02.md`,
which validated the same sandbox-vs-NVMe relationship for this harness).
The ±35% figure elsewhere in this task's verification (see the
`make perf/check` section below) belongs to a different measurement
domain — autobench SMR-apply gate tolerances — and does not apply to
`perf_decomp` cells; it is not cited here.

| run | store_eventual_update (ov) | store_eventual_no_overlay | overlay effect (no_ov − ov) |
|---|--:|--:|--:|
| 1 | 4767 ns | 4549 ns | −219 ns |
| 2 | 4810 ns | 4382 ns | −427 ns |
| 3 | 4561 ns | 4374 ns | −187 ns |
| 4 | 4545 ns | 4423 ns | −123 ns |
| 5 | 4602 ns | 4487 ns | −115 ns |

The design doc's expected direction was a **positive** gap of "very
roughly 1–2 µs" (overlay-enabled faster). What was actually observed here
is a small, consistently **negative** gap (overlay-enabled ~120–430 ns
*slower*) across all five runs. Two things are true at once:

1. The sign is stable across five repeats — this is not simple noise
   flipping sign run to run.
2. The magnitude (~120–430 ns) is well inside the ±3–17% noise band this
   harness documents for single-threaded cells: applied to the ~4.5–4.8 µs
   cell median, that's roughly **±135–800 ns** — a range that comfortably
   covers the observed −115..−427 ns swing on its own. It is also small
   next to the MVCC-tax term this cell is meant to isolate (1975 ns
   measured on the NVMe host in the prior decomposition doc). A swing this
   size, within the harness's own documented band, is exactly the kind of
   measurement this loaded sandbox is not trusted to resolve directionally
   per `CLAUDE.md`'s benchmarking policy ("never draw a perf conclusion...
   from a local run").
3. Falls out of the earlier "conclusions" section of the decomposition
   doc: this cell only ever isolates the CoW-clone-vs-bounded-copy delta.
   With a single-record-per-txn workload against a 10k-row table, the
   overlay's `Arc::make_mut` copy is itself bounded by how many entries are
   currently buffered (≤128, but typically far fewer between warm-up
   flushes) — at low buffered-entry counts the two paths' costs may simply
   be close enough that sandbox noise dominates. This is exactly the kind
   of gap the fleet A/B (Step 6, out of this task's scope) is designed to
   resolve, not something to chase further on this host.

**Honest wrinkle, left unresolved for the fleet run:** even granting that
the magnitude sits inside the noise band, the *sign* came out the opposite
of the design doc's prediction — small negative (overlay-enabled slightly
slower) instead of the predicted positive 1–2 µs win — and the observed
magnitude is far below that 1–2 µs prediction in either direction. Nothing
here contradicts the design (the band covers zero), but nothing here
confirms it either. This is recorded as unresolved for the fleet A/B to
settle, the same posture taken with the `checkpoint_ms` finding below.

No tuning was attempted per the task brief's direction-only instruction.
This result is recorded as-is for the review gate; it does not by itself
support or refute the ship criterion below — only the fleet run does.

## `make perf/check` findings (sandbox, direction-only, non-gating)

`autobench/CLAUDE.md` documents that this gate's committed baselines
(`autobench/baselines/*.json`) are NVMe-host medians and states plainly:
*"`make perf/check` fails on the noisy virtualized sandbox by design —
different host shape, not a regression."* Per the task brief, these results
are recorded for the review gate rather than tuned against.

`smr-apply-microbench` (SMR mode, explicit-version `SingleWriter` — the
overlay engages on every apply, since the `state` table carries no index):
seven consecutive runs on the loaded local sandbox.

| metric | baseline (NVMe) | observed range (7 runs) | direction |
|---|--:|--:|---|
| `apply_sw_batch_throughput` | 233,340 | 515,707 – 556,680 | **+121% to +139%** (large, stable win) |
| `apply_p99_ns` | 19,099 | 12,428 – 14,370 | **−25% to −35%** (stable win) |
| `checkpoint_ms` | 22.7 | 31.1 – 40.0 | **+37% to +76%** (stable regression) |
| `apply_throughput` (whole-pipeline, includes in-loop checkpoint) | 24,659 | 16,515 – 22,380 | −9% to −33% (noisy, tracks checkpoint_ms) |
| `snapshot_stream_ms` (in-memory, no disk I/O) | 44.5 | 42.1 – 52.5 | mostly flat, one noisy outlier |
| `read_p99_under_load_ns` | 547 | 570 – 851 | noisy (already a 39%-tolerance tail metric) |

Two findings pull in opposite directions, and both are worth recording:

1. **`apply_sw_batch_throughput` — a large, consistent win exactly where
   the design predicted one.** This cell is T=8 sequential auto-versioned
   commits on the main `SingleWriter` store — precisely the regime the
   overlay targets (bounded memcpy replacing a per-commit node-clone
   chain). `apply_p99_ns` (the pinned-apply latency tail) improves in the
   same direction. Both are stable across all 7 runs — this is a real
   signal, not sandbox noise flipping sign.
2. **`checkpoint_ms` — a consistent regression, but not attributable to
   the overlay's design (double-buffering).** Per the brief's instruction
   to check whether "the apply workload's batched cells now double-buffer"
   before flagging: `checkpoint()` does **not** flush the overlay — it
   serializes the merged view (`Table::merged_iter`, the same choke point
   `collect_serialized_rows` uses for `snapshot_stream`) without touching
   `data`. If the overlay's per-row merge branch were the cause,
   `snapshot_stream_ms` — which serializes the *identical* merged view,
   in memory, via the same code path — would show the same consistent
   regression. It does not (44.5 → 42–52, no consistent direction). What
   *does* differ: `checkpoint()` uniquely does real disk I/O on this path
   (`write_checkpoint`'s fsync, a WAL-prune round-trip through the
   background thread, `cleanup_old_checkpoints`'s directory scan+unlink) —
   none of which `snapshot_stream`'s in-memory `Read` impl touches. On a
   loaded/virtualized sandbox disk, that is exactly the kind of cost this
   gate's own baseline doc warns is not sandbox-comparable. `mw-commit-microbench`
   (`MultiWriter`, which never enables the overlay by construction) shows
   two "regressions" — `mw_scaling_8x` and `mw_scaling_efficiency` — that
   are pre-documented false positives (`infer_direction` gates those two
   the wrong way; both values actually *improved*), reinforcing that this
   host's gate output needs the documented caveats applied rather than
   read at face value.

**Flagged for the review gate, not resolved here:** whether `checkpoint_ms`'s
regression reproduces on the NVMe bench host (Step 6, out of this task's
scope) is the deciding question. If it does reproduce there, the most
likely structural cause given the above is disk-I/O-path contention from
the overlay-enabled `SingleWriter` apply loop generating a different write
pattern to the same disk `checkpoint()` fsyncs against (e.g. more, smaller
WAL/tree writes between checkpoints instead of fewer, larger ones) rather
than the checkpoint serialization path itself. No tuning was attempted
per the brief's direction-only instruction for this sandbox.

## Validation

Ship iff eventual A beats Fjall same-host glibc, with C/E unchanged; A/B
recipe: bench-oneshot competitor plus a branch-vs-main A/F run,
OVERLAY_CAP arms 64/128/256 if marginal.

This fleet A/B (one c6id.2xlarge, real AWS spend) is **out of scope for
this task** — it is Step 6 of the task brief and requires explicit user
authorization before any `make up`/`bench-oneshot` per `CLAUDE.md`'s
bench-infra guardrails. Everything above this section is the local,
non-billable artifact set the fleet run consumes.
