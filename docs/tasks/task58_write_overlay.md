# Task 58: Write overlay — bounded mini-memtable for SingleWriter commits

**Status:** implemented (Tasks 1–5), perf-cell + docs, final-review fix wave applied; fleet A/B ship gate outstanding (manual, out of scope for this doc).

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

`Table<R, K>` carries an `Overlay`; `TableSnapshot` (batch-rollback
capture) mirrors it and restores it wholesale on rollback. There is no
separate `overlay_disabled` flag (the design doc predicted one): a cap of
`0` is the disable, and an indexed table is kept overlay-free by
`overlay_write_ready()`'s `!indexes.is_empty()` arm rather than by a flag.

Cost model: after a commit shares `entries` with the latest snapshot, the
next write's `Arc::make_mut` deep-copies the buffered entries instead of
cloning tree nodes. **That copy is not a flat memcpy** — the design doc's
inherited "≈2 KB, ~100 ns" phrasing is wrong and is corrected here.
Cloning `Vec<(K, OverlayOp<R>)>` runs the element clone per entry: one
atomic refcount increment on each entry's `Arc<R>`, plus a `K` clone (a
heap allocation for `String`/`Vec<u8>` keys, a register copy for `u64`).
So the per-write CoW cost is **linear in the number of currently buffered
entries**, dominated by one atomic RMW each, and it *grows as the overlay
fills* — cheapest right after a flush, most expensive at `OVERLAY_CAP − 1`.
The trick still holds (a bounded, cap-sized cost replacing the
unbounded-fanout node-clone chain), but the per-entry constant is an
atomic-refcount constant, not a memcpy constant. This is the direct reason
the cap-tuning arms in Validation below point *downward* rather than
upward.

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
through `Table::merged_iter`, which returns an `overlay::TableIter`: with a
nonempty overlay, `TableIter::Merged` — a two-pointer merge of the sorted
overlay slice and the tree iterator, overlay wins ties, tombstones swallow
the tree's entry; with an **empty** overlay, `TableIter::Plain` — the bare
tree iterator, one `match` per step and no peek/compare at all (the
quiet-store fast path; see the perf sections below for why it exists).
`len()` = `data.len() + len_delta`. The design doc calls out
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
  the overlay (and zero its cap) before touching `self.data` — closing a
  Task-3 review finding that `get`/`merged_get_arc` honor a nonempty
  overlay regardless of index state, so buffered rows must be flushed
  before an indexed table's DDL and its update/delete else-branches start
  bypassing the overlay. What keeps it empty *afterwards* is
  `overlay_write_ready()`'s `!indexes.is_empty()` arm, not the zeroed cap:
  a later `open_table` re-applies the store's cap via `set_overlay_cap`,
  but no write on an indexed table ever buffers again.
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

### Final-review fix wave

- **The store's `put` really buffers now.** `TableWriter::put` →
  `TableWriter::upsert` reached the data through `Table::upsert_arc`, which
  force-flushes and writes the tree directly — so `Table::put`'s buffering
  branch was **dead in production**: every explicitly-keyed store write
  bypassed the overlay and, worse, flushed it. `upsert` now calls
  `Table::put` (which buffers when `overlay_write_ready()` and otherwise
  falls back to `upsert_arc` verbatim). WAL emission order, write-set /
  intent recording, counter advancement and insert-or-replace semantics are
  unchanged — the record is still serialized and the key still encoded
  *before* the row moves into the table, and the write set is still
  recorded only after the write succeeds. `upsert_arc` stays reserved for
  the MultiWriter commit-merge and replay paths, which must not route
  through the overlay.
- **Empty-overlay scan short-circuit.** `merged_iter` returns
  `TableIter::Plain(tree_range)` when the overlay is empty instead of
  building a `MergedIter` with two `Peekable`s and running a peek/compare
  per step. The two-variant enum keeps `iter`/`range`'s public
  `impl Iterator` signatures unchanged. Aimed squarely at the
  `checkpoint_ms`/`snapshot_stream_ms` regression discussed below.
- **`insert_with_id` fenced with `flush_overlay()`.** It probes and writes
  `self.data` directly (duplicate-key check, then `insert_mut`).
  Unreachable with a live overlay today, but the fence keeps the
  "direct-tree access implies flushed" invariant web closed by
  construction rather than by audit.
- **`ULTIMA_OVERLAY_CAP` parse hardening.** A *set but unparsable* value
  used to fall back to the default silently (`.ok().and_then(parse.ok())`),
  which on a bench host means measuring the default while believing you
  measured an arm. It now `eprintln!`s a warning naming the offending value
  and the cap actually used. Deliberately not a panic: `Store::new` runs in
  recovery contexts where aborting is worse than a default. Untested by
  design — `std::env` is process-global and a test would race the rest of
  the suite.

## Test inventory

- **Property test (centerpiece):** `overlay_table_is_observationally_identical_to_plain_table`
  (`src/table.rs`) — an overlay table and a plain table driven by identical
  random op sequences (insert/put/update/delete/get/range/iter/len) across
  8 seeds × caps `{1, 2, 3, 8}` (forcing flushes at every boundary
  alignment) must be observationally identical at every step. This is the
  single test guarding the merge/tombstone/`len_delta` surface. The `put`
  arm (final fix wave) draws explicit keys from `0..20`, deliberately
  overlapping the auto-increment range so put-over-existing,
  put-over-tombstone and `next_id` advancement are all in the net — the
  store's `TableWriter::put` path now routes here.
- `src/overlay.rs` unit tests: sorted-insert/replace-in-place, the full
  `len_delta` transition table, CoW-clone-doesn't-leak, cap-0-disabled,
  `take_entries` reset.
- `src/table.rs`: `put_lands_in_overlay_and_advances_the_counter`,
  `set_overlay_cap_change_flushes_instead_of_dropping`, plus the existing
  batch-rollback and DDL suites exercising the overlay through
  `TableSnapshot` capture/restore and `define_index`/`define_custom_index`
  flush-and-disable.
- `src/store.rs`: `store_put_buffers_in_the_overlay` (the regression test
  for the dead-`put`-path bug: a store-level `put` must show
  `overlay_len_probe() > 0` before commit, be visible to a merged read
  pre-commit, collapse a repeated key to one buffered entry, and be visible
  to a reader after commit), SingleWriter-enable / MultiWriter-never-enables wiring,
  DDL flush+disable at the store level, MVCC visibility (old `ReadTx` sees
  its frozen overlay; new sees new state), recovery and checkpoint
  round-trips with a nonempty overlay at the cut point, and the
  `bulk_load` Delta-path fix above.

### Ported from the parallel implementation

A second, independently-written overlay implementation existed on
`feat/task58-write-overlay` (overlay on by default, cap 128, an
`ULTIMA_OVERLAY_CAP` read per write, a public `flush_overlay_for_test`
hook). Its *tests* were reviewed against this branch's and the gaps
closed; the implementation itself was not merged. All sizing below is
against this branch's `OVERLAY_CAP = 32`, and no public API was added.

- `src/store.rs` `bulk_load_delta_sees_rows_a_commit_left_in_the_write_overlay`
  and `bulk_load_delta_sees_a_base_split_between_tree_and_overlay` — the
  Delta path's `base_typed.clone() + flush_overlay() + data_ref()` had no
  test at all. The second seeds 100 rows so the base straddles the cap
  boundary (96 flushed, 4 buffered) and asserts the straddle rather than
  assuming it. Dropping the `flush_overlay()` makes them fail with
  `KeyNotFound` and `len 97 != 101`.
- `src/store.rs` `snapshot_stream_serializes_the_merged_view` — the
  snapshot-stream serialization walk (`Table::collect_serialized_rows`) is a
  *different* walk from the checkpoint's (`registry::serialize_table` via
  `Table::len`/`Table::iter`). It was **not** unpinned — a raw-tree walk
  already fails 11 of the 32 tests in `tests/snapshot_stream.rs` — but that
  existing coverage is entirely all-overlay, so a walk reading *only* the
  overlay would satisfy it too. This adds the case that separates them: a base
  straddling the cap boundary, tombstones over tree-resident rows, and an
  overlaid overwrite.
- `src/store.rs` `checkpoint_serializes_the_merged_view` re-based from
  `Persistence::standalone` onto `Persistence::smr`, so the assertions bind
  the checkpoint's own content with no WAL beside it.
- `src/store.rs` `old_snapshot_keeps_its_frozen_overlay` resized: version 1
  is 20 rows (entirely overlay-resident, asserted), version 2 is ~390 ops
  with the overwrites and deletes placed mid-stream so a flush runs *over*
  them. The previous two-write form could not observe a flush at all — it
  passes unchanged against a `flush_overlay` that discards tombstones,
  where the resized form fails with `len 400 != 393`.
- `tests/overlay_equivalence.rs` (new binary) — the MultiWriter-vs-
  SingleWriter differential and the two MultiWriter OCC guards. MultiWriter
  hard-zeroes the cap, so the two writer modes are a free differential
  oracle over the public API with no test hook. Teeth: a `Table::len` that
  ignores `len_delta` diverges the two modes; removing the MultiWriter
  cap-zero trips `merge_keys_from`'s overlay assertion in the disjoint-key
  merge guard; and the task47 mutation harness
  (`--features mutation-testing`, `ULTIMA_MUTATION=drop-merge-key` /
  `skip-writeset-validation`) fails the disjoint-key and overlapping-key
  guards respectively.
- **Not ported.** The parallel branch's bare-`Table` equivalence and batch-
  rollback drivers need its public `flush_overlay_for_test`; this branch
  covers the same ground internally via
  `table::tests::overlay_table_is_observationally_identical_to_plain_table`
  and `rollback_restores_the_overlay`, where the hooks are `pub(crate)`.
  Its `overlay_cap_independence` binary tests a per-write env-var read this
  branch resolves once in `Store::new`. Its store-level `define_index`
  mid-transaction test duplicates
  `store::tests::define_index_flushes_and_disables_the_overlay`. Its WAL-
  replay-with-a-live-overlay test is unreachable here: recovery builds
  tables through `Table::empty_with_counter` and replays via the registry
  closures, never through `WriteTx::ensure_dirty_entry`, so a replayed
  table's overlay cap stays 0 and the test would assert nothing about the
  overlay. `store::tests::recovery_replays_into_an_equivalent_table_with_overlay_pending`
  already covers the reachable half — a WAL written by a store whose
  overlay was live at the cut point replays correctly.

## Local direction numbers (sandbox, direction-only — do not draw a perf conclusion from these)

> These `perf_decomp` numbers predate the final review and predate the
> store-`put` fix (before which explicitly-keyed store writes never reached
> the overlay at all). The **method of record for local measurement** is the
> same-binary interleaved `ULTIMA_OVERLAY_CAP=0` A/B described two sections
> down; this cell is kept as the earlier direction-only artifact.

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

## `make perf/check` output — cross-host, NOT an A/B (no conclusions drawn)

`make perf/check` compares *this sandbox run* against
`autobench/baselines/*.json`, which are **NVMe-host medians recorded on a
different machine**. `autobench/CLAUDE.md` says so plainly: *"`make
perf/check` fails on the noisy virtualized sandbox by design — different
host shape, not a regression."* Every number below is therefore
**cross-host and non-comparable**: it says nothing about the overlay,
because the two sides differ in host *and* in build. It is recorded only
as run evidence that the gate was executed.

`smr-apply-microbench` (SMR mode, explicit-version `SingleWriter` — the
overlay engages on every apply, since the `state` table carries no index),
seven consecutive runs on the loaded local sandbox:

| metric | baseline (**NVMe host**) | observed (**sandbox**, 7 runs) |
|---|--:|--:|
| `apply_sw_batch_throughput` | 233,340 | 515,707 – 556,680 |
| `apply_p99_ns` | 19,099 | 12,428 – 14,370 |
| `checkpoint_ms` | 22.7 | 31.1 – 40.0 |
| `apply_throughput` (whole-pipeline, includes in-loop checkpoint) | 24,659 | 16,515 – 22,380 |
| `snapshot_stream_ms` (in-memory, no disk I/O) | 44.5 | 42.1 – 52.5 |
| `read_p99_under_load_ns` | 547 | 570 – 851 |

**Struck.** Earlier revisions of this doc read the table above as an A/B and
drew two conclusions from it — a "+121% to +139% large, stable win" on
`apply_sw_batch_throughput` and a "**−25% to −35%** stable win" on
`apply_p99_ns`, plus a "+37% to +76% stable regression" on
`checkpoint_ms`. **All three conclusions are withdrawn**: a sandbox number
against an NVMe-recorded baseline measures the host difference, not the
change. The overlay's real local signal is the same-binary A/B in the next
section — which, notably, finds `apply_p99_ns` moving in the *opposite*
direction from what this cross-host table appeared to show.

(For completeness on gate-reading hygiene: `mw-commit-microbench`
— `MultiWriter`, which never enables the overlay by construction — also
reports `mw_scaling_8x` and `mw_scaling_efficiency` "regressions" that are
pre-documented false positives: `infer_direction` gates those two the
wrong way and both values actually improved.)

## Local method of record: same-binary interleaved A/B (`ULTIMA_OVERLAY_CAP=0`)

The only trustworthy local comparison is **one binary, one host, arms
interleaved**: run `smr-apply-microbench` with the overlay at its default
cap, then again with `ULTIMA_OVERLAY_CAP=0` (which disables the overlay at
`Store::new` and reproduces main's write path exactly), alternating arms
and repeating, so host drift lands on both arms equally. Same build, same
process shape, no cross-host baseline anywhere in the comparison. Sandbox
magnitudes are still not publishable — but *sign* and *rank* are usable
when the arms don't overlap.

Result of the final review's interleaved run:

| metric | overlay vs `CAP=0`, same binary | reading |
|---|--:|---|
| `apply_sw_batch_throughput` | **+35% to +78%** | win; 8/8 repeats non-overlapping between arms |
| `apply_p99_ns` | **+31% to +124% (WORSE)** | regression; the flush spike |
| `read_p99_under_load_ns` | **−25% to −60% (better)** | win |
| `checkpoint_ms` | **+19% median (worse)** | regression |
| `snapshot_stream_ms` | **+6% median (worse)** | regression, same direction as checkpoint |

Three things follow:

1. **Throughput up, write tail down — and that trade was predicted.** The
   overlay converts a per-commit node-clone chain into a bounded per-write
   copy plus one batched tree pass every `OVERLAY_CAP` writes. The batched
   pass is *cheaper per row* but it is **not free and it is not amortized
   within a transaction**: one commit in every `OVERLAY_CAP` pays the whole
   flush inline. That spike is exactly what a p99 metric samples, so
   `apply_sw_batch_throughput` improving while `apply_p99_ns` worsens is
   the design's own trade showing up, not a contradiction. The spec
   anticipated it ("worst-case flush latency … rides inside one commit;
   `OVERLAY_CAP` bounds it"). What was *not* anticipated is the magnitude:
   +31–124% on the tail is large enough that it must be an explicit ship
   consideration, not a footnote — see Validation.
2. **`checkpoint_ms` and `snapshot_stream_ms` regress together, and the
   old WAL/disk-I/O attribution was wrong.** The prior revision blamed
   `checkpoint()`'s "fsync, a WAL-prune round-trip, `cleanup_old_checkpoints`"
   and argued `snapshot_stream_ms` was flat. Both halves fail: the
   `smr-apply-microbench` store runs in **SMR (checkpoint-only) mode,
   which has no WAL at all**, so there is no WAL-prune round-trip to
   blame; and the same-binary A/B shows `snapshot_stream_ms` *not* flat but
   regressing +6% in the same direction. The surviving hypothesis is the
   shared one: both paths serialize through `Table::merged_iter`
   (`collect_serialized_rows`), and with a nonempty overlay every step of
   that scan paid a peek/compare merge instead of a bare tree step. The
   evidence is a **sign test, not a magnitude match** (+19% vs +6% on two
   cells with different absolute costs and different noise), so this stays
   a hypothesis rather than a measurement — hedged, and to be re-tested on
   the fleet host.
3. **The empty-overlay short-circuit in the final fix wave targets exactly
   this.** `merged_iter` now returns `TableIter::Plain` — the bare tree
   iterator — whenever the overlay is empty, so every quiet-table scan
   (every `ReadTx`, every MultiWriter store, every table between flushes)
   costs one `match` per step rather than the merge. It does **not** help
   the case where the checkpoint cut lands on a *nonempty* overlay, which
   is the regime this cell measures; whether the remaining gap survives is
   a fleet-run question.

## Validation — GATE PASSED 2026-08-04

The fleet ship gate ran on 2026-08-04 and **passed at cap 32**:
eventual YCSB A 2.41 ms vs Fjall 2.71 ms same-host glibc (1.12x ahead;
F 1.35x ahead), C guardrail +3.2% (within the <=5% under-writes budget),
E improved -6%. At the spec's original cap 128 the A cell only tied Fjall
(2.76 vs 2.71); the cap sweep confirmed the corrected cost model (per-entry
Arc-clone, linear in cap) and the default is now **32**. Full numbers and
the write-tail p99 side-signal: `docs/benchmarks/write-overlay-gate-nvme-2026-08-04.md`
(results `dist/20260804T185027Z-overlay-gate/`).

## Validation

Ship iff eventual A beats Fjall same-host glibc, with C/E unchanged.
A/B recipe: `bench-oneshot competitor` plus a branch-vs-main A/F run.

**Fleet metric set** — throughput alone is no longer sufficient given the
same-binary result above. Each arm must report, at minimum:

- eventual A / F throughput (the ship criterion),
- C / E quiet-store read guardrails,
- **write-tail latency (p99/p99.9) and `apply_p99_ns`** — first-class
  ship-blockers now, not diagnostics: the local A/B found +31–124% on the
  apply tail, and a throughput win bought with a tail that large is not
  automatically a good trade,
- `checkpoint_ms` / `snapshot_stream_ms` (to settle the serialize-scan
  hypothesis above).

**`OVERLAY_CAP` tuning arms: 16 / 32 / 128** (was 64/128/256 — reversed
direction). Rationale: the corrected cost model says per-write CoW cost is
**linear in the number of buffered entries** (one `Arc` refcount atomic
plus a `K` clone each), not a flat memcpy, so it grows with cap; flush
frequency meanwhile scales as 1/cap, and each flush is a bounded batched
pass whose *per-row* cost is already the cheap part. Raising the cap
therefore makes the common-path write more expensive to buy fewer flushes,
and it makes each flush spike bigger — the wrong direction on both the
mean and the tail. Smaller caps are the promising region: they shrink both
the per-write copy and the flush spike, at the cost of more (individually
cheaper) flushes. 128 is retained as the incumbent control arm.

This fleet A/B (one c6id.2xlarge, real AWS spend) is **out of scope for
this task** — it is Step 6 of the task brief and requires explicit user
authorization before any `make up`/`bench-oneshot` per `CLAUDE.md`'s
bench-infra guardrails. Everything above this section is the local,
non-billable artifact set the fleet run consumes.
