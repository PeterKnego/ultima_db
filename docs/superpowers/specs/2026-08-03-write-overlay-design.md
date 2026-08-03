# Write Overlay (task58 candidate) — design

**Date:** 2026-08-03
**Status:** approved design, pre-implementation
**Prior art / motivation:** `docs/benchmarks/ycsb-eventual-write-decomposition-2026-08-02.md`
(the ~2 µs/txn MVCC path-clone tax), task57 (frame-at-commit + BufPool, which
fixed the WAL side), `docs/benchmarks/allocator-level-field-nvme-2026-08-03.md`
(eventual A is the last Fjall-held cell, 1.14–1.29× depending on host).

## Goal and success criterion

Flip the eventual-tier YCSB A cell: **UltimaDB beats Fjall on A, same host,
glibc, shipped config** in a fleet A/B, with quiet-store reads (C, E)
statistically unchanged and reads-under-writes within ≤5%.

The mechanism: today every commit pays an O(height×T) copy-on-write clone
chain on the B-tree because the writer's tree shares nodes with the latest
snapshot (~2 µs/txn measured; the tree op floor itself is ~250 ns). The
overlay converts that per-transaction tax into a bounded small memcpy per
transaction plus one *batched* tree pass per `OVERLAY_CAP` writes
(batching measured at ~⅓ the per-op tree cost in the task52-era data).

## V1 scope (auto-degrading restrictions)

- **SingleWriter stores only.** MultiWriter stores never enable overlays;
  the OCC merge path (`merge_keys_from` / `upsert_arc`) is untouched and
  carries a `debug_assert!(overlay.is_empty())` tripwire.
- **Non-indexed tables only.** `define_index` / `define_custom_index` flush
  the overlay and set a per-table `overlay_disabled` flag; indexed tables
  use today's write path verbatim.
- No public API, no config surface. `OVERLAY_CAP` is an internal const
  (initial 128), env-overridable for bench tuning only (task57 precedent).
- Rejected alternatives, recorded: per-commit frozen delta *chains*
  (read-probe depth grows between flushes — violates the read budget);
  in-place commit with snapshot-on-demand (changes MVCC retention
  semantics; `begin_read(Some(v))` and `num_snapshots_retained` promise
  materialized versions).

## Data structure

```rust
struct Overlay<R, K> {
    entries: Arc<Vec<(K, OverlayOp<R>)>>, // sorted by K, len <= OVERLAY_CAP
    len_delta: i64,                        // merged len = data.len() + len_delta
}
enum OverlayOp<R> { Put(Arc<R>), Tombstone }
```

`Table<R, K>` gains an `Overlay` plus the `overlay_disabled` flag.
`TableSnapshot` (batch-rollback capture) grows the same fields and
restores them wholesale.

Cost model: after a commit shares `entries` with the latest snapshot, the
next write's `Arc::make_mut` copies ≤ `OVERLAY_CAP` entries (≈2 KB at
cap 128 — ~100 ns) instead of cloning tree nodes. A binary-search insert
(memmove within the Vec) adds the same order. This bounded copy replacing
the unbounded-fanout node-clone chain is the entire trick.

## Write path

`insert` / `put` / `update` / `delete` on an enabled table:

1. If `entries.len() == OVERLAY_CAP`: **flush** (below), then continue.
2. Merged `get` (overlay-then-tree) decides existence — preserving today's
   error contract (`update` of a missing key errs; `delete` returns the
   removed record) and giving the exact `len_delta` adjustment.
3. Write the `Put(Arc<R>)` or `Tombstone` into `entries` via `make_mut` +
   binary-search insert (replacing an existing overlay entry in place).
   One asymmetry: deleting a row that exists **only in the overlay**
   (inserted since the last flush — the merged `get` hit the overlay and
   the tree does not have the key) removes the overlay entry outright
   instead of writing a tombstone. Tombstones therefore always shadow a
   real tree row, which is what makes flush's `remove_mut` infallible.
4. WAL emission in `TableWriter` is **unchanged** — the logical `WalOp` is
   produced exactly as today, before/regardless of where the row lands.
5. Auto-increment (`next_id`) logic unchanged; `insert` allocates the id
   as today, the row lands in the overlay.

Batch ops (`insert_batch` etc.) use the same path per row; their
snapshot-and-restore rollback captures the overlay with the rest.

## Flush

Triggered by a full overlay (step 1 above) and by `define_index`. The
writer owns its table clone, so flush replays the ≤ `OVERLAY_CAP` sorted
entries into `data` in one batched pass — `Put` → `insert_mut`,
`Tombstone` → `remove_mut` — then resets `entries` to a fresh empty Vec
and re-bases `len_delta` to 0. Sorted order gives the tree its best-case
locality; the batched pass clones the root/inner nodes once for the whole
batch instead of once per transaction. Worst-case flush latency (~100–200 µs
for 128 warm inserts) rides inside one commit; `OVERLAY_CAP` bounds it.
Flush on owned data is infallible by construction (entries were validated
at write time), so there is no partial-flush error path.

## Read paths

- **`get`**: `if !overlay.is_empty()` → binary-search; `Put` returns the
  record, `Tombstone` returns `None`; else fall through to the tree. Empty
  check is one branch — the quiet-store fast path.
- **All multi-row paths** (`iter`, `range`, scans, checkpoint
  serialization, snapshot streaming) flow through **one merged iterator**:
  a two-pointer merge of the sorted overlay slice and the tree iterator;
  overlay wins ties, tombstones swallow the tree's entry. Empty overlay
  degenerates to the tree iterator plus one dead branch per step.
  **Correctness linchpin: no direct `self.data.iter()`/range remains
  outside this choke point.**
- `len()` = `data.len() + len_delta` (never negative by construction).

## Durability, recovery, and composition

- The overlay is volatile acceleration only. WAL ops are logical and
  unchanged; **recovery replays through the normal table API** and simply
  rebuilds an overlay. Checkpoints/snapshot streaming serialize the merged
  view; no on-disk format changes anywhere.
- `bulk_load` installs fresh tables (empty overlay) — no interaction.
- Composes with SMR mode, every `Durability`/`WalWrite` combo, and
  `fanout-t8` without special cases.

## Testing

1. **Property test (centerpiece):** an overlay table and a plain table
   driven by identical random op sequences (insert/put/update/delete/get/
   range/iter/len, flushes forced at varying caps including 1 and 2) must
   be observationally identical. Guards the merge/tombstone/`len_delta`
   surface in one place.
2. MVCC visibility: commit, keep old `ReadTx`, write more — old snapshot
   sees its frozen overlay; new sees the new state.
3. Batch rollback restores overlay + `len_delta` exactly.
4. `define_index` on a nonempty overlay: flush happens, index sees merged
   data, subsequent writes bypass the overlay.
5. WAL-recovery round-trip and checkpoint with a nonempty overlay at the
   cut point.
6. Tombstone-over-range and tombstone-at-iterator-boundary cases.
7. MultiWriter store: overlay never engages (tripwire test).

## Validation plan

- Local: `perf_decomp` gains an overlay-vs-main cell (direction only, per
  the sandbox rules).
- Ship gate: fleet A/B on one c6id.2xlarge — main vs branch, eventual A/F
  plus C/E guardrail cells, interleaved ×2, shipped config. Ship iff
  **A beats Fjall same-host** and C/E are unchanged within noise.
  `OVERLAY_CAP` tuning (64/128/256) may use one extra arm if the first
  result is marginal.
