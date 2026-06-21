# task38: WAL Inline Fsync (`Durability::ConsistentInline`)

**Status:** Implemented and validated on real NVMe (AWS c6id.4xlarge, 2026-06-21) — inline+prealloc 40.6 µs/commit vs async-PerEntry 153.5 µs (3.78×); see §6.
**Related:**
- `docs/superpowers/specs/2026-06-21-wal-inline-fsync-config-design.md` (full design spec)
- `docs/tasks/task37_wal_preallocation.md` (preallocation, which made the handoff the dominant cost on fast disk)
- `../ultima_cluster/docs/wal-journal-handoff-tax-2026-06-21.md` (problem statement + evidence)

---

## 1. Motivation

A `Durability::Consistent` commit pays a **cross-thread WAL handoff** of ~20–35 µs per
commit:

```
committer: enqueue entry ──► (wake) bg thread: fsync ──► (signal) ──► (wake) committer returns
```

That is **two scheduler wakeups per commit** — pure tax for serial (SingleWriter) commits
where there is nothing in flight to amortize the fsync over.  Before WAL preallocation
(task37), this overhead was hidden under a slow fsync (~1 ms on spinning disk); after
preallocation brought the fsync itself down to ~35 µs on NVMe, the handoff became roughly
**half the durable-commit cost**.

**Measured on tmpfs** (fsync ≈ 0, so the measurement *is* the handoff):
`singlewriter_persistence_bench / standalone_consistent`, 200 serial commits:
- async (bg-thread): **~35.0 µs/commit**
- inline (committer fsyncs itself): **~3.06 µs/commit**
- → **handoff cost ≈ 32 µs/commit**

**Projected NVMe** (from the YCSB-A / prealloc data): durable commit ~72 µs → ~38 µs (~1.9×)
once the handoff is removed.

The spike proved the win behind the `ULTIMA_WAL_INLINE` env var (commit `921fd19`).  This
feature productionizes it: a real config knob, and the fsync moved off the store lock so
readers do not block.

---

## 2. Config surface

```rust
pub enum Durability {
    Eventual,
    Consistent,
    /// Same guarantee as `Consistent` (commit blocks until the entry is fsynced; no
    /// data loss on crash).  Differs only in mechanism: the committing thread performs
    /// the fsync itself — no WAL background thread, no cross-thread handoff.
    /// SingleWriter only (see Store::new).  Best for serial durable commits on fast disk.
    ConsistentInline,
}
```

Usage:

```rust
Persistence::Standalone {
    dir,
    durability: Durability::ConsistentInline,
    wal_write: WalWrite::PerEntry,   // wal_write is orthogonal; all variants work
}
```

`ConsistentInline` is **additive** — no existing `Persistence::Standalone { .. }` literal
changes; callers that do not opt in are byte-for-byte identical.  The same rationale as
`WalWrite::CoalescedPrealloc` (task37): a new enum variant adds no churn to the existing
struct shape.

`wal_write` applies independently: `PerEntry`, `Coalesced`, and `CoalescedPrealloc` all
work with `ConsistentInline` — the write strategy and the fsync mechanism are orthogonal.

---

## 3. SingleWriter restriction

`Store::new` returns `Err(Error::Persistence("Durability::ConsistentInline requires WriterMode::SingleWriter"))` when
`ConsistentInline` + `MultiWriter` are combined.

**Why MultiWriter is unsupported:**
- The inline path has no background thread; the sink (`Arc<Mutex<Box<dyn WalSink>>>`) is
  locked by the committing thread.  Concurrent committers would acquire the mutex in
  lock-acquisition order, which can diverge from the finalized commit version order.
- `MultiWriter` finalizes commit versions under the commit lock (task15) and relies on the
  WAL-append order equalling version order for recovery correctness.  Concurrent inline
  committers could append out of version order, creating a recovery hazard.
- `MultiWriter` also wants group-commit batching to amortize per-fsync costs across
  concurrent writers; inline defeats that, so the combination would be both unsafe and
  counterproductive.

---

## 4. Off-lock inline mechanism

The async `Consistent` commit already releases the store lock before waiting for fsync
(store.rs, commit sequence): acquire `store_inner` → `wal.write()` (enqueue) → set
`needs_wal_wait` → **drop `store_inner` lock** → `waiter.wait()` off-lock (bg thread
fsyncs) → re-acquire `store_inner` → install snapshot.  The SingleWriter writer slot
is held across the drop, so releasing the lock there is already proven safe.

`ConsistentInline` reuses this exact structure, substituting "committer fsyncs itself"
for "wait for bg thread":

- **`WalHandle.sync_sink: Arc<Mutex<Box<dyn WalSink>>>`** — an `Arc` so the returned
  waiter can hold the sink reference past the dropped store lock.  No background thread,
  no channel.
- **`wal.write(entry)` (under `store_inner`)** does **no I/O**; it clones the `Arc` and
  returns `SyncWaiter::InlineSync { sink, entry, durability, poison }`.
- **`needs_wal_wait` is `true` for `InlineSync`** (same as `WaitForEpoch`), so `commit()`
  drops `store_inner` before calling `wait()`.
- **`SyncWaiter::wait()` for `InlineSync`** (off-lock, on the committing thread):
  1. Lock the sink mutex (uncontended under SingleWriter).
  2. `poison.check()?` — abort if a previous commit poisoned the WAL.
  3. `sink.append(&entry)?; sink.sync()?`
  4. On `Ok`: `durability.publish(entry.version)` → return `Ok`.
  5. On `Err`: `poison.poison(..); durability.publish_error(version, ..); return Err`.
- `commit()` then re-acquires `store_inner` and installs the snapshot — so the commit
  becomes visible only **after** it is durable.

Net per commit: **no cross-thread handoff** and **no lock-held-during-fsync**.
Readers (`begin_read`, which takes the read lock) never block on the flush.

### Bulk-load marker durability

`Store::bulk_load` in Standalone mode writes a `WalOp::BulkLoad` marker.  With the async
path this marker is handed off to the background thread like any other entry.  With
`ConsistentInline` there is no background thread, so the marker's `SyncWaiter::InlineSync`
is driven via `wait()` directly to drive the marker's waiter — the same `append + sync`
sequence as a normal commit, just invoked synchronously on the calling thread.  Recovery
is unaffected.

---

## 5. Recovery

Recovery is **unchanged**.  `ConsistentInline` writes the identical on-disk WAL format;
`Store::recover()` reads it via `scan_wal` / `read_wal` exactly as for the async path.
When `wal_write: CoalescedPrealloc`, the tail-tolerant scan still applies (recovery mode
is driven by `wal_write`, not `durability`).

Prune (checkpoint path) runs inline on the calling thread: lock the sink,
`prune(up_to_version)`, return a ready receiver.  Serialized with appends because there is
no concurrent writer (SingleWriter).

---

## 6. Measured and projected performance

| Configuration | Commit latency | Notes |
|---|---|---|
| `Consistent` (async, bg thread) | ~35.0 µs | tmpfs, handoff cost isolated |
| `ConsistentInline` (inline) | ~3.06 µs | tmpfs, handoff eliminated |
| `Consistent` (async, NVMe) | ~72 µs | projected: ~35 fsync + ~35 handoff + ~1.5 work |
| `ConsistentInline` (inline, NVMe) | ~38 µs | projected: ~35 fsync + ~1.5 work (~1.9×) |

All measurements are serial (SingleWriter), 200 commits/iteration, from
`singlewriter_persistence_bench`.

### Bench-host A/B — real NVMe (2026-06-21, confirmed)

Run on one AWS `c6id.4xlarge` node, local NVMe instance store (ext4), 16 vCPU,
`singlewriter_persistence_bench`, 200 serial commits/iteration, `TMPDIR` on NVMe:

| config | µs/commit | vs old default |
|---|---|---|
| `Consistent` async, `PerEntry` (old default) | 153.5 | 1.0× |
| `Consistent` async, `CoalescedPrealloc` | 61.2 | 2.5× |
| `ConsistentInline`, `PerEntry` | 130.8 | 1.17× |
| **`ConsistentInline`, `CoalescedPrealloc`** | **40.6** | **3.78×** |
| (reference: `inmemory` 1.70 µs/commit; `Eventual` 2.48 µs/commit) | | |

**Inline removes a fixed ~21 µs/commit handoff** (153.5→130.8 on PerEntry;
61.2→40.6 on prealloc). Its *relative* impact scales inversely with fsync cost:
only ~15% on `PerEntry` (whose ~150 µs `sync_all` dominates), but **1.51×** on the
prealloc path (61.2→40.6, the measured number landing on the design's ~38 µs
projection). Stacked with preallocation, **3.78×** vs the old async-`PerEntry`
default. Statistically clean (p<0.05); `inmemory`/`Eventual` flat as controls.

The earlier ~1.9× projection assumed a ~72 µs async baseline; the measured
singlewriter async-prealloc baseline is 61 µs, so the inline-on-prealloc win is
~1.51× — the same fixed ~21 µs handoff removal, validated on real hardware.

The `standalone_consistent_inline` bench arm (`benches/singlewriter_persistence_bench.rs`)
uses `PerEntry`; for the inline+prealloc number above its `wal_write` was switched
to `CoalescedPrealloc` for the run. Consider adding a dedicated inline+prealloc arm
if this becomes a tracked metric.

---

## 7. Testing

| Test | What it covers |
|---|---|
| `inline_wal_durability_and_recovery_{per_entry,coalesced_prealloc}` | Commit N entries, drop without clean shutdown, reopen + `recover()` → all N present |
| `inline_wal_every_acked_commit_survives` | Every acked `ConsistentInline` commit survives recovery |
| `inline_multiwriter_is_rejected` | `Store::new` with `ConsistentInline + MultiWriter` returns `Err` |
| `inline_singlewriter_succeeds` | `Store::new` with `ConsistentInline + SingleWriter` succeeds |
| `inline_wal_write_returns_inline_sync` | `WalHandle::with_sink_inline` → `write()` returns `InlineSync` (not `Done`) |
| `inline_wal_wait_makes_durable` | `wait()` drives `append+sync`; `read_wal` sees every entry; `durable_version()` advances |
| `inline_consistent_default_path_unchanged` | Full suite green with `Consistent`/`Eventual`; removed env var breaks nothing |

---

## 8. Files changed

| File | Change |
|---|---|
| `src/persistence.rs` | `Durability::ConsistentInline` variant |
| `src/wal.rs` | `sync_sink: Arc<Mutex<Box<dyn WalSink>>>`; `with_sink_inline`/`with_sink_kind_inline` constructors; `SyncWaiter::InlineSync` variant; `write()` staging; `wait()` inline append+fsync |
| `src/store.rs` | `ConsistentInline` validation (`Store::new`); inline handle selection; `needs_wal_wait` audit (both sites); `consistent`/`wal_consistent` matches updated to include `ConsistentInline`; env-gate removed |
| `benches/singlewriter_persistence_bench.rs` | `standalone_consistent_inline` bench arm (A/B pair) |
| `docs/tasks/task38_wal_inline_fsync.md` | This file |
| `docs/superpowers/specs/2026-06-21-wal-inline-fsync-config-design.md` | Retained design history |
