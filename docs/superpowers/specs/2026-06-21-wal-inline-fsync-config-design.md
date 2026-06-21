# Inline-fsync WAL — productionization design

**Date:** 2026-06-21
**Branch:** `feat/wal-inline-fsync-config`
**Status:** Design — approved, pending implementation
**Supersedes:** the env-gated spike (`ULTIMA_WAL_INLINE`, landed on `main` as commit
`921fd19`). This replaces that env var with a real config knob and fixes the
spike's lock-held-during-fsync behavior.
**Related:** `../ultima_cluster/docs/wal-journal-handoff-tax-2026-06-21.md` (the
problem), `docs/tasks/task37_wal_preallocation.md` (preallocation, which makes the
handoff the dominant cost on fast disk).

## 1. Motivation

A `Durability::Consistent` commit pays a cross-thread WAL handoff (~20–35 µs):
`commit → enqueue → wake bg thread → fsync → signal → wake committer`. For
**serial (SingleWriter) commits there is no batching to amortize it**, so it is
pure tax — and once preallocation made the fsync itself cheap (~35 µs on NVMe), the
handoff is roughly *half* the durable-commit cost. Measured: removing it took the
WAL-only commit from ~35 µs to ~3 µs on tmpfs (the handoff is ~32 µs); projected
NVMe durable commit ~72 µs → ~38 µs (~1.9×).

The spike proved the win behind `ULTIMA_WAL_INLINE`. This productionizes it: a real
config knob, and the fsync moved off the store lock so readers don't block.

## 2. Decisions (settled in brainstorming)

1. **Knob = a new `Durability` variant** (`ConsistentInline`), not a new field.
   Additive enum variant → zero churn to the 36 `Persistence::Standalone { … }`
   literals; mirrors the preallocation precedent (a `WalWrite` variant chosen for
   the same reason). The mild semantic overload (the variant encodes fsync
   *mechanism*, not a different guarantee) is accepted.
2. **Off-lock inline fsync**, not lock-held. Reuse the async path's existing
   lock-drop/re-acquire commit structure so readers never block on the flush.
3. **Fail-fast** on `ConsistentInline` + `MultiWriter`: `Store::new` returns
   `Err` (the combo is an ordering hazard, see §4).
4. The spike's `ULTIMA_WAL_INLINE` env var is **removed**.

## 3. Config surface

```rust
pub enum Durability {
    Eventual,
    Consistent,
    /// Same guarantee as `Consistent` (commit blocks until the entry is
    /// fsynced; no data loss on crash). Differs only in mechanism: the
    /// committing thread performs the fsync itself — no WAL background thread,
    /// no cross-thread handoff. SingleWriter only (see Store::new). Best for
    /// serial durable commits on fast disk.
    ConsistentInline,
}
```

- `Persistence::Standalone { durability: ConsistentInline, .. }` is the only new
  usage; `dir`/`wal_write` are unchanged. `wal_write` still applies (inline works
  with `PerEntry`/`Coalesced`/`CoalescedPrealloc` — the fsync mechanism is
  orthogonal to the write strategy).
- Everywhere the code derives "is this consistent?" via
  `matches!(durability, Durability::Consistent)`, it must also match
  `ConsistentInline` (the commit must block until durable). Audit those sites.

## 4. Validation (Store::new)

- `ConsistentInline` + `WriterMode::MultiWriter` → `Err(Error::Persistence(
  "Durability::ConsistentInline requires WriterMode::SingleWriter"))`.
  **Why it's invalid:** inline has no bg thread; the sink sits behind a mutex and
  committers append in lock-acquisition order. MultiWriter finalizes commit
  *versions* under the commit lock (task15) and relies on the bg-thread/epoch path
  to keep WAL-append order == version order for recovery. Concurrent inline
  committers could append out of version order → recovery hazard. (MultiWriter also
  *wants* group-commit batching, which inline defeats.)
- `ConsistentInline` + `SingleWriter` → build the inline WAL handle.

## 5. Off-lock inline mechanism

The async `Consistent` commit already does (store.rs ~2395–2457): acquire
`store_inner` → `wal.write()` (enqueue) → set `needs_wal_wait` → **drop the lock,
`waiter.wait()` off-lock** (bg thread fsyncs) → re-acquire → install snapshot. The
SingleWriter writer slot is held across the gap, so it is already safe to release
the lock there. Inline reuses this exact structure, substituting "the committer
fsyncs itself" for "wait for the bg thread":

- **`WalHandle.sync_sink: Arc<Mutex<Box<dyn WalSink>>>`** — `Arc` so the returned
  waiter can hold the sink past the dropped store lock. No bg thread, no channel.
- **`write(entry)`** (under `store_inner`) does **no I/O**; it returns a new
  `SyncWaiter::InlineSync { sink: Arc clone, entry, durability: Arc clone,
  poison: Arc clone }`.
- **`needs_wal_wait` is true for `InlineSync`** (as it is for `WaitForEpoch`), so
  `commit()` drops `store_inner` before waiting.
- **`SyncWaiter::wait()` for `InlineSync`** (off-lock, on the committing thread):
  lock the sink; `poison.check()?`; `sink.append(&entry)?; sink.sync()?`; on `Ok`
  `durability.publish(entry.version)` and return `Ok`; on `Err` `poison.poison(..)`
  + `durability.publish_error(version, ..)` + return `Err` (the `?` aborts the
  commit and Drop releases the writer slot — identical to the async failure path).
- `commit()` then re-acquires `store_inner` and installs the snapshot, so the
  commit becomes visible only after it is durable.

Net per commit: **no cross-thread handoff** (the committer fsyncs itself) **and no
lock-held-during-fsync** (the fsync runs off-lock). Readers (`begin_read`, read
lock) never block on the flush.

### Component boundaries
- `wal.rs` owns the inline backend: the `sync_sink` field, `with_sink_inline` /
  `with_sink_kind_inline` constructors, the `InlineSync` `SyncWaiter` variant, and
  inline `write`/`request_prune`. (The spike's versions exist; this changes them
  from "fsync-under-lock, return `Done`" to "stage entry, fsync in `wait()`", and
  `sync_sink` becomes `Arc<Mutex<…>>`.)
- `store.rs` owns config validation + handle selection (the env gate is replaced
  by the `ConsistentInline` match), and the `needs_wal_wait` site already drops the
  lock — only the match that sets it changes.

## 6. Recovery & prune

- **Recovery is unchanged.** Inline writes the identical on-disk WAL format;
  `Store::recover()` reads it via `scan_wal`/`read_wal` exactly as for the async
  path. (If `wal_write: CoalescedPrealloc`, the tail-tolerant scan still applies.)
- **Prune** (checkpoint path, off the commit hot path) runs inline on the calling
  thread: lock the sink, `prune(up_to_version)`, return a ready receiver — the
  spike's approach, unchanged. Serialized with appends because there is no
  concurrent writer (SingleWriter).

## 7. Testing

- **Durability + recovery (SingleWriter + ConsistentInline):** commit N entries,
  drop without clean shutdown, reopen + `recover()` → all N present. Run for both
  `wal_write: PerEntry` and `CoalescedPrealloc`.
- **Every acked commit survives:** the durability-equivalence test, with
  `ConsistentInline`.
- **Validation:** `Store::new` with `ConsistentInline` + `MultiWriter` returns the
  expected `Err`; `+ SingleWriter` succeeds.
- **Off-lock waiter unit test:** `WalHandle::with_sink_inline` → `write()` returns
  `InlineSync` (not `Done`); `wait()` makes it durable; `read_wal` sees every entry;
  `durable_version()` advances.
- **Reader-not-blocked (best-effort):** a reader thread makes progress while a
  writer commits under `ConsistentInline` — structural assurance from the off-lock
  design; a timing assert is noisy, so a liveness/no-deadlock test suffices.
- **Default path unchanged:** full suite green with `Consistent`/`Eventual`
  (no behavior change); the removed env var breaks nothing.
- **Bench:** a `singlewriter_persistence_bench` arm on `ConsistentInline` for the
  A/B (vs `standalone_consistent`), measured on a fast-disk host.

## 8. Out of scope

- MultiWriter inline (errors by design).
- Making `ConsistentInline` any default (it stays opt-in; `Consistent` is default).
- Any change to `Consistent`/`Eventual` semantics or the async bg-thread path.
- Bench-host validation of the magnitude (separate; needs NVMe — the fleet was
  torn down).

## 9. Risks

- **`needs_wal_wait` audit:** every site that branches on `matches!(durability,
  Consistent)` must include `ConsistentInline`, or an inline commit could skip the
  durability wait (data-loss bug) or mis-handle it. The implementation must
  enumerate and update all such sites; tests cover the commit path.
- **Sink `Arc<Mutex>` overhead:** one mutex lock/unlock per commit on the inline
  path (uncontended under SingleWriter) — negligible vs the fsync.
- **Lock-drop correctness:** reuses the *existing* SingleWriter lock-drop/re-acquire
  structure (proven for the async path), so no new concurrency machinery; the risk
  is wiring `InlineSync` into the same gate correctly, which the tests pin.
