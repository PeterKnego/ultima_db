# Task 28 — Eventual-Durability Watermark

**Status:** WAL half prototyped (`src/wal.rs`, `src/store.rs`) — landed behind the `persistence` feature with 11 unit tests. Journal mirror prototyped (`ultima_journal`) — `SeqWatermark` + `Journal::{durable_seq,wait_durable,on_durable}`, 9 tests. Both halves clippy clean. **SMR wiring attempted in `ultima_cluster` and REVERTED** — the monotonic watermark is the wrong primitive for the Raft log durability callback (see §"Why the watermark is wrong for the Raft log" below). The standalone WAL/journal primitives remain valid; only the openraft log-callback use of them does not.
**Module:** `src/wal.rs`, `src/store.rs` (`ultima-db`, `persistence` feature); analogous change in `ultima_journal` (`src/journal/writer.rs`, `src/journal/mod.rs`, `src/notifier.rs`).
**Related:** [task13_persistence.md](task13_persistence.md) (WAL + checkpoints), [task15_three_phase_consistent_persistence.md](task15_three_phase_consistent_persistence.md) (the `SyncWaiter`/epoch machinery this builds on), [task26_journal.md](task26_journal.md) (journal side), [task27_snapshot_stream.md](task27_snapshot_stream.md) (snapshot transport).

## Goal

Let a caller learn **when a previously-committed version has become fsync-durable**, without blocking on the commit hot path.

Today durability is observable only in `Durability::Consistent`: `commit()` blocks until fsync via `SyncWaiter::wait()` (`wal.rs:337`). In `Durability::Eventual`, `commit()` returns the moment the WAL entry is *queued* — the background thread fsyncs asynchronously (`wal.rs:427`) and the caller is handed `SyncWaiter::Done`, a fire-and-forget no-op. There is no callback, no future, and no "last durable version" accessor. A caller who wants the Eventual throughput win but also needs to *eventually* know "version N is now safe" (to ack a client, advance a replication watermark, release a buffer, prune an upstream) has no way to find out.

This task adds an opt-in, monotonic **durable-version watermark** plus a wait/notify surface, keyed by the same version number `commit()` already returns. The same primitive is mirrored in `ultima_journal`, where the wrinkle is that the journal's fsync is a *barrier* over many records, so async notifications must be resolved to the appropriate log entry by a high-water rule rather than 1:1.

## Problem

`Durability::Eventual` (`persistence.rs:36`) is designed for throughput: ~20× lower per-append latency than Consistent (see `ultima_journal/benches/append_throughput.rs`) because the synchronous fsync is removed from the hot path. But "fire and forget" is too coarse for several real callers:

- **SMR / replication** — a follower (or a leader acking to itself) needs to advance a *durable log index* watermark. That watermark must reflect actual fsync, not buffered write, or a crash loses acknowledged entries.
- **Client acks under relaxed durability** — a service that commits in Eventual mode for throughput but wants to send a "durably committed" ack to the client *after the fact*, batched, off the hot path.
- **Buffer / upstream reclamation** — anything holding resources until the data is safe on disk.

The information already exists inside the WAL background thread (it knows which versions it just fsynced); it simply is not published. The fix is mostly plumbing plus a small wait/notify surface, and it is **strictly additive** — Consistent-mode callers are unaffected.

## Design

### Key observation

`WalEntry` already carries `version: u64` (`wal.rs:54`), and the background writer drains and writes entries **in commit order** under monotonic versions (`wal.rs:416-426`). So the last entry in each fsynced batch is that batch's high-water version. Publishing the watermark is a matter of surfacing what the fsync thread already knows — not new bookkeeping on the hot path.

### The watermark primitive

Replace the opaque epoch counter in `WalSyncState` (`wal.rs:328`, which counts *entries*) with a **version-keyed** watermark. Version subsumes the epoch — it is monotonic, and it is meaningful to callers because `commit()` hands it back.

```rust
pub(crate) struct WalDurability {
    /// Highest commit version whose WAL bytes are fsynced. Monotonic.
    durable_version: AtomicU64,
    inner: Mutex<WaiterSet>,   // guards condvar + callback heap (lost-wakeup safety)
    condvar: Condvar,
}

struct WaiterSet {
    /// Min-heap of (target_version, callback). Fired when watermark >= target.
    callbacks: BinaryHeap<Reverse<(u64, Box<dyn FnOnce(Result<()>) + Send>)>>,
    /// Sticky error from a failed fsync, surfaced to waiters at/below its version.
    last_error: Option<(u64, Error)>,
}
```

### Background thread: publish after fsync

Minimal change to the writer loop (`wal.rs:416-441`). Entries arrive in version order, so the last drained entry is the batch high-water mark:

```rust
while let Ok(first) = rx.recv() {
    let mut hwm = first.version;
    write_entry_to_file(&mut file, &first)?;
    while let Ok(entry) = rx.try_recv() {
        hwm = entry.version;            // monotonic: last == highest
        write_entry_to_file(&mut file, &entry)?;
    }
    match file.sync_all() {
        Ok(())  => durability.publish(hwm),
        Err(e)  => durability.publish_error(hwm, e),
    }
}
```

`publish` keeps the existing store-under-mutex-then-notify discipline that prevents lost wakeups (today at `wal.rs:434-439`). Ready callbacks are drained into a local `Vec` and fired **after** the lock is dropped, so a callback re-entering `on_durable` cannot deadlock — the same pattern the journal's `Signal::complete` already uses (`notifier.rs:115-128`):

```rust
fn publish(&self, version: u64) {
    let ready = {
        let mut set = self.inner.lock().unwrap();
        let cur = self.durable_version.load(Acquire);
        if version > cur { self.durable_version.store(version, Release); } // defensive max
        self.condvar.notify_all();
        set.drain_ready(version)   // pop all targets <= version
    };
    for cb in ready { cb(Ok(())); }
}
```

### Public Store API (additive, opt-in)

```rust
impl Store {
    /// Highest committed version known to be fsync-durable.
    /// Eventual: trails latest_version by up to the batch/idle window.
    /// Consistent: trails by ~one in-flight batch.
    pub fn durable_version(&self) -> u64;

    /// Block until `version` is durable. No-op if already; Err on fsync failure.
    pub fn wait_durable(&self, version: u64) -> Result<()>;

    /// Fire `cb` once `version` is durable — inline if already durable.
    pub fn on_durable(&self, version: u64, cb: impl FnOnce(Result<()>) + Send + 'static);
}
```

Usage in Eventual mode — commit stays off the fsync path, durability is observed later:

```rust
let v = tx.commit()?;            // hot path: no blocking
// ... batched, or at a flush boundary ...
store.on_durable(v, |res| ack_client(res));   // or store.wait_durable(v)?
```

`pending_wal_writes()` (`store.rs:413`) already exposes the complementary "how far behind durability is" count.

`Durability::Consistent` is untouched: `commit()` keeps its synchronous phase-2 wait (task15). The watermark is additive — Consistent callers can ignore it.

## Journal side — resolving an async fsync to a log entry

In SMR deployments ultima_db runs checkpoint-only (`Persistence::Smr`); there is no WAL, and durability is provided by `ultima_journal`. The same watermark is needed there, but the resolution rule differs because **the journal's fsync is a barrier, not per-record**.

The timer path fsyncs the *active segment* (`ultima_journal/src/journal/writer.rs`, `fsync_active_segment`), which makes durable **every seq written before that fsync** — not one specific append. The resolution rule:

> `durable_seq` = the highest seq present in the file **at the instant `sync_all()` was issued**.

Snapshot the high-water seq *before* the fsync and publish it *after* success — never the live `last_seq`, or an append whose bytes raced in after the fsync started would be wrongly reported durable:

```rust
fn fsync_active_segment(state) -> Result<()> {
    let to_publish = { state.lock().unwrap().last_written_seq };  // snapshot BEFORE
    if let Some(seg) = state.lock().unwrap().segments.last_mut() { seg.fsync()?; }
    state.durability.publish(to_publish);   // everything <= this is now durable
    Ok(())
}
```

Notifiers are then registered **by seq** and resolved against the watermark, instead of completing eagerly after the buffered write (the current Eventual behavior). In Eventual mode, `append(seq, …)` parks its `Notifier` in a per-seq waiter set; the next successful fsync's `publish(hwm)` completes every parked notifier with `seq <= hwm`. The journal exposes the same `durable_seq()` / `on_durable(seq, …)` accessors.

**Semantics to document:** completion is *coarse* — a seq resolves on the first fsync that **covers** it (a later append's batch fsync, or the idle-timer fsync, whichever comes first). It is genuinely durable by then; it is just not 1:1 with the originating `append` call.

## Composition in SMR

The payoff. By the `StoreStateMachine` pinning invariant (see task27 / the `uc_service` adapter), **seq == Raft log_index == store version**: every apply opens `begin_write(Some(log_index))` and `store.latest_version() == log_index` afterward. Therefore:

```
journal.durable_seq()  ==  durable log_index  ==  durable store version
```

No mapping table is required — the identity holds by construction. The service answers "is the write at version N durable?" with `journal.durable_seq() >= N`, and openraft's durable-log watermark (needed to ack appends) is fed by the same `on_durable(seq, …)` hook. One watermark, three meanings.

## Edge cases

- **fsync failure** — a failed `sync_all` must **not** advance the watermark. Park a sticky `last_error` so waiters at/below the attempted hwm receive `Err`; let the next successful fsync (or recovery) re-advance. Open question: does one failure poison the handle permanently, or is it transient until recovery? (Lean: surface the error, keep the watermark where it was, let recovery decide.)
- **Monotonicity defense** — use `store(max(cur, v))` rather than a blind store, in case a future change reorders batches.
- **Callback hygiene** — fire drained callbacks *after* unlocking; on `close()`, fail all parked waiters with `Closed` so a never-reached target cannot leak.
- **Prune / checkpoint interaction** — `prune_wal` / `purge_before` removes entries `<=` a version that is already durable, so the watermark never regresses; assert `prune_target <= durable_version`.
- **Consistent mode unchanged** — keep `commit()`'s synchronous wait; the watermark is purely additive.

## Implementation plan

1. ✅ **`WalDurability` primitive** (`src/wal.rs`) — version-keyed watermark + parked-callback set + condvar + `closed` flag. **Prototype note:** rather than *replacing* `WalSyncState`'s epoch counter, the watermark was added *alongside* it and runs in both modes. The Consistent `SyncWaiter`/epoch path is untouched, so existing behavior carries zero regression risk. Consolidating the two (deriving the Consistent wait from `durable_version >= my_version` and dropping the epoch counter) is deferred as a non-essential cleanup.
2. ✅ **Background thread** (`WalHandle::new`) — tracks batch `hwm = max(successfully-written versions)`, calls `publish` on a successful fsync / `publish_error` on failure. `Drop` calls `close()` after joining, releasing stragglers.
3. ✅ **Store accessors** (`src/store.rs`) — `durable_version`, `wait_durable`, `on_durable`. Each clones the `Arc<WalDurability>` out from under the store read lock before (potentially) blocking, so a wait never holds `inner`. No-WAL (`None`/`Smr`) degrades to `0` / immediate `Ok`.
4. ✅ **Tests** (11, in `src/wal.rs`) — watermark advances in Eventual and Consistent; `wait_durable` already-durable is immediate; `on_complete` fires once after fsync and inline when already durable; `close` releases a parked `on_complete` and unblocks a blocked `wait` with `Err`; `publish_error` poisons waiters ≤ hwm without advancing, and a later `publish` still advances; Store-level Eventual end-to-end and no-WAL no-op. Full lib suite green (323 passed), clippy clean.
5. ✅ **Journal mirror** (`ultima_journal`) — `SeqWatermark` primitive (`src/journal/writer.rs`), published from the single-threaded `writer_loop` at both fsync sites (the Consistent per-batch fsync and the Eventual idle-timer fsync), with `Journal::{durable_seq, wait_durable, on_durable}` accessors and `close()` on drop. **Prototype notes:** (a) like the WAL half, this is *additive* — the existing per-append `Notifier` semantics are unchanged (Eventual still resolves on the buffered write); the watermark is the separate durability signal, rather than re-parking the `append` Notifier. (b) The Consistent fsync was moved out of `write_batch` into `writer_loop` so the watermark can be published *after* the fsync without holding the `WriterState` lock during callback fan-out; `write_batch` now returns the batch high-water seq. (c) The "snapshot high-water seq before fsync" rule is trivially safe here because the writer is single-threaded — `last_seq` cannot change between the snapshot and the fsync. 9 tests (`src/journal/{mod,writer}.rs`); idle-timer fsync now also surfaces fsync errors via `publish_error` (previously best-effort/ignored).
6. ❌ **SMR wiring** (`ultima_cluster` `uc_node`) — ATTEMPTED and REVERTED. Routing openraft's `IOFlushed` log-durability callback through `journal.on_durable(seq)` is **incorrect** (see below). The original per-append `Notifier` wiring is correct and was restored.

## Why the watermark is wrong for the Raft log

The `durable_seq` watermark is **monotonic** ("highest seq ever fsynced"). That is exactly right for a WAL whose versions only ever increase. But **Raft log indices are not monotonic**: a follower resolving a log conflict (or installing a snapshot) truncates a suffix and re-appends entries at indices it has already seen. So the journal seq goes *backwards* and is then rewritten.

After `truncate_after(5)` the watermark still reads `durable_seq == 10`. When the new leader's entry at index 6 is re-appended, `on_durable(6)` sees `10 >= 6` and fires openraft's `IOFlushed` callback **immediately and inline** — before the new index-6 bytes are fsynced. That both (a) signals durability prematurely (a genuine safety regression — unbounded under `Eventual`, a microsecond window under `Consistent`) and (b) fires the callback synchronously inside `append()` while `append_lock` is held, which the per-append `Notifier` never does (in `Consistent` it is always still pending when `append()` returns, so it fires later from the journal's bg thread).

The per-append `Notifier` is the correct primitive here: it tracks *this specific append operation's* durability, immune to index regression. The watermark answers "is everything up to seq N durable," which is the wrong question once N can be rewritten.

**Implication for the original goal (safe Eventual for the Raft log):** neither existing journal primitive fits. The monotonic watermark is wrong (truncation); the `Notifier` fires *pre-fsync* in `Eventual`. Safely enabling `Eventual`/group-commit for the Raft log needs a **per-append, post-fsync** signal — i.e. make the append `Notifier` resolve after the batch fsync in `Eventual` mode (the "park the append Notifier" option this task deliberately skipped in favour of the additive watermark). That is the correct follow-up, tracked separately.

### Debugging note (how this surfaced)

The wiring made `cargo test -p uc_node` hang. `rust-gdb` on the live process showed openraft's SM-apply debug assertion `assert_eq!(end - 1, got_last_index)` (`core/sm/worker.rs:214`) panicking across `leader_failover`, `snapshot_install_on_new_follower`, `membership_change_remove_node`, `three_node_cluster_elects_leader`, leaving the harness blocked on a dead worker. **However, the identical panics + hang reproduce on the clean/reverted code** — they are a *pre-existing* parallel-test-isolation bug (concurrent clusters colliding on shared shmem ring resources), unrelated to this change: the suite passes 5/5 in ~7s under `--test-threads=1`. So the hang did not validate or invalidate the wiring; the wiring was reverted on the independent correctness grounds above.

The WAL half is ~190 lines across `wal.rs` + `store.rs` (plus tests), strictly additive, with no behavior change for existing Consistent callers. The journal half follows.

### Open item

A failed fsync poisons waiters only *transiently*: the sticky `last_error` resolves versions `<=` the failed hwm to `Err`, but a later successful batch still advances the watermark and resolves higher versions. The handle is **not** permanently poisoned on a single fsync failure — matching "surface the error, let recovery decide." If a terminal-on-failure policy is preferred, it's a small change to `publish_error`.
