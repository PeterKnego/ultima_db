# Task 15: Three-Phase Consistent Persistence

## Goal

Replace the single-phase commit (hold lock → write WAL → fsync → promote
snapshot) with a three-phase protocol that minimizes lock hold time for
`Durability::Consistent` mode. Consistent fsync blocks for milliseconds; holding
the store-wide `RwLock<StoreInner>` during that time blocks all readers and
writers. The three-phase design moves the fsync wait outside the lock.

## Problem

In the original commit path, `Durability::Consistent` held the store write lock
for the entire duration of WAL write + fsync + snapshot promotion. An fsync to
spinning disk takes 5-15ms; even on NVMe it's 50-200us. During that window:

- All `begin_read()` calls block (they need a read lock on `StoreInner`).
- All other `commit()` calls block (they need a write lock).
- All `begin_write()` calls block.

This serializes the entire store on disk I/O latency, defeating the purpose of
an in-memory MVCC architecture.

## Design

### Three-phase commit protocol

```
WriteTx::commit(self)
  │
  ├─ OCC validation (MultiWriter only, unchanged)
  │
  ├─ Phase 1: PREPARE (write lock held)
  │   ├─ Finalize commit version (MultiWriter: bump if ≤ last submitted)
  │   ├─ Submit WAL entry to background thread via mpsc channel (no fsync)
  │   ├─ Record committed write set (MultiWriter OCC)
  │   ├─ Take a promotion ticket (MultiWriter; see PromoteGate below)
  │   └─ Release write lock
  │
  ├─ Phase 2: SYNC (no lock held)
  │   ├─ Block on SyncWaiter until background thread fsyncs this entry
  │   └─ MultiWriter: wait for promotion turn (ticket order)
  │
  └─ Phase 3: PROMOTE (write lock re-acquired briefly)
      ├─ Build new snapshot by forking the *current* latest and
      │   substituting this commit's tables
      ├─ Insert snapshot into inner.snapshots
      ├─ Update latest_version
      ├─ Decrement active_writer_count (SingleWriter holds the slot
      │   through the fsync wait so begin_write stays exclusive)
      └─ Run auto GC if configured
```

For `Durability::Eventual` and `Persistence::None`, phases 2 and 3 collapse:
the snapshot is promoted immediately at the end of phase 1 (the lock is never
released and re-acquired).

### Why this is safe

**Readers don't see uncommitted data.** The snapshot only becomes visible in
phase 3 after the WAL entry is durable. If the process crashes during phase 2,
the snapshot is never promoted — on recovery, the WAL entry will be replayed
to reconstruct it.

**Version gaps are temporary.** Between phase 1 and phase 3, the version number
is "allocated" (the write set is recorded) but the snapshot is not yet visible.
A concurrent `begin_read(None)` will get the previous `latest_version`. This is
correct: the transaction is not committed until phase 3 completes. If the
process crashes in phase 2, the version is recovered via WAL replay.

**OCC validation is unaffected.** Write sets are recorded in phase 1 under the
lock, so concurrent writers performing OCC validation in their own phase 1 will
see our write set. The write set being visible before the snapshot is promoted is
conservative (may cause spurious conflicts), never unsafe (cannot miss a conflict).

**`needs_cleanup` flag prevents double-decrement.** `self.needs_cleanup = false`
is set after decrementing `active_writer_count`. If the commit errors or panics
before that point (e.g. an fsync failure in phase 2), the `Drop` impl performs
the decrement instead — exactly once either way.

### Promotion ordering (lost-update fix)

The original implementation assembled the snapshot and decremented
`active_writer_count` in phase 1, *before* the fsync wait, and phase 3 merely
inserted the pre-built snapshot. That had three reproducible failure modes
while a commit was parked in phase 2:

1. **SingleWriter exclusivity broke.** `begin_write` gates on
   `active_writer_count`, which had already been decremented — a second
   writer was admitted, forked from a latest that lacked the parked commit,
   and whichever promoted at the higher version silently erased the other
   (both `commit()` calls returned `Ok`; the WAL contained both, so recovery
   diverged from live state).
2. **MultiWriter disjoint-table commits erased each other.** Per-table locks
   don't overlap for disjoint tables, so writer B's snapshot fork ran while
   writer A's snapshot was still unpromoted; B's higher-versioned snapshot
   lacked A's table wholesale.
3. **Duplicate versions.** The auto-version bump compared against
   `latest_version`, which lags while commits are parked — two writers could
   both be bumped to the same version, and the second
   `snapshots.insert(v, …)` silently replaced the first.

The fix restructures commit around one rule: **`latest_version` must strictly
advance at every promotion, and every promotion forks from the latest at
promote time.**

- **SingleWriter** holds the writer slot (`active_writer_count`) until the
  snapshot is promoted, so `begin_write` returns `WriterBusy` for the entire
  commit including the fsync wait, and builds the snapshot after the wait.
- **MultiWriter** finalizes versions at WAL submission monotonically: the
  bump compares against `max(latest_version, last_submitted_version)` and
  allocates from `next_version`, so versions are unique and strictly
  increasing in submission order, and the WAL entry version always equals
  the final commit version (recovery's version-based replay filter and
  `prune_wal` stay sound). A `PromoteGate` (ticket + condvar FIFO) then
  forces phase-3 promotion in submission order, so each promote re-forks
  from a latest that already contains every earlier commit. Tables in the
  commit's merge set cannot have changed during the wait — their per-table
  locks are held through promotion. A commit whose fsync fails advances the
  gate without promoting so later writers don't park forever.
- **Stores whose commits can never park skip the gate entirely**
  (`StoreInner::commit_may_park`): with `Persistence::None`, SMR, or
  Eventual durability, every commit holds the `inner` write lock
  continuously from version assignment through promotion, so promotion
  order trivially equals submission order and the ticket/gate would be
  pure overhead. Benchmarks (`multiwriter_persistence_bench`) showed a
  5–6% commit-throughput regression for inmemory/Eventual when all
  commits paid the gate; with the skip, all configurations are within the
  ±2% measurement noise floor of the pre-fix baseline. Only Standalone +
  Consistent stores (and test mock WALs) use the gate — there its cost is
  unmeasurable against the fsync wait. A commit that doesn't park on a
  gated store (e.g. empty WAL ops) still takes a ticket and may briefly
  wait for parked predecessors — required for correctness, since
  promoting past them would fork from a latest that lacks their data.

### Background WAL writer

Both `Consistent` and `Eventual` modes use the same background thread
architecture (`WalHandle`):

```
Committing thread              Background WAL thread
      │                                │
      ├─ send(WalEntry) via channel    │
      │                                ├─ recv() → first entry
      │                                ├─ try_recv() → drain batch
      │                                ├─ write_all() for each entry
      │                                ├─ sync_all() once for entire batch
      │                                └─ update fsynced_epoch, notify_all
      │
      ├─ (Consistent) wait on condvar
      │   until fsynced_epoch >= my epoch
      │
      └─ (Eventual) return immediately
```

**Batching:** The background thread blocks on `recv()` for the first entry, then
drains all queued entries via `try_recv()`. The entire batch shares a single
`sync_all()` call. Under load, multiple concurrent committers benefit from group
commit — one fsync covers many transactions.

**Epoch tracking (`WalSyncState`):** Each `Consistent` write atomically
increments `next_epoch` and receives an epoch number. The background thread
increments `fsynced_epoch` by the batch size after fsync. Waiters block on a
condvar until `fsynced_epoch >= their_epoch`.

### Eventual mode flush-on-drop

`WalHandle::drop()` drops the channel sender, causing the background thread's
`recv()` to return `Err`. The thread drains remaining `try_recv()` entries,
fsyncs, and exits. `drop()` then joins the thread. This guarantees all submitted
WAL entries are durable when the `Store` is dropped, even in `Eventual` mode.

## Interaction with other subsystems

### Checkpoints

Checkpoints are independent of the three-phase commit. `store.checkpoint()`
serializes the current latest snapshot (which is already promoted and visible).
After a successful checkpoint, `prune_wal()` removes WAL entries with
`version <= checkpoint_version`.

### SMR mode

SMR mode (`Persistence::Smr`) does not use WAL, so the three-phase protocol
is irrelevant — commits always take the single-phase fast path (promote
immediately under the existing lock).

### Recovery

Recovery is unchanged. WAL entries that were fsynced but whose snapshots were
never promoted (crash during phase 3) are replayed normally — the replay
logic applies ops to the table map, producing the same snapshot that would
have been promoted.

## Files changed

| File | Changes |
|---|---|
| `src/store.rs` | `commit()` restructured into three phases with lock release/reacquire |
| `src/wal.rs` | `WalHandle` unified to background thread for both modes; `WalSyncState` and `SyncWaiter` added for epoch-based sync |

## Testing

### Unit tests
- `WalSyncState` epoch tracking: waiter blocks until epoch is reached
- `SyncWaiter::Done` returns immediately (Eventual path)
- `MockWal` with `flush()` / `flush_one()` for controlled sync simulation
- `WalHandle` consistent and eventual write modes

### Integration tests
- Three-phase commit with `Consistent` durability: WAL entry is durable before
  snapshot becomes visible
- Concurrent readers are not blocked during phase 2 (fsync wait)
- Crash simulation: WAL entry written but snapshot not promoted — recovery
  replays correctly
- `Eventual` mode: snapshot promoted immediately, WAL fsynced asynchronously
- `Store::drop` flushes all pending `Eventual` WAL entries

## Decisions

1. **Unified background thread for both modes.** Originally `Consistent` used a
   direct `File` write in the committing thread while `Eventual` used a
   background thread. Unifying both behind a channel + background thread enabled
   group commit batching for `Consistent` mode and simplified the code to a
   single `WalHandle` implementation.

2. **Epoch-based sync instead of per-entry condvar.** A single monotonic counter
   (`fsynced_epoch`) with one condvar is simpler and more efficient than
   allocating a condvar per WAL entry. The background thread increments the
   epoch by the batch size, waking all waiters whose epochs are now satisfied.

3. **Lock release between phase 1 and phase 3.** The alternative — holding a
   read lock during phase 2 — would still block writers. Fully releasing the
   lock allows other transactions to proceed (reads against older snapshots,
   new `begin_write` calls, other phase-1 prepares). The cost is a second
   write-lock acquisition in phase 3, but this is a brief O(n-tables) insert.

4. **`needs_cleanup` flag for panic safety.** Rather than using a separate RAII
   guard for the writer count, a simple boolean flag checked in `Drop` handles
   the case where phase 1 completes (count decremented) but the `WriteTx` is
   dropped before phase 3 due to a panic.