# Task 29: Durability Failure Handling

> Composes with [task28_eventual_durability_watermark.md](task28_eventual_durability_watermark.md),
> which landed in parallel and also rewrites the WAL background thread. See
> "Composition with the durable-version watermark" below for how the two unify.

## Summary

Makes `Durability::Consistent` commits **truthful**: a WAL write or fsync failure now
causes `commit()` to return `Err(Error::Poisoned)` *without* making the transaction
visible, and **poisons** the store so every subsequent `begin_write`/`commit`/`checkpoint`
returns `Error::Poisoned`. Recovery is by dropping the `Store` and re-creating it via
`Store::recover()` — there is no in-process unpoison.

### The invariant

> A `commit()` that returns `Ok` in `Durability::Consistent` means the data is fsynced
> to disk. If the WAL write or fsync fails, `commit()` returns `Err` and the transaction
> is not made visible.

This holds in both `Consistent` and `Eventual` modes. `Eventual` still loses the last
in-flight entries on crash (by definition), but a durability failure now poisons the
latch so the store stops silently accepting "durable" writes.

## The bug this fixes

The WAL background thread previously discarded the fsync result (`let _ = file.sync_all()`)
and *unconditionally* advanced its synced epoch and woke waiters. A blocked
`Consistent` `commit()` would wake, install the snapshot, and return `Ok` — claiming
durability when the append or fsync had actually failed. There was also no poison/error
state, so the store kept accepting commits forever after a WAL failure. This is the
exact failure mode every embedded database surveyed (`redb`, `sled`, `fjall`,
`lmdb`/`libmdbx`, `leveldb`, `rocksdb`) engineers against.

## Design

### `WalPoison` latch (`src/wal.rs`)

`AtomicBool` (fast-path flag) + `Mutex<Option<String>>` (first-cause-wins message).
`poison(msg)` records the cause once and sets the flag with `Release`; `check()` returns
`Err(Error::Poisoned(cause))` when set. Wrapped in `Arc`, it is shared between the WAL
background thread (sets it) and `StoreInner` (reads it).

### Background thread: detect, latch, release (`spawn_wal_thread`)

- Advances the durable epoch (`WalSyncState::fsynced_epoch`) **only after a fully
  successful batch** — every append and the single fsync succeed.
- On any append/fsync error: `poison.poison(..)`, `notify_all()` to wake waiters, and
  the thread returns. The epoch is **not** advanced for the failed batch (its entries
  never reached disk).
- Lost-wakeup safety: the epoch/poison update and `notify_all()` happen under
  `WalSyncState::mu`, the same mutex the waiter holds across its check + `condvar.wait()`.

### `SyncWaiter::wait() -> Result<()>`

Loops while `fsynced_epoch < epoch && !poisoned`. Returns `Ok` if `fsynced_epoch >= epoch`
(the entry's batch fsynced before any later failure — the epoch check comes **first**),
else `Err(Error::Poisoned)`. This makes partial batches correct: entries durable before
the failing entry resolve `Ok`; the failing entry and everything after resolve `Err`.

### Commit abort-before-install (`src/store.rs`)

`commit_single_writer` and `commit_multi_writer` install the new snapshot *after* the WAL
wait, so changing `w.wait()` to `w.wait()?` means an `Err` returns before
`snapshots.insert` / `latest_version` advance — the transaction never becomes visible.
Writer bookkeeping (`active_writer_count -= 1`, `needs_cleanup = false`) already ran
before the wait, and `WriteTx::drop` releases write intents unconditionally, so the abort
path leaks no intents and does not double-decrement.

`begin_write` and `checkpoint` call `inner.wal_poison.check()?` at entry.

### `WalSink` seam (testability)

`trait WalSink { append, sync }` abstracts the durable backing store. `FileSink` is the
production impl; `FaultySink` (test-only) injects append/sync failures. WAL-file opening
moved from the background thread (which used `.expect()` and panicked a detached thread on
failure) into `WalHandle::new`, which now returns `Result`.

### Directory fsyncs

`sync_dir(dir)` (Unix: open dir + `sync_all`; no-op elsewhere) is called after WAL file
creation (`WalHandle::new`), checkpoint rename (`checkpoint::write_checkpoint`), and WAL
prune rewrite (`wal::prune_wal`), so the directory entries / renames are durable, not just
the file contents.

## Deliberate decisions

- **Checkpoint failure does not poison.** `checkpoint()` returns `Err` on failure but does
  not poison: it writes temp + fsync + atomic rename, the previous checkpoint stays valid,
  and the WAL still holds durability, so a retry is safe and non-destructive. (A genuinely
  failing disk will poison on the next WAL fsync anyway.)
- **No in-process unpoison / no process abort.** Recovery is drop + `recover()`, matching
  redb's `needs_recovery` model rather than sled's `process::abort()`.
- **No fsyncgate EIO-retry.** The first fsync failure is treated as fatal, matching every
  database surveyed.

## Composition with the durable-version watermark (task28)

task28 added `WalDurability`, a version-keyed durability **watermark**
(`publish`/`publish_error`/`wait`/`on_complete`, plus `Store::durable_version`/
`wait_durable`/`on_durable`) running alongside the Consistent epoch path. Both features
modify the WAL background thread, so they are unified in `spawn_wal_thread`:

- **Successful batch:** advance the Consistent epoch (this task) **and**
  `durability.publish(hwm)` (task28), where `hwm` is the batch's high-water version.
- **Failed append/fsync:** **poison is authoritative** (this task) — `poison.poison(..)`
  latches the store, then `durability.publish_error(hwm, ..)` surfaces the error to
  watermark waiters at/below `hwm`, the thread stops, and `durability.close()` (after the
  loop / in `WalHandle::drop`) releases any watermark waiter parked above `hwm`.

This supersedes task28's original "transient" failure policy (its open item: "a failed
fsync poisons waiters only transiently … if a terminal-on-failure policy is preferred,
it's a small change to `publish_error`"). With this task that policy is now terminal: any
fsync failure poisons the store, so `durable_version()` simply stops advancing and every
durability waiter resolves `Err`. The watermark remains fully additive for the
success-path Eventual/Consistent use cases task28 targets.

## Known limitations

- A poison-aborted commit increments neither the `commits` nor the `rollbacks` metric (it
  is neither). After poison the store is defunct, so this metric drift is inert, but
  health checks reading those counters should treat `Error::Poisoned` as the signal.
- In MultiWriter mode, a poison abort leaves the just-pushed `committed_write_sets` entry
  for the aborted version in memory. This is inert because no later commit succeeds once
  poisoned; it is not unwound.

## Tests

- `wal_poison_latches_first_cause` — latch semantics.
- `wal_sync_failure_poisons_and_waiter_errors` — Consistent fsync failure → `wait()` errors + poisoned.
- `wal_durable_batch_before_failure_returns_ok` — partial-batch correctness.
- `wal_eventual_sync_failure_poisons` — Eventual failure poisons the latch (no waiter).
- `poisoned_commit_returns_err_and_is_not_visible` — commit returns `Err`, `latest_version` unchanged.
- `begin_write_fails_after_poison` — store refuses writes after poison.
- `sync_dir_on_normal_directory_succeeds`, `write_checkpoint_dir_fsync_roundtrip` — dir fsync guards.

## References

Survey of `../db/`: redb (`needs_recovery` + dual-slot commit), sled (`global_error` +
flusher `process::abort`), fjall (`is_poisoned` + `PoisonDart`, cites USENIX ATC '20
Rebello et al.), lmdb/libmdbx (`MDB_FATAL_ERROR`/`ENV_FATAL_ERROR`), leveldb (`bg_error_`),
rocksdb (severity ladder + retryable-only auto-resume). Related:
`docs/tasks/task13_persistence.md`, `docs/tasks/task15_three_phase_consistent_persistence.md`,
`docs/tasks/task26_journal.md`.
