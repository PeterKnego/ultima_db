# How to set up durable persistence and recover after a crash

## Enable the feature

Persistence is off by default. Enable the `persistence` cargo feature:

```toml
ultima-db = { version = "0.3", features = ["persistence"] }
```

This adds `serde`/`bincode` and widens the `Record` bound to
`Serialize + DeserializeOwned`, so every stored type needs serde derives.
See the [cargo features reference](../reference/cargo-features.md).

## Configure a Standalone store

Use `Persistence::standalone(dir, durability, wal_write)` when UltimaDB owns
durability (WAL + checkpoints). Pick the durability and WAL-write mode for
your workload with [How to choose a configuration](choose-a-configuration.md);
field-by-field details are in the
[configuration reference](../reference/configuration.md).

```rust
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};

let config = StoreConfig::builder()
    .persistence(Persistence::standalone(
        dir.into(),
        Durability::Consistent,
        WalWrite::CoalescedPrealloc,
    ))
    .build();
let store = Store::new(config)?;
```

If a consensus log (Raft/Paxos) already provides durability, use
`Persistence::smr(dir)` instead — checkpoint-only, no WAL.

## Follow the startup sequence

Registration must come first: `recover()` deserializes checkpoint and WAL
bytes through the registry, and an unregistered table fails recovery.

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct User {
    name: String,
    age: u32,
}

let store = Store::new(config)?;

// 1. Register EVERY table type the store has ever persisted.
store.register_table::<User>("users")?;

// 2. Then recover: loads the latest checkpoint, replays the WAL after it.
store.recover()?;
```

If a table is keyed by anything other than `u64`, register it with
`register_table_keyed` — the key type must match the one the table was
created with, or registration/replay fails with `Error::TypeMismatch`:

```rust
store.register_table_keyed::<User, String>("users_by_email")?;
```

Run this sequence on every start, including the very first — on an empty
directory `recover()` is a cheap no-op.

## Write durably

With `Durability::Consistent` (or `ConsistentInline`), `commit()` blocks
until the batch is fsynced — when it returns `Ok`, the commit survives a
crash. Nothing more to do.

With `Durability::Eventual`, `commit()` returns before the fsync. If you
need to know when a commit became durable, use the version `commit()`
returns:

```rust
let mut wtx = store.begin_write(None)?;
wtx.open_table::<User>("users")?.insert(User {
    name: "Alice".into(),
    age: 30,
})?;
let version = wtx.commit()?;

// Poll: highest fsync-durable version (trails latest_version by up to
// one background batch window).
let _ = store.durable_version();

// Block until this commit is durable.
store.wait_durable(version)?;

// Or get an out-of-band ack on the WAL background thread.
store.on_durable(version, |res| {
    // res is Err if a covering fsync failed.
    let _ = res;
});
```

Dropping the store flushes and fsyncs all pending WAL writes, so a clean
shutdown loses nothing even under `Eventual`.

## Checkpoint to bound recovery time

`Store::checkpoint()` writes the latest snapshot to disk, then prunes the
WAL up to that version. Recovery cost is checkpoint load + replay of
everything after it, so checkpoint whenever the WAL has grown enough that
replay time would hurt — on a timer, every N commits, or at shutdown. It
takes no store lock during I/O; reads and writes proceed concurrently.

Always checkpoint after a bulk load in Standalone mode — bulk loads bypass
the WAL, and skipping the checkpoint makes them non-durable (see
[How to bulk-load, back up, and restore data](bulk-load-and-restore.md)).

## Recover after a crash

Run the same startup sequence (register, then `recover()`). What happens
next depends on what the crash left behind:

**Torn WAL tail — recovers silently.** A truncated, zero-filled, or garbage
tail on the last entry is a normal crash artifact. Recovery keeps the
durable prefix, drops the torn entry, and the store is fully usable. Under
`Consistent` durability the torn entry was never acknowledged; under
`Eventual` it falls inside the accepted loss window. No action needed.

**Mid-file corruption — fails loudly.** A CRC failure inside the WAL body
(`Error::WalCorrupted`) or anywhere in a checkpoint
(`Error::CheckpointCorrupted`) is bit rot or external damage, not a crash
artifact. Recovery refuses to proceed rather than silently dropping
committed data. Do not delete the damaged file and retry against the
remainder unless you accept losing everything it contained — restore the
directory from a backup or re-seed from a replica instead
(see [How to replicate with snapshot streams](replicate-with-snapshot-streams.md)).

**`Error::BulkLoadNotCheckpointed` — the crash followed an un-checkpointed
bulk load.** Commits made on top of the load cannot be replayed. Redo the
bulk load from its source, this time checkpointing after it. See
[How to bulk-load, back up, and restore data](bulk-load-and-restore.md).

A stray `checkpoint_*.bin.tmp` from a crash mid-checkpoint is ignored;
recovery uses the last complete checkpoint.
