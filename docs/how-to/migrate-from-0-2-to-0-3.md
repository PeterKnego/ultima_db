# How to migrate a persistent store from 0.2.x to 0.3.0

0.3.0 changed every persisted format (WAL, checkpoint, snapshot stream) for
arbitrary primary keys, and it refuses pre-0.3.0 data outright: a v1 WAL is
rejected the moment the store is **opened** (in all three `WalWrite` modes),
and a v1 checkpoint is rejected at `recover()`. This only concerns you if
you use the `persistence` feature — an in-memory store has nothing to
migrate.

**The trap:** there is no in-place upgrade, and checkpointing on 0.2.x
before upgrading does not help — 0.3.0 rejects v1 checkpoints too. Worse,
pointing a 0.3.0 store at the old directory makes it *permanently
unrecoverable*: the fresh store restarts at version 0 and writes
`checkpoint_1.bin`, recovery always picks the highest-versioned checkpoint
file, and pruning only deletes files below the newest one — so a leftover
v1 `checkpoint_500.bin` outranks your migrated data forever, and every
later `recover()` hard-errors on it. Never mix the two formats in one
directory.

The path is export with the old binary, import with the new one:

## 1. Export each table with the 0.2.x binary

There is no `export` API — a `ReadTx` walk is the export. Recover the
existing directory and write the rows out in a format you control:

```rust
// on the 0.2.x binary
let store = Store::new(
    StoreConfig::builder()
        .persistence(Persistence::standalone(
            old_dir,
            Durability::Consistent,
            WalWrite::PerEntry, // whatever your deployment used
        ))
        .build(),
)?;
store.register_table::<User>("users")?;
store.recover()?;

let rtx = store.begin_read(None)?;
let users = rtx.open_table::<User>("users")?;
let rows: Vec<(u64, User)> = users
    .iter()
    .map(|(id, u)| (id, u.clone()))
    .collect();
// serialize `rows` to a file — serde_json, bincode, CSV, your choice
```

Repeat for every table. The rows come out in ascending id order, which is
exactly what the import side wants.

## 2. Import with the 0.3.0 binary into a fresh, empty directory

Fresh means fresh: a new directory, or one from which every
`checkpoint_*.bin` and `wal.bin` has been deleted. Then bulk-load.
Existing `u64`-keyed tables need no key change:

```rust
// on the 0.3.0 binary — new_dir must contain no 0.2.x files
let store = Store::new(
    StoreConfig::builder()
        .persistence(Persistence::standalone(
            new_dir,
            Durability::Consistent,
            WalWrite::PerEntry,
        ))
        .build(),
)?;
store.register_table::<User>("users")?;

let rows: Vec<(u64, User)> = /* deserialize the export */;
store.bulk_load::<User>(
    "users",
    BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
    BulkLoadOptions::default(),
)?;
```

If several tables must land as one atomic snapshot, stage them on
`Store::bulk_load_batch()` and commit once instead. If you want to take the
opportunity to move a table off `u64` onto a natural key, load it with
`Store::bulk_load_keyed::<R, K>` — see
[How to use natural primary keys](use-natural-primary-keys.md).

## 3. Checkpoint

`BulkLoadOptions::default()` has `checkpoint_after: true`, so the load
above already wrote a checkpoint. If you disabled it, call
`store.checkpoint()?` now — until a checkpoint exists, the migrated data is
in memory only.

## 4. Verify, then retire the old directory

Reopen the new directory with a fresh 0.3.0 process, `recover()`, and spot-
check row counts before deleting or archiving the 0.2.x directory. The old
directory is your only fallback until this passes.

## Mixed-version replication

The snapshot-stream wire format also bumped (`FILE_FORMAT_V` 1 → 2), and
the break is symmetric: a 0.2.x follower cannot install a 0.3.0 leader's
stream, and a 0.3.0 follower cannot install a 0.2.x leader's — both
directions reject cleanly rather than mis-parse. For an SMR cluster this
means snapshot state transfer cannot carry state across the version
boundary, so plan the upgrade order: migrate each node's local data through
the procedure above (or upgrade a node's binary with an empty store and
re-seed it via a snapshot stream from a peer that is already on 0.3.0).
See [How to replicate a store with snapshot streams](replicate-with-snapshot-streams.md).

## Compile-time changes

The API breaks in this release (renamed `WriteConflict` field, `&K`
arguments on direct `Table` use, `#[non_exhaustive]` on
`SnapshotStreamError`) all surface as compile errors — fix what the
compiler flags, consulting the 0.3.0 section of the
[CHANGELOG](../../CHANGELOG.md). Code that goes through
`open_table`/`begin_read`/`begin_write` is largely unaffected.

## Related

- [How to bulk load and restore data](bulk-load-and-restore.md)
- [How to use natural primary keys](use-natural-primary-keys.md)
- [Key encoding and formats reference](../reference/key-encoding-and-formats.md)
