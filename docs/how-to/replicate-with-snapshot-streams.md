# How to replicate a store with snapshot streams

A snapshot stream is a self-validating byte stream of one frozen store
version. The producer side hands you a `SnapshotReader` implementing
`std::io::Read`; the consumer side accepts any `std::io::Read` — the
transport in between (TCP, gRPC chunks, a file) is yours. Both ends require
the `persistence` cargo feature (see the
[cargo features reference](../reference/cargo-features.md)).

## Produce a stream on the source

Call `Store::snapshot_stream(None)` to stream the latest committed version,
or `Some(v)` for a specific retained version. The snapshot is frozen at the
call — it streams from the live in-memory store under MVCC, so concurrent
writers are never blocked, and later commits do not change the bytes you
read.

```rust
let mut reader = store.snapshot_stream(None)?; // impl std::io::Read
std::io::copy(&mut reader, &mut conn)?;        // conn: your transport
```

If streaming takes long enough that auto-GC could evict the version, pin it
first. A `VersionPin` is `Send + Sync + Clone`, so hand it to the streaming
thread and drop it when done:

```rust
let pin = store.pin_version(None)?; // pin latest, race-free
std::thread::spawn({
    let store = store.clone();
    move || -> std::io::Result<()> {
        let mut reader = store
            .snapshot_stream(Some(pin.version()))
            .map_err(std::io::Error::other)?;
        std::io::copy(&mut reader, &mut conn)?;
        drop(pin); // release the version back to GC
        Ok(())
    }
});
```

Pinning is not atomic with a commit: between `commit()` returning `v` and a
later `pin_version(Some(v))`, concurrent committers can push `v` out of the
retention window, and the pin fails with `Error::VersionNotFound`. Either
pin before publishing the version number (as above, `pin_version(None)`
under a single applier is race-free), retry on the error, or keep enough
`num_snapshots_retained` slack.

## Ship from a checkpoint instead

If you would rather serve state transfer from disk than from the live store
— the usual shape of a Raft `get_current_snapshot` — open an on-disk
checkpoint as a reader. It emits the identical wire format:

```rust
let latest = store
    .list_checkpoints()?           // ascending versions
    .pop()
    .expect("no checkpoint on disk");
let mut reader = store.open_checkpoint_reader(latest)?;
```

`Store::checkpoint()` prunes older checkpoint files, so expect
`list_checkpoints` to return one entry in steady state — treat it as "what
is available", not history.

## Install on the destination

Register every expected table first (`register_table` /
`register_table_keyed` — the registry supplies the deserializers), then:

```rust
dst.register_table::<User>("users")?;
let version = dst.install_snapshot_stream(
    conn, // impl std::io::Read
    InstallOptions {
        on_extra_tables: OnExtra::Drop,
        commit_version: Some(snapshot_index),
        ..Default::default()
    },
)?;
```

Choose the options for your situation:

- **`on_extra_tables`** — what happens to tables the destination has but the
  stream does not carry. If the install must make this store an exact
  mirror of the source (Raft `InstallSnapshot` semantics), use
  `OnExtra::Drop`. If you are merging one store's tables into another, keep
  the default `OnExtra::Keep`.
- **`on_unknown_tables`** — what happens to stream tables the destination
  has not registered. The default `OnUnknown::Drop` skips them; use
  `OnUnknown::Error` if an unrecognized table means a deployment bug you
  want surfaced.
- **`commit_version`** — `Some(v)` lands the installed snapshot at exactly
  version `v` (it must be strictly above the destination's
  `latest_version`, else `InvalidCommitVersion`); `None` auto-assigns.
  Use `Some` when the version must equal a consensus log index.

Secondary indexes defined on the destination table are rebuilt automatically
over the installed rows (custom indexes are the exception — the install
refuses with `CustomIndexUnsupported`; drop them before, redefine after).

The install path treats the stream as hostile input: truncation, CRC
mismatch, key-type mismatch, and implausible lengths are all rejected
cleanly with the destination left untouched — no validation of your own is
needed (format details in the
[key encoding and formats reference](../reference/key-encoding-and-formats.md)).

## Run under a consensus log (SMR)

If a Raft/Paxos log sits above the store, durability comes from the
consensus log, not from UltimaDB — configure checkpoint-only persistence
and make version assignment explicit so replicas cannot diverge:

```rust
let store = Store::new(
    StoreConfig::builder()
        .persistence(Persistence::smr(dir))
        .require_explicit_version(true)
        .build(),
)?;
store.register_table::<User>("users")?;
store.recover()?; // load the latest local checkpoint on restart
```

Apply each committed log entry with its log index as the version, and
checkpoint periodically so a local restart replays less of the log:

```rust
let mut tx = store.begin_write(Some(log_index))?;
{
    let mut users = tx.open_table::<User>("users")?;
    users.insert(User { name: "bob".into() })?;
}
tx.commit()?;
// periodically:
store.checkpoint()?;
```

Snapshot streams are the follower-catchup mechanism in this setup: the
leader serves `open_checkpoint_reader` (or `snapshot_stream` from a pinned
live version), and the follower installs with `OnExtra::Drop` and
`commit_version: Some(snapshot_log_index)` so its store version equals the
last log index the snapshot covers — log application then resumes from
there. Checkpoint on the follower after the install so the transferred
state survives its own restarts.

A 0.2.x node and a 0.3.0 node reject each other's snapshot streams cleanly
(wire format version bump), so plan cluster upgrade order — see
[How to migrate a persistent store from 0.2.x to 0.3.0](migrate-from-0-2-to-0-3.md).

## Related

- [How to choose a configuration](choose-a-configuration.md)
- [How to bulk load and restore data](bulk-load-and-restore.md)
- [How to set up durable persistence](set-up-durable-persistence.md)
