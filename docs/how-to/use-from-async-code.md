# How to use UltimaDB from async code

UltimaDB's API is synchronous and blocking. It works fine under tokio or any
other runtime, provided transaction work runs where blocking is allowed.

The thread-safety bounds (asserted in `tests/send_bounds.rs`):

- `Store: Send + Sync + Clone` — clone it freely; every task holds its own
  clone.
- `ReadTx: Send + Sync` — movable and shareable.
- `WriteTx: Send + !Sync` — movable between threads, never usable from two
  at once.

So the compiler lets you hold a `WriteTx` across an `.await`. Do not.

## Why a `WriteTx` must not cross an await point

Three blocking behaviors matter on an async runtime:

- **An open `WriteTx` holds resources.** In `SingleWriter` mode it holds the
  writer slot (every other `begin_write` fails with `WriterBusy`); in
  `MultiWriter` it holds write intents on the keys it touched (conflicting
  writers park on them). Parking the task on a long `.await` stalls every
  other writer for the duration.
- **`commit()` blocks** — on the store write lock, and under
  `Durability::Consistent`/`ConsistentInline` on the WAL fsync.
- **`Drop` of an uncommitted `WriteTx` also blocks** — rollback takes the
  store write lock to release the writer slot and intents. This includes the
  implicit drop on an early return and on a cancelled or aborted task, so
  even the failure paths of an async fn block.

## Run write transactions inside `spawn_blocking`

Keep the whole open/use/commit-or-drop sequence synchronous inside one
`spawn_blocking` closure. Clone the `Store` and move the clone in:

```rust
async fn add_note(store: &Store, text: String) -> ultima_db::Result<u64> {
    let store = store.clone(); // O(1); the clone shares the same store
    tokio::task::spawn_blocking(move || {
        let mut wtx = store.begin_write(None)?;
        {
            let mut t = wtx.open_table::<String>("notes")?;
            t.insert(text)?;
        }
        wtx.commit() // returns the committed version
    })
    .await
    .expect("blocking task panicked")
}
```

The `WriteTx` is created, used, and consumed (by `commit` or by drop on the
error path) entirely inside the closure — no await point can interleave, and
cancellation of the outer future cannot strand an open transaction.

If you run `WriterMode::MultiWriter`, put the retry loop from
[How to handle write conflicts](handle-write-conflicts.md) inside the same
closure: `CommitWaiter::wait`/`wait_timeout` block the thread, so they
belong in `spawn_blocking` too.

## Reads are cheaper — but do not hold them either

`begin_read` and reads through a `ReadTx` never block: the transaction is an
`Arc` onto an immutable snapshot. Calling them directly from async code is
safe. The cost of a long-lived `ReadTx` is memory, not blocking — it pins
its snapshot version against `Store::gc()`, so a `ReadTx` held across a slow
await keeps old table versions alive.

Prefer extracting what you need and dropping the transaction before
awaiting:

```rust
let rtx = store.begin_read(None)?;
let note = rtx.open_table::<String>("notes")?.get(1).cloned();
drop(rtx); // release the snapshot pin before any await

send_somewhere(note).await;
```

If you genuinely need a consistent point-in-time view to survive across
tasks or awaits — a long export, a replication pass — use
`Store::pin_version` instead of parking a `ReadTx`: a `VersionPin` is
`Send + Sync + Clone` and keeps exactly one snapshot alive by design. See
[How to replicate with snapshot streams](replicate-with-snapshot-streams.md).

## Related

- [How to choose a configuration](choose-a-configuration.md) — durability
  settings decide how long `commit()` blocks.
- [How to handle write conflicts](handle-write-conflicts.md) — the retry
  loop that belongs inside `spawn_blocking`.
