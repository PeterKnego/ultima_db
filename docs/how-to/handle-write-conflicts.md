# How to handle write conflicts between concurrent writers

Applies when several threads commit through the same `Store`. Enable
concurrent writers with
`StoreConfig::builder().writer_mode(WriterMode::MultiWriter).build()` — see
the [configuration reference](../reference/configuration.md) and
[How to choose a configuration](choose-a-configuration.md).

## Retry with a fresh base

`Error::WriteConflict` means a concurrent writer modified the same keys in
the same table. Your transaction's base snapshot is stale, so recovery is
always the same: drop the failed `WriteTx`, `begin_write` again (the new
transaction rebases onto the winner's snapshot), and replay the whole
transaction body — reads included, so decisions are re-made against current
data.

Put the body in a function so the replay is exact and the failed `WriteTx`
is dropped the moment it returns `Err` (adapted from
`examples/concurrent_writes.rs`):

```rust
use ultima_db::{Error, Result, Store};

fn increment(store: &Store) -> Result<u64> {
    let mut wtx = store.begin_write(None)?; // fresh base on every attempt
    {
        let mut t = wtx.open_table::<u64>("counters")?;
        let cur = t.get(1).copied().unwrap_or(0);
        t.update(1, cur + 1)?;
    }
    wtx.commit()
}

let version = loop {
    match increment(&store) {
        Ok(v) => break v,
        Err(Error::WriteConflict { .. }) => continue, // rebase and replay
        Err(e) => return Err(e),
    }
};
```

The conflict can surface at `commit()` (key-level OCC) or earlier, at the
`update`/`delete` that collided with an in-flight writer's intent. The loop
handles both, because either way the body is replayed from a fresh base.

`WriteConflict::key_digests` carries `PrimaryKey::hash64` digests, not row
keys — treat them as opaque identifiers for logging, and expect the rare
digest collision to produce a spurious conflict, which the retry loop absorbs
like any other. (Before 0.3.0 the field was named `keys` and carried row
ids.)

## Wait for the winner instead of spinning

When the conflicting writer is still in flight, the error carries
`wait_for: Some(CommitWaiter)`. Retrying immediately would just collide with
the same writer again; block on the waiter until the holder commits or
aborts, then retry:

```rust
use std::time::Duration;

let version = loop {
    match increment(&store) {
        Ok(v) => break v,
        Err(Error::WriteConflict { wait_for: Some(w), .. }) => {
            // Our failed WriteTx is already dropped (increment returned
            // Err), so waiting here cannot deadlock on our own intents.
            if !w.wait_timeout(Duration::from_secs(5)) {
                // Holder still live after 5s — a wedged writer. Surface
                // your application's timeout error instead of spinning.
                panic!("conflicting writer did not finish in 5s");
            }
            // Holder finished; the retry rebases onto its result.
        }
        Err(Error::WriteConflict { wait_for: None, .. }) => {
            // Winner already committed — retry right away.
        }
        Err(e) => return Err(e),
    }
};
```

`CommitWaiter::wait()` blocks unbounded. It is correct only if you dropped
your own `WriteTx` first (two writers waiting on each other's intents would
otherwise hang forever); prefer `wait_timeout` anywhere you cannot prove
that ordering.

## Do not treat `WriterBusy` as a conflict

`Error::WriterBusy` comes from `begin_write` in the default `SingleWriter`
mode when another `WriteTx` is currently open. Nothing conflicted and there
is nothing to rebase — the slot is simply taken. If you hit it, either
serialize writes yourself (one writer thread, a channel of write requests),
or switch to `MultiWriter` and handle `WriteConflict` as above. Retrying
`begin_write` in a loop works but is a busy-wait.

## Define indexes in their own transaction

If a MultiWriter transaction calls `define_index`/`define_custom_index` on a
table and a concurrent writer commits to that table first, the commit fails
with `Error::IndexDdlConflict` — the merge path replays only written keys
and cannot carry DDL, so the store refuses rather than silently dropping the
index. Keep DDL in a dedicated transaction that writes nothing else, and
retry on that error:

```rust
use ultima_db::IndexKind;

loop {
    let mut wtx = store.begin_write(None)?;
    wtx.open_table::<User>("users")?
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())?;
    match wtx.commit() {
        Ok(_) => break,
        Err(Error::IndexDdlConflict { .. }) => continue, // table was busy; retry
        Err(e) => return Err(e),
    }
}
```

For index usage after the DDL lands, see
[How to query with indexes](query-with-indexes.md).

## Related

- Serializable isolation adds a second retryable error,
  `SerializationFailure` — see
  [How to prevent write skew](prevent-write-skew.md).
- Retry loops from async code belong inside `spawn_blocking` — see
  [How to use UltimaDB from async code](use-from-async-code.md).
