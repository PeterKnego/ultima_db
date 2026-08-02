# How to prevent write skew with Serializable isolation

If your transactions decide what to write from reads of rows they do not
write, the default Snapshot Isolation permits write skew — see
[the isolation explanation](../explanation/isolation.md) for what the
anomaly is and why SI allows it. To prevent it, enable SSI:

```rust
let store = Store::new(
    StoreConfig::builder()
        .writer_mode(WriterMode::MultiWriter)
        .isolation_level(IsolationLevel::Serializable)
        .build(),
)?;
```

Both settings are required — `Serializable` is inert under the default
`SingleWriter` mode (no concurrent writers, nothing to validate). See the
[isolation levels reference](../reference/isolation-levels.md) for the
guarantee tables.

## Route the deciding read through the WriteTx

SSI tracks reads only on the committing `WriteTx`. A read performed on a
separate `ReadTx` — even one opened a moment earlier, on the same thread,
against the same snapshot — contributes nothing to validation, and the
commit will not abort no matter what concurrent writers did to the rows you
observed.

The wrong pattern bypasses SSI entirely:

```rust
// WRONG: read on ReadTx, then act on a separate WriteTx — no SSI protection.
let rtx = store.begin_read(None)?;
let observed = rtx.open_table::<u32>("doctors")?
    .iter()
    .filter(|(_, s)| **s == 1)
    .count();
drop(rtx);

let mut wtx = store.begin_write(None)?;
if observed >= 2 {
    wtx.open_table::<u32>("doctors")?.update(1, 0)?;
}
wtx.commit()?; // wtx.read_set is empty — write skew is NOT detected.
```

The correct pattern routes the conditional read through the same `WriteTx`:

```rust
// RIGHT: read on the WriteTx so the scan is in its read set.
let mut wtx = store.begin_write(None)?;
let observed = wtx.open_table::<u32>("doctors")?
    .iter()
    .filter(|(_, s)| **s == 1)
    .count();
if observed >= 2 {
    wtx.open_table::<u32>("doctors")?.update(1, 0)?;
}
wtx.commit()?; // SerializationFailure if a concurrent commit modified "doctors".
```

## Handle `SerializationFailure`

A failed validation returns `Error::SerializationFailure { table, version }`.
Unlike `WriteConflict` there is no `CommitWaiter` — the conflicting commit
has already finished, so there is nothing to wait for. Re-`begin_write` and
replay the transaction body immediately:

```rust
loop {
    let mut wtx = store.begin_write(None)?;
    let observed = wtx.open_table::<u32>("doctors")?
        .iter()
        .filter(|(_, s)| **s == 1)
        .count();
    if observed >= 2 {
        wtx.open_table::<u32>("doctors")?.update(1, 0)?;
    }
    match wtx.commit() {
        Ok(_) => break,
        Err(Error::SerializationFailure { .. }) => continue, // fresh base, replay
        Err(Error::WriteConflict { .. }) => continue,        // OCC still applies too
        Err(e) => return Err(e),
    }
}
```

Expect false positives: point reads (`get`, `contains`, `get_many`) are
tracked per key, but any scan — `iter`, `range`, `len`, index lookups —
records a coarse whole-table flag, so a scanning transaction conflicts with
*any* concurrent commit to that table, even on keys outside the range you
read. Retry loops must tolerate this; the granularity table is in the
[isolation levels reference](../reference/isolation-levels.md).

## Verify the guarantee

The store ships both sides of the anomaly as integration tests —
`si_allows_write_skew_table_scan` and `ssi_prevents_write_skew_via_table_scan`
in `tests/store_integration.rs`. From a checkout of the ultima_db repo:

```console
$ cargo test --test store_integration write_skew
```

To verify your own invariants, adapt that pair: run two writers from the
same base version, each scanning before writing a disjoint key, and assert
that the second commit returns `SerializationFailure` under `Serializable`
but succeeds under `SnapshotIsolation`. If you need to convince yourself of
the anomaly itself first, the walked-through scenario is in
[the isolation explanation](../explanation/isolation.md).

## Related

- [How to handle write conflicts](handle-write-conflicts.md) — the
  `WriteConflict` retry loop that MultiWriter requires regardless of
  isolation level.
- [How to choose a configuration](choose-a-configuration.md) — when SSI's
  tracking cost is worth paying.
