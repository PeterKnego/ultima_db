# How to prevent write skew with Serializable isolation

Write skew is the anomaly Snapshot Isolation permits: two transactions each
read an overlapping set of rows, then write disjoint rows based on what they
read, and both commit — producing a state no serial order could. If your
transactions decide what to write from reads of rows they do not write,
enable SSI:

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

## Verify the guarantee in your own tests

Construct the anomaly deliberately: two writers from the same base, each
scanning the table before writing a disjoint key. Under the default SI, both
commits succeed and the invariant breaks; the store's own versions of these
tests are `si_allows_write_skew_table_scan` and
`ssi_prevents_write_skew_via_table_scan` in `tests/store_integration.rs`.

```rust
// SI permits write skew: both commits succeed.
let store = Store::new(
    StoreConfig::builder()
        .writer_mode(WriterMode::MultiWriter)
        .isolation_level(IsolationLevel::SnapshotIsolation)
        .build(),
).unwrap();

// Seed two doctors on call.
{
    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table::<String>("doctors").unwrap();
    t.insert("on".to_string()).unwrap();
    t.insert("on".to_string()).unwrap();
    wtx.commit().unwrap();
}

// Two concurrent writers from the same base.
let mut wtx_a = store.begin_write(None).unwrap();
let mut wtx_b = store.begin_write(None).unwrap();

// A scans + writes id=1.
{ let _: Vec<_> = wtx_a.open_table::<String>("doctors").unwrap().iter().collect(); }
wtx_a.open_table::<String>("doctors").unwrap().update(1, "off".to_string()).unwrap();

// B scans + writes id=2.
{ let _: Vec<_> = wtx_b.open_table::<String>("doctors").unwrap().iter().collect(); }
wtx_b.open_table::<String>("doctors").unwrap().update(2, "off".to_string()).unwrap();

wtx_a.commit().expect("A commits");
wtx_b.commit().expect("B commits — write skew permitted under SI");

// Invariant violated: 0 doctors on call.
```

The same scenario under `IsolationLevel::Serializable` aborts the second
committer:

```rust
// SSI prevents write skew: A commits; B aborts with SerializationFailure.
let store = Store::new(
    StoreConfig::builder()
        .writer_mode(WriterMode::MultiWriter)
        .isolation_level(IsolationLevel::Serializable)
        .build(),
).unwrap();

// Same seed as the SI version.
{
    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table::<String>("doctors").unwrap();
    t.insert("on".to_string()).unwrap();
    t.insert("on".to_string()).unwrap();
    wtx.commit().unwrap();
}

let mut wtx_a = store.begin_write(None).unwrap();
let mut wtx_b = store.begin_write(None).unwrap();

// A scans (records table_scan flag) + writes id=1.
{
    let t = wtx_a.open_table::<String>("doctors").unwrap();
    assert!(t.iter().filter(|(_, s)| *s == "on").count() >= 2);
}
wtx_a.open_table::<String>("doctors").unwrap().update(1, "off".to_string()).unwrap();

// B scans (records table_scan flag) + writes id=2.
{
    let t = wtx_b.open_table::<String>("doctors").unwrap();
    assert!(t.iter().filter(|(_, s)| *s == "on").count() >= 2);
}
wtx_b.open_table::<String>("doctors").unwrap().update(2, "off".to_string()).unwrap();

wtx_a.commit().expect("A commits");

// B's iter() recorded a table scan on "doctors"; A's commit modified a key
// there after B's base version — SSI flags the conflict.
let res = wtx_b.commit();
assert!(matches!(
    res,
    Err(Error::SerializationFailure { ref table, .. }) if table == "doctors"
));
```

The SI-side anomalies work the same way with a pinned `ReadTx`: open a
`ReadTx`, commit a change from a `WriteTx`, and assert the reader still sees
its original snapshot — that one recipe verifies no dirty reads (read before
the writer commits), no nonrepeatable reads (re-read the same key after a
committed update), and no phantoms (re-count after a committed insert).
Both isolation levels give these three guarantees; only write skew separates
them.

## Related

- [How to handle write conflicts](handle-write-conflicts.md) — the
  `WriteConflict` retry loop that MultiWriter requires regardless of
  isolation level.
- [How to choose a configuration](choose-a-configuration.md) — when SSI's
  tracking cost is worth paying.
