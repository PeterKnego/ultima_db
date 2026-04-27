# Isolation Levels in UltimaDB

## The four standard SQL isolation levels

SQL defines four isolation levels by which anomalies each one prevents:

| Anomaly            | Read Uncommitted | Read Committed | Repeatable Read | Serializable |
|--------------------|:----------------:|:--------------:|:---------------:|:------------:|
| Dirty read         | possible         | prevented      | prevented       | prevented    |
| Nonrepeatable read | possible         | possible       | prevented       | prevented    |
| Phantom read       | possible         | possible       | possible*       | prevented    |
| Serialization anomaly | possible      | possible       | possible        | prevented    |

\* PostgreSQL Repeatable Read also prevents phantom reads; the SQL standard does not require this.

### Anomaly definitions

**Dirty read** — a transaction reads a value written by another transaction that has not yet committed. If the writer rolls back, the reader observed data that never existed.

**Nonrepeatable read** — a transaction reads the same row twice and gets different values because another transaction committed a change between the two reads.

**Phantom read** — a transaction re-executes a range query and gets a different set of rows because another transaction inserted or deleted rows matching the predicate.

**Serialization anomaly (write skew)** — two concurrent transactions each read an overlapping set of rows, then each writes to a disjoint subset. The combined result is one that could not have been produced by any serial execution of the two transactions. Classic example: an on-call system where both doctors see "two on call, I can take a break" and both update themselves to "not on call", leaving zero doctors on call.

---

## What UltimaDB currently implements

UltimaDB supports **two isolation levels**, selected via `StoreConfig::isolation_level`:

- **`IsolationLevel::SnapshotIsolation`** (default) — equivalent to PostgreSQL's *Repeatable Read*. Zero overhead. Prevents dirty/nonrepeatable/phantom reads but **allows write skew**.
- **`IsolationLevel::Serializable`** (opt-in) — Snapshot Isolation plus read-set tracking and commit-time validation. Additionally prevents write skew. Only takes effect under `WriterMode::MultiWriter`; in `SingleWriter` mode there are no concurrent writers and SSI degenerates to SI semantically (and pays no overhead — both tracking and validation are skipped).

Both modes share the same SI guarantees for `ReadTx` (read-only transactions cannot write-skew, so SSI tracking is not applied to them).

### How Snapshot Isolation works

- `Store::new(StoreConfig::default())` seeds a version-0 empty snapshot.
- Every `WriteTx::commit` atomically publishes a new `Arc<Snapshot>` containing an immutable copy of all tables.
- `ReadTx` holds an `Arc<Snapshot>` pinned to a specific version. Because the Arc keeps that snapshot alive regardless of subsequent commits, all reads within the transaction see a consistent, unchanging point-in-time view.
- `WriteTx` lazily copies (O(1) BTree root `Arc` clone) only the tables it touches. Changes are invisible to any concurrent `ReadTx` until `commit` returns.

### Anomalies prevented under SI

| Anomaly            | Prevented? | Reason |
|--------------------|:----------:|--------|
| Dirty read         | Yes | Uncommitted `WriteTx` changes live only in its private `dirty` map; no `ReadTx` can observe them. |
| Nonrepeatable read | Yes | `ReadTx` is pinned to a fixed `Arc<Snapshot>`. Later commits do not mutate it. |
| Phantom read       | Yes | Range queries on a `ReadTx` iterate over the same immutable BTree root regardless of concurrent inserts. |
| Serialization anomaly | **No** | Write skew is possible (see the example below). |

### Anomalies prevented under Serializable (opt-in)

| Anomaly            | Prevented? | Reason |
|--------------------|:----------:|--------|
| Dirty read         | Yes | Same as SI — uncommitted writes live only in dirty map. |
| Nonrepeatable read | Yes | Same as SI — fixed snapshot view. |
| Phantom read       | Yes | Same as SI — immutable BTree root for the duration. |
| Serialization anomaly | **Yes** | `WriteTx` records its read set; commit aborts with `Error::SerializationFailure` if any read was invalidated by a concurrent commit since the tx's base version. |

### Write skew — the anomaly SI permits but SSI prevents

```
T1: r = rtx.open_table("doctors").len()  // sees 2
T2: r = rtx.open_table("doctors").len()  // sees 2
T1: wtx.open_table("doctors").update(1, "off_call")  // T1 goes off call
T2: wtx.open_table("doctors").update(2, "off_call")  // T2 goes off call
T1: commit → v2
T2: commit → v3   // succeeds under SI! disjoint key writes, no conflict detected
// result: 0 doctors on call — impossible in any serial execution
```

Under SI, UltimaDB does not detect this because `WriteTx` does not track which keys were *read*, only which keys were *written*. Under SSI, `T2`'s `len()` call records a coarse `table_scan` flag for `doctors`; when `T1` commits a write to that table, `T2`'s commit sees the conflict via the `committed_write_sets` walk and returns `Error::SerializationFailure`.

---

## How Serializable works in UltimaDB

The implementation lives in `src/store.rs` and reuses the same `committed_write_sets` data structure that key-level OCC (task 19) and the sharded commit path (task 20) already maintain. The full per-feature record is in `docs/tasks/task21_serializable_isolation.md`.

### Read-set tracking

Each `WriteTx` carries:

```rust
read_set: Option<RefCell<BTreeMap<String, ReadSetEntry>>>
```

`None` in `IsolationLevel::SnapshotIsolation` (zero overhead — branch elided). Also `None` under `IsolationLevel::Serializable` + `WriterMode::SingleWriter`, since SingleWriter has no concurrent writers and validation is unconditionally skipped (allocating a read set there would be pure waste). `Some(_)` only in `IsolationLevel::Serializable` + `WriterMode::MultiWriter`. Keyed by table name, with:

```rust
struct ReadSetEntry {
    keys: BTreeSet<u64>,   // precise per-key reads
    table_scan: bool,      // coarse "any non-key read happened" flag
}
```

The `RefCell` is required because `WriteTx`'s read methods on `TableWriter` take `&self`, but tracking mutates the read set. `WriteTx` is `!Send + !Sync`, so no `Mutex` is needed.

### Two granularities of tracking

`TableWriter` read methods route through one of two helpers, `record_point_read` and `record_table_scan`:

- **Precise per-key** — `get`, `contains`, `get_many`, `resolve` (each individual id is recorded).
- **Coarse table-scan flag** — `iter`, `range`, `len`, `is_empty`, `first`, `last`, `get_unique`, `get_by_index`, `get_by_key`, `index_range`, `custom_index`.

`TableReader` (used by `ReadTx`) does not record reads — read-only transactions cannot produce write skew, so SSI tracking is not applied.

### Validation at commit

`validate_read_set` runs in `commit_multi_writer`'s Phase 1, after `validate_write_set`. It walks the same `committed_write_sets` vector that OCC already maintains, skipping entries with `cws.version <= base_version`. For each later-than-base committed write set, and each `(table, entry)` in the read set, three conflict criteria apply:

1. The committer **deleted** a table we read → conflict.
2. `entry.table_scan == true` and the committer modified **any key** in that table → conflict.
3. `entry.table_scan == false` and the committer modified a key that intersects `entry.keys` → conflict.

First-match wins. The failure mode is:

```rust
Error::SerializationFailure { table: String, version: u64 }
```

There is no `wait_for` (unlike `WriteConflict`) because the conflicting committer has **already finished** — there is nothing left to wait on. Retry must rebase against a fresh base (re-`begin_write` and replay).

`commit_single_writer` does not call `validate_read_set`. SingleWriter has no concurrent writers, so SSI is correctly a no-op there. `begin_write` further gates the `read_set` allocation on `(Serializable, MultiWriter)`, so SingleWriter+SSI also pays nothing for per-read tracking — `record_*` short-circuits at `as_ref()?` exactly like SI.

### Cost

- `IsolationLevel::SnapshotIsolation` (default): zero overhead. The `Option` is `None`, every `record_*` call short-circuits, the `validate_read_set` branch is elided.
- `IsolationLevel::Serializable` + `WriterMode::SingleWriter`: zero overhead. Both validation AND tracking are skipped — the `read_set` is `None` (gated on `(Serializable, MultiWriter)` at `begin_write`), so `record_*` calls elide just like SI, and `commit_single_writer` skips `validate_read_set`.
- `IsolationLevel::Serializable` + `WriterMode::MultiWriter`: one `BTreeSet::insert` per point read, one `bool` set per scan, plus one `committed_write_sets` walk at commit. Estimated <5% on write-heavy workloads (smallbank), 10–20% on read-heavy mixes (YCSB-B).
- `ReadTx` (always, regardless of mode): zero overhead.

### Using SSI correctly

**SSI tracks reads only on the committing `WriteTx`, not on separate `ReadTx` instances.**
The read set lives on `WriteTx`; `validate_read_set` walks *that* writer's recorded
reads. A scan performed through a `ReadTx` opened earlier — even one second earlier,
on the same thread, against the same snapshot — contributes nothing to the
`WriteTx` that follows it, and the commit will not abort no matter what concurrent
writers did to the rows the `ReadTx` observed.

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
wtx.commit()?; // SerializationFailure if any concurrent commit modified "doctors".
```

This is a deliberate design choice. `ReadTx` is read-only and cannot write-skew on
its own, so tracking is per-writer: each `WriteTx` is the unit of "what did this
transaction observe before deciding to write?", and the validator only needs to
check the reads of the writer that's about to commit. If you need a serializable
read-modify-write, do the read on the `WriteTx`.

### v1 caveats

- **Range/scan reads use coarse table-scan tracking.** `iter`, `range`, `index_range`, `len`, `is_empty`, `first`, `last`, `get_unique`, `get_by_index`, `get_by_key`, `custom_index` flip the `table_scan` flag for the table; any concurrent commit on that table is then treated as a conflict at validation time. This produces **false positives** (a concurrent commit on a key outside the read range will still fail the SSI commit) but no **false negatives**. Range-key tracking is a v2 follow-up.
- **No SSI for `update_batch` / `delete_batch` early-fail.** Those still rely on commit-time OCC (same as task 20's batch limitation).
- **First-match validation** by `committed_write_sets` vector position. In MultiWriter the vector is naturally version-ordered; in SMR mode (explicit versions) ordering is the caller's responsibility.
- **`get_many` / `resolve` allocate per id.** Batch reads call `record_point_read` once per id, each constructing a `String::from(table)` entry key. A batch-aware helper would collapse to one allocation; deferred until a profile shows it.
- **`ReadTx` has no SSI tracking.** Read-only transactions cannot produce write skew, so tracking is intentionally not applied. A workflow needing a serializable-equivalent guarantee against in-flight committers across a longer read-only computation would need a different mechanism.

---

## How to test each isolation guarantee

The patterns below use the UltimaDB transaction API. Each test intentionally constructs the anomaly scenario and then asserts whether the anomaly occurred or was prevented.

### Dirty read (prevented by SI)

```rust
// Setup: T1 writes but does not commit; T2 must not see T1's value.
let store = Store::new(StoreConfig::default()).unwrap();
let mut wtx = store.begin_write(None).unwrap();
wtx.open_table::<u32>("t").unwrap().insert(42).unwrap();
// Do NOT commit yet.

let rtx = store.begin_read(None).unwrap();  // pinned to v0
assert!(matches!(rtx.open_table::<u32>("t"), Err(Error::KeyNotFound)));
wtx.rollback();
```

### Nonrepeatable read (prevented by SI)

```rust
// T_read opens a snapshot; T_write commits a change; T_read re-reads and must see the old value.
let store = Store::new(StoreConfig::default()).unwrap();
{
    let mut w = store.begin_write(None).unwrap();
    w.open_table::<u32>("t").unwrap().insert(1).unwrap();
    w.commit().unwrap();
}

let rtx = store.begin_read(None).unwrap();  // pinned to v1
let first_read = *rtx.open_table::<u32>("t").unwrap().get(1).unwrap();

{
    let mut w = store.begin_write(None).unwrap();
    w.open_table::<u32>("t").unwrap().update(1, 999).unwrap();
    w.commit().unwrap();
}

// rtx still reads from v1 — value must not have changed.
let second_read = *rtx.open_table::<u32>("t").unwrap().get(1).unwrap();
assert_eq!(first_read, second_read);  // both 1, not 999
```

### Phantom read (prevented by SI)

```rust
// T_read counts rows; T_write inserts a row; T_read re-counts and must see the old count.
let store = Store::new(StoreConfig::default()).unwrap();
{
    let mut w = store.begin_write(None).unwrap();
    w.open_table::<u32>("t").unwrap().insert(10).unwrap();
    w.commit().unwrap();
}

let rtx = store.begin_read(None).unwrap();
let count_before = rtx.open_table::<u32>("t").unwrap().len();

{
    let mut w = store.begin_write(None).unwrap();
    w.open_table::<u32>("t").unwrap().insert(20).unwrap();
    w.commit().unwrap();
}

let count_after = rtx.open_table::<u32>("t").unwrap().len();
assert_eq!(count_before, count_after);  // both 1, phantom row not visible
```

### Write skew under SI (anomaly is permitted)

The default `IsolationLevel::SnapshotIsolation` does **not** detect write skew. Two
writers, each scanning the table to verify "at least one other doctor is on call",
each updating themselves off-call, will both commit — the disjoint-key writes look
like normal MultiWriter OCC traffic.

```rust
// SI permits write skew: both commits succeed.
let store = Store::new(StoreConfig {
    writer_mode: WriterMode::MultiWriter,
    isolation_level: IsolationLevel::SnapshotIsolation,
    ..StoreConfig::default()
}).unwrap();

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

### Write skew under Serializable (prevented)

The same scenario with `IsolationLevel::Serializable` records each writer's `iter()`
as a coarse `table_scan` on `doctors`. When A commits a key in that table, B's
commit-time validation walks `committed_write_sets`, sees `cws.tables.contains_key("doctors")`,
and aborts with `Error::SerializationFailure`. The retry path must rebase
(re-`begin_write` and replay against the fresh base).

```rust
// SSI prevents write skew: A commits; B aborts with SerializationFailure.
let store = Store::new(StoreConfig {
    writer_mode: WriterMode::MultiWriter,
    isolation_level: IsolationLevel::Serializable,
    ..StoreConfig::default()
}).unwrap();

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

// B's iter() recorded table_scan=true on "doctors". A's commit modified a key in
// "doctors" after B's base version → SSI flags the conflict.
let res = wtx_b.commit();
assert!(matches!(
    res,
    Err(Error::SerializationFailure { ref table, .. }) if table == "doctors"
));
```

These two tests live inlined in `tests/store_integration.rs` as
`si_allows_write_skew_table_scan` and `ssi_prevents_write_skew_via_table_scan`.
Two further integration tests cover the precise (point-key) path:
`ssi_read_then_write_conflicts_on_concurrent_modify` (point read invalidated by
concurrent write) and `ssi_disjoint_point_reads_dont_conflict` (disjoint
point reads/writes do not produce false positives).
