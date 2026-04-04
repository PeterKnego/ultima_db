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

UltimaDB implements **Snapshot Isolation (SI)**, which is equivalent to PostgreSQL's *Repeatable Read* level.

### How it works

- `Store::new()` seeds a version-0 empty snapshot.
- Every `WriteTx::commit` atomically publishes a new `Arc<Snapshot>` containing an immutable copy of all tables.
- `ReadTx` holds an `Arc<Snapshot>` pinned to a specific version. Because the Arc keeps that snapshot alive regardless of subsequent commits, all reads within the transaction see a consistent, unchanging point-in-time view.
- `WriteTx` lazily copies (O(1) BTree root `Arc` clone) only the tables it touches. Changes are invisible to any concurrent `ReadTx` until `commit` returns.

### Anomalies prevented

| Anomaly            | Prevented? | Reason |
|--------------------|:----------:|--------|
| Dirty read         | Yes | Uncommitted `WriteTx` changes live only in its private `dirty` map; no `ReadTx` can observe them. |
| Nonrepeatable read | Yes | `ReadTx` is pinned to a fixed `Arc<Snapshot>`. Later commits do not mutate it. |
| Phantom read       | Yes | Range queries on a `ReadTx` iterate over the same immutable BTree root regardless of concurrent inserts. |
| Serialization anomaly | **No** | Write skew is possible (see below). |

### Write skew — the remaining anomaly

```
T1: r = rtx.open_table("doctors").len()  // sees 2
T2: r = rtx.open_table("doctors").len()  // sees 2
T1: wtx.open_table("doctors").update(1, "off_call")  // T1 goes off call
T2: wtx.open_table("doctors").update(2, "off_call")  // T2 goes off call
T1: commit → v2
T2: commit → v3   // succeeds! no conflict detected
// result: 0 doctors on call — impossible in any serial execution
```

UltimaDB does not detect this because `WriteTx` does not track which keys were *read*, only which tables were *written*.

---

## Achieving Serializable (SSI)

Serializable Snapshot Isolation adds a read-set tracking phase on top of SI. The canonical algorithm is described in the paper *Serializable Snapshot Isolation in PostgreSQL* (Ports & Grittner, VLDB 2012).

### What would need to change in UltimaDB

1. **Track read sets in `WriteTx`**: Record every `(table, key)` pair accessed via `get`, and every `(table, range)` accessed via `range`, during the transaction's lifetime.

2. **Track write sets**: Already implicit — every table in `dirty` with the keys inserted/updated/deleted.

3. **Conflict detection at commit time**: Before publishing the new snapshot, scan all `WriteTx` instances that committed *after* this transaction's base version. For each such committer:
   - If the committer wrote a key that this transaction *read* → potential rw-anti-dependency.
   - If this transaction wrote a key that the committer *read* → another rw-anti-dependency in the other direction.
   - If both directions exist in a cycle → abort with `Err(Error::WriteConflict)`.

4. **Concurrent `WriteTx` registry**: The `Store` would need a list of in-flight and recently-committed write transactions (with their read/write sets) that can be checked at commit time. Entries can be GC'd once all snapshots older than the entry's base version are released.

### Approximate API change

```rust
// WriteTx gains read-set tracking (transparent to callers):
pub fn open_table<R: 'static>(&mut self, name: &str) -> Result<&mut Table<R>>
// Table gets read-set hooks — get/range record accessed keys into WriteTx.read_set

// Store gains an in-flight registry:
struct Store {
    // ... existing fields ...
    in_flight: Vec<TxRecord>,  // read/write sets of active and recently committed txs
}

// commit() returns WriteConflict if an rw-antidependency cycle is detected
pub fn commit(self, store: &mut Store) -> Result<u64>
```

---

## How to test each isolation guarantee

The patterns below use the UltimaDB transaction API. Each test intentionally constructs the anomaly scenario and then asserts whether the anomaly occurred or was prevented.

### Dirty read (prevented by SI)

```rust
// Setup: T1 writes but does not commit; T2 must not see T1's value.
let mut store = Store::new();
let mut wtx = store.begin_write(None).unwrap();
wtx.open_table::<u32>("t").unwrap().insert(42);
// Do NOT commit yet.

let rtx = store.begin_read(None).unwrap();  // pinned to v0
assert!(matches!(rtx.open_table::<u32>("t"), Err(Error::KeyNotFound)));
wtx.rollback();
```

### Nonrepeatable read (prevented by SI)

```rust
// T_read opens a snapshot; T_write commits a change; T_read re-reads and must see the old value.
let mut store = Store::new();
{
    let mut w = store.begin_write(None).unwrap();
    w.open_table::<u32>("t").unwrap().insert(1);
    w.commit(&mut store).unwrap();
}

let rtx = store.begin_read(None).unwrap();  // pinned to v1
let first_read = *rtx.open_table::<u32>("t").unwrap().get(1).unwrap();

{
    let mut w = store.begin_write(None).unwrap();
    w.open_table::<u32>("t").unwrap().update(1, 999).unwrap();
    w.commit(&mut store).unwrap();
}

// rtx still reads from v1 — value must not have changed.
let second_read = *rtx.open_table::<u32>("t").unwrap().get(1).unwrap();
assert_eq!(first_read, second_read);  // both 1, not 999
```

### Phantom read (prevented by SI)

```rust
// T_read counts rows; T_write inserts a row; T_read re-counts and must see the old count.
let mut store = Store::new();
{
    let mut w = store.begin_write(None).unwrap();
    w.open_table::<u32>("t").unwrap().insert(10);
    w.commit(&mut store).unwrap();
}

let rtx = store.begin_read(None).unwrap();
let count_before = rtx.open_table::<u32>("t").unwrap().len();

{
    let mut w = store.begin_write(None).unwrap();
    w.open_table::<u32>("t").unwrap().insert(20);
    w.commit(&mut store).unwrap();
}

let count_after = rtx.open_table::<u32>("t").unwrap().len();
assert_eq!(count_before, count_after);  // both 1, phantom row not visible
```

### Write skew (currently possible — would be prevented by SSI)

```rust
// Two transactions each see "enough" of a shared resource, each consume one unit,
// leaving the invariant violated after both commit.
let mut store = Store::new();
{
    // Seed: two doctors on call (value 1 = on_call).
    let mut w = store.begin_write(None).unwrap();
    let t = w.open_table::<u32>("doctors").unwrap();
    t.insert(1); // doctor 1 on call
    t.insert(1); // doctor 2 on call
    w.commit(&mut store).unwrap();
}

// Both transactions start from the same base (v1).
let mut wtx_a = store.begin_write(None).unwrap();
let mut wtx_b = store.begin_write(Some(3)).unwrap();

// Both read: 2 doctors on call — safe to go off call.
let count_a = wtx_a.open_table::<u32>("doctors").unwrap().len();
let count_b = wtx_b.open_table::<u32>("doctors").unwrap().len();
assert_eq!(count_a, 2);
assert_eq!(count_b, 2);

// Each goes off call (sets their own row to 0).
wtx_a.open_table::<u32>("doctors").unwrap().update(1, 0).unwrap();
wtx_b.open_table::<u32>("doctors").unwrap().update(2, 0).unwrap();

wtx_a.commit(&mut store).unwrap();
wtx_b.commit(&mut store).unwrap(); // succeeds under SI — would abort under SSI

// Invariant violated: 0 doctors on call.
let rtx = store.begin_read(None).unwrap();
let t = rtx.open_table::<u32>("doctors").unwrap();
let on_call: u32 = t.range(..).map(|(_, v)| v).sum();
assert_eq!(on_call, 0); // demonstrates the anomaly SI allows
// Under SSI, wtx_b.commit() would return Err(Error::WriteConflict).
```

The write-skew test is structured to **demonstrate the anomaly** rather than assert it is prevented. When SSI is implemented, the final `assert_eq!(on_call, 0)` line would be replaced by an assertion that one of the commits returned `Err(Error::WriteConflict)`.
