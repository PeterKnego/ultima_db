# Isolation levels reference

UltimaDB implements two isolation levels, selected by
`StoreConfig::isolation_level`:

| Variant | Default | Equivalent to |
|---|---|---|
| `IsolationLevel::SnapshotIsolation` | ✓ | PostgreSQL Repeatable Read |
| `IsolationLevel::Serializable` | | Serializable Snapshot Isolation (SSI) |

`Serializable` takes effect only under `WriterMode::MultiWriter`; under
`SingleWriter` it behaves as `SnapshotIsolation` and pays no tracking or
validation overhead. `ReadTx` (read-only transactions) always has
snapshot-isolation guarantees and is never tracked.

For the concepts and design reasoning, see
[the isolation explanation](../explanation/isolation.md); for the working
patterns, see [How to prevent write skew](../how-to/prevent-write-skew.md).

Anomaly terms used below: *dirty read* — reading another transaction's
uncommitted write; *non-repeatable read* — re-reading a row and seeing a
concurrently committed change; *phantom read* — re-running a range query and
seeing a different row set; *write skew* — two concurrent transactions each
read an overlapping set, then write disjoint subsets, producing a state no
serial execution could.

## Anomalies prevented

### `SnapshotIsolation` (default)

| Anomaly | Prevented | Mechanism |
|---|:-:|---|
| Dirty read | yes | Uncommitted `WriteTx` changes live only in the transaction's private dirty map; no `ReadTx` can observe them. |
| Non-repeatable read | yes | `ReadTx` holds an `Arc<Snapshot>` pinned to a fixed version; later commits do not mutate it. |
| Phantom read | yes | Range queries iterate an immutable B-tree root for the life of the transaction. |
| Write skew | **no** | `WriteTx` tracks written keys only, not reads. |

### `Serializable` (opt-in)

| Anomaly | Prevented | Mechanism |
|---|:-:|---|
| Dirty read | yes | As under SnapshotIsolation. |
| Non-repeatable read | yes | As under SnapshotIsolation. |
| Phantom read | yes | As under SnapshotIsolation. |
| Write skew | yes | Each `WriteTx` records its read set; commit returns `Error::SerializationFailure` if a recorded read was invalidated by a commit after the transaction's base version. |

## Read-set tracking granularity (`Serializable` + `MultiWriter`)

Reads made through a `TableWriter` are recorded at one of two granularities:

| Granularity | Methods |
|---|---|
| Precise, per key | `get`, `contains`, `get_many`, `resolve` |
| Coarse, per-table scan flag | `iter`, `range`, `len`, `is_empty`, `first`, `last`, `get_unique`, `get_by_index`, `get_by_key`, `index_range`, `custom_index` |

Reads made through a `TableReader` (`ReadTx`) are not recorded.

## Commit validation (`Serializable` + `MultiWriter`)

Validation runs at commit, after write-set (OCC) validation, against the
committed write sets newer than the transaction's base version. A conflict is
reported when any of the following holds, first match wins:

1. A later commit deleted a table this transaction read.
2. This transaction's scan flag is set for a table, and a later commit
   modified any key in that table.
3. This transaction recorded point reads for a table, and a later commit
   modified one of those keys.

The failure is:

```rust
Error::SerializationFailure { table: String, version: u64 }
```

`SerializationFailure` carries no `CommitWaiter` (unlike
`Error::WriteConflict`): the conflicting commit has already finished. Retry
requires a fresh `begin_write` against the new base.

## Cost

| Configuration | Overhead |
|---|---|
| `SnapshotIsolation` (any writer mode) | Zero. The read set is never allocated. |
| `Serializable` + `SingleWriter` | Zero. Tracking and validation are both skipped. |
| `Serializable` + `MultiWriter` | One `BTreeSet::insert` per point read, one flag write per scan, one committed-write-sets walk at commit. |
| `ReadTx` (any configuration) | Zero. |

Measured (`examples/ssi_cost.rs`, SmallBank, 16 writers, 10 hot keys,
500 bursts × 50 ops/writer): SnapshotIsolation ≈ 10.4–10.8 k commits/s,
Serializable ≈ 10.3–10.5 k commits/s; slowdown −0.4 % to +4.9 % across runs,
mean ≈ 1–2 %. Retry ratios are not elevated under SSI (~6.5–6.9 in both
modes). Read-heavy workloads (YCSB-B) are not measured.

## Limitations (v1)

- Range, scan, and index reads use the coarse per-table flag. A concurrent
  commit to any key of a scanned table fails validation, including keys
  outside the scanned range: false positives are possible, false negatives
  are not.
- `update_batch` / `delete_batch` early-fail paths rely on commit-time OCC
  only; no read-set entry is recorded for them.
- Validation order follows committed-write-set order. Under `MultiWriter`
  that order is version order; under SMR (explicit versions) ordering is the
  caller's responsibility.
- `get_many` / `resolve` record one read-set entry per id, each allocating a
  table-name key.
- `ReadTx` is never tracked. A read-only computation cannot be validated
  against in-flight committers.
