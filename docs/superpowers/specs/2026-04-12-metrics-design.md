# Metrics & Observability Design

## Goal

Add operational metrics to UltimaDB so users can monitor store and table activity. Two layers:

1. **Internal counters** (always compiled, zero external deps) — atomic counters read via `store.metrics()` returning a plain snapshot struct.
2. **`metrics` crate integration** (behind `feature = "metrics"`) — emits the same counters through the `metrics` facade so users can plug in Prometheus, OpenTelemetry, stdout, etc.

## Public API

### `Store::metrics()`

```rust
impl Store {
    /// Returns a point-in-time snapshot of all operational metrics.
    pub fn metrics(&self) -> MetricsSnapshot;
}
```

### Snapshot types

```rust
pub struct MetricsSnapshot {
    pub commits: u64,
    pub rollbacks: u64,
    pub gc_runs: u64,
    pub snapshots_collected: u64,
    pub write_conflicts: u64,
    pub tables: HashMap<String, TableMetricsSnapshot>,
}

pub struct TableMetricsSnapshot {
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub primary_key_reads: u64,
    pub primary_key_scans: u64,
    pub indexes: HashMap<String, IndexMetricsSnapshot>,
}

pub struct IndexMetricsSnapshot {
    pub reads: u64,
    pub range_scans: u64,
}
```

### `TableReader` (new wrapper)

`ReadTx::open_table` returns `TableReader<'_, R>` instead of `&Table<R>`. This wraps `&Table<R>` + metrics reference to instrument read operations.

```rust
pub struct TableReader<'tx, R: Send + Sync + 'static> {
    table: &'tx Table<R>,
    metrics: &'tx StoreMetrics,
    table_name: String,
}
```

`TableReader` exposes the same read methods as `TableWriter`: `get`, `get_many`, `contains`, `first`, `last`, `resolve`, `range`, `iter`, `len`, `is_empty`, `get_unique`, `get_by_index`, `get_by_key`, `index_range`, `custom_index`.

This is a breaking change to `ReadTx::open_table` and the `Readable` trait.

## Internal Counters

### `StoreMetrics` (`pub(crate)`)

```rust
pub(crate) struct StoreMetrics {
    // Store-level
    commits: AtomicU64,
    rollbacks: AtomicU64,
    gc_runs: AtomicU64,
    snapshots_collected: AtomicU64,
    write_conflicts: AtomicU64,

    // Per-table and per-index
    tables: RwLock<HashMap<String, TableMetrics>>,
}

pub(crate) struct TableMetrics {
    inserts: AtomicU64,
    updates: AtomicU64,
    deletes: AtomicU64,
    primary_key_reads: AtomicU64,
    primary_key_scans: AtomicU64,
    indexes: RwLock<HashMap<String, IndexMetrics>>,
}

pub(crate) struct IndexMetrics {
    reads: AtomicU64,
    range_scans: AtomicU64,
}
```

All atomic operations use `Relaxed` ordering — we need no cross-thread synchronization guarantees from the counters themselves.

`StoreMetrics` exposes increment methods:

- `inc_commit()`, `inc_rollback()`, `inc_gc_run()`, `inc_snapshots_collected(n: u64)`, `inc_write_conflict()`
- `inc_inserts(table, n)`, `inc_updates(table, n)`, `inc_deletes(table, n)`
- `inc_primary_key_reads(table, n)`, `inc_primary_key_scans(table)`
- `inc_index_reads(table, index)`, `inc_index_range_scans(table, index)`
- `register_table(name)`, `register_index(table, index)` — called from `define_index`
- `snapshot()` → `MetricsSnapshot` — reads all atomics

## Wiring

- `Store` owns `Arc<StoreMetrics>`.
- `WriteTx` receives `Arc<StoreMetrics>` clone at creation.
- `TableWriter` holds `&StoreMetrics` reference + `table_name: String`.
- `ReadTx` receives `Arc<StoreMetrics>` clone at creation.
- `TableReader` holds `&StoreMetrics` reference + `table_name: String`.

## Instrumentation Points

### Store-level (in `store.rs`)

| Counter | Location |
|---|---|
| `commits` | `WriteTx::commit()`, after successful snapshot publish |
| `rollbacks` | `WriteTx::rollback()` and `WriteTx::Drop` (only if `commit()` was not called) |
| `gc_runs` | `Store::gc()`, at entry |
| `snapshots_collected` | `Store::gc()`, incremented by number of versions removed |
| `write_conflicts` | `WriteTx::commit()`, on conflict detection (MultiWriter path) |

### Table-level (in `TableWriter` and `TableReader`)

| Counter | Methods |
|---|---|
| `inserts` | `insert()` +1, `insert_batch()` +batch_size (on success) |
| `updates` | `update()` +1, `update_batch()` +batch_size (on success) |
| `deletes` | `delete()` +1, `delete_batch()` +batch_size (on success) |
| `primary_key_reads` | `get()` +1, `get_many()` +count, `contains()` +1, `first()` +1, `last()` +1, `resolve()` +count |
| `primary_key_scans` | `range()` +1, `iter()` +1 |

### Index-level (in `TableWriter` and `TableReader`)

| Counter | Methods |
|---|---|
| `reads` | `get_unique()` +1, `get_by_index()` +1, `get_by_key()` +1 |
| `range_scans` | `index_range()` +1 |

Index metrics entries are created when `define_index` is called, staying in sync with actual indexes.

## `metrics` Crate Integration

Behind `feature = "metrics"`. At each instrumentation point, alongside the atomic bump, emit via the `metrics` facade.

### Metric names and labels

```
ultima.commits                                          (counter)
ultima.rollbacks                                        (counter)
ultima.gc_runs                                          (counter)
ultima.snapshots_collected                              (counter)
ultima.write_conflicts                                  (counter)

ultima.table.inserts         table=<name>               (counter)
ultima.table.updates         table=<name>               (counter)
ultima.table.deletes         table=<name>               (counter)
ultima.table.primary_reads   table=<name>               (counter)
ultima.table.primary_scans   table=<name>               (counter)

ultima.index.reads           table=<name>, index=<name> (counter)
ultima.index.range_scans     table=<name>, index=<name> (counter)
```

### Emission helper

```rust
#[cfg(feature = "metrics")]
#[inline]
fn emit(name: &'static str, labels: &[(&'static str, String)], val: u64) {
    metrics::counter!(name, labels).increment(val);
}
```

Called inside each `StoreMetrics::inc_*` method. Compiles away entirely when the feature is off.

### Cargo.toml

```toml
[features]
metrics = ["dep:metrics"]

[dependencies]
metrics = { version = "0.24", optional = true }
```

## Performance

- Atomic `fetch_add` with `Relaxed` ordering: ~1ns per operation.
- `RwLock` read on the table/index maps: uncontended read lock, ~10-20ns. Only taken on per-operation metrics lookup (not on store-level counters).
- `String` clone for `table_name` at `open_table` time: one allocation per `open_table` call.
- `metrics` crate emission (when enabled): one additional counter lookup + increment per operation.

All negligible compared to B-tree traversal costs.

## Module Structure

- **New file:** `src/metrics.rs` — `StoreMetrics`, `MetricsSnapshot`, `TableMetricsSnapshot`, `IndexMetricsSnapshot`, emission helpers.
- **Modified:** `src/store.rs` — add `Arc<StoreMetrics>` to `Store`/`WriteTx`/`ReadTx`, add `TableReader`, instrument all methods, change `Readable` trait.
- **Modified:** `src/lib.rs` — add `mod metrics`, re-export public types, add `metrics` feature.
- **Modified:** `Cargo.toml` — add `metrics` feature and optional dependency.

## Out of Scope

- `ReadTx` scan/read counting per iterator element (we count scan initiation, not rows scanned).
- Histograms or timing (transaction duration, operation latency).
- Table memory usage estimation (separate task).
- Gauges (current snapshot count, current table count) — could be derived from the snapshot but not tracked as live counters.
