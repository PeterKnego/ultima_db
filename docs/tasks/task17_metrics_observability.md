# Task: Metrics & Observability

## Motivation

UltimaDB had no way to observe operational activity. Users running in production need counters for commits/rollbacks/GC and per-table/per-index read and write volumes — both for debugging (why is this table slow?) and for dashboards/alerting.

Two audiences exist:
1. **Embedded users** who just want to call `store.metrics()` and read a struct, with zero external dependencies.
2. **Operators** who already run a metrics pipeline (Prometheus / OpenTelemetry / stdout exporters) via the `metrics` crate facade and want those counters emitted there as well.

Both are supported from the same instrumentation points.

---

## Goals

- Internal atomic counters, always compiled, zero runtime overhead worth measuring.
- Public snapshot API via `Store::metrics() -> MetricsSnapshot`.
- Optional `metrics` crate emission behind `feature = "metrics"` — same instrumentation points, same counters, no code duplication.
- Reads are instrumented too, not just writes. This requires `ReadTx::open_table` to return a wrapper type rather than `&Table<R>`.

## Non-goals

- Histograms / timings (transaction duration, operation latency).
- Per-row counting inside iterators (scans count *initiation*, not rows yielded).
- Gauges (current snapshot count, live table count).
- Memory usage estimation.

---

## Architecture

### Module layout

| Component | Location | Visibility |
|-----------|----------|------------|
| `StoreMetrics`, `TableMetrics`, `IndexMetrics` (counter structs) | `src/metrics.rs` | `pub(crate)` |
| `MetricsSnapshot`, `TableMetricsSnapshot`, `IndexMetricsSnapshot` | `src/metrics.rs` | `pub`, re-exported from crate root |
| `TableReader<'tx, R>` | `src/store.rs` | `pub`, re-exported via `transaction.rs` |
| `Store::metrics()` | `src/store.rs` | `pub` |

### Ownership and wiring

```
Store           owns    Arc<StoreMetrics>
WriteTx         owns    Arc<StoreMetrics>    (cloned at begin_write)
ReadTx          owns    Arc<StoreMetrics>    (cloned at begin_read)
TableWriter     owns    Arc<StoreMetrics>    (cloned at open_table) + table_name: String
TableReader     borrows &StoreMetrics        (from ReadTx) + table_name: String
```

`TableWriter` holds an `Arc` (not a reference) because it needs to live alongside mutable borrows of `WriteTx::dirty` — a shared reference back to `self.metrics` would conflict with the `&mut self.dirty` borrow the table handle requires. Cloning the `Arc` sidesteps the borrow checker without any runtime cost (interior mutability via atomics / `RwLock`). `TableReader` can use a plain `&StoreMetrics` because `ReadTx` is immutable.

### Counter storage

All counters are `AtomicU64` with `Ordering::Relaxed`. We need no cross-thread happens-before guarantees from the counters themselves — they are diagnostic, not control-plane.

Per-table and per-index maps live behind `RwLock<HashMap<String, ...>>`:

```rust
pub(crate) struct StoreMetrics {
    // Store-level atomics
    commits, rollbacks, gc_runs, snapshots_collected, write_conflicts: AtomicU64,
    // Lazily populated; keyed by table name.
    tables: RwLock<HashMap<String, TableMetrics>>,
}
```

Read lock on `tables` is taken once per metric increment — uncontended, ~10-20ns. The write lock is taken only inside `register_table` / `register_index`, i.e. once per `open_table` / `define_index` call.

### Registration

- `WriteTx::open_table` calls `metrics.register_table(name)` before constructing `TableWriter`. Idempotent.
- `ReadTx::open_table` also calls `register_table` (tables opened only for reads still get tracked).
- `define_index` calls `metrics.register_index(table, index)`.

Increment methods on unregistered tables/indexes are silent no-ops, so there is no failure mode for a race between a reader and a `register_table` call.

### `TableReader` — the breaking change

Before this task, `ReadTx::open_table` returned `&Table<R>` directly. Read metrics require an instrumentation shim, so it now returns `TableReader<'_, R>`:

```rust
pub struct TableReader<'tx, R: Record> {
    table: &'tx Table<R>,
    metrics: &'tx StoreMetrics,
    table_name: String,
}
```

`TableReader` mirrors the read surface of `TableWriter`: `get`, `get_many`, `contains`, `first`, `last`, `resolve`, `range`, `iter`, `len`, `is_empty`, `get_unique`, `get_by_index`, `get_by_key`, `index_range`, `custom_index`. Each method bumps the relevant counter and delegates.

The `Readable` trait was updated accordingly. All call sites in tests and examples were updated mechanically — the method surface is the same, only the returned type changed.

---

## Instrumentation points

### Store-level

| Counter | Where |
|---|---|
| `commits` | `WriteTx::commit`, after snapshot publish (both WAL-wait and no-wait branches) |
| `rollbacks` | `WriteTx::rollback` and `WriteTx::Drop` (only when `commit` was not called — guarded by `needs_cleanup`) |
| `gc_runs` | `Store::gc` → `gc_inner`, at entry |
| `snapshots_collected` | `gc_inner`, incremented by `before_len - after_len` |
| `write_conflicts` | `WriteTx::commit`, MultiWriter path, when OCC validation fails |

### Table-level (`TableWriter` + `TableReader`)

| Counter | Methods |
|---|---|
| `inserts` | `insert` (+1), `insert_batch` (+batch_size) on success |
| `updates` | `update` (+1), `update_batch` (+batch_size) on success |
| `deletes` | `delete` (+1), `delete_batch` (+batch_size) on success |
| `primary_key_reads` | `get`, `contains`, `first`, `last` (+1 each); `get_many`, `resolve` (+ids.len()) |
| `primary_key_scans` | `range`, `iter` (+1 each — counts initiation, not rows yielded) |

### Index-level

| Counter | Methods |
|---|---|
| `reads` | `get_unique`, `get_by_index`, `get_by_key` (+1 each) |
| `range_scans` | `index_range` (+1) |

Batch operations count by batch size on success. Failures (rollback via snapshot-restore) do **not** bump counters — increments happen after the mutation returns `Ok`.

---

## `metrics` crate integration

Behind `feature = "metrics"`. Each `StoreMetrics::inc_*` method, after bumping the atomic, calls a single `emit` helper:

```rust
#[cfg(feature = "metrics")]
#[inline]
fn emit(name: &'static str, labels: &[(&'static str, String)], val: u64) {
    metrics::counter!(name, labels).increment(val);
}
```

When the feature is off, `emit` and all its call sites compile away entirely.

### Metric names

```
ultima.commits                                           (counter)
ultima.rollbacks                                         (counter)
ultima.gc_runs                                           (counter)
ultima.snapshots_collected                               (counter)
ultima.write_conflicts                                   (counter)

ultima.table.inserts          table=<name>               (counter)
ultima.table.updates          table=<name>               (counter)
ultima.table.deletes          table=<name>               (counter)
ultima.table.primary_reads    table=<name>               (counter)
ultima.table.primary_scans    table=<name>               (counter)

ultima.index.reads            table=<name>, index=<name> (counter)
ultima.index.range_scans      table=<name>, index=<name> (counter)
```

### Cargo.toml

```toml
[features]
metrics = ["dep:metrics"]

[dependencies]
metrics = { version = "0.24", optional = true }
```

---

## Design decisions

### 1. `Arc<StoreMetrics>` in `TableWriter`, not `&StoreMetrics`

The obvious design is `&'tx StoreMetrics` borrowed from `WriteTx`. It does not work: `TableWriter` also borrows `&'tx mut` to `dirty` (for the table) and `write_set`. Borrowing `self.metrics` as `&` alongside `&mut self.dirty` fails the borrow checker because `WriteTx` as a whole is mutably borrowed. Cloning the `Arc<StoreMetrics>` into `TableWriter` sidesteps this at zero runtime cost — atomics give us interior mutability, so shared ownership is fine.

`TableReader` can use `&StoreMetrics` because `ReadTx` is not mutably borrowed anywhere during `open_table`.

### 2. `Relaxed` ordering on all atomics

Counters are diagnostic. No correctness property of the store depends on happens-before ordering between `metrics.inc_commit()` and any other memory access. `Relaxed` is cheapest and sufficient.

### 3. Silent no-op on unregistered table/index

`inc_inserts("nonexistent", 1)` does nothing rather than panicking or auto-registering. This means the lookup is cheap (a `RwLock::read` + `HashMap::get`), and the registration path (write lock) is taken only at `open_table` / `define_index`. Auto-registration on every increment would require a write lock on a hot path.

### 4. `String` allocation for `table_name` in `TableWriter` / `TableReader`

Each `open_table` call allocates one `String` for `table_name`. Alternative: store `&'tx str` borrowed from the snapshot's key. Rejected because it complicates lifetimes for marginal gain — one allocation per `open_table` is immeasurable against the B-tree downcast and lookup.

### 5. Reads counted at initiation of iterators, not per row

`range()` and `iter()` return lazy iterators. Counting rows yielded would require a wrapper iterator type. We count the *scan* itself — the metric is meaningful as "how many times did someone open a scan" rather than "how many rows were read." A row-level counter is out of scope.

### 6. Breaking `ReadTx::open_table` return type

Returning `&Table<R>` directly was the previous API. We changed it to `TableReader<'_, R>` rather than adding a new method (e.g. `open_table_reader`) because:
- A second method would bifurcate the `Readable` trait.
- `TableReader` exposes exactly the same read methods as `&Table<R>`, so almost all call sites are source-compatible.
- Only code that explicitly typed the return as `&Table<R>` breaks; updating is mechanical.

---

## Performance

| Operation | Cost |
|---|---|
| Atomic `fetch_add` (Relaxed) | ~1ns |
| `RwLock::read` on `tables` map (uncontended) | ~10-20ns |
| `String` clone for `table_name` (open_table only) | one allocation |
| `emit` when `feature = "metrics"` off | zero (compiles away) |
| `emit` when on | one counter lookup + increment in the `metrics` crate |

All negligible against B-tree traversal (hundreds of ns to low μs per operation).

---

## Files touched

- **Created:** `src/metrics.rs`
- **Modified:** `src/store.rs` — wiring, `TableReader`, instrumentation of `commit` / `rollback` / `gc` / `TableWriter` / `TableReader`.
- **Modified:** `src/lib.rs` — `pub mod metrics`, re-exports.
- **Modified:** `src/transaction.rs` — re-export `TableReader`.
- **Modified:** `Cargo.toml` — `metrics` feature + optional dependency.
- **Modified:** `examples/*.rs`, `tests/*.rs` — updated call sites for `TableReader` return type.

## Implementation sequence

Five commits landed in order:

1. `feat: add StoreMetrics counters and snapshot types`
2. `feat: wire metrics into Store/WriteTx/ReadTx and instrument store-level counters`
3. `feat: add TableReader, update Readable trait, fix all call sites`
4. `feat: instrument TableWriter write/read/index methods for metrics`
5. `feat: add metrics crate dependency, end-to-end test, cleanup`
