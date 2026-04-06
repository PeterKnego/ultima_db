# Task 11: Multi-Writer OCC with Key-Level Conflict Detection

## Motivation

UltimaDB assumed single-writer by convention but didn't enforce it at runtime. Two `WriteTx` instances could exist simultaneously and commit independently, producing divergent version histories with no error. This task adds:

1. **Runtime single-writer enforcement** — `WriterMode::SingleWriter` returns `Error::WriterBusy` if another `WriteTx` is active
2. **Optimistic Concurrency Control** — `WriterMode::MultiWriter` enables concurrent write transactions with key-level write-write conflict detection

Isolation level remains **Snapshot Isolation**. Read-set tracking for SSI is out of scope.

---

## Architecture

### WriterMode configuration

```rust
pub enum WriterMode {
    SingleWriter,  // default — at most one active WriteTx
    MultiWriter,   // concurrent WriteTx with OCC validation
}
```

Added to `StoreConfig` with default `SingleWriter` — existing users see no behavior change except that concurrent `begin_write` calls now properly return an error.

### TableWriter wrapper

`WriteTx::open_table` returns `TableWriter<R>` instead of `&mut Table<R>`. This is a **breaking API change**.

```rust
pub struct TableWriter<'tx, R: Send + Sync + 'static> {
    table: &'tx mut Table<R>,
    write_set: Option<&'tx mut HashSet<u64>>,  // None in SingleWriter mode
}
```

- **Read methods** (get, scan, index queries): pass-through, no tracking
- **Write methods** (insert, update, delete, batch ops): delegate to `Table`, then record affected key IDs in `write_set` if `Some`
- In `SingleWriter` mode, `write_set` is `None` — the only overhead is an `Option` branch check

### Conflict detection

Each `WriteTx` in `MultiWriter` mode accumulates a `write_set: HashMap<String, HashSet<u64>>` (table name → set of modified key IDs).

On commit, the transaction validates against a log of committed write sets (`StoreInner::committed_write_sets`). For each committed write set with `version > self.base_version`, it checks for key-level intersection. If any overlap is found, commit returns `Error::WriteConflict { table, keys, version }`.

Table-level conflicts are also handled: if a committed transaction deleted a table that the current transaction modified (or vice versa), that's a conflict.

### Write-set log pruning

Committed write sets are retained as long as any in-flight writer has a `base_version ≤ entry.version`. When no active writers remain, the entire log is cleared. Pruning runs on every commit and every `WriteTx` drop.

### Drop guard

If a `WriteTx` is dropped without committing, it cleans up its active-writer registration and triggers write-set pruning. Uses a `committed` flag to distinguish committed vs abandoned transactions.

### Error variants

```rust
#[error("write conflict on table '{table}', keys {keys:?} (conflicting version {version})")]
WriteConflict { table: String, keys: Vec<u64>, version: u64 },

#[error("another write transaction is active (SingleWriter mode)")]
WriterBusy,
```

---

## Files Modified

| File | Changes |
|------|---------|
| `src/store.rs` | `WriterMode`, `CommittedWriteSet`, `StoreInner` fields, `TableWriter` wrapper, `begin_write` mode check, `commit` validation, `Drop` guard, write-set pruning |
| `src/error.rs` | `WriteConflict` struct variant, `WriterBusy` variant |
| `src/lib.rs` | Re-export `WriterMode`, `TableWriter` |
| `src/transaction.rs` | Re-export `TableWriter` |
| `tests/store_integration.rs` | Multi-writer integration tests |
| `examples/basic_usage.rs` | Updated for `TableWriter` return type |
| `examples/multi_store.rs` | Updated for `TableWriter` return type |

---

## YCSB Multi-Writer Benchmarks

### Motivation

The OCC implementation needs benchmarking under realistic contention patterns, and comparison with other embedded databases that support concurrent transactions (RocksDB, Fjall) to validate design decisions.

### Shared benchmark suite (`ycsb_common.rs`)

Extends the existing YCSB common module with a `MultiWriterEngine` trait and `bench_multiwriter_workloads()` function, following the same pattern as the single-threaded `YcsbEngine` trait.

```rust
pub struct BurstResult {
    pub committed: u64,
    pub conflicts: u64,
}

pub trait MultiWriterEngine {
    fn name(&self) -> &str;
    fn execute_burst(&mut self, key_sets: &[Vec<u64>]) -> BurstResult;
    fn verify_key(&self, key: u64) -> bool;
}
```

**Burst pattern**: Since `WriteTx` is not `Send` (due to `dyn Any` type erasure), benchmarks simulate concurrent writers on a single thread: open W writers, execute updates, commit in sequence, retry on conflict.

### Benchmark scenarios

| Group | Description | Parameters |
|-------|-------------|------------|
| `multiwriter_low_contention` | Zipfian key distribution, 4 writers | `MW_WRITERS=4`, `MW_OPS_PER_WRITER=50` |
| `multiwriter_high_contention` | Hot keys 1..=10, 4 writers | Same |
| `multiwriter_scaling` | Zipfian keys, vary writer count | 1, 2, 4, 8 writers |

### Correctness verification

A **smoke test** runs before timed benchmarks: executes one burst, then asserts all writers committed (including retries) and all written keys are readable via `verify_key`.

### Conflict rate reporting

Each benchmark group reports contention statistics after completion via `report_conflicts()`:
```
[ultima] high_contention: 150 bursts, 312 total conflicts, avg 2.1/burst (52.0% of commits)
```

Uses `Cell<u64>` counters to accumulate across criterion iterations without affecting timed measurements.

### Engine comparison files

| File | Engine | Transaction API |
|------|--------|-----------------|
| `benches/ycsb_multiwriter_bench.rs` | UltimaDB | `WriterMode::MultiWriter`, `begin_write`, OCC retry on `WriteConflict` |
| `benches/ycsb_multiwriter_rocksdb_bench.rs` | RocksDB | `OptimisticTransactionDB`, `transaction()`, retry on commit error |
| `benches/ycsb_multiwriter_fjall_bench.rs` | Fjall | `OptimisticTxDatabase`, `write_tx()`, retry on `Conflict` |

ReDB is excluded — single-writer only by design.

The Ultima bench file also includes two engine-specific benchmarks not in the shared suite:
- `baseline_single_writer` — `SingleWriter` mode, sequential commits (no OCC overhead baseline)
- `no_contention_diff_tables` — `MultiWriter` mode, each writer uses a different table (zero-conflict baseline)

### critcmp comparison

Benchmark IDs use a fixed function name (`burst`) rather than the engine name, so `critcmp` matches them across baselines:

```bash
make bench/multiwriter/compare
# Runs all three engines with --save-baseline mw-{engine}, then:
# critcmp mw-ultima mw-rocksdb mw-fjall
```

Baseline names use `mw-` prefix to avoid collision with YCSB single-threaded baselines.

---

## Architectural Decisions

1. **Key-level granularity over table-level**: Table-level conflict detection would reject too many valid concurrent transactions (e.g., two writers updating disjoint keys in the same table). Key-level detection using `HashSet<u64>` intersection is precise and efficient for the expected write-set sizes.

2. **Write-write only, no read-set tracking**: Implementing SSI (read-write conflict detection) would require tracking all keys read by each transaction, significantly increasing memory overhead and complexity. Snapshot Isolation with write-write detection covers the most important concurrent use cases. Write skew remains possible but is a known limitation documented in `docs/isolation-levels.md`.

3. **`TableWriter` wrapper vs modifying `Table`**: Adding write-set tracking directly to `Table` would couple it to transaction semantics. The wrapper keeps `Table` as a pure data structure and makes the tracking opt-in per writer mode.

4. **Burst-pattern benchmarks vs true multi-threading**: `WriteTx` cannot be `Send` because `Table` is stored as `Box<dyn Any>` in the dirty map. The burst pattern (open N writers, execute, commit sequentially) accurately measures OCC validation overhead and conflict handling without requiring `Send`. True multi-threaded benchmarks are deferred until `Send + Sync` bounds are added.

5. **Separate bench files per engine**: Matches the existing YCSB comparison pattern. Each engine links only its own dependencies. Can run individual engines or compare all three via Makefile.

6. **`OptimisticTransactionDB` for RocksDB, `OptimisticTxDatabase` for Fjall**: Both use OCC like UltimaDB, making the comparison fair. RocksDB also offers `TransactionDB` (pessimistic locking) but OCC is the closer analogue.
