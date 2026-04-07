# Task 14: SmallBank Multi-Table Transactional Benchmark

## Motivation

Existing YCSB benchmarks test single-table key-value workloads. They don't stress multi-table transactions, secondary index lookups, cross-table mutations, or multi-writer contention — all core to UltimaDB's value proposition. SmallBank (Alomari et al., ICDE 2008) is a standard benchmark designed specifically for studying transactional contention and serialization conflicts.

---

## Architecture

### Data model

Three tables model a banking application, each with a unique `customer_id` index:

| Table      | Fields                    |
|------------|---------------------------|
| `Account`  | `customer_id: u64`, `name: String` |
| `Savings`  | `customer_id: u64`, `balance: f64` |
| `Checking` | `customer_id: u64`, `balance: f64` |

Preloaded with 10,000 accounts (`INITIAL_SAVINGS = 10,000`, `INITIAL_CHECKING = 10,000`).

### Transaction types

Six operations matching the SmallBank paper:

| Op                | Tables touched       | Read/Write | Description |
|-------------------|---------------------|------------|-------------|
| `Balance`         | Savings, Checking   | Read-only  | Sum balances for a customer |
| `DepositChecking` | Checking            | Read-Write | Add to checking balance |
| `TransactSavings` | Savings             | Read-Write | Add/subtract from savings |
| `Amalgamate`      | Savings, Checking   | Read-Write | Move all savings to dest's checking |
| `WriteCheck`      | Savings, Checking   | Read-Write | Deduct from checking with overdraft penalty |
| `SendPayment`     | Checking            | Read-Write | Transfer between two customers |

### Workload distributions

Three workload mixes, each running 500 ops per iteration:

- **Mixed** (paper ratios): 15% Balance, 15% DepositChecking, 15% TransactSavings, 15% Amalgamate, 25% WriteCheck, 15% SendPayment
- **Read-heavy**: 80% Balance, 10% DepositChecking, 10% WriteCheck
- **Write-heavy**: 30% Amalgamate, 30% SendPayment, 20% DepositChecking, 20% TransactSavings

Access pattern uses Zipfian distribution (theta=0.99) for realistic skew — a small number of "hot" accounts receive disproportionate traffic.

### Multi-writer contention

SmallBank's original purpose is studying contention. The benchmark includes a burst pattern that opens 4 concurrent `WriteTx` in `MultiWriter` mode, executes 50 ops each, then commits sequentially with conflict retry:

- **Low contention**: Zipfian across full 10,000-account keyspace
- **High contention**: Zipfian across only 10 hot accounts (maximizes conflicts)

The burst pattern is an artifact of single-threaded execution (`WriteTx` is not `Send`) — all 4 writers fully overlap, producing ~75% abort rate at high contention. Abort rate reporting was removed since it's a structural constant of the burst pattern, not a meaningful metric.

### Parameterizable configs

`SmallBankConfig` controls the store configuration. Three presets:

| Config                   | Feature gate   | Persistence mode | Durability |
|--------------------------|---------------|------------------|------------|
| `inmemory`               | always         | None             | N/A        |
| `standalone_consistent`  | `persistence`  | Standalone       | Consistent (sync fsync) |
| `standalone_eventual`    | `persistence`  | Standalone       | Eventual (async fsync) |

SMR was removed — without checkpoint calls during the benchmark it takes the identical code path as inmemory, producing no useful comparison.

Each config runs all three workloads plus both contention scenarios.

### In-flight WAL tracking

For Eventual durability, the benchmark samples `Store::pending_wal_writes()` after each operation and reports the average in-flight WAL writes per workload. This uses the `in_flight: AtomicU64` counter in `WalHandle` (incremented on channel send, decremented after background fsync).

---

## Implementation

### Files

| File | Description |
|------|-------------|
| `benches/smallbank_bench.rs` | Complete benchmark (~970 lines) |
| `Cargo.toml` | `[[bench]]` entry for `smallbank_bench` |
| `Makefile` | `bench/smallbank` and `bench/smallbank/persistent` targets |

### Supporting changes

- **`src/wal.rs`**: Added `in_flight: Arc<AtomicU64>` to `WalHandle`, `pending_writes()` method. Made `WalOp`, `WalEntry`, `read_wal` public for integration tests. `WalHandle` implements `Drop` to flush pending writes and join the background thread on store shutdown. Eventual mode background thread batches queued entries via `try_recv` before fsyncing.
- **`src/store.rs`**: Added `wal_enabled: bool` to `WriteTx` to avoid persistence overhead on non-persistent stores. Moved serialization inside the `if let Some(w) = &mut self.wal_ops` guard with early-return pattern. Added `pending_wal_writes()` public method on `Store`.

### Running

```bash
# In-memory only
make bench/smallbank

# All configs (in-memory + persistent)
make bench/smallbank/persistent
```

Both targets use `critcmp` to display grouped comparison tables.

---

## Criterion configuration

- Sample size: 30
- Measurement time: 15 seconds
- Throughput: reported as elements (ops) per second
- Baseline name: `smallbank` (for `critcmp` grouping)
