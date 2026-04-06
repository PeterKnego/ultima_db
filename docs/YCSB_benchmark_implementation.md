# YCSB Benchmark Implementation

## Overview

UltimaDB implements all six standard YCSB (Yahoo! Cloud Serving Benchmark) workloads A–F, with comparison benchmarks against Fjall, RocksDB, and ReDB. See `docs/tasks/task10_ycsb_comparison_benchmarks.md` for multi-engine architecture and design decisions.

## Files

| File | Description |
|------|-------------|
| `benches/ycsb_common.rs` | Shared types, distributions, generators, `YcsbEngine` trait |
| `benches/ycsb_bench.rs` | UltimaDB benchmark |
| `benches/ycsb_fjall_bench.rs` | Fjall benchmark |
| `benches/ycsb_rocksdb_bench.rs` | RocksDB benchmark |
| `benches/ycsb_redb_bench.rs` | ReDB benchmark |

## Configuration

| Constant | Value | Notes |
|----------|-------|-------|
| `NUM_RECORDS` | 10,000 | Preloaded records |
| `OPS_PER_ITER` | 1,000 | Operations per benchmark iteration |
| `FIELD_SIZE` | 100 bytes | Per field |
| `ZIPFIAN_CONSTANT` | 0.99 | Standard YCSB skew |

Criterion config: 50 samples, 10s measurement time.

## Record format

`YcsbRecord`: 10 fields x 100 bytes = ~1 KB per record, matching the YCSB spec. Records are deterministically generated from a seed to avoid RNG overhead in measured paths. For byte-oriented stores, records are serialized via `serde` + `bincode`.

## Distribution generators

**ZipfianGenerator (Scrambled Zipfian)** — Used by workloads A, B, C, E, F. FNV-1a hashing spreads hot keys across the keyspace (`scrambled = fnv_hash(raw_zipf) % item_count`), preventing artificial clustering at low IDs.

**LatestGenerator** — Used by workload D. Exponential distribution biased toward the maximum ID (`offset = -(max_id * 0.1) * ln(u)`), modeling "read latest" access patterns.

## Workloads

| Workload | Name | Mix | Distribution |
|----------|------|-----|-------------|
| A | Update Heavy | 50% read, 50% update | Zipfian |
| B | Read Mostly | 95% read, 5% update | Zipfian |
| C | Read Only | 100% read | Zipfian |
| D | Read Latest | 95% read, 5% insert | Latest-biased |
| E | Short Ranges | 95% scan, 5% insert | Zipfian start, range 1–100 |
| F | Read-Modify-Write | 50% read, 50% RMW | Zipfian |

## Engine trait

Each database implements the `YcsbEngine` trait:

```rust
pub trait YcsbEngine {
    fn name(&self) -> &str;
    fn execute(&mut self, ops: &[YcsbOp]);
}
```

`bench_all_workloads()` registers all 6 workloads with Criterion using `BenchmarkGroup` and `Throughput::Elements`, producing IDs like `ycsb_c_read_only/ultima` for cross-engine comparison via `critcmp`.

## UltimaDB execution model

- **Read/Scan:** Opens a read transaction, looks up key(s), uses `black_box()` to prevent optimization
- **Update/Insert:** Opens a write transaction, mutates, commits
- **ReadModifyWrite:** Read transaction to fetch, then write transaction to update `field0` and commit
- **Preload:** 10,000 records in a single write transaction with `num_snapshots_retained: 2` and `auto_snapshot_gc: true`

## Running

```bash
make bench/ycsb              # UltimaDB only
make bench/ycsb/fjall        # Fjall only
make bench/ycsb/rocksdb      # RocksDB only
make bench/ycsb/redb         # ReDB only
make bench/ycsb/compare      # All engines side-by-side (requires critcmp)

# Single workload
cargo bench --bench ycsb_bench -- ycsb_c_read_only
```

## Limitations

1. Single-threaded only (no multi-threaded contention testing)
2. No configurable parameters (compile-time constants)
3. No Workload D insert tracking (`max_id` is fixed)
4. Per-operation transactions in UltimaDB (no batching benchmarks)
5. No secondary index benchmarks
