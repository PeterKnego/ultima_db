# Task 10: YCSB Comparison Benchmarks

## Motivation

Task 8 established YCSB A–F benchmarks for UltimaDB (see `docs/YCSB_benchmark_implementation.md`), but performance numbers in isolation lack context. This task runs the same workloads against Fjall, RocksDB, and ReDB to understand where UltimaDB excels and where it trades off.

---

## Architecture

### Shared workload module (`benches/ycsb_common.rs`)

All database-agnostic code lives here: `YcsbRecord`, distributions, `YcsbOp`, workload generators, and the `YcsbEngine` trait. Each database implements `YcsbEngine` in its own bench file and calls `bench_all_workloads()` to register all 6 workloads.

**`YcsbEngine` trait:**
```rust
pub trait YcsbEngine {
    fn name(&self) -> &str;           // used as Criterion benchmark ID suffix
    fn execute(&mut self, ops: &[YcsbOp]);
}
```

**Criterion grouping:** Benchmarks use `BenchmarkGroup` with IDs of the form `ycsb_{workload}/{engine}` (e.g. `ycsb_c_read_only/rocksdb`). This lets `critcmp -g '(.+)/[^/]+'` group by workload and compare engines side-by-side. Each group sets `Throughput::Elements(OPS_PER_ITER)` so critcmp reports ops/sec.

### Serialization

Fjall, RocksDB, and ReDB store raw bytes, so `YcsbRecord` uses `serde::Serialize` + `serde::Deserialize` with `bincode` for encoding. The serialization overhead is intentionally included — any byte-oriented store pays this cost in practice. UltimaDB stores native Rust structs and avoids this overhead, which is a genuine architectural advantage.

### Key encoding

Keys are `u64.to_be_bytes()` (big-endian 8 bytes). Big-endian preserves lexicographic ordering, which is critical for correct range scan behavior in all byte-oriented stores.

---

## Database engines

### Fjall (`benches/ycsb_fjall_bench.rs`)

- **Storage:** Disk-based LSM-tree via `tempfile::tempdir()`
- **API:** Direct `keyspace.insert/get/range` (no explicit transactions). Per-operation durability matches UltimaDB's per-operation transaction pattern.
- **Drop ordering:** `FjallEngine` field order is `keyspace → _db → _tmpdir` to ensure correct drop sequence.

### RocksDB (`benches/ycsb_rocksdb_bench.rs`)

- **Storage:** Disk-based via `tempfile::tempdir()`, configured to minimize I/O:
  - 256 MB write buffer (keeps data in memtables)
  - Auto-compaction disabled
  - WAL disabled (`WriteOptions::disable_wal(true)`) for both preload and benchmark writes
- **In-memory mode not used:** RocksDB's `Env::mem_env()` fails with `"Not implemented: GetAbsolutePath"`. The tmpdir + WAL-disabled + large-write-buffer config achieves near-in-memory performance.
- **Crate:** `rocksdb = "0.24.0"`

### ReDB (`benches/ycsb_redb_bench.rs`)

- **Storage:** True in-memory via `InMemoryBackend::new()` — no disk I/O at all.
- **Durability:** `Durability::None` on write transactions to skip fsync.
- **Transaction strategy:** Write-containing batches use a single write transaction for the entire `execute()` call. Read-only batches (workload C) use a read transaction. This is fairer than per-op transactions since redb transactions have significant overhead.
- **Borrow workaround:** `ReadModifyWrite` decodes the value in a nested scope to drop the `AccessGuard` before calling `table.insert()`, avoiding a borrow conflict.
- **Crate:** `redb = "4.0.0"`

---

## Files

| File | Description |
|------|-------------|
| `benches/ycsb_common.rs` | Shared `YcsbEngine` trait, workload types, distributions, generators |
| `benches/ycsb_bench.rs` | UltimaDB YCSB benchmark |
| `benches/ycsb_fjall_bench.rs` | Fjall YCSB benchmark |
| `benches/ycsb_rocksdb_bench.rs` | RocksDB YCSB benchmark |
| `benches/ycsb_redb_bench.rs` | ReDB YCSB benchmark (in-memory backend) |
| `Cargo.toml` | Dev-deps: `fjall`, `rocksdb`, `redb`, `serde`, `bincode`, `tempfile` |

---

## Running

```bash
# Individual engines
make bench/ycsb              # UltimaDB only
make bench/ycsb/fjall        # Fjall only
make bench/ycsb/rocksdb      # RocksDB only
make bench/ycsb/redb         # ReDB only

# Compare all engines side-by-side (requires critcmp)
make bench/ycsb/compare
```

The `bench/ycsb/compare` target saves each engine as a separate Criterion baseline, then runs:
```bash
critcmp -g '(.+)/[^/]+' ultima fjall rocksdb redb
```

---

## Fairness notes

- All benchmarks use identical RNG seeds (42–47) producing the same operation sequences
- All use the same Criterion config: 50 samples, 10s measurement time
- UltimaDB stores native structs; byte-oriented stores (Fjall, RocksDB, ReDB) pay bincode serialization cost — this reflects real-world usage differences
- UltimaDB and ReDB run fully in-memory; RocksDB uses tmpdir with WAL disabled; Fjall uses tmpdir with default durability — these are fundamental architectural differences being measured
- Fjall's higher variance is expected due to background compaction and disk I/O
