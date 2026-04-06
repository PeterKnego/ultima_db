# Task 8: YCSB Benchmarks

## Motivation

Task 6 established the benchmark harness (Criterion + pprof + critcmp) but only included a minimal `Store::new()` benchmark. To understand UltimaDB's performance characteristics under realistic workloads, we need benchmarks that model actual database access patterns — not just isolated micro-operations.

YCSB (Yahoo! Cloud Serving Benchmark) is the industry-standard benchmark for key-value and document stores. It defines six workloads (A–F) that cover the spectrum of read/write ratios, access distributions, and operation types that real applications produce. Implementing YCSB against UltimaDB exercises the full stack: B-tree lookups, CoW path copies on mutation, transaction begin/commit cycles, and snapshot GC.

---

## Design decisions

### 1. YCSB workload selection — all six core workloads

**Alternatives considered:**

| Approach | Pros | Cons |
|----------|------|------|
| **A. All six YCSB workloads (A–F)** | Complete coverage of access patterns; industry-comparable results; exercises reads, writes, scans, and read-modify-write | More code; longer benchmark suite runtime |
| B. Subset (A, B, C only) | Simpler; covers the most common read/write ratios | Misses scan performance (E), insert-heavy patterns (D), and read-modify-write (F) which stress CoW differently |
| C. Custom workloads only | Tailored to UltimaDB's specific API | Not comparable to other systems; easy to accidentally miss important access patterns |

**Chosen: A.** UltimaDB's CoW B-tree behaves differently under each workload: read-only (C) measures pure lookup speed with zero copying; update-heavy (A) stresses path-copy cost; scan (E) tests range iteration; read-modify-write (F) exercises the read-then-write transaction pattern. Omitting any workload leaves a blind spot.

### 2. Scrambled Zipfian distribution (not uniform or pure Zipfian)

**Alternatives considered:**

| Approach | Pros | Cons |
|----------|------|------|
| **A. Scrambled Zipfian** | Realistic skew (80/20 rule); hot keys spread across the keyspace via FNV-1a hash; matches YCSB reference implementation | Custom implementation needed |
| B. Pure Zipfian | Simpler; still models skewed access | Hot keys cluster at the start of the keyspace, making B-tree node access patterns unrealistically localized |
| C. Uniform random | Simplest | No skew — most real workloads are heavily skewed; would not stress hot-path caching or CoW sharing |

**Chosen: A.** Pure Zipfian concentrates hot keys at low IDs, which means the leftmost B-tree leaf handles most traffic — this flatters tree structures with good locality but doesn't represent real workloads where popular keys are scattered. Scrambling via FNV-1a (`fnv_hash(raw_zipf) % item_count`) distributes hot keys across the full keyspace while preserving the frequency distribution (θ = 0.99, matching the YCSB default).

### 3. Latest-biased generator for Workload D (not Zipfian)

Workload D models "read latest" — newly inserted records are read most frequently. A Zipfian generator doesn't capture this temporal bias. The `LatestGenerator` uses an exponential distribution biased toward `max_id`: `offset = -(max_id * 0.1) * ln(u)`, where `u` is uniform random. This naturally favors recently inserted records while still occasionally reading older ones.

### 4. Record layout — 10 fields × 100 bytes ≈ 1 KB per record

The YCSB specification defines a default record as 10 fields of 100 bytes each. We match this exactly with `YcsbRecord` containing `field0..field9`, each a 100-char `String`. This ensures memory pressure and allocation patterns are representative. The record is generated deterministically from a seed to avoid RNG overhead in the measured path.

### 5. Operation batching — 1,000 ops per iteration

Each Criterion iteration executes 1,000 operations rather than one. This amortizes the per-iteration overhead (Criterion's timing machinery, `iter_batched_ref` setup/teardown) across enough operations that the measured time reflects actual database throughput. The operations are pre-generated in the setup closure (outside the measured region) so RNG and allocation costs don't pollute timing.

### 6. Separate benchmark binary (not merged into store_bench)

The YCSB benchmarks live in `benches/ycsb_bench.rs` as a separate `[[bench]]` target. This keeps the existing `store_bench.rs` focused on micro-benchmarks while YCSB represents macro/workload-level benchmarks. It also allows running them independently via `cargo bench --bench ycsb_bench` or `make bench/ycsb`.

### 7. Aggressive snapshot GC in preload

The preloaded store uses `StoreConfig { num_snapshots_retained: 2, auto_snapshot_gc: true }`. During benchmarking, each write transaction commits and triggers GC. Retaining only 2 snapshots keeps memory stable across long benchmark runs (50 samples × 1,000 ops = 50,000 write transactions in update-heavy workloads) and reflects a realistic production configuration.

---

## Architecture

### Workload specifications

| Workload | Name | Mix | Distribution | Exercises |
|----------|------|-----|--------------|-----------|
| A | Update Heavy | 50% read, 50% update | Zipfian | CoW path copy under write pressure |
| B | Read Mostly | 95% read, 5% update | Zipfian | Read-dominated with occasional CoW |
| C | Read Only | 100% read | Zipfian | Pure B-tree lookup; zero allocation |
| D | Read Latest | 95% read, 5% insert | Latest-biased | Temporal locality; growing table |
| E | Short Ranges | 95% scan, 5% insert | Zipfian | B-tree range iteration (1–100 keys) |
| F | Read-Modify-Write | 50% read, 50% RMW | Zipfian | Full transaction cycle: read → modify → write |

### Component structure

```
benches/ycsb_bench.rs
    │
    ├── YcsbRecord          — 10×100-byte fields, deterministic from seed
    ├── ZipfianGenerator    — Scrambled Zipfian (θ=0.99, FNV-1a scramble)
    ├── LatestGenerator     — Exponential bias toward max_id (Workload D)
    ├── YcsbOp enum         — Read | Update | Insert | Scan | ReadModifyWrite
    │
    ├── preload_store()     — Creates Store, inserts 10,000 records in one tx
    ├── execute_ops()       — Dispatches ops against Store (measured region)
    │
    ├── gen_workload_{a..f} — Generate 1,000 ops with correct mix + distribution
    └── bench_workload_{a..f} — Criterion bench functions using iter_batched_ref
```

### Data flow per benchmark iteration

```
[Setup: gen_workload_X]         [Measured: execute_ops]
         │                              │
    RNG + Zipfian/Latest ──►  Vec<YcsbOp>  ──►  match on each op
                                                    │
                                        Read:   begin_read → open_table → get → black_box
                                        Update: begin_write → open_table → update → commit
                                        Insert: begin_write → open_table → insert → commit
                                        Scan:   begin_read → open_table → range → iterate
                                        RMW:    begin_read → get ; begin_write → update → commit
```

Key: operation generation (RNG, allocation) happens in the setup closure and is excluded from timing. Only `execute_ops` is measured.

### Criterion configuration

```rust
criterion_group! {
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
        .sample_size(50)
        .measurement_time(Duration::from_secs(10));
    targets = bench_workload_a..f
}
```

- **sample_size(50)**: Sufficient for statistical confidence without excessive runtime (6 workloads × ~10s each ≈ 1 min).
- **measurement_time(10s)**: Allows Criterion to run enough iterations per sample for stable measurements given the 1,000-ops-per-iteration batch size.
- **PProfProfiler(100 Hz)**: Same as store_bench — activated only with `--profile-time`.

---

## Files changed

| File | Change |
|------|--------|
| `Cargo.toml` | Added `[[bench]] name = "ycsb_bench" harness = false` |
| `benches/ycsb_bench.rs` | New file: YCSB workloads A–F with Zipfian/Latest generators |
| `Makefile` | Added `bench/ycsb` target and updated `.PHONY` |

---

## Usage

```bash
# Run all YCSB workloads
make bench/ycsb

# Run a specific workload
cargo bench --bench ycsb_bench -- ycsb_c_read_only

# Save baseline and compare across changes
make bench/save NAME=before
# ... make changes ...
make bench/save NAME=after
make bench/compare BASE=before NEW=after

# Generate flamegraphs for YCSB workloads
cargo bench --bench ycsb_bench -- --profile-time 5
open target/criterion/ycsb_a_update_heavy/profile/flamegraph.svg
```

---

## Limitations and future work

- **Single-threaded only.** All workloads run on one thread. YCSB normally supports configurable thread counts to measure contention. UltimaDB's current single-writer design makes multi-threaded write benchmarks premature, but multi-reader benchmarks would be valuable.
- **No configurable parameters.** Record count (10K), ops per iteration (1K), field size (100B), and Zipfian skew (0.99) are compile-time constants. A future iteration could accept these via environment variables for sweep testing.
- **No Workload D insert tracking.** Workload D inserts new records but `LatestGenerator.max_id` is fixed at preload time. In a full YCSB implementation, the generator would track the growing max ID so reads shift toward newer records as the benchmark progresses.
- **Per-operation transactions.** Each update/insert opens and commits its own transaction. Batching multiple writes per transaction would be a separate benchmark measuring amortized commit cost.
- **No secondary index benchmarks.** The YCSB table has no indexes defined. Adding indexed lookups (by a field value rather than primary key) would exercise the index maintenance path during writes.