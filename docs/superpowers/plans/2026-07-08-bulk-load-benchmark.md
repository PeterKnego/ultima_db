# Bulk-load Benchmark vs Competitors — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a timed bulk-load (ingest-into-empty-db) benchmark comparing UltimaDB's `insert_batch` and `Store::bulk_load` paths against RocksDB, Fjall, and ReDB, in both durability tiers.

**Architecture:** A shared, engine-agnostic criterion helper (`bench_bulk_load`) in `bench_workloads` times a `load(&mut engine, N)` closure while keeping db-open and teardown untimed. Each engine/arm is its own bench binary emitting identical IDs `bulk_load/{N}`, so `critcmp` aligns them across per-engine saved baselines — exactly like the existing YCSB comparison. A Makefile target runs all five binaries per tier and prints a `critcmp` table.

**Tech Stack:** Rust, criterion 0.8, `ultima-bench-workloads` (shared generators, already a dep with `persistence` enabled), `compare-benches` crate (rocksdb 0.24 / fjall 3 / redb 4), `critcmp`.

## Global Constraints

- Copyright header on every new file: `// SPDX-License-Identifier: Apache-2.0` then `// Copyright 2026 Peter Knego`.
- Record type: reuse `ultima_bench_workloads::ycsb::YcsbRecord` (≈1 KB, `Clone + Serialize + Deserialize`); build with `YcsbRecord::new(seed)`.
- Keys for competitors: big-endian `u64`, `id.to_be_bytes()`; values: `bincode::serde::encode_to_vec(rec, bincode::config::standard())`.
- Sizes: `BULK_SIZES = [10_000, 100_000, 1_000_000]`.
- Durability tier from `bench_durability()` (env `ULTIMA_BENCH_DURABILITY`): `NonDurable` (no end-of-load fsync) / `Strict` (exactly one fsync at end of load).
- Disk dir from `bench_disk_dir()` (env `ULTIMA_BENCH_DIR`; real disk guard for the Makefile target).
- Fairness invariant: each engine ingests keys `1..=N` into an empty db via its idiomatic single-batch path; exactly one durability sync at the end in Strict, none in NonDurable. No per-record fsync.
- All bench binaries use `harness = false` and `criterion_main!`.
- UltimaDB benches need NO `required-features` line (persistence is unified in via the `ultima-bench-workloads` dev-dependency, matching `ycsb_bench`).

---

### Task 1: Shared bulk-load harness in `bench_workloads`

**Files:**
- Create: `bench_workloads/src/bulk_load.rs`
- Modify: `bench_workloads/src/lib.rs` (add `pub mod bulk_load;`)

**Interfaces:**
- Consumes: `criterion::{Criterion, BenchmarkId, Throughput, BatchSize}`, `std::time::Duration`; `crate::ycsb::{bench_disk_dir, bench_durability, BenchDurability}` (existing).
- Produces:
  - `pub const BULK_SIZES: &[u64]`
  - `pub fn bulk_load_criterion() -> Criterion`
  - `pub fn bench_bulk_load<E>(c: &mut Criterion, make_empty: impl FnMut(u64) -> E, load: impl FnMut(&mut E, u64))`
  - `pub struct UltimaBulkStore { pub store: ultima_db::Store, pub _tmpdir: tempfile::TempDir }`
  - `pub fn ultima_bulk_store() -> UltimaBulkStore` (fresh empty standalone store, table `"ycsb"` registered, durability per tier)

- [ ] **Step 1: Write the module**

Create `bench_workloads/src/bulk_load.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared, engine-agnostic harness for the bulk-load comparison benchmark.
//!
//! Times ingesting N records into a **fresh, empty** database: db-open is
//! untimed criterion setup, teardown (flush + drop + rm tempdir) is untimed,
//! only the ingest + one end-of-load durability sync (Strict tier) is timed.
//! Every engine/arm binary emits identical IDs `bulk_load/{N}` so `critcmp`
//! aligns them across per-engine saved baselines.

use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};

use crate::ycsb::{BenchDurability, bench_disk_dir, bench_durability};

/// Record counts to load. Sequential auto-increment ids (UltimaDB's best case
/// for `insert_mut`; all engines load the same ascending keyspace).
pub const BULK_SIZES: &[u64] = &[10_000, 100_000, 1_000_000];

/// Criterion config tuned for a small number of expensive per-iteration loads.
pub fn bulk_load_criterion() -> Criterion {
    Criterion::default()
        .sample_size(10) // criterion minimum; each sample builds a whole db
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
}

/// Register one criterion bench per size under group `bulk_load`.
///
/// `make_empty(N)` opens a fresh empty db (untimed). `load(&mut e, N)` ingests
/// records `1..=N` and performs the single end-of-load durability sync in the
/// Strict tier (timed). The loaded db is returned from the routine so its drop
/// happens outside the timing window.
pub fn bench_bulk_load<E>(
    c: &mut Criterion,
    mut make_empty: impl FnMut(u64) -> E,
    mut load: impl FnMut(&mut E, u64),
) {
    let mut group = c.benchmark_group("bulk_load");
    for &n in BULK_SIZES {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || make_empty(n),
                |mut e| {
                    load(&mut e, n);
                    e
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

/// A fresh empty UltimaDB store held with its temp dir (drop order: store
/// first so the WAL bg thread joins before the dir is removed).
pub struct UltimaBulkStore {
    pub store: Store,
    pub _tmpdir: TempDir,
}

/// Build a fresh empty standalone UltimaDB store with the `"ycsb"` table type
/// registered and durability matching the current tier — mirrors the store
/// config in `benches/ycsb_bench.rs` (Coalesced WAL; Eventual/Consistent).
pub fn ultima_bulk_store() -> UltimaBulkStore {
    use crate::ycsb::YcsbRecord;
    let tmpdir = tempfile::tempdir_in(bench_disk_dir()).expect("failed to create temp dir");
    let durability = match bench_durability() {
        BenchDurability::NonDurable => Durability::Eventual,
        BenchDurability::Strict => Durability::Consistent,
    };
    let store = Store::new(
        StoreConfig::builder()
            .num_snapshots_retained(2)
            .auto_snapshot_gc(true)
            .persistence(Persistence::standalone(
                tmpdir.path().to_path_buf(),
                durability,
                WalWrite::Coalesced,
            ))
            .build(),
    )
    .expect("failed to build store");
    store
        .register_table::<YcsbRecord>("ycsb")
        .expect("register_table failed");
    UltimaBulkStore {
        store,
        _tmpdir: tmpdir,
    }
}
```

- [ ] **Step 2: Wire the module into the crate**

Modify `bench_workloads/src/lib.rs` — add after the existing `pub mod ycsb;` line:

```rust
pub mod bulk_load;
```

- [ ] **Step 3: Compile-check the crate**

Run: `cargo build -p ultima-bench-workloads`
Expected: `Finished` with no errors. (Confirms `bench_bulk_load`, `ultima_bulk_store`, and the `crate::ycsb::YcsbRecord` path resolve.)

- [ ] **Step 4: Commit**

```bash
git add bench_workloads/src/bulk_load.rs bench_workloads/src/lib.rs
git commit -m "bench(bulk-load): shared engine-agnostic bulk-load harness"
```

---

### Task 2: UltimaDB `insert_batch` bench binary

**Files:**
- Create: `benches/ycsb_bulk_load_ultima_batch_bench.rs`
- Modify: `Cargo.toml` (add `[[bench]]`)

**Interfaces:**
- Consumes: `ultima_bench_workloads::bulk_load::{bench_bulk_load, bulk_load_criterion, ultima_bulk_store, UltimaBulkStore}`, `ultima_bench_workloads::ycsb::YcsbRecord`.
- Produces: bench binary `ycsb_bulk_load_ultima_batch_bench`, IDs `bulk_load/{N}`.

- [ ] **Step 1: Write the bench**

Create `benches/ycsb_bulk_load_ultima_batch_bench.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bulk-load arm: UltimaDB `Table::insert_batch` in one write transaction.
//! This is the path `insert_mut` (task48) accelerates — many inserts sharing a
//! single transaction, reusing the privatized spine. Strict tier: the commit
//! is `Durability::Consistent` (WAL coalesced + one fsync). See
//! docs/tasks/task49_bulk_load_bench.md.

use criterion::{Criterion, criterion_group, criterion_main};
use ultima_bench_workloads::bulk_load::*;
use ultima_bench_workloads::ycsb::YcsbRecord;

fn bench(c: &mut Criterion) {
    bench_bulk_load(
        c,
        |_n| ultima_bulk_store(),
        |e: &mut UltimaBulkStore, n| {
            let records: Vec<YcsbRecord> = (1..=n).map(YcsbRecord::new).collect();
            let mut wtx = e.store.begin_write(None).unwrap();
            {
                let mut t = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                t.insert_batch(records).unwrap();
            }
            wtx.commit().unwrap();
        },
    );
}

criterion_group! { name = benches; config = bulk_load_criterion(); targets = bench }
criterion_main!(benches);
```

- [ ] **Step 2: Register the bench**

Modify `Cargo.toml` — add after the existing `table_insert_e2e_bench` `[[bench]]` block:

```toml
[[bench]]
name = "ycsb_bulk_load_ultima_batch_bench"
harness = false
```

- [ ] **Step 3: Smoke-run and verify results emit for all sizes**

Run: `cargo bench --bench ycsb_bulk_load_ultima_batch_bench -- --quick`
Expected: three benchmark lines `bulk_load/10000`, `bulk_load/100000`, `bulk_load/1000000`, each with a `time:` result, no panics.

- [ ] **Step 4: Commit**

```bash
git add benches/ycsb_bulk_load_ultima_batch_bench.rs Cargo.toml
git commit -m "bench(bulk-load): UltimaDB insert_batch arm"
```

---

### Task 3: UltimaDB `Store::bulk_load` bench binary

**Files:**
- Create: `benches/ycsb_bulk_load_ultima_sorted_bench.rs`
- Modify: `Cargo.toml` (add `[[bench]]`)

**Interfaces:**
- Consumes: same harness as Task 2, plus `ultima_db::bulk_load::{BulkLoadInput, BulkSource, BulkLoadOptions}`.
- Produces: bench binary `ycsb_bulk_load_ultima_sorted_bench`, IDs `bulk_load/{N}`.

- [ ] **Step 1: Write the bench**

Create `benches/ycsb_bulk_load_ultima_sorted_bench.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bulk-load arm: UltimaDB `Store::bulk_load` (BTree::from_sorted, O(N)) — the
//! specialized restore path and the ceiling this benchmark measures. Strict
//! tier: `checkpoint_after: true` writes a durable checkpoint (one fsync). See
//! docs/tasks/task49_bulk_load_bench.md.

use criterion::{Criterion, criterion_group, criterion_main};
use ultima_bench_workloads::bulk_load::*;
use ultima_bench_workloads::ycsb::YcsbRecord;
use ultima_db::bulk_load::{BulkLoadInput, BulkLoadOptions, BulkSource};

fn bench(c: &mut Criterion) {
    bench_bulk_load(
        c,
        |_n| ultima_bulk_store(),
        |e: &mut UltimaBulkStore, n| {
            let records: Vec<YcsbRecord> = (1..=n).map(YcsbRecord::new).collect();
            let checkpoint_after =
                bench_durability_is_strict();
            e.store
                .bulk_load::<YcsbRecord>(
                    "ycsb",
                    BulkLoadInput::Replace(BulkSource::auto_id_vec(records)),
                    BulkLoadOptions {
                        create_if_missing: true,
                        checkpoint_after,
                    },
                )
                .unwrap();
        },
    );
}

criterion_group! { name = benches; config = bulk_load_criterion(); targets = bench }
criterion_main!(benches);
```

- [ ] **Step 2: Add the `bench_durability_is_strict` helper to the harness**

The bench above needs a tier boolean. Modify `bench_workloads/src/bulk_load.rs` — add this public fn at the end of the file:

```rust
/// True iff the current tier is Strict (one end-of-load fsync).
pub fn bench_durability_is_strict() -> bool {
    matches!(bench_durability(), BenchDurability::Strict)
}
```

(Confirm `bench_durability` and `BenchDurability` are already imported at the top of the module — they are, from Task 1's `use crate::ycsb::{...}`.)

- [ ] **Step 3: Register the bench**

Modify `Cargo.toml` — add after Task 2's block:

```toml
[[bench]]
name = "ycsb_bulk_load_ultima_sorted_bench"
harness = false
```

- [ ] **Step 4: Smoke-run and verify**

Run: `cargo bench --bench ycsb_bulk_load_ultima_sorted_bench -- --quick`
Expected: `bulk_load/10000`, `bulk_load/100000`, `bulk_load/1000000` each with a `time:` result; no panics. Sanity: these times should be ≤ the Task 2 `insert_batch` times (from_sorted is the ceiling).

- [ ] **Step 5: Commit**

```bash
git add benches/ycsb_bulk_load_ultima_sorted_bench.rs bench_workloads/src/bulk_load.rs Cargo.toml
git commit -m "bench(bulk-load): UltimaDB Store::bulk_load (from_sorted) arm"
```

---

### Task 4: RocksDB bench binary

**Files:**
- Create: `compare_benches/benches/ycsb_bulk_load_rocksdb_bench.rs`
- Modify: `compare_benches/Cargo.toml` (add `[[bench]]`)

**Interfaces:**
- Consumes: harness `bench_bulk_load`, `bulk_load_criterion`; `ycsb::{YcsbRecord, bench_disk_dir, bench_durability, BenchDurability}`; `rocksdb::{DB, Options, WriteBatch, WriteOptions}`.
- Produces: bench binary `ycsb_bulk_load_rocksdb_bench`, IDs `bulk_load/{N}`.

- [ ] **Step 1: Write the bench**

Create `compare_benches/benches/ycsb_bulk_load_rocksdb_bench.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bulk-load arm: RocksDB. Idiomatic single-batch ingest — one `WriteBatch` of
//! N puts written once. Strict tier: `set_sync(true)` on that single write (one
//! fsync at end); NonDurable: WAL on, `set_sync(false)`. Large write buffer +
//! auto-compaction disabled so the window measures the write path, matching the
//! YCSB RocksDB bench. See docs/tasks/task49_bulk_load_bench.md.

use criterion::{Criterion, criterion_group, criterion_main};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use tempfile::TempDir;

use ultima_bench_workloads::bulk_load::{bench_bulk_load, bulk_load_criterion};
use ultima_bench_workloads::ycsb::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

struct RocksEngine {
    db: DB,
    _tmpdir: TempDir,
    sync: bool,
}

fn make_empty(_n: u64) -> RocksEngine {
    let tmpdir = tempfile::tempdir_in(bench_disk_dir()).expect("failed to create temp dir");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(256 * 1024 * 1024);
    opts.set_disable_auto_compactions(true);
    let db = DB::open(&opts, tmpdir.path()).expect("failed to open rocksdb");
    RocksEngine {
        db,
        _tmpdir: tmpdir,
        sync: bench_durability() == BenchDurability::Strict,
    }
}

fn load(e: &mut RocksEngine, n: u64) {
    let mut batch = WriteBatch::default();
    for i in 1..=n {
        let value =
            bincode::serde::encode_to_vec(YcsbRecord::new(i), BINCODE_CFG).expect("serialize");
        batch.put(encode_key(i), value);
    }
    let mut wo = WriteOptions::default();
    wo.set_sync(e.sync);
    e.db.write_opt(batch, &wo).expect("write_opt failed");
}

fn bench(c: &mut Criterion) {
    bench_bulk_load(c, make_empty, load);
}

criterion_group! { name = benches; config = bulk_load_criterion(); targets = bench }
criterion_main!(benches);
```

- [ ] **Step 2: Register the bench**

Modify `compare_benches/Cargo.toml` — add after the last `[[bench]]` block:

```toml
[[bench]]
name = "ycsb_bulk_load_rocksdb_bench"
harness = false
```

- [ ] **Step 3: Smoke-run and verify**

Run: `cargo bench -p compare-benches --bench ycsb_bulk_load_rocksdb_bench -- --quick`
Expected: `bulk_load/10000`, `bulk_load/100000`, `bulk_load/1000000` each with a `time:` result; no panics.

- [ ] **Step 4: Commit**

```bash
git add compare_benches/benches/ycsb_bulk_load_rocksdb_bench.rs compare_benches/Cargo.toml
git commit -m "bench(bulk-load): RocksDB WriteBatch arm"
```

---

### Task 5: Fjall bench binary

**Files:**
- Create: `compare_benches/benches/ycsb_bulk_load_fjall_bench.rs`
- Modify: `compare_benches/Cargo.toml` (add `[[bench]]`)

**Interfaces:**
- Consumes: harness; `ycsb::*`; `fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode}`.
- Produces: bench binary `ycsb_bulk_load_fjall_bench`, IDs `bulk_load/{N}`.

- [ ] **Step 1: Write the bench**

Create `compare_benches/benches/ycsb_bulk_load_fjall_bench.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bulk-load arm: Fjall. Insert N records, then one `persist(SyncAll)` in the
//! Strict tier (one journal fsync at end); NonDurable inserts buffer with no
//! fsync. Field order matters for drop (keyspace before db). See
//! docs/tasks/task49_bulk_load_bench.md.

use criterion::{Criterion, criterion_group, criterion_main};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use tempfile::TempDir;

use ultima_bench_workloads::bulk_load::{bench_bulk_load, bulk_load_criterion};
use ultima_bench_workloads::ycsb::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

struct FjallEngine {
    keyspace: Keyspace,
    db: Database,
    _tmpdir: TempDir,
    sync: bool,
}

fn make_empty(_n: u64) -> FjallEngine {
    let tmpdir = tempfile::tempdir_in(bench_disk_dir()).expect("failed to create temp dir");
    let db = Database::builder(tmpdir.path())
        .open()
        .expect("failed to open fjall database");
    let keyspace = db
        .keyspace("ycsb", KeyspaceCreateOptions::default)
        .expect("failed to create keyspace");
    FjallEngine {
        keyspace,
        db,
        _tmpdir: tmpdir,
        sync: bench_durability() == BenchDurability::Strict,
    }
}

fn load(e: &mut FjallEngine, n: u64) {
    for i in 1..=n {
        let value =
            bincode::serde::encode_to_vec(YcsbRecord::new(i), BINCODE_CFG).expect("serialize");
        e.keyspace.insert(encode_key(i), value).expect("insert failed");
    }
    if e.sync {
        e.db.persist(PersistMode::SyncAll).expect("persist failed");
    }
}

fn bench(c: &mut Criterion) {
    bench_bulk_load(c, make_empty, load);
}

criterion_group! { name = benches; config = bulk_load_criterion(); targets = bench }
criterion_main!(benches);
```

- [ ] **Step 2: Register the bench**

Modify `compare_benches/Cargo.toml` — add:

```toml
[[bench]]
name = "ycsb_bulk_load_fjall_bench"
harness = false
```

- [ ] **Step 3: Smoke-run and verify**

Run: `cargo bench -p compare-benches --bench ycsb_bulk_load_fjall_bench -- --quick`
Expected: three `bulk_load/{N}` results; no panics.

- [ ] **Step 4: Commit**

```bash
git add compare_benches/benches/ycsb_bulk_load_fjall_bench.rs compare_benches/Cargo.toml
git commit -m "bench(bulk-load): Fjall arm"
```

---

### Task 6: ReDB bench binary

**Files:**
- Create: `compare_benches/benches/ycsb_bulk_load_redb_bench.rs`
- Modify: `compare_benches/Cargo.toml` (add `[[bench]]`)

**Interfaces:**
- Consumes: harness; `ycsb::*`; `redb::{Database, Durability, TableDefinition}`.
- Produces: bench binary `ycsb_bulk_load_redb_bench`, IDs `bulk_load/{N}`.

- [ ] **Step 1: Write the bench**

Create `compare_benches/benches/ycsb_bulk_load_redb_bench.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bulk-load arm: ReDB. One write transaction, N inserts, one commit. Strict
//! tier: `Durability::Immediate` (fsync on that single commit); NonDurable:
//! `Durability::None`. See docs/tasks/task49_bulk_load_bench.md.

use criterion::{Criterion, criterion_group, criterion_main};
use redb::{Database, Durability, TableDefinition};
use tempfile::TempDir;

use ultima_bench_workloads::bulk_load::{bench_bulk_load, bulk_load_criterion};
use ultima_bench_workloads::ycsb::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();
const TABLE: TableDefinition<[u8; 8], &[u8]> = TableDefinition::new("ycsb");

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

struct RedbEngine {
    db: Database,
    _tmpdir: TempDir,
    durability: Durability,
}

fn make_empty(_n: u64) -> RedbEngine {
    let tmpdir = tempfile::tempdir_in(bench_disk_dir()).expect("failed to create temp dir");
    let db = Database::create(tmpdir.path().join("ycsb.redb")).expect("failed to create redb");
    let durability = match bench_durability() {
        BenchDurability::NonDurable => Durability::None,
        BenchDurability::Strict => Durability::Immediate,
    };
    RedbEngine {
        db,
        _tmpdir: tmpdir,
        durability,
    }
}

fn load(e: &mut RedbEngine, n: u64) {
    let mut tx = e.db.begin_write().expect("begin_write failed");
    tx.set_durability(e.durability).expect("set_durability failed");
    {
        let mut table = tx.open_table(TABLE).expect("open_table failed");
        for i in 1..=n {
            let value =
                bincode::serde::encode_to_vec(YcsbRecord::new(i), BINCODE_CFG).expect("serialize");
            table.insert(encode_key(i), value.as_slice()).expect("insert failed");
        }
    }
    tx.commit().expect("commit failed");
}

fn bench(c: &mut Criterion) {
    bench_bulk_load(c, make_empty, load);
}

criterion_group! { name = benches; config = bulk_load_criterion(); targets = bench }
criterion_main!(benches);
```

- [ ] **Step 2: Register the bench**

Modify `compare_benches/Cargo.toml` — add:

```toml
[[bench]]
name = "ycsb_bulk_load_redb_bench"
harness = false
```

- [ ] **Step 3: Smoke-run and verify**

Run: `cargo bench -p compare-benches --bench ycsb_bulk_load_redb_bench -- --quick`
Expected: three `bulk_load/{N}` results; no panics.

- [ ] **Step 4: Commit**

```bash
git add compare_benches/benches/ycsb_bulk_load_redb_bench.rs compare_benches/Cargo.toml
git commit -m "bench(bulk-load): ReDB arm"
```

---

### Task 7: Makefile target + task doc

**Files:**
- Modify: `Makefile` (add `bench/bulk-load/compare` target + `.PHONY` entry)
- Create: `docs/tasks/task49_bulk_load_bench.md`
- Modify: `docs/tasks/task48_btree_insert_mut.md` (cross-link in §5)

**Interfaces:**
- Consumes: the five bench binaries from Tasks 2–6; `critcmp`.
- Produces: `make bench/bulk-load/compare ULTIMA_BENCH_DIR=…`.

- [ ] **Step 1: Add the Makefile target**

Modify `Makefile`. Append the `.PHONY` name to the existing `.PHONY:` line (add ` bench/bulk-load/compare`). Then add this target after the `bench/ycsb/compare` block:

```makefile
# Bulk-load ingest comparison (build empty db of N records). Five arms:
# UltimaDB insert_batch, UltimaDB Store::bulk_load, RocksDB, Fjall, ReDB.
# Same ULTIMA_BENCH_DIR real-disk guard as bench/ycsb/compare.
bench/bulk-load/compare:
	$(call check_cmd,critcmp)
	@if [ -z "$(ULTIMA_BENCH_DIR)" ]; then \
	  echo "ERROR: ULTIMA_BENCH_DIR is not set — refusing to run."; \
	  echo "  Point it at a real disk-backed dir (NOT a tmpfs like /tmp):"; \
	  echo "    make bench/bulk-load/compare ULTIMA_BENCH_DIR=\$$HOME/bench-disk"; \
	  exit 1; \
	fi
	@mkdir -p "$(ULTIMA_BENCH_DIR)"
	@for tier in nondurable strict; do \
	  echo "===== bulk-load tier: $$tier ====="; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench --bench ycsb_bulk_load_ultima_batch_bench  -- --save-baseline ub_batch_$$tier  || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench --bench ycsb_bulk_load_ultima_sorted_bench -- --save-baseline ub_sorted_$$tier || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_bulk_load_rocksdb_bench -- --save-baseline ub_rocksdb_$$tier || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_bulk_load_fjall_bench   -- --save-baseline ub_fjall_$$tier   || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_bulk_load_redb_bench    -- --save-baseline ub_redb_$$tier    || exit 1; \
	done
	@echo "===== non-durable tier (build cost) ====="
	critcmp ub_batch_nondurable ub_sorted_nondurable ub_rocksdb_nondurable ub_fjall_nondurable ub_redb_nondurable
	@echo "===== strict tier (one fsync at end of load) ====="
	critcmp ub_batch_strict ub_sorted_strict ub_rocksdb_strict ub_fjall_strict ub_redb_strict
```

- [ ] **Step 2: Verify the Makefile target parses and guards**

Run: `make bench/bulk-load/compare` (with `ULTIMA_BENCH_DIR` unset)
Expected: prints the `ERROR: ULTIMA_BENCH_DIR is not set` guard and exits non-zero — confirms the target is wired and the guard fires. (Do NOT run the full comparison here; it is the deliverable, run separately on a quiet real-disk host.)

- [ ] **Step 3: Write the task doc**

Create `docs/tasks/task49_bulk_load_bench.md`:

```markdown
# task49: Bulk-load benchmark vs competitors

**Status:** Implemented. Timed bulk-ingest comparison (build empty db of N
records) across five arms — UltimaDB `insert_batch`, UltimaDB `Store::bulk_load`,
RocksDB, Fjall, ReDB — in both durability tiers. Run:
`make bench/bulk-load/compare ULTIMA_BENCH_DIR=$HOME/bench-disk`.
**Related:** `docs/tasks/task48_btree_insert_mut.md` (the `insert_mut` win this
exercises), `docs/superpowers/specs/2026-07-08-bulk-load-benchmark-design.md`.

## Why

The YCSB comparison (`ycsb_bench`) times only single-op-per-transaction writes,
where `insert_mut` cannot help (a lone insert CoW-clones the whole path), and it
loads its data as untimed setup. This benchmark times the multi-insert-per-
transaction ingest path — where `insert_mut` actually wins — against the same
competitors.

## What is measured

Per engine/arm and size N ∈ {10 000, 100 000, 1 000 000}: wall-clock to ingest
records `1..=N` into an empty database via that engine's idiomatic single-batch
path, plus one end-of-load durability sync (Strict tier) or none (NonDurable).
Db-open and teardown are untimed criterion setup.

Fairness invariant: idiomatic single-batch path per engine; exactly one fsync at
the end in Strict, none in NonDurable. No per-record fsync.

## Arms

| arm            | load path                                                        |
|----------------|------------------------------------------------------------------|
| ub_batch       | `Table::insert_batch` in one `WriteTx` → commit (`insert_mut`)    |
| ub_sorted      | `Store::bulk_load(Replace(auto_id_vec), checkpoint_after)`        |
| ub_rocksdb     | one `WriteBatch` of N puts → `write_opt(sync)`                    |
| ub_fjall       | insert N → `persist(SyncAll)` once (Strict)                       |
| ub_redb        | one write-txn, N inserts, `set_durability`, commit               |

## Results

_(fill in the two `critcmp` tables — non-durable and strict — from a run on a
quiet real-disk host.)_

## Notes

- Absolute numbers are host-relative; the deliverable is the cross-engine A/B on
  a quiet real-disk `ULTIMA_BENCH_DIR`. Opt-in tier, not wired into `make bench`
  or CI (like the rest of `compare-benches`).
- `ub_sorted` (from_sorted, O(N)) is the ceiling and should be ≤ `ub_batch`.
```

- [ ] **Step 4: Cross-link from task48**

Modify `docs/tasks/task48_btree_insert_mut.md` — at the end of §5 (after the
end-to-end paragraph), add:

```markdown
A cross-engine timed bulk-load comparison (UltimaDB `insert_batch` / `bulk_load`
vs RocksDB / Fjall / ReDB) lives in `docs/tasks/task49_bulk_load_bench.md`
(`make bench/bulk-load/compare`).
```

- [ ] **Step 5: Commit**

```bash
git add Makefile docs/tasks/task49_bulk_load_bench.md docs/tasks/task48_btree_insert_mut.md
git commit -m "bench(bulk-load): make target + task49 doc"
```

---

## Self-Review

**Spec coverage:**
- §2 what is measured → Task 1 harness (timing boundary) + Tasks 2–6 arms. ✓
- §3 arms (6 rows) → ub_batch (T2), ub_sorted (T3), rocksdb (T4), fjall (T5), redb (T6). ✓ (Two UltimaDB arms delivered as two binaries instead of one file — refinement for `critcmp` identical-ID alignment; noted in plan header and Task boundaries.)
- §4 components/file layout → harness (T1), 5 bench files (T2–T6), Makefile + task49 (T7). ✓
- §5 data flow (iter_batched setup/routine/drop) → Task 1 `bench_bulk_load`. ✓
- §6 error handling (`.expect`/`.unwrap`) → all bench code uses it. ✓
- §7 testing/validation → per-task smoke steps + Task 7 Makefile guard check + task49 results placeholder for the real run. ✓
- §8 scope (no indexes, no threads, reuse YcsbRecord) → honored throughout. ✓

**Placeholder scan:** The only intentional blank is the task49 "Results" section, which is data produced by running the deliverable on a quiet host — not a plan/code gap. All code steps show complete code.

**Type consistency:** `bench_bulk_load(c, make_empty, load)` signature identical across Tasks 1–6; `UltimaBulkStore{store,_tmpdir}` used in T2/T3; `bench_durability_is_strict()` defined in T3 step 2 and used in T3 step 1; `bench_durability() == BenchDurability::Strict` used in T4/T5, `match bench_durability()` in T6 — all consistent with the existing `ycsb` module exports. Bench IDs `bulk_load/{N}` identical across all five binaries (harness-owned), so `critcmp` aligns them.
```
