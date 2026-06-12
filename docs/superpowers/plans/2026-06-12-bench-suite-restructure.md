# Bench Suite Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the benchmark suite into a first-party tier (default) and an opt-in competitor-comparison tier, extract shared workload generators into a lib crate, and consolidate three overlapping multiwriter-scaling benches into one.

**Architecture:** Two new workspace members: `bench_workloads` (lib crate holding the YCSB/SmallBank generators currently `#[path]`-included from `benches/`) and `compare_benches` (bench-only crate holding the 7 RocksDB/Fjall/ReDB baseline benches plus their dev-deps, so the root crate stops compiling RocksDB). A new `benches/multiwriter_scaling_bench.rs` replaces `disjoint_tables_bench.rs`, `smallbank_scaling_bench.rs`, and the scaling group inside `bench_multiwriter_workloads`.

**Tech Stack:** Rust (edition 2024), criterion 0.8, cargo workspaces. Spec: `docs/superpowers/specs/2026-06-12-autobench-perf-harness-design.md` §2, §8.

**Conventions:** Every new/moved file keeps the `// SPDX-License-Identifier: Apache-2.0` + copyright header. Run `cargo clippy -- -D warnings` before every commit (project lint gate).

---

### Task 1: `bench_workloads` lib crate

The common modules are currently included via `#[path = "ycsb_common.rs"] mod ycsb_common;` in each bench binary. Move them into a proper lib crate so `compare_benches` (Task 2) and the autobench harness (separate plan) can use them too.

**Files:**
- Create: `bench_workloads/Cargo.toml`, `bench_workloads/src/lib.rs`
- Move: `benches/ycsb_common.rs` → `bench_workloads/src/ycsb.rs`, `benches/smallbank_common.rs` → `bench_workloads/src/smallbank.rs`
- Modify: `Cargo.toml` (workspace members + dev-dep), all 11 bench files listed in Step 4

- [ ] **Step 1: Create the crate**

```bash
mkdir -p bench_workloads/src
git mv benches/ycsb_common.rs bench_workloads/src/ycsb.rs
git mv benches/smallbank_common.rs bench_workloads/src/smallbank.rs
```

`bench_workloads/Cargo.toml`:

```toml
[package]
name = "ultima-bench-workloads"
version = "0.1.0"
edition = "2024"
publish = false
description = "Shared YCSB/SmallBank workload generators for UltimaDB benches"
license = "Apache-2.0"

[dependencies]
ultima-db = { path = "..", features = ["persistence"] }
criterion = { version = "0.8", features = ["html_reports"] }
rand = "0.10"
serde = { version = "1", features = ["derive"] }
```

`bench_workloads/src/lib.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared workload generators (YCSB, SmallBank) used by the first-party
//! benches, the competitor baselines in `compare_benches`, and the autobench
//! harness. Extracted from the former `benches/{ycsb,smallbank}_common.rs`
//! `#[path]` includes.

pub mod smallbank;
pub mod ycsb;
```

- [ ] **Step 2: Fix up the moved modules**

In both `bench_workloads/src/ycsb.rs` and `bench_workloads/src/smallbank.rs`:
- Delete the comment line `// Included via \`#[path]\` in multiple bench binaries, each using a different subset.` (and the smallbank equivalent if present).
- Try removing the `#![allow(dead_code, unused_imports)]` inner attribute at the top: as lib-crate modules with `pub` items, dead-code lints mostly disappear. If `cargo clippy` then complains about genuinely unused private helpers, put the attribute back (inner attributes are valid at the top of module files) rather than deleting code.

- [ ] **Step 3: Register in the workspace**

In root `Cargo.toml`:

```toml
[workspace]
members = ["ultima_vector", "ultima_journal", "bench_workloads"]
```

and under `[dev-dependencies]` add:

```toml
ultima-bench-workloads = { path = "bench_workloads" }
```

- [ ] **Step 4: Update bench imports**

Eleven files include the common modules. Find them:

```bash
grep -l 'ycsb_common\|smallbank_common' benches/*.rs
```

Expected: `ycsb_bench.rs`, `ycsb_fjall_bench.rs`, `ycsb_rocksdb_bench.rs`, `ycsb_redb_bench.rs`, `ycsb_multiwriter_bench.rs`, `ycsb_multiwriter_rocksdb_bench.rs`, `ycsb_multiwriter_fjall_bench.rs`, `smallbank_bench.rs`, `smallbank_rocksdb_bench.rs`, `smallbank_fjall_bench.rs`, `smallbank_scaling_bench.rs`.

In each, replace the include-pattern:

```rust
#[path = "ycsb_common.rs"]
mod ycsb_common;
use ycsb_common::{...};
```

with a direct use of the lib crate (keep the same imported item list):

```rust
use ultima_bench_workloads::ycsb::{...};
```

and likewise:

```rust
#[path = "smallbank_common.rs"]
mod smallbank_common;
use smallbank_common::*;
```

becomes:

```rust
use ultima_bench_workloads::smallbank::*;
```

(Some files may write `use ycsb_common::{A, B}` on multiple lines or use qualified paths like `ycsb_common::FOO` in the body — grep each file for `ycsb_common::`/`smallbank_common::` after editing and rewrite stragglers to `ultima_bench_workloads::ycsb::`/`::smallbank::`.)

- [ ] **Step 5: Verify everything builds**

```bash
cargo bench --no-run --features bench-internals
cargo clippy --benches --features bench-internals -- -D warnings
cargo test -p ultima-bench-workloads
```

Expected: all bench targets compile; clippy clean. (`bench-internals` implies `persistence`, so the feature-gated benches build too.)

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(bench): extract ycsb/smallbank workloads into bench_workloads crate"
```

---

### Task 2: `compare_benches` crate (competitor tier)

Move the 7 RocksDB/Fjall/ReDB baseline benches and their dev-deps out of the root crate. After this, plain `cargo bench`/`cargo test` at the root never compiles RocksDB.

**Files:**
- Create: `compare_benches/Cargo.toml`, `compare_benches/benches/` (7 moved files)
- Modify: root `Cargo.toml` (remove 7 `[[bench]]` sections + rocksdb/fjall/redb dev-deps; add member), `Makefile`

- [ ] **Step 1: Create crate and move the benches**

```bash
mkdir -p compare_benches/benches compare_benches/src
echo '// SPDX-License-Identifier: Apache-2.0' > compare_benches/src/lib.rs
for f in ycsb_fjall_bench ycsb_rocksdb_bench ycsb_redb_bench \
         ycsb_multiwriter_rocksdb_bench ycsb_multiwriter_fjall_bench \
         smallbank_rocksdb_bench smallbank_fjall_bench; do
  git mv benches/$f.rs compare_benches/benches/$f.rs
done
```

`compare_benches/Cargo.toml`:

```toml
[package]
name = "compare-benches"
version = "0.1.0"
edition = "2024"
publish = false
description = "Competitor baseline benchmarks (RocksDB, Fjall, ReDB) — opt-in tier"
license = "Apache-2.0"

[dev-dependencies]
ultima-db = { path = "..", features = ["persistence"] }
ultima-bench-workloads = { path = "../bench_workloads" }
criterion = { version = "0.8", features = ["html_reports"] }
rand = "0.10"
serde = { version = "1", features = ["derive"] }
bincode = { version = "2", features = ["serde"] }
tempfile = "3"
fjall = "3"
rocksdb = "0.24.0"
redb = "4.0.0"

[[bench]]
name = "ycsb_fjall_bench"
harness = false

[[bench]]
name = "ycsb_rocksdb_bench"
harness = false

[[bench]]
name = "ycsb_redb_bench"
harness = false

[[bench]]
name = "ycsb_multiwriter_rocksdb_bench"
harness = false

[[bench]]
name = "ycsb_multiwriter_fjall_bench"
harness = false

[[bench]]
name = "smallbank_rocksdb_bench"
harness = false

[[bench]]
name = "smallbank_fjall_bench"
harness = false
```

(`src/lib.rs` exists only because a package needs a target; it stays empty apart from the license header.)

- [ ] **Step 2: Clean the root crate**

In root `Cargo.toml`:
- Add `"compare_benches"` to `[workspace] members`.
- Delete the seven `[[bench]]` sections matching the moved files.
- Delete `fjall`, `rocksdb`, `redb` from `[dev-dependencies]` (keep `bincode`, `tempfile`, `serde` — still used by tests and remaining benches).

- [ ] **Step 3: Retarget the Makefile**

Update these targets to use `-p compare-benches` (the bench names are unchanged):

```makefile
bench/ycsb/fjall:
	cargo bench -p compare-benches --bench ycsb_fjall_bench

bench/ycsb/rocksdb:
	cargo bench -p compare-benches --bench ycsb_rocksdb_bench

bench/ycsb/redb:
	cargo bench -p compare-benches --bench ycsb_redb_bench

bench/multiwriter/rocksdb:
	cargo bench -p compare-benches --bench ycsb_multiwriter_rocksdb_bench

bench/multiwriter/fjall:
	cargo bench -p compare-benches --bench ycsb_multiwriter_fjall_bench
```

In `bench/ycsb/compare` and `bench/multiwriter/compare`, prefix the competitor lines with `-p compare-benches` the same way (the `--save-baseline` flags stay; criterion baselines share the workspace `target/`, so critcmp still works). Add an aggregate opt-in target and document it:

```makefile
# Competitor baseline tier (RocksDB/Fjall/ReDB) — not part of `make bench`
bench/compare-engines:
	cargo bench -p compare-benches
```

Add `bench/compare-engines` to the `.PHONY` line.

- [ ] **Step 4: Verify**

```bash
cargo bench -p compare-benches --no-run          # competitor tier compiles
cargo bench --no-run --features bench-internals  # root tier compiles
cargo tree -e dev -i rocksdb 2>&1 | head -5      # expect: error "package ID specification ... did not match" OR only compare-benches in the inverse tree
cargo clippy --benches --features bench-internals -- -D warnings
```

The `cargo tree` check from the root package proves rocksdb left the root dev-dep graph: run it as `cargo tree -e dev -i rocksdb -p ultima-db`; expected output is an error that `rocksdb` is not in the dependency graph of `ultima-db`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(bench): move competitor baselines to opt-in compare_benches crate"
```

---

### Task 3: Consolidated `multiwriter_scaling_bench`

Three benches probe multiwriter commit scaling. Merge them into one writer-count sweep with four contention shapes; delete the originals and the scaling group inside the shared YCSB suite.

**Files:**
- Create: `benches/multiwriter_scaling_bench.rs`
- Modify: `bench_workloads/src/ycsb.rs` (delete scaling group), `bench_workloads/src/smallbank.rs` (gain two helpers), root `Cargo.toml`, `Makefile`
- Delete: `benches/disjoint_tables_bench.rs`, `benches/smallbank_scaling_bench.rs`

- [ ] **Step 1: Move SmallBank concurrent helpers into the lib**

`benches/smallbank_scaling_bench.rs` contains `fn build_store()`, `fn execute_ops_on_tx(...)`, and `fn run_burst(...)` (lines ~33–98, ~108–210, ~212–250 — locate by name). Cut these three functions **verbatim** out of that file and paste into `bench_workloads/src/smallbank.rs`, with these changes only:
- Mark all three `pub`.
- Rename `build_store` → `pub fn build_concurrent_store() -> (Store, tempfile::TempDir)` and `run_burst` → `pub fn run_smallbank_burst(store: &Store, op_sets: &[Vec<SmallBankOp>])` (avoids collisions with other helpers).
- Add the imports they need at the top of `smallbank.rs` (dedupe with existing ones):

```rust
use std::sync::{Arc, Barrier};
use std::thread;
use ultima_db::{IndexKind, Store, StoreConfig, WriterMode};
```

- Add to `bench_workloads/Cargo.toml` `[dependencies]`: `tempfile = "3"` (needed by `build_concurrent_store`).

Also move `fn gen_hot_ops(num_writers: usize, seed: u64) -> Vec<Vec<SmallBankOp>>` from the same file, as `pub fn gen_hot_ops(...)` (it uses `gen_mixed_workload_n`, `ZipfianGenerator`, `OPS_PER_WRITER` — already in the module; replace its `OPS_PER_WRITER_LOCAL` reference with the module's `OPS_PER_WRITER` constant, both are 50).

- [ ] **Step 2: Delete the scaling group from the shared YCSB suite**

In `bench_workloads/src/ycsb.rs`, inside `pub fn bench_multiwriter_workloads`, delete the entire block starting with the comment `// Scaling: vary number of concurrent writers (1, 2, 4, 8)` through its closing brace (the block containing the `multiwriter_scaling` benchmark group). The low/high-contention groups and the smoke test stay. This removes scaling from the competitor multiwriter benches too — scaling is now first-party-only by design (spec §8).

- [ ] **Step 3: Write the consolidated bench**

Create `benches/multiwriter_scaling_bench.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

#![allow(clippy::drop_non_drop, clippy::redundant_iter_cloned)]

//! Consolidated multi-writer scaling bench: one writer-count sweep, four
//! contention shapes. Replaces the former `disjoint_tables_bench`,
//! `smallbank_scaling_bench`, and the scaling group of the shared YCSB
//! multi-writer suite.
//!
//! Groups (criterion id `mw_scaling_<shape>/<writers>`):
//! - `ycsb_low`        — one shared table, Zipfian over 10K keys (in-memory)
//! - `ycsb_high`       — one shared table, hot keys 1..=10 (in-memory)
//! - `disjoint`        — one table per writer (Eventual persistence)
//! - `smallbank_high`  — 3-table txns on hot accounts (Eventual persistence)

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng};
use ultima_bench_workloads::smallbank::{
    build_concurrent_store, gen_hot_ops, run_smallbank_burst,
};
use ultima_bench_workloads::ycsb::{NUM_RECORDS, YcsbRecord, ZipfianGenerator, ZIPFIAN_CONSTANT};
use ultima_db::{Store, StoreConfig, WriterMode};

const WRITER_COUNTS: &[usize] = &[1, 2, 4, 8, 16];
const MULTI_WRITER_COUNTS: &[usize] = &[2, 4, 8, 16];
const OPS_PER_WRITER: usize = 50;
const POOL_SIZE: usize = 64;

// ---------------------------------------------------------------------------
// Shape 1+2: YCSB shared table (ported from the shared suite's scaling group)
// ---------------------------------------------------------------------------

fn ycsb_store() -> Store {
    let store = Store::new(StoreConfig {
        num_snapshots_retained: 2,
        auto_snapshot_gc: true,
        writer_mode: WriterMode::MultiWriter,
        ..StoreConfig::default()
    })
    .unwrap();
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
        for i in 1..=NUM_RECORDS {
            table.insert(YcsbRecord::new(i)).unwrap();
        }
    }
    wtx.commit().unwrap();
    store
}

/// One burst: each key set is one writer's updates on the shared table,
/// committed concurrently with retry-on-conflict. Returns total conflicts.
fn ycsb_burst(store: &Store, key_sets: &[Vec<u64>]) -> u64 {
    let barrier = Arc::new(Barrier::new(key_sets.len()));
    let handles: Vec<_> = key_sets
        .iter()
        .cloned()
        .map(|keys| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut conflicts = 0u64;
                loop {
                    let mut wtx = store.begin_write(None).unwrap();
                    {
                        let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                        for &key in &keys {
                            let _ = table.update(key, YcsbRecord::new(key.wrapping_add(1)));
                        }
                    }
                    match wtx.commit() {
                        Ok(_) => return conflicts,
                        Err(ultima_db::Error::WriteConflict { .. }) => {
                            conflicts += 1;
                            continue;
                        }
                        Err(e) => panic!("unexpected error: {e}"),
                    }
                }
            })
        })
        .collect();
    handles.into_iter().map(|h| h.join().unwrap()).sum()
}

fn bench_ycsb_shared(c: &mut Criterion, shape: &str, hot: bool, seed: u64) {
    let store = ycsb_store();
    let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
    let mut rng = StdRng::seed_from_u64(seed);

    let mut group = c.benchmark_group(format!("mw_scaling_{shape}"));
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(10));
    for &n in WRITER_COUNTS {
        let total_ops = (n * OPS_PER_WRITER) as u64;
        group.throughput(Throughput::Elements(total_ops));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &nw| {
            b.iter(|| {
                let key_sets: Vec<Vec<u64>> = (0..nw)
                    .map(|_| {
                        (0..OPS_PER_WRITER)
                            .map(|_| {
                                if hot {
                                    rng.random_range(1..=10u64)
                                } else {
                                    zipf.next(&mut rng)
                                }
                            })
                            .collect()
                    })
                    .collect();
                black_box(ycsb_burst(&store, &key_sets))
            });
        });
    }
    group.finish();
}

fn bench_ycsb_low(c: &mut Criterion) {
    bench_ycsb_shared(c, "ycsb_low", false, 500);
}

fn bench_ycsb_high(c: &mut Criterion) {
    bench_ycsb_shared(c, "ycsb_high", true, 501);
}

// ---------------------------------------------------------------------------
// Shape 3: disjoint tables (ported verbatim from disjoint_tables_bench.rs;
// functions renamed with a disjoint_ prefix)
// ---------------------------------------------------------------------------

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ItemState {
    value: u64,
}

const KEYS_PER_TABLE: u64 = 10_000;

fn disjoint_table_name(idx: usize) -> String {
    format!("shard_{idx:02}")
}

fn disjoint_build_store(max_writers: usize) -> (Store, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir().unwrap();
    let cfg = StoreConfig {
        num_snapshots_retained: 2,
        auto_snapshot_gc: true,
        writer_mode: WriterMode::MultiWriter,
        persistence: ultima_db::Persistence::Standalone {
            dir: tmpdir.path().to_path_buf(),
            durability: ultima_db::Durability::Eventual,
            wal_write: ultima_db::WalWrite::PerEntry,
        },
        ..StoreConfig::default()
    };
    let store = Store::new(cfg).unwrap();
    for i in 0..max_writers {
        store
            .register_table::<ItemState>(&disjoint_table_name(i))
            .unwrap();
    }
    let mut wtx = store.begin_write(None).unwrap();
    for i in 0..max_writers {
        let name = disjoint_table_name(i);
        let mut t = wtx.open_table::<ItemState>(name.as_str()).unwrap();
        let batch: Vec<ItemState> = (1..=KEYS_PER_TABLE).map(|v| ItemState { value: v }).collect();
        t.insert_batch(batch).unwrap();
    }
    wtx.commit().unwrap();
    (store, tmpdir)
}

fn disjoint_gen_ops(num_writers: usize, seed: u64) -> Vec<Vec<(u64, u64)>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..num_writers)
        .map(|_| {
            (0..OPS_PER_WRITER)
                .map(|_| {
                    let id = rng.random_range(1..=KEYS_PER_TABLE);
                    let new_value = KEYS_PER_TABLE + rng.random_range(1..=10_000_000);
                    (id, new_value)
                })
                .collect()
        })
        .collect()
}

fn disjoint_run_burst(store: &Store, table_names: &[String], op_sets: &[Vec<(u64, u64)>]) {
    let barrier = Arc::new(Barrier::new(op_sets.len()));
    let handles: Vec<_> = op_sets
        .iter()
        .zip(table_names)
        .map(|(ops, tname)| {
            let ops = ops.clone();
            let tname = tname.clone();
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                loop {
                    let mut wtx = store.begin_write(None).unwrap();
                    let write_res: Result<(), ultima_db::Error> = {
                        let mut t = wtx.open_table::<ItemState>(tname.as_str()).unwrap();
                        let mut res = Ok(());
                        for (id, new_value) in &ops {
                            if let Err(e) = t.update(*id, ItemState { value: *new_value }) {
                                res = Err(e);
                                break;
                            }
                        }
                        res
                    };
                    match write_res {
                        Ok(()) => {}
                        Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                            drop(wtx);
                            if let Some(w) = wait_for {
                                w.wait();
                            }
                            continue;
                        }
                        Err(e) => panic!("ops error: {e}"),
                    }
                    match wtx.commit() {
                        Ok(_) => return,
                        Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                            if let Some(w) = wait_for {
                                w.wait();
                            }
                            continue;
                        }
                        Err(e) => panic!("commit error: {e}"),
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

fn bench_disjoint(c: &mut Criterion) {
    let max_writers = *MULTI_WRITER_COUNTS.iter().max().unwrap();
    let (store, _tmp) = disjoint_build_store(max_writers);

    let mut group = c.benchmark_group("mw_scaling_disjoint");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(10));
    for &n in MULTI_WRITER_COUNTS {
        let total_ops = (n * OPS_PER_WRITER) as u64;
        group.throughput(Throughput::Elements(total_ops));
        let names: Vec<String> = (0..n).map(disjoint_table_name).collect();
        let pool: Vec<Vec<Vec<(u64, u64)>>> = (0..POOL_SIZE)
            .map(|i| disjoint_gen_ops(n, 1000 + i as u64))
            .collect();
        let cursor = AtomicUsize::new(0);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched_ref(
                || {
                    let idx = cursor.fetch_add(1, Ordering::Relaxed) % pool.len();
                    pool[idx].clone()
                },
                |op_sets| {
                    disjoint_run_burst(&store, &names, op_sets);
                    black_box(());
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Shape 4: SmallBank hot accounts (helpers live in bench_workloads::smallbank)
// ---------------------------------------------------------------------------

fn bench_smallbank_high(c: &mut Criterion) {
    let mut group = c.benchmark_group("mw_scaling_smallbank_high");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(10));
    for &n in MULTI_WRITER_COUNTS {
        let total_ops = (n * OPS_PER_WRITER) as u64;
        group.throughput(Throughput::Elements(total_ops));
        let pool: Vec<_> = (0..POOL_SIZE).map(|i| gen_hot_ops(n, 500 + i as u64)).collect();
        let (store, _tmp) = build_concurrent_store();
        let cursor = AtomicUsize::new(0);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched_ref(
                || {
                    let idx = cursor.fetch_add(1, Ordering::Relaxed) % pool.len();
                    pool[idx].clone()
                },
                |op_sets| {
                    run_smallbank_burst(&store, op_sets);
                    black_box(());
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    mw_scaling,
    bench_ycsb_low,
    bench_ycsb_high,
    bench_disjoint,
    bench_smallbank_high
);
criterion_main!(mw_scaling);
```

- [ ] **Step 4: Delete the superseded benches and update Cargo.toml**

```bash
git rm benches/disjoint_tables_bench.rs benches/smallbank_scaling_bench.rs
```

In root `Cargo.toml`: delete the `[[bench]]` sections for `disjoint_tables_bench` and `smallbank_scaling_bench`; add:

```toml
[[bench]]
name = "multiwriter_scaling_bench"
harness = false
required-features = ["persistence"]
```

- [ ] **Step 5: Verify compile and smoke-run**

```bash
cargo bench --no-run --features bench-internals
cargo clippy --benches --features bench-internals -- -D warnings
cargo bench --bench multiwriter_scaling_bench --features persistence -- --test
cargo bench -p compare-benches --no-run   # competitor suite still compiles after the scaling-group removal
```

`-- --test` runs each benchmark once without measurement — expected to finish in ~1–2 min with no panics.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(bench): consolidate multiwriter scaling benches into one matrix"
```

---

### Task 4: Default tier in Makefile + docs

**Files:**
- Modify: `Makefile`, `CLAUDE.md`

- [ ] **Step 1: Make `make bench` the first-party tier**

With the competitor benches gone from the root crate, `cargo bench` at the root is already first-party-only — but it silently skips the feature-gated benches. Make the default explicit and complete:

```makefile
# First-party tier (default). Competitor baselines: bench/compare-engines.
bench:
	cargo bench --features bench-internals
```

Add a scaling convenience target and register new names in `.PHONY`:

```makefile
bench/scaling:
	cargo bench --bench multiwriter_scaling_bench --features persistence
```

- [ ] **Step 2: Update CLAUDE.md**

In the Build & Test Commands section, after the `cargo bench` line, replace/add:

```bash
cargo bench                              # first-party benchmarks (criterion)
make bench/compare-engines               # competitor baselines (RocksDB/Fjall/ReDB), opt-in
```

In the architecture section, add one bullet (after the `ultima_vector` bullet):

```markdown
- **Bench crates**: `bench_workloads` (shared YCSB/SmallBank generators, lib) and `compare_benches` (RocksDB/Fjall/ReDB baselines, opt-in tier — keeps their deps out of the root crate). First-party benches stay in `benches/`.
```

- [ ] **Step 3: Final verification**

```bash
make lint
cargo test --features persistence
cargo test -p ultima-journal
cargo test -p ultima-vector
make bench >/dev/null 2>&1 &   # optional: confirm it starts; Ctrl-C after the first group
```

The first three must pass (workspace verification gate).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs+make: first-party default bench tier, compare-engines opt-in"
```
