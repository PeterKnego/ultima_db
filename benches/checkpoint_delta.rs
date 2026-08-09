// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Incremental checkpoints: the crossover question, and recovery time vs
//! chain length.
//!
//! This bench answers the two questions that set the recommended
//! `checkpoint_chain_max` default — see
//! `docs/tasks/task61_incremental_checkpoints.md` ("Measurements"):
//!
//! 1. **Crossover.** Fixed ~100k-row dataset, dirty fraction in
//!    `{1%, 10%, 50%, 100%}` between checkpoints, `checkpoint_chain_max` in
//!    `{1 (always full), 8}`. Measures `checkpoint()` wall time (criterion)
//!    and reports the checkpoint directory's byte footprint on stderr
//!    (informational only — not a criterion metric, and not something to
//!    read as a conclusion from a local run; see below).
//! 2. **Recovery vs chain length.** Chains of 1, 2, 4, 8, 16 files; measures
//!    `Store::recover()` wall time.
//!
//! A 100%-dirty delta is *expected* to be slower than a full checkpoint at
//! the same dataset size — it pays the ordered diff plus per-key framing on
//! top of what a full write already pays. Locating where the delta stops
//! being cheaper than the full is the point of bench 1.
//!
//! # Running this
//!
//! Locally — **for correctness only**, to prove the bench compiles and
//! completes without panicking:
//!
//! ```text
//! cargo bench --bench checkpoint_delta --features persistence
//! ```
//!
//! Per `CLAUDE.md`, the sandbox has a ±2x noise floor: **do not draw any
//! perf conclusion, record any timing, or compute any ratio from that run.**
//!
//! On the bench-infra NVMe host — the only place a number from this bench is
//! trustworthy — this rides the dedicated `checkpoint-delta` target (real,
//! billable AWS resources; requires explicit authorization before
//! provisioning):
//!
//! ```text
//! cd bench-infra && make bench-oneshot TARGET=checkpoint-delta
//! make status   # confirm nothing is left running
//! ```
//!
//! Results land in `bench-out/dist/<ts>/`.

use std::hint::black_box;
use std::path::Path;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use ultima_db::{Persistence, Store, StoreConfig};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Row {
    value: u64,
    payload: Vec<u8>,
}

const PAYLOAD_LEN: usize = 64;

/// Fixed dataset size for the crossover bench — "~100k rows" per the task
/// brief. Held constant across cells so only the dirty fraction and
/// `chain_max` vary.
const ROWS: u64 = 100_000;
const DIRTY_FRACTIONS_PCT: [u64; 4] = [1, 10, 50, 100];
const CHAIN_MAX_CELLS: [usize; 2] = [1, 8];

/// Smaller dataset for the chain-length sweep: what is being measured there
/// is how recovery cost *scales with chain length*, not the absolute dataset
/// size, and 16 chained checkpoints over 100k rows would make the sweep
/// needlessly slow to run.
const RECOVERY_ROWS: u64 = 20_000;
const CHAIN_LENGTHS: [usize; 5] = [1, 2, 4, 8, 16];

fn make_store(dir: &Path, chain_max: usize) -> Store {
    let store = Store::new(
        StoreConfig::builder()
            .persistence(Persistence::smr(dir.to_path_buf()))
            .checkpoint_chain_max(chain_max)
            .build(),
    )
    .unwrap();
    store.register_table::<Row>("data").unwrap();
    store.recover().unwrap();
    store
}

/// Insert `n` fresh rows, returning their assigned primary keys in insertion
/// order (auto-increment ids are not assumed to start at any particular
/// value here — the returned keys are used directly for later updates).
fn seed_rows(store: &Store, n: u64) -> Vec<u64> {
    let mut wtx = store.begin_write(None).unwrap();
    let keys = {
        let mut table = wtx.open_table::<Row>("data").unwrap();
        let records: Vec<Row> = (0..n)
            .map(|i| Row {
                value: i,
                payload: vec![0u8; PAYLOAD_LEN],
            })
            .collect();
        table.insert_batch(records).unwrap()
    };
    wtx.commit().unwrap();
    keys
}

/// Update `keys` in place — the "dirty set" between two checkpoints. `salt`
/// varies the written bytes across calls so repeated dirtying does not
/// degenerate into a true no-op update.
fn dirty_rows(store: &Store, keys: &[u64], salt: u64) {
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<Row>("data").unwrap();
        let updates: Vec<(u64, Row)> = keys
            .iter()
            .map(|&k| {
                (
                    k,
                    Row {
                        value: k.wrapping_add(salt),
                        payload: vec![salt as u8; PAYLOAD_LEN],
                    },
                )
            })
            .collect();
        table.update_batch(updates).unwrap();
    }
    wtx.commit().unwrap();
}

/// Sum of `checkpoint_*.bin` file sizes in `dir`. Informational only: printed
/// to stderr so a bench-host run can eyeball bytes written without a second
/// tool. Not a criterion metric, not asserted on, and not something a local
/// sandbox run should report as a conclusion.
fn checkpoint_dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("checkpoint_"))
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

/// Bench 1: `checkpoint()` wall time as a function of dirty fraction, at
/// `chain_max = 1` (always full — today's behaviour) and `chain_max = 8`
/// (delta whenever the chain has room).
fn bench_checkpoint_crossover(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_delta_crossover");

    for &chain_max in &CHAIN_MAX_CELLS {
        for &pct in &DIRTY_FRACTIONS_PCT {
            let dir = tempfile::tempdir_in(ultima_bench_workloads::ycsb::bench_disk_dir()).unwrap();
            let store = make_store(dir.path(), chain_max);
            let keys = seed_rows(&store, ROWS);
            // Establishes the chain base; always full (nothing to diff against yet).
            store.checkpoint().unwrap();

            let dirty_n = (ROWS * pct / 100).max(1) as usize;
            let dirty_keys = &keys[..dirty_n];
            let mut salt = 0u64;

            let id = BenchmarkId::new(format!("chain_max_{chain_max}"), format!("{pct}pct_dirty"));
            group.bench_function(id, |b| {
                b.iter_batched(
                    || {
                        salt = salt.wrapping_add(1);
                        dirty_rows(&store, dirty_keys, salt);
                    },
                    |()| {
                        black_box(store.checkpoint().unwrap());
                    },
                    BatchSize::PerIteration,
                );
            });

            eprintln!(
                "[checkpoint_delta_crossover] chain_max={chain_max} dirty={pct}% \
                 dir_bytes(informational, not a perf number)={}",
                checkpoint_dir_bytes(dir.path())
            );
        }
    }

    group.finish();
}

/// Build a checkpoint directory holding a chain of exactly `chain_len` files
/// (one full + `chain_len - 1` deltas, each dirtying ~1% of `RECOVERY_ROWS`).
fn build_chain_dir(chain_len: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir_in(ultima_bench_workloads::ycsb::bench_disk_dir()).unwrap();
    let store = make_store(dir.path(), chain_len.max(1));
    let keys = seed_rows(&store, RECOVERY_ROWS);
    store.checkpoint().unwrap(); // full; chain length 1

    let dirty_n = (RECOVERY_ROWS / 100).max(1) as usize;
    for i in 1..chain_len {
        dirty_rows(&store, &keys[..dirty_n], i as u64);
        store.checkpoint().unwrap(); // delta; extends the chain by one
    }
    dir
}

/// Bench 2: `Store::recover()` wall time as a function of chain length. This
/// is the curve that should set the recommended `checkpoint_chain_max`
/// default (see the task doc) — recovery cost is expected to grow with chain
/// length since replay folds every delta in order.
fn bench_recovery_vs_chain_length(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_recovery_vs_chain_length");

    for &chain_len in &CHAIN_LENGTHS {
        let dir = build_chain_dir(chain_len);

        eprintln!(
            "[checkpoint_recovery_vs_chain_length] chain_len={chain_len} \
             dir_bytes(informational, not a perf number)={}",
            checkpoint_dir_bytes(dir.path())
        );

        group.bench_function(BenchmarkId::new("chain_len", chain_len), |b| {
            b.iter(|| {
                let store = Store::new(
                    StoreConfig::builder()
                        .persistence(Persistence::smr(dir.path().to_path_buf()))
                        .build(),
                )
                .unwrap();
                store.register_table::<Row>("data").unwrap();
                store.recover().unwrap();
                black_box(&store);
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = checkpoint_delta;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(1));
    targets = bench_checkpoint_crossover, bench_recovery_vs_chain_length
}
criterion_main!(checkpoint_delta);
