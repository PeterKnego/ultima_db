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
