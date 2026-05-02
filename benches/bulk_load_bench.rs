// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Compares `Store::bulk_load` (sorted, bottom-up build) against
//! `TableWriter::insert_batch` (the existing batch-insert path) at 100k
//! and 1M rows.
//!
//! Run with `cargo bench --bench bulk_load_bench`. Use `-- --quick` for a
//! smoke-test pass.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ultima_db::{BulkLoadInput, BulkLoadOptions, BulkSource, Store};

fn bench_bulk_vs_insert_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_vs_insert_batch");
    for &n in &[100_000usize, 1_000_000] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("insert_batch", n), &n, |b, &n| {
            b.iter(|| {
                let store = Store::default();
                let mut wtx = store.begin_write(None).unwrap();
                let mut t = wtx.open_table::<String>("t").unwrap();
                t.insert_batch((0..n).map(|i| format!("v{i}")).collect())
                    .unwrap();
                wtx.commit().unwrap();
                store
            });
        });

        group.bench_with_input(BenchmarkId::new("bulk_load_sorted", n), &n, |b, &n| {
            b.iter(|| {
                let store = Store::default();
                let rows: Vec<(u64, String)> =
                    (1u64..=n as u64).map(|i| (i, format!("v{i}"))).collect();
                store
                    .bulk_load::<String>(
                        "t",
                        BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
                        BulkLoadOptions {
                            checkpoint_after: false,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                store
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_bulk_vs_insert_batch);
criterion_main!(benches);