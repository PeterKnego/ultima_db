// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::io::Read;
use std::hint::black_box;
use serde::{Serialize, Deserialize};
use ultima_db::{Store, StoreConfig};

#[derive(Serialize, Deserialize, Clone)]
struct Row { v: u64, name: String }

fn bench_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_build");
    for &n in &[1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("rows={n}"), |b| {
            let store = Store::new(StoreConfig::default()).unwrap();
            store.register_table::<Row>("rows").unwrap();
            let mut tx = store.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=n as u64 {
                t.insert(Row { v: i, name: format!("r{i}") }).unwrap();
            }
            tx.commit().unwrap();
            b.iter(|| {
                let mut buf = Vec::new();
                let mut r = store.snapshot_stream(None).unwrap();
                r.read_to_end(&mut buf).unwrap();
                black_box(buf);
            });
        });
    }
    group.finish();
}

fn bench_install(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_install");
    for &n in &[1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("rows={n}"), |b| {
            // Build once; install in the timed loop.
            let src = Store::new(StoreConfig::default()).unwrap();
            src.register_table::<Row>("rows").unwrap();
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=n as u64 {
                t.insert(Row { v: i, name: format!("r{i}") }).unwrap();
            }
            tx.commit().unwrap();
            let mut bytes = Vec::new();
            src.snapshot_stream(None).unwrap().read_to_end(&mut bytes).unwrap();
            b.iter(|| {
                let dst = Store::new(StoreConfig::default()).unwrap();
                dst.register_table::<Row>("rows").unwrap();
                dst.install_snapshot_stream(
                    std::io::Cursor::new(&bytes),
                    Default::default(),
                ).unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_build, bench_install);
criterion_main!(benches);
