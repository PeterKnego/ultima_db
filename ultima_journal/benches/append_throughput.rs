// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;
use ultima_journal::{Durability, Journal, JournalConfig};

fn bench_append_consistent(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_consistent");
    for &payload_size in &[64usize, 256, 1024, 4096] {
        group.throughput(Throughput::Bytes(payload_size as u64));
        group.bench_function(format!("p{payload_size}"), |b| {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = JournalConfig::new(dir.path());
            cfg.durability = Durability::Consistent;
            let j = Journal::open(cfg).unwrap();
            let payload = vec![0xABu8; payload_size];
            let mut next_seq = 1u64;
            b.iter(|| {
                let n = j.append(next_seq, 0, &payload).unwrap();
                n.wait().unwrap();
                next_seq += 1;
                black_box(next_seq);
            });
        });
    }
    group.finish();
}

fn bench_append_eventual(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_eventual");
    for &payload_size in &[64usize, 256, 1024] {
        group.throughput(Throughput::Bytes(payload_size as u64));
        group.bench_function(format!("p{payload_size}"), |b| {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = JournalConfig::new(dir.path());
            cfg.durability = Durability::Eventual;
            let j = Journal::open(cfg).unwrap();
            let payload = vec![0xABu8; payload_size];
            let mut next_seq = 1u64;
            b.iter(|| {
                let _ = j.append(next_seq, 0, &payload).unwrap();
                next_seq += 1;
                black_box(next_seq);
            });
        });
    }
    group.finish();
}

fn bench_iter_range(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
    for i in 1..=10_000u64 {
        j.append(i, 0, b"payload").unwrap().wait().unwrap();
    }
    c.bench_function("iter_range_1000", |b| {
        b.iter(|| {
            let v = j.read_range(1..=1000).unwrap();
            black_box(v);
        });
    });
}

criterion_group!(benches, bench_append_consistent, bench_append_eventual, bench_iter_range);
criterion_main!(benches);
