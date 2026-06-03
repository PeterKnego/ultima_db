// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
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

fn bench_append_batched(c: &mut Criterion) {
    // Eventual mode: the Notifier resolves on the buffered write (fsync is
    // off-clock), so this measures the write path — exactly where coalescing
    // helps. Each iter submits a burst, then waits the final notifier; the
    // writer drains the burst into batches and (after this feature) coalesces
    // each batch into one write_all.
    let mut group = c.benchmark_group("append_batched_eventual");
    const BURST: u64 = 256;
    for &payload_size in &[64usize, 256, 1024, 4096] {
        group.throughput(Throughput::Bytes(payload_size as u64 * BURST));
        group.bench_function(format!("p{payload_size}"), |b| {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = JournalConfig::new(dir.path());
            cfg.durability = Durability::Eventual;
            let j = Journal::open(cfg).unwrap();
            let payload = vec![0xABu8; payload_size];
            let mut next_seq = 1u64;
            b.iter(|| {
                let mut last = None;
                for _ in 0..BURST {
                    last = Some(j.append(next_seq, 0, &payload).unwrap());
                    next_seq += 1;
                }
                last.unwrap().wait().unwrap();
                black_box(next_seq);
            });
        });
    }
    group.finish();
}

fn bench_point_read(c: &mut Criterion) {
    // Fill one large (~16 MiB) segment, then point-read at deterministically
    // rotating seqs. Measures per-read cost: full-segment scan (before) vs the
    // bounded windowed read (after). Eventual durability keeps the fill fast
    // (no per-append fsync); waiting the last notifier ensures every record is
    // written to the file before reads begin.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.durability = Durability::Eventual;
    let j = Journal::open(cfg).unwrap();
    let payload = vec![0xABu8; 1024];
    let n: u64 = 16 * 1024; // ~16 MiB of 1 KiB records, one segment
    let mut last = None;
    for i in 1..=n {
        last = Some(j.append(i, i, &payload).unwrap());
    }
    last.unwrap().wait().unwrap();

    let mut k = 0u64;
    c.bench_function("point_read", |b| {
        b.iter(|| {
            // Rotate the target deterministically across [1, n] to hit
            // different index windows without RNG.
            k = k.wrapping_add(2_654_435_761) % n + 1;
            let r = j.read(black_box(k)).unwrap();
            black_box(r);
        });
    });
}

fn bench_range_read(c: &mut Criterion) {
    // Fill one large (~16 MiB) segment, then read a small localized range (64
    // consecutive seqs) from a rotating position. before: full scan of the whole
    // segment; after: bounded windowed span read. Eventual durability + wait-last
    // keeps the fill fast and ensures all records are written before reads.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.durability = Durability::Eventual;
    let j = Journal::open(cfg).unwrap();
    let payload = vec![0xABu8; 1024];
    let n: u64 = 16 * 1024; // ~16 MiB of 1 KiB records, one segment
    let mut last = None;
    for i in 1..=n {
        last = Some(j.append(i, i, &payload).unwrap());
    }
    last.unwrap().wait().unwrap();

    let mut k = 0u64;
    c.bench_function("range_read", |b| {
        b.iter(|| {
            // Rotate the 64-wide slice start deterministically across [1, n-64].
            k = k.wrapping_add(2_654_435_761) % (n - 64) + 1;
            let v = j.read_range(black_box(k)..=black_box(k + 63)).unwrap();
            black_box(v);
        });
    });
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

criterion_group!(
    benches,
    bench_append_consistent,
    bench_append_eventual,
    bench_append_batched,
    bench_point_read,
    bench_range_read,
    bench_iter_range
);
criterion_main!(benches);
