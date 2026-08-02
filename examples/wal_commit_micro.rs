// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//
// Isolates the committer-side WAL cost of one eventual-mode single-op commit:
// key encode + 1KB record serialize + WalOp/WalEntry construction + enqueue.
// Compare against the `WAL serialize+enqueue` bucket from perf_decomp.
//
// Run: cargo run --release --features "persistence bench-internals" --example wal_commit_micro

#[cfg(feature = "bench-internals")]
fn main() {
    use std::hint::black_box;
    use std::time::Instant;
    use ultima_bench_workloads::ycsb::{OPS_PER_ITER, YcsbRecord, bench_disk_dir};
    use ultima_db::wal::{BenchWal, WalEntry, WalOp, WalSinkKind};

    const REPS: usize = 60;
    const WARMUP: usize = 10;

    let dir = tempfile::tempdir_in(bench_disk_dir()).unwrap();
    let wal = BenchWal::new(dir.path(), false, WalSinkKind::Coalesced).unwrap();
    let table = String::from("ycsb");

    let mut samples = Vec::new();
    for i in 0..(REPS + WARMUP) {
        let t = Instant::now();
        for k in 0..OPS_PER_ITER as u64 {
            // Mirror TableWriter::update's WAL work per op...
            let key = ultima_db::PrimaryKey::encode(&(k + 1));
            let data =
                bincode::serde::encode_to_vec(YcsbRecord::new(k + 1), bincode::config::standard())
                    .unwrap();
            let op = WalOp::Update {
                table: table.clone(),
                key_type: <u64 as ultima_db::PrimaryKey>::KEY_TYPE_ID,
                key,
                data,
            };
            // ...and commit's entry build + enqueue.
            let entry = WalEntry {
                version: k + 2,
                ops: vec![op],
            };
            wal.commit_eventual(entry).unwrap();
            black_box(());
        }
        let ns = t.elapsed().as_nanos() as f64 / OPS_PER_ITER as f64;
        if i >= WARMUP {
            samples.push(ns);
        }
    }
    samples.sort_by(f64::total_cmp);
    println!(
        "wal committer-side per op: median {:.0} ns (p10 {:.0} p90 {:.0})",
        samples[samples.len() / 2],
        samples[samples.len() / 10],
        samples[samples.len() * 9 / 10]
    );
}

#[cfg(not(feature = "bench-internals"))]
fn main() {
    eprintln!("rebuild with --features \"persistence bench-internals\"");
}
