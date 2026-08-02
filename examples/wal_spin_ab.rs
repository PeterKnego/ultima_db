// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//
// Paired A/B harness for WAL configs: two Eventual stores exercised in
// interleaved bursts so machine drift cancels; reports per-burst paired
// deltas. A/B #1 compares two identical stores — a live noise-floor check
// that calibrates how small a real effect this harness can resolve. A/B #2
// compares Coalesced vs CoalescedPrealloc.
//
// (This file started as the recv-spin A/B; the spin was refuted on the NVMe
// fleet host — see docs/benchmarks/ycsb-eventual-write-decomposition-2026-08-02.md
// — and removed, but the paired harness earned its keep.)
//
// Run: cargo run --release --features persistence --example wal_spin_ab

#[cfg(feature = "bench-mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::time::Instant;

use rand::SeedableRng;
use rand::rngs::StdRng;
use ultima_bench_workloads::ycsb::{
    NUM_RECORDS, OPS_PER_ITER, YcsbRecord, ZIPFIAN_CONSTANT, ZipfianGenerator, bench_disk_dir,
};
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};

const REPS: usize = 40;
const WARMUP: usize = 8;

fn make_store_wal(dir: &std::path::Path, wal_write: WalWrite) -> Store {
    let store = Store::new(
        StoreConfig::builder()
            .num_snapshots_retained(2)
            .auto_snapshot_gc(true)
            .persistence(Persistence::standalone(
                dir.to_path_buf(),
                Durability::Eventual,
                wal_write,
            ))
            .build(),
    )
    .unwrap();
    store.register_table::<YcsbRecord>("ycsb").unwrap();
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

fn paired_ab(name_a: &str, store_a: &Store, name_b: &str, store_b: &Store, keys: &[u64]) {
    let mut a_s = Vec::new();
    let mut b_s = Vec::new();
    let mut deltas = Vec::new();
    for i in 0..(REPS + WARMUP) {
        let (a, b) = if i % 2 == 0 {
            let a = burst(store_a, keys);
            let b = burst(store_b, keys);
            (a, b)
        } else {
            let b = burst(store_b, keys);
            let a = burst(store_a, keys);
            (a, b)
        };
        if i >= WARMUP {
            a_s.push(a);
            b_s.push(b);
            deltas.push(b - a);
        }
    }
    for v in [&mut a_s, &mut b_s, &mut deltas] {
        v.sort_by(f64::total_cmp);
    }
    let med = |v: &Vec<f64>| v[v.len() / 2];
    println!("{name_a} vs {name_b}, eventual update ns/op ({REPS} paired bursts):");
    println!("  {name_a:<12} median {:8.0}", med(&a_s));
    println!("  {name_b:<12} median {:8.0}", med(&b_s));
    println!(
        "  paired delta (b-a) median {:8.0}  p10 {:8.0}  p90 {:8.0}  => {:+.1}%",
        med(&deltas),
        deltas[deltas.len() / 10],
        deltas[deltas.len() * 9 / 10],
        100.0 * med(&deltas) / med(&a_s)
    );
}

fn make_store(dir: &std::path::Path) -> Store {
    let store = Store::new(
        StoreConfig::builder()
            .num_snapshots_retained(2)
            .auto_snapshot_gc(true)
            .persistence(Persistence::standalone(
                dir.to_path_buf(),
                Durability::Eventual,
                WalWrite::Coalesced,
            ))
            .build(),
    )
    .unwrap();
    store.register_table::<YcsbRecord>("ycsb").unwrap();
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

fn burst(store: &Store, keys: &[u64]) -> f64 {
    let t = Instant::now();
    for &k in keys {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
        let _ = table.update(k, YcsbRecord::new(k.wrapping_add(1)));
        wtx.commit().unwrap();
    }
    t.elapsed().as_nanos() as f64 / keys.len() as f64
}

fn main() {
    let mut rng = StdRng::seed_from_u64(42);
    let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
    let keys: Vec<u64> = (0..OPS_PER_ITER).map(|_| zipf.next(&mut rng)).collect();

    let base = bench_disk_dir();
    let dir_a = tempfile::tempdir_in(&base).unwrap();
    let dir_b = tempfile::tempdir_in(&base).unwrap();

    // Noise floor: two identical stores. Any "effect" here is harness noise.
    let store_x = make_store(dir_a.path());
    let store_y = make_store(dir_b.path());
    paired_ab("null-a", &store_x, "null-b", &store_y, &keys);
    drop(store_x);
    drop(store_y);

    // Second A/B: Coalesced vs CoalescedPrealloc under Eventual.
    let dir_c = tempfile::tempdir_in(&base).unwrap();
    let dir_d = tempfile::tempdir_in(&base).unwrap();
    let store_c = make_store_wal(dir_c.path(), WalWrite::Coalesced);
    let store_d = make_store_wal(dir_d.path(), WalWrite::CoalescedPrealloc);
    paired_ab("coalesced", &store_c, "prealloc", &store_d, &keys);
}
