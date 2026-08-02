// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//
// Paired A/B of the WAL recv-spin: two Eventual stores whose WAL threads were
// spawned with the spin disabled (A) and enabled (B), exercised in interleaved
// bursts so machine drift cancels. Reports per-burst paired deltas.
//
// Run: cargo run --release --features persistence --example wal_spin_ab

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

    // The env var is read once at WAL-thread spawn, so each store freezes
    // the setting that was live when it was constructed.
    unsafe { std::env::set_var("ULTIMA_WAL_RECV_SPIN_US", "0") };
    let store_off = make_store(dir_a.path());
    unsafe { std::env::set_var("ULTIMA_WAL_RECV_SPIN_US", "30") };
    let store_on = make_store(dir_b.path());

    let mut off = Vec::new();
    let mut on = Vec::new();
    let mut deltas = Vec::new();
    for i in 0..(REPS + WARMUP) {
        // Alternate which arm goes first within the pair.
        let (a, b) = if i % 2 == 0 {
            let a = burst(&store_off, &keys);
            let b = burst(&store_on, &keys);
            (a, b)
        } else {
            let b = burst(&store_on, &keys);
            let a = burst(&store_off, &keys);
            (a, b)
        };
        if i >= WARMUP {
            off.push(a);
            on.push(b);
            deltas.push(b - a);
        }
    }
    for v in [&mut off, &mut on, &mut deltas] {
        v.sort_by(f64::total_cmp);
    }
    let med = |v: &Vec<f64>| v[v.len() / 2];
    println!("eventual update, ns/op ({} paired bursts):", REPS);
    println!("  spin OFF  median {:8.0}", med(&off));
    println!("  spin ON   median {:8.0}", med(&on));
    println!(
        "  paired delta (on-off) median {:8.0}  p10 {:8.0}  p90 {:8.0}",
        med(&deltas),
        deltas[deltas.len() / 10],
        deltas[deltas.len() * 9 / 10]
    );
    println!(
        "  => spin ON is {:+.1}% vs OFF",
        100.0 * med(&deltas) / med(&off)
    );
    drop(store_off);
    drop(store_on);

    // Second A/B: Coalesced vs CoalescedPrealloc under Eventual.
    let dir_c = tempfile::tempdir_in(&base).unwrap();
    let dir_d = tempfile::tempdir_in(&base).unwrap();
    let store_c = make_store_wal(dir_c.path(), WalWrite::Coalesced);
    let store_d = make_store_wal(dir_d.path(), WalWrite::CoalescedPrealloc);
    paired_ab("coalesced", &store_c, "prealloc", &store_d, &keys);
}
