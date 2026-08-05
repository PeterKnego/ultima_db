// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//
// Timing decomposition of the eventual-tier YCSB update op. Splits the
// per-op cost into: record construction, raw tree op (cold, in-place),
// MVCC path-clone tax (warm, shared with a snapshot), fixed txn/commit
// bookkeeping, Store install machinery, and the WAL serialize+enqueue.
// Direction-only on the sandbox; single-threaded cells resolve to ±3–17%.
//
// Run: cargo run --release --features persistence --example perf_decomp

#[cfg(feature = "bench-mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::hint::black_box;
use std::time::Instant;

use rand::SeedableRng;
use rand::rngs::StdRng;
use ultima_bench_workloads::ycsb::{
    NUM_RECORDS, OPS_PER_ITER, YcsbRecord, ZIPFIAN_CONSTANT, ZipfianGenerator, bench_disk_dir,
};
use ultima_db::{Durability, Persistence, Store, StoreConfig, Table, WalWrite};

const REPS: usize = 60;
const WARMUP: usize = 10;

fn keys() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(42);
    let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
    (0..OPS_PER_ITER).map(|_| zipf.next(&mut rng)).collect()
}

/// Run `f` REPS times over the burst, report median ns/op.
fn measure(name: &str, keys: &[u64], mut f: impl FnMut(&[u64])) -> f64 {
    let mut samples = Vec::with_capacity(REPS);
    for i in 0..(REPS + WARMUP) {
        let t = Instant::now();
        f(keys);
        let ns = t.elapsed().as_nanos() as f64 / keys.len() as f64;
        if i >= WARMUP {
            samples.push(ns);
        }
    }
    samples.sort_by(f64::total_cmp);
    let med = samples[samples.len() / 2];
    let p10 = samples[samples.len() / 10];
    let p90 = samples[samples.len() * 9 / 10];
    println!("{name:<24} median {med:8.0} ns/op   (p10 {p10:8.0}  p90 {p90:8.0})");
    med
}

fn preload_table() -> Table<YcsbRecord> {
    let mut t = Table::new();
    for i in 1..=NUM_RECORDS {
        t.insert(YcsbRecord::new(i)).unwrap();
    }
    t
}

fn make_store(persistence: Persistence) -> Store {
    let store = Store::new(
        StoreConfig::builder()
            .num_snapshots_retained(2)
            .auto_snapshot_gc(true)
            .persistence(persistence)
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

fn main() {
    let keys = keys();
    println!(
        "YCSB update-op decomposition — {} ops/burst, {} reps, 10k records, zipfian",
        OPS_PER_ITER, REPS
    );

    // (1) Harness record construction only.
    let c_record = measure("record_build", &keys, |ks| {
        for &k in ks {
            black_box(YcsbRecord::new(k.wrapping_add(1)));
        }
    });

    // (2) Raw table update, uniquely owned tree -> make_mut in place.
    let mut cold = preload_table();
    let c_cold = measure("table_cold", &keys, |ks| {
        for &k in ks {
            cold.update(&k, YcsbRecord::new(k.wrapping_add(1))).unwrap();
        }
    });

    // (3) Raw table update with a live O(1) clone refreshed per op —
    // every update path-clones, exactly like sharing with the latest snapshot.
    let mut warm = preload_table();
    let c_warm = measure("table_warm", &keys, |ks| {
        for &k in ks {
            let snap = warm.clone();
            warm.update(&k, YcsbRecord::new(k.wrapping_add(1))).unwrap();
            black_box(&snap);
        }
    });

    // (4) Empty txn: begin_write + open_table + commit, no data op.
    let store_none = make_store(Persistence::None);
    let c_empty = measure("store_none_empty_txn", &keys, |ks| {
        for _ in ks {
            let mut wtx = store_none.begin_write(None).unwrap();
            let _ = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
            wtx.commit().unwrap();
        }
    });

    // (5) Full store update op, no persistence.
    let c_none = measure("store_none_update", &keys, |ks| {
        for &k in ks {
            let mut wtx = store_none.begin_write(None).unwrap();
            let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
            let _ = table.update(k, YcsbRecord::new(k.wrapping_add(1)));
            wtx.commit().unwrap();
        }
    });
    drop(store_none);

    // (6) Full store update op, Eventual + Coalesced WAL on real disk.
    let dir = tempfile::tempdir_in(bench_disk_dir()).unwrap();
    let store_ev = make_store(Persistence::standalone(
        dir.path().to_path_buf(),
        Durability::Eventual,
        WalWrite::Coalesced,
    ));
    let c_ev = measure("store_eventual_update", &keys, |ks| {
        for &k in ks {
            let mut wtx = store_ev.begin_write(None).unwrap();
            let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
            let _ = table.update(k, YcsbRecord::new(k.wrapping_add(1)));
            wtx.commit().unwrap();
        }
    });
    drop(store_ev);

    // (7) Same as (6) with the overlay disabled — isolates the overlay's effect.
    unsafe { std::env::set_var("ULTIMA_OVERLAY_CAP", "0") };
    let dir2 = tempfile::tempdir_in(bench_disk_dir()).unwrap();
    let store_no_ov = make_store(Persistence::standalone(
        dir2.path().to_path_buf(),
        Durability::Eventual,
        WalWrite::Coalesced,
    ));
    let c_no_ov = measure("store_eventual_no_overlay", &keys, |ks| {
        for &k in ks {
            let mut wtx = store_no_ov.begin_write(None).unwrap();
            let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
            let _ = table.update(k, YcsbRecord::new(k.wrapping_add(1)));
            wtx.commit().unwrap();
        }
    });
    unsafe { std::env::remove_var("ULTIMA_OVERLAY_CAP") };
    drop(store_no_ov);
    println!("  overlay effect (no_ov - ov) {:8.0}", c_no_ov - c_ev);

    // Sub-decompose the WAL bucket: serialize vs channel-send-to-parked-thread.
    let c_ser = measure("wal_serialize_only", &keys, |ks| {
        for &k in ks {
            let rec = YcsbRecord::new(k.wrapping_add(1));
            black_box(
                bincode::serde::encode_to_vec(&rec, bincode::config::standard()).unwrap(),
            );
        }
    });

    // Receiver bumps `processed` after each message and parks again in recv();
    // the sender waits for the drain before the next send, so every send hits
    // a parked receiver — the same rhythm as serial one-op commits. Only the
    // send calls are timed.
    let processed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
    let sink = {
        let processed = std::sync::Arc::clone(&processed);
        std::thread::spawn(move || {
            while let Ok(v) = rx.recv() {
                black_box(v.len());
                processed.fetch_add(1, std::sync::atomic::Ordering::Release);
            }
        })
    };
    let mut sent = 0u64;
    let mut send_samples = Vec::with_capacity(REPS);
    for i in 0..(REPS + WARMUP) {
        let mut total_ns = 0u128;
        for _ in &keys {
            let payload = vec![0u8; 1024];
            let t = Instant::now();
            tx.send(payload).unwrap();
            total_ns += t.elapsed().as_nanos();
            sent += 1;
            while processed.load(std::sync::atomic::Ordering::Acquire) < sent {
                std::hint::spin_loop();
            }
        }
        if i >= WARMUP {
            send_samples.push(total_ns as f64 / keys.len() as f64);
        }
    }
    send_samples.sort_by(f64::total_cmp);
    let c_send = send_samples[send_samples.len() / 2];
    println!("{:<24} median {c_send:8.0} ns/op   (send-to-parked-receiver only)", "mpsc_send_parked");
    drop(tx);
    let _ = sink.join();

    println!("\nDerived shares (medians, ns/op):");
    println!("  record construction        {:8.0}", c_record);
    println!("  tree op floor (cold)       {:8.0}", c_cold - c_record);
    println!("  MVCC path-clone tax        {:8.0}", c_warm - c_cold);
    println!("  txn+commit bookkeeping     {:8.0}", c_empty);
    println!("  store install residual     {:8.0}", c_none - c_warm - c_empty);
    println!("  WAL serialize+enqueue      {:8.0}", c_ev - c_none);
    println!("  TOTAL (eventual update)    {:8.0}", c_ev);
    println!("\nWAL bucket sub-decomposition (medians, ns/op):");
    println!("  bincode record serialize   {:8.0}", c_ser - c_record);
    println!("  mpsc send to parked rx     {:8.0}", c_send);
    println!(
        "  unexplained WAL residual   {:8.0}",
        (c_ev - c_none) - (c_ser - c_record) - c_send
    );
}
