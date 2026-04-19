//! Profile where MultiWriter commit time goes at N=16.
//!
//! Runs the smallbank contention workload (hot keys 1..=10) with 16
//! writers for a fixed number of bursts, then dumps per-phase commit
//! time from StoreMetrics. No criterion; just raw wall-clock over a
//! fixed workload.
//!
//! Usage: cargo run --example profile_commit --release --features persistence

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use rand::rngs::StdRng;
#[allow(unused_imports)]
use rand::{Rng, RngExt, SeedableRng};
use ultima_db::{IndexKind, Store, StoreConfig, WriterMode};

const NUM_ACCOUNTS: u64 = 10_000;
const NUM_WRITERS: usize = 16;
const OPS_PER_WRITER: usize = 50;
const NUM_BURSTS: usize = 500;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Savings {
    customer_id: u64,
    balance: f64,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Checking {
    customer_id: u64,
    balance: f64,
}

#[derive(Clone)]
enum Op {
    DepositChecking(u64, f64),
    TransactSavings(u64, f64),
    Amalgamate { source: u64, dest: u64 },
    WriteCheck(u64, f64),
    SendPayment { source: u64, dest: u64, amount: f64 },
}

fn hot_zipf(rng: &mut StdRng) -> u64 {
    // Very cheap approximation: uniform on 1..=10 with bias.
    // Full Zipfian isn't needed — we just want many collisions.
    let r: f64 = rng.random();
    (1 + (r * r * 10.0) as u64).min(10)
}

fn gen_ops(rng: &mut StdRng, n: usize) -> Vec<Op> {
    (0..n)
        .map(|_| {
            let roll: f64 = rng.random();
            if roll < 0.20 {
                Op::DepositChecking(hot_zipf(rng), rng.random_range(1.0..500.0))
            } else if roll < 0.40 {
                Op::TransactSavings(hot_zipf(rng), rng.random_range(1.0..500.0))
            } else if roll < 0.55 {
                let source = hot_zipf(rng);
                let mut dest = hot_zipf(rng);
                while dest == source { dest = hot_zipf(rng); }
                Op::Amalgamate { source, dest }
            } else if roll < 0.80 {
                Op::WriteCheck(hot_zipf(rng), rng.random_range(1.0..1000.0))
            } else {
                let source = hot_zipf(rng);
                let mut dest = hot_zipf(rng);
                while dest == source { dest = hot_zipf(rng); }
                Op::SendPayment { source, dest, amount: rng.random_range(1.0..500.0) }
            }
        })
        .collect()
}

fn apply(wtx: &mut ultima_db::WriteTx, ops: &[Op]) -> Result<(), ultima_db::Error> {
    for op in ops {
        match op {
            Op::DepositChecking(cid, amount) => {
                let mut c = wtx.open_table::<Checking>("checking").unwrap();
                if let Some((id, rec)) = c.get_unique::<u64>("customer_id", cid).unwrap() {
                    let new = Checking { balance: rec.balance + amount, ..*rec };
                    c.update(id, new)?;
                }
            }
            Op::TransactSavings(cid, amount) => {
                let mut s = wtx.open_table::<Savings>("savings").unwrap();
                if let Some((id, rec)) = s.get_unique::<u64>("customer_id", cid).unwrap() {
                    let new = Savings { balance: rec.balance + amount, ..*rec };
                    s.update(id, new)?;
                }
            }
            Op::Amalgamate { source, dest } => {
                let mut s = wtx.open_table::<Savings>("savings").unwrap();
                let amt = if let Some((id, rec)) =
                    s.get_unique::<u64>("customer_id", source).unwrap()
                {
                    let a = rec.balance;
                    s.update(id, Savings { balance: 0.0, ..*rec })?;
                    a
                } else { 0.0 };
                drop(s);
                let mut c = wtx.open_table::<Checking>("checking").unwrap();
                if let Some((id, rec)) = c.get_unique::<u64>("customer_id", dest).unwrap() {
                    c.update(id, Checking { balance: rec.balance + amt, ..*rec })?;
                }
            }
            Op::WriteCheck(cid, amount) => {
                let s = wtx.open_table::<Savings>("savings").unwrap();
                let sbal = s.get_unique::<u64>("customer_id", cid).unwrap()
                    .map(|(_, s)| s.balance).unwrap_or(0.0);
                drop(s);
                let mut c = wtx.open_table::<Checking>("checking").unwrap();
                if let Some((id, rec)) = c.get_unique::<u64>("customer_id", cid).unwrap() {
                    let total = sbal + rec.balance;
                    let penalty = if total < *amount { 1.0 } else { 0.0 };
                    c.update(id, Checking { balance: rec.balance - amount - penalty, ..*rec })?;
                }
            }
            Op::SendPayment { source, dest, amount } => {
                let mut c = wtx.open_table::<Checking>("checking").unwrap();
                if let Some((src_id, src_rec)) = c.get_unique::<u64>("customer_id", source).unwrap()
                    && src_rec.balance >= *amount
                    && let Some((dst_id, dst_rec)) = c.get_unique::<u64>("customer_id", dest).unwrap()
                {
                    let src_new = Checking { balance: src_rec.balance - amount, ..*src_rec };
                    let dst_new = Checking { balance: dst_rec.balance + amount, ..*dst_rec };
                    c.update(src_id, src_new)?;
                    c.update(dst_id, dst_new)?;
                }
            }
        }
    }
    Ok(())
}

fn run_burst(store: &Store, op_sets: &[Vec<Op>]) -> (u64, u64) {
    let barrier = Arc::new(Barrier::new(op_sets.len()));
    let handles: Vec<_> = op_sets
        .iter()
        .cloned()
        .map(|ops| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut retries = 0u64;
                loop {
                    let mut wtx = store.begin_write(None).unwrap();
                    match apply(&mut wtx, &ops) {
                        Ok(()) => {}
                        Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                            drop(wtx);
                            retries += 1;
                            if let Some(w) = wait_for { w.wait(); }
                            continue;
                        }
                        Err(e) => panic!("ops err: {e}"),
                    }
                    match wtx.commit() {
                        Ok(_) => return (1u64, retries),
                        Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                            retries += 1;
                            if let Some(w) = wait_for { w.wait(); }
                            continue;
                        }
                        Err(e) => panic!("commit err: {e}"),
                    }
                }
            })
        })
        .collect();
    let mut c = 0; let mut r = 0;
    for h in handles {
        let (ci, ri) = h.join().unwrap();
        c += ci; r += ri;
    }
    (c, r)
}

fn main() {
    let tmpdir = tempfile::tempdir().unwrap();
    let cfg = StoreConfig {
        num_snapshots_retained: 2,
        auto_snapshot_gc: true,
        writer_mode: WriterMode::MultiWriter,
        persistence: ultima_db::Persistence::Standalone {
            dir: tmpdir.path().to_path_buf(),
            durability: ultima_db::Durability::Eventual,
        },
        ..StoreConfig::default()
    };
    let store = Store::new(cfg).unwrap();
    store.register_table::<Savings>("savings").unwrap();
    store.register_table::<Checking>("checking").unwrap();
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut s = wtx.open_table::<Savings>("savings").unwrap();
        s.define_index("customer_id", IndexKind::Unique, |s: &Savings| s.customer_id).unwrap();
        let b: Vec<_> = (1..=NUM_ACCOUNTS)
            .map(|i| Savings { customer_id: i, balance: 10_000.0 }).collect();
        s.insert_batch(b).unwrap();
    }
    {
        let mut c = wtx.open_table::<Checking>("checking").unwrap();
        c.define_index("customer_id", IndexKind::Unique, |c: &Checking| c.customer_id).unwrap();
        let b: Vec<_> = (1..=NUM_ACCOUNTS)
            .map(|i| Checking { customer_id: i, balance: 10_000.0 }).collect();
        c.insert_batch(b).unwrap();
    }
    wtx.commit().unwrap();

    // Pre-gen all bursts.
    let mut rng = StdRng::seed_from_u64(7777);
    let bursts: Vec<Vec<Vec<Op>>> = (0..NUM_BURSTS)
        .map(|_| {
            (0..NUM_WRITERS).map(|_| gen_ops(&mut rng, OPS_PER_WRITER)).collect()
        })
        .collect();

    // Warmup.
    for i in 0..20 {
        let (_c, _r) = run_burst(&store, &bursts[i]);
    }

    // Baseline metrics *before* measurement window.
    let m0 = store.metrics();

    let wall_start = Instant::now();
    let mut total_committed = 0u64;
    let mut total_retries = 0u64;
    for i in 0..NUM_BURSTS {
        let (c, r) = run_burst(&store, &bursts[i]);
        black_box(c);
        total_committed += c;
        total_retries += r;
    }
    let wall_ns = wall_start.elapsed().as_nanos() as u64;

    let m1 = store.metrics();

    let commits = m1.commits - m0.commits;
    let conflicts = m1.write_conflicts - m0.write_conflicts;
    let rollbacks = m1.rollbacks - m0.rollbacks;
    let p0 = m1.commit_ns_phase0_acquire_locks - m0.commit_ns_phase0_acquire_locks;
    let p1 = m1.commit_ns_phase1_read_validate - m0.commit_ns_phase1_read_validate;
    let p2 = m1.commit_ns_phase2_merge - m0.commit_ns_phase2_merge;
    let p3 = m1.commit_ns_phase3_install - m0.commit_ns_phase3_install;
    let total_phase = p0 + p1 + p2 + p3;

    let total_ops = NUM_BURSTS as u64 * NUM_WRITERS as u64 * OPS_PER_WRITER as u64;
    let throughput = total_ops as f64 / (wall_ns as f64 / 1e9);

    println!("\n=== profile_commit @ N={NUM_WRITERS} x {OPS_PER_WRITER} ops x {NUM_BURSTS} bursts ===");
    println!("wall-clock:        {:>10.3} ms", wall_ns as f64 / 1e6);
    println!("total committed:   {commits} tx (expected {total_committed})");
    println!("total write_confl: {conflicts}");
    println!("total rollbacks:   {rollbacks}");
    println!("total retries:     {total_retries}");
    println!("throughput:        {:>10.0} ops/s", throughput);
    println!();
    println!("--- cumulative commit-phase time (across all writer threads) ---");
    println!("phase 0 (acquire table locks):  {:>8.1} ms  ({:>5.1}%)",
             p0 as f64 / 1e6, 100.0 * p0 as f64 / total_phase as f64);
    println!("phase 1 (read + OCC validate):  {:>8.1} ms  ({:>5.1}%)",
             p1 as f64 / 1e6, 100.0 * p1 as f64 / total_phase as f64);
    println!("phase 2 (merge, lock-free):     {:>8.1} ms  ({:>5.1}%)",
             p2 as f64 / 1e6, 100.0 * p2 as f64 / total_phase as f64);
    println!("phase 3 (global write lock):    {:>8.1} ms  ({:>5.1}%)",
             p3 as f64 / 1e6, 100.0 * p3 as f64 / total_phase as f64);
    println!("sum of phases:                  {:>8.1} ms",
             total_phase as f64 / 1e6);
    println!();
    println!("--- per-commit averages ---");
    let n = commits.max(1);
    println!("phase 0 avg:  {:>7.1} µs", p0 as f64 / 1000.0 / n as f64);
    println!("phase 1 avg:  {:>7.1} µs", p1 as f64 / 1000.0 / n as f64);
    println!("phase 2 avg:  {:>7.1} µs", p2 as f64 / 1000.0 / n as f64);
    println!("phase 3 avg:  {:>7.1} µs", p3 as f64 / 1000.0 / n as f64);
    println!("total avg:    {:>7.1} µs", total_phase as f64 / 1000.0 / n as f64);
}
