//! SSI cost measurement: smallbank-style hot-key contention at N=16,
//! same workload run twice (SI vs Serializable), throughput compared.
//!
//! Usage: cargo run --example ssi_cost --release

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
#[allow(unused_imports)]
use rand::{Rng, RngExt, SeedableRng};
use ultima_db::{IndexKind, IsolationLevel, Store, StoreConfig, WriterMode};

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
    // Cheap approximation: bias towards low IDs in 1..=10.
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
                while dest == source {
                    dest = hot_zipf(rng);
                }
                Op::Amalgamate { source, dest }
            } else if roll < 0.80 {
                Op::WriteCheck(hot_zipf(rng), rng.random_range(1.0..1000.0))
            } else {
                let source = hot_zipf(rng);
                let mut dest = hot_zipf(rng);
                while dest == source {
                    dest = hot_zipf(rng);
                }
                Op::SendPayment {
                    source,
                    dest,
                    amount: rng.random_range(1.0..500.0),
                }
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
                } else {
                    0.0
                };
                drop(s);
                let mut c = wtx.open_table::<Checking>("checking").unwrap();
                if let Some((id, rec)) = c.get_unique::<u64>("customer_id", dest).unwrap() {
                    c.update(id, Checking { balance: rec.balance + amt, ..*rec })?;
                }
            }
            Op::WriteCheck(cid, amount) => {
                let s = wtx.open_table::<Savings>("savings").unwrap();
                let sbal = s
                    .get_unique::<u64>("customer_id", cid)
                    .unwrap()
                    .map(|(_, s)| s.balance)
                    .unwrap_or(0.0);
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
                if let Some((src_id, src_rec)) =
                    c.get_unique::<u64>("customer_id", source).unwrap()
                    && src_rec.balance >= *amount
                    && let Some((dst_id, dst_rec)) =
                        c.get_unique::<u64>("customer_id", dest).unwrap()
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
        .map(|ops| {
            let ops = ops.clone();
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
                            if let Some(w) = wait_for {
                                w.wait();
                            }
                            continue;
                        }
                        Err(ultima_db::Error::SerializationFailure { .. }) => {
                            drop(wtx);
                            retries += 1;
                            // No wait_for: conflicting writer already finished.
                            continue;
                        }
                        Err(e) => panic!("ops err: {e}"),
                    }
                    match wtx.commit() {
                        Ok(_) => return (1u64, retries),
                        Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                            retries += 1;
                            if let Some(w) = wait_for {
                                w.wait();
                            }
                            continue;
                        }
                        Err(ultima_db::Error::SerializationFailure { .. }) => {
                            retries += 1;
                            continue;
                        }
                        Err(e) => panic!("commit err: {e}"),
                    }
                }
            })
        })
        .collect();
    let mut c = 0;
    let mut r = 0;
    for h in handles {
        let (ci, ri) = h.join().unwrap();
        c += ci;
        r += ri;
    }
    (c, r)
}

fn run(label: &str, isolation: IsolationLevel) -> (u64, u64, Duration) {
    let store = Store::new(StoreConfig {
        writer_mode: WriterMode::MultiWriter,
        isolation_level: isolation,
        ..StoreConfig::default()
    })
    .unwrap();

    // Seed: NUM_ACCOUNTS rows in each table with a unique-by-customer_id index.
    {
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut s = wtx.open_table::<Savings>("savings").unwrap();
            s.define_index("customer_id", IndexKind::Unique, |r: &Savings| r.customer_id)
                .unwrap();
            let batch: Vec<Savings> = (1..=NUM_ACCOUNTS)
                .map(|i| Savings { customer_id: i, balance: 10_000.0 })
                .collect();
            s.insert_batch(batch).unwrap();
        }
        {
            let mut c = wtx.open_table::<Checking>("checking").unwrap();
            c.define_index("customer_id", IndexKind::Unique, |r: &Checking| r.customer_id)
                .unwrap();
            let batch: Vec<Checking> = (1..=NUM_ACCOUNTS)
                .map(|i| Checking { customer_id: i, balance: 5_000.0 })
                .collect();
            c.insert_batch(batch).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut total_commits = 0u64;
    let mut total_retries = 0u64;
    let start = Instant::now();
    for burst in 0..NUM_BURSTS {
        let op_sets: Vec<Vec<Op>> = (0..NUM_WRITERS)
            .map(|w| {
                let mut rng = StdRng::seed_from_u64((burst * NUM_WRITERS + w) as u64);
                gen_ops(&mut rng, OPS_PER_WRITER)
            })
            .collect();
        let (c, r) = run_burst(&store, &op_sets);
        total_commits += c;
        total_retries += r;
    }
    let elapsed = start.elapsed();
    black_box((total_commits, total_retries, elapsed));
    let throughput = total_commits as f64 / elapsed.as_secs_f64();
    let retry_ratio = total_retries as f64 / total_commits as f64;
    println!(
        "{label}: {} commits in {:.2?} ({:.0} commits/s), {} retries (ratio {:.2})",
        total_commits, elapsed, throughput, total_retries, retry_ratio
    );
    (total_commits, total_retries, elapsed)
}

fn main() {
    println!(
        "smallbank-style hot-key contention: N={NUM_WRITERS} writers x {OPS_PER_WRITER} ops x {NUM_BURSTS} bursts, hot-keys=10\n"
    );

    let (si_commits, si_retries, si_elapsed) = run("SI ", IsolationLevel::SnapshotIsolation);
    let (ssi_commits, ssi_retries, ssi_elapsed) = run("SSI", IsolationLevel::Serializable);

    let si_throughput = si_commits as f64 / si_elapsed.as_secs_f64();
    let ssi_throughput = ssi_commits as f64 / ssi_elapsed.as_secs_f64();
    let slowdown_pct = (1.0 - ssi_throughput / si_throughput) * 100.0;
    let si_retry_ratio = si_retries as f64 / si_commits as f64;
    let ssi_retry_ratio = ssi_retries as f64 / ssi_commits as f64;

    println!();
    println!("=== Summary ===");
    println!("SI  throughput:  {:>8.0} commits/s  (retry ratio {:.2})", si_throughput, si_retry_ratio);
    println!("SSI throughput:  {:>8.0} commits/s  (retry ratio {:.2})", ssi_throughput, ssi_retry_ratio);
    println!(
        "Slowdown:        {:>8.1}%  (SSI vs SI on smallbank N={NUM_WRITERS}, hot-keys=10, {NUM_BURSTS} bursts)",
        slowdown_pct
    );
}
