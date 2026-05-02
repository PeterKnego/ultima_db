// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

#![allow(clippy::drop_non_drop, clippy::redundant_iter_cloned)]

//! SmallBank contention bench swept over writer counts. Probes whether
//! the intent-map mutex becomes a bottleneck as N grows.
//!
//! Uses the same setup as `smallbank_bench` but runs the high-contention
//! mixed workload (hot keys 1..=10) at N ∈ {2, 4, 8, 16} concurrent
//! writers. OPS_PER_WRITER stays at 50, so the total op count scales
//! linearly with N — throughput normalized over total ops exposes the
//! per-op cost curve.

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::SeedableRng;
use ultima_db::{IndexKind, Store, StoreConfig, WriterMode};

#[path = "smallbank_common.rs"]
mod smallbank_common;
use smallbank_common::*;

const WRITER_COUNTS: &[usize] = &[2, 4, 8, 16];
const OPS_PER_WRITER_LOCAL: usize = 50;
const POOL_SIZE: usize = 64;

fn build_store() -> (Store, tempfile::TempDir) {
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
    store.register_table::<Account>("accounts").unwrap();
    store.register_table::<Savings>("savings").unwrap();
    store.register_table::<Checking>("checking").unwrap();
    {
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut accounts = wtx.open_table::<Account>("accounts").unwrap();
            accounts.define_index("customer_id", IndexKind::Unique, |a: &Account| a.customer_id).unwrap();
            let batch: Vec<Account> = (1..=NUM_ACCOUNTS)
                .map(|i| Account { customer_id: i, name: format!("C{i}") })
                .collect();
            accounts.insert_batch(batch).unwrap();
        }
        {
            let mut s = wtx.open_table::<Savings>("savings").unwrap();
            s.define_index("customer_id", IndexKind::Unique, |s: &Savings| s.customer_id).unwrap();
            let batch: Vec<Savings> = (1..=NUM_ACCOUNTS)
                .map(|i| Savings { customer_id: i, balance: INITIAL_SAVINGS })
                .collect();
            s.insert_batch(batch).unwrap();
        }
        {
            let mut c = wtx.open_table::<Checking>("checking").unwrap();
            c.define_index("customer_id", IndexKind::Unique, |c: &Checking| c.customer_id).unwrap();
            let batch: Vec<Checking> = (1..=NUM_ACCOUNTS)
                .map(|i| Checking { customer_id: i, balance: INITIAL_CHECKING })
                .collect();
            c.insert_batch(batch).unwrap();
        }
        wtx.commit().unwrap();
    }
    (store, tmpdir)
}

fn gen_hot_ops(num_writers: usize, seed: u64) -> Vec<Vec<SmallBankOp>> {
    let hot_zipf = ZipfianGenerator::new(10, ZIPFIAN_CONSTANT);
    let mut rng = StdRng::seed_from_u64(seed);
    (0..num_writers)
        .map(|_| gen_mixed_workload_n(&mut rng, &hot_zipf, OPS_PER_WRITER_LOCAL))
        .collect()
}

fn execute_ops_on_tx(wtx: &mut ultima_db::WriteTx, ops: &[SmallBankOp]) -> Result<(), ultima_db::Error> {
    for op in ops {
        match op {
            SmallBankOp::Balance(_) => {}
            SmallBankOp::DepositChecking(cid, amount) => {
                let mut c = wtx.open_table::<Checking>("checking").unwrap();
                if let Some((id, rec)) = c.get_unique::<u64>("customer_id", cid).unwrap() {
                    let new = Checking { balance: rec.balance + amount, ..*rec };
                    c.update(id, new)?;
                }
            }
            SmallBankOp::TransactSavings(cid, amount) => {
                let mut s = wtx.open_table::<Savings>("savings").unwrap();
                if let Some((id, rec)) = s.get_unique::<u64>("customer_id", cid).unwrap() {
                    let new = Savings { balance: rec.balance + amount, ..*rec };
                    s.update(id, new)?;
                }
            }
            SmallBankOp::Amalgamate { source, dest } => {
                let mut s = wtx.open_table::<Savings>("savings").unwrap();
                let amt = if let Some((id, rec)) = s.get_unique::<u64>("customer_id", source).unwrap() {
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
            SmallBankOp::WriteCheck(cid, amount) => {
                let s = wtx.open_table::<Savings>("savings").unwrap();
                let sbal = s.get_unique::<u64>("customer_id", cid).unwrap().map(|(_, s)| s.balance).unwrap_or(0.0);
                drop(s);
                let mut c = wtx.open_table::<Checking>("checking").unwrap();
                if let Some((id, rec)) = c.get_unique::<u64>("customer_id", cid).unwrap() {
                    let tot = sbal + rec.balance;
                    let penalty = if tot < *amount { 1.0 } else { 0.0 };
                    c.update(id, Checking { balance: rec.balance - amount - penalty, ..*rec })?;
                }
            }
            SmallBankOp::SendPayment { source, dest, amount } => {
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

fn run_burst(store: &Store, op_sets: &[Vec<SmallBankOp>]) {
    let barrier = Arc::new(Barrier::new(op_sets.len()));
    let handles: Vec<_> = op_sets
        .iter()
        .cloned()
        .map(|ops| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                loop {
                    let mut wtx = store.begin_write(None).unwrap();
                    match execute_ops_on_tx(&mut wtx, &ops) {
                        Ok(()) => {}
                        Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                            drop(wtx);
                            if let Some(w) = wait_for { w.wait(); }
                            continue;
                        }
                        Err(e) => panic!("ops error: {e}"),
                    }
                    match wtx.commit() {
                        Ok(_) => return,
                        Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                            if let Some(w) = wait_for { w.wait(); }
                            continue;
                        }
                        Err(e) => panic!("commit error: {e}"),
                    }
                }
            })
        })
        .collect();
    for h in handles { h.join().unwrap(); }
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("smallbank_scaling_high");
    group.sample_size(20).measurement_time(Duration::from_secs(10));
    for &n in WRITER_COUNTS {
        let total_ops = (n * OPS_PER_WRITER_LOCAL) as u64;
        group.throughput(Throughput::Elements(total_ops));
        // Pre-generate a pool of bursts per writer count so each iter
        // grabs a fresh one.
        let pool: Vec<Vec<Vec<SmallBankOp>>> = (0..POOL_SIZE)
            .map(|i| gen_hot_ops(n, 500 + i as u64))
            .collect();
        let (store, _tmp) = build_store();
        let cursor = std::sync::atomic::AtomicUsize::new(0);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched_ref(
                || {
                    let idx = cursor.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % pool.len();
                    pool[idx].clone()
                },
                |op_sets| {
                    black_box(run_burst(&store, op_sets));
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(scaling, bench);
criterion_main!(scaling);
