#![allow(clippy::redundant_iter_cloned)]

//! SmallBank benchmark for RocksDB using OptimisticTransactionDB.
//!
//! Three tables (`accounts`, `savings`, `checking`) are modeled as column
//! families. Each customer's records live at key = `customer_id.to_be_bytes()`
//! in each CF. Secondary index by customer_id is implicit (key == customer_id).
//!
//! Concurrent burst writers each own an `Arc<OptimisticTransactionDB>` clone
//! and run on their own OS thread; optimistic conflicts drive retry.

use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{Criterion, criterion_group, criterion_main};
use rocksdb::{
    ColumnFamilyDescriptor, OptimisticTransactionDB, Options, SingleThreaded,
};
use tempfile::TempDir;

#[path = "smallbank_common.rs"]
mod smallbank_common;
use smallbank_common::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();
const CF_ACCOUNTS: &str = "accounts";
const CF_SAVINGS: &str = "savings";
const CF_CHECKING: &str = "checking";

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

// ---------------------------------------------------------------------------
// RocksDB engine
// ---------------------------------------------------------------------------

struct RocksDbEngine {
    db: Arc<OptimisticTransactionDB<SingleThreaded>>,
    _tmpdir: TempDir,
}

impl RocksDbEngine {
    fn preload() -> Self {
        let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_write_buffer_size(256 * 1024 * 1024);
        opts.set_disable_auto_compactions(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_ACCOUNTS, Options::default()),
            ColumnFamilyDescriptor::new(CF_SAVINGS, Options::default()),
            ColumnFamilyDescriptor::new(CF_CHECKING, Options::default()),
        ];
        let db: OptimisticTransactionDB<SingleThreaded> =
            OptimisticTransactionDB::open_cf_descriptors(&opts, tmpdir.path(), cfs)
                .expect("failed to open rocksdb");

        {
            let cf_acc = db.cf_handle(CF_ACCOUNTS).unwrap();
            let cf_sav = db.cf_handle(CF_SAVINGS).unwrap();
            let cf_chk = db.cf_handle(CF_CHECKING).unwrap();
            let mut wo = rocksdb::WriteOptions::default();
            wo.disable_wal(true);
            for i in 1..=NUM_ACCOUNTS {
                let k = encode_key(i);
                let a = Account {
                    customer_id: i,
                    name: format!("Customer_{i}"),
                };
                let s = Savings { customer_id: i, balance: INITIAL_SAVINGS };
                let c = Checking { customer_id: i, balance: INITIAL_CHECKING };
                db.put_cf_opt(
                    &cf_acc,
                    k,
                    bincode::serde::encode_to_vec(&a, BINCODE_CFG).unwrap(),
                    &wo,
                )
                .unwrap();
                db.put_cf_opt(
                    &cf_sav,
                    k,
                    bincode::serde::encode_to_vec(&s, BINCODE_CFG).unwrap(),
                    &wo,
                )
                .unwrap();
                db.put_cf_opt(
                    &cf_chk,
                    k,
                    bincode::serde::encode_to_vec(&c, BINCODE_CFG).unwrap(),
                    &wo,
                )
                .unwrap();
            }
        }

        Self { db: Arc::new(db), _tmpdir: tmpdir }
    }

    fn get_savings(
        db: &OptimisticTransactionDB<SingleThreaded>,
        cid: u64,
    ) -> Option<Savings> {
        let cf = db.cf_handle(CF_SAVINGS).unwrap();
        db.get_cf(&cf, encode_key(cid))
            .unwrap()
            .map(|bytes| bincode::serde::decode_from_slice(&bytes, BINCODE_CFG).unwrap().0)
    }

    fn get_checking(
        db: &OptimisticTransactionDB<SingleThreaded>,
        cid: u64,
    ) -> Option<Checking> {
        let cf = db.cf_handle(CF_CHECKING).unwrap();
        db.get_cf(&cf, encode_key(cid))
            .unwrap()
            .map(|bytes| bincode::serde::decode_from_slice(&bytes, BINCODE_CFG).unwrap().0)
    }

    /// Apply one op on the shared db using a fresh small transaction per op.
    /// Used by the single-writer sequential path.
    fn execute_single_op(db: &OptimisticTransactionDB<SingleThreaded>, op: &SmallBankOp) {
        match op {
            SmallBankOp::Balance(cid) => {
                let _ = Self::get_savings(db, *cid).map(|s| s.balance);
                let _ = Self::get_checking(db, *cid).map(|c| c.balance);
            }
            _ => {
                // Wrap all mutating ops in an OCC transaction (retry on conflict).
                loop {
                    let txn = db.transaction();
                    apply_op_in_txn(db, &txn, op);
                    match txn.commit() {
                        Ok(_) => break,
                        Err(_) => continue,
                    }
                }
            }
        }
    }
}

fn apply_op_in_txn(
    db: &OptimisticTransactionDB<SingleThreaded>,
    txn: &rocksdb::Transaction<'_, OptimisticTransactionDB<SingleThreaded>>,
    op: &SmallBankOp,
) {
    let cf_sav = db.cf_handle(CF_SAVINGS).unwrap();
    let cf_chk = db.cf_handle(CF_CHECKING).unwrap();

    let get_sav = |cid: u64| -> Option<Savings> {
        txn.get_cf(&cf_sav, encode_key(cid))
            .unwrap()
            .map(|b| bincode::serde::decode_from_slice(&b, BINCODE_CFG).unwrap().0)
    };
    let get_chk = |cid: u64| -> Option<Checking> {
        txn.get_cf(&cf_chk, encode_key(cid))
            .unwrap()
            .map(|b| bincode::serde::decode_from_slice(&b, BINCODE_CFG).unwrap().0)
    };
    let put_sav = |cid: u64, s: &Savings| {
        txn.put_cf(
            &cf_sav,
            encode_key(cid),
            bincode::serde::encode_to_vec(s, BINCODE_CFG).unwrap(),
        )
        .unwrap();
    };
    let put_chk = |cid: u64, c: &Checking| {
        txn.put_cf(
            &cf_chk,
            encode_key(cid),
            bincode::serde::encode_to_vec(c, BINCODE_CFG).unwrap(),
        )
        .unwrap();
    };

    match op {
        SmallBankOp::Balance(_) => {}
        SmallBankOp::DepositChecking(cid, amount) => {
            if let Some(mut c) = get_chk(*cid) {
                c.balance += amount;
                put_chk(*cid, &c);
            }
        }
        SmallBankOp::TransactSavings(cid, amount) => {
            if let Some(mut s) = get_sav(*cid) {
                s.balance += amount;
                put_sav(*cid, &s);
            }
        }
        SmallBankOp::Amalgamate { source, dest } => {
            let src_amt = if let Some(mut s) = get_sav(*source) {
                let amt = s.balance;
                s.balance = 0.0;
                put_sav(*source, &s);
                amt
            } else {
                0.0
            };
            if let Some(mut c) = get_chk(*dest) {
                c.balance += src_amt;
                put_chk(*dest, &c);
            }
        }
        SmallBankOp::WriteCheck(cid, amount) => {
            let sbal = get_sav(*cid).map(|s| s.balance).unwrap_or(0.0);
            if let Some(mut c) = get_chk(*cid) {
                let total = sbal + c.balance;
                let penalty = if total < *amount { 1.0 } else { 0.0 };
                c.balance -= amount + penalty;
                put_chk(*cid, &c);
            }
        }
        SmallBankOp::SendPayment { source, dest, amount } => {
            if let Some(mut src) = get_chk(*source)
                && src.balance >= *amount
                && let Some(mut dst) = get_chk(*dest)
            {
                src.balance -= amount;
                dst.balance += amount;
                put_chk(*source, &src);
                put_chk(*dest, &dst);
            }
        }
    }
}

impl SmallBankEngine for RocksDbEngine {
    fn name(&self) -> &str {
        "rocksdb"
    }

    fn execute(&mut self, ops: &[SmallBankOp]) {
        for op in ops {
            Self::execute_single_op(&self.db, op);
        }
    }

    fn execute_burst(&mut self, op_sets: &[Vec<SmallBankOp>]) -> BurstResult {
        let barrier = Arc::new(Barrier::new(op_sets.len()));
        let handles: Vec<_> = op_sets
            .iter()
            .cloned()
            .map(|ops| {
                let db = Arc::clone(&self.db);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut retries = 0u64;
                    loop {
                        let txn = db.transaction();
                        for op in &ops {
                            apply_op_in_txn(&db, &txn, op);
                        }
                        match txn.commit() {
                            Ok(_) => return (1u64, retries),
                            Err(_) => {
                                retries += 1;
                                continue;
                            }
                        }
                    }
                })
            })
            .collect();

        let mut committed = 0u64;
        let mut aborted = 0u64;
        for h in handles {
            let (c, r) = h.join().unwrap();
            committed += c;
            aborted += r;
        }
        BurstResult { committed, aborted }
    }
}

// ---------------------------------------------------------------------------
// Criterion entry
// ---------------------------------------------------------------------------

fn bench_smallbank(c: &mut Criterion) {
    let mut engine = RocksDbEngine::preload();
    bench_workloads(c, &mut engine);
    bench_contention(c, &mut engine);
}

criterion_group! {
    name = smallbank_rocksdb;
    config = smallbank_criterion();
    targets = bench_smallbank
}
criterion_main!(smallbank_rocksdb);
