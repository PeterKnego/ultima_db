#![allow(
    clippy::needless_borrows_for_generic_args,
    clippy::unnecessary_to_owned,
    clippy::redundant_iter_cloned
)]

//! SmallBank benchmark for Fjall using OptimisticTxDatabase.
//!
//! Three tables (`accounts`, `savings`, `checking`) are modeled as three
//! keyspaces. Key = `customer_id.to_be_bytes()` in each. Secondary index by
//! customer_id is implicit (key == customer_id).
//!
//! Concurrent burst writers share Arc-wrapped db+keyspaces; each thread owns
//! its own transaction and retries on conflict.

use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{Criterion, criterion_group, criterion_main};
use fjall::{
    KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx, Readable,
};
use tempfile::TempDir;

#[path = "smallbank_common.rs"]
mod smallbank_common;
use smallbank_common::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

// ---------------------------------------------------------------------------
// Fjall engine
// ---------------------------------------------------------------------------

struct FjallEngine {
    db: Arc<OptimisticTxDatabase>,
    #[allow(dead_code)]
    ks_accounts: Arc<OptimisticTxKeyspace>,
    ks_savings: Arc<OptimisticTxKeyspace>,
    ks_checking: Arc<OptimisticTxKeyspace>,
    _tmpdir: TempDir,
}

impl FjallEngine {
    fn preload() -> Self {
        let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
        let db = OptimisticTxDatabase::builder(tmpdir.path())
            .open()
            .expect("failed to open fjall database");
        let ks_accounts = db
            .keyspace("accounts", KeyspaceCreateOptions::default)
            .expect("accounts keyspace");
        let ks_savings = db
            .keyspace("savings", KeyspaceCreateOptions::default)
            .expect("savings keyspace");
        let ks_checking = db
            .keyspace("checking", KeyspaceCreateOptions::default)
            .expect("checking keyspace");

        for i in 1..=NUM_ACCOUNTS {
            let k = encode_key(i);
            let a = Account { customer_id: i, name: format!("Customer_{i}") };
            let s = Savings { customer_id: i, balance: INITIAL_SAVINGS };
            let c = Checking { customer_id: i, balance: INITIAL_CHECKING };
            ks_accounts
                .insert(k, bincode::serde::encode_to_vec(&a, BINCODE_CFG).unwrap())
                .unwrap();
            ks_savings
                .insert(k, bincode::serde::encode_to_vec(&s, BINCODE_CFG).unwrap())
                .unwrap();
            ks_checking
                .insert(k, bincode::serde::encode_to_vec(&c, BINCODE_CFG).unwrap())
                .unwrap();
        }

        Self {
            db: Arc::new(db),
            ks_accounts: Arc::new(ks_accounts),
            ks_savings: Arc::new(ks_savings),
            ks_checking: Arc::new(ks_checking),
            _tmpdir: tmpdir,
        }
    }
}

fn apply_op_in_txn(
    txn: &mut OptimisticWriteTx,
    ks_savings: &OptimisticTxKeyspace,
    ks_checking: &OptimisticTxKeyspace,
    op: &SmallBankOp,
) {
    let decode_sav = |b: &[u8]| -> Savings {
        bincode::serde::decode_from_slice(b, BINCODE_CFG).unwrap().0
    };
    let decode_chk = |b: &[u8]| -> Checking {
        bincode::serde::decode_from_slice(b, BINCODE_CFG).unwrap().0
    };

    match op {
        SmallBankOp::Balance(_) => {
            // Read-only ops aren't issued inside write transactions in the
            // burst path (each op is bundled with writes). This arm is kept
            // for completeness.
        }
        SmallBankOp::DepositChecking(cid, amount) => {
            let k = encode_key(*cid);
            if let Some(bytes) = txn.get(ks_checking, &k).unwrap() {
                let mut c = decode_chk(&bytes);
                c.balance += amount;
                txn.insert(
                    ks_checking,
                    k,
                    bincode::serde::encode_to_vec(&c, BINCODE_CFG).unwrap(),
                );
            }
        }
        SmallBankOp::TransactSavings(cid, amount) => {
            let k = encode_key(*cid);
            if let Some(bytes) = txn.get(ks_savings, &k).unwrap() {
                let mut s = decode_sav(&bytes);
                s.balance += amount;
                txn.insert(
                    ks_savings,
                    k,
                    bincode::serde::encode_to_vec(&s, BINCODE_CFG).unwrap(),
                );
            }
        }
        SmallBankOp::Amalgamate { source, dest } => {
            let src_k = encode_key(*source);
            let src_amt = if let Some(bytes) = txn.get(ks_savings, &src_k).unwrap() {
                let mut s = decode_sav(&bytes);
                let amt = s.balance;
                s.balance = 0.0;
                txn.insert(
                    ks_savings,
                    src_k,
                    bincode::serde::encode_to_vec(&s, BINCODE_CFG).unwrap(),
                );
                amt
            } else {
                0.0
            };
            let dst_k = encode_key(*dest);
            if let Some(bytes) = txn.get(ks_checking, &dst_k).unwrap() {
                let mut c = decode_chk(&bytes);
                c.balance += src_amt;
                txn.insert(
                    ks_checking,
                    dst_k,
                    bincode::serde::encode_to_vec(&c, BINCODE_CFG).unwrap(),
                );
            }
        }
        SmallBankOp::WriteCheck(cid, amount) => {
            let k = encode_key(*cid);
            let sbal = txn
                .get(ks_savings, &k)
                .unwrap()
                .map(|b| decode_sav(&b).balance)
                .unwrap_or(0.0);
            if let Some(bytes) = txn.get(ks_checking, &k).unwrap() {
                let mut c = decode_chk(&bytes);
                let total = sbal + c.balance;
                let penalty = if total < *amount { 1.0 } else { 0.0 };
                c.balance -= amount + penalty;
                txn.insert(
                    ks_checking,
                    k,
                    bincode::serde::encode_to_vec(&c, BINCODE_CFG).unwrap(),
                );
            }
        }
        SmallBankOp::SendPayment { source, dest, amount } => {
            let src_k = encode_key(*source);
            let dst_k = encode_key(*dest);
            if let Some(src_bytes) = txn.get(ks_checking, &src_k).unwrap() {
                let mut src = decode_chk(&src_bytes);
                if src.balance >= *amount
                    && let Some(dst_bytes) = txn.get(ks_checking, &dst_k).unwrap()
                {
                    let mut dst = decode_chk(&dst_bytes);
                    src.balance -= amount;
                    dst.balance += amount;
                    txn.insert(
                        ks_checking,
                        src_k,
                        bincode::serde::encode_to_vec(&src, BINCODE_CFG).unwrap(),
                    );
                    txn.insert(
                        ks_checking,
                        dst_k,
                        bincode::serde::encode_to_vec(&dst, BINCODE_CFG).unwrap(),
                    );
                }
            }
        }
    }
}

impl SmallBankEngine for FjallEngine {
    fn name(&self) -> &str {
        "fjall"
    }

    fn verify(&self) -> StateHash {
        let mut accounts: std::collections::BTreeMap<u64, AccountState> =
            std::collections::BTreeMap::new();
        for guard in self.ks_savings.inner().iter() {
            let v = guard.value().unwrap();
            let s: Savings = bincode::serde::decode_from_slice(&v, BINCODE_CFG).unwrap().0;
            accounts.entry(s.customer_id).or_default().savings = s.balance;
        }
        for guard in self.ks_checking.inner().iter() {
            let v = guard.value().unwrap();
            let c: Checking = bincode::serde::decode_from_slice(&v, BINCODE_CFG).unwrap().0;
            accounts.entry(c.customer_id).or_default().checking = c.balance;
        }
        hash_accounts(accounts)
    }

    fn execute(&mut self, ops: &[SmallBankOp]) {
        for op in ops {
            match op {
                SmallBankOp::Balance(cid) => {
                    let k = encode_key(*cid);
                    let _ = self.ks_savings.get(k).unwrap();
                    let _ = self.ks_checking.get(k).unwrap();
                }
                _ => loop {
                    let mut txn = self.db.write_tx().expect("write_tx failed");
                    apply_op_in_txn(
                        &mut txn,
                        self.ks_savings.as_ref(),
                        self.ks_checking.as_ref(),
                        op,
                    );
                    match txn.commit().expect("io error") {
                        Ok(()) => break,
                        Err(_) => continue,
                    }
                },
            }
        }
    }

    fn execute_burst(&mut self, op_sets: &[Vec<SmallBankOp>]) -> BurstResult {
        let barrier = Arc::new(Barrier::new(op_sets.len()));
        let handles: Vec<_> = op_sets
            .iter()
            .cloned()
            .map(|ops| {
                let db = Arc::clone(&self.db);
                let ks_savings = Arc::clone(&self.ks_savings);
                let ks_checking = Arc::clone(&self.ks_checking);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut retries = 0u64;
                    loop {
                        let mut txn = db.write_tx().expect("write_tx failed");
                        for op in &ops {
                            apply_op_in_txn(&mut txn, ks_savings.as_ref(), ks_checking.as_ref(), op);
                        }
                        match txn.commit().expect("io error") {
                            Ok(()) => return (1u64, retries),
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
    {
        let mut engine = FjallEngine::preload();
        assert_matches_reference(&mut engine);
    }
    {
        let mut engine = FjallEngine::preload();
        assert_matches_reference_concurrent(&mut engine);
    }

    let fixture = generate_fixture(FIXTURE_POOL_SIZE);
    let mut engine = FjallEngine::preload();
    bench_workloads(c, &mut engine, &fixture);
    bench_contention(c, &mut engine, &fixture);
}

criterion_group! {
    name = smallbank_fjall;
    config = smallbank_criterion();
    targets = bench_smallbank
}
criterion_main!(smallbank_fjall);
