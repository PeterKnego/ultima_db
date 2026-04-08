#![allow(clippy::drop_non_drop)] // drop(TableWriter) releases borrow, needed for multi-table txns

//! SmallBank benchmark — a multi-table transactional workload.
//!
//! Models a banking application with 3 tables (accounts, savings, checking)
//! and 6 transaction types that exercise ACID, batch operations, and
//! multi-index search & update.
//!
//! Parameterizable via `SmallBankConfig` to compare in-memory vs. persistent,
//! different durability levels, writer modes, etc.
//!
//! Reference: Alomari et al., "The Cost of Serializability on Platforms That
//! Use Snapshot Isolation" (ICDE 2008).

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng};
use ultima_db::{IndexKind, Store, StoreConfig, WriterMode};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NUM_ACCOUNTS: u64 = 10_000;
const OPS_PER_ITER: usize = 500;
const INITIAL_SAVINGS: f64 = 10_000.0;
const INITIAL_CHECKING: f64 = 10_000.0;
const ZIPFIAN_CONSTANT: f64 = 0.99;

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Account {
    customer_id: u64,
    name: String,
}

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

// ---------------------------------------------------------------------------
// Transaction types
// ---------------------------------------------------------------------------

enum SmallBankOp {
    /// Read savings + checking balance for a customer.
    Balance(u64),
    /// Deposit into checking account.
    DepositChecking(u64, f64),
    /// Add/subtract from savings (can go negative = fee).
    TransactSavings(u64, f64),
    /// Move all savings from source to destination's checking. Zero source savings.
    Amalgamate { source: u64, dest: u64 },
    /// Write a check: deduct from checking, apply overdraft penalty if total < 0.
    WriteCheck(u64, f64),
    /// Transfer from source's checking to dest's checking.
    SendPayment { source: u64, dest: u64, amount: f64 },
}

// ---------------------------------------------------------------------------
// Zipfian generator (same as YCSB)
// ---------------------------------------------------------------------------

struct ZipfianGenerator {
    item_count: u64,
    theta: f64,
    zeta_n: f64,
    alpha: f64,
    eta: f64,
}

impl ZipfianGenerator {
    fn new(item_count: u64, skew: f64) -> Self {
        let theta = skew;
        let zeta_2 = Self::zeta(2, theta);
        let zeta_n = Self::zeta(item_count, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / item_count as f64).powf(1.0 - theta)) / (1.0 - zeta_2 / zeta_n);
        Self { item_count, theta, zeta_n, alpha, eta }
    }

    fn zeta(n: u64, theta: f64) -> f64 {
        (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
    }

    fn next(&self, rng: &mut impl Rng) -> u64 {
        let u: f64 = rng.random();
        let uz = u * self.zeta_n;
        let raw = if uz < 1.0 {
            0
        } else if uz < 1.0 + (0.5_f64).powf(self.theta) {
            1
        } else {
            let spread = self.item_count as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha);
            spread as u64
        };
        let scrambled = Self::fnv_hash(raw) % self.item_count;
        scrambled + 1 // 1-based customer IDs
    }

    fn fnv_hash(val: u64) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in &val.to_le_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }
}

// ---------------------------------------------------------------------------
// Workload generation
// ---------------------------------------------------------------------------

/// Mixed workload matching SmallBank paper ratios:
/// 15% Balance, 15% DepositChecking, 15% TransactSavings,
/// 15% Amalgamate, 25% WriteCheck, 15% SendPayment
fn gen_mixed_workload(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<SmallBankOp> {
    (0..OPS_PER_ITER)
        .map(|_| {
            let roll: f64 = rng.random();
            if roll < 0.15 {
                SmallBankOp::Balance(zipf.next(rng))
            } else if roll < 0.30 {
                SmallBankOp::DepositChecking(zipf.next(rng), rng.random_range(1.0..500.0))
            } else if roll < 0.45 {
                let amount = if rng.random_bool(0.8) {
                    rng.random_range(1.0..500.0)
                } else {
                    -rng.random_range(1.0..100.0)
                };
                SmallBankOp::TransactSavings(zipf.next(rng), amount)
            } else if roll < 0.60 {
                let source = zipf.next(rng);
                let mut dest = zipf.next(rng);
                while dest == source {
                    dest = zipf.next(rng);
                }
                SmallBankOp::Amalgamate { source, dest }
            } else if roll < 0.85 {
                SmallBankOp::WriteCheck(zipf.next(rng), rng.random_range(1.0..1000.0))
            } else {
                let source = zipf.next(rng);
                let mut dest = zipf.next(rng);
                while dest == source {
                    dest = zipf.next(rng);
                }
                SmallBankOp::SendPayment {
                    source,
                    dest,
                    amount: rng.random_range(1.0..500.0),
                }
            }
        })
        .collect()
}

/// Read-heavy workload: 80% Balance, 10% DepositChecking, 10% WriteCheck
fn gen_read_heavy_workload(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<SmallBankOp> {
    (0..OPS_PER_ITER)
        .map(|_| {
            let roll: f64 = rng.random();
            if roll < 0.80 {
                SmallBankOp::Balance(zipf.next(rng))
            } else if roll < 0.90 {
                SmallBankOp::DepositChecking(zipf.next(rng), rng.random_range(1.0..500.0))
            } else {
                SmallBankOp::WriteCheck(zipf.next(rng), rng.random_range(1.0..1000.0))
            }
        })
        .collect()
}

/// Write-heavy workload: 30% Amalgamate, 30% SendPayment, 20% DepositChecking, 20% TransactSavings
fn gen_write_heavy_workload(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<SmallBankOp> {
    (0..OPS_PER_ITER)
        .map(|_| {
            let roll: f64 = rng.random();
            if roll < 0.30 {
                let source = zipf.next(rng);
                let mut dest = zipf.next(rng);
                while dest == source {
                    dest = zipf.next(rng);
                }
                SmallBankOp::Amalgamate { source, dest }
            } else if roll < 0.60 {
                let source = zipf.next(rng);
                let mut dest = zipf.next(rng);
                while dest == source {
                    dest = zipf.next(rng);
                }
                SmallBankOp::SendPayment {
                    source,
                    dest,
                    amount: rng.random_range(1.0..500.0),
                }
            } else if roll < 0.80 {
                SmallBankOp::DepositChecking(zipf.next(rng), rng.random_range(1.0..500.0))
            } else {
                SmallBankOp::TransactSavings(zipf.next(rng), rng.random_range(1.0..500.0))
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Benchmark configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SmallBankConfig {
    name: String,
    store_config: StoreConfig,
    /// Directory for persistence (None = in-memory).
    #[cfg(feature = "persistence")]
    persistence: ultima_db::Persistence,
}

impl SmallBankConfig {
    fn inmemory() -> Self {
        Self {
            name: "inmemory".into(),
            store_config: StoreConfig {
                num_snapshots_retained: 2,
                auto_snapshot_gc: true,
                ..StoreConfig::default()
            },
            #[cfg(feature = "persistence")]
            persistence: ultima_db::Persistence::None,
        }
    }

    #[cfg(feature = "persistence")]
    fn standalone_consistent() -> Self {
        Self {
            name: "standalone_consistent".into(),
            store_config: StoreConfig {
                num_snapshots_retained: 2,
                auto_snapshot_gc: true,
                ..StoreConfig::default()
            },
            persistence: ultima_db::Persistence::Standalone {
                dir: std::path::PathBuf::new(), // placeholder, set at runtime
                durability: ultima_db::Durability::Consistent,
            },
        }
    }

    #[cfg(feature = "persistence")]
    fn standalone_eventual() -> Self {
        Self {
            name: "standalone_eventual".into(),
            store_config: StoreConfig {
                num_snapshots_retained: 2,
                auto_snapshot_gc: true,
                ..StoreConfig::default()
            },
            persistence: ultima_db::Persistence::Standalone {
                dir: std::path::PathBuf::new(), // placeholder, set at runtime
                durability: ultima_db::Durability::Eventual,
            },
        }
    }

}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

struct SmallBankEngine {
    store: Store,
    _tmpdir: Option<tempfile::TempDir>,
    /// Tracks sum and count of in-flight WAL samples for eventual mode reporting.
    #[cfg(feature = "persistence")]
    in_flight_sum: u64,
    #[cfg(feature = "persistence")]
    in_flight_samples: u64,
}

impl SmallBankEngine {
    fn new(config: &SmallBankConfig) -> Self {
        #[allow(unused_mut)]
        let mut store_config = config.store_config.clone();
        let tmpdir;

        #[cfg(feature = "persistence")]
        {
            tmpdir = match &config.persistence {
                ultima_db::Persistence::None => None,
                ultima_db::Persistence::Standalone { durability, .. } => {
                    let dir = tempfile::tempdir().unwrap();
                    store_config.persistence = ultima_db::Persistence::Standalone {
                        dir: dir.path().to_path_buf(),
                        durability: *durability,
                    };
                    Some(dir)
                }
                ultima_db::Persistence::Smr { .. } => None
            };
        }
        #[cfg(not(feature = "persistence"))]
        {
            tmpdir = None;
        }

        let store = Store::new(store_config).unwrap();

        #[cfg(feature = "persistence")]
        {
            store.register_table::<Account>("accounts").unwrap();
            store.register_table::<Savings>("savings").unwrap();
            store.register_table::<Checking>("checking").unwrap();
        }

        // Preload data using batch insert.
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut accounts = wtx.open_table::<Account>("accounts").unwrap();
            accounts
                .define_index("customer_id", IndexKind::Unique, |a: &Account| a.customer_id)
                .unwrap();
            let batch: Vec<Account> = (1..=NUM_ACCOUNTS)
                .map(|i| Account {
                    customer_id: i,
                    name: format!("Customer_{i}"),
                })
                .collect();
            accounts.insert_batch(batch).unwrap();
        }
        {
            let mut savings = wtx.open_table::<Savings>("savings").unwrap();
            savings
                .define_index("customer_id", IndexKind::Unique, |s: &Savings| s.customer_id)
                .unwrap();
            let batch: Vec<Savings> = (1..=NUM_ACCOUNTS)
                .map(|i| Savings {
                    customer_id: i,
                    balance: INITIAL_SAVINGS,
                })
                .collect();
            savings.insert_batch(batch).unwrap();
        }
        {
            let mut checking = wtx.open_table::<Checking>("checking").unwrap();
            checking
                .define_index("customer_id", IndexKind::Unique, |c: &Checking| {
                    c.customer_id
                })
                .unwrap();
            let batch: Vec<Checking> = (1..=NUM_ACCOUNTS)
                .map(|i| Checking {
                    customer_id: i,
                    balance: INITIAL_CHECKING,
                })
                .collect();
            checking.insert_batch(batch).unwrap();
        }
        wtx.commit().unwrap();

        SmallBankEngine {
            store,
            _tmpdir: tmpdir,
            #[cfg(feature = "persistence")]
            in_flight_sum: 0,
            #[cfg(feature = "persistence")]
            in_flight_samples: 0,
        }
    }

    /// Sample the in-flight WAL counter (eventual mode only).
    #[cfg(feature = "persistence")]
    fn sample_in_flight(&mut self) {
        let pending = self.store.pending_wal_writes();
        self.in_flight_sum += pending;
        self.in_flight_samples += 1;
    }

    /// Report average in-flight WAL writes, if any samples were taken.
    #[cfg(feature = "persistence")]
    fn report_in_flight(&self, config_name: &str, workload: &str) {
        if self.in_flight_samples > 0 {
            let avg = self.in_flight_sum as f64 / self.in_flight_samples as f64;
            eprintln!(
                "  [{config_name}] {workload}: avg in-flight WAL writes: {avg:.2} \
                 ({} samples)",
                self.in_flight_samples
            );
        }
    }

    fn execute(&mut self, ops: &[SmallBankOp]) {
        for op in ops {
            match op {
                SmallBankOp::Balance(cid) => {
                    let rtx = self.store.begin_read(None).unwrap();
                    let savings = rtx.open_table::<Savings>("savings").unwrap();
                    let checking = rtx.open_table::<Checking>("checking").unwrap();
                    let sb = savings.get_unique::<u64>("customer_id", cid).unwrap();
                    let cb = checking.get_unique::<u64>("customer_id", cid).unwrap();
                    black_box((sb.map(|(_, s)| s.balance), cb.map(|(_, c)| c.balance)));
                }

                SmallBankOp::DepositChecking(cid, amount) => {
                    let mut wtx = self.store.begin_write(None).unwrap();
                    {
                        let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                        if let Some((id, rec)) =
                            checking.get_unique::<u64>("customer_id", cid).unwrap()
                        {
                            let new = Checking {
                                balance: rec.balance + amount,
                                ..*rec
                            };
                            let _ = checking.update(id, new);
                        }
                    }
                    wtx.commit().unwrap();
                }

                SmallBankOp::TransactSavings(cid, amount) => {
                    let mut wtx = self.store.begin_write(None).unwrap();
                    {
                        let mut savings = wtx.open_table::<Savings>("savings").unwrap();
                        if let Some((id, rec)) =
                            savings.get_unique::<u64>("customer_id", cid).unwrap()
                        {
                            let new = Savings {
                                balance: rec.balance + amount,
                                ..*rec
                            };
                            let _ = savings.update(id, new);
                        }
                    }
                    wtx.commit().unwrap();
                }

                SmallBankOp::Amalgamate { source, dest } => {
                    let mut wtx = self.store.begin_write(None).unwrap();
                    {
                        let mut savings = wtx.open_table::<Savings>("savings").unwrap();
                        let source_amount =
                            if let Some((id, rec)) =
                                savings.get_unique::<u64>("customer_id", source).unwrap()
                            {
                                let amt = rec.balance;
                                let _ = savings.update(
                                    id,
                                    Savings {
                                        balance: 0.0,
                                        ..*rec
                                    },
                                );
                                amt
                            } else {
                                0.0
                            };
                        drop(savings);

                        let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                        if let Some((id, rec)) =
                            checking.get_unique::<u64>("customer_id", dest).unwrap()
                        {
                            let _ = checking.update(
                                id,
                                Checking {
                                    balance: rec.balance + source_amount,
                                    ..*rec
                                },
                            );
                        }
                    }
                    wtx.commit().unwrap();
                }

                SmallBankOp::WriteCheck(cid, amount) => {
                    let mut wtx = self.store.begin_write(None).unwrap();
                    {
                        let savings = wtx.open_table::<Savings>("savings").unwrap();
                        let sbal = savings
                            .get_unique::<u64>("customer_id", cid)
                            .unwrap()
                            .map(|(_, s)| s.balance)
                            .unwrap_or(0.0);
                        drop(savings);

                        let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                        if let Some((id, rec)) =
                            checking.get_unique::<u64>("customer_id", cid).unwrap()
                        {
                            let total = sbal + rec.balance;
                            let penalty = if total < *amount { 1.0 } else { 0.0 };
                            let _ = checking.update(
                                id,
                                Checking {
                                    balance: rec.balance - amount - penalty,
                                    ..*rec
                                },
                            );
                        }
                    }
                    wtx.commit().unwrap();
                }

                SmallBankOp::SendPayment {
                    source,
                    dest,
                    amount,
                } => {
                    let mut wtx = self.store.begin_write(None).unwrap();
                    {
                        let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                        if let Some((src_id, src_rec)) =
                            checking.get_unique::<u64>("customer_id", source).unwrap()
                            && src_rec.balance >= *amount
                        {
                            let src_new = Checking {
                                balance: src_rec.balance - amount,
                                ..*src_rec
                            };
                            if let Some((dst_id, dst_rec)) =
                                checking.get_unique::<u64>("customer_id", dest).unwrap()
                            {
                                let dst_new = Checking {
                                    balance: dst_rec.balance + amount,
                                    ..*dst_rec
                                };
                                let _ = checking.update(src_id, src_new);
                                let _ = checking.update(dst_id, dst_new);
                            }
                        }
                    }
                    wtx.commit().unwrap();
                }
            }
            #[cfg(feature = "persistence")]
            self.sample_in_flight();
        }
    }
}

// ---------------------------------------------------------------------------
// Multi-writer contention support
// ---------------------------------------------------------------------------

const NUM_WRITERS: usize = 4;
const OPS_PER_WRITER: usize = 50;

#[allow(dead_code)]
struct BurstResult {
    committed: u64,
    aborted: u64,
}

impl SmallBankEngine {
    /// Execute a burst of concurrent writers. Each writer gets a slice of ops.
    /// Simulates concurrency by opening all writers, executing, then committing
    /// sequentially (WriteTx is not Send). Retries on conflict.
    fn execute_burst(&mut self, op_sets: &[Vec<SmallBankOp>]) -> BurstResult {
        let mut committed = 0u64;
        let mut aborted = 0u64;

        // Open all writers simultaneously
        let mut writers: Vec<_> = (0..op_sets.len())
            .map(|_| Some(self.store.begin_write(None).unwrap()))
            .collect();

        // Each writer executes its ops
        for (w, wtx_opt) in writers.iter_mut().enumerate() {
            let wtx = wtx_opt.as_mut().unwrap();
            Self::execute_ops_on_tx(wtx, &op_sets[w]);
        }

        // Commit in sequence — retry on conflict
        for w in 0..op_sets.len() {
            let wtx = writers[w].take().unwrap();
            match wtx.commit() {
                Ok(_) => committed += 1,
                Err(ultima_db::Error::WriteConflict { .. }) => {
                    aborted += 1;
                    // Retry with fresh transaction
                    let mut wtx = self.store.begin_write(None).unwrap();
                    Self::execute_ops_on_tx(&mut wtx, &op_sets[w]);
                    wtx.commit().unwrap();
                    committed += 1;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        BurstResult { committed, aborted }
    }

    /// Execute SmallBank ops within an existing WriteTx.
    fn execute_ops_on_tx(wtx: &mut ultima_db::WriteTx, ops: &[SmallBankOp]) {
        for op in ops {
            match op {
                SmallBankOp::Balance(_) => {
                    // Read-only — use the write tx's snapshot for reads
                }

                SmallBankOp::DepositChecking(cid, amount) => {
                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((id, rec)) =
                        checking.get_unique::<u64>("customer_id", cid).unwrap()
                    {
                        let new = Checking {
                            balance: rec.balance + amount,
                            ..*rec
                        };
                        let _ = checking.update(id, new);
                    }
                }

                SmallBankOp::TransactSavings(cid, amount) => {
                    let mut savings = wtx.open_table::<Savings>("savings").unwrap();
                    if let Some((id, rec)) =
                        savings.get_unique::<u64>("customer_id", cid).unwrap()
                    {
                        let new = Savings {
                            balance: rec.balance + amount,
                            ..*rec
                        };
                        let _ = savings.update(id, new);
                    }
                }

                SmallBankOp::Amalgamate { source, dest } => {
                    let mut savings = wtx.open_table::<Savings>("savings").unwrap();
                    let source_amount =
                        if let Some((id, rec)) =
                            savings.get_unique::<u64>("customer_id", source).unwrap()
                        {
                            let amt = rec.balance;
                            let _ = savings.update(
                                id,
                                Savings {
                                    balance: 0.0,
                                    ..*rec
                                },
                            );
                            amt
                        } else {
                            0.0
                        };
                    drop(savings);

                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((id, rec)) =
                        checking.get_unique::<u64>("customer_id", dest).unwrap()
                    {
                        let _ = checking.update(
                            id,
                            Checking {
                                balance: rec.balance + source_amount,
                                ..*rec
                            },
                        );
                    }
                }

                SmallBankOp::WriteCheck(cid, amount) => {
                    let savings = wtx.open_table::<Savings>("savings").unwrap();
                    let sbal = savings
                        .get_unique::<u64>("customer_id", cid)
                        .unwrap()
                        .map(|(_, s)| s.balance)
                        .unwrap_or(0.0);
                    drop(savings);

                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((id, rec)) =
                        checking.get_unique::<u64>("customer_id", cid).unwrap()
                    {
                        let total = sbal + rec.balance;
                        let penalty = if total < *amount { 1.0 } else { 0.0 };
                        let _ = checking.update(
                            id,
                            Checking {
                                balance: rec.balance - amount - penalty,
                                ..*rec
                            },
                        );
                    }
                }

                SmallBankOp::SendPayment {
                    source,
                    dest,
                    amount,
                } => {
                    let mut checking = wtx.open_table::<Checking>("checking").unwrap();
                    if let Some((src_id, src_rec)) =
                        checking.get_unique::<u64>("customer_id", source).unwrap()
                        && src_rec.balance >= *amount
                    {
                        let src_new = Checking {
                            balance: src_rec.balance - amount,
                            ..*src_rec
                        };
                        if let Some((dst_id, dst_rec)) =
                            checking.get_unique::<u64>("customer_id", dest).unwrap()
                        {
                            let dst_new = Checking {
                                balance: dst_rec.balance + amount,
                                ..*dst_rec
                            };
                            let _ = checking.update(src_id, src_new);
                            let _ = checking.update(dst_id, dst_new);
                        }
                    }
                }
            }
        }
    }
}

fn gen_contention_ops(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<Vec<SmallBankOp>> {
    (0..NUM_WRITERS)
        .map(|_| gen_mixed_workload_n(rng, zipf, OPS_PER_WRITER))
        .collect()
}

fn gen_high_contention_ops(rng: &mut impl Rng) -> Vec<Vec<SmallBankOp>> {
    // Hot accounts 1..=10 to maximize conflicts
    let hot_zipf = ZipfianGenerator::new(10, ZIPFIAN_CONSTANT);
    (0..NUM_WRITERS)
        .map(|_| gen_mixed_workload_n(rng, &hot_zipf, OPS_PER_WRITER))
        .collect()
}

/// Like gen_mixed_workload but with configurable count.
fn gen_mixed_workload_n(
    rng: &mut impl Rng,
    zipf: &ZipfianGenerator,
    count: usize,
) -> Vec<SmallBankOp> {
    (0..count)
        .map(|_| {
            let roll: f64 = rng.random();
            if roll < 0.15 {
                SmallBankOp::Balance(zipf.next(rng))
            } else if roll < 0.30 {
                SmallBankOp::DepositChecking(zipf.next(rng), rng.random_range(1.0..500.0))
            } else if roll < 0.45 {
                let amount = if rng.random_bool(0.8) {
                    rng.random_range(1.0..500.0)
                } else {
                    -rng.random_range(1.0..100.0)
                };
                SmallBankOp::TransactSavings(zipf.next(rng), amount)
            } else if roll < 0.60 {
                let source = zipf.next(rng);
                let mut dest = zipf.next(rng);
                while dest == source {
                    dest = zipf.next(rng);
                }
                SmallBankOp::Amalgamate { source, dest }
            } else if roll < 0.85 {
                SmallBankOp::WriteCheck(zipf.next(rng), rng.random_range(1.0..1000.0))
            } else {
                let source = zipf.next(rng);
                let mut dest = zipf.next(rng);
                while dest == source {
                    dest = zipf.next(rng);
                }
                SmallBankOp::SendPayment {
                    source,
                    dest,
                    amount: rng.random_range(1.0..500.0),
                }
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

fn bench_contention(c: &mut Criterion, config: &SmallBankConfig) {
    let zipf = ZipfianGenerator::new(NUM_ACCOUNTS, ZIPFIAN_CONSTANT);
    let cfg_name = &config.name;
    let total_ops = (NUM_WRITERS * OPS_PER_WRITER) as u64;

    // Low contention: Zipfian across full keyspace
    {
        let mut rng = StdRng::seed_from_u64(400);
        let mut mw_config = config.clone();
        mw_config.store_config.writer_mode = WriterMode::MultiWriter;
        let mut engine = SmallBankEngine::new(&mw_config);

        let mut group = c.benchmark_group(format!("smallbank_contention_low/{cfg_name}"));
        group.throughput(Throughput::Elements(total_ops));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || gen_contention_ops(&mut rng, &zipf),
                |op_sets| {
                    black_box(engine.execute_burst(op_sets));
                },
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    // High contention: hot accounts 1..=10
    {
        let mut rng = StdRng::seed_from_u64(500);
        let mut mw_config = config.clone();
        mw_config.store_config.writer_mode = WriterMode::MultiWriter;
        let mut engine = SmallBankEngine::new(&mw_config);

        let mut group = c.benchmark_group(format!("smallbank_contention_high/{cfg_name}"));
        group.throughput(Throughput::Elements(total_ops));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || gen_high_contention_ops(&mut rng),
                |op_sets| {
                    black_box(engine.execute_burst(op_sets));
                },
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }
}

fn bench_config(c: &mut Criterion, config: &SmallBankConfig) {
    let zipf = ZipfianGenerator::new(NUM_ACCOUNTS, ZIPFIAN_CONSTANT);
    let cfg_name = &config.name;

    // Mixed workload (paper ratios)
    {
        let mut rng = StdRng::seed_from_u64(100);
        let mut engine = SmallBankEngine::new(config);
        let mut group = c.benchmark_group(format!("smallbank_mixed/{cfg_name}"));
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || gen_mixed_workload(&mut rng, &zipf),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
        #[cfg(feature = "persistence")]
        engine.report_in_flight(cfg_name, "mixed");
    }

    // Read-heavy workload
    {
        let mut rng = StdRng::seed_from_u64(200);
        let mut engine = SmallBankEngine::new(config);
        let mut group = c.benchmark_group(format!("smallbank_read_heavy/{cfg_name}"));
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || gen_read_heavy_workload(&mut rng, &zipf),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
        #[cfg(feature = "persistence")]
        engine.report_in_flight(cfg_name, "read_heavy");
    }

    // Write-heavy workload
    {
        let mut rng = StdRng::seed_from_u64(300);
        let mut engine = SmallBankEngine::new(config);
        let mut group = c.benchmark_group(format!("smallbank_write_heavy/{cfg_name}"));
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || gen_write_heavy_workload(&mut rng, &zipf),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
        #[cfg(feature = "persistence")]
        engine.report_in_flight(cfg_name, "write_heavy");
    }
}

fn bench_smallbank(c: &mut Criterion) {
    let configs = vec![
        SmallBankConfig::inmemory(),
        #[cfg(feature = "persistence")]
        SmallBankConfig::standalone_consistent(),
        #[cfg(feature = "persistence")]
        SmallBankConfig::standalone_eventual(),
    ];

    for config in &configs {
        bench_config(c, config);
        bench_contention(c, config);
    }
}

criterion_group! {
    name = smallbank;
    config = Criterion::default()
        .sample_size(30)
        .measurement_time(Duration::from_secs(15));
    targets = bench_smallbank
}
criterion_main!(smallbank);
