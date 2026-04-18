//! Shared smallbank workload: record types, op types, Zipfian generator,
//! workload generators, and the `SmallBankEngine` trait that backends implement.
//!
//! Included via `#[path = "smallbank_common.rs"]` in each bench binary.

#![allow(dead_code, unused_imports)]

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const NUM_ACCOUNTS: u64 = 10_000;
pub const OPS_PER_ITER: usize = 500;
pub const INITIAL_SAVINGS: f64 = 10_000.0;
pub const INITIAL_CHECKING: f64 = 10_000.0;
pub const ZIPFIAN_CONSTANT: f64 = 0.99;

pub const NUM_WRITERS: usize = 4;
pub const OPS_PER_WRITER: usize = 50;

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Account {
    pub customer_id: u64,
    pub name: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Savings {
    pub customer_id: u64,
    pub balance: f64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Checking {
    pub customer_id: u64,
    pub balance: f64,
}

// ---------------------------------------------------------------------------
// Transaction types
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub enum SmallBankOp {
    Balance(u64),
    DepositChecking(u64, f64),
    TransactSavings(u64, f64),
    Amalgamate { source: u64, dest: u64 },
    WriteCheck(u64, f64),
    SendPayment { source: u64, dest: u64, amount: f64 },
}

// ---------------------------------------------------------------------------
// Zipfian generator
// ---------------------------------------------------------------------------

pub struct ZipfianGenerator {
    item_count: u64,
    theta: f64,
    zeta_n: f64,
    alpha: f64,
    eta: f64,
}

impl ZipfianGenerator {
    pub fn new(item_count: u64, skew: f64) -> Self {
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

    pub fn next(&self, rng: &mut impl Rng) -> u64 {
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
// Workload generators
// ---------------------------------------------------------------------------

pub fn gen_mixed_workload(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<SmallBankOp> {
    gen_mixed_workload_n(rng, zipf, OPS_PER_ITER)
}

pub fn gen_mixed_workload_n(
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

pub fn gen_read_heavy_workload(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<SmallBankOp> {
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

pub fn gen_write_heavy_workload(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<SmallBankOp> {
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

pub fn gen_contention_ops(
    rng: &mut impl Rng,
    zipf: &ZipfianGenerator,
) -> Vec<Vec<SmallBankOp>> {
    (0..NUM_WRITERS)
        .map(|_| gen_mixed_workload_n(rng, zipf, OPS_PER_WRITER))
        .collect()
}

pub fn gen_high_contention_ops(rng: &mut impl Rng) -> Vec<Vec<SmallBankOp>> {
    // Hot accounts 1..=10 to maximize conflicts
    let hot_zipf = ZipfianGenerator::new(10, ZIPFIAN_CONSTANT);
    (0..NUM_WRITERS)
        .map(|_| gen_mixed_workload_n(rng, &hot_zipf, OPS_PER_WRITER))
        .collect()
}

// ---------------------------------------------------------------------------
// Engine trait & BurstResult
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default)]
pub struct BurstResult {
    pub committed: u64,
    pub aborted: u64,
}

pub trait SmallBankEngine {
    fn name(&self) -> &str;

    /// Execute ops sequentially, one transaction per op. Used for single-writer
    /// workload measurements.
    fn execute(&mut self, ops: &[SmallBankOp]);

    /// Execute `op_sets.len()` writers concurrently on separate OS threads.
    /// Each writer retries on conflict. Returns totals across all writers.
    fn execute_burst(&mut self, op_sets: &[Vec<SmallBankOp>]) -> BurstResult;
}

// ---------------------------------------------------------------------------
// Shared criterion config
// ---------------------------------------------------------------------------

pub fn smallbank_criterion() -> Criterion {
    Criterion::default()
        .sample_size(30)
        .measurement_time(Duration::from_secs(15))
}

// ---------------------------------------------------------------------------
// Shared bench harness
// ---------------------------------------------------------------------------

pub fn bench_workloads(c: &mut Criterion, engine: &mut impl SmallBankEngine) {
    let cfg_name = engine.name().to_owned();
    let zipf = ZipfianGenerator::new(NUM_ACCOUNTS, ZIPFIAN_CONSTANT);

    // Mixed workload (paper ratios)
    {
        let mut rng = StdRng::seed_from_u64(100);
        let mut group = c.benchmark_group(format!("smallbank_mixed/{cfg_name}"));
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("ops", |b| {
            b.iter_batched_ref(
                || gen_mixed_workload(&mut rng, &zipf),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    // Read-heavy workload
    {
        let mut rng = StdRng::seed_from_u64(200);
        let mut group = c.benchmark_group(format!("smallbank_read_heavy/{cfg_name}"));
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("ops", |b| {
            b.iter_batched_ref(
                || gen_read_heavy_workload(&mut rng, &zipf),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    // Write-heavy workload
    {
        let mut rng = StdRng::seed_from_u64(300);
        let mut group = c.benchmark_group(format!("smallbank_write_heavy/{cfg_name}"));
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("ops", |b| {
            b.iter_batched_ref(
                || gen_write_heavy_workload(&mut rng, &zipf),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }
}

pub fn bench_contention(c: &mut Criterion, engine: &mut impl SmallBankEngine) {
    let cfg_name = engine.name().to_owned();
    let zipf = ZipfianGenerator::new(NUM_ACCOUNTS, ZIPFIAN_CONSTANT);
    let total_ops = (NUM_WRITERS * OPS_PER_WRITER) as u64;

    // Low contention: Zipfian across full keyspace
    {
        let mut rng = StdRng::seed_from_u64(400);
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
