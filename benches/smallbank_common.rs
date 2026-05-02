// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared smallbank workload: record types, op types, Zipfian generator,
//! workload generators, the `SmallBankEngine` trait that backends implement,
//! and the pre-generated `WorkloadFixture` + reference implementation that
//! lets cross-DB parity tests verify identical final state.
//!
//! Included via `#[path = "smallbank_common.rs"]` in each bench binary.

#![allow(dead_code, unused_imports)]

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

    /// Compute a deterministic hash of the current durable state (sum of
    /// balances per table, rounded). Used by parity tests to confirm that
    /// every backend that applied the same sequential op stream landed in
    /// the same final state.
    fn verify(&self) -> StateHash;
}

// ---------------------------------------------------------------------------
// Pre-generated workload fixture
//
// Why pre-generate? Each backend's criterion run decides its own iteration
// count (fast engines run more iters in the same wall-clock budget). If each
// iteration re-generates ops via a shared-seed `StdRng`, the RNG state
// diverges between backends after the first mismatched iter count. A fixed
// fixture means iteration N on every backend runs the byte-for-byte same op
// slice — which is what we need for cross-DB comparison and correctness
// verification.
// ---------------------------------------------------------------------------

pub const FIXTURE_POOL_SIZE: usize = 256;
pub const FIXTURE_SEED: u64 = 0x5B_5B_5B_5B;
pub const CORRECTNESS_ITERS: usize = 32;

pub struct WorkloadFixture {
    pub mixed: Vec<Vec<SmallBankOp>>,
    pub read_heavy: Vec<Vec<SmallBankOp>>,
    pub write_heavy: Vec<Vec<SmallBankOp>>,
    pub contention_low: Vec<Vec<Vec<SmallBankOp>>>,
    pub contention_high: Vec<Vec<Vec<SmallBankOp>>>,
}

pub fn generate_fixture(pool_size: usize) -> Arc<WorkloadFixture> {
    let zipf = ZipfianGenerator::new(NUM_ACCOUNTS, ZIPFIAN_CONSTANT);
    let mut rng_mixed = StdRng::seed_from_u64(100);
    let mut rng_read = StdRng::seed_from_u64(200);
    let mut rng_write = StdRng::seed_from_u64(300);
    let mut rng_cont_low = StdRng::seed_from_u64(400);
    let mut rng_cont_high = StdRng::seed_from_u64(500);

    Arc::new(WorkloadFixture {
        mixed: (0..pool_size)
            .map(|_| gen_mixed_workload(&mut rng_mixed, &zipf))
            .collect(),
        read_heavy: (0..pool_size)
            .map(|_| gen_read_heavy_workload(&mut rng_read, &zipf))
            .collect(),
        write_heavy: (0..pool_size)
            .map(|_| gen_write_heavy_workload(&mut rng_write, &zipf))
            .collect(),
        contention_low: (0..pool_size)
            .map(|_| gen_contention_ops(&mut rng_cont_low, &zipf))
            .collect(),
        contention_high: (0..pool_size)
            .map(|_| gen_high_contention_ops(&mut rng_cont_high))
            .collect(),
    })
}

// ---------------------------------------------------------------------------
// Reference implementation — the oracle for correctness checks
//
// `ReferenceState` tracks per-customer savings and checking as plain f64.
// `apply_op` mutates it exactly the way a correctly-implemented smallbank
// backend should. Running the same op sequence through the reference and
// through each backend must produce the same `StateHash`.
//
// Correctness tests only use this in sequential mode — multi-threaded bursts
// have non-deterministic commit interleavings, so parity against a linear
// reference would be meaningless.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default)]
pub struct AccountState {
    pub savings: f64,
    pub checking: f64,
}

pub struct ReferenceState {
    pub accounts: BTreeMap<u64, AccountState>,
}

impl ReferenceState {
    pub fn seeded(num_accounts: u64) -> Self {
        let accounts = (1..=num_accounts)
            .map(|i| {
                (
                    i,
                    AccountState {
                        savings: INITIAL_SAVINGS,
                        checking: INITIAL_CHECKING,
                    },
                )
            })
            .collect();
        Self { accounts }
    }

    pub fn apply(&mut self, op: &SmallBankOp) {
        match op {
            SmallBankOp::Balance(_) => {}
            SmallBankOp::DepositChecking(cid, amount) => {
                if let Some(a) = self.accounts.get_mut(cid) {
                    a.checking += amount;
                }
            }
            SmallBankOp::TransactSavings(cid, amount) => {
                if let Some(a) = self.accounts.get_mut(cid) {
                    a.savings += amount;
                }
            }
            SmallBankOp::Amalgamate { source, dest } => {
                let src_amt = self
                    .accounts
                    .get_mut(source)
                    .map(|a| {
                        let amt = a.savings;
                        a.savings = 0.0;
                        amt
                    })
                    .unwrap_or(0.0);
                if let Some(a) = self.accounts.get_mut(dest) {
                    a.checking += src_amt;
                }
            }
            SmallBankOp::WriteCheck(cid, amount) => {
                if let Some(a) = self.accounts.get_mut(cid) {
                    let total = a.savings + a.checking;
                    let penalty = if total < *amount { 1.0 } else { 0.0 };
                    a.checking -= amount + penalty;
                }
            }
            SmallBankOp::SendPayment { source, dest, amount } => {
                let allow = self
                    .accounts
                    .get(source)
                    .map(|a| a.checking >= *amount)
                    .unwrap_or(false);
                let dest_exists = self.accounts.contains_key(dest);
                if allow && dest_exists {
                    self.accounts.get_mut(source).unwrap().checking -= amount;
                    self.accounts.get_mut(dest).unwrap().checking += amount;
                }
            }
        }
    }

    pub fn apply_all(&mut self, ops: &[SmallBankOp]) {
        for op in ops {
            self.apply(op);
        }
    }

    pub fn hash(&self) -> StateHash {
        hash_accounts(self.accounts.iter().map(|(id, a)| (*id, *a)))
    }
}

/// Deterministic representation of the final state used for cross-DB equality.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateHash {
    pub account_count: u64,
    pub savings_bits: u64,
    pub checking_bits: u64,
    pub mix: u64,
}

pub fn hash_accounts<I>(accounts: I) -> StateHash
where
    I: IntoIterator<Item = (u64, AccountState)>,
{
    let mut total_savings = 0.0_f64;
    let mut total_checking = 0.0_f64;
    let mut h = DefaultHasher::new();
    let mut count = 0u64;
    let mut rows: Vec<(u64, AccountState)> = accounts.into_iter().collect();
    rows.sort_by_key(|(id, _)| *id);
    for (id, a) in &rows {
        count += 1;
        total_savings += a.savings;
        total_checking += a.checking;
        id.hash(&mut h);
        a.savings.to_bits().hash(&mut h);
        a.checking.to_bits().hash(&mut h);
    }
    StateHash {
        account_count: count,
        savings_bits: total_savings.to_bits(),
        checking_bits: total_checking.to_bits(),
        mix: h.finish(),
    }
}

/// Run the first `CORRECTNESS_ITERS` batches of the mixed workload sequentially
/// through `engine`, run the same through the reference, and return
/// `(engine_hash, reference_hash)`. Callers assert they match.
pub fn run_correctness_check(engine: &mut impl SmallBankEngine) -> (StateHash, StateHash) {
    let fixture = generate_fixture(CORRECTNESS_ITERS);
    let mut reference = ReferenceState::seeded(NUM_ACCOUNTS);
    for ops in &fixture.mixed {
        engine.execute(ops);
        reference.apply_all(ops);
    }
    (engine.verify(), reference.hash())
}

/// Asserts `engine`'s final state matches the reference after a fixed
/// sequential workload. Panics with a detailed diff on mismatch. Intended to
/// be called from the criterion entry point so every `cargo bench` run
/// exercises correctness before perf measurement.
pub fn assert_matches_reference(engine: &mut impl SmallBankEngine) {
    let name = engine.name().to_owned();
    let (engine_hash, reference_hash) = run_correctness_check(engine);
    if engine_hash != reference_hash {
        panic!(
            "[{name}] final state diverged from reference\n  engine   : {engine_hash:?}\n  reference: {reference_hash:?}"
        );
    }
    eprintln!("[{name}] sequential correctness check passed: {engine_hash:?}");
}

// ---------------------------------------------------------------------------
// Concurrent correctness — commutative-only burst workload
//
// A real smallbank mix (WriteCheck, Amalgamate, SendPayment) is
// order-sensitive: final state depends on which writer commits first, so
// replaying through a linear reference is meaningless. But if we restrict
// the burst to *commutative* ops — DepositChecking and TransactSavings,
// both of which do `balance += amount` — then any serialization of the
// successful commits yields the same final state. That lets us verify real
// multi-threaded commit correctness: the engine's post-burst state must
// match the reference state computed by applying every op once, regardless
// of the commit interleaving.
//
// This exercises the thing we actually care about: that OCC retries neither
// duplicate writes nor drop them.
// ---------------------------------------------------------------------------

pub fn gen_commutative_burst(
    rng: &mut impl Rng,
    zipf: &ZipfianGenerator,
) -> Vec<Vec<SmallBankOp>> {
    // INTEGER amounts only. f64 addition is non-associative for general
    // values, so with commit-order-dependent sum order, per-account balances
    // would differ bit-for-bit across engines even when the ops are
    // mathematically commutative. Integers up to 2^53 are represented
    // exactly in f64 and their sums are bit-exact regardless of order.
    (0..NUM_WRITERS)
        .map(|_| {
            (0..OPS_PER_WRITER)
                .map(|_| {
                    let cid = zipf.next(rng);
                    let amount = rng.random_range(1u32..500u32) as f64;
                    if rng.random_bool(0.5) {
                        SmallBankOp::DepositChecking(cid, amount)
                    } else {
                        SmallBankOp::TransactSavings(cid, amount)
                    }
                })
                .collect()
        })
        .collect()
}

pub const CONCURRENT_CORRECTNESS_BURSTS: usize = 16;

/// Runs a series of commutative-only bursts through the engine and through
/// the reference (applying each writer's ops once, in any order, since the
/// ops are commutative). Final state must match bit-for-bit.
pub fn assert_matches_reference_concurrent(engine: &mut impl SmallBankEngine) {
    let name = engine.name().to_owned();
    let zipf = ZipfianGenerator::new(NUM_ACCOUNTS, ZIPFIAN_CONSTANT);
    let mut rng = StdRng::seed_from_u64(777);

    let mut reference = ReferenceState::seeded(NUM_ACCOUNTS);
    let mut total_committed = 0u64;
    let mut total_aborted = 0u64;
    for _ in 0..CONCURRENT_CORRECTNESS_BURSTS {
        let burst = gen_commutative_burst(&mut rng, &zipf);
        let result = engine.execute_burst(&burst);
        total_committed += result.committed;
        total_aborted += result.aborted;
        for writer_ops in &burst {
            reference.apply_all(writer_ops);
        }
    }

    let engine_hash = engine.verify();
    let reference_hash = reference.hash();
    if engine_hash != reference_hash {
        panic!(
            "[{name}] burst state diverged from reference (commutative ops)\n\
             committed={total_committed}, aborted={total_aborted}\n\
             engine   : {engine_hash:?}\n  reference: {reference_hash:?}"
        );
    }
    eprintln!(
        "[{name}] concurrent correctness check passed: committed={total_committed}, aborted={total_aborted}, hash={engine_hash:?}"
    );
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

/// Per-bench counter: lets each `iter_batched_ref` advance deterministically
/// through the shared fixture pool.
struct Cursor(AtomicUsize);

impl Cursor {
    fn new() -> Self {
        Self(AtomicUsize::new(0))
    }
    fn next(&self, len: usize) -> usize {
        self.0.fetch_add(1, Ordering::Relaxed) % len
    }
}

pub fn bench_workloads(
    c: &mut Criterion,
    engine: &mut impl SmallBankEngine,
    fixture: &Arc<WorkloadFixture>,
) {
    let cfg_name = engine.name().to_owned();

    let mut run = |c: &mut Criterion, label: &str, pool: &[Vec<SmallBankOp>]| {
        let cursor = Cursor::new();
        let mut group = c.benchmark_group(format!("smallbank_{label}/{cfg_name}"));
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("ops", |b| {
            b.iter_batched_ref(
                || pool[cursor.next(pool.len())].clone(),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
    };

    run(c, "mixed", &fixture.mixed);
    run(c, "read_heavy", &fixture.read_heavy);
    run(c, "write_heavy", &fixture.write_heavy);
}

pub fn bench_contention(
    c: &mut Criterion,
    engine: &mut impl SmallBankEngine,
    fixture: &Arc<WorkloadFixture>,
) {
    let cfg_name = engine.name().to_owned();
    let total_ops = (NUM_WRITERS * OPS_PER_WRITER) as u64;

    let mut run = |c: &mut Criterion, label: &str, pool: &[Vec<Vec<SmallBankOp>>]| {
        let cursor = Cursor::new();
        let mut group = c.benchmark_group(format!("smallbank_contention_{label}/{cfg_name}"));
        group.throughput(Throughput::Elements(total_ops));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || pool[cursor.next(pool.len())].clone(),
                |op_sets| {
                    black_box(engine.execute_burst(op_sets));
                },
                BatchSize::SmallInput,
            );
        });
        group.finish();
    };

    run(c, "low", &fixture.contention_low);
    run(c, "high", &fixture.contention_high);
}

/// Extended contention bench that runs at a parameterized writer count,
/// generating its own pool on the fly. Used to compare engines at N=16
/// alongside the standard N=4 runs.
pub fn bench_contention_at_n(
    c: &mut Criterion,
    engine: &mut impl SmallBankEngine,
    n_writers: usize,
    pool_size: usize,
) {
    let cfg_name = engine.name().to_owned();
    let total_ops = (n_writers * OPS_PER_WRITER) as u64;

    let zipf_full = ZipfianGenerator::new(NUM_ACCOUNTS, ZIPFIAN_CONSTANT);
    let hot_zipf = ZipfianGenerator::new(10, ZIPFIAN_CONSTANT);

    let mut rng_low = StdRng::seed_from_u64(400 + n_writers as u64);
    let low_pool: Vec<Vec<Vec<SmallBankOp>>> = (0..pool_size)
        .map(|_| {
            (0..n_writers)
                .map(|_| gen_mixed_workload_n(&mut rng_low, &zipf_full, OPS_PER_WRITER))
                .collect()
        })
        .collect();

    let mut rng_high = StdRng::seed_from_u64(500 + n_writers as u64);
    let high_pool: Vec<Vec<Vec<SmallBankOp>>> = (0..pool_size)
        .map(|_| {
            (0..n_writers)
                .map(|_| gen_mixed_workload_n(&mut rng_high, &hot_zipf, OPS_PER_WRITER))
                .collect()
        })
        .collect();

    let mut run = |c: &mut Criterion, label: &str, pool: &[Vec<Vec<SmallBankOp>>]| {
        let cursor = Cursor::new();
        let mut group = c.benchmark_group(format!(
            "smallbank_contention_{label}_n{n_writers}/{cfg_name}"
        ));
        group.throughput(Throughput::Elements(total_ops));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || pool[cursor.next(pool.len())].clone(),
                |op_sets| {
                    black_box(engine.execute_burst(op_sets));
                },
                BatchSize::SmallInput,
            );
        });
        group.finish();
    };

    run(c, "low", &low_pool);
    run(c, "high", &high_pool);
}
