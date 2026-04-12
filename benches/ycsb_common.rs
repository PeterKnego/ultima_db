// Included via `#[path]` in multiple bench binaries, each using a different subset.
#![allow(dead_code, unused_imports)]

use std::cell::Cell;
use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use rand::Rng;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const NUM_RECORDS: u64 = 10_000;
pub const OPS_PER_ITER: usize = 1_000;
pub const FIELD_SIZE: usize = 100;
pub const ZIPFIAN_CONSTANT: f64 = 0.99;

// ---------------------------------------------------------------------------
// Record type — 10 fields × 100 bytes ≈ 1 KB, matching YCSB spec
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[allow(dead_code)]
pub struct YcsbRecord {
    pub field0: String,
    pub field1: String,
    pub field2: String,
    pub field3: String,
    pub field4: String,
    pub field5: String,
    pub field6: String,
    pub field7: String,
    pub field8: String,
    pub field9: String,
}

impl YcsbRecord {
    pub fn new(seed: u64) -> Self {
        let fill = |offset: u8| -> String {
            let byte = ((seed.wrapping_mul(31).wrapping_add(offset as u64)) & 0xFF) as u8;
            let ch = (b'A' + byte % 26) as char;
            std::iter::repeat_n(ch, FIELD_SIZE).collect()
        };
        Self {
            field0: fill(0),
            field1: fill(1),
            field2: fill(2),
            field3: fill(3),
            field4: fill(4),
            field5: fill(5),
            field6: fill(6),
            field7: fill(7),
            field8: fill(8),
            field9: fill(9),
        }
    }
}

// ---------------------------------------------------------------------------
// Scrambled Zipfian generator
// ---------------------------------------------------------------------------

pub struct ZipfianGenerator {
    item_count: u64,
    theta: f64,
    zeta_n: f64,
    #[allow(dead_code)]
    zeta_2: f64,
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
        Self { item_count, theta, zeta_n, zeta_2, alpha, eta }
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

        // Scramble via FNV-1a to spread hot keys across the keyspace.
        let scrambled = Self::fnv_hash(raw) % self.item_count;
        scrambled + 1 // keys are 1-based
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
// Latest-biased generator (Workload D)
// ---------------------------------------------------------------------------

pub struct LatestGenerator {
    max_id: u64,
}

impl LatestGenerator {
    pub fn new(max_id: u64) -> Self {
        Self { max_id }
    }

    pub fn next(&self, rng: &mut impl Rng) -> u64 {
        // Exponential distribution biased toward max_id.
        let u: f64 = rng.random();
        let offset = (-(self.max_id as f64 * 0.1) * u.ln()) as u64;
        if offset >= self.max_id {
            1
        } else {
            self.max_id - offset
        }
    }
}

// ---------------------------------------------------------------------------
// Operation types
// ---------------------------------------------------------------------------

pub enum YcsbOp {
    Read(u64),
    Update(u64),
    Insert,
    Scan(u64, u64),
    ReadModifyWrite(u64),
}

// ---------------------------------------------------------------------------
// Workload generators — each returns OPS_PER_ITER operations
// ---------------------------------------------------------------------------

pub fn gen_workload_a(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<YcsbOp> {
    (0..OPS_PER_ITER)
        .map(|_| {
            let key = zipf.next(rng);
            if rng.random_bool(0.5) {
                YcsbOp::Read(key)
            } else {
                YcsbOp::Update(key)
            }
        })
        .collect()
}

pub fn gen_workload_b(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<YcsbOp> {
    (0..OPS_PER_ITER)
        .map(|_| {
            let key = zipf.next(rng);
            if rng.random_bool(0.95) {
                YcsbOp::Read(key)
            } else {
                YcsbOp::Update(key)
            }
        })
        .collect()
}

pub fn gen_workload_c(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<YcsbOp> {
    (0..OPS_PER_ITER)
        .map(|_| YcsbOp::Read(zipf.next(rng)))
        .collect()
}

pub fn gen_workload_d(rng: &mut impl Rng, latest: &LatestGenerator) -> Vec<YcsbOp> {
    (0..OPS_PER_ITER)
        .map(|_| {
            if rng.random_bool(0.95) {
                YcsbOp::Read(latest.next(rng))
            } else {
                YcsbOp::Insert
            }
        })
        .collect()
}

pub fn gen_workload_e(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<YcsbOp> {
    (0..OPS_PER_ITER)
        .map(|_| {
            if rng.random_bool(0.95) {
                let start = zipf.next(rng);
                let count = rng.random_range(1..=100);
                YcsbOp::Scan(start, count)
            } else {
                YcsbOp::Insert
            }
        })
        .collect()
}

pub fn gen_workload_f(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<YcsbOp> {
    (0..OPS_PER_ITER)
        .map(|_| {
            let key = zipf.next(rng);
            if rng.random_bool(0.5) {
                YcsbOp::Read(key)
            } else {
                YcsbOp::ReadModifyWrite(key)
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Engine trait — implement this per database to get all 6 YCSB benchmarks
// ---------------------------------------------------------------------------

pub trait YcsbEngine {
    /// Short prefix for benchmark names (e.g. "ultima", "fjall").
    fn name(&self) -> &str;

    /// Execute a batch of YCSB operations against the database.
    fn execute(&mut self, ops: &[YcsbOp]);
}

/// Register all 6 YCSB workloads for the given engine with Criterion.
///
/// Benchmark IDs use the format `ycsb_{workload}/{engine}` so that
/// `critcmp` groups workloads together across engines.
pub fn bench_all_workloads(c: &mut Criterion, engine: &mut impl YcsbEngine) {
    // Workload A: Update Heavy
    {
        let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
        let mut rng = StdRng::seed_from_u64(42);
        let mut group = c.benchmark_group("ycsb_a_update_heavy");
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || gen_workload_a(&mut rng, &zipf),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    // Workload B: Read Mostly
    {
        let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
        let mut rng = StdRng::seed_from_u64(43);
        let mut group = c.benchmark_group("ycsb_b_read_mostly");
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || gen_workload_b(&mut rng, &zipf),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    // Workload C: Read Only
    {
        let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
        let mut rng = StdRng::seed_from_u64(44);
        let mut group = c.benchmark_group("ycsb_c_read_only");
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || gen_workload_c(&mut rng, &zipf),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    // Workload D: Read Latest
    {
        let latest = LatestGenerator::new(NUM_RECORDS);
        let mut rng = StdRng::seed_from_u64(45);
        let mut group = c.benchmark_group("ycsb_d_read_latest");
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || gen_workload_d(&mut rng, &latest),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    // Workload E: Short Ranges
    {
        let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
        let mut rng = StdRng::seed_from_u64(46);
        let mut group = c.benchmark_group("ycsb_e_short_ranges");
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || gen_workload_e(&mut rng, &zipf),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    // Workload F: Read-Modify-Write
    {
        let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
        let mut rng = StdRng::seed_from_u64(47);
        let mut group = c.benchmark_group("ycsb_f_read_modify_write");
        group.throughput(Throughput::Elements(OPS_PER_ITER as u64));
        group.bench_function("burst", |b| {
            b.iter_batched_ref(
                || gen_workload_f(&mut rng, &zipf),
                |ops| engine.execute(ops),
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }
}

/// Standard Criterion config for YCSB benchmarks.
pub fn ycsb_criterion() -> Criterion {
    Criterion::default()
        .sample_size(50)
        .measurement_time(Duration::from_secs(10))
}

// ---------------------------------------------------------------------------
// Multi-writer engine trait — implement per database for contention benchmarks
// ---------------------------------------------------------------------------

/// Result of a single burst of concurrent writers.
pub struct BurstResult {
    pub committed: u64,
    pub conflicts: u64,
}

/// Engine trait for multi-writer contention benchmarks.
///
/// Each engine opens N concurrent transactions, executes updates for each
/// writer's key set, commits in sequence, and retries on conflict.
pub trait MultiWriterEngine {
    /// Short prefix for benchmark names (e.g. "ultima", "rocksdb").
    fn name(&self) -> &str;

    /// Execute a burst of concurrent writers. Each inner `Vec<u64>` is one
    /// writer's set of keys to update. The engine should:
    /// 1. Open one transaction per writer
    /// 2. Execute updates for each writer's keys
    /// 3. Commit in sequence, retrying on conflict
    fn execute_burst(&mut self, key_sets: &[Vec<u64>]) -> BurstResult;

    /// Read back a key and return true if it exists with a valid value.
    /// Used by the smoke test to verify correctness after a burst.
    fn verify_key(&self, key: u64) -> bool;
}

/// Number of concurrent writers in a burst (for low/high contention).
pub const MW_WRITERS: usize = 4;
/// Operations each writer performs per burst.
pub const MW_OPS_PER_WRITER: usize = 50;

/// Register multi-writer contention benchmarks for the given engine.
///
/// Benchmark IDs use the format `multiwriter_{scenario}/{engine}` so that
/// `critcmp` groups scenarios together across engines.
pub fn bench_multiwriter_workloads(c: &mut Criterion, engine: &mut impl MultiWriterEngine) {
    let name = engine.name().to_owned();

    // Smoke test: run one burst and verify all keys are readable afterward
    {
        let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
        let mut rng = StdRng::seed_from_u64(999);
        let key_sets: Vec<Vec<u64>> = (0..MW_WRITERS)
            .map(|_| {
                (0..MW_OPS_PER_WRITER)
                    .map(|_| zipf.next(&mut rng))
                    .collect()
            })
            .collect();
        let result = engine.execute_burst(&key_sets);
        assert_eq!(
            result.committed, MW_WRITERS as u64,
            "{name}: expected all {MW_WRITERS} writers to commit (with retries), got {}",
            result.committed
        );
        for key_set in &key_sets {
            for &key in key_set {
                assert!(
                    engine.verify_key(key),
                    "{name}: key {key} not found after burst commit"
                );
            }
        }
    }

    // Low contention: Zipfian keys, 4 writers, same table
    {
        let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
        let mut rng = StdRng::seed_from_u64(300);
        let total_ops = (MW_WRITERS * MW_OPS_PER_WRITER) as u64;
        let burst_conflicts = Cell::new(0u64);
        let burst_count = Cell::new(0u64);

        let mut group = c.benchmark_group("multiwriter_low_contention");
        group.throughput(Throughput::Elements(total_ops));
        group.bench_function("burst", |b| {
            b.iter(|| {
                let key_sets: Vec<Vec<u64>> = (0..MW_WRITERS)
                    .map(|_| {
                        (0..MW_OPS_PER_WRITER)
                            .map(|_| zipf.next(&mut rng))
                            .collect()
                    })
                    .collect();
                let result = engine.execute_burst(&key_sets);
                burst_conflicts.set(burst_conflicts.get() + result.conflicts);
                burst_count.set(burst_count.get() + 1);
                black_box(result)
            });
        });
        group.finish();
        report_conflicts(&name, "low_contention", burst_count.get(), burst_conflicts.get(), MW_WRITERS);
    }

    // High contention: hot keys 1..=10, 4 writers
    {
        let mut rng = StdRng::seed_from_u64(400);
        let total_ops = (MW_WRITERS * MW_OPS_PER_WRITER) as u64;
        let burst_conflicts = Cell::new(0u64);
        let burst_count = Cell::new(0u64);

        let mut group = c.benchmark_group("multiwriter_high_contention");
        group.throughput(Throughput::Elements(total_ops));
        group.bench_function("burst", |b| {
            b.iter(|| {
                let key_sets: Vec<Vec<u64>> = (0..MW_WRITERS)
                    .map(|_| {
                        (0..MW_OPS_PER_WRITER)
                            .map(|_| rng.random_range(1..=10u64))
                            .collect()
                    })
                    .collect();
                let result = engine.execute_burst(&key_sets);
                burst_conflicts.set(burst_conflicts.get() + result.conflicts);
                burst_count.set(burst_count.get() + 1);
                black_box(result)
            });
        });
        group.finish();
        report_conflicts(&name, "high_contention", burst_count.get(), burst_conflicts.get(), MW_WRITERS);
    }

    // Scaling: vary number of concurrent writers (1, 2, 4, 8)
    {
        let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
        let mut rng = StdRng::seed_from_u64(500);

        let mut group = c.benchmark_group("multiwriter_scaling");
        for num_writers in [1, 2, 4, 8] {
            let total_ops = (num_writers * MW_OPS_PER_WRITER) as u64;
            group.throughput(Throughput::Elements(total_ops));
            let burst_conflicts = Cell::new(0u64);
            let burst_count = Cell::new(0u64);
            group.bench_with_input(
                BenchmarkId::from_parameter(num_writers),
                &num_writers,
                |b, &nw| {
                    b.iter(|| {
                        let key_sets: Vec<Vec<u64>> = (0..nw)
                            .map(|_| {
                                (0..MW_OPS_PER_WRITER)
                                    .map(|_| zipf.next(&mut rng))
                                    .collect()
                            })
                            .collect();
                        let result = engine.execute_burst(&key_sets);
                        burst_conflicts.set(burst_conflicts.get() + result.conflicts);
                        burst_count.set(burst_count.get() + 1);
                        black_box(result)
                    });
                },
            );
            report_conflicts(&name, &format!("scaling/{num_writers}w"), burst_count.get(), burst_conflicts.get(), num_writers);
        }
        group.finish();
    }
}

fn report_conflicts(engine: &str, scenario: &str, bursts: u64, conflicts: u64, writers: usize) {
    if bursts > 0 {
        let avg_conflicts = conflicts as f64 / bursts as f64;
        let conflict_rate = avg_conflicts / writers as f64 * 100.0;
        eprintln!(
            "  [{engine}] {scenario}: {bursts} bursts, {conflicts} total conflicts, \
             avg {avg_conflicts:.1}/burst ({conflict_rate:.1}% of commits)"
        );
    }
}
