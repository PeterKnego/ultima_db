use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use rand::Rng;
use rand::rngs::StdRng;
use rand::SeedableRng;
use ultima_db::Store;
use ultima_db::StoreConfig;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NUM_RECORDS: u64 = 10_000;
const OPS_PER_ITER: usize = 1_000;
const FIELD_SIZE: usize = 100;
const ZIPFIAN_CONSTANT: f64 = 0.99;

// ---------------------------------------------------------------------------
// Record type — 10 fields × 100 bytes ≈ 1 KB, matching YCSB spec
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct YcsbRecord {
    field0: String,
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
}

impl YcsbRecord {
    fn new(seed: u64) -> Self {
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

struct ZipfianGenerator {
    item_count: u64,
    theta: f64,
    zeta_n: f64,
    #[allow(dead_code)]
    zeta_2: f64,
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
        Self { item_count, theta, zeta_n, zeta_2, alpha, eta }
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

struct LatestGenerator {
    max_id: u64,
}

impl LatestGenerator {
    fn new(max_id: u64) -> Self {
        Self { max_id }
    }

    fn next(&self, rng: &mut impl Rng) -> u64 {
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

enum YcsbOp {
    Read(u64),
    Update(u64),
    Insert,
    Scan(u64, u64),
    ReadModifyWrite(u64),
}

// ---------------------------------------------------------------------------
// Preload helper
// ---------------------------------------------------------------------------

fn preload_store() -> Store {
    let store = Store::new(StoreConfig {
        num_snapshots_retained: 2,
        auto_snapshot_gc: true,
    });
    let mut wtx = store.begin_write(None).unwrap();
    {
        let table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
        for i in 1..=NUM_RECORDS {
            table.insert(YcsbRecord::new(i)).unwrap();
        }
    }
    wtx.commit().unwrap();
    store
}

// ---------------------------------------------------------------------------
// Operation executor
// ---------------------------------------------------------------------------

fn execute_ops(store: &Store, ops: &[YcsbOp]) {
    for op in ops {
        match op {
            YcsbOp::Read(key) => {
                let rtx = store.begin_read(None).unwrap();
                let table = rtx.open_table::<YcsbRecord>("ycsb").unwrap();
                black_box(table.get(*key));
            }
            YcsbOp::Update(key) => {
                let mut wtx = store.begin_write(None).unwrap();
                let table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                let _ = table.update(*key, YcsbRecord::new(key.wrapping_add(1)));
                wtx.commit().unwrap();
            }
            YcsbOp::Insert => {
                let mut wtx = store.begin_write(None).unwrap();
                let table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                let id = table.insert(YcsbRecord::new(0)).unwrap();
                black_box(id);
                wtx.commit().unwrap();
            }
            YcsbOp::Scan(start, count) => {
                let rtx = store.begin_read(None).unwrap();
                let table = rtx.open_table::<YcsbRecord>("ycsb").unwrap();
                for item in table.range(*start..start.saturating_add(*count)) {
                    black_box(item);
                }
            }
            YcsbOp::ReadModifyWrite(key) => {
                // Read phase
                let record = {
                    let rtx = store.begin_read(None).unwrap();
                    let table = rtx.open_table::<YcsbRecord>("ycsb").unwrap();
                    table.get(*key).cloned()
                };
                // Modify + write phase
                if let Some(mut rec) = record {
                    rec.field0 = std::iter::repeat_n('X', FIELD_SIZE).collect();
                    let mut wtx = store.begin_write(None).unwrap();
                    let table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                    let _ = table.update(*key, rec);
                    wtx.commit().unwrap();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Workload generators — each returns OPS_PER_ITER operations
// ---------------------------------------------------------------------------

fn gen_workload_a(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<YcsbOp> {
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

fn gen_workload_b(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<YcsbOp> {
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

fn gen_workload_c(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<YcsbOp> {
    (0..OPS_PER_ITER)
        .map(|_| YcsbOp::Read(zipf.next(rng)))
        .collect()
}

fn gen_workload_d(rng: &mut impl Rng, latest: &LatestGenerator) -> Vec<YcsbOp> {
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

fn gen_workload_e(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<YcsbOp> {
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

fn gen_workload_f(rng: &mut impl Rng, zipf: &ZipfianGenerator) -> Vec<YcsbOp> {
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
// Benchmark functions
// ---------------------------------------------------------------------------

fn bench_workload_a(c: &mut Criterion) {
    let store = preload_store();
    let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
    let mut rng = StdRng::seed_from_u64(42);

    c.bench_function("ycsb_a_update_heavy", |b| {
        b.iter_batched_ref(
            || gen_workload_a(&mut rng, &zipf),
            |ops| execute_ops(&store, ops),
            BatchSize::SmallInput,
        );
    });
}

fn bench_workload_b(c: &mut Criterion) {
    let store = preload_store();
    let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
    let mut rng = StdRng::seed_from_u64(43);

    c.bench_function("ycsb_b_read_mostly", |b| {
        b.iter_batched_ref(
            || gen_workload_b(&mut rng, &zipf),
            |ops| execute_ops(&store, ops),
            BatchSize::SmallInput,
        );
    });
}

fn bench_workload_c(c: &mut Criterion) {
    let store = preload_store();
    let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
    let mut rng = StdRng::seed_from_u64(44);

    c.bench_function("ycsb_c_read_only", |b| {
        b.iter_batched_ref(
            || gen_workload_c(&mut rng, &zipf),
            |ops| execute_ops(&store, ops),
            BatchSize::SmallInput,
        );
    });
}

fn bench_workload_d(c: &mut Criterion) {
    let store = preload_store();
    let latest = LatestGenerator::new(NUM_RECORDS);
    let mut rng = StdRng::seed_from_u64(45);

    c.bench_function("ycsb_d_read_latest", |b| {
        b.iter_batched_ref(
            || gen_workload_d(&mut rng, &latest),
            |ops| execute_ops(&store, ops),
            BatchSize::SmallInput,
        );
    });
}

fn bench_workload_e(c: &mut Criterion) {
    let store = preload_store();
    let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
    let mut rng = StdRng::seed_from_u64(46);

    c.bench_function("ycsb_e_short_ranges", |b| {
        b.iter_batched_ref(
            || gen_workload_e(&mut rng, &zipf),
            |ops| execute_ops(&store, ops),
            BatchSize::SmallInput,
        );
    });
}

fn bench_workload_f(c: &mut Criterion) {
    let store = preload_store();
    let zipf = ZipfianGenerator::new(NUM_RECORDS, ZIPFIAN_CONSTANT);
    let mut rng = StdRng::seed_from_u64(47);

    c.bench_function("ycsb_f_read_modify_write", |b| {
        b.iter_batched_ref(
            || gen_workload_f(&mut rng, &zipf),
            |ops| execute_ops(&store, ops),
            BatchSize::SmallInput,
        );
    });
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

criterion_group! {
    name = ycsb;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
        .sample_size(50)
        .measurement_time(Duration::from_secs(10));
    targets = bench_workload_a, bench_workload_b, bench_workload_c,
              bench_workload_d, bench_workload_e, bench_workload_f
}
criterion_main!(ycsb);
