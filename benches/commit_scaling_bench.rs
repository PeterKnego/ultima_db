// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Does MultiWriter commit throughput scale with the writer count when writers
//! touch DISJOINT data (no logical conflicts)?
//!
//! Sweeps 1/2/4/8 persistent writer threads across two contention shapes and
//! three persistence tiers, reporting aggregate commits/sec (read the `thrpt`
//! line; scaling = thrpt(N) / thrpt(1)).
//!
//! Shapes:
//! - `same_table` — all writers update one shared table on disjoint key ranges.
//!   They never logically conflict, but they all serialize on that table's
//!   per-table commit lock (see `commit_multi_writer` Phase 0 in src/store.rs).
//! - `disjoint_table` — each writer owns its own table. The merge phase runs in
//!   parallel (distinct table locks), but every commit still serializes through
//!   the brief global write lock in Phase 3 (version finalize + snapshot install
//!   + promote gate).
//!
//! Tiers: `inmem` (Persistence::None), `smr` (checkpoint-only — commit does no
//! I/O), `consistent` (Standalone, per-commit fsync inside three-phase commit).
//!
//! Expected result: for `inmem`/`smr`, disjoint keys give NO speedup and in fact
//! negative scaling — the commit critical section + cross-core cacheline traffic
//! dominates tiny in-memory commits, so adding writers lowers aggregate
//! throughput. Concurrency only pays when there is expensive per-commit I/O to
//! overlap: under `consistent`, three-phase commit releases the store lock during
//! fsync, so multiple writers overlap their fsync waits and throughput climbs
//! with the writer count.
//!
//! Measured (2026-06-18, Claude sandbox — noisy/virtualized, few cores; re-run
//! on the real bench host for authoritative numbers). Aggregate commits/sec by
//! writer count (`smr` tracks `inmem` and is omitted):
//!
//! | tier / shape                  | N=1   | N=2   | N=4   | N=8   |
//! |-------------------------------|-------|-------|-------|-------|
//! | inmem / same_table            | ~469K | ~380K | ~268K | ~227K |
//! | inmem / disjoint_table        | ~452K | ~588K | ~298K | ~234K |
//! | consistent / same_table       | ~26K* | ~33K  | ~32K  | ~30K  |
//! | consistent / disjoint_table   | ~25K* | ~63K  | ~58K  | ~29K  |
//!
//! (*the N=1 `consistent` baseline is noisy on this host; the N=2 jump is the
//! reliable signal.) Reading: in-memory commits do NOT scale on disjoint keys —
//! same-table goes negative immediately (lock-bound), disjoint-table gets one
//! step at N=2 then declines. Under `consistent`, concurrency clearly helps —
//! disjoint-table ~2.5× at N=2 from fsync overlap — until the writer count
//! exceeds the core count and contention takes over again (N=8 collapse). So
//! disjoint keys buy conflict-freedom, not commit-throughput scaling; scaling
//! comes from overlapping expensive per-commit I/O.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::JoinHandle;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ultima_db::{Persistence, Store, StoreConfig, WalWrite, WriterMode};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Record {
    value: u64,
}

const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];
const COMMITS_PER_THREAD: u64 = 100;

/// A table name and the number of rows to preload into it.
type TableSpec = (String, u64);
/// Per-worker `(table, key_base)` assignment.
type Assignment = (String, u64);
/// Builds `(tables_to_preload, per_worker_assignments)` for a given thread count.
type ShapeSetup = fn(usize) -> (Vec<TableSpec>, Vec<Assignment>);

#[derive(Clone, Copy)]
enum Tier {
    Inmem,
    Smr,
    Consistent,
}

impl Tier {
    fn label(self) -> &'static str {
        match self {
            Tier::Inmem => "inmem",
            Tier::Smr => "smr",
            Tier::Consistent => "consistent",
        }
    }
}

/// One transaction: update a single preloaded key, blocking on the holder's
/// waiter then retrying if a (here unexpected) conflict surfaces.
fn commit_one(store: &Store, table: &str, key: u64) {
    loop {
        let mut wtx = store.begin_write(None).unwrap();
        let res = wtx
            .open_table::<Record>(table)
            .unwrap()
            .update(key, Record { value: key });
        if let Err(ultima_db::Error::WriteConflict { wait_for, .. }) = res {
            drop(wtx);
            if let Some(w) = wait_for {
                w.wait();
            }
            continue;
        }
        res.unwrap();
        match wtx.commit() {
            Ok(_) => break,
            Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                if let Some(w) = wait_for {
                    w.wait();
                }
                continue;
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
}

/// Build a MultiWriter store, register/preload the named tables (`rows` rows
/// each), and return it. `tmpdir` supplies the directory for durable tiers.
fn make_store(tier: Tier, tmpdir: Option<&std::path::Path>, tables: &[(String, u64)]) -> Store {
    let persistence = match (tier, tmpdir) {
        (Tier::Inmem, _) => Persistence::None,
        (Tier::Smr, Some(d)) => Persistence::smr(d.to_path_buf()),
        (Tier::Consistent, Some(d)) => Persistence::standalone(
            d.to_path_buf(),
            ultima_db::Durability::Consistent,
            WalWrite::PerEntry,
        ),
        _ => Persistence::None,
    };
    let durable = !matches!(persistence, Persistence::None);

    let store = Store::new(
        StoreConfig::builder()
            .num_snapshots_retained(2)
            .writer_mode(WriterMode::MultiWriter)
            .persistence(persistence)
            .build(),
    )
    .unwrap();
    for (name, _) in tables {
        store.register_table::<Record>(name.as_str()).unwrap();
    }
    if durable {
        store.recover().unwrap();
    }
    let mut wtx = store.begin_write(None).unwrap();
    for (name, rows) in tables {
        let mut t = wtx.open_table::<Record>(name.as_str()).unwrap();
        for i in 0..*rows {
            t.insert(Record { value: i }).unwrap();
        }
    }
    wtx.commit().unwrap();
    store
}

/// A persistent pool of writer threads, one per `(table, key_base)` assignment.
/// `run_batch` releases all workers to each commit COMMITS_PER_THREAD updates
/// (keys `key_base+1 ..= key_base+COMMITS_PER_THREAD` on their table) and blocks
/// until they finish — no thread spawn in the timed path.
struct Pool {
    start: Arc<Barrier>,
    end: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
}

impl Pool {
    fn new(store: &Store, assignments: Vec<(String, u64)>) -> Self {
        let n = assignments.len();
        let start = Arc::new(Barrier::new(n + 1));
        let end = Arc::new(Barrier::new(n + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let handles = assignments
            .into_iter()
            .map(|(table, key_base)| {
                let store = store.clone();
                let start = Arc::clone(&start);
                let end = Arc::clone(&end);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    loop {
                        start.wait();
                        if stop.load(Ordering::Acquire) {
                            break;
                        }
                        for i in 0..COMMITS_PER_THREAD {
                            commit_one(&store, &table, key_base + i + 1);
                        }
                        end.wait();
                    }
                })
            })
            .collect();
        Pool {
            start,
            end,
            stop,
            handles,
        }
    }

    #[inline]
    fn run_batch(&self) {
        self.start.wait();
        self.end.wait();
    }

    fn shutdown(self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for h in self.handles {
            h.join().unwrap();
        }
    }
}

/// Tables + per-worker assignments for the `same_table` shape: one shared table,
/// worker `t` owns the disjoint key block `[t*CPT .. (t+1)*CPT]`.
fn same_table_setup(n: usize) -> (Vec<TableSpec>, Vec<Assignment>) {
    let tables = vec![("data".to_string(), n as u64 * COMMITS_PER_THREAD)];
    let assignments = (0..n)
        .map(|t| ("data".to_string(), t as u64 * COMMITS_PER_THREAD))
        .collect();
    (tables, assignments)
}

/// Tables + assignments for the `disjoint_table` shape: one table per worker.
fn disjoint_table_setup(n: usize) -> (Vec<TableSpec>, Vec<Assignment>) {
    let tables: Vec<(String, u64)> = (0..n)
        .map(|t| (format!("data{t}"), COMMITS_PER_THREAD))
        .collect();
    let assignments = (0..n).map(|t| (format!("data{t}"), 0)).collect();
    (tables, assignments)
}

fn bench_commit_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("commit_scaling");

    let shapes: &[(&str, ShapeSetup)] = &[
        ("same_table", same_table_setup),
        ("disjoint_table", disjoint_table_setup),
    ];

    for &tier in &[Tier::Inmem, Tier::Smr, Tier::Consistent] {
        for &(shape, setup) in shapes {
            for &n in THREAD_COUNTS {
                let (tables, assignments) = setup(n);
                // Keep the tempdir alive for the whole bench_function.
                let tmpdir = match tier {
                    Tier::Inmem => None,
                    _ => Some(tempfile::tempdir_in(ultima_bench_workloads::ycsb::bench_disk_dir()).unwrap()),
                };
                let store = make_store(tier, tmpdir.as_ref().map(|d| d.path()), &tables);
                let pool = Pool::new(&store, assignments);

                group.throughput(Throughput::Elements(n as u64 * COMMITS_PER_THREAD));
                let id = BenchmarkId::new(format!("{}/{}", tier.label(), shape), n);
                group.bench_function(id, |b| {
                    b.iter(|| {
                        pool.run_batch();
                        black_box(());
                    });
                });

                pool.shutdown();
            }
        }
    }

    group.finish();
}

criterion_group! {
    name = commit_scaling;
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(Duration::from_secs(6));
    targets = bench_commit_scaling
}
criterion_main!(commit_scaling);
