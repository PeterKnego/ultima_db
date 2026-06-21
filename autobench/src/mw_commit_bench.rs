// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Fitness function for the `multiwriter-commit` task. Stresses the
//! MultiWriter OCC commit path: concurrent writers with key-level conflict
//! detection. Two contention shapes are measured:
//!   * Phase A (disjoint) — each writer owns a private id range, so every
//!     commit takes the fast path (single Arc swap, no merge).
//!   * Phase B (mixed) — a shared hot set interleaved with cold keys by pure
//!     index arithmetic, so commits collide and take the `commit_multi_writer`
//!     merge slow path (`merge_keys_from`/`upsert_arc` + index maintenance).
//!
//! A concurrent reader runs over Phase B to measure read latency under write
//! contention. No disk is used (Persistence::None).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use ultima_db::{IndexKind, Persistence, Store, StoreConfig, WriterMode};

use crate::sampling::percentile;

/// Every key `run()` emits, in stable order. The smoke test and
/// `--write-baseline` iterate this list.
pub const METRIC_KEYS: &[&str] = &[
    "mw_commit_throughput",
    "mw_commit_p99_ns",
    "mw_disjoint_throughput",
    "mw_scaling_8x",
    "mw_scaling_efficiency",
    "mw_conflict_rate",
    "read_p99_under_load_ns",
];

/// Writer count for the scaling metric (`mw_scaling_8x`). High enough to expose
/// the serialized commit-install critical section: with disjoint id ranges there
/// are no logical conflicts, so any throughput ceiling at this width is the
/// `inner.write()` hold time during snapshot install, not OCC.
const SCALING_WRITERS: u64 = 8;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct MwRow {
    key: u64,
    val: u64,
    blob: Vec<u8>, // 56 bytes: keeps rows non-trivial without dwarfing them
}

impl MwRow {
    fn new(key: u64, val: u64) -> Self {
        Self {
            key,
            val,
            blob: vec![0xAB; 56],
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub keyspace: u64,
    pub writers: u64,
    pub keys_per_commit: u64,
    pub rounds: u64,
    pub disjoint_rounds: u64,
    pub warmup_rounds: u64,
    /// Cold-key stride: keeps each writer's cold-key walk well-spread and
    /// distinct from its neighbours so cold commits stay on the fast path.
    pub conflict_stride: u64,
}

impl Config {
    pub fn standard() -> Self {
        Self {
            keyspace: 50_000,
            writers: 4,
            keys_per_commit: 4,
            rounds: 4_000,
            disjoint_rounds: 4_000,
            warmup_rounds: 200,
            conflict_stride: 97,
        }
    }

    pub fn quick() -> Self {
        Self {
            keyspace: 2_000,
            // 4 writers contending on a small shared hot set make a
            // zero-conflict run statistically impossible across `rounds`
            // (the mixed-phase conflict rate is timing-dependent: a writer
            // only conflicts if it stages a hot key while another holds that
            // key's intent). 2 writers / 50 rounds was too thin and the smoke
            // test's `mw_conflict_rate > 0` assertion flaked ~40% of the time.
            writers: 4,
            keys_per_commit: 2,
            rounds: 250,
            disjoint_rounds: 250,
            warmup_rounds: 5,
            conflict_stride: 13,
        }
    }

    /// `standard()` unless AUTOBENCH_QUICK=1.
    pub fn from_env() -> Self {
        if std::env::var("AUTOBENCH_QUICK").as_deref() == Ok("1") {
            Self::quick()
        } else {
            Self::standard()
        }
    }
}

/// Size of the shared hot set in Phase B. Kept small so the deterministic key
/// schedule reliably collides across writers (drives the merge slow path).
fn hot_size(cfg: &Config) -> u64 {
    cfg.writers * 2
}

fn mw_store() -> Store {
    Store::new(StoreConfig {
        writer_mode: WriterMode::MultiWriter,
        persistence: Persistence::None,
        ..StoreConfig::default()
    })
    .unwrap()
}

/// Insert `rows` rows, then (in a separate single-writer txn, no concurrency)
/// define a NonUnique secondary index on a bucketed `val` so the merge slow
/// path also exercises index maintenance.
fn preload(store: &Store, rows: u64) {
    {
        let mut tx = store.begin_write(None).unwrap();
        {
            let mut t = tx.open_table::<MwRow>("state").unwrap();
            let batch: Vec<MwRow> = (1..=rows).map(|i| MwRow::new(i, 0)).collect();
            t.insert_batch(batch).unwrap();
        }
        tx.commit().unwrap();
    }
    // Define the index in its own transaction with no concurrent writers
    // (CLAUDE.md: index DDL under MultiWriter must not race).
    {
        let mut tx = store.begin_write(None).unwrap();
        {
            let mut t = tx.open_table::<MwRow>("state").unwrap();
            t.define_index("val_bucket", IndexKind::NonUnique, |r: &MwRow| r.val % 64)
                .unwrap();
        }
        tx.commit().unwrap();
    }
}

/// Phase A: `writers` threads, each owning a disjoint id range, run
/// `disjoint_rounds` rounds updating `keys_per_commit` ids inside its OWN range.
/// No cross-writer overlap → every commit takes the fast path. Returns commits/sec.
fn run_phase_disjoint(cfg: &Config) -> f64 {
    let store = mw_store();
    preload(&store, cfg.keyspace);
    let range = (cfg.keyspace / cfg.writers).max(cfg.keys_per_commit + 1);
    let barrier = Arc::new(Barrier::new(cfg.writers as usize));

    let t = Instant::now();
    let handles: Vec<_> = (0..cfg.writers)
        .map(|w| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            let rounds = cfg.disjoint_rounds;
            let kpc = cfg.keys_per_commit;
            std::thread::spawn(move || {
                let lo = w * range + 1; // writer w owns lo ..= lo+range-1
                let mut rot = 0u64;
                for _ in 0..rounds {
                    barrier.wait();
                    loop {
                        let mut tx = store.begin_write(None).unwrap();
                        let write_res: Result<(), ultima_db::Error> = {
                            let mut tbl = tx.open_table::<MwRow>("state").unwrap();
                            let mut res = Ok(());
                            for j in 0..kpc {
                                let id = lo + (rot + j) % range;
                                if let Err(e) = tbl.update(id, MwRow::new(id, rot + j)) {
                                    res = Err(e);
                                    break;
                                }
                            }
                            res
                        };
                        match write_res {
                            Ok(()) => {}
                            Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                                drop(tx);
                                if let Some(waiter) = wait_for {
                                    waiter.wait();
                                }
                                continue;
                            }
                            Err(e) => panic!("mw-disjoint: write error: {e}"),
                        }
                        match tx.commit() {
                            Ok(_) => break,
                            Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                                if let Some(waiter) = wait_for {
                                    waiter.wait();
                                }
                                continue;
                            }
                            Err(e) => panic!("mw-disjoint: commit error: {e}"),
                        }
                    }
                    rot += kpc;
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    (cfg.writers * cfg.disjoint_rounds) as f64 / secs
}

/// Run the disjoint phase with the writer count overridden to `writers`.
/// Used for the scaling metric: aggregate commits/sec at 1 vs SCALING_WRITERS
/// disjoint writers isolates how well the commit path scales with concurrency.
fn run_phase_disjoint_n(cfg: &Config, writers: u64) -> f64 {
    let local = Config {
        writers,
        ..cfg.clone()
    };
    run_phase_disjoint(&local)
}

/// One mixed-phase commit's key set for writer `w`, round `r`: keys alternate
/// between a shared hot key (collision-prone → slow path) and a cold key in the
/// writer's own stride lane. Pure index arithmetic — deterministic, no rand.
fn mixed_keys(cfg: &Config, w: u64, r: u64) -> Vec<u64> {
    let hot = hot_size(cfg);
    let kpc = cfg.keys_per_commit;
    let big = w.wrapping_mul(1_000_003).wrapping_add(1); // per-writer offset
    (0..kpc)
        .map(|j| {
            if (r + j).is_multiple_of(2) {
                1 + ((r * kpc + j) % hot) // shared hot key
            } else {
                hot + 1 + ((big + r * cfg.conflict_stride + j) % (cfg.keyspace - hot))
            }
        })
        .collect()
}

/// Run the mixed read-modify-write logic for one writer over `rounds` rounds,
/// barrier-synchronised per round so the conflict rate is set by the key
/// schedule, not commit speed. Records per-commit latency on success; counts
/// conflicts at both the write-site and commit-site. Returns latency samples.
fn run_mixed_writer(
    cfg: &Config,
    store: &Store,
    w: u64,
    rounds: u64,
    barrier: &Arc<Barrier>,
    conflicts: &Arc<AtomicU64>,
) -> Vec<f64> {
    let mut lat: Vec<f64> = Vec::with_capacity(rounds as usize);
    for r in 0..rounds {
        barrier.wait();
        let keys = mixed_keys(cfg, w, r);
        let t = Instant::now();
        loop {
            let mut tx = store.begin_write(None).unwrap();
            let write_res: Result<(), ultima_db::Error> = {
                let mut tbl = tx.open_table::<MwRow>("state").unwrap();
                let mut res = Ok(());
                for &id in &keys {
                    // Read-modify-write so overlapping commits produce real merges.
                    let cur = tbl.get(id).map(|row| row.val).unwrap_or(0);
                    if let Err(e) = tbl.update(id, MwRow::new(id, cur + 1)) {
                        res = Err(e);
                        break;
                    }
                }
                res
            };
            match write_res {
                Ok(()) => {}
                Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                    conflicts.fetch_add(1, Ordering::Relaxed);
                    drop(tx);
                    if let Some(waiter) = wait_for {
                        waiter.wait();
                    }
                    continue;
                }
                Err(e) => panic!("mw-mixed: write error: {e}"),
            }
            match tx.commit() {
                Ok(_) => break,
                Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                    conflicts.fetch_add(1, Ordering::Relaxed);
                    if let Some(waiter) = wait_for {
                        waiter.wait();
                    }
                    continue;
                }
                Err(e) => panic!("mw-mixed: commit error: {e}"),
            }
        }
        lat.push(t.elapsed().as_nanos() as f64);
    }
    lat
}

/// Phase B: `writers` threads on a fresh store run `rounds` mixed commits each.
/// Returns (throughput, pooled latency samples, total conflict count).
fn run_phase_mixed(cfg: &Config, store: &Store) -> (f64, Vec<f64>, u64) {
    let conflicts = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(cfg.writers as usize));
    let t = Instant::now();
    let handles: Vec<_> = (0..cfg.writers)
        .map(|w| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            let conflicts = Arc::clone(&conflicts);
            let cfg_writers = cfg.writers;
            let cfg_kpc = cfg.keys_per_commit;
            let cfg_keyspace = cfg.keyspace;
            let cfg_stride = cfg.conflict_stride;
            let rounds = cfg.rounds;
            std::thread::spawn(move || {
                let local = Config {
                    keyspace: cfg_keyspace,
                    writers: cfg_writers,
                    keys_per_commit: cfg_kpc,
                    rounds,
                    disjoint_rounds: rounds,
                    warmup_rounds: 0,
                    conflict_stride: cfg_stride,
                };
                run_mixed_writer(&local, &store, w, rounds, &barrier, &conflicts)
            })
        })
        .collect();
    let mut all_lat: Vec<f64> = Vec::new();
    for h in handles {
        all_lat.extend(h.join().unwrap());
    }
    let secs = t.elapsed().as_secs_f64();
    let throughput = (cfg.writers * cfg.rounds) as f64 / secs;
    (throughput, all_lat, conflicts.load(Ordering::Relaxed))
}

pub fn run(cfg: &Config) -> BTreeMap<String, f64> {
    let mut m = BTreeMap::new();

    // -- Warmup: run the mixed logic on a throwaway store, discard results.
    if cfg.warmup_rounds > 0 {
        let warm = Config {
            keyspace: cfg.keyspace,
            writers: cfg.writers,
            keys_per_commit: cfg.keys_per_commit,
            rounds: cfg.warmup_rounds,
            disjoint_rounds: cfg.warmup_rounds,
            warmup_rounds: 0,
            conflict_stride: cfg.conflict_stride,
        };
        let store = mw_store();
        preload(&store, warm.keyspace);
        let _ = run_phase_mixed(&warm, &store);
    }

    // -- Phase A: disjoint writers (fast path) → mw_disjoint_throughput.
    let disjoint_tput = run_phase_disjoint(cfg);

    // -- Scaling probe: aggregate disjoint throughput at 1 vs SCALING_WRITERS.
    // Disjoint ids → no logical conflict, so the only thing limiting the
    // wide run is the serialized commit-install critical section. `8x` is the
    // gated target (maximize); `efficiency` = wide/single is the Goodhart guard
    // (8.0 = perfect linear scaling; <1 = adding writers loses throughput).
    let single_tput = run_phase_disjoint_n(cfg, 1);
    let scaling_tput = run_phase_disjoint_n(cfg, SCALING_WRITERS);
    let scaling_efficiency = if single_tput > 0.0 {
        scaling_tput / single_tput
    } else {
        0.0
    };

    // -- Phase B: mixed overlap (slow path) on a fresh store, with a
    // concurrent reader thread measuring read latency under load.
    let store = mw_store();
    preload(&store, cfg.keyspace);
    let stop = Arc::new(AtomicBool::new(false));
    let keyspace = cfg.keyspace;
    let reader = {
        let store = store.clone();
        let stop = stop.clone();
        std::thread::spawn(move || -> Vec<f64> {
            let mut samples: Vec<f64> = Vec::with_capacity(1_000_000);
            let mut k = 1u64;
            while !stop.load(Ordering::Relaxed) {
                k = k.wrapping_add(2_654_435_761) % keyspace + 1;
                let t = Instant::now();
                let r = store.begin_read(None).unwrap();
                let tbl = r.open_table::<MwRow>("state").unwrap();
                std::hint::black_box(tbl.get(k));
                samples.push(t.elapsed().as_nanos() as f64);
            }
            samples
        })
    };

    let (commit_tput, mut lat, conflict_count) = run_phase_mixed(cfg, &store);

    stop.store(true, Ordering::Relaxed);
    let mut read_samples = reader.join().unwrap();

    let total_attempts = (cfg.writers * cfg.rounds) as f64;
    let conflict_rate = conflict_count as f64 / total_attempts;

    eprintln!(
        "[autobench] mw-commit conflicts = {} over {} commits (rate {:.3})",
        conflict_count, total_attempts as u64, conflict_rate
    );

    m.insert("mw_commit_throughput".into(), commit_tput);
    m.insert("mw_commit_p99_ns".into(), percentile(&mut lat, 99.0));
    m.insert("mw_disjoint_throughput".into(), disjoint_tput);
    m.insert("mw_scaling_8x".into(), scaling_tput);
    m.insert("mw_scaling_efficiency".into(), scaling_efficiency);
    m.insert("mw_conflict_rate".into(), conflict_rate);
    m.insert(
        "read_p99_under_load_ns".into(),
        percentile(&mut read_samples, 99.0),
    );
    m
}
