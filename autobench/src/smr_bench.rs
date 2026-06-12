// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Fitness function for the `smr-apply` task. Reproduces the cluster apply
//! loop: Persistence::Smr, per-entry WriteTx pinned to the log index,
//! reader thread on begin_read(None), snapshot+checkpoint cadence; plus
//! multi-txn iterations (T=8 sequential, and T=8 across 4 OCC writers).

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use ultima_db::{InstallOptions, Persistence, Store, StoreConfig, WriterMode};

use crate::diskcheck;
use crate::sampling::percentile;

/// Every key `run()` emits, in stable order. The smoke test and
/// `--write-baseline` iterate this list.
pub const METRIC_KEYS: &[&str] = &[
    "apply_p99_ns",
    "apply_throughput",
    "apply_sw_batch_throughput",
    "apply_mw_throughput",
    "apply_mw_p99_ns",
    "read_p99_under_load_ns",
    "snapshot_freeze_p99_us",
    "snapshot_stream_ms",
    "checkpoint_ms",
    "restore_install_ms",
];

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SmrRow {
    pub key: u64,
    pub val: u64,
    pub blob: Vec<u8>, // 56 bytes: keeps rows non-trivial without dwarfing them
}

impl SmrRow {
    fn new(key: u64, val: u64) -> Self {
        Self {
            key,
            val,
            blob: vec![0xCD; 56],
        }
    }
}

pub struct Config {
    pub preload_rows: u64,
    pub apply_iters: u64,
    /// Snapshot freeze+stream+checkpoint every this many iterations.
    /// Spec §4 says "every 5000" (the cluster default); we use 2000 so a
    /// 20k-iteration run yields ~10 snapshot events — enough samples for a
    /// stable percentile. Cadence changes sample count, not per-event cost.
    pub snapshot_every: u64,
    pub batch_iters: usize, // rounds for the T=8 sequential metric
    pub mw_iters: usize,    // rounds for the 4-writer OCC metric
    pub allow_tmpfs: bool,
}

impl Config {
    pub fn standard() -> Self {
        Self {
            preload_rows: 100_000,
            apply_iters: 20_000,
            snapshot_every: 2_000,
            batch_iters: 500,
            mw_iters: 300,
            allow_tmpfs: false,
        }
    }

    pub fn quick() -> Self {
        Self {
            preload_rows: 5_000,
            apply_iters: 1_000,
            snapshot_every: 250,
            batch_iters: 20,
            mw_iters: 10,
            allow_tmpfs: true,
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

// Batch=1: each apply (~µs-scale) is far above clock resolution, and the p99
// must see individual slow commits, not 32-apply means.
const APPLY_BATCH: u64 = 1;

fn smr_store(dir: &std::path::Path, mode: WriterMode) -> Store {
    let store = Store::new(StoreConfig {
        writer_mode: mode,
        persistence: Persistence::Smr {
            dir: dir.to_path_buf(),
        },
        ..StoreConfig::default()
    })
    .unwrap();
    store.register_table::<SmrRow>("state").unwrap();
    store.recover().unwrap();
    store
}

pub fn run(cfg: &Config) -> BTreeMap<String, f64> {
    let root = diskcheck::bench_root("autobench-smr");
    diskcheck::assert_real_disk(&root, cfg.allow_tmpfs);
    let mut m = BTreeMap::new();

    // -- (a) Main store: T=1 pinned apply loop + reader thread + snapshot cadence.
    // Mirrors the cluster apply path: Persistence::Smr SingleWriter, one
    // WriteTx per log entry pinned to the (monotonic) index, a concurrent
    // reader on begin_read(None), and a periodic freeze→stream→checkpoint.
    let main_dir = tempfile::Builder::new()
        .prefix("smr")
        .tempdir_in(&root)
        .unwrap();
    let store = smr_store(main_dir.path(), WriterMode::SingleWriter);
    let rows = cfg.preload_rows;

    // Preload `rows` rows under a single pinned write (version 1).
    {
        let mut tx = store.begin_write(Some(1)).unwrap();
        {
            let mut t = tx.open_table::<SmrRow>("state").unwrap();
            let batch: Vec<SmrRow> = (1..=rows).map(|i| SmrRow::new(i, 0)).collect();
            t.insert_batch(batch).unwrap();
        }
        tx.commit().unwrap();
    }

    // Reader thread: tight begin_read(None) + get loop, golden-ratio key
    // rotation to defeat caching, recording per-op latency until stopped.
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let store = store.clone();
        let stop = stop.clone();
        std::thread::spawn(move || -> Vec<f64> {
            // Pre-allocate generously: at standard config this loop can spin
            // millions of times over the apply window (~tens of MB of f64s).
            let mut samples: Vec<f64> = Vec::with_capacity(4_000_000);
            let mut k = 1u64;
            while !stop.load(Ordering::Relaxed) {
                k = k.wrapping_add(2_654_435_761) % rows + 1;
                let t = Instant::now();
                let r = store.begin_read(None).unwrap();
                let tbl = r.open_table::<SmrRow>("state").unwrap();
                std::hint::black_box(tbl.get(k));
                samples.push(t.elapsed().as_nanos() as f64);
            }
            samples
        })
    };

    let mut apply_samples: Vec<f64> = Vec::with_capacity(cfg.apply_iters as usize + 1);
    let mut freeze_us: Vec<f64> = Vec::new();
    let mut stream_ms: Vec<f64> = Vec::new();
    let mut checkpoint_ms: Vec<f64> = Vec::new();

    let mut idx = 1u64; // last pinned version (preload used version 1)
    let mut applied = 0u64;
    let loop_t = Instant::now();
    while applied < cfg.apply_iters {
        // Snapshot cadence: between batches, fire a freeze→stream→checkpoint
        // when this batch crosses a `snapshot_every` boundary. Calling
        // snapshot_stream(None) immediately after a commit on the latest
        // version is GC-safe (default config retains 10 snapshots).
        if (idx - 1) % cfg.snapshot_every < APPLY_BATCH {
            let t = Instant::now();
            let mut rdr = store.snapshot_stream(None).unwrap();
            freeze_us.push(t.elapsed().as_nanos() as f64 / 1e3);

            let t = Instant::now();
            let mut buf = Vec::new();
            rdr.read_to_end(&mut buf).unwrap();
            stream_ms.push(t.elapsed().as_secs_f64() * 1e3);

            let t = Instant::now();
            store.checkpoint().unwrap();
            checkpoint_ms.push(t.elapsed().as_secs_f64() * 1e3);
        }

        // Time one batch of APPLY_BATCH single-entry applies; record the
        // per-entry mean as one sample.
        let t = Instant::now();
        for _ in 0..APPLY_BATCH {
            idx += 1;
            let id = idx % rows + 1;
            let mut tx = store.begin_write(Some(idx)).unwrap();
            {
                let mut tbl = tx.open_table::<SmrRow>("state").unwrap();
                tbl.update(id, SmrRow::new(id, idx)).unwrap();
            }
            tx.commit().unwrap();
            applied += 1;
        }
        apply_samples.push(t.elapsed().as_nanos() as f64 / APPLY_BATCH as f64);
    }
    // apply_throughput includes the in-loop snapshot/checkpoint work — it is a
    // whole-apply-pipeline number, not the pure commit rate (that's the p99).
    let loop_secs = loop_t.elapsed().as_secs_f64();

    stop.store(true, Ordering::Relaxed);
    let mut read_samples = reader.join().unwrap();

    m.insert("apply_p99_ns".into(), percentile(&mut apply_samples, 99.0));
    m.insert("apply_throughput".into(), applied as f64 / loop_secs);
    m.insert(
        "read_p99_under_load_ns".into(),
        percentile(&mut read_samples, 99.0),
    );
    m.insert(
        "snapshot_freeze_p99_us".into(),
        percentile(&mut freeze_us, 99.0),
    );
    m.insert("snapshot_stream_ms".into(), percentile(&mut stream_ms, 50.0));
    m.insert(
        "checkpoint_ms".into(),
        percentile(&mut checkpoint_ms, 50.0),
    );

    // -- (b) restore_install_ms: stream the main store, install into a fresh
    // Smr store, time the install. Per src/snapshot_stream/install.rs:105 the
    // install lands at the destination's base_version+1 and RETURNS that
    // version — so a fresh store (version 0) installs at version 1, which does
    // NOT equal the source version. Assert against the return value + row count.
    {
        let mut buf = Vec::new();
        store.snapshot_stream(None).unwrap().read_to_end(&mut buf).unwrap();

        let dir2 = tempfile::Builder::new()
            .prefix("smr-restore")
            .tempdir_in(&root)
            .unwrap();
        let store2 = smr_store(dir2.path(), WriterMode::SingleWriter);

        let t = Instant::now();
        let installed = store2
            .install_snapshot_stream(&buf[..], InstallOptions::default())
            .unwrap();
        let elapsed_ms = t.elapsed().as_secs_f64() * 1e3;

        assert_eq!(
            installed,
            store2.latest_version(),
            "smr: latest_version must reflect the install return value"
        );
        let r = store2.begin_read(None).unwrap();
        let t2 = r.open_table::<SmrRow>("state").unwrap();
        assert_eq!(
            t2.len() as u64,
            rows,
            "smr: row count after install != preload_rows"
        );
        m.insert("restore_install_ms".into(), elapsed_ms);
    }

    // Reuses the main store post-apply-loop: auto-GC bounds retained snapshots to ~10, so this measures a warm steady-state store, not a cold one.
    // -- (c) apply_sw_batch_throughput: on the main SingleWriter store, run
    // batch_iters rounds of T=8 sequential txns (auto-versioned), each
    // updating one distinct row. Measures the per-commit SingleWriter rate
    // without the snapshot cadence overhead.
    {
        const T: u64 = 8;
        let t = Instant::now();
        let mut row = 1u64;
        for _ in 0..cfg.batch_iters {
            for _ in 0..T {
                row = row % rows + 1;
                let mut tx = store.begin_write(None).unwrap();
                {
                    let mut tbl = tx.open_table::<SmrRow>("state").unwrap();
                    tbl.update(row, SmrRow::new(row, row)).unwrap();
                }
                tx.commit().unwrap();
            }
        }
        let txns = (cfg.batch_iters as u64 * T) as f64;
        m.insert(
            "apply_sw_batch_throughput".into(),
            txns / t.elapsed().as_secs_f64(),
        );
    }

    // -- (d) apply_mw_throughput + apply_mw_p99_ns: fresh MultiWriter Smr
    // store; 4 writers × 2 txns/round on DISJOINT id ranges so the OCC
    // intent check only collides on accidental scheduler interleaving. Retry
    // handles BOTH the write-site and commit-site WriteConflict (the pattern
    // at benches/multiwriter_scaling_bench.rs:231-251).
    {
        const WRITERS: u64 = 4;
        const TXNS_PER_WRITER: u64 = 2;
        const RANGE: u64 = 2_500; // ids per writer
        let mw_rows = WRITERS * RANGE; // 10_000

        let mw_dir = tempfile::Builder::new()
            .prefix("smr-mw")
            .tempdir_in(&root)
            .unwrap();
        let mw_store = smr_store(mw_dir.path(), WriterMode::MultiWriter);
        {
            let mut tx = mw_store.begin_write(None).unwrap();
            {
                let mut t = tx.open_table::<SmrRow>("state").unwrap();
                let batch: Vec<SmrRow> = (1..=mw_rows).map(|i| SmrRow::new(i, 0)).collect();
                t.insert_batch(batch).unwrap();
            }
            tx.commit().unwrap();
        }

        let conflicts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(WRITERS as usize));
        let mw_t = Instant::now();
        let handles: Vec<_> = (0..WRITERS)
            .map(|w| {
                let store = mw_store.clone();
                let barrier = Arc::clone(&barrier);
                let conflicts = Arc::clone(&conflicts);
                let rounds = cfg.mw_iters as u64;
                std::thread::spawn(move || -> Vec<f64> {
                    let lo = w * RANGE + 1; // writer w owns lo ..= lo+RANGE-1
                    let mut lat: Vec<f64> = Vec::with_capacity((rounds * TXNS_PER_WRITER) as usize);
                    let mut rot = 0u64;
                    for _ in 0..rounds {
                        barrier.wait();
                        for _ in 0..TXNS_PER_WRITER {
                            let id = lo + rot % RANGE;
                            rot += 1;
                            let t = Instant::now();
                            loop {
                                let mut tx = store.begin_write(None).unwrap();
                                let write_res: Result<(), ultima_db::Error> = {
                                    let mut tbl =
                                        tx.open_table::<SmrRow>("state").unwrap();
                                    tbl.update(id, SmrRow::new(id, rot))
                                };
                                match write_res {
                                    Ok(()) => {}
                                    Err(ultima_db::Error::WriteConflict {
                                        wait_for, ..
                                    }) => {
                                        conflicts.fetch_add(1, Ordering::Relaxed);
                                        drop(tx);
                                        if let Some(waiter) = wait_for {
                                            waiter.wait();
                                        }
                                        continue;
                                    }
                                    Err(e) => panic!("smr-mw: write error: {e}"),
                                }
                                match tx.commit() {
                                    Ok(_) => break,
                                    Err(ultima_db::Error::WriteConflict {
                                        wait_for, ..
                                    }) => {
                                        conflicts.fetch_add(1, Ordering::Relaxed);
                                        if let Some(waiter) = wait_for {
                                            waiter.wait();
                                        }
                                        continue;
                                    }
                                    Err(e) => panic!("smr-mw: commit error: {e}"),
                                }
                            }
                            lat.push(t.elapsed().as_nanos() as f64);
                        }
                    }
                    lat
                })
            })
            .collect();

        let mut all_lat: Vec<f64> = Vec::new();
        for h in handles {
            all_lat.extend(h.join().unwrap());
        }
        let mw_secs = mw_t.elapsed().as_secs_f64();
        let total_txns = (WRITERS * TXNS_PER_WRITER * cfg.mw_iters as u64) as f64;
        m.insert("apply_mw_throughput".into(), total_txns / mw_secs);
        m.insert("apply_mw_p99_ns".into(), percentile(&mut all_lat, 99.0));
        eprintln!(
            "[autobench] smr mw conflicts = {}",
            conflicts.load(Ordering::Relaxed)
        );
    }

    m
}
