#![allow(clippy::redundant_iter_cloned)]

//! Multi-writer contention benchmarks for RocksDB using OptimisticTransactionDB.
//!
//! Each writer runs on its own OS thread, sharing an `Arc<OptimisticTransactionDB>`.

use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};
use rocksdb::{OptimisticTransactionDB, Options};
use tempfile::TempDir;

#[path = "ycsb_common.rs"]
mod ycsb_common;
use ycsb_common::{
    bench_multiwriter_workloads, ycsb_criterion, BurstResult, MultiWriterEngine, YcsbRecord,
    NUM_RECORDS,
};

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

// ---------------------------------------------------------------------------
// RocksDB multi-writer engine
// ---------------------------------------------------------------------------

struct RocksDbMultiWriterEngine {
    db: Arc<OptimisticTransactionDB>,
    _tmpdir: TempDir,
}

impl RocksDbMultiWriterEngine {
    fn preload() -> Self {
        let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(256 * 1024 * 1024);
        opts.set_disable_auto_compactions(true);
        let db = OptimisticTransactionDB::open(&opts, tmpdir.path())
            .expect("failed to open rocksdb");

        {
            let mut write_opts = rocksdb::WriteOptions::default();
            write_opts.disable_wal(true);
            for i in 1..=NUM_RECORDS {
                let key = encode_key(i);
                let value = bincode::serde::encode_to_vec(YcsbRecord::new(i), BINCODE_CFG)
                    .expect("serialize failed");
                db.put_opt(key, value, &write_opts).expect("put failed");
            }
        }

        RocksDbMultiWriterEngine { db: Arc::new(db), _tmpdir: tmpdir }
    }
}

impl MultiWriterEngine for RocksDbMultiWriterEngine {
    fn name(&self) -> &str {
        "rocksdb"
    }

    fn execute_burst(&mut self, key_sets: &[Vec<u64>]) -> BurstResult {
        let barrier = Arc::new(Barrier::new(key_sets.len()));
        let handles: Vec<_> = key_sets
            .iter()
            .cloned()
            .map(|keys| {
                let db = Arc::clone(&self.db);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut conflicts = 0u64;
                    loop {
                        let txn = db.transaction();
                        for &key in &keys {
                            let k = encode_key(key);
                            let value = bincode::serde::encode_to_vec(
                                YcsbRecord::new(key.wrapping_add(1)),
                                BINCODE_CFG,
                            )
                            .expect("serialize failed");
                            txn.put(k, value).expect("put failed");
                        }
                        match txn.commit() {
                            Ok(_) => return (1u64, conflicts),
                            Err(_) => {
                                conflicts += 1;
                                continue;
                            }
                        }
                    }
                })
            })
            .collect();

        let mut committed = 0u64;
        let mut conflicts = 0u64;
        for h in handles {
            let (c, x) = h.join().unwrap();
            committed += c;
            conflicts += x;
        }
        BurstResult { committed, conflicts }
    }

    fn verify_key(&self, key: u64) -> bool {
        self.db.get(encode_key(key)).expect("get failed").is_some()
    }
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

fn bench_multiwriter(c: &mut Criterion) {
    let mut engine = RocksDbMultiWriterEngine::preload();
    bench_multiwriter_workloads(c, &mut engine);
}

criterion_group! {
    name = multiwriter_rocksdb;
    config = ycsb_criterion();
    targets = bench_multiwriter
}
criterion_main!(multiwriter_rocksdb);
