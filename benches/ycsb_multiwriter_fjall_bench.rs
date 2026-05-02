// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

#![allow(clippy::redundant_iter_cloned)]

//! Multi-writer contention benchmarks for Fjall using OptimisticTxDatabase.
//!
//! Each writer runs on its own OS thread, sharing Arc-wrapped db+keyspace.

use std::sync::{Arc, Barrier};
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace};
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
// Fjall multi-writer engine
// ---------------------------------------------------------------------------

struct FjallMultiWriterEngine {
    db: Arc<OptimisticTxDatabase>,
    keyspace: Arc<OptimisticTxKeyspace>,
    _tmpdir: TempDir,
}

impl FjallMultiWriterEngine {
    fn preload() -> Self {
        let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
        let db = OptimisticTxDatabase::builder(tmpdir.path())
            .open()
            .expect("failed to open fjall database");
        let keyspace = db
            .keyspace("ycsb", KeyspaceCreateOptions::default)
            .expect("failed to create keyspace");

        for i in 1..=NUM_RECORDS {
            let key = encode_key(i);
            let value = bincode::serde::encode_to_vec(YcsbRecord::new(i), BINCODE_CFG)
                .expect("serialize failed");
            keyspace.insert(key, value).expect("insert failed");
        }

        FjallMultiWriterEngine {
            db: Arc::new(db),
            keyspace: Arc::new(keyspace),
            _tmpdir: tmpdir,
        }
    }
}

impl MultiWriterEngine for FjallMultiWriterEngine {
    fn name(&self) -> &str {
        "fjall"
    }

    fn execute_burst(&mut self, key_sets: &[Vec<u64>]) -> BurstResult {
        let barrier = Arc::new(Barrier::new(key_sets.len()));
        let handles: Vec<_> = key_sets
            .iter()
            .cloned()
            .map(|keys| {
                let db = Arc::clone(&self.db);
                let keyspace = Arc::clone(&self.keyspace);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut conflicts = 0u64;
                    loop {
                        let mut txn = db.write_tx().expect("write_tx failed");
                        for &key in &keys {
                            let k = encode_key(key);
                            let value = bincode::serde::encode_to_vec(
                                YcsbRecord::new(key.wrapping_add(1)),
                                BINCODE_CFG,
                            )
                            .expect("serialize failed");
                            txn.insert(keyspace.as_ref(), k, value);
                        }
                        match txn.commit().expect("io error on commit") {
                            Ok(()) => return (1u64, conflicts),
                            Err(_conflict) => {
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
        self.keyspace
            .get(encode_key(key))
            .expect("get failed")
            .is_some()
    }
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

fn bench_multiwriter(c: &mut Criterion) {
    let mut engine = FjallMultiWriterEngine::preload();
    bench_multiwriter_workloads(c, &mut engine);
}

criterion_group! {
    name = multiwriter_fjall;
    config = ycsb_criterion();
    targets = bench_multiwriter
}
criterion_main!(multiwriter_fjall);
