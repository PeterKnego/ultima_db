//! Multi-writer contention benchmarks for Fjall using OptimisticTxDatabase.

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
    db: OptimisticTxDatabase,
    keyspace: OptimisticTxKeyspace,
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
            db,
            keyspace,
            _tmpdir: tmpdir,
        }
    }
}

impl MultiWriterEngine for FjallMultiWriterEngine {
    fn name(&self) -> &str {
        "fjall"
    }

    fn execute_burst(&mut self, key_sets: &[Vec<u64>]) -> BurstResult {
        let mut committed = 0u64;
        let mut conflicts = 0u64;

        // Open all transactions simultaneously
        let mut txns: Vec<_> = (0..key_sets.len())
            .map(|_| Some(self.db.write_tx().expect("write_tx failed")))
            .collect();

        // Each transaction updates its keys
        for (w, txn_opt) in txns.iter_mut().enumerate() {
            let txn = txn_opt.as_mut().unwrap();
            for &key in &key_sets[w] {
                let k = encode_key(key);
                let value =
                    bincode::serde::encode_to_vec(YcsbRecord::new(key.wrapping_add(1)), BINCODE_CFG)
                        .expect("serialize failed");
                txn.insert(&self.keyspace, k, value);
            }
        }

        // Commit in sequence — retry on conflict
        for w in 0..key_sets.len() {
            let txn = txns[w].take().unwrap();
            match txn.commit().expect("io error on commit") {
                Ok(()) => committed += 1,
                Err(_conflict) => {
                    conflicts += 1;
                    // Retry with fresh transaction
                    let mut txn = self.db.write_tx().expect("write_tx failed");
                    for &key in &key_sets[w] {
                        let k = encode_key(key);
                        let value = bincode::serde::encode_to_vec(
                            YcsbRecord::new(key.wrapping_add(1)),
                            BINCODE_CFG,
                        )
                        .expect("serialize failed");
                        txn.insert(&self.keyspace, k, value);
                    }
                    txn.commit()
                        .expect("io error on retry")
                        .expect("retry conflicted");
                    committed += 1;
                }
            }
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
