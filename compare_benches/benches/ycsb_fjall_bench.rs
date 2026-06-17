// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use tempfile::TempDir;

use ultima_bench_workloads::ycsb::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

// ---------------------------------------------------------------------------
// Fjall engine — field order matters for drop (first declared = first dropped)
// ---------------------------------------------------------------------------

struct FjallEngine {
    keyspace: Keyspace,
    db: Database,
    _tmpdir: TempDir,
    next_id: u64,
    // Strict tier → `persist(SyncAll)` (fsync the journal) after every write.
    sync: bool,
}

impl FjallEngine {
    fn preload() -> Self {
        let tmpdir = tempfile::tempdir_in(bench_disk_dir()).expect("failed to create temp dir");
        let db = Database::builder(tmpdir.path())
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

        FjallEngine {
            keyspace,
            db,
            _tmpdir: tmpdir,
            next_id: NUM_RECORDS + 1,
            sync: bench_durability() == BenchDurability::Strict,
        }
    }

    /// Strict tier: fsync the journal so the just-written op is durable. In the
    /// non-durable tier `insert` already persisted with `PersistMode::Buffer`
    /// (OS page cache, no fsync), so this is a no-op.
    fn sync_if_strict(&self) {
        if self.sync {
            self.db
                .persist(PersistMode::SyncAll)
                .expect("persist failed");
        }
    }
}

impl YcsbEngine for FjallEngine {
    fn name(&self) -> &str {
        "fjall"
    }

    fn execute(&mut self, ops: &[YcsbOp]) {
        for op in ops {
            match op {
                YcsbOp::Read(key) => {
                    let val = self.keyspace.get(encode_key(*key)).expect("read failed");
                    black_box(val);
                }
                YcsbOp::Update(key) => {
                    let k = encode_key(*key);
                    let record = YcsbRecord::new(key.wrapping_add(1));
                    let value = bincode::serde::encode_to_vec(record, BINCODE_CFG)
                        .expect("serialize failed");
                    self.keyspace.insert(k, value).expect("insert failed");
                    self.sync_if_strict();
                }
                YcsbOp::Insert => {
                    let id = self.next_id;
                    self.next_id += 1;
                    let k = encode_key(id);
                    let record = YcsbRecord::new(0);
                    let value = bincode::serde::encode_to_vec(record, BINCODE_CFG)
                        .expect("serialize failed");
                    self.keyspace.insert(k, value).expect("insert failed");
                    self.sync_if_strict();
                }
                YcsbOp::Scan(start, count) => {
                    let start_key = encode_key(*start);
                    let end_key = encode_key(start.saturating_add(*count));
                    for guard in self.keyspace.range(start_key..end_key) {
                        let kv = guard.into_inner().expect("scan item failed");
                        black_box(kv);
                    }
                }
                YcsbOp::ReadModifyWrite(key) => {
                    let k = encode_key(*key);
                    let maybe_val = self.keyspace.get(k).expect("read failed");
                    if let Some(bytes) = maybe_val {
                        let (mut record, _): (YcsbRecord, _) =
                            bincode::serde::decode_from_slice(&bytes, BINCODE_CFG)
                                .expect("deserialize failed");
                        record.field0 = std::iter::repeat_n('X', FIELD_SIZE).collect();
                        let new_value = bincode::serde::encode_to_vec(record, BINCODE_CFG)
                            .expect("serialize failed");
                        self.keyspace.insert(k, new_value).expect("insert failed");
                        self.sync_if_strict();
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

fn bench_ycsb(c: &mut Criterion) {
    let mut engine = FjallEngine::preload();
    bench_all_workloads(c, &mut engine);
}

criterion_group! {
    name = ycsb_fjall;
    config = ycsb_criterion();
    targets = bench_ycsb
}
criterion_main!(ycsb_fjall);
