// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use redb::{
    backends::InMemoryBackend, Database, Durability, ReadableDatabase, ReadableTable,
    TableDefinition,
};

#[path = "ycsb_common.rs"]
mod ycsb_common;
use ycsb_common::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

const TABLE: TableDefinition<[u8; 8], &[u8]> = TableDefinition::new("ycsb");

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

// ---------------------------------------------------------------------------
// ReDB engine — in-memory backend, no durability overhead
// ---------------------------------------------------------------------------

struct RedbEngine {
    db: Database,
    next_id: u64,
}

impl RedbEngine {
    fn preload() -> Self {
        let backend = InMemoryBackend::new();
        let db = Database::builder()
            .create_with_backend(backend)
            .expect("failed to create redb");

        let tx = db.begin_write().expect("begin_write failed");
        {
            let mut table = tx.open_table(TABLE).expect("open_table failed");
            for i in 1..=NUM_RECORDS {
                let key = encode_key(i);
                let value = bincode::serde::encode_to_vec(YcsbRecord::new(i), BINCODE_CFG)
                    .expect("serialize failed");
                table.insert(key, value.as_slice()).expect("insert failed");
            }
        }
        tx.commit().expect("commit failed");

        RedbEngine {
            db,
            next_id: NUM_RECORDS + 1,
        }
    }
}

impl YcsbEngine for RedbEngine {
    fn name(&self) -> &str {
        "redb"
    }

    fn execute(&mut self, ops: &[YcsbOp]) {
        // Batch all writes in a single transaction per execute call
        let mut has_writes = false;
        for op in ops {
            match op {
                YcsbOp::Update(_) | YcsbOp::Insert | YcsbOp::ReadModifyWrite(_) => {
                    has_writes = true;
                    break;
                }
                _ => {}
            }
        }

        if has_writes {
            let mut tx = self.db.begin_write().expect("begin_write failed");
            tx.set_durability(Durability::None).expect("set_durability failed");
            {
                let mut table = tx.open_table(TABLE).expect("open_table failed");
                for op in ops {
                    match op {
                        YcsbOp::Read(key) => {
                            let val = table.get(encode_key(*key)).expect("get failed");
                            black_box(val);
                        }
                        YcsbOp::Update(key) => {
                            let k = encode_key(*key);
                            let record = YcsbRecord::new(key.wrapping_add(1));
                            let value = bincode::serde::encode_to_vec(record, BINCODE_CFG)
                                .expect("serialize failed");
                            table.insert(k, value.as_slice()).expect("insert failed");
                        }
                        YcsbOp::Insert => {
                            let id = self.next_id;
                            self.next_id += 1;
                            let k = encode_key(id);
                            let record = YcsbRecord::new(0);
                            let value = bincode::serde::encode_to_vec(record, BINCODE_CFG)
                                .expect("serialize failed");
                            table.insert(k, value.as_slice()).expect("insert failed");
                        }
                        YcsbOp::Scan(start, count) => {
                            let start_key = encode_key(*start);
                            let end_key = encode_key(start.saturating_add(*count));
                            let range = table.range(start_key..end_key).expect("range failed");
                            for item in range {
                                let kv = item.expect("range item failed");
                                black_box(kv);
                            }
                        }
                        YcsbOp::ReadModifyWrite(key) => {
                            let k = encode_key(*key);
                            let new_value = {
                                let guard = table.get(k).expect("get failed");
                                guard.map(|g| {
                                    let (mut record, _): (YcsbRecord, _) =
                                        bincode::serde::decode_from_slice(g.value(), BINCODE_CFG)
                                            .expect("deserialize failed");
                                    record.field0 =
                                        std::iter::repeat_n('X', FIELD_SIZE).collect();
                                    bincode::serde::encode_to_vec(record, BINCODE_CFG)
                                        .expect("serialize failed")
                                })
                            };
                            if let Some(val) = new_value {
                                table.insert(k, val.as_slice()).expect("insert failed");
                            }
                        }
                    }
                }
            }
            tx.commit().expect("commit failed");
        } else {
            // Read-only path: use a read transaction
            let tx = self.db.begin_read().expect("begin_read failed");
            let table = tx.open_table(TABLE).expect("open_table failed");
            for op in ops {
                match op {
                    YcsbOp::Read(key) => {
                        let val = table.get(encode_key(*key)).expect("get failed");
                        black_box(val);
                    }
                    YcsbOp::Scan(start, count) => {
                        let start_key = encode_key(*start);
                        let end_key = encode_key(start.saturating_add(*count));
                        let range = table.range(start_key..end_key).expect("range failed");
                        for item in range {
                            let kv = item.expect("range item failed");
                            black_box(kv);
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

fn bench_ycsb(c: &mut Criterion) {
    let mut engine = RedbEngine::preload();
    bench_all_workloads(c, &mut engine);
}

criterion_group! {
    name = ycsb_redb;
    config = ycsb_criterion();
    targets = bench_ycsb
}
criterion_main!(ycsb_redb);
