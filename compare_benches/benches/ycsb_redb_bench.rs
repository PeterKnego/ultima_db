// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use redb::{Database, Durability, ReadableDatabase, TableDefinition};
use tempfile::TempDir;

use ultima_bench_workloads::ycsb::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

const TABLE: TableDefinition<[u8; 8], &[u8]> = TableDefinition::new("ycsb");

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

// ---------------------------------------------------------------------------
// ReDB engine — on-disk file (real disk via bench_disk_dir). Commits **one
// write transaction per write op** to match the per-op commit granularity of
// the other engines (UltimaDB/RocksDB/Fjall) — otherwise ReDB's per-batch
// transaction would do a single fsync where the others do ~500, wrecking the
// strict-tier comparison. Durability tier: NonDurable = `Durability::None`
// (no fsync), Strict = `Durability::Immediate` (fsync on commit).
// ---------------------------------------------------------------------------

struct RedbEngine {
    db: Database,
    _tmpdir: TempDir,
    next_id: u64,
    durability: Durability,
}

impl RedbEngine {
    fn preload() -> Self {
        let tmpdir = tempfile::tempdir_in(bench_disk_dir()).expect("failed to create temp dir");
        let db = Database::create(tmpdir.path().join("ycsb.redb")).expect("failed to create redb");

        // Bulk preload in one transaction — not measured.
        let mut tx = db.begin_write().expect("begin_write failed");
        tx.set_durability(Durability::None)
            .expect("set_durability failed");
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

        let durability = match bench_durability() {
            BenchDurability::NonDurable => Durability::None,
            BenchDurability::Strict => Durability::Immediate,
        };

        RedbEngine {
            db,
            _tmpdir: tmpdir,
            next_id: NUM_RECORDS + 1,
            durability,
        }
    }

    /// Open a write transaction with the configured durability for a single op.
    fn begin_write(&self) -> redb::WriteTransaction {
        let mut tx = self.db.begin_write().expect("begin_write failed");
        tx.set_durability(self.durability)
            .expect("set_durability failed");
        tx
    }
}

impl YcsbEngine for RedbEngine {
    fn name(&self) -> &str {
        "redb"
    }

    fn execute(&mut self, ops: &[YcsbOp]) {
        // One transaction per op, matching the other engines' per-op commit
        // granularity. Reads use a read transaction; writes use a write
        // transaction committed (and, in the strict tier, fsync'd) immediately.
        for op in ops {
            match op {
                YcsbOp::Read(key) => {
                    let tx = self.db.begin_read().expect("begin_read failed");
                    let table = tx.open_table(TABLE).expect("open_table failed");
                    let val = table.get(encode_key(*key)).expect("get failed");
                    black_box(val);
                }
                YcsbOp::Scan(start, count) => {
                    let tx = self.db.begin_read().expect("begin_read failed");
                    let table = tx.open_table(TABLE).expect("open_table failed");
                    let start_key = encode_key(*start);
                    let end_key = encode_key(start.saturating_add(*count));
                    let range = table.range(start_key..end_key).expect("range failed");
                    for item in range {
                        let kv = item.expect("range item failed");
                        black_box(kv);
                    }
                }
                YcsbOp::Update(key) => {
                    let k = encode_key(*key);
                    let record = YcsbRecord::new(key.wrapping_add(1));
                    let value = bincode::serde::encode_to_vec(record, BINCODE_CFG)
                        .expect("serialize failed");
                    let tx = self.begin_write();
                    {
                        let mut table = tx.open_table(TABLE).expect("open_table failed");
                        table.insert(k, value.as_slice()).expect("insert failed");
                    }
                    tx.commit().expect("commit failed");
                }
                YcsbOp::Insert => {
                    let id = self.next_id;
                    self.next_id += 1;
                    let k = encode_key(id);
                    let record = YcsbRecord::new(0);
                    let value = bincode::serde::encode_to_vec(record, BINCODE_CFG)
                        .expect("serialize failed");
                    let tx = self.begin_write();
                    {
                        let mut table = tx.open_table(TABLE).expect("open_table failed");
                        table.insert(k, value.as_slice()).expect("insert failed");
                    }
                    tx.commit().expect("commit failed");
                }
                YcsbOp::ReadModifyWrite(key) => {
                    let k = encode_key(*key);
                    // Read phase
                    let new_value = {
                        let tx = self.db.begin_read().expect("begin_read failed");
                        let table = tx.open_table(TABLE).expect("open_table failed");
                        let guard = table.get(k).expect("get failed");
                        guard.map(|g| {
                            let (mut record, _): (YcsbRecord, _) =
                                bincode::serde::decode_from_slice(g.value(), BINCODE_CFG)
                                    .expect("deserialize failed");
                            record.field0 = std::iter::repeat_n('X', FIELD_SIZE).collect();
                            bincode::serde::encode_to_vec(record, BINCODE_CFG)
                                .expect("serialize failed")
                        })
                    };
                    // Write phase
                    if let Some(val) = new_value {
                        let tx = self.begin_write();
                        {
                            let mut table = tx.open_table(TABLE).expect("open_table failed");
                            table.insert(k, val.as_slice()).expect("insert failed");
                        }
                        tx.commit().expect("commit failed");
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
    let mut engine = RedbEngine::preload();
    bench_all_workloads(c, &mut engine);
}

criterion_group! {
    name = ycsb_redb;
    config = ycsb_criterion();
    targets = bench_ycsb
}
criterion_main!(ycsb_redb);
