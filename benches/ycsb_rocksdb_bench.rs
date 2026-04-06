use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use rocksdb::{DB, Options};
use tempfile::TempDir;

#[path = "ycsb_common.rs"]
mod ycsb_common;
use ycsb_common::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

// ---------------------------------------------------------------------------
// RocksDB engine
// ---------------------------------------------------------------------------

struct RocksDbEngine {
    db: DB,
    _tmpdir: TempDir,
    next_id: u64,
}

impl RocksDbEngine {
    fn preload() -> Self {
        let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Minimize disk I/O: large write buffer, no WAL, no fsync
        opts.set_write_buffer_size(256 * 1024 * 1024);
        opts.set_disable_auto_compactions(true);
        let db = DB::open(&opts, tmpdir.path()).expect("failed to open rocksdb");

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

        RocksDbEngine {
            db,
            _tmpdir: tmpdir,
            next_id: NUM_RECORDS + 1,
        }
    }
}

impl YcsbEngine for RocksDbEngine {
    fn name(&self) -> &str {
        "rocksdb"
    }

    fn execute(&mut self, ops: &[YcsbOp]) {
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.disable_wal(true);

        for op in ops {
            match op {
                YcsbOp::Read(key) => {
                    let val = self.db.get(encode_key(*key)).expect("get failed");
                    black_box(val);
                }
                YcsbOp::Update(key) => {
                    let k = encode_key(*key);
                    let record = YcsbRecord::new(key.wrapping_add(1));
                    let value =
                        bincode::serde::encode_to_vec(record, BINCODE_CFG).expect("serialize failed");
                    self.db.put_opt(k, value, &write_opts).expect("put failed");
                }
                YcsbOp::Insert => {
                    let id = self.next_id;
                    self.next_id += 1;
                    let k = encode_key(id);
                    let record = YcsbRecord::new(0);
                    let value =
                        bincode::serde::encode_to_vec(record, BINCODE_CFG).expect("serialize failed");
                    self.db.put_opt(k, value, &write_opts).expect("put failed");
                }
                YcsbOp::Scan(start, count) => {
                    let start_key = encode_key(*start);
                    let iter = self.db.iterator(rocksdb::IteratorMode::From(
                        &start_key,
                        rocksdb::Direction::Forward,
                    ));
                    for item in iter.take(*count as usize) {
                        let kv = item.expect("iterator failed");
                        black_box(kv);
                    }
                }
                YcsbOp::ReadModifyWrite(key) => {
                    let k = encode_key(*key);
                    if let Some(bytes) = self.db.get(&k).expect("get failed") {
                        let (mut record, _): (YcsbRecord, _) =
                            bincode::serde::decode_from_slice(&bytes, BINCODE_CFG)
                                .expect("deserialize failed");
                        record.field0 = std::iter::repeat_n('X', FIELD_SIZE).collect();
                        let new_value = bincode::serde::encode_to_vec(record, BINCODE_CFG)
                            .expect("serialize failed");
                        self.db.put_opt(k, new_value, &write_opts).expect("put failed");
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
    let mut engine = RocksDbEngine::preload();
    bench_all_workloads(c, &mut engine);
}

criterion_group! {
    name = ycsb_rocksdb;
    config = ycsb_criterion();
    targets = bench_ycsb
}
criterion_main!(ycsb_rocksdb);