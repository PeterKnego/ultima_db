// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bulk-load arm: RocksDB. Idiomatic single-batch ingest — one `WriteBatch` of
//! N puts written once. Strict tier: `set_sync(true)` on that single write (one
//! fsync at end); NonDurable: WAL on, `set_sync(false)`. Large write buffer +
//! auto-compaction disabled so the window measures the write path, matching the
//! YCSB RocksDB bench. See docs/tasks/task49_bulk_load_bench.md.

use criterion::{Criterion, criterion_group, criterion_main};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use tempfile::TempDir;

use ultima_bench_workloads::bulk_load::{bench_bulk_load, bulk_load_criterion};
use ultima_bench_workloads::ycsb::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

struct RocksEngine {
    db: DB,
    _tmpdir: TempDir,
    sync: bool,
}

fn make_empty(_n: u64) -> RocksEngine {
    let tmpdir = tempfile::tempdir_in(bench_disk_dir()).expect("failed to create temp dir");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(256 * 1024 * 1024);
    opts.set_disable_auto_compactions(true);
    let db = DB::open(&opts, tmpdir.path()).expect("failed to open rocksdb");
    RocksEngine {
        db,
        _tmpdir: tmpdir,
        sync: bench_durability() == BenchDurability::Strict,
    }
}

fn load(e: &mut RocksEngine, n: u64) {
    let mut batch = WriteBatch::default();
    for i in 1..=n {
        let value =
            bincode::serde::encode_to_vec(YcsbRecord::new(i), BINCODE_CFG).expect("serialize");
        batch.put(encode_key(i), value);
    }
    let mut wo = WriteOptions::default();
    wo.set_sync(e.sync);
    e.db.write_opt(batch, &wo).expect("write_opt failed");
}

fn bench(c: &mut Criterion) {
    bench_bulk_load(c, make_empty, load);
}

criterion_group! { name = benches; config = bulk_load_criterion(); targets = bench }
criterion_main!(benches);
