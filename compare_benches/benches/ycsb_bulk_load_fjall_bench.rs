// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bulk-load arm: Fjall. Insert N records, then one `persist(SyncAll)` in the
//! Strict tier (one journal fsync at end); NonDurable inserts buffer with no
//! fsync. Field order matters for drop (keyspace before db). See
//! docs/tasks/task49_bulk_load_bench.md.

use criterion::{Criterion, criterion_group, criterion_main};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use tempfile::TempDir;

use ultima_bench_workloads::bulk_load::{bench_bulk_load, bulk_load_criterion};
use ultima_bench_workloads::ycsb::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

struct FjallEngine {
    keyspace: Keyspace,
    db: Database,
    _tmpdir: TempDir,
    sync: bool,
}

fn make_empty(_n: u64) -> FjallEngine {
    let tmpdir = tempfile::tempdir_in(bench_disk_dir()).expect("failed to create temp dir");
    let db = Database::builder(tmpdir.path())
        .open()
        .expect("failed to open fjall database");
    let keyspace = db
        .keyspace("ycsb", KeyspaceCreateOptions::default)
        .expect("failed to create keyspace");
    FjallEngine {
        keyspace,
        db,
        _tmpdir: tmpdir,
        sync: bench_durability() == BenchDurability::Strict,
    }
}

fn load(e: &mut FjallEngine, n: u64) {
    for i in 1..=n {
        let value =
            bincode::serde::encode_to_vec(YcsbRecord::new(i), BINCODE_CFG).expect("serialize");
        e.keyspace.insert(encode_key(i), value).expect("insert failed");
    }
    if e.sync {
        e.db.persist(PersistMode::SyncAll).expect("persist failed");
    }
}

fn bench(c: &mut Criterion) {
    bench_bulk_load(c, make_empty, load);
}

criterion_group! { name = benches; config = bulk_load_criterion(); targets = bench }
criterion_main!(benches);
