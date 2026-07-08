// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bulk-load arm: ReDB. One write transaction, N inserts, one commit. Strict
//! tier: `Durability::Immediate` (fsync on that single commit); NonDurable:
//! `Durability::None`. See docs/tasks/task49_bulk_load_bench.md.

use criterion::{Criterion, criterion_group, criterion_main};
use redb::{Database, Durability, TableDefinition};
use tempfile::TempDir;

use ultima_bench_workloads::bulk_load::{bench_bulk_load, bulk_load_criterion};
use ultima_bench_workloads::ycsb::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();
const TABLE: TableDefinition<[u8; 8], &[u8]> = TableDefinition::new("ycsb");

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

struct RedbEngine {
    db: Database,
    _tmpdir: TempDir,
    durability: Durability,
}

fn make_empty(_n: u64) -> RedbEngine {
    let tmpdir = tempfile::tempdir_in(bench_disk_dir()).expect("failed to create temp dir");
    let db = Database::create(tmpdir.path().join("ycsb.redb")).expect("failed to create redb");
    let durability = match bench_durability() {
        BenchDurability::NonDurable => Durability::None,
        BenchDurability::Strict => Durability::Immediate,
    };
    RedbEngine {
        db,
        _tmpdir: tmpdir,
        durability,
    }
}

fn load(e: &mut RedbEngine, n: u64) {
    let mut tx = e.db.begin_write().expect("begin_write failed");
    tx.set_durability(e.durability).expect("set_durability failed");
    {
        let mut table = tx.open_table(TABLE).expect("open_table failed");
        for i in 1..=n {
            let value =
                bincode::serde::encode_to_vec(YcsbRecord::new(i), BINCODE_CFG).expect("serialize");
            table.insert(encode_key(i), value.as_slice()).expect("insert failed");
        }
    }
    tx.commit().expect("commit failed");
}

fn bench(c: &mut Criterion) {
    bench_bulk_load(c, make_empty, load);
}

criterion_group! { name = benches; config = bulk_load_criterion(); targets = bench }
criterion_main!(benches);
