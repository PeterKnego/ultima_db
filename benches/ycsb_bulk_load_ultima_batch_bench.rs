// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bulk-load arm: UltimaDB `Table::insert_batch` in one write transaction.
//! This is the path `insert_mut` (task48) accelerates — many inserts sharing a
//! single transaction, reusing the privatized spine. Strict tier: the commit
//! is `Durability::Consistent` (WAL coalesced + one fsync). See
//! docs/tasks/task49_bulk_load_bench.md.

use criterion::{Criterion, criterion_group, criterion_main};
use ultima_bench_workloads::bulk_load::*;
use ultima_bench_workloads::ycsb::YcsbRecord;

fn bench(c: &mut Criterion) {
    bench_bulk_load(
        c,
        |_n| ultima_bulk_store(),
        |e: &mut UltimaBulkStore, n| {
            let records: Vec<YcsbRecord> = (1..=n).map(YcsbRecord::new).collect();
            let mut wtx = e.store.begin_write(None).unwrap();
            {
                let mut t = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                t.insert_batch(records).unwrap();
            }
            wtx.commit().unwrap();
        },
    );
}

criterion_group! { name = benches; config = bulk_load_criterion(); targets = bench }
criterion_main!(benches);
