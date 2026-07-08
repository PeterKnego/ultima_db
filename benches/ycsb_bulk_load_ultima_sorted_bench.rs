// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bulk-load arm: UltimaDB `Store::bulk_load` (BTree::from_sorted, O(N)) — the
//! specialized restore path and the ceiling this benchmark measures. Strict
//! tier: `checkpoint_after: true` writes a durable checkpoint (one fsync). See
//! docs/tasks/task49_bulk_load_bench.md.

use criterion::{Criterion, criterion_group, criterion_main};
use ultima_bench_workloads::bulk_load::*;
use ultima_bench_workloads::ycsb::YcsbRecord;
use ultima_db::bulk_load::{BulkLoadInput, BulkLoadOptions, BulkSource};

fn bench(c: &mut Criterion) {
    bench_bulk_load(
        c,
        |_n| ultima_bulk_store(),
        |e: &mut UltimaBulkStore, n| {
            let records: Vec<YcsbRecord> = (1..=n).map(YcsbRecord::new).collect();
            let checkpoint_after =
                bench_durability_is_strict();
            e.store
                .bulk_load::<YcsbRecord>(
                    "ycsb",
                    BulkLoadInput::Replace(BulkSource::auto_id_vec(records)),
                    BulkLoadOptions {
                        create_if_missing: true,
                        checkpoint_after,
                    },
                )
                .unwrap();
        },
    );
}

criterion_group! { name = benches; config = bulk_load_criterion(); targets = bench }
criterion_main!(benches);
