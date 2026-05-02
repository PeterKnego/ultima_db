// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use ultima_db::{Store, StoreConfig};

#[path = "ycsb_common.rs"]
mod ycsb_common;
use ycsb_common::*;

// ---------------------------------------------------------------------------
// UltimaDB engine
// ---------------------------------------------------------------------------

struct UltimaEngine {
    store: Store,
}

impl UltimaEngine {
    fn preload() -> Self {
        let store = Store::new(StoreConfig {
            num_snapshots_retained: 2,
            auto_snapshot_gc: true,
            ..StoreConfig::default()
        }).unwrap();
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
            for i in 1..=NUM_RECORDS {
                table.insert(YcsbRecord::new(i)).unwrap();
            }
        }
        wtx.commit().unwrap();
        UltimaEngine { store }
    }
}

impl YcsbEngine for UltimaEngine {
    fn name(&self) -> &str {
        "ultima"
    }

    fn execute(&mut self, ops: &[YcsbOp]) {
        for op in ops {
            match op {
                YcsbOp::Read(key) => {
                    let rtx = self.store.begin_read(None).unwrap();
                    let table = rtx.open_table::<YcsbRecord>("ycsb").unwrap();
                    black_box(table.get(*key));
                }
                YcsbOp::Update(key) => {
                    let mut wtx = self.store.begin_write(None).unwrap();
                    let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                    let _ = table.update(*key, YcsbRecord::new(key.wrapping_add(1)));
                    wtx.commit().unwrap();
                }
                YcsbOp::Insert => {
                    let mut wtx = self.store.begin_write(None).unwrap();
                    let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                    let id = table.insert(YcsbRecord::new(0)).unwrap();
                    black_box(id);
                    wtx.commit().unwrap();
                }
                YcsbOp::Scan(start, count) => {
                    let rtx = self.store.begin_read(None).unwrap();
                    let table = rtx.open_table::<YcsbRecord>("ycsb").unwrap();
                    for item in table.range(*start..start.saturating_add(*count)) {
                        black_box(item);
                    }
                }
                YcsbOp::ReadModifyWrite(key) => {
                    let record = {
                        let rtx = self.store.begin_read(None).unwrap();
                        let table = rtx.open_table::<YcsbRecord>("ycsb").unwrap();
                        table.get(*key).cloned()
                    };
                    if let Some(mut rec) = record {
                        rec.field0 = std::iter::repeat_n('X', FIELD_SIZE).collect();
                        let mut wtx = self.store.begin_write(None).unwrap();
                        let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
                        let _ = table.update(*key, rec);
                        wtx.commit().unwrap();
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
    let mut engine = UltimaEngine::preload();
    bench_all_workloads(c, &mut engine);
}

criterion_group! {
    name = ycsb;
    config = ycsb_criterion();
    targets = bench_ycsb
}
criterion_main!(ycsb);
