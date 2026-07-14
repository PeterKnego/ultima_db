// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};

use ultima_bench_workloads::ycsb::*;

// ---------------------------------------------------------------------------
// UltimaDB engine
// ---------------------------------------------------------------------------

struct UltimaEngine {
    store: Store,
    // Held so the WAL/checkpoint dir lives as long as the engine.
    _tmpdir: tempfile::TempDir,
}

impl UltimaEngine {
    fn preload() -> Self {
        // On-disk under `bench_disk_dir()` (real disk, not tmpfs). Durability
        // tier is selected by `ULTIMA_BENCH_DURABILITY`: NonDurable = `Eventual`
        // (async fsync, matching the competitors' no-fsync write path), Strict =
        // `Consistent` (commit blocks until fsync). `Coalesced` in both so the
        // only variable across tiers is the fsync semantics.
        let tmpdir = tempfile::tempdir_in(bench_disk_dir()).expect("failed to create temp dir");
        let durability = match bench_durability() {
            BenchDurability::NonDurable => Durability::Eventual,
            // A/B toggle: `ULTIMA_BENCH_INLINE` (any non-empty value) selects
            // `standalone_fast`'s off-lock inline fsync (no bg-thread handoff;
            // SingleWriter-only — the YCSB bench is single-writer). Pair with
            // `ULTIMA_BENCH_PREALLOC=1` for the full `standalone_fast` preset.
            BenchDurability::Strict => match std::env::var_os("ULTIMA_BENCH_INLINE") {
                Some(v) if !v.is_empty() => Durability::ConsistentInline,
                _ => Durability::Consistent,
            },
        };
        // A/B toggle: `ULTIMA_BENCH_PREALLOC` (any non-empty value) selects the
        // preallocating WAL sink, so prealloc-on vs prealloc-off is the only
        // variable. Default stays `Coalesced` to match the committed baseline.
        let wal_write = match std::env::var_os("ULTIMA_BENCH_PREALLOC") {
            Some(v) if !v.is_empty() => WalWrite::CoalescedPrealloc,
            _ => WalWrite::Coalesced,
        };
        // UltimaDB-only toggle: `ULTIMA_BENCH_SMR` (any non-empty value) runs the
        // store in checkpoint-only SMR mode (`Persistence::smr`) — no per-commit
        // WAL: commits do only the in-memory CoW update, durability is assumed to
        // come from an external consensus log. This overrides the WAL durability
        // tier above (SMR has no WAL/fsync knob). No competitor equivalent, so it
        // is not part of the shared `BenchDurability` matrix; it measures
        // UltimaDB's leanest single-writer write path against its own Eventual arm.
        let persistence = match std::env::var_os("ULTIMA_BENCH_SMR") {
            Some(v) if !v.is_empty() => Persistence::smr(tmpdir.path().to_path_buf()),
            _ => Persistence::standalone(tmpdir.path().to_path_buf(), durability, wal_write),
        };
        let store = Store::new(
            StoreConfig::builder()
                .num_snapshots_retained(2)
                .auto_snapshot_gc(true)
                .persistence(persistence)
                .build(),
        )
        .unwrap();
        store.register_table::<YcsbRecord>("ycsb").unwrap();
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
            for i in 1..=NUM_RECORDS {
                table.insert(YcsbRecord::new(i)).unwrap();
            }
        }
        wtx.commit().unwrap();
        UltimaEngine {
            store,
            _tmpdir: tmpdir,
        }
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
