// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use turbokv::{Compression, Db, DbOptions};

use ultima_bench_workloads::ycsb::*;

const BINCODE_CFG: bincode::config::Configuration = bincode::config::standard();

fn encode_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

// ---------------------------------------------------------------------------
// TurboKV engine — async-only API (tokio), driven from the sync criterion
// harness via one `block_on` per 1,000-op burst so the runtime hop is paid
// once per burst, not once per op. Field order matters for drop (first
// declared = first dropped): the Db must go before the runtime that hosts its
// background flush/compaction tasks, and the tmpdir last.
// ---------------------------------------------------------------------------

struct TurbokvEngine {
    db: Db,
    rt: Runtime,
    _tmpdir: TempDir,
    next_id: u64,
}

/// Durability preset per bench tier. The shared `ULTIMA_BENCH_DURABILITY`
/// matrix maps onto turbokv's presets as:
///
/// - NonDurable → `DbOptions::durable()`: WAL appended, no per-write sync —
///   like-for-like with UltimaDB `Eventual` / Fjall `Buffer` (WAL written,
///   no fsync on the commit path).
/// - Strict → `DbOptions::paranoid()`: WAL synced before each ack — like
///   UltimaDB `Consistent*` / Fjall `SyncAll`.
///
/// `ULTIMA_BENCH_TURBOKV_NOWAL` (any non-empty value, NonDurable tier only)
/// swaps in `DbOptions::fast()` — no WAL at all. That is turbokv's headline
/// column and has no like-for-like peer in the matrix (every other engine
/// still writes its log), so it is an extra labelled arm, not the tier.
fn options() -> DbOptions {
    let nowal = matches!(std::env::var_os("ULTIMA_BENCH_TURBOKV_NOWAL"), Some(v) if !v.is_empty());
    let opts = match bench_durability() {
        BenchDurability::NonDurable if nowal => DbOptions::fast(),
        BenchDurability::NonDurable => DbOptions::durable(),
        BenchDurability::Strict => DbOptions::paranoid(),
    };
    // Match turbokv's own published protocol (compression off) and the other
    // engines in this matrix, which store the bincode bytes verbatim.
    opts.with_compression(Compression::None)
}

impl TurbokvEngine {
    fn preload() -> Self {
        let tmpdir = tempfile::tempdir_in(bench_disk_dir()).expect("failed to create temp dir");
        let rt = Runtime::new().expect("failed to build tokio runtime");
        let db = rt
            .block_on(Db::open_with_options(tmpdir.path(), options()))
            .expect("failed to open turbokv database");

        rt.block_on(async {
            for i in 1..=NUM_RECORDS {
                let key = encode_key(i);
                let value = bincode::serde::encode_to_vec(YcsbRecord::new(i), BINCODE_CFG)
                    .expect("serialize failed");
                db.insert(key, value).await.expect("insert failed");
            }
        });

        TurbokvEngine {
            db,
            rt,
            _tmpdir: tmpdir,
            next_id: NUM_RECORDS + 1,
        }
    }
}

impl YcsbEngine for TurbokvEngine {
    fn name(&self) -> &str {
        "turbokv"
    }

    fn execute(&mut self, ops: &[YcsbOp]) {
        let db = &self.db;
        let next_id = &mut self.next_id;
        self.rt.block_on(async move {
            for op in ops {
                match op {
                    YcsbOp::Read(key) => {
                        let val = db.get(encode_key(*key)).await.expect("read failed");
                        black_box(val);
                    }
                    YcsbOp::Update(key) => {
                        let k = encode_key(*key);
                        let record = YcsbRecord::new(key.wrapping_add(1));
                        let value = bincode::serde::encode_to_vec(record, BINCODE_CFG)
                            .expect("serialize failed");
                        db.insert(k, value).await.expect("insert failed");
                    }
                    YcsbOp::Insert => {
                        let id = *next_id;
                        *next_id += 1;
                        let k = encode_key(id);
                        let record = YcsbRecord::new(0);
                        let value = bincode::serde::encode_to_vec(record, BINCODE_CFG)
                            .expect("serialize failed");
                        db.insert(k, value).await.expect("insert failed");
                    }
                    YcsbOp::Scan(start, count) => {
                        // `range` is start-inclusive / end-exclusive, same
                        // bounds the fjall adapter uses.
                        let start_key = encode_key(*start);
                        let end_key = encode_key(start.saturating_add(*count));
                        let kvs = db.range(start_key, end_key).await.expect("scan failed");
                        for kv in kvs {
                            black_box(kv);
                        }
                    }
                    YcsbOp::ReadModifyWrite(key) => {
                        let k = encode_key(*key);
                        let maybe_val = db.get(k).await.expect("read failed");
                        if let Some(bytes) = maybe_val {
                            let (mut record, _): (YcsbRecord, _) =
                                bincode::serde::decode_from_slice(&bytes, BINCODE_CFG)
                                    .expect("deserialize failed");
                            record.field0 = std::iter::repeat_n('X', FIELD_SIZE).collect();
                            let new_value = bincode::serde::encode_to_vec(record, BINCODE_CFG)
                                .expect("serialize failed");
                            db.insert(k, new_value).await.expect("insert failed");
                        }
                    }
                }
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

fn bench_ycsb(c: &mut Criterion) {
    let mut engine = TurbokvEngine::preload();
    bench_all_workloads(c, &mut engine);
}

criterion_group! {
    name = ycsb_turbokv;
    config = ycsb_criterion();
    targets = bench_ycsb
}
criterion_main!(ycsb_turbokv);
