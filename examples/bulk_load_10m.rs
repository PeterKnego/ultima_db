// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! One-shot timing: how fast can we ingest 10M records via `Store::bulk_load`?
//!
//! Run: `cargo run --release --example bulk_load_10m`

use std::time::Instant;

use ultima_db::{
    BulkLoadInput, BulkLoadOptions, BulkSource, IndexKind, Store,
};

#[derive(Clone)]
#[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
struct Row {
    name: String,
    bucket: u32,
}

fn fmt_throughput(n: usize, elapsed_secs: f64) -> String {
    let per_sec = n as f64 / elapsed_secs;
    if per_sec >= 1e6 {
        format!("{:.2} M rows/s", per_sec / 1e6)
    } else {
        format!("{:.0} K rows/s", per_sec / 1e3)
    }
}

fn measure<F: FnOnce() -> Store>(label: &str, n: usize, build: F) {
    let start = Instant::now();
    let _store = build();
    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64();
    println!(
        "{:<55} {:>8.2}s  ({})",
        label,
        secs,
        fmt_throughput(n, secs),
    );
}

fn main() {
    let n: usize = std::env::var("N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000_000);

    println!("Bulk-load timing: N = {n} rows\n");

    // ---- 1. Generate input once (don't include this in measured time). ----
    let gen_start = Instant::now();
    let rows_string: Vec<(u64, String)> = (1u64..=n as u64)
        .map(|i| (i, format!("v{i}")))
        .collect();
    println!(
        "  (input generation: {:.2}s for {} String rows)\n",
        gen_start.elapsed().as_secs_f64(),
        n,
    );

    // ---- 2. bulk_load Replace, sorted, no indexes ----
    let rows = rows_string.clone();
    measure("bulk_load Replace sorted, no indexes", n, || {
        let store = Store::default();
        store.bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
            BulkLoadOptions { checkpoint_after: false, ..Default::default() },
        ).unwrap();
        store
    });

    // ---- 3. bulk_load Replace, unsorted (we sort internally) ----
    let mut rows = rows_string.clone();
    // Reverse so sort has work to do (worst case for already-sorted detection).
    rows.reverse();
    measure("bulk_load Replace unsorted (reverse), no indexes", n, || {
        let store = Store::default();
        store.bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::unsorted_vec(rows)),
            BulkLoadOptions { checkpoint_after: false, ..Default::default() },
        ).unwrap();
        store
    });

    // ---- 4. With one unique secondary index ----
    let row_data: Vec<(u64, Row)> = (1u64..=n as u64)
        .map(|i| (i, Row { name: format!("user_{i}"), bucket: (i % 1024) as u32 }))
        .collect();

    measure("bulk_load Replace sorted + 1 unique index", n, || {
        let store = Store::default();
        // Define the index on an empty table first so bulk_load preserves it.
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<Row>("t").unwrap();
            t.define_index("by_name", IndexKind::Unique, |r: &Row| r.name.clone()).unwrap();
            wtx.commit().unwrap();
        }
        store.bulk_load::<Row>(
            "t",
            BulkLoadInput::Replace(BulkSource::sorted_vec(row_data.clone())),
            BulkLoadOptions { checkpoint_after: false, ..Default::default() },
        ).unwrap();
        store
    });

    // ---- 5. With one unique + one non-unique secondary index ----
    measure("bulk_load Replace sorted + unique + non-unique idx", n, || {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<Row>("t").unwrap();
            t.define_index("by_name", IndexKind::Unique, |r: &Row| r.name.clone()).unwrap();
            t.define_index("by_bucket", IndexKind::NonUnique, |r: &Row| r.bucket).unwrap();
            wtx.commit().unwrap();
        }
        store.bulk_load::<Row>(
            "t",
            BulkLoadInput::Replace(BulkSource::sorted_vec(row_data.clone())),
            BulkLoadOptions { checkpoint_after: false, ..Default::default() },
        ).unwrap();
        store
    });

    // ---- 6. Reference: insert_batch via WriteTx (the slow path) ----
    measure("insert_batch via WriteTx (reference)", n, || {
        let store = Store::default();
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.insert_batch((1..=n).map(|i| format!("v{i}")).collect()).unwrap();
        wtx.commit().unwrap();
        store
    });
}
