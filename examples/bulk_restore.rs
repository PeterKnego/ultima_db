//! Example: full + delta restore using `Store::bulk_load`.
//!
//! Demonstrates the two `BulkLoadInput` variants against a synthetic
//! "users" table: a `Replace` for the base backup, then a `Delta` that
//! applies an incremental backup on top (inserts, one update, three
//! deletes).
//!
//! Run: `cargo run --example bulk_restore`

use ultima_db::{BulkDelta, BulkLoadInput, BulkLoadOptions, BulkSource, Store};

fn main() {
    let store = Store::default();

    // ----- Full restore (the "base backup") -----
    let base: Vec<(u64, String)> = (1u64..=10_000).map(|i| (i, format!("user_{i}"))).collect();
    let v1 = store
        .bulk_load::<String>(
            "users",
            BulkLoadInput::Replace(BulkSource::sorted_vec(base)),
            BulkLoadOptions::default(),
        )
        .expect("base restore");
    println!("Base restored: 10000 rows at version {v1}");

    // ----- Incremental delta (the "incremental backup") -----
    let delta = BulkDelta::<String> {
        inserts: (10_001u64..=10_500)
            .map(|i| (i, format!("user_{i}")))
            .collect(),
        updates: vec![(42, "user_42_renamed".into())],
        deletes: vec![100, 200, 300],
    };
    let v2 = store
        .bulk_load::<String>(
            "users",
            BulkLoadInput::Delta(delta),
            BulkLoadOptions::default(),
        )
        .expect("delta restore");
    println!("Delta applied: +500 inserts, 1 update, 3 deletes at version {v2}");

    let rtx = store.begin_read(None).unwrap();
    let users = rtx.open_table::<String>("users").unwrap();
    println!(
        "Final: {} rows; user 42 = {:?}",
        users.len(),
        users.get(42)
    );
    assert_eq!(users.len(), 10_497);
    assert_eq!(users.get(42).map(String::as_str), Some("user_42_renamed"));
    assert_eq!(users.get(100), None);
}
