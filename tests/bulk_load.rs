// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use ultima_db::{BulkDelta, BulkLoadInput, BulkLoadOptions, BulkSource, Error, IndexKind, Store};

#[test]
fn bulk_load_replace_on_empty_store_creates_table() {
    let store = Store::default();
    let rows: Vec<(u64, String)> = (1u64..=1000).map(|i| (i, format!("v{i}"))).collect();
    let input = BulkLoadInput::Replace(BulkSource::sorted_vec(rows));
    let v = store
        .bulk_load::<String>("t", input, BulkLoadOptions::default())
        .unwrap();
    assert!(v >= 1);

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.get(1).map(String::as_str), Some("v1"));
    assert_eq!(t.get(1000).map(String::as_str), Some("v1000"));
    assert_eq!(t.len(), 1000);
}

#[test]
fn bulk_load_replace_with_existing_data_rebuilds_indexes() {
    let store = Store::default();

    // Seed the store with a normal write that creates a table.
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.insert("seed1".into()).unwrap();
        t.insert("seed2".into()).unwrap();
        // No indexes yet — simpler path. Indexed-replace exercised below.
        wtx.commit().unwrap();
    }

    let rows: Vec<(u64, String)> = (1u64..=100).map(|i| (i, format!("v{i}"))).collect();
    let v = store
        .bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
            BulkLoadOptions::default(),
        )
        .unwrap();

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.len(), 100);
    assert_eq!(t.get(1).map(String::as_str), Some("v1"));
    drop(rtx);

    // A read at the prior version still sees the seed.
    let rtx_old = store.begin_read(Some(v - 1)).unwrap();
    let t_old = rtx_old.open_table::<String>("t").unwrap();
    assert_eq!(t_old.len(), 2);
}

#[test]
fn bulk_load_replace_preserves_index_definitions() {
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
    struct U {
        email: String,
    }

    let store = Store::default();

    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<U>("u").unwrap();
        t.insert(U {
            email: "a@x".into(),
        })
        .unwrap();
        t.define_index("by_email", IndexKind::Unique, |u: &U| u.email.clone())
            .unwrap();
        wtx.commit().unwrap();
    }

    let rows: Vec<(u64, U)> = (1u64..=10)
        .map(|i| {
            (
                i,
                U {
                    email: format!("u{i}@x"),
                },
            )
        })
        .collect();
    store
        .bulk_load::<U>(
            "u",
            BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
            BulkLoadOptions::default(),
        )
        .unwrap();

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<U>("u").unwrap();
    let (id, u) = t
        .get_unique("by_email", &"u3@x".to_string())
        .unwrap()
        .unwrap();
    assert_eq!(id, 3);
    assert_eq!(u.email, "u3@x");
}

#[test]
fn bulk_load_replace_auto_id_assigns_sequential_ids() {
    let store = Store::default();
    let rows: Vec<String> = (0..50).map(|i| format!("r{i}")).collect();
    store
        .bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::auto_id_vec(rows)),
            BulkLoadOptions::default(),
        )
        .unwrap();

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.len(), 50);
    assert_eq!(t.get(1).map(String::as_str), Some("r0"));
    assert_eq!(t.get(50).map(String::as_str), Some("r49"));
    assert_eq!(t.get(51), None);
}

#[test]
fn bulk_load_replace_missing_table_without_create_errors() {
    let store = Store::default();
    let rows: Vec<(u64, String)> = vec![(1, "x".into())];
    let opts = BulkLoadOptions {
        create_if_missing: false,
        ..Default::default()
    };
    let res = store.bulk_load::<String>(
        "missing",
        BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
        opts,
    );
    assert!(matches!(res, Err(Error::TableNotFound(_))));
}

#[test]
fn bulk_load_delta_applies_inserts_updates_deletes() {
    let store = Store::default();
    // Seed: ids 1..=10 with values "v1".."v10".
    let seed: Vec<(u64, String)> = (1u64..=10).map(|i| (i, format!("v{i}"))).collect();
    store
        .bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::sorted_vec(seed)),
            BulkLoadOptions::default(),
        )
        .unwrap();

    let delta = BulkDelta {
        inserts: vec![(11, "v11".into()), (12, "v12".into())],
        updates: vec![(5, "v5_new".into())],
        deletes: vec![3, 7],
    };
    store
        .bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default())
        .unwrap();

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.len(), 10); // 10 - 2 deletes + 2 inserts = 10
    assert_eq!(t.get(3), None);
    assert_eq!(t.get(7), None);
    assert_eq!(t.get(5).map(String::as_str), Some("v5_new"));
    assert_eq!(t.get(11).map(String::as_str), Some("v11"));
    assert_eq!(t.get(12).map(String::as_str), Some("v12"));
}

#[test]
fn bulk_load_delta_rejects_overlapping_ids_across_buckets() {
    let store = Store::default();
    let seed: Vec<(u64, String)> = vec![(1, "x".into())];
    store
        .bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::sorted_vec(seed)),
            BulkLoadOptions::default(),
        )
        .unwrap();

    let bad = BulkDelta {
        inserts: vec![(2, "i".into())],
        updates: vec![(2, "u".into())],
        deletes: vec![],
    };
    let res = store.bulk_load::<String>("t", BulkLoadInput::Delta(bad), BulkLoadOptions::default());
    assert!(matches!(res, Err(Error::InvalidBulkLoadInput(_))));
}

#[test]
fn bulk_load_delta_missing_table_errors() {
    let store = Store::default();
    let bad = BulkDelta::<String> {
        inserts: vec![(1, "x".into())],
        updates: vec![],
        deletes: vec![],
    };
    let res = store.bulk_load::<String>(
        "missing",
        BulkLoadInput::Delta(bad),
        BulkLoadOptions::default(),
    );
    assert!(matches!(res, Err(Error::TableNotFound(_))));
}

#[test]
fn write_tx_table_bulk_load_via_batch() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table::<String>("t").unwrap();
    t.bulk_load(BulkLoadInput::Replace(BulkSource::auto_id_vec(
        (0..50).map(|i| format!("v{i}")).collect(),
    )))
    .unwrap();
    drop(t);
    wtx.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.len(), 50);
}

#[test]
fn bulk_load_replace_does_not_disturb_existing_read_tx() {
    let store = Store::default();
    let seed: Vec<(u64, String)> = (1u64..=5).map(|i| (i, format!("old{i}"))).collect();
    let v1 = store
        .bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::sorted_vec(seed)),
            BulkLoadOptions::default(),
        )
        .unwrap();

    let rtx_pre = store.begin_read(Some(v1)).unwrap();

    let new_seed: Vec<(u64, String)> = (1u64..=5).map(|i| (i, format!("new{i}"))).collect();
    let v2 = store
        .bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::sorted_vec(new_seed)),
            BulkLoadOptions::default(),
        )
        .unwrap();
    assert!(v2 > v1);

    // The pre-existing ReadTx pinned at v1 still sees the original data.
    let t_pre = rtx_pre.open_table::<String>("t").unwrap();
    assert_eq!(t_pre.get(1).map(String::as_str), Some("old1"));
    assert_eq!(t_pre.get(5).map(String::as_str), Some("old5"));

    // A fresh read sees the new snapshot.
    let rtx_post = store.begin_read(None).unwrap();
    let t_post = rtx_post.open_table::<String>("t").unwrap();
    assert_eq!(t_post.get(1).map(String::as_str), Some("new1"));
    assert_eq!(t_post.get(5).map(String::as_str), Some("new5"));
}

#[test]
fn bulk_load_batch_two_tables_install_atomically() {
    use ultima_db::{AddOptions, BulkLoadInput, BulkLoadOptions, BulkSource, Store};

    let store = Store::default();
    let v_before = store.latest_version();

    let str_rows: Vec<(u64, String)> = (1u64..=10).map(|i| (i, format!("s{i}"))).collect();
    let u64_rows: Vec<(u64, u64)> = (1u64..=5).map(|i| (i, i * 100)).collect();

    let mut batch = store.bulk_load_batch();
    batch
        .add::<String>(
            "strings",
            BulkLoadInput::Replace(BulkSource::sorted_vec(str_rows)),
            AddOptions::default(),
        )
        .unwrap();
    batch
        .add::<u64>(
            "u64s",
            BulkLoadInput::Replace(BulkSource::sorted_vec(u64_rows)),
            AddOptions::default(),
        )
        .unwrap();
    let v_after = batch.commit(BulkLoadOptions::default()).unwrap();
    assert!(v_after > v_before);

    let rtx = store.begin_read(None).unwrap();
    let s = rtx.open_table::<String>("strings").unwrap();
    let u = rtx.open_table::<u64>("u64s").unwrap();
    assert_eq!(s.len(), 10);
    assert_eq!(u.len(), 5);
    assert_eq!(s.get(1).map(String::as_str), Some("s1"));
    assert_eq!(u.get(3).copied(), Some(300));
}

#[test]
fn bulk_load_batch_drop_without_commit_is_noop() {
    use ultima_db::{AddOptions, BulkLoadInput, BulkSource, Store};

    let store = Store::default();
    let v_before = store.latest_version();

    {
        let mut batch = store.bulk_load_batch();
        batch
            .add::<String>(
                "t",
                BulkLoadInput::Replace(BulkSource::sorted_vec(vec![(1, "a".into())])),
                AddOptions::default(),
            )
            .unwrap();
        // Drop without commit.
    }

    assert_eq!(store.latest_version(), v_before);
    let rtx = store.begin_read(None).unwrap();
    assert!(matches!(
        rtx.open_table::<String>("t"),
        Err(ultima_db::Error::TableNotFound(_)),
    ));
}

#[test]
fn bulk_load_batch_conflict_detection() {
    use ultima_db::{AddOptions, BulkLoadInput, BulkLoadOptions, BulkSource, Error, Store};

    let store = Store::default();
    let mut batch = store.bulk_load_batch();
    batch
        .add::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::sorted_vec(vec![(1, "from-batch".into())])),
            AddOptions::default(),
        )
        .unwrap();

    // A concurrent writer commits between batch.add and batch.commit.
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("other")
            .unwrap()
            .insert("x".into())
            .unwrap();
        wtx.commit().unwrap();
    }

    let res = batch.commit(BulkLoadOptions::default());
    assert!(matches!(res, Err(Error::WriteConflict { .. })));
}

#[test]
fn bulk_load_batch_empty_commit_is_noop() {
    use ultima_db::{BulkLoadOptions, Store};

    let store = Store::default();
    let v_before = store.latest_version();
    let v = store
        .bulk_load_batch()
        .commit(BulkLoadOptions::default())
        .unwrap();
    assert_eq!(v, v_before);
    assert_eq!(store.latest_version(), v_before);
}

#[test]
fn bulk_load_batch_rejects_delta() {
    use ultima_db::{AddOptions, BulkDelta, BulkLoadInput, Error, Store};

    let store = Store::default();
    let mut batch = store.bulk_load_batch();
    let res = batch.add::<String>(
        "t",
        BulkLoadInput::Delta(BulkDelta::default()),
        AddOptions::default(),
    );
    assert!(matches!(res, Err(Error::InvalidBulkLoadInput(_))));
}

#[test]
fn bulk_load_batch_create_if_missing_false_errors() {
    use ultima_db::{AddOptions, BulkLoadInput, BulkSource, Error, Store};

    let store = Store::default();
    let mut batch = store.bulk_load_batch();
    let res = batch.add::<String>(
        "missing",
        BulkLoadInput::Replace(BulkSource::sorted_vec(vec![(1, "x".into())])),
        AddOptions {
            create_if_missing: false,
        },
    );
    assert!(matches!(res, Err(Error::TableNotFound(_))));
}

#[test]
fn bulk_load_batch_add_validates_input_eagerly() {
    use ultima_db::{AddOptions, BulkLoadInput, BulkSource, Error, Store};

    let store = Store::default();
    let mut batch = store.bulk_load_batch();
    // BulkSource::Sorted with duplicate IDs — caught by materialize_source
    // immediately at `add`, not deferred to commit.
    let res = batch.add::<String>(
        "t",
        BulkLoadInput::Replace(BulkSource::sorted_vec(vec![
            (1, "a".into()),
            (1, "b".into()),
        ])),
        AddOptions::default(),
    );
    assert!(matches!(res, Err(Error::InvalidBulkLoadInput(_))));
}

// ---------------------------------------------------------------------------
// Materialize-time error paths (BulkSource validation)
// ---------------------------------------------------------------------------

#[test]
fn bulk_load_replace_sorted_non_ascending_rejected() {
    let store = Store::default();
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Replace(BulkSource::sorted_vec(vec![
            (5, "a".into()),
            (3, "b".into()),
        ])),
        BulkLoadOptions::default(),
    );
    let err = res.unwrap_err();
    match err {
        Error::InvalidBulkLoadInput(msg) => assert!(msg.contains("not ascending")),
        other => panic!("expected InvalidBulkLoadInput(not ascending), got {other:?}"),
    }
}

#[test]
fn bulk_load_replace_unsorted_with_duplicate_id_rejected() {
    let store = Store::default();
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Replace(BulkSource::unsorted_vec(vec![
            (3, "a".into()),
            (1, "b".into()),
            (3, "c".into()),
        ])),
        BulkLoadOptions::default(),
    );
    assert!(matches!(res, Err(Error::InvalidBulkLoadInput(_))));
}

#[test]
fn bulk_load_replace_unsorted_sorts_internally() {
    let store = Store::default();
    let v = store
        .bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::unsorted_vec(vec![
                (3, "c".into()),
                (1, "a".into()),
                (2, "b".into()),
            ])),
            BulkLoadOptions::default(),
        )
        .unwrap();
    let rtx = store.begin_read(Some(v)).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.get(1).map(String::as_str), Some("a"));
    assert_eq!(t.get(2).map(String::as_str), Some("b"));
    assert_eq!(t.get(3).map(String::as_str), Some("c"));
}

#[test]
fn bulk_load_replace_auto_id_via_unsorted_vec_assigns_1_to_n() {
    let store = Store::default();
    let v = store
        .bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::auto_id_vec(vec![
                "a".into(),
                "b".into(),
                "c".into(),
            ])),
            BulkLoadOptions::default(),
        )
        .unwrap();
    let rtx = store.begin_read(Some(v)).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.get(1).map(String::as_str), Some("a"));
    assert_eq!(t.get(2).map(String::as_str), Some("b"));
    assert_eq!(t.get(3).map(String::as_str), Some("c"));
    assert_eq!(t.len(), 3);
}

// ---------------------------------------------------------------------------
// Delta validation error paths (materialize_delta)
// ---------------------------------------------------------------------------

fn store_with_seed_rows(ids: &[u64]) -> Store {
    let store = Store::default();
    let rows: Vec<(u64, String)> = ids.iter().map(|id| (*id, format!("v{id}"))).collect();
    store
        .bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
            BulkLoadOptions::default(),
        )
        .unwrap();
    store
}

#[test]
fn bulk_load_delta_duplicate_in_inserts_rejected() {
    let store = store_with_seed_rows(&[1, 2, 3]);
    let delta = BulkDelta {
        inserts: vec![(10, "a".into()), (10, "b".into())],
        ..Default::default()
    };
    let res =
        store.bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default());
    match res.unwrap_err() {
        Error::InvalidBulkLoadInput(msg) => assert!(msg.contains("inserts")),
        other => panic!("expected InvalidBulkLoadInput(inserts), got {other:?}"),
    }
}

#[test]
fn bulk_load_delta_duplicate_in_updates_rejected() {
    let store = store_with_seed_rows(&[1, 2, 3]);
    let delta = BulkDelta {
        updates: vec![(2, "x".into()), (2, "y".into())],
        ..Default::default()
    };
    let res =
        store.bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default());
    match res.unwrap_err() {
        Error::InvalidBulkLoadInput(msg) => assert!(msg.contains("updates")),
        other => panic!("expected InvalidBulkLoadInput(updates), got {other:?}"),
    }
}

#[test]
fn bulk_load_delta_duplicate_in_deletes_rejected() {
    let store = store_with_seed_rows(&[1, 2, 3]);
    let delta: BulkDelta<String> = BulkDelta {
        deletes: vec![1, 1],
        ..Default::default()
    };
    let res =
        store.bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default());
    match res.unwrap_err() {
        Error::InvalidBulkLoadInput(msg) => assert!(msg.contains("deletes")),
        other => panic!("expected InvalidBulkLoadInput(deletes), got {other:?}"),
    }
}

#[test]
fn bulk_load_delta_cross_bucket_insert_update_overlap() {
    let store = store_with_seed_rows(&[1, 2, 3]);
    let delta = BulkDelta {
        inserts: vec![(2, "new".into())],
        updates: vec![(2, "upd".into())],
        ..Default::default()
    };
    let res =
        store.bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default());
    match res.unwrap_err() {
        Error::InvalidBulkLoadInput(msg) => assert!(msg.contains("multiple buckets")),
        other => panic!("expected InvalidBulkLoadInput(multiple buckets), got {other:?}"),
    }
}

#[test]
fn bulk_load_delta_cross_bucket_update_delete_overlap() {
    let store = store_with_seed_rows(&[1, 2, 3]);
    let delta = BulkDelta {
        updates: vec![(2, "upd".into())],
        deletes: vec![2],
        ..Default::default()
    };
    let res =
        store.bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default());
    match res.unwrap_err() {
        Error::InvalidBulkLoadInput(msg) => assert!(msg.contains("multiple buckets")),
        other => panic!("expected InvalidBulkLoadInput(multiple buckets), got {other:?}"),
    }
}

#[test]
fn bulk_load_delta_cross_bucket_insert_delete_overlap() {
    let store = store_with_seed_rows(&[1, 2, 3]);
    let delta: BulkDelta<String> = BulkDelta {
        inserts: vec![(7, "new".into())],
        deletes: vec![7],
        ..Default::default()
    };
    let res =
        store.bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default());
    match res.unwrap_err() {
        Error::InvalidBulkLoadInput(msg) => assert!(msg.contains("multiple buckets")),
        other => panic!("expected InvalidBulkLoadInput(multiple buckets), got {other:?}"),
    }
}

#[test]
fn bulk_load_delta_update_missing_id_returns_key_not_found() {
    let store = store_with_seed_rows(&[1, 2, 3]);
    let delta = BulkDelta {
        updates: vec![(99, "ghost".into())],
        ..Default::default()
    };
    let res =
        store.bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default());
    assert!(matches!(res, Err(Error::KeyNotFound)));
}

#[test]
fn bulk_load_delta_delete_missing_id_returns_key_not_found() {
    let store = store_with_seed_rows(&[1, 2, 3]);
    let delta: BulkDelta<String> = BulkDelta {
        deletes: vec![99],
        ..Default::default()
    };
    let res =
        store.bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default());
    assert!(matches!(res, Err(Error::KeyNotFound)));
}

#[test]
fn bulk_load_delta_insert_existing_id_returns_duplicate_key() {
    let store = store_with_seed_rows(&[1, 2, 3]);
    let delta = BulkDelta {
        inserts: vec![(2, "boom".into())],
        ..Default::default()
    };
    let res =
        store.bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default());
    assert!(matches!(res, Err(Error::DuplicateKey(_))));
}

#[test]
fn bulk_load_delta_merges_inserts_into_base_in_id_order() {
    // Exercise the sort-merge loop in materialize_delta: base ids interleave
    // with inserted ids, with deletes and updates mixed in.
    let store = store_with_seed_rows(&[1, 3, 5, 7]);
    let delta = BulkDelta {
        inserts: vec![(2, "two".into()), (6, "six".into())],
        updates: vec![(3, "three-updated".into())],
        deletes: vec![5],
    };
    store
        .bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default())
        .unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    let observed: Vec<(u64, String)> = t.iter().map(|(id, s)| (id, s.clone())).collect();
    assert_eq!(
        observed,
        vec![
            (1, "v1".into()),
            (2, "two".into()),
            (3, "three-updated".into()),
            (6, "six".into()),
            (7, "v7".into()),
        ]
    );
}

// ---------------------------------------------------------------------------
// WriteTx::bulk_load — in-tx convenience wrapper
// ---------------------------------------------------------------------------

#[test]
fn write_tx_bulk_load_replace_sorted_resets_table() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.insert("seed".into()).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.bulk_load(BulkLoadInput::Replace(BulkSource::sorted_vec(vec![
            (10, "a".into()),
            (20, "b".into()),
        ])))
        .unwrap();
        wtx.commit().unwrap();
    }
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t.get(10).map(String::as_str), Some("a"));
    assert_eq!(t.get(20).map(String::as_str), Some("b"));
}

#[test]
fn write_tx_bulk_load_replace_auto_id_continues_from_next_id() {
    // Unlike Store::bulk_load (1..=N on fresh table), WriteTx::bulk_load
    // Replace+AutoId uses the in-tx insert path which auto-assigns from
    // the table's current next_id.
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.insert("seed1".into()).unwrap();
        t.insert("seed2".into()).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.bulk_load(BulkLoadInput::Replace(BulkSource::auto_id_vec(vec![
            "x".into(),
            "y".into(),
        ])))
        .unwrap();
        wtx.commit().unwrap();
    }
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.len(), 2);
    // After Replace deletes the seed rows, next_id has advanced past them.
    let ids: Vec<u64> = t.iter().map(|(id, _)| id).collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.iter().all(|id| *id >= 3));
}

#[test]
fn write_tx_bulk_load_delta_applies_each_bucket() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        for s in ["v1", "v2", "v3"] {
            t.insert(s.into()).unwrap();
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        let delta = BulkDelta {
            inserts: vec![(10, "ten".into())],
            updates: vec![(2, "two-upd".into())],
            deletes: vec![3],
        };
        t.bulk_load(BulkLoadInput::Delta(delta)).unwrap();
        wtx.commit().unwrap();
    }
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.get(1).map(String::as_str), Some("v1"));
    assert_eq!(t.get(2).map(String::as_str), Some("two-upd"));
    assert_eq!(t.get(3), None);
    assert_eq!(t.get(10).map(String::as_str), Some("ten"));
}

// ---------------------------------------------------------------------------
// Index rebuild behavior across Replace
// ---------------------------------------------------------------------------

#[test]
fn bulk_load_replace_preserves_unique_index_definition() {
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
    struct U {
        email: String,
    }

    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<U>("u").unwrap();
        t.define_index("by_email", IndexKind::Unique, |u: &U| u.email.clone())
            .unwrap();
        t.insert(U {
            email: "old@x.com".into(),
        })
        .unwrap();
        wtx.commit().unwrap();
    }

    let rows: Vec<(u64, U)> = (1u64..=10)
        .map(|i| {
            (
                i,
                U {
                    email: format!("user{i}@x.com"),
                },
            )
        })
        .collect();
    store
        .bulk_load::<U>(
            "u",
            BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
            BulkLoadOptions::default(),
        )
        .unwrap();

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<U>("u").unwrap();
    let hit = t
        .get_unique::<String>("by_email", &"user5@x.com".to_string())
        .unwrap();
    assert!(hit.is_some());
    let stale = t
        .get_unique::<String>("by_email", &"old@x.com".to_string())
        .unwrap();
    assert!(stale.is_none());
}

// ---------------------------------------------------------------------------
// Bulk loads vs in-flight writers (OCC visibility)
// ---------------------------------------------------------------------------

mod bulk_load_occ {
    use ultima_db::{
        BulkDelta, BulkLoadInput, BulkLoadOptions, BulkSource, Error, IndexKind, IsolationLevel,
        Store, StoreConfig, WriterMode,
    };

    fn replace_input(n: u64) -> BulkLoadInput<String> {
        let rows: Vec<(u64, String)> = (1..=n).map(|i| (i, format!("bulk{i}"))).collect();
        BulkLoadInput::Replace(BulkSource::sorted_vec(rows))
    }

    fn seed(store: &Store, table: &str, rows: &[&str]) {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>(table).unwrap();
        for r in rows {
            t.insert((*r).to_string()).unwrap();
        }
        wtx.commit().unwrap();
    }

    fn multi_writer_store() -> Store {
        Store::new(StoreConfig::builder().writer_mode(WriterMode::MultiWriter).build())
            .unwrap()
    }

    /// A MultiWriter transaction whose writes were computed against the
    /// pre-replace table must conflict at commit — its dirty table would
    /// otherwise wholesale-overwrite the bulk-loaded data.
    #[test]
    fn inflight_writer_on_replaced_table_conflicts_at_commit() {
        let store = multi_writer_store();
        seed(&store, "t", &["seed1", "seed2"]);

        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t")
            .unwrap()
            .update(1, "from_writer".into())
            .unwrap();

        let bulk_v = store
            .bulk_load::<String>("t", replace_input(3), BulkLoadOptions::default())
            .unwrap();

        let res = wtx.commit();
        assert!(
            matches!(res, Err(Error::WriteConflict { .. })),
            "writer based on pre-replace data must conflict, got {res:?}"
        );

        // The bulk-loaded data must be intact at latest.
        assert_eq!(store.latest_version(), bulk_v);
        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table::<String>("t").unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(t.get(1).map(String::as_str), Some("bulk1"));
    }

    /// The sibling of the test above, for a transaction that **opened** the
    /// replaced table but never wrote to it.
    ///
    /// `WriteTx::open_table` eagerly clones the table into `self.dirty`, so a
    /// write-free transaction still carries a full pre-replace snapshot of it.
    /// At commit:
    ///
    /// * `validate_write_set` walks `cws.deleted_tables` but only conflicts
    ///   when `self.write_set[deleted]` is **non-empty**, and deliberately so —
    ///   the comment there says "opened but nothing written must not count",
    ///   because a snapshot read of a since-replaced table is fine under SI.
    /// * `install_batch_inner` records its `CommittedWriteSet` with
    ///   `tables: BTreeMap::new()`, so `has_concurrent` — computed only from
    ///   `cws.tables.contains_key(n)` — is `false` for the replaced table.
    /// * Phase 2 therefore takes the fast path and installs `my_dirty`
    ///   wholesale.
    ///
    /// The read-safety argument justifies skipping the *conflict*; it does not
    /// address the transaction then writing that stale snapshot back. If the
    /// assertions below fail, the bulk load was silently reverted by a
    /// transaction that wrote nothing.
    #[test]
    fn write_free_txn_that_opened_a_replaced_table_must_not_revert_the_bulk_load() {
        let store = multi_writer_store();
        seed(&store, "t", &["seed1", "seed2"]);

        // Opens the table — and nothing else. No insert, update or delete.
        let mut wtx = store.begin_write(None).unwrap();
        let t = wtx.open_table::<String>("t").unwrap();
        assert_eq!(t.len(), 2, "the txn holds a pre-replace clone");
        drop(t);

        let bulk_v = store
            .bulk_load::<String>("t", replace_input(3), BulkLoadOptions::default())
            .unwrap();

        // The transaction must still see its own snapshot: not conflicting a
        // write-free opener is justified on the grounds that a snapshot read of
        // a since-replaced table is fine under SI, so that read has to keep
        // working. This is the guarantee the fix must not have traded away.
        let t = wtx.open_table::<String>("t").unwrap();
        assert_eq!(t.len(), 2, "the txn must still read its own pre-replace snapshot");
        assert_eq!(t.get(1).map(String::as_str), Some("seed1"));
        drop(t);

        // Whether this conflicts is not the point — either outcome is
        // defensible for a write-free transaction, and pinning one here would
        // freeze `validate_write_set`'s opened-but-unwritten policy inside a
        // bulk-load test. Bound it to the defensible set so an unrelated error
        // cannot pass silently, and let the data assertions carry the contract.
        let res = wtx.commit();
        assert!(
            matches!(res, Ok(_) | Err(Error::WriteConflict { .. })),
            "unexpected commit outcome: {res:?}"
        );

        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table::<String>("t").unwrap();
        assert_eq!(
            t.len(),
            3,
            "bulk-loaded table was reverted by a write-free txn (commit -> {res:?})"
        );
        assert_eq!(t.get(1).map(String::as_str), Some("bulk1"));
        assert_eq!(t.get(3).map(String::as_str), Some("bulk3"));
        assert!(
            t.get(2).map(String::as_str) != Some("seed2"),
            "pre-replace row resurrected (commit -> {res:?})"
        );
        assert!(store.latest_version() >= bulk_v);
    }

    /// The one behaviour this fix intentionally changed.
    ///
    /// `define_index` records into `ddl_tables`, never into `write_set`, so a
    /// DDL-only transaction slips past `validate_write_set` exactly like a
    /// write-free one. Before the fix it took the fast path and reinstated its
    /// pre-replace clone — destroying the bulk load. Of the three available
    /// outcomes (install wholesale and lose the load; take the merge slow path
    /// and silently drop the DDL, which task41 forbids; or fail loudly) only
    /// the third is defensible, so it now raises `IndexDdlConflict` — the same
    /// contract task41 already established for DDL racing an ordinary commit.
    ///
    /// The existing `mw_index_ddl_with_concurrent_commit_errors` drives that
    /// conflict through `cws.tables`; this drives it through `deleted_tables`,
    /// which nothing else covers.
    #[test]
    fn ddl_only_txn_on_a_bulk_replaced_table_fails_loudly() {
        let store = multi_writer_store();
        seed(&store, "t", &["seed1", "seed2"]);

        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t")
            .unwrap()
            .define_index("by_val", IndexKind::Unique, |s: &String| s.clone())
            .unwrap();

        store
            .bulk_load::<String>("t", replace_input(3), BulkLoadOptions::default())
            .unwrap();

        let res = wtx.commit();
        assert!(
            matches!(res, Err(Error::IndexDdlConflict { .. })),
            "DDL over a bulk-replaced table must fail loudly, got {res:?}"
        );

        // And the load survives, which is the point of failing rather than
        // installing.
        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table::<String>("t").unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(t.get(1).map(String::as_str), Some("bulk1"));
    }

    /// The mirror image of `inflight_writer_on_replaced_table_conflicts_at_commit`:
    /// a transaction that **deletes** a table a concurrent `bulk_load` replaced
    /// must conflict, not silently drop the freshly loaded data.
    ///
    /// `validate_write_set`'s second loop conflicts when a concurrent commit
    /// *modified* a table this transaction deleted — but a bulk install's
    /// `CommittedWriteSet` has an empty `tables` map, so a check against
    /// `cws.tables` alone never fires and the delete wins. The delete was
    /// decided against contents the load has since replaced, so it must lose.
    #[test]
    fn delete_table_conflicts_with_a_concurrent_bulk_replace() {
        let store = multi_writer_store();
        seed(&store, "t", &["seed1", "seed2"]);

        let mut wtx = store.begin_write(None).unwrap();
        wtx.delete_table("t");

        let bulk_v = store
            .bulk_load::<String>("t", replace_input(3), BulkLoadOptions::default())
            .unwrap();

        let res = wtx.commit();
        assert!(
            matches!(res, Err(Error::WriteConflict { ref table, .. }) if table == "t"),
            "delete over a bulk-replaced table must conflict, got {res:?}"
        );

        // The load must still be there — the point of conflicting.
        assert_eq!(store.latest_version(), bulk_v);
        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table::<String>("t").unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(t.get(1).map(String::as_str), Some("bulk1"));
    }

    /// The delete must still not conflict with a *different* table's load —
    /// `installed_tables` is per-table like every other OCC check.
    #[test]
    fn delete_table_does_not_conflict_with_a_bulk_replace_of_another_table() {
        let store = multi_writer_store();
        seed(&store, "t", &["seed1"]);
        seed(&store, "other", &["o1"]);

        let mut wtx = store.begin_write(None).unwrap();
        wtx.delete_table("t");

        store
            .bulk_load::<String>("other", replace_input(3), BulkLoadOptions::default())
            .unwrap();

        wtx.commit().expect("unrelated table load must not conflict");

        let rtx = store.begin_read(None).unwrap();
        assert!(rtx.open_table::<String>("t").is_err());
        assert_eq!(rtx.open_table::<String>("other").unwrap().len(), 3);
    }

    /// The same fix also closes a *lost `delete_table`*: a transaction that
    /// deleted a table and reopened it (write-free) racing a bulk replace used
    /// to commit `Ok` with its `delete_table` silently discarded.
    ///
    /// Pre-fix trace: the write set for "t" is empty, so
    /// `validate_write_set`'s first loop does not fire; the second loop found
    /// nothing because a bulk install's `tables` map is empty; and Phase 2
    /// took the `(Some(_), _)` arm, keeping the bulk-loaded table as-is. The
    /// reopen had already cleared "t" from `deleted_tables`, so nothing
    /// removed it at install — commit succeeded and the table survived with
    /// the loaded contents, the delete having simply evaporated.
    ///
    /// Note this needs the write-free form. The same shape *with* a row write
    /// has always conflicted, on the first loop, which neither fix touches.
    #[test]
    fn write_free_delete_and_recreate_over_a_bulk_replace_conflicts() {
        let store = multi_writer_store();
        seed(&store, "t", &["seed1", "seed2"]);

        let mut wtx = store.begin_write(None).unwrap();
        assert!(wtx.delete_table("t"));
        assert!(wtx.open_table::<String>("t").unwrap().is_empty());

        store
            .bulk_load::<String>("t", replace_input(3), BulkLoadOptions::default())
            .unwrap();

        let res = wtx.commit();
        assert!(
            matches!(res, Err(Error::WriteConflict { ref table, .. }) if table == "t"),
            "a delete racing a bulk replace must conflict, not be dropped, got {res:?}"
        );

        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table::<String>("t").unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(t.get(1).map(String::as_str), Some("bulk1"));
    }

    /// Same as above through the Delta path.
    #[test]
    fn inflight_writer_on_delta_loaded_table_conflicts_at_commit() {
        let store = multi_writer_store();
        seed(&store, "t", &["seed1", "seed2"]);

        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t")
            .unwrap()
            .update(2, "from_writer".into())
            .unwrap();

        let delta = BulkDelta {
            inserts: vec![(3, "delta3".to_string())],
            updates: vec![],
            deletes: vec![],
        };
        store
            .bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default())
            .unwrap();

        let res = wtx.commit();
        assert!(
            matches!(res, Err(Error::WriteConflict { .. })),
            "writer based on pre-delta data must conflict, got {res:?}"
        );

        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table::<String>("t").unwrap();
        assert_eq!(t.get(3).map(String::as_str), Some("delta3"));
    }

    /// A writer that never touched the replaced table is unaffected: it
    /// commits cleanly and both its data and the bulk data are present.
    #[test]
    fn inflight_writer_on_other_table_unaffected() {
        let store = multi_writer_store();
        seed(&store, "t", &["seed1"]);

        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("other")
            .unwrap()
            .insert("writer_row".into())
            .unwrap();

        store
            .bulk_load::<String>("t", replace_input(2), BulkLoadOptions::default())
            .unwrap();

        wtx.commit().expect("writer on a different table must commit");

        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table::<String>("t").unwrap();
        assert_eq!(t.len(), 2, "bulk data lost");
        let other = rtx.open_table::<String>("other").unwrap();
        assert_eq!(other.len(), 1, "writer data lost");
    }

    /// SingleWriter mode has no commit-time OCC, so the load itself must be
    /// refused while a writer is active — otherwise the writer's eventual
    /// commit silently overwrites the loaded table.
    #[test]
    fn refused_while_single_writer_active() {
        let store = Store::default(); // SingleWriter
        seed(&store, "t", &["seed1"]);

        let wtx = store.begin_write(None).unwrap();
        let res = store.bulk_load::<String>("t", replace_input(2), BulkLoadOptions::default());
        assert!(
            matches!(res, Err(Error::WriterBusy)),
            "bulk load with an active SingleWriter must be refused, got {res:?}"
        );
        drop(wtx);

        // Once the writer is gone the load goes through.
        store
            .bulk_load::<String>("t", replace_input(2), BulkLoadOptions::default())
            .expect("load after writer release");
    }

    /// Delta path: same SingleWriter refusal.
    #[test]
    fn delta_refused_while_single_writer_active() {
        let store = Store::default();
        seed(&store, "t", &["seed1"]);

        let wtx = store.begin_write(None).unwrap();
        let delta = BulkDelta {
            inserts: vec![(2, "delta2".to_string())],
            updates: vec![],
            deletes: vec![],
        };
        let res =
            store.bulk_load::<String>("t", BulkLoadInput::Delta(delta), BulkLoadOptions::default());
        assert!(
            matches!(res, Err(Error::WriterBusy)),
            "delta load with an active SingleWriter must be refused, got {res:?}"
        );
        drop(wtx);
    }

    /// Under Serializable isolation, a transaction that *read* the replaced
    /// table must fail with SerializationFailure (its reads no longer hold).
    #[test]
    fn serializable_reader_of_replaced_table_fails() {
        let store = Store::new(
            StoreConfig::builder()
                .writer_mode(WriterMode::MultiWriter)
                .isolation_level(IsolationLevel::Serializable)
                .build(),
        )
        .unwrap();
        seed(&store, "t", &["seed1"]);

        let mut wtx = store.begin_write(None).unwrap();
        // Point-read the soon-to-be-replaced table; write a different one.
        let _ = wtx.open_table::<String>("t").unwrap().get(1);
        wtx.open_table::<String>("other")
            .unwrap()
            .insert("x".into())
            .unwrap();

        store
            .bulk_load::<String>("t", replace_input(2), BulkLoadOptions::default())
            .unwrap();

        let res = wtx.commit();
        assert!(
            matches!(res, Err(Error::SerializationFailure { .. })),
            "serializable reader of replaced table must fail, got {res:?}"
        );
    }

    /// Under plain Snapshot Isolation the same read is permitted — only the
    /// transaction's *writes* matter, and they're on a different table.
    #[test]
    fn snapshot_isolation_reader_of_replaced_table_commits() {
        let store = multi_writer_store();
        seed(&store, "t", &["seed1"]);

        let mut wtx = store.begin_write(None).unwrap();
        let _ = wtx.open_table::<String>("t").unwrap().get(1);
        wtx.open_table::<String>("other")
            .unwrap()
            .insert("x".into())
            .unwrap();

        store
            .bulk_load::<String>("t", replace_input(2), BulkLoadOptions::default())
            .unwrap();

        wtx.commit()
            .expect("SI reader of replaced table must commit");
    }
}

// ---------------------------------------------------------------------------
// Bulk loads on tables with custom indexes
// ---------------------------------------------------------------------------

mod bulk_load_custom_index {
    use ultima_db::{BulkLoadInput, BulkLoadOptions, BulkSource, CustomIndex, Error, Store};

    /// Minimal custom index: counts live records.
    #[derive(Clone, Default)]
    struct CountIndex {
        n: u64,
    }

    impl CustomIndex<String> for CountIndex {
        fn on_insert(&mut self, _id: u64, _r: &String) -> Result<(), Error> {
            self.n += 1;
            Ok(())
        }
        fn on_update(&mut self, _id: u64, _old: &String, _new: &String) -> Result<(), Error> {
            Ok(())
        }
        fn on_delete(&mut self, _id: u64, _r: &String) {
            self.n -= 1;
        }
    }

    /// A bulk Replace on a table with a custom index cannot preserve the
    /// index definition (custom indexes have no generic "make empty" hook).
    /// That must surface as an `Err`, never a panic — it is a documented,
    /// supported combination (`VectorCollection` steers users to define
    /// indexes on its data table, then `restore_vec` hits this path).
    #[test]
    fn replace_with_custom_index_errors_instead_of_panicking() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("seed".into()).unwrap();
            t.define_custom_index("count", CountIndex::default()).unwrap();
            wtx.commit().unwrap();
        }

        let rows: Vec<(u64, String)> = vec![(1, "bulk".into())];
        let res = store.bulk_load::<String>(
            "t",
            BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
            BulkLoadOptions::default(),
        );
        assert!(
            res.is_err(),
            "bulk Replace over a custom index must error, got {res:?}"
        );

        // The table is untouched by the failed load.
        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table::<String>("t").unwrap();
        assert_eq!(t.get(1).map(String::as_str), Some("seed"));
    }

    /// Same through the Delta path.
    #[test]
    fn delta_with_custom_index_errors_instead_of_panicking() {
        use ultima_db::BulkDelta;
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("seed".into()).unwrap();
            t.define_custom_index("count", CountIndex::default()).unwrap();
            wtx.commit().unwrap();
        }

        let delta = BulkDelta {
            inserts: vec![(2, "delta".to_string())],
            updates: vec![],
            deletes: vec![],
        };
        let res = store.bulk_load::<String>(
            "t",
            BulkLoadInput::Delta(delta),
            BulkLoadOptions::default(),
        );
        assert!(
            res.is_err(),
            "bulk Delta over a custom index must error, got {res:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Store::bulk_load_keyed — bulk load over an arbitrary primary key
// ---------------------------------------------------------------------------

mod bulk_load_keyed {
    use ultima_db::{Error, IndexKind, Store};

    #[test]
    fn bulk_load_string_keyed_table() {
        let store = Store::default();

        let rows = vec![
            ("a@x.com".to_string(), "Alice".to_string()),
            ("b@x.com".to_string(), "Bob".to_string()),
            ("c@x.com".to_string(), "Carol".to_string()),
        ];

        store
            .bulk_load_keyed::<String, String>("emails", rows, None)
            .unwrap();

        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table_keyed::<String, String>("emails").unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(t.get("b@x.com".to_string()), Some(&"Bob".to_string()));
    }

    #[test]
    fn bulk_load_rejects_unsorted_keys() {
        let store = Store::default();
        let rows = vec![
            ("b@x.com".to_string(), "Bob".to_string()),
            ("a@x.com".to_string(), "Alice".to_string()),
        ];
        let err = store
            .bulk_load_keyed::<String, String>("emails", rows, None)
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidBulkLoadInput(_)),
            "expected InvalidBulkLoadInput, got {err:?}"
        );
        // Rejected before anything was built: no table exists.
        let rtx = store.begin_read(None).unwrap();
        assert!(rtx.open_table_keyed::<String, String>("emails").is_err());
    }

    #[test]
    fn bulk_load_rejects_duplicate_keys() {
        let store = Store::default();
        let rows = vec![
            ("dup".to_string(), "first".to_string()),
            ("dup".to_string(), "second".to_string()),
        ];
        let err = store
            .bulk_load_keyed::<String, String>("t", rows, None)
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidBulkLoadInput(_)),
            "expected InvalidBulkLoadInput, got {err:?}"
        );
    }

    /// Variable-length keys, including a prefix pair ("b" < "bb"), load and
    /// scan back in `K`'s `Ord` order.
    ///
    /// Note what this does *not* test: the bulk-load path never calls
    /// `PrimaryKey::encode` — `from_bulk` → `BTree::from_sorted` is keyed on
    /// `K` throughout — so this passes with an arbitrarily broken `encode`.
    /// The encode-order ⟺ key-order equivalence is only load-bearing where
    /// keys become bytes, and it is asserted there, on the wire, in
    /// `tests/snapshot_stream.rs::round_trips_a_string_keyed_table`.
    #[test]
    fn variable_length_keys_load_and_range_in_key_order() {
        let store = Store::default();
        let keys = ["", "a", "ab", "b", "bb", "c", "zzzzzzzzzzzz"];
        let rows: Vec<(String, u64)> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.to_string(), i as u64))
            .collect();

        store.bulk_load_keyed::<u64, String>("t", rows, None).unwrap();

        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table_keyed::<u64, String>("t").unwrap();
        assert_eq!(t.len(), keys.len());
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(t.get(k.to_string()), Some(&(i as u64)), "key {k:?}");
        }
        let scanned: Vec<String> = t.range(..).map(|(k, _)| k.clone()).collect();
        let expected: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        assert_eq!(scanned, expected, "rows must come back in key order");
    }

    /// Replace semantics: previous contents go away, index *definitions* stay
    /// and are rebuilt over the loaded rows.
    #[test]
    fn preserves_index_definitions_and_replaces_contents() {
        let store = Store::default();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table_keyed::<u64, String>("t").unwrap();
            t.define_index("by_value", IndexKind::Unique, |r: &u64| *r)
                .unwrap();
            t.put("old".to_string(), 900).unwrap();
            wtx.commit().unwrap();
        }

        let rows: Vec<(String, u64)> = vec![
            ("k1".to_string(), 10),
            ("k2".to_string(), 20),
            ("k3".to_string(), 30),
        ];
        store.bulk_load_keyed::<u64, String>("t", rows, None).unwrap();

        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table_keyed::<u64, String>("t").unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(t.get("old".to_string()), None, "replace must drop old rows");
        assert_eq!(
            t.get_unique("by_value", &20u64).unwrap(),
            Some(("k2".to_string(), &20))
        );
        assert_eq!(
            t.get_unique("by_value", &900u64).unwrap(),
            None,
            "the replaced row must be gone from the index too"
        );
    }

    /// `create_if_missing: false` on a table that does not exist.
    #[test]
    fn honours_create_if_missing_false() {
        use ultima_db::BulkLoadOptions;

        let store = Store::default();
        let opts = BulkLoadOptions {
            create_if_missing: false,
            checkpoint_after: false,
        };
        let err = store
            .bulk_load_keyed::<String, String>("nope", vec![], Some(opts))
            .unwrap_err();
        assert!(
            matches!(err, Error::TableNotFound(_)),
            "expected TableNotFound, got {err:?}"
        );
    }

    /// A `u64`-keyed table loaded through the keyed entry point keeps its
    /// auto-increment counter: a subsequent `insert` must not reissue an id
    /// the load already used.
    #[test]
    fn u64_keys_advance_the_auto_increment_counter() {
        let store = Store::default();
        let rows: Vec<(u64, String)> = (1u64..=5).map(|i| (i, format!("v{i}"))).collect();
        store.bulk_load_keyed::<String, u64>("t", rows, None).unwrap();

        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        let id = t.insert("next".to_string()).unwrap();
        assert_eq!(id, 6, "counter must resume past the highest loaded id");
        wtx.commit().unwrap();
    }

    #[test]
    fn empty_rows_creates_an_empty_table() {
        let store = Store::default();
        store
            .bulk_load_keyed::<String, String>("t", vec![], None)
            .unwrap();
        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table_keyed::<String, String>("t").unwrap();
        assert_eq!(t.len(), 0);
    }
}

// ---------------------------------------------------------------------------
// bulk_load_keyed vs a commit landing during the build (OCC)
// ---------------------------------------------------------------------------

mod bulk_load_keyed_occ {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ultima_db::{Error, IndexKind, Store, StoreConfig, WriterMode};

    /// `bulk_load_keyed` must capture its base version *before* building the
    /// table, not after. The build is unbounded off-lock work — the index
    /// rebuild calls the extractor once per row — so a commit can land in the
    /// middle of it. Reading `latest_version()` afterwards compares a
    /// post-commit version against itself, and the install then overwrites the
    /// concurrent commit with no error.
    ///
    /// The extractor is the hook that makes the interleaving deterministic:
    /// it is caller code, invoked at a point strictly between the base-version
    /// capture and the install, with no store lock held.
    #[test]
    fn conflicts_with_a_commit_that_lands_during_the_build() {
        let store = Store::new(
            StoreConfig::builder()
                .writer_mode(WriterMode::MultiWriter)
                .build(),
        )
        .unwrap();

        // 0 = disarmed (seeding), 1 = armed, 2 = already fired.
        let state = Arc::new(AtomicUsize::new(0));
        let hook_state = state.clone();
        let hook_store = store.clone();

        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table_keyed::<u64, String>("t").unwrap();
            t.define_index("by_value", IndexKind::NonUnique, move |r: &u64| {
                if hook_state
                    .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    // A concurrent committer advances latest_version while
                    // the bulk load is mid-build.
                    let mut w = hook_store.begin_write(None).unwrap();
                    w.open_table_keyed::<u64, String>("other")
                        .unwrap()
                        .put("x".to_string(), 7)
                        .unwrap();
                    w.commit().unwrap();
                }
                *r
            })
            .unwrap();
            t.put("seed".to_string(), 0).unwrap();
            wtx.commit().unwrap();
        }

        state.store(1, Ordering::SeqCst);
        let rows: Vec<(String, u64)> = (0..8u64).map(|i| (format!("k{i}"), i)).collect();
        let res = store.bulk_load_keyed::<u64, String>("t", rows, None);

        assert_eq!(state.load(Ordering::SeqCst), 2, "the hook must have fired");
        assert!(
            matches!(res, Err(Error::WriteConflict { .. })),
            "a commit during the build must be refused, got {res:?}"
        );

        // The concurrent commit survived intact — the failure mode this
        // guards is the install silently erasing it.
        let rtx = store.begin_read(None).unwrap();
        let other = rtx.open_table_keyed::<u64, String>("other").unwrap();
        assert_eq!(other.get("x".to_string()), Some(&7));
        // And the load did not land: "t" still holds only the seed row.
        let t = rtx.open_table_keyed::<u64, String>("t").unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t.get("seed".to_string()), Some(&0));
    }

    /// The uncontended path must still succeed — the fix must not turn every
    /// load with an index into a conflict.
    #[test]
    fn succeeds_when_no_commit_lands_during_the_build() {
        let store = Store::new(
            StoreConfig::builder()
                .writer_mode(WriterMode::MultiWriter)
                .build(),
        )
        .unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table_keyed::<u64, String>("t").unwrap();
            t.define_index("by_value", IndexKind::NonUnique, |r: &u64| *r)
                .unwrap();
            t.put("seed".to_string(), 0).unwrap();
            wtx.commit().unwrap();
        }

        let rows: Vec<(String, u64)> = (0..8u64).map(|i| (format!("k{i}"), i)).collect();
        store.bulk_load_keyed::<u64, String>("t", rows, None).unwrap();

        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table_keyed::<u64, String>("t").unwrap();
        assert_eq!(t.len(), 8);
        // The index definition survived and was rebuilt over the new rows.
        assert_eq!(t.get_by_index("by_value", &3u64).unwrap().len(), 1);
    }
}

// ---------------------------------------------------------------------------
// bulk_load_keyed must not create a table that contradicts its registration
// ---------------------------------------------------------------------------

#[cfg(feature = "persistence")]
mod bulk_load_keyed_registration {
    use serde::{Deserialize, Serialize};
    use ultima_db::{Error, Store, StoreConfig};

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
    struct Row {
        v: u64,
    }

    /// Creating a table from nothing is the only moment nothing else fixes its
    /// key type, so it must be held to any registration the store already has.
    /// Without the check the load returns `Ok`, the store then holds a
    /// `Table<Row, String>` under a `u64` registration, and every subsequent
    /// `checkpoint()` fails permanently with an opaque downcast error.
    ///
    /// `open_table_keyed` has always refused this; `bulk_load_keyed` must
    /// agree, or it is a public route back into the disagreement that the
    /// snapshot-stream guards exist to catch.
    #[test]
    fn refuses_a_key_type_that_contradicts_the_registration() {
        let store = Store::new(StoreConfig::default()).unwrap();
        store.register_table::<Row>("t").unwrap(); // u64-keyed

        let rows = vec![("k".to_string(), Row { v: 1 })];
        let err = store
            .bulk_load_keyed::<Row, String>("t", rows, None)
            .unwrap_err();
        assert!(
            matches!(err, Error::TypeMismatch(ref n) if n == "t"),
            "expected TypeMismatch, got {err:?}"
        );

        // The same refusal `open_table_keyed` gives, and the store is clean:
        // a checkpoint still works.
        let mut wtx = store.begin_write(None).unwrap();
        assert!(matches!(
            wtx.open_table_keyed::<Row, String>("t"),
            Err(Error::TypeMismatch(_))
        ));
        drop(wtx);
    }

    /// An unregistered name is still free to take any key type — the check
    /// constrains, it does not require registration.
    #[test]
    fn allows_any_key_type_for_an_unregistered_table() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let rows = vec![("k".to_string(), Row { v: 1 })];
        store
            .bulk_load_keyed::<Row, String>("unregistered", rows, None)
            .unwrap();

        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table_keyed::<Row, String>("unregistered").unwrap();
        assert_eq!(t.get("k".to_string()), Some(&Row { v: 1 }));
    }

    /// A matching registration is of course fine.
    #[test]
    fn allows_the_registered_key_type() {
        let store = Store::new(StoreConfig::default()).unwrap();
        store.register_table_keyed::<Row, String>("t").unwrap();
        let rows = vec![("k".to_string(), Row { v: 1 })];
        store.bulk_load_keyed::<Row, String>("t", rows, None).unwrap();

        let rtx = store.begin_read(None).unwrap();
        assert_eq!(
            rtx.open_table_keyed::<Row, String>("t").unwrap().len(),
            1
        );
    }
}
