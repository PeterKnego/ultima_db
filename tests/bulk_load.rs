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
        t.insert(U { email: "a@x".into() }).unwrap();
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
        .bulk_load::<String>(
            "t",
            BulkLoadInput::Delta(delta),
            BulkLoadOptions::default(),
        )
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
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Delta(bad),
        BulkLoadOptions::default(),
    );
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
        Err(ultima_db::Error::KeyNotFound),
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
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Delta(delta),
        BulkLoadOptions::default(),
    );
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
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Delta(delta),
        BulkLoadOptions::default(),
    );
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
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Delta(delta),
        BulkLoadOptions::default(),
    );
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
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Delta(delta),
        BulkLoadOptions::default(),
    );
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
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Delta(delta),
        BulkLoadOptions::default(),
    );
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
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Delta(delta),
        BulkLoadOptions::default(),
    );
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
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Delta(delta),
        BulkLoadOptions::default(),
    );
    assert!(matches!(res, Err(Error::KeyNotFound)));
}

#[test]
fn bulk_load_delta_delete_missing_id_returns_key_not_found() {
    let store = store_with_seed_rows(&[1, 2, 3]);
    let delta: BulkDelta<String> = BulkDelta {
        deletes: vec![99],
        ..Default::default()
    };
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Delta(delta),
        BulkLoadOptions::default(),
    );
    assert!(matches!(res, Err(Error::KeyNotFound)));
}

#[test]
fn bulk_load_delta_insert_existing_id_returns_duplicate_key() {
    let store = store_with_seed_rows(&[1, 2, 3]);
    let delta = BulkDelta {
        inserts: vec![(2, "boom".into())],
        ..Default::default()
    };
    let res = store.bulk_load::<String>(
        "t",
        BulkLoadInput::Delta(delta),
        BulkLoadOptions::default(),
    );
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
        .bulk_load::<String>(
            "t",
            BulkLoadInput::Delta(delta),
            BulkLoadOptions::default(),
        )
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
        t.insert(U { email: "old@x.com".into() }).unwrap();
        wtx.commit().unwrap();
    }

    let rows: Vec<(u64, U)> = (1u64..=10)
        .map(|i| (i, U { email: format!("user{i}@x.com") }))
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
