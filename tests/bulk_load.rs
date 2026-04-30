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
