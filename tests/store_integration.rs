use ultima_db::{Error, Store};

// ---------------------------------------------------------------------------
// Full CRUD via transactions
// ---------------------------------------------------------------------------

#[test]
fn end_to_end_write_commit_read() {
    let mut store = Store::new();

    let v = {
        let mut wtx = store.begin_write(None).unwrap();
        let table = wtx.open_table::<String>("notes").unwrap();

        let id1 = table.insert("first note".to_string());
        let id2 = table.insert("second note".to_string());
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);

        assert_eq!(table.get(id1), Some(&"first note".to_string()));

        table.update(id1, "updated first".to_string()).unwrap();
        assert_eq!(table.get(id1), Some(&"updated first".to_string()));
        assert!(matches!(table.update(99, "x".to_string()), Err(Error::KeyNotFound)));

        table.delete(id2).unwrap();
        assert_eq!(table.get(id2), None);
        assert_eq!(table.len(), 1);
        assert!(matches!(table.delete(99), Err(Error::KeyNotFound)));

        let id3 = table.insert("third note".to_string());
        let results: Vec<_> = table.range(id1..=id3).collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (id1, &"updated first".to_string()));
        assert_eq!(results[1], (id3, &"third note".to_string()));

        wtx.commit(&mut store).unwrap()
    };

    let rtx = store.begin_read(None).unwrap();
    assert_eq!(rtx.version(), v);
    let table = rtx.open_table::<String>("notes").unwrap();
    assert_eq!(table.get(1), Some(&"updated first".to_string()));
    assert_eq!(table.get(2), None);
    assert_eq!(table.get(3), Some(&"third note".to_string()));
}

// ---------------------------------------------------------------------------
// Snapshot isolation
// ---------------------------------------------------------------------------

#[test]
fn snapshot_isolation() {
    let mut store = Store::new();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string());
        wtx.commit(&mut store).unwrap();
    }

    let rtx_v1 = store.begin_read(Some(1)).unwrap();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("bob".to_string());
        wtx.commit(&mut store).unwrap();
    }

    // v1 read still sees only alice
    let users_v1 = rtx_v1.open_table::<String>("users").unwrap();
    assert_eq!(users_v1.len(), 1);
    assert_eq!(users_v1.get(1), Some(&"alice".to_string()));
    assert_eq!(users_v1.get(2), None);

    // Latest read sees both
    let rtx_latest = store.begin_read(None).unwrap();
    let users_latest = rtx_latest.open_table::<String>("users").unwrap();
    assert_eq!(users_latest.len(), 2);
    assert_eq!(users_latest.get(2), Some(&"bob".to_string()));
}

// ---------------------------------------------------------------------------
// Rollback
// ---------------------------------------------------------------------------

#[test]
fn rollback_leaves_store_unchanged() {
    let mut store = Store::new();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("keep".to_string());
        wtx.commit(&mut store).unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("discard".to_string());
        wtx.rollback();
    }

    assert_eq!(store.latest_version(), 1);
    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<String>("t").unwrap();
    assert_eq!(table.len(), 1);
    assert_eq!(table.get(2), None);
}

// ---------------------------------------------------------------------------
// Multiple read transactions at different versions
// ---------------------------------------------------------------------------

#[test]
fn multiple_read_tx_coexist_at_different_versions() {
    let mut store = Store::new();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<u32>("scores").unwrap().insert(10u32);
        wtx.commit(&mut store).unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<u32>("scores").unwrap().insert(20u32);
        wtx.commit(&mut store).unwrap();
    }

    let r1 = store.begin_read(Some(1)).unwrap();
    let r2 = store.begin_read(Some(2)).unwrap();

    assert_eq!(r1.open_table::<u32>("scores").unwrap().len(), 1);
    assert_eq!(r2.open_table::<u32>("scores").unwrap().len(), 2);
}

// ---------------------------------------------------------------------------
// Two independent tables across versions
// ---------------------------------------------------------------------------

#[test]
fn two_tables_independent_across_versions() {
    let mut store = Store::new();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string());
        wtx.commit(&mut store).unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<u64>("posts").unwrap().insert(42u64);
        wtx.commit(&mut store).unwrap();
    }

    let r1 = store.begin_read(Some(1)).unwrap();
    assert_eq!(r1.open_table::<String>("users").unwrap().len(), 1);
    assert!(matches!(r1.open_table::<u64>("posts"), Err(Error::KeyNotFound)));

    let r2 = store.begin_read(Some(2)).unwrap();
    assert_eq!(r2.open_table::<String>("users").unwrap().len(), 1);
    assert_eq!(r2.open_table::<u64>("posts").unwrap().get(1), Some(&42u64));
}

// ---------------------------------------------------------------------------
// Write conflict on stale explicit version
// ---------------------------------------------------------------------------

#[test]
fn write_conflict_on_stale_explicit_version() {
    let mut store = Store::new();
    {
        let wtx = store.begin_write(Some(3)).unwrap();
        wtx.commit(&mut store).unwrap();
    }
    assert!(matches!(store.begin_write(Some(2)), Err(Error::WriteConflict)));
    assert!(matches!(store.begin_write(Some(3)), Err(Error::WriteConflict)));
}

// ---------------------------------------------------------------------------
// Auto-increment IDs do not repeat across versions
// ---------------------------------------------------------------------------

#[test]
fn auto_increment_ids_continue_from_base() {
    let mut store = Store::new();

    {
        let mut wtx = store.begin_write(None).unwrap();
        let t = wtx.open_table::<String>("t").unwrap();
        t.insert("a".to_string()); // id 1
        t.insert("b".to_string()); // id 2
        t.insert("c".to_string()); // id 3
        wtx.commit(&mut store).unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        let t = wtx.open_table::<String>("t").unwrap();
        let id = t.insert("d".to_string());
        assert_eq!(id, 4);
        wtx.commit(&mut store).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Multiple tables in one atomic write transaction
// ---------------------------------------------------------------------------

#[test]
fn multi_table_write_is_atomic() {
    let mut store = Store::new();

    // Commit "users" and "posts" in a single write transaction.
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string());
        wtx.open_table::<u64>("posts").unwrap().insert(42u64);
        wtx.commit(&mut store).unwrap();
    }

    // A single read at the committed version sees BOTH tables.
    let rtx = store.begin_read(None).unwrap();
    assert_eq!(rtx.open_table::<String>("users").unwrap().get(1), Some(&"alice".to_string()));
    assert_eq!(rtx.open_table::<u64>("posts").unwrap().get(1), Some(&42u64));

    // There is no intermediate version where only one table exists.
    assert_eq!(store.latest_version(), 1);
}

// ---------------------------------------------------------------------------
// Untouched tables survive a commit
// ---------------------------------------------------------------------------

#[test]
fn untouched_tables_survive_commit() {
    let mut store = Store::new();

    // v1: create "users"
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string());
        wtx.commit(&mut store).unwrap();
    }

    // v2: write only to "logs" — do NOT open "users"
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("logs").unwrap().insert("event".to_string());
        wtx.commit(&mut store).unwrap();
    }

    // At v2, "users" must still be present and unchanged.
    let rtx = store.begin_read(None).unwrap();
    assert_eq!(rtx.open_table::<String>("users").unwrap().get(1), Some(&"alice".to_string()));
    assert_eq!(rtx.open_table::<String>("logs").unwrap().get(1), Some(&"event".to_string()));
}

// ---------------------------------------------------------------------------
// Two overlapping write transactions
// ---------------------------------------------------------------------------

#[test]
fn two_overlapping_write_txs_each_see_their_own_base() {
    let mut store = Store::new();

    // Seed v1
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<u32>("counter").unwrap().insert(0u32);
        wtx.commit(&mut store).unwrap();
    }

    // Open both write transactions before either commits.
    let mut wtx_a = store.begin_write(None).unwrap(); // will be v2
    let mut wtx_b = store.begin_write(Some(3)).unwrap(); // will be v3

    // Each writer sees only the v1 base — not the other writer's changes.
    wtx_a.open_table::<u32>("counter").unwrap().insert(100u32);
    wtx_b.open_table::<u32>("counter").unwrap().insert(200u32);

    let v_a = wtx_a.commit(&mut store).unwrap();
    let v_b = wtx_b.commit(&mut store).unwrap();

    assert_eq!(v_a, 2);
    assert_eq!(v_b, 3);

    // v2 has id=2 from wtx_a (started from v1 which had id=1 with value 0)
    let r2 = store.begin_read(Some(2)).unwrap();
    let c2 = r2.open_table::<u32>("counter").unwrap();
    assert_eq!(c2.get(1), Some(&0u32)); // from base v1
    assert_eq!(c2.get(2), Some(&100u32)); // wtx_a's insert

    // v3 has id=2 from wtx_b — it also started from v1, so it did NOT see wtx_a's id=2
    let r3 = store.begin_read(Some(3)).unwrap();
    let c3 = r3.open_table::<u32>("counter").unwrap();
    assert_eq!(c3.get(1), Some(&0u32)); // from base v1
    assert_eq!(c3.get(2), Some(&200u32)); // wtx_b's insert (same id, different value)
    assert_eq!(c3.len(), 2); // wtx_b did NOT see wtx_a's commit
}

// ---------------------------------------------------------------------------
// Empty commit (no tables opened)
// ---------------------------------------------------------------------------

#[test]
fn empty_commit_produces_new_version_with_same_data() {
    let mut store = Store::new();

    // Seed with one table
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("hello".to_string());
        wtx.commit(&mut store).unwrap();
    }

    // Empty commit: open a write tx but touch nothing
    {
        let wtx = store.begin_write(None).unwrap();
        wtx.commit(&mut store).unwrap();
    }

    assert_eq!(store.latest_version(), 2);

    // The table from v1 is still accessible at v2
    let rtx = store.begin_read(None).unwrap();
    assert_eq!(rtx.open_table::<String>("t").unwrap().get(1), Some(&"hello".to_string()));
}

// ---------------------------------------------------------------------------
// Read at version 0 (empty store)
// ---------------------------------------------------------------------------

#[test]
fn read_at_version_zero_sees_empty_store() {
    let mut store = Store::new();

    // Commit something so the store advances past v0
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("data".to_string());
        wtx.commit(&mut store).unwrap();
    }

    // A read pinned to v0 sees no tables at all
    let rtx = store.begin_read(Some(0)).unwrap();
    assert!(matches!(rtx.open_table::<String>("t"), Err(Error::KeyNotFound)));
}

// ---------------------------------------------------------------------------
// Read snapshot survives many subsequent writes
// ---------------------------------------------------------------------------

#[test]
fn old_read_snapshot_survives_many_writes() {
    let mut store = Store::new();

    // Commit v1 with a known record
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("original".to_string());
        wtx.commit(&mut store).unwrap();
    }

    // Pin a reader at v1
    let rtx = store.begin_read(Some(1)).unwrap();

    // Perform many subsequent writes that overwrite the same key
    for i in 2u64..=10 {
        let mut wtx = store.begin_write(None).unwrap();
        let t = wtx.open_table::<String>("t").unwrap();
        t.update(1, format!("version {i}")).unwrap();
        wtx.commit(&mut store).unwrap();
    }

    assert_eq!(store.latest_version(), 10);

    // The v1 reader still sees the original data
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.get(1), Some(&"original".to_string()));
    assert_eq!(t.len(), 1);

    // Latest sees the final overwrite
    let latest = store.begin_read(None).unwrap();
    assert_eq!(latest.open_table::<String>("t").unwrap().get(1), Some(&"version 10".to_string()));
}
