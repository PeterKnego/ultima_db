use ultima_db::{Error, IndexKind, Store};

// ---------------------------------------------------------------------------
// Full CRUD via transactions
// ---------------------------------------------------------------------------

#[test]
fn end_to_end_write_commit_read() {
    let mut store = Store::new();

    let v = {
        let mut wtx = store.begin_write(None).unwrap();
        let table = wtx.open_table::<String>("notes").unwrap();

        let id1 = table.insert("first note".to_string()).unwrap();
        let id2 = table.insert("second note".to_string()).unwrap();
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

        let id3 = table.insert("third note".to_string()).unwrap();
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
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string()).unwrap();
        wtx.commit(&mut store).unwrap();
    }

    let rtx_v1 = store.begin_read(Some(1)).unwrap();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("bob".to_string()).unwrap();
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
        wtx.open_table::<String>("t").unwrap().insert("keep".to_string()).unwrap();
        wtx.commit(&mut store).unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("discard".to_string()).unwrap();
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
        wtx.open_table::<u32>("scores").unwrap().insert(10u32).unwrap();
        wtx.commit(&mut store).unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<u32>("scores").unwrap().insert(20u32).unwrap();
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
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string()).unwrap();
        wtx.commit(&mut store).unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<u64>("posts").unwrap().insert(42u64).unwrap();
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
        t.insert("a".to_string()).unwrap(); // id 1
        t.insert("b".to_string()).unwrap(); // id 2
        t.insert("c".to_string()).unwrap(); // id 3
        wtx.commit(&mut store).unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        let t = wtx.open_table::<String>("t").unwrap();
        let id = t.insert("d".to_string()).unwrap();
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
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string()).unwrap();
        wtx.open_table::<u64>("posts").unwrap().insert(42u64).unwrap();
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
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string()).unwrap();
        wtx.commit(&mut store).unwrap();
    }

    // v2: write only to "logs" — do NOT open "users"
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("logs").unwrap().insert("event".to_string()).unwrap();
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
        wtx.open_table::<u32>("counter").unwrap().insert(0u32).unwrap();
        wtx.commit(&mut store).unwrap();
    }

    // Open both write transactions before either commits.
    let mut wtx_a = store.begin_write(None).unwrap(); // will be v2
    let mut wtx_b = store.begin_write(Some(3)).unwrap(); // will be v3

    // Each writer sees only the v1 base — not the other writer's changes.
    wtx_a.open_table::<u32>("counter").unwrap().insert(100u32).unwrap();
    wtx_b.open_table::<u32>("counter").unwrap().insert(200u32).unwrap();

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
        wtx.open_table::<String>("t").unwrap().insert("hello".to_string()).unwrap();
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
        wtx.open_table::<String>("t").unwrap().insert("data".to_string()).unwrap();
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
        wtx.open_table::<String>("t").unwrap().insert("original".to_string()).unwrap();
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

// ===========================================================================
// Index integration tests
// ===========================================================================

#[derive(Debug, Clone, PartialEq)]
struct User {
    email: String,
    age: u32,
    name: String,
}

// ---------------------------------------------------------------------------
// Basic unique index CRUD
// ---------------------------------------------------------------------------

#[test]
fn unique_index_insert_and_lookup() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
        .unwrap();

    let id = table
        .insert(User {
            email: "alice@example.com".into(),
            age: 30,
            name: "Alice".into(),
        })
        .unwrap();

    let result = table
        .get_unique::<String>("by_email", &"alice@example.com".to_string())
        .unwrap();
    assert_eq!(result.unwrap().0, id);
    assert_eq!(result.unwrap().1.name, "Alice");

    // Absent key returns None
    let absent = table
        .get_unique::<String>("by_email", &"nobody@example.com".to_string())
        .unwrap();
    assert!(absent.is_none());

    wtx.commit(&mut store).unwrap();
}

// ---------------------------------------------------------------------------
// Unique index rejects duplicates
// ---------------------------------------------------------------------------

#[test]
fn unique_index_rejects_duplicate() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
        .unwrap();

    table
        .insert(User {
            email: "alice@example.com".into(),
            age: 30,
            name: "Alice".into(),
        })
        .unwrap();

    let result = table.insert(User {
        email: "alice@example.com".into(),
        age: 25,
        name: "Evil Alice".into(),
    });
    assert!(matches!(result, Err(Error::DuplicateKey(_))));

    // Table should still have only 1 record
    assert_eq!(table.len(), 1);

    wtx.rollback();
}

// ---------------------------------------------------------------------------
// Non-unique index: multiple records per key
// ---------------------------------------------------------------------------

#[test]
fn non_unique_index_groups_by_key() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
        .unwrap();

    table
        .insert(User { email: "a@x.com".into(), age: 30, name: "Alice".into() })
        .unwrap();
    table
        .insert(User { email: "b@x.com".into(), age: 30, name: "Bob".into() })
        .unwrap();
    table
        .insert(User { email: "c@x.com".into(), age: 25, name: "Charlie".into() })
        .unwrap();

    let age_30 = table.get_by_index::<u32>("by_age", &30).unwrap();
    assert_eq!(age_30.len(), 2);
    let names: Vec<&str> = age_30.iter().map(|(_, u)| u.name.as_str()).collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));

    let age_25 = table.get_by_index::<u32>("by_age", &25).unwrap();
    assert_eq!(age_25.len(), 1);
    assert_eq!(age_25[0].1.name, "Charlie");

    let age_99 = table.get_by_index::<u32>("by_age", &99).unwrap();
    assert!(age_99.is_empty());

    wtx.commit(&mut store).unwrap();
}

// ---------------------------------------------------------------------------
// Index maintained on update
// ---------------------------------------------------------------------------

#[test]
fn index_updated_on_record_update() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
        .unwrap();

    let id = table
        .insert(User { email: "old@x.com".into(), age: 30, name: "Alice".into() })
        .unwrap();

    table
        .update(id, User { email: "new@x.com".into(), age: 30, name: "Alice".into() })
        .unwrap();

    // Old key gone
    assert!(table
        .get_unique::<String>("by_email", &"old@x.com".to_string())
        .unwrap()
        .is_none());
    // New key present
    assert_eq!(
        table
            .get_unique::<String>("by_email", &"new@x.com".to_string())
            .unwrap()
            .unwrap()
            .0,
        id
    );

    wtx.commit(&mut store).unwrap();
}

// ---------------------------------------------------------------------------
// Index maintained on delete
// ---------------------------------------------------------------------------

#[test]
fn index_cleaned_on_delete() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
        .unwrap();

    let id = table
        .insert(User { email: "alice@x.com".into(), age: 30, name: "Alice".into() })
        .unwrap();

    table.delete(id).unwrap();

    assert!(table
        .get_unique::<String>("by_email", &"alice@x.com".to_string())
        .unwrap()
        .is_none());

    wtx.commit(&mut store).unwrap();
}

// ---------------------------------------------------------------------------
// Index survives MVCC snapshots
// ---------------------------------------------------------------------------

#[test]
fn index_works_across_snapshots() {
    let mut store = Store::new();

    // v1: insert alice with index
    {
        let mut wtx = store.begin_write(None).unwrap();
        let table = wtx.open_table::<User>("users").unwrap();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .insert(User { email: "alice@x.com".into(), age: 30, name: "Alice".into() })
            .unwrap();
        wtx.commit(&mut store).unwrap();
    }

    // Pin reader at v1
    let rtx_v1 = store.begin_read(Some(1)).unwrap();

    // v2: insert bob
    {
        let mut wtx = store.begin_write(None).unwrap();
        let table = wtx.open_table::<User>("users").unwrap();
        // Index definition is preserved from v1 (via Table clone)
        table
            .insert(User { email: "bob@x.com".into(), age: 25, name: "Bob".into() })
            .unwrap();
        wtx.commit(&mut store).unwrap();
    }

    // v1 reader: only sees alice
    let t_v1 = rtx_v1.open_table::<User>("users").unwrap();
    assert!(t_v1
        .get_unique::<String>("by_email", &"alice@x.com".to_string())
        .unwrap()
        .is_some());
    assert!(t_v1
        .get_unique::<String>("by_email", &"bob@x.com".to_string())
        .unwrap()
        .is_none());

    // Latest reader: sees both
    let rtx_latest = store.begin_read(None).unwrap();
    let t_latest = rtx_latest.open_table::<User>("users").unwrap();
    assert!(t_latest
        .get_unique::<String>("by_email", &"bob@x.com".to_string())
        .unwrap()
        .is_some());
    assert_eq!(t_latest.len(), 2);
}

// ---------------------------------------------------------------------------
// Multiple indexes on same table
// ---------------------------------------------------------------------------

#[test]
fn multiple_indexes_on_same_table() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
        .unwrap();
    table
        .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
        .unwrap();

    table
        .insert(User { email: "alice@x.com".into(), age: 30, name: "Alice".into() })
        .unwrap();
    table
        .insert(User { email: "bob@x.com".into(), age: 30, name: "Bob".into() })
        .unwrap();

    // Unique lookup by email
    let alice = table
        .get_unique::<String>("by_email", &"alice@x.com".to_string())
        .unwrap()
        .unwrap();
    assert_eq!(alice.1.name, "Alice");

    // Non-unique lookup by age
    let age_30 = table.get_by_index::<u32>("by_age", &30).unwrap();
    assert_eq!(age_30.len(), 2);

    wtx.commit(&mut store).unwrap();
}

// ---------------------------------------------------------------------------
// Backfill: define_index on non-empty table
// ---------------------------------------------------------------------------

#[test]
fn define_index_backfills_existing_data() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    // Insert data BEFORE defining index
    table
        .insert(User { email: "alice@x.com".into(), age: 30, name: "Alice".into() })
        .unwrap();
    table
        .insert(User { email: "bob@x.com".into(), age: 25, name: "Bob".into() })
        .unwrap();

    // Define index — should backfill
    table
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
        .unwrap();

    // Lookup works on pre-existing data
    let alice = table
        .get_unique::<String>("by_email", &"alice@x.com".to_string())
        .unwrap()
        .unwrap();
    assert_eq!(alice.1.name, "Alice");

    let bob = table
        .get_unique::<String>("by_email", &"bob@x.com".to_string())
        .unwrap()
        .unwrap();
    assert_eq!(bob.1.name, "Bob");

    wtx.commit(&mut store).unwrap();
}

// ---------------------------------------------------------------------------
// Index range scan
// ---------------------------------------------------------------------------

#[test]
fn index_range_scan() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
        .unwrap();

    table
        .insert(User { email: "a@x.com".into(), age: 20, name: "A".into() })
        .unwrap();
    table
        .insert(User { email: "b@x.com".into(), age: 25, name: "B".into() })
        .unwrap();
    table
        .insert(User { email: "c@x.com".into(), age: 30, name: "C".into() })
        .unwrap();
    table
        .insert(User { email: "d@x.com".into(), age: 35, name: "D".into() })
        .unwrap();

    // Range: ages 25..=30
    let results = table.index_range::<u32>("by_age", 25..=30).unwrap();
    assert_eq!(results.len(), 2);
    let names: Vec<&str> = results.iter().map(|(_, u)| u.name.as_str()).collect();
    assert!(names.contains(&"B"));
    assert!(names.contains(&"C"));

    wtx.commit(&mut store).unwrap();
}

// ---------------------------------------------------------------------------
// IndexNotFound error
// ---------------------------------------------------------------------------

#[test]
fn lookup_on_undefined_index_returns_error() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    let result = table.get_unique::<String>("nonexistent", &"x".to_string());
    assert!(matches!(result, Err(Error::IndexNotFound(_))));

    wtx.rollback();
}

// ---------------------------------------------------------------------------
// IndexTypeMismatch error
// ---------------------------------------------------------------------------

#[test]
fn lookup_with_wrong_key_type_returns_error() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
        .unwrap();

    // Try to look up with wrong key type (u32 instead of String)
    let result = table.get_unique::<u32>("by_email", &42u32);
    assert!(matches!(result, Err(Error::IndexTypeMismatch(_))));

    wtx.rollback();
}

// ---------------------------------------------------------------------------
// Multi-index: insert rollback when second unique index fails
// ---------------------------------------------------------------------------

#[test]
fn multi_index_insert_rollback_on_second_index_failure() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
        .unwrap();
    table
        .define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone())
        .unwrap();

    // Insert first user
    table
        .insert(User { email: "alice@x.com".into(), age: 30, name: "Alice".into() })
        .unwrap();

    // Insert user with unique email but duplicate name — should fail
    let result = table.insert(User {
        email: "different@x.com".into(),
        age: 25,
        name: "Alice".into(), // duplicate name
    });
    assert!(matches!(result, Err(Error::DuplicateKey(_))));

    // Table should still have exactly 1 record
    assert_eq!(table.len(), 1);

    // The email index should NOT have "different@x.com" (rollback must have cleaned it)
    assert!(table
        .get_unique::<String>("by_email", &"different@x.com".to_string())
        .unwrap()
        .is_none());

    // The original alice should still be intact in both indexes
    assert!(table
        .get_unique::<String>("by_email", &"alice@x.com".to_string())
        .unwrap()
        .is_some());
    assert!(table
        .get_unique::<String>("by_name", &"Alice".to_string())
        .unwrap()
        .is_some());

    wtx.rollback();
}

// ---------------------------------------------------------------------------
// Multi-index: update rollback when second index fails
// ---------------------------------------------------------------------------

#[test]
fn multi_index_update_rollback_on_second_index_failure() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
        .unwrap();
    table
        .define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone())
        .unwrap();

    let id1 = table
        .insert(User { email: "alice@x.com".into(), age: 30, name: "Alice".into() })
        .unwrap();
    let _id2 = table
        .insert(User { email: "bob@x.com".into(), age: 25, name: "Bob".into() })
        .unwrap();

    // Try updating alice's name to "Bob" — conflicts with by_name index
    let result = table.update(
        id1,
        User { email: "alice@x.com".into(), age: 30, name: "Bob".into() },
    );
    assert!(matches!(result, Err(Error::DuplicateKey(_))));

    // Alice should still be findable by original email and name
    let alice = table
        .get_unique::<String>("by_email", &"alice@x.com".to_string())
        .unwrap()
        .unwrap();
    assert_eq!(alice.1.name, "Alice");

    let alice_by_name = table
        .get_unique::<String>("by_name", &"Alice".to_string())
        .unwrap()
        .unwrap();
    assert_eq!(alice_by_name.0, id1);

    wtx.rollback();
}

// ---------------------------------------------------------------------------
// Multi-index: delete removes from all indexes
// ---------------------------------------------------------------------------

#[test]
fn multi_index_delete_cleans_all_indexes() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
        .unwrap();
    table
        .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
        .unwrap();

    let id = table
        .insert(User { email: "alice@x.com".into(), age: 30, name: "Alice".into() })
        .unwrap();
    table
        .insert(User { email: "bob@x.com".into(), age: 30, name: "Bob".into() })
        .unwrap();

    table.delete(id).unwrap();

    // Email index: alice gone
    assert!(table
        .get_unique::<String>("by_email", &"alice@x.com".to_string())
        .unwrap()
        .is_none());

    // Age index: only bob remains for age 30
    let age_30 = table.get_by_index::<u32>("by_age", &30).unwrap();
    assert_eq!(age_30.len(), 1);
    assert_eq!(age_30[0].1.name, "Bob");

    wtx.commit(&mut store).unwrap();
}

// ---------------------------------------------------------------------------
// Multi-index: MVCC isolation — clone has independent indexes
// ---------------------------------------------------------------------------

#[test]
fn multi_index_mvcc_clone_isolation() {
    let mut store = Store::new();

    // v1: insert alice with two indexes
    {
        let mut wtx = store.begin_write(None).unwrap();
        let table = wtx.open_table::<User>("users").unwrap();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
            .unwrap();
        table
            .insert(User { email: "alice@x.com".into(), age: 30, name: "Alice".into() })
            .unwrap();
        wtx.commit(&mut store).unwrap();
    }

    // Pin reader at v1
    let rtx_v1 = store.begin_read(Some(1)).unwrap();

    // v2: insert bob and update alice's age
    {
        let mut wtx = store.begin_write(None).unwrap();
        let table = wtx.open_table::<User>("users").unwrap();
        table
            .insert(User { email: "bob@x.com".into(), age: 30, name: "Bob".into() })
            .unwrap();
        table
            .update(1, User { email: "alice@x.com".into(), age: 31, name: "Alice".into() })
            .unwrap();
        wtx.commit(&mut store).unwrap();
    }

    // v1 snapshot: alice at age 30, no bob
    let t_v1 = rtx_v1.open_table::<User>("users").unwrap();
    let age_30_v1 = t_v1.get_by_index::<u32>("by_age", &30).unwrap();
    assert_eq!(age_30_v1.len(), 1);
    assert_eq!(age_30_v1[0].1.name, "Alice");
    assert!(t_v1
        .get_unique::<String>("by_email", &"bob@x.com".to_string())
        .unwrap()
        .is_none());

    // v2 snapshot: alice at age 31, bob at age 30
    let rtx_v2 = store.begin_read(None).unwrap();
    let t_v2 = rtx_v2.open_table::<User>("users").unwrap();
    let age_30_v2 = t_v2.get_by_index::<u32>("by_age", &30).unwrap();
    assert_eq!(age_30_v2.len(), 1);
    assert_eq!(age_30_v2[0].1.name, "Bob");
    let age_31_v2 = t_v2.get_by_index::<u32>("by_age", &31).unwrap();
    assert_eq!(age_31_v2.len(), 1);
    assert_eq!(age_31_v2[0].1.name, "Alice");
}

// ---------------------------------------------------------------------------
// Multi-index: update with mixed unique + non-unique
// ---------------------------------------------------------------------------

#[test]
fn multi_index_update_mixed_unique_and_non_unique() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    table
        .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
        .unwrap();
    table
        .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
        .unwrap();

    let id = table
        .insert(User { email: "alice@x.com".into(), age: 30, name: "Alice".into() })
        .unwrap();
    table
        .insert(User { email: "bob@x.com".into(), age: 30, name: "Bob".into() })
        .unwrap();

    // Update alice: change email and age
    table
        .update(
            id,
            User { email: "alice_new@x.com".into(), age: 25, name: "Alice".into() },
        )
        .unwrap();

    // Unique index updated
    assert!(table
        .get_unique::<String>("by_email", &"alice@x.com".to_string())
        .unwrap()
        .is_none());
    assert!(table
        .get_unique::<String>("by_email", &"alice_new@x.com".to_string())
        .unwrap()
        .is_some());

    // Non-unique index updated: age 30 now only has bob
    let age_30 = table.get_by_index::<u32>("by_age", &30).unwrap();
    assert_eq!(age_30.len(), 1);
    assert_eq!(age_30[0].1.name, "Bob");

    // age 25 has alice
    let age_25 = table.get_by_index::<u32>("by_age", &25).unwrap();
    assert_eq!(age_25.len(), 1);
    assert_eq!(age_25[0].1.name, "Alice");

    wtx.commit(&mut store).unwrap();
}

// ---------------------------------------------------------------------------
// Compound index (tuple keys)
// ---------------------------------------------------------------------------

#[test]
fn compound_index_on_multiple_fields() {
    let mut store = Store::new();
    let mut wtx = store.begin_write(None).unwrap();
    let table = wtx.open_table::<User>("users").unwrap();

    // Index on (age, name)
    table
        .define_index("by_age_name", IndexKind::Unique, |u: &User| (u.age, u.name.clone()))
        .unwrap();

    table
        .insert(User { email: "a@x.com".into(), age: 30, name: "Alice".into() })
        .unwrap();
    table
        .insert(User { email: "b@x.com".into(), age: 30, name: "Bob".into() })
        .unwrap();
    table
        .insert(User { email: "c@x.com".into(), age: 25, name: "Alice".into() })
        .unwrap();

    // Lookup by (30, "Alice")
    let alice_30 = table
        .get_unique::<(u32, String)>("by_age_name", &(30, "Alice".to_string()))
        .unwrap()
        .unwrap();
    assert_eq!(alice_30.1.email, "a@x.com");

    // Lookup by (25, "Alice")
    let alice_25 = table
        .get_unique::<(u32, String)>("by_age_name", &(25, "Alice".to_string()))
        .unwrap()
        .unwrap();
    assert_eq!(alice_25.1.email, "c@x.com");

    // Reject duplicate (30, "Alice")
    let dup = table.insert(User { email: "d@x.com".into(), age: 30, name: "Alice".into() });
    assert!(matches!(dup, Err(Error::DuplicateKey(_))));

    // Range scan on compound index: all users aged 30
    let age_30 = table
        .index_range::<(u32, String)>("by_age_name", (30, "".into())..=(30, "\u{ffff}".into()))
        .unwrap();
    assert_eq!(age_30.len(), 2);
    let names: Vec<String> = age_30.iter().map(|(_, u)| u.name.clone()).collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));

    wtx.commit(&mut store).unwrap();
}

