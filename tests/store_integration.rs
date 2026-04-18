use ultima_db::{Error, IndexKind, Store, TableDef};

// ---------------------------------------------------------------------------
// Full CRUD via transactions
// ---------------------------------------------------------------------------

#[test]
fn end_to_end_write_commit_read() {
    let store = Store::default();

    let v = {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<String>("notes").unwrap();

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

        wtx.commit().unwrap()
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
    let store = Store::default();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    let rtx_v1 = store.begin_read(Some(1)).unwrap();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("bob".to_string()).unwrap();
        wtx.commit().unwrap();
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
    let store = Store::default();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("keep".to_string()).unwrap();
        wtx.commit().unwrap();
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
    let store = Store::default();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<u32>("scores").unwrap().insert(10u32).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<u32>("scores").unwrap().insert(20u32).unwrap();
        wtx.commit().unwrap();
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
    let store = Store::default();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string()).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<u64>("posts").unwrap().insert(42u64).unwrap();
        wtx.commit().unwrap();
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
    let store = Store::default();
    {
        let wtx = store.begin_write(Some(3)).unwrap();
        wtx.commit().unwrap();
    }
    assert!(matches!(store.begin_write(Some(2)), Err(Error::WriteConflict { .. })));
    assert!(matches!(store.begin_write(Some(3)), Err(Error::WriteConflict { .. })));
}

// ---------------------------------------------------------------------------
// Auto-increment IDs do not repeat across versions
// ---------------------------------------------------------------------------

#[test]
fn auto_increment_ids_continue_from_base() {
    let store = Store::default();

    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.insert("a".to_string()).unwrap(); // id 1
        t.insert("b".to_string()).unwrap(); // id 2
        t.insert("c".to_string()).unwrap(); // id 3
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        let id = t.insert("d".to_string()).unwrap();
        assert_eq!(id, 4);
        wtx.commit().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Multiple tables in one atomic write transaction
// ---------------------------------------------------------------------------

#[test]
fn multi_table_write_is_atomic() {
    let store = Store::default();

    // Commit "users" and "posts" in a single write transaction.
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string()).unwrap();
        wtx.open_table::<u64>("posts").unwrap().insert(42u64).unwrap();
        wtx.commit().unwrap();
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
    let store = Store::default();

    // v1: create "users"
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    // v2: write only to "logs" — do NOT open "users"
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("logs").unwrap().insert("event".to_string()).unwrap();
        wtx.commit().unwrap();
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
fn two_overlapping_write_txs_to_different_tables() {
    use ultima_db::{StoreConfig, WriterMode};
    let store = Store::new(StoreConfig {
        writer_mode: WriterMode::MultiWriter,
        ..StoreConfig::default()
    }).unwrap();

    // Open both write transactions before either commits — different tables.
    let mut wtx_a = store.begin_write(None).unwrap(); // will be v1
    let mut wtx_b = store.begin_write(Some(2)).unwrap(); // will be v2

    wtx_a.open_table::<u32>("table_a").unwrap().insert(100u32).unwrap();
    wtx_b.open_table::<u32>("table_b").unwrap().insert(200u32).unwrap();

    let v_a = wtx_a.commit().unwrap();
    let v_b = wtx_b.commit().unwrap();

    assert_eq!(v_a, 1);
    assert_eq!(v_b, 2);

    // v1 has table_a only — wtx_a committed first, before wtx_b.
    let r1 = store.begin_read(Some(1)).unwrap();
    assert_eq!(r1.open_table::<u32>("table_a").unwrap().get(1), Some(&100u32));
    assert!(matches!(r1.open_table::<u32>("table_b"), Err(Error::KeyNotFound)));

    // v2 has BOTH tables. wtx_b rebased onto the current latest snapshot at
    // commit time — v1 (which contains wtx_a's table_a). OCC confirmed no
    // concurrent commit touched table_b, so wtx_b's table_b is safely merged
    // on top.
    let r2 = store.begin_read(Some(2)).unwrap();
    assert_eq!(r2.open_table::<u32>("table_a").unwrap().get(1), Some(&100u32));
    assert_eq!(r2.open_table::<u32>("table_b").unwrap().get(1), Some(&200u32));
}

// ---------------------------------------------------------------------------
// Empty commit (no tables opened)
// ---------------------------------------------------------------------------

#[test]
fn empty_commit_produces_new_version_with_same_data() {
    let store = Store::default();

    // Seed with one table
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("hello".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    // Empty commit: open a write tx but touch nothing
    {
        let wtx = store.begin_write(None).unwrap();
        wtx.commit().unwrap();
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
    let store = Store::default();

    // Commit something so the store advances past v0
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("data".to_string()).unwrap();
        wtx.commit().unwrap();
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
    let store = Store::default();

    // Commit v1 with a known record
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("original".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    // Pin a reader at v1
    let rtx = store.begin_read(Some(1)).unwrap();

    // Perform many subsequent writes that overwrite the same key
    for i in 2u64..=10 {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.update(1, format!("version {i}")).unwrap();
        wtx.commit().unwrap();
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
#[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
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
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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

    wtx.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Unique index rejects duplicates
// ---------------------------------------------------------------------------

#[test]
fn unique_index_rejects_duplicate() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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

    wtx.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Index maintained on update
// ---------------------------------------------------------------------------

#[test]
fn index_updated_on_record_update() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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

    wtx.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Index maintained on delete
// ---------------------------------------------------------------------------

#[test]
fn index_cleaned_on_delete() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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

    wtx.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Index survives MVCC snapshots
// ---------------------------------------------------------------------------

#[test]
fn index_works_across_snapshots() {
    let store = Store::default();

    // v1: insert alice with index
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .insert(User { email: "alice@x.com".into(), age: 30, name: "Alice".into() })
            .unwrap();
        wtx.commit().unwrap();
    }

    // Pin reader at v1
    let rtx_v1 = store.begin_read(Some(1)).unwrap();

    // v2: insert bob
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        // Index definition is preserved from v1 (via Table clone)
        table
            .insert(User { email: "bob@x.com".into(), age: 25, name: "Bob".into() })
            .unwrap();
        wtx.commit().unwrap();
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
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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

    wtx.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Backfill: define_index on non-empty table
// ---------------------------------------------------------------------------

#[test]
fn define_index_backfills_existing_data() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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

    wtx.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Index range scan
// ---------------------------------------------------------------------------

#[test]
fn index_range_scan() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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

    wtx.commit().unwrap();
}

// ---------------------------------------------------------------------------
// IndexNotFound error
// ---------------------------------------------------------------------------

#[test]
fn lookup_on_undefined_index_returns_error() {
    let store = Store::default();
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
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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

    wtx.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Multi-index: MVCC isolation — clone has independent indexes
// ---------------------------------------------------------------------------

#[test]
fn multi_index_mvcc_clone_isolation() {
    let store = Store::default();

    // v1: insert alice with two indexes
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
            .unwrap();
        table
            .insert(User { email: "alice@x.com".into(), age: 30, name: "Alice".into() })
            .unwrap();
        wtx.commit().unwrap();
    }

    // Pin reader at v1
    let rtx_v1 = store.begin_read(Some(1)).unwrap();

    // v2: insert bob and update alice's age
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        table
            .insert(User { email: "bob@x.com".into(), age: 30, name: "Bob".into() })
            .unwrap();
        table
            .update(1, User { email: "alice@x.com".into(), age: 31, name: "Alice".into() })
            .unwrap();
        wtx.commit().unwrap();
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
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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

    wtx.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Compound index (tuple keys)
// ---------------------------------------------------------------------------

#[test]
fn compound_index_on_multiple_fields() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<User>("users").unwrap();

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

    wtx.commit().unwrap();
}

// ===========================================================================
// Batch operation integration tests
// ===========================================================================

#[test]
fn batch_insert_visible_after_commit() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        let ids = table
            .insert_batch(vec![
                User { email: "a@x.com".into(), age: 30, name: "A".into() },
                User { email: "b@x.com".into(), age: 25, name: "B".into() },
                User { email: "c@x.com".into(), age: 30, name: "C".into() },
            ])
            .unwrap();
        assert_eq!(ids.len(), 3);
        wtx.commit().unwrap();
    }
    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 3);
    assert!(table.get_unique::<String>("by_email", &"b@x.com".to_string()).unwrap().is_some());
}

#[test]
fn batch_update_visible_after_commit() {
    let store = Store::default();
    let id1;
    let id2;
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        id1 = table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        id2 = table.insert(User { email: "b@x.com".into(), age: 25, name: "B".into() }).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        table
            .update_batch(vec![
                (id1, User { email: "a_new@x.com".into(), age: 31, name: "A".into() }),
                (id2, User { email: "b_new@x.com".into(), age: 26, name: "B".into() }),
            ])
            .unwrap();
        wtx.commit().unwrap();
    }
    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.get(id1).unwrap().email, "a_new@x.com");
    assert!(table.get_unique::<String>("by_email", &"a@x.com".to_string()).unwrap().is_none());
    assert!(table.get_unique::<String>("by_email", &"a_new@x.com".to_string()).unwrap().is_some());
}

#[test]
fn batch_delete_visible_after_commit() {
    let store = Store::default();
    let id1;
    let id2;
    let id3;
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        id1 = table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        id2 = table.insert(User { email: "b@x.com".into(), age: 25, name: "B".into() }).unwrap();
        id3 = table.insert(User { email: "c@x.com".into(), age: 20, name: "C".into() }).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        table.delete_batch(&[id1, id3]).unwrap();
        wtx.commit().unwrap();
    }
    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 1);
    assert_eq!(table.get(id2).unwrap().email, "b@x.com");
    assert!(table.get_unique::<String>("by_email", &"a@x.com".to_string()).unwrap().is_none());
}

#[test]
fn batch_insert_rollback_on_constraint() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table.insert(User { email: "taken@x.com".into(), age: 30, name: "A".into() }).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        let res = table.insert_batch(vec![
            User { email: "new@x.com".into(), age: 25, name: "B".into() },
            User { email: "taken@x.com".into(), age: 20, name: "C".into() },
        ]);
        assert!(matches!(res, Err(Error::DuplicateKey(_))));
        // Table in this write tx should be unchanged
        assert_eq!(table.len(), 1);
    }
    // Store should still be at the original version
    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 1);
}

#[test]
fn batch_ops_mvcc_isolation() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table.insert(User { email: "a@x.com".into(), age: 30, name: "A".into() }).unwrap();
        wtx.commit().unwrap();
    }

    // Open a read tx before the batch mutation
    let rtx_before = store.begin_read(None).unwrap();

    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<User>("users").unwrap();
        table
            .insert_batch(vec![
                User { email: "b@x.com".into(), age: 25, name: "B".into() },
                User { email: "c@x.com".into(), age: 20, name: "C".into() },
            ])
            .unwrap();
        wtx.commit().unwrap();
    }

    // The pre-batch read tx should still see only the original record
    let table_before = rtx_before.open_table::<User>("users").unwrap();
    assert_eq!(table_before.len(), 1);

    // A new read tx should see all 3 records
    let rtx_after = store.begin_read(None).unwrap();
    let table_after = rtx_after.open_table::<User>("users").unwrap();
    assert_eq!(table_after.len(), 3);
}

const NOTES: TableDef<String> = TableDef::new("notes");

#[test]
fn table_def_const_open_table() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table(NOTES).unwrap();
    let id = table.insert("hello".to_string()).unwrap();
    wtx.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table(NOTES).unwrap();
    assert_eq!(table.get(id), Some(&"hello".to_string()));
}

#[test]
fn begin_read_nonexistent_version_returns_version_not_found() {
    let store = Store::default();
    assert!(matches!(store.begin_read(Some(99)), Err(Error::VersionNotFound(99))));
}

#[test]
fn delete_returns_removed_record() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    let mut table = wtx.open_table::<String>("t").unwrap();
    let id = table.insert("hello".to_string()).unwrap();
    let old = table.delete(id).unwrap();
    assert_eq!(*old, "hello".to_string());
    wtx.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Task 6: delete_table
// ---------------------------------------------------------------------------

#[test]
fn delete_table_removes_table_from_snapshot() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap().insert("alice".to_string()).unwrap();
        wtx.open_table::<String>("logs").unwrap().insert("event".to_string()).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        assert!(wtx.delete_table("users"));
        assert!(!wtx.delete_table("nonexistent"));
        wtx.commit().unwrap();
    }

    let rtx = store.begin_read(None).unwrap();
    assert!(matches!(rtx.open_table::<String>("users"), Err(Error::KeyNotFound)));
    assert_eq!(rtx.open_table::<String>("logs").unwrap().get(1), Some(&"event".to_string()));
}

#[test]
fn delete_table_then_recreate_in_same_tx() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("old".to_string()).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.delete_table("t");
        let mut table = wtx.open_table::<String>("t").unwrap();
        assert!(table.is_empty());
        table.insert("new".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<String>("t").unwrap();
    assert_eq!(table.len(), 1);
    assert_eq!(table.get(1), Some(&"new".to_string()));
}

// ---------------------------------------------------------------------------
// Task 7: table_names
// ---------------------------------------------------------------------------

#[test]
fn read_tx_table_names() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap();
        wtx.open_table::<u64>("posts").unwrap();
        wtx.commit().unwrap();
    }
    let rtx = store.begin_read(None).unwrap();
    let names = rtx.table_names();
    assert_eq!(names, vec!["posts", "users"]);
}

#[test]
fn write_tx_table_names_includes_dirty_and_base() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap();
        wtx.commit().unwrap();
    }
    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<String>("logs").unwrap();
    let names = wtx.table_names();
    assert_eq!(names, vec!["logs", "users"]);
}

#[test]
fn write_tx_table_names_excludes_deleted() {
    let store = Store::default();
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("users").unwrap();
        wtx.open_table::<String>("logs").unwrap();
        wtx.commit().unwrap();
    }
    let mut wtx = store.begin_write(None).unwrap();
    wtx.delete_table("users");
    let names = wtx.table_names();
    assert_eq!(names, vec!["logs"]);
}

#[test]
fn read_tx_table_names_empty_store() {
    let store = Store::default();
    let rtx = store.begin_read(None).unwrap();
    assert!(rtx.table_names().is_empty());
}

// ---------------------------------------------------------------------------
// Readable trait
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
#[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
#[allow(dead_code)]
struct TestRecord {
    name: String,
    value: i64,
}

#[test]
fn readable_trait_generic_access() {
    use ultima_db::{Readable, StoreConfig};

    fn count_records<T: Readable>(readable: &T) -> usize {
        let table = readable.open_table::<TestRecord>("widgets").unwrap();
        table.iter().count()
    }

    let store = Store::new(StoreConfig::default()).unwrap();
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<TestRecord>("widgets").unwrap();
        table.insert(TestRecord { name: "a".into(), value: 1 }).unwrap();
        table.insert(TestRecord { name: "b".into(), value: 2 }).unwrap();
    }
    wtx.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    assert_eq!(count_records(&rtx), 2);
    assert_eq!(rtx.version(), 1);
}

// ===========================================================================
// Multi-writer OCC tests
// ===========================================================================

/// Store::new returns Err when WAL directory is unwritable.
#[cfg(feature = "persistence")]
#[test]
fn store_new_returns_error_on_bad_wal_path() {
    use ultima_db::{Durability, Persistence, StoreConfig};
    let result = Store::new(StoreConfig {
        persistence: Persistence::Standalone {
            dir: std::path::PathBuf::from("/nonexistent/deeply/nested/path/that/cannot/exist"),
            durability: Durability::Consistent,
        },
        ..StoreConfig::default()
    });
    assert!(result.is_err(), "Store::new should return Err for unwritable WAL directory");
}

/// Helper: create a store in `MultiWriter` mode with default config.
fn multi_writer_store() -> Store {
    use ultima_db::{StoreConfig, WriterMode};
    Store::new(StoreConfig {
        writer_mode: WriterMode::MultiWriter,
        ..StoreConfig::default()
    }).unwrap()
}

// ---------------------------------------------------------------------------
// SingleWriter enforcement — `begin_write` returns `WriterBusy` when another
// `WriteTx` is active, and allows new writers after commit/rollback/drop.
// ---------------------------------------------------------------------------

/// Second `begin_write` while the first is still active → `WriterBusy`.
#[test]
fn single_writer_blocks_second_begin_write() {
    let store = Store::default(); // SingleWriter mode
    let _wtx = store.begin_write(None).unwrap();
    assert!(matches!(store.begin_write(None), Err(Error::WriterBusy)));
}

/// After committing the first writer, a second `begin_write` succeeds.
#[test]
fn single_writer_allows_after_commit() {
    let store = Store::default();
    let wtx = store.begin_write(None).unwrap();
    wtx.commit().unwrap();
    let _wtx2 = store.begin_write(None).unwrap();
}

/// Rollback releases the writer slot.
#[test]
fn single_writer_allows_after_rollback() {
    let store = Store::default();
    let wtx = store.begin_write(None).unwrap();
    wtx.rollback();
    let _wtx2 = store.begin_write(None).unwrap();
}

/// Drop guard releases the writer slot (no explicit commit/rollback needed).
#[test]
fn single_writer_allows_after_drop() {
    let store = Store::default();
    {
        let _wtx = store.begin_write(None).unwrap();
    } // dropped
    let _wtx2 = store.begin_write(None).unwrap();
}

// ---------------------------------------------------------------------------
// MultiWriter: disjoint tables — no overlap, both commit successfully.
// ---------------------------------------------------------------------------

/// Two writers to different tables have no overlap → both commit, and the
/// later commit rebases onto the earlier so the final snapshot contains
/// BOTH tables.
#[test]
fn multi_writer_disjoint_tables_both_commit() {
    let store = multi_writer_store();

    let mut wtx_a = store.begin_write(None).unwrap();
    let mut wtx_b = store.begin_write(None).unwrap();

    wtx_a.open_table::<String>("t1").unwrap().insert("a".to_string()).unwrap();
    wtx_b.open_table::<String>("t2").unwrap().insert("b".to_string()).unwrap();

    let v_a = wtx_a.commit().unwrap();
    let v_b = wtx_b.commit().unwrap();

    assert!(v_a < v_b);

    let rtx = store.begin_read(Some(v_b)).unwrap();
    assert_eq!(rtx.open_table::<String>("t1").unwrap().get(1), Some(&"a".to_string()));
    assert_eq!(rtx.open_table::<String>("t2").unwrap().get(1), Some(&"b".to_string()));
}

// ---------------------------------------------------------------------------
// MultiWriter: two writers to the same table always conflict (table-level OCC).
// The retry pattern is the supported way to land non-conflicting edits to the
// same table from concurrent writers.
// ---------------------------------------------------------------------------

/// Two writers to the same table — even on different keys — conflict. The
/// second writer must retry, which rebases onto the first's commit and lands
/// cleanly.
#[test]
fn multi_writer_same_table_conflicts_and_retry_succeeds() {
    let store = multi_writer_store();

    // Seed table with record id=1
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("seed".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    let mut wtx_a = store.begin_write(None).unwrap();
    let mut wtx_b = store.begin_write(None).unwrap();

    // Both share the same base (v1). Writer A inserts id=2, writer B updates id=1.
    wtx_a.open_table::<String>("t").unwrap().insert("from_a".to_string()).unwrap();
    wtx_b.open_table::<String>("t").unwrap().update(1, "from_b".to_string()).unwrap();

    wtx_a.commit().unwrap();
    // Table-level OCC: any concurrent commit to the same table conflicts,
    // even if keys are disjoint.
    let err = wtx_b.commit().unwrap_err();
    assert!(matches!(err, Error::WriteConflict { table, .. } if table == "t"));

    // Retry B with a fresh transaction. It now bases on the snapshot that
    // already contains A's insert, applies its update, and commits cleanly.
    let mut wtx_b = store.begin_write(None).unwrap();
    wtx_b.open_table::<String>("t").unwrap().update(1, "from_b".to_string()).unwrap();
    wtx_b.commit().unwrap();

    // Final state has both A's insert (id=2) and B's update (id=1).
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.get(1), Some(&"from_b".to_string()));
    assert_eq!(t.get(2), Some(&"from_a".to_string()));
}

// ---------------------------------------------------------------------------
// MultiWriter: overlapping keys → WriteConflict with table name, keys, version.
// ---------------------------------------------------------------------------

/// Both writers update key 1 → second commit returns `WriteConflict`
/// with the conflicting table, keys, and the version that caused the conflict.
#[test]
fn multi_writer_overlapping_keys_conflict() {
    let store = multi_writer_store();

    // Seed table
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("original".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    let mut wtx_a = store.begin_write(None).unwrap();
    let mut wtx_b = store.begin_write(None).unwrap();

    // Both update key 1
    wtx_a.open_table::<String>("t").unwrap().update(1, "from_a".to_string()).unwrap();
    wtx_b.open_table::<String>("t").unwrap().update(1, "from_b".to_string()).unwrap();

    wtx_a.commit().unwrap();
    let err = wtx_b.commit().unwrap_err();

    match err {
        Error::WriteConflict { table, keys, version } => {
            assert_eq!(table, "t");
            assert!(keys.contains(&1));
            assert_eq!(version, 2); // wtx_a committed as v2
        }
        other => panic!("expected WriteConflict, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// MultiWriter: drop without commit — Drop guard cleans up tracking state.
// ---------------------------------------------------------------------------

/// Dropping a `WriteTx` without commit decrements `active_writer_count`
/// and removes the base version, allowing subsequent writers to proceed.
#[test]
fn multi_writer_drop_without_commit_cleanup() {
    let store = multi_writer_store();

    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("will_drop".to_string()).unwrap();
        // dropped without commit
    }

    // Next writer should succeed and not see the dropped writer's changes
    let mut wtx2 = store.begin_write(None).unwrap();
    let table = wtx2.open_table::<String>("t").unwrap();
    assert!(table.is_empty());
    wtx2.commit().unwrap();
}

// ---------------------------------------------------------------------------
// MultiWriter: write-set log pruning — committed write sets are discarded
// once no in-flight writer needs them for validation.
// ---------------------------------------------------------------------------

/// After sequential commits with no overlapping writers, the write-set log
/// is fully pruned (no active writer holds an old base version).
#[test]
fn multi_writer_write_set_pruned_after_all_writers_complete() {
    let store = multi_writer_store();

    // Commit several transactions
    for i in 0..5u32 {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<u32>("t").unwrap().insert(i).unwrap();
        wtx.commit().unwrap();
    }

    // With no active writers, the committed_write_sets should be pruned
    assert_eq!(store.committed_write_set_count(), 0);
}

// ---------------------------------------------------------------------------
// MultiWriter: table deletion conflicts — deleting a table that a concurrent
// writer modified (or vice versa) is a WriteConflict.
// ---------------------------------------------------------------------------

/// Writer A deletes table, writer B modifies a key in it → B gets `WriteConflict`.
#[test]
fn multi_writer_delete_table_conflicts_with_modify() {
    let store = multi_writer_store();

    // Seed
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("data".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    let mut wtx_a = store.begin_write(None).unwrap();
    let mut wtx_b = store.begin_write(None).unwrap();

    // A deletes the table
    wtx_a.delete_table("t");
    // B modifies a key in the table
    wtx_b.open_table::<String>("t").unwrap().update(1, "updated".to_string()).unwrap();

    wtx_a.commit().unwrap();
    let err = wtx_b.commit().unwrap_err();
    assert!(matches!(err, Error::WriteConflict { table, .. } if table == "t"));
}

/// Reverse direction: writer A modifies, writer B deletes → B gets `WriteConflict`.
#[test]
fn multi_writer_modify_conflicts_with_delete() {
    let store = multi_writer_store();

    // Seed
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("data".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    let mut wtx_a = store.begin_write(None).unwrap();
    let mut wtx_b = store.begin_write(None).unwrap();

    // A modifies key
    wtx_a.open_table::<String>("t").unwrap().update(1, "from_a".to_string()).unwrap();
    // B deletes the table
    wtx_b.delete_table("t");

    wtx_a.commit().unwrap();
    let err = wtx_b.commit().unwrap_err();
    assert!(matches!(err, Error::WriteConflict { table, .. } if table == "t"));
}

// ---------------------------------------------------------------------------
// MultiWriter: batch operations — insert_batch records all auto-assigned IDs
// in the write set for conflict detection.
// ---------------------------------------------------------------------------

/// Both writers `insert_batch` into an empty table → same auto-assigned IDs
/// → second commit detects key overlap.
#[test]
fn multi_writer_insert_batch_tracks_keys() {
    let store = multi_writer_store();

    let mut wtx_a = store.begin_write(None).unwrap();
    let mut wtx_b = store.begin_write(None).unwrap();

    // A inserts batch (ids 1, 2, 3)
    wtx_a
        .open_table::<String>("t")
        .unwrap()
        .insert_batch(vec!["a".into(), "b".into(), "c".into()])
        .unwrap();
    // B also inserts (ids 1, 2, 3 — same since both start from empty table)
    wtx_b
        .open_table::<String>("t")
        .unwrap()
        .insert_batch(vec!["x".into(), "y".into(), "z".into()])
        .unwrap();

    wtx_a.commit().unwrap();
    let err = wtx_b.commit().unwrap_err();
    assert!(matches!(err, Error::WriteConflict { table, keys, .. } if table == "t" && !keys.is_empty()));
}

// ---------------------------------------------------------------------------
// MultiWriter: conflict cleanup — a failed commit (WriteConflict) must still
// decrement active_writer_count so subsequent writers aren't blocked.
// ---------------------------------------------------------------------------

/// After a `WriteConflict`, the failed writer's tracking state is cleaned up
/// and a new writer can proceed normally.
#[test]
fn multi_writer_conflict_cleans_up_active_writer() {
    let store = multi_writer_store();

    // Seed
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("seed".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    let mut wtx_a = store.begin_write(None).unwrap();
    let mut wtx_b = store.begin_write(None).unwrap();

    wtx_a.open_table::<String>("t").unwrap().update(1, "a".to_string()).unwrap();
    wtx_b.open_table::<String>("t").unwrap().update(1, "b".to_string()).unwrap();

    wtx_a.commit().unwrap();
    let _ = wtx_b.commit(); // fails with WriteConflict, but should clean up

    // Should be able to start a new writer
    let wtx_c = store.begin_write(None).unwrap();
    wtx_c.commit().unwrap();
}

// ---------------------------------------------------------------------------
// MultiWriter: read-only writer — a WriteTx that opens a table but only reads
// has an empty write set, so it never conflicts.
// ---------------------------------------------------------------------------

/// update_batch that fails should NOT poison the write set with phantom IDs.
/// If it does, an intervening rollback and reopen may produce spurious
/// WriteConflict later. (With table-level OCC, we exercise poisoning via a
/// rollback-and-retry pattern rather than two concurrent writers on the
/// same table, which would conflict regardless.)
#[test]
fn multi_writer_update_batch_failure_does_not_poison_write_set() {
    let store = multi_writer_store();

    // Seed with one record.
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("hello".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    // Writer A: attempt an update_batch on a nonexistent ID (should fail).
    let mut wtx_a = store.begin_write(None).unwrap();
    {
        let mut table = wtx_a.open_table::<String>("t").unwrap();
        let res = table.update_batch(vec![
            (1, "updated".to_string()),
            (999, "missing".to_string()), // KeyNotFound
        ]);
        assert!(res.is_err());
    }

    // Writer B: modify key 1 on "t" and commit.
    {
        let mut wtx_b = store.begin_write(None).unwrap();
        wtx_b.open_table::<String>("t").unwrap().update(1, "from_b".to_string()).unwrap();
        wtx_b.commit().unwrap();
    }

    // Writer A writes to a DIFFERENT table "u". If the failed batch had
    // poisoned write_set["t"] with phantom IDs, table-level OCC would flag
    // a conflict with B's commit to "t". Clean write_set["t"] → no conflict.
    {
        let mut table_u = wtx_a.open_table::<String>("u").unwrap();
        table_u.insert("new_record".to_string()).unwrap();
    }
    wtx_a.commit().unwrap();
}

/// Writer B opens a table and reads but writes nothing → empty write set → no conflict.
#[test]
fn multi_writer_read_only_tx_no_conflict() {
    let store = multi_writer_store();

    // Seed
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("data".to_string()).unwrap();
        wtx.commit().unwrap();
    }

    let mut wtx_a = store.begin_write(None).unwrap();
    let mut wtx_b = store.begin_write(None).unwrap();

    // A modifies key 1
    wtx_a.open_table::<String>("t").unwrap().update(1, "a".to_string()).unwrap();
    // B only reads (opens table but doesn't write)
    let table_b = wtx_b.open_table::<String>("t").unwrap();
    assert_eq!(table_b.get(1), Some(&"data".to_string())); // sees base

    wtx_a.commit().unwrap();
    wtx_b.commit().unwrap(); // should succeed — B had no writes
}

// ---------------------------------------------------------------------------
// Real-thread concurrent writes
//
// These tests drive WriterMode::MultiWriter from multiple OS threads. They
// verify that Store is Send + Sync and that the OCC machinery behaves
// correctly when transactions genuinely overlap in wall-clock time rather
// than being interleaved on a single thread.
// ---------------------------------------------------------------------------

use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::thread;

/// Store is the handle that crosses thread boundaries. Verify by inspection
/// that a Store clone compiles inside a thread::spawn move closure.
#[test]
fn concurrent_store_is_send_and_sync() {
    let store = multi_writer_store();
    let h = {
        let store = store.clone();
        thread::spawn(move || {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<u64>("t").unwrap().insert(42).unwrap();
            wtx.commit().unwrap()
        })
    };
    let v = h.join().unwrap();
    assert!(v >= 1);
}

/// Two threads, each writing to its own disjoint table — both commits succeed.
#[test]
fn concurrent_disjoint_tables_both_commit() {
    let store = multi_writer_store();
    let barrier = Arc::new(Barrier::new(2));

    let store_a = store.clone();
    let b_a = barrier.clone();
    let ta = thread::spawn(move || {
        b_a.wait();
        let mut wtx = store_a.begin_write(None).unwrap();
        wtx.open_table::<String>("t_a").unwrap().insert("a".into()).unwrap();
        wtx.commit().unwrap()
    });

    let store_b = store.clone();
    let b_b = barrier.clone();
    let tb = thread::spawn(move || {
        b_b.wait();
        let mut wtx = store_b.begin_write(None).unwrap();
        wtx.open_table::<String>("t_b").unwrap().insert("b".into()).unwrap();
        wtx.commit().unwrap()
    });

    let va = ta.join().unwrap();
    let vb = tb.join().unwrap();
    assert_ne!(va, vb);

    let final_v = store.latest_version();
    let rtx = store.begin_read(Some(final_v)).unwrap();
    assert_eq!(rtx.open_table::<String>("t_a").unwrap().get(1), Some(&"a".to_string()));
    assert_eq!(rtx.open_table::<String>("t_b").unwrap().get(1), Some(&"b".to_string()));
}

/// N threads, same table, disjoint keys — all commits eventually succeed.
/// No conflicts expected because each thread writes a unique, pre-seeded key.
#[test]
fn concurrent_same_table_disjoint_keys_all_commit() {
    const THREADS: u64 = 8;
    let store = multi_writer_store();

    // Preload ids 1..=THREADS so each thread owns exactly one key.
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<u64>("t").unwrap();
        for i in 1..=THREADS {
            t.insert(i).unwrap();
        }
        wtx.commit().unwrap();
    }

    let barrier = Arc::new(Barrier::new(THREADS as usize));
    let handles: Vec<_> = (1..=THREADS)
        .map(|i| {
            let store = store.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                b.wait();
                // Retry on conflict — can still happen if other threads' commits race.
                loop {
                    let mut wtx = store.begin_write(None).unwrap();
                    wtx.open_table::<u64>("t").unwrap().update(i, i * 100).unwrap();
                    match wtx.commit() {
                        Ok(_) => return,
                        Err(Error::WriteConflict { .. }) => continue,
                        Err(e) => panic!("unexpected error: {e}"),
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<u64>("t").unwrap();
    for i in 1..=THREADS {
        assert_eq!(t.get(i), Some(&(i * 100)));
    }
}

/// N threads, same table, *overlapping* keys — some commits conflict and
/// retry. Once all retries drain, final state reflects every logical write.
#[test]
fn concurrent_same_table_overlapping_keys_with_retry() {
    const THREADS: usize = 8;
    const WRITES_PER_THREAD: u64 = 20;
    let store = multi_writer_store();

    // Seed a single key that every thread will contend on, plus per-thread keys.
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<u64>("t").unwrap();
        t.insert(0).unwrap(); // hot key id=1
        for _ in 0..THREADS as u64 {
            t.insert(0).unwrap(); // ids 2..=THREADS+1
        }
        wtx.commit().unwrap();
    }

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS as u64)
        .map(|tid| {
            let store = store.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                b.wait();
                let mut conflicts = 0u64;
                for n in 0..WRITES_PER_THREAD {
                    loop {
                        let mut wtx = store.begin_write(None).unwrap();
                        {
                            let mut t = wtx.open_table::<u64>("t").unwrap();
                            // Always touch the hot key id=1.
                            t.update(1, tid * 1000 + n).unwrap();
                            // And the per-thread key (id = 2 + tid).
                            t.update(2 + tid, tid * 1000 + n).unwrap();
                        }
                        match wtx.commit() {
                            Ok(_) => break,
                            Err(Error::WriteConflict { .. }) => {
                                conflicts += 1;
                                continue;
                            }
                            Err(e) => panic!("unexpected error: {e}"),
                        }
                    }
                }
                conflicts
            })
        })
        .collect();

    let total_conflicts: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    // Real contention on the hot key should produce *some* conflicts. If this
    // ever fires 0, OCC is not detecting cross-thread overlap.
    assert!(
        total_conflicts > 0,
        "expected at least one WriteConflict across {THREADS} threads hitting hot key; got {total_conflicts}",
    );

    // Final state: each per-thread key shows its last logical write
    // (tid * 1000 + WRITES_PER_THREAD - 1).
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<u64>("t").unwrap();
    for tid in 0..THREADS as u64 {
        let expected = tid * 1000 + WRITES_PER_THREAD - 1;
        assert_eq!(t.get(2 + tid), Some(&expected), "thread {tid} final value");
    }
}

/// Readers opened on older versions continue to see their snapshot while
/// writers promote new snapshots concurrently.
#[test]
fn concurrent_readers_unaffected_by_writers() {
    let store = multi_writer_store();

    // Commit v1 so we have a snapshot to pin.
    {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<u64>("t").unwrap().insert(100).unwrap();
        wtx.commit().unwrap();
    }

    let rtx_v1 = store.begin_read(Some(1)).unwrap();
    assert_eq!(rtx_v1.version(), 1);

    // Spawn writers that push many new versions.
    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let store = store.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                b.wait();
                for _ in 0..10 {
                    loop {
                        let mut wtx = store.begin_write(None).unwrap();
                        wtx.open_table::<u64>("t").unwrap().insert(999).unwrap();
                        if wtx.commit().is_ok() {
                            break;
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    // v1 reader still reads exactly what was there at v1.
    let t = rtx_v1.open_table::<u64>("t").unwrap();
    assert_eq!(t.len(), 1);
    assert_eq!(t.get(1), Some(&100));
}

/// Stress test: many threads, many commits, validate that the set of
/// successfully-committed primary keys exactly matches what we asked for.
#[test]
fn concurrent_stress_insert_batch_no_lost_writes() {
    const THREADS: usize = 8;
    const INSERTS_PER_THREAD: u64 = 25;
    let store = multi_writer_store();

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS as u64)
        .map(|tid| {
            let store = store.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                b.wait();
                let mut inserted = Vec::with_capacity(INSERTS_PER_THREAD as usize);
                for n in 0..INSERTS_PER_THREAD {
                    let marker = tid * 10_000 + n;
                    loop {
                        let mut wtx = store.begin_write(None).unwrap();
                        let id = wtx
                            .open_table::<u64>("data")
                            .unwrap()
                            .insert(marker)
                            .unwrap();
                        match wtx.commit() {
                            Ok(_) => {
                                inserted.push((id, marker));
                                break;
                            }
                            Err(Error::WriteConflict { .. }) => continue,
                            Err(e) => panic!("unexpected: {e}"),
                        }
                    }
                }
                inserted
            })
        })
        .collect();

    let mut all_inserted: Vec<(u64, u64)> = Vec::new();
    for h in handles {
        all_inserted.extend(h.join().unwrap());
    }

    // Final table should contain exactly the markers we inserted, and their
    // ids should be unique (no two threads got the same assigned id).
    let ids: HashSet<u64> = all_inserted.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids.len(), all_inserted.len(), "auto-assigned ids collided");

    let expected_markers: HashSet<u64> = all_inserted.iter().map(|(_, m)| *m).collect();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<u64>("data").unwrap();
    let actual_markers: HashSet<u64> = t.iter().map(|(_, v)| *v).collect();
    assert_eq!(actual_markers, expected_markers);
    assert_eq!(t.len(), (THREADS as u64 * INSERTS_PER_THREAD) as usize);
}
