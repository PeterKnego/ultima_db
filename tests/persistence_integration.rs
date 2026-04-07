#![cfg(feature = "persistence")]

use std::path::Path;
use ultima_db::{Durability, Persistence, Store, StoreConfig};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct User {
    name: String,
    age: u32,
}

fn standalone_config(dir: &Path, durability: Durability) -> StoreConfig {
    StoreConfig {
        persistence: Persistence::Standalone {
            dir: dir.to_path_buf(),
            durability,
        },
        ..StoreConfig::default()
    }
}

fn smr_config(dir: &Path) -> StoreConfig {
    StoreConfig {
        persistence: Persistence::Smr {
            dir: dir.to_path_buf(),
        },
        ..StoreConfig::default()
    }
}

/// Helper: create store, register User table, recover from disk.
fn open_store(config: StoreConfig) -> Store {
    let store = Store::new(config);
    store.register_table::<User>("users").unwrap();
    store.recover().unwrap();
    store
}

// ---------------------------------------------------------------------------
// Standalone: WAL recovery (Consistent)
// ---------------------------------------------------------------------------

#[test]
fn standalone_wal_recovery_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    // Write data
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap().insert(User { name: "Alice".into(), age: 30 }).unwrap();
        wtx.commit().unwrap();

        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap().insert(User { name: "Bob".into(), age: 25 }).unwrap();
        wtx.commit().unwrap();
    }

    // Recover from WAL
    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.version(), 2);
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 2);
    assert_eq!(table.get(1).unwrap(), &User { name: "Alice".into(), age: 30 });
    assert_eq!(table.get(2).unwrap(), &User { name: "Bob".into(), age: 25 });
}

// ---------------------------------------------------------------------------
// Standalone: Checkpoint round-trip
// ---------------------------------------------------------------------------

#[test]
fn standalone_checkpoint_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap().insert(User { name: "Alice".into(), age: 30 }).unwrap();
        wtx.commit().unwrap();
        store.checkpoint().unwrap();
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.version(), 1);
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 1);
    assert_eq!(table.get(1).unwrap(), &User { name: "Alice".into(), age: 30 });
}

// ---------------------------------------------------------------------------
// Standalone: Checkpoint + WAL
// ---------------------------------------------------------------------------

#[test]
fn standalone_checkpoint_plus_wal_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        for i in 1..=3u32 {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users").unwrap().insert(User {
                name: format!("User{i}"),
                age: 20 + i,
            }).unwrap();
            wtx.commit().unwrap();
        }
        // Checkpoint at v3 (prunes WAL)
        store.checkpoint().unwrap();

        // Write 2 more (WAL only)
        for i in 4..=5u32 {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users").unwrap().insert(User {
                name: format!("User{i}"),
                age: 20 + i,
            }).unwrap();
            wtx.commit().unwrap();
        }
    }

    // Recover: checkpoint (v3) + WAL replay (v4, v5)
    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.version(), 5);
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 5);
    for i in 1u64..=5 {
        assert_eq!(table.get(i).unwrap().name, format!("User{i}"));
    }
}

// ---------------------------------------------------------------------------
// SMR: checkpoint only
// ---------------------------------------------------------------------------

#[test]
fn smr_checkpoint_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let config = smr_config(dir.path());

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap().insert(User { name: "Alice".into(), age: 30 }).unwrap();
        wtx.commit().unwrap();
        store.checkpoint().unwrap();
    }

    let store2 = open_store(config);
    assert_eq!(store2.latest_version(), 1);
    let rtx = store2.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.get(1).unwrap(), &User { name: "Alice".into(), age: 30 });
}

// ---------------------------------------------------------------------------
// WAL pruning on checkpoint
// ---------------------------------------------------------------------------

#[test]
fn wal_pruned_after_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    let store = open_store(config);
    for i in 1..=5u32 {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap().insert(User {
            name: format!("User{i}"),
            age: 20 + i,
        }).unwrap();
        wtx.commit().unwrap();
    }

    store.checkpoint().unwrap();

    // WAL file should exist but contain no entries after checkpoint.
    let wal_path = dir.path().join("wal.bin");
    let entries = ultima_db::wal::read_wal(&wal_path).unwrap();
    assert!(entries.is_empty(), "WAL should be empty after checkpoint");
}

// ---------------------------------------------------------------------------
// Eventual durability: smoke test
// ---------------------------------------------------------------------------

#[test]
fn standalone_eventual_basic() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Eventual);

    let store = open_store(config);
    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<User>("users").unwrap().insert(User { name: "Alice".into(), age: 30 }).unwrap();
    wtx.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    assert_eq!(rtx.open_table::<User>("users").unwrap().get(1).unwrap().name, "Alice");
}

// ---------------------------------------------------------------------------
// Persistence::None is unchanged
// ---------------------------------------------------------------------------

#[test]
fn persistence_none_unchanged() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<User>("users").unwrap().insert(User { name: "Alice".into(), age: 30 }).unwrap();
    wtx.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    assert_eq!(rtx.open_table::<User>("users").unwrap().get(1).unwrap().name, "Alice");
}

// ---------------------------------------------------------------------------
// Update and delete survive recovery
// ---------------------------------------------------------------------------

#[test]
fn wal_update_and_delete_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table::<User>("users").unwrap();
            t.insert(User { name: "Alice".into(), age: 30 }).unwrap();
            t.insert(User { name: "Bob".into(), age: 25 }).unwrap();
            t.insert(User { name: "Charlie".into(), age: 35 }).unwrap();
        }
        wtx.commit().unwrap();

        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table::<User>("users").unwrap();
            t.update(1, User { name: "Alice Updated".into(), age: 31 }).unwrap();
            t.delete(2).unwrap();
        }
        wtx.commit().unwrap();
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 2);
    assert_eq!(table.get(1).unwrap(), &User { name: "Alice Updated".into(), age: 31 });
    assert_eq!(table.get(2), None);
    assert_eq!(table.get(3).unwrap(), &User { name: "Charlie".into(), age: 35 });
}

// ---------------------------------------------------------------------------
// Delete table survives recovery
// ---------------------------------------------------------------------------

#[test]
fn wal_delete_table_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap().insert(User { name: "Alice".into(), age: 30 }).unwrap();
        wtx.commit().unwrap();

        let mut wtx = store.begin_write(None).unwrap();
        wtx.delete_table("users");
        wtx.commit().unwrap();
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert!(rtx.open_table::<User>("users").is_err());
}

// ---------------------------------------------------------------------------
// Eventual: drop flushes all pending WAL writes
// ---------------------------------------------------------------------------

#[test]
fn eventual_drop_flushes_all_pending_wal_writes() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Eventual);
    let num_records = 5_000;

    // Write thousands of records in Eventual mode, then immediately drop.
    {
        let store = open_store(config.clone());
        for i in 0..num_records {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users")
                .unwrap()
                .insert(User {
                    name: format!("User_{i}"),
                    age: i,
                })
                .unwrap();
            wtx.commit().unwrap();
        }
        // Drop store immediately — pending WAL writes must be flushed.
    }

    // Recover and verify every record survived.
    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.version(), num_records as u64);
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), num_records as usize);
    for i in 0..num_records {
        let id = (i + 1) as u64;
        let user = table.get(id).unwrap();
        assert_eq!(user.name, format!("User_{i}"));
        assert_eq!(user.age, i);
    }
}
