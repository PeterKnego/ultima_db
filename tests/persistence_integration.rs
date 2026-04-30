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
    let store = Store::new(config).unwrap();
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

// ---------------------------------------------------------------------------
// Consistent: WAL recovery via three-phase commit
// ---------------------------------------------------------------------------
// With three-phase commit, WAL entries are written by a background thread.
// If the store is dropped between WAL fsync (phase 2) and snapshot promotion
// (phase 3), the WAL has the entry but the in-memory snapshot doesn't.
// Recovery must replay the WAL and reconstruct the missing snapshot.

#[test]
fn consistent_wal_recovery_after_multiple_commits() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    // Write several transactions, then drop without checkpointing.
    {
        let store = open_store(config.clone());
        for i in 1..=10u32 {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users")
                .unwrap()
                .insert(User {
                    name: format!("User_{i}"),
                    age: 20 + i,
                })
                .unwrap();
            wtx.commit().unwrap();
        }
        // Drop store — WAL background thread flushes pending entries.
    }

    // Recover from WAL only (no checkpoint).
    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.version(), 10);
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 10);
    for i in 1..=10u64 {
        let user = table.get(i).unwrap();
        assert_eq!(user.name, format!("User_{i}"));
    }
}

#[test]
fn consistent_checkpoint_plus_wal_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        // 5 commits, then checkpoint at v5.
        for i in 1..=5u32 {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users")
                .unwrap()
                .insert(User { name: format!("User_{i}"), age: i })
                .unwrap();
            wtx.commit().unwrap();
        }
        store.checkpoint().unwrap();

        // 5 more commits (WAL only).
        for i in 6..=10u32 {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users")
                .unwrap()
                .insert(User { name: format!("User_{i}"), age: i })
                .unwrap();
            wtx.commit().unwrap();
        }
    }

    // Recover: checkpoint (v5) + WAL replay (v6..v10).
    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.version(), 10);
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 10);
    for i in 1..=10u64 {
        assert_eq!(table.get(i).unwrap().name, format!("User_{i}"));
    }
}

// ---------------------------------------------------------------------------
// Recovery returns error (not panic) on unregistered table
// ---------------------------------------------------------------------------

#[test]
fn recover_unregistered_table_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    // Write data with a registered User table.
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap()
            .insert(User { name: "Alice".into(), age: 30 }).unwrap();
        wtx.commit().unwrap();
        store.checkpoint().unwrap();
    }

    // Recover WITHOUT registering the User table — should return an error,
    // not panic.
    {
        let store = Store::new(config).unwrap();
        // Deliberately do NOT call store.register_table::<User>("users")
        let result = store.recover();
        assert!(result.is_err(), "recover() should return Err for unregistered table, not panic");
        let err = result.unwrap_err();
        assert!(
            matches!(err, ultima_db::Error::TableNotRegistered(ref name) if name == "users"),
            "expected TableNotRegistered('users'), got: {err:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Bulk-load: persistence integration (Phase 8)
// ---------------------------------------------------------------------------

#[test]
fn bulk_load_persists_via_checkpoint_and_recovers() {
    use ultima_db::{BulkLoadInput, BulkLoadOptions, BulkSource};

    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let rows: Vec<(u64, User)> = (1u64..=100)
            .map(|i| (i, User { name: format!("v{i}"), age: i as u32 }))
            .collect();
        store
            .bulk_load::<User>(
                "users",
                BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
                BulkLoadOptions::default(),
            )
            .unwrap();
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 100);
    assert_eq!(table.get(1).unwrap().name, "v1");
    assert_eq!(table.get(100).unwrap().name, "v100");
}

#[test]
fn bulk_load_skip_checkpoint_loses_data_on_crash() {
    use ultima_db::{BulkLoadInput, BulkLoadOptions, BulkSource};

    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let rows: Vec<(u64, User)> = (1u64..=10)
            .map(|i| (i, User { name: format!("v{i}"), age: i as u32 }))
            .collect();
        store
            .bulk_load::<User>(
                "users",
                BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
                BulkLoadOptions {
                    create_if_missing: true,
                    checkpoint_after: false,
                },
            )
            .unwrap();
    }

    // Bulk load without checkpoint is in-memory only — not WAL'd. After
    // reopen, the table is missing or empty.
    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    let missing_or_empty = match rtx.open_table::<User>("users") {
        Err(_) => true,
        Ok(t) => t.is_empty(),
    };
    assert!(
        missing_or_empty,
        "bulk_load with checkpoint_after=false should not survive a restart",
    );
}

// ---------------------------------------------------------------------------
// Persistence::None: checkpoint/recover/pending_wal_writes
// ---------------------------------------------------------------------------

#[test]
fn checkpoint_on_persistence_none_errors() {
    let store = Store::default();
    let res = store.checkpoint();
    assert!(res.is_err(), "checkpoint() must error in Persistence::None");
}

#[test]
fn recover_on_persistence_none_is_noop() {
    // Default Store has Persistence::None — recover must short-circuit Ok.
    let store = Store::default();
    store.recover().expect("recover is no-op for Persistence::None");
}

#[test]
fn pending_wal_writes_zero_without_wal_handle() {
    // Persistence::None has no WAL handle — pending_wal_writes must return 0.
    let store = Store::default();
    assert_eq!(store.pending_wal_writes(), 0);
}

// ---------------------------------------------------------------------------
// update_batch + delete_batch through the WAL path
// ---------------------------------------------------------------------------

#[test]
fn update_batch_and_delete_batch_replay_through_wal() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    // Seed three users.
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<User>("users").unwrap();
        t.insert(User { name: "Alice".into(), age: 30 }).unwrap();
        t.insert(User { name: "Bob".into(), age: 25 }).unwrap();
        t.insert(User { name: "Carol".into(), age: 40 }).unwrap();
        drop(t);
        wtx.commit().unwrap();
    }

    // Use update_batch and delete_batch — both must produce WAL ops.
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<User>("users").unwrap();
        t.update_batch(vec![
            (1, User { name: "Alice2".into(), age: 31 }),
            (3, User { name: "Carol2".into(), age: 41 }),
        ])
        .unwrap();
        t.delete_batch(&[2]).unwrap();
        drop(t);
        wtx.commit().unwrap();
    }

    // Reopen and confirm the batched edits replayed from the WAL.
    let store = open_store(config);
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<User>("users").unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t.get(1).unwrap().age, 31);
    assert_eq!(t.get(2), None);
    assert_eq!(t.get(3).unwrap().name, "Carol2");
}

// ---------------------------------------------------------------------------
// WriteTx::bulk_load via Delta + Replace — exercises in-tx upsert WAL branch
// ---------------------------------------------------------------------------

#[test]
fn write_tx_bulk_load_replays_through_wal() {
    use ultima_db::{BulkDelta, BulkLoadInput, BulkSource};

    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<User>("users").unwrap();
        t.bulk_load(BulkLoadInput::Replace(BulkSource::sorted_vec(vec![
            (1, User { name: "Alice".into(), age: 30 }),
            (2, User { name: "Bob".into(), age: 25 }),
        ])))
        .unwrap();
        drop(t);
        wtx.commit().unwrap();
    }

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<User>("users").unwrap();
        t.bulk_load(BulkLoadInput::Delta(BulkDelta {
            inserts: vec![(10, User { name: "Eve".into(), age: 22 })],
            updates: vec![(1, User { name: "Alice2".into(), age: 31 })],
            deletes: vec![2],
        }))
        .unwrap();
        drop(t);
        wtx.commit().unwrap();
    }

    let store = open_store(config);
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<User>("users").unwrap();
    assert_eq!(t.get(1).unwrap().name, "Alice2");
    assert_eq!(t.get(2), None);
    assert_eq!(t.get(10).unwrap().name, "Eve");
}

// ---------------------------------------------------------------------------
// Read-only commit: ensures the empty-ops branch in commit-time WAL submit
// is exercised when persistence is enabled.
// ---------------------------------------------------------------------------

#[test]
fn read_only_write_tx_commit_with_persistence_no_wal_entry() {
    let dir = tempfile::tempdir().unwrap();
    let config = standalone_config(dir.path(), Durability::Consistent);

    // Seed.
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User { name: "Alice".into(), age: 30 })
            .unwrap();
        wtx.commit().unwrap();
    }

    // Now open a WriteTx, only read, and commit. No WAL ops should be written.
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        {
            let t = wtx.open_table::<User>("users").unwrap();
            assert_eq!(t.get(1).unwrap().name, "Alice");
        }
        wtx.commit().unwrap();
    }

    // Reopen — data must still be intact and recoverable.
    let store = open_store(config);
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<User>("users").unwrap();
    assert_eq!(t.get(1).unwrap().name, "Alice");
}
