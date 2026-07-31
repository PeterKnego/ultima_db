// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

#![cfg(feature = "persistence")]

mod common;

use std::path::Path;
use ultima_db::{Durability, Error, Persistence, Store, StoreConfig, WalWrite};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct User {
    name: String,
    age: u32,
}

fn standalone_config(dir: &Path, durability: Durability) -> StoreConfig {
    StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.to_path_buf(),
            durability,
            WalWrite::PerEntry,
        ))
        .build()
}

fn smr_config(dir: &Path) -> StoreConfig {
    StoreConfig::builder()
        .persistence(Persistence::smr(dir.to_path_buf()))
        .build()
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
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    // Write data
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();
        wtx.commit().unwrap();

        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: "Bob".into(),
                age: 25,
            })
            .unwrap();
        wtx.commit().unwrap();
    }

    // Recover from WAL
    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.version(), 2);
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 2);
    assert_eq!(
        table.get(1).unwrap(),
        &User {
            name: "Alice".into(),
            age: 30
        }
    );
    assert_eq!(
        table.get(2).unwrap(),
        &User {
            name: "Bob".into(),
            age: 25
        }
    );
}

// ---------------------------------------------------------------------------
// Standalone: Checkpoint round-trip
// ---------------------------------------------------------------------------

#[test]
fn standalone_checkpoint_roundtrip() {
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();
        wtx.commit().unwrap();
        store.checkpoint().unwrap();
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.version(), 1);
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 1);
    assert_eq!(
        table.get(1).unwrap(),
        &User {
            name: "Alice".into(),
            age: 30
        }
    );
}

// ---------------------------------------------------------------------------
// Standalone: Checkpoint + WAL
// ---------------------------------------------------------------------------

#[test]
fn standalone_checkpoint_plus_wal_recovery() {
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        for i in 1..=3u32 {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users")
                .unwrap()
                .insert(User {
                    name: format!("User{i}"),
                    age: 20 + i,
                })
                .unwrap();
            wtx.commit().unwrap();
        }
        // Checkpoint at v3 (prunes WAL)
        store.checkpoint().unwrap();

        // Write 2 more (WAL only)
        for i in 4..=5u32 {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users")
                .unwrap()
                .insert(User {
                    name: format!("User{i}"),
                    age: 20 + i,
                })
                .unwrap();
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
    let dir = common::test_scratch::scratch_dir();
    let config = smr_config(dir.path());

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();
        wtx.commit().unwrap();
        store.checkpoint().unwrap();
    }

    let store2 = open_store(config);
    assert_eq!(store2.latest_version(), 1);
    let rtx = store2.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(
        table.get(1).unwrap(),
        &User {
            name: "Alice".into(),
            age: 30
        }
    );
}

// ---------------------------------------------------------------------------
// SMR: checkpoint concurrent with writes (consistent-prefix recovery)
// ---------------------------------------------------------------------------

/// Stress the SMR checkpoint path while a writer is actively committing, then
/// verify recovery lands on a consistent committed prefix — never a torn or
/// partial snapshot.
///
/// SMR mode has no WAL: durability is owned by the external consensus log, and
/// `recover()` restores only the latest *checkpoint*. So unlike the Standalone
/// concurrent test (`checkpoint_concurrent_with_commits_loses_no_acknowledged_commit`),
/// commits made after the winning checkpoint are *expected* to be absent after
/// recovery — in a real deployment the consensus log replays them. What must
/// always hold is that the recovered snapshot is an exact prefix: if recovery
/// lands at version V, rows 1..=V are present and correct and nothing exists
/// above V. With `begin_write(None)` the i-th commit inserts row `i` at version
/// `i`, so `len() == latest_version()` is the prefix invariant.
#[test]
fn smr_checkpoint_concurrent_with_writes_recovers_consistent_prefix() {
    use std::sync::atomic::{AtomicBool, Ordering};

    const ROUNDS: usize = 6;
    const COMMITS: usize = 1000;

    for round in 0..ROUNDS {
        let dir = common::test_scratch::scratch_dir();
        let config = smr_config(dir.path());

        {
            let store = open_store(config.clone());

            // Commit one row before spawning the racer so every checkpoint it
            // takes captures at least version 1 (no empty-store checkpoint can
            // win the race and recover to an uninteresting version 0).
            {
                let mut wtx = store.begin_write(None).unwrap();
                wtx.open_table::<User>("users")
                    .unwrap()
                    .insert(User {
                        name: "row1".into(),
                        age: 1,
                    })
                    .unwrap();
                wtx.commit().unwrap();
            }

            let done = std::sync::Arc::new(AtomicBool::new(false));
            // SMR commits are pure in-memory (no WAL/fsync), so the writer would
            // otherwise blast through every commit before this thread is even
            // scheduled. The `started` gate makes the checkpointer provably loop
            // before any further commit runs, so the write window genuinely
            // overlaps an active checkpoint loop.
            let started = std::sync::Arc::new(AtomicBool::new(false));
            let ckpt_done = done.clone();
            let ckpt_started = started.clone();
            let ckpt_store = store.clone();
            let checkpointer = std::thread::spawn(move || {
                let mut count = 0usize;
                while !ckpt_done.load(Ordering::Acquire) {
                    // A concurrent checkpoint must never corrupt the snapshot.
                    // Transient errors are fine; a torn checkpoint is not.
                    let _ = ckpt_store.checkpoint();
                    count += 1;
                    ckpt_started.store(true, Ordering::Release);
                }
                count
            });

            while !started.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            for i in 2..=COMMITS {
                let mut wtx = store.begin_write(None).unwrap();
                wtx.open_table::<User>("users")
                    .unwrap()
                    .insert(User {
                        name: format!("row{i}"),
                        age: i as u32,
                    })
                    .unwrap();
                wtx.commit().unwrap();
            }

            done.store(true, Ordering::Release);
            let ckpt_runs = checkpointer.join().unwrap();
            assert!(ckpt_runs > 0, "round {round}: checkpoint loop never ran");
        }

        // Recover from whichever checkpoint won the race. SMR has no WAL, so we
        // land on some committed version V in 1..=COMMITS, and the recovered
        // snapshot must be an exact prefix of the write sequence.
        let store = open_store(config);
        let v = store.latest_version();
        assert!(
            (1..=COMMITS as u64).contains(&v),
            "round {round}: recovered version {v} outside committed range 1..={COMMITS}"
        );
        let rtx = store.begin_read(None).unwrap();
        let users = rtx.open_table::<User>("users").unwrap();
        assert_eq!(
            users.len(),
            v as usize,
            "round {round}: row count {} != recovered version {v} (torn checkpoint)",
            users.len()
        );
        for i in 1..=v {
            assert_eq!(
                users.get(i),
                Some(&User {
                    name: format!("row{i}"),
                    age: i as u32,
                }),
                "round {round}: row {i} missing/wrong in recovered prefix (version {v})"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// WAL pruning on checkpoint
// ---------------------------------------------------------------------------

#[test]
fn wal_pruned_after_checkpoint() {
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    let store = open_store(config);
    for i in 1..=5u32 {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: format!("User{i}"),
                age: 20 + i,
            })
            .unwrap();
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
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Eventual);

    let store = open_store(config);
    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<User>("users")
        .unwrap()
        .insert(User {
            name: "Alice".into(),
            age: 30,
        })
        .unwrap();
    wtx.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    assert_eq!(
        rtx.open_table::<User>("users")
            .unwrap()
            .get(1)
            .unwrap()
            .name,
        "Alice"
    );
}

// ---------------------------------------------------------------------------
// Persistence::None is unchanged
// ---------------------------------------------------------------------------

#[test]
fn persistence_none_unchanged() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<User>("users")
        .unwrap()
        .insert(User {
            name: "Alice".into(),
            age: 30,
        })
        .unwrap();
    wtx.commit().unwrap();

    let rtx = store.begin_read(None).unwrap();
    assert_eq!(
        rtx.open_table::<User>("users")
            .unwrap()
            .get(1)
            .unwrap()
            .name,
        "Alice"
    );
}

// ---------------------------------------------------------------------------
// Update and delete survive recovery
// ---------------------------------------------------------------------------

#[test]
fn wal_update_and_delete_recovery() {
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table::<User>("users").unwrap();
            t.insert(User {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();
            t.insert(User {
                name: "Bob".into(),
                age: 25,
            })
            .unwrap();
            t.insert(User {
                name: "Charlie".into(),
                age: 35,
            })
            .unwrap();
        }
        wtx.commit().unwrap();

        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table::<User>("users").unwrap();
            t.update(
                1,
                User {
                    name: "Alice Updated".into(),
                    age: 31,
                },
            )
            .unwrap();
            t.delete(2).unwrap();
        }
        wtx.commit().unwrap();
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 2);
    assert_eq!(
        table.get(1).unwrap(),
        &User {
            name: "Alice Updated".into(),
            age: 31
        }
    );
    assert_eq!(table.get(2), None);
    assert_eq!(
        table.get(3).unwrap(),
        &User {
            name: "Charlie".into(),
            age: 35
        }
    );
}

// ---------------------------------------------------------------------------
// Delete table survives recovery
// ---------------------------------------------------------------------------

#[test]
fn wal_delete_table_recovery() {
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();
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
    let dir = common::test_scratch::scratch_dir();
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
    let dir = common::test_scratch::scratch_dir();
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
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        // 5 commits, then checkpoint at v5.
        for i in 1..=5u32 {
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
        store.checkpoint().unwrap();

        // 5 more commits (WAL only).
        for i in 6..=10u32 {
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
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    // Write data with a registered User table.
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();
        wtx.commit().unwrap();
        store.checkpoint().unwrap();
    }

    // Recover WITHOUT registering the User table — should return an error,
    // not panic.
    {
        let store = Store::new(config).unwrap();
        // Deliberately do NOT call store.register_table::<User>("users")
        let result = store.recover();
        assert!(
            result.is_err(),
            "recover() should return Err for unregistered table, not panic"
        );
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

    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let rows: Vec<(u64, User)> = (1u64..=100)
            .map(|i| {
                (
                    i,
                    User {
                        name: format!("v{i}"),
                        age: i as u32,
                    },
                )
            })
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

    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let rows: Vec<(u64, User)> = (1u64..=10)
            .map(|i| {
                (
                    i,
                    User {
                        name: format!("v{i}"),
                        age: i as u32,
                    },
                )
            })
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
    store
        .recover()
        .expect("recover is no-op for Persistence::None");
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
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    // Seed three users.
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<User>("users").unwrap();
        t.insert(User {
            name: "Alice".into(),
            age: 30,
        })
        .unwrap();
        t.insert(User {
            name: "Bob".into(),
            age: 25,
        })
        .unwrap();
        t.insert(User {
            name: "Carol".into(),
            age: 40,
        })
        .unwrap();
        drop(t);
        wtx.commit().unwrap();
    }

    // Use update_batch and delete_batch — both must produce WAL ops.
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<User>("users").unwrap();
        t.update_batch(vec![
            (
                1,
                User {
                    name: "Alice2".into(),
                    age: 31,
                },
            ),
            (
                3,
                User {
                    name: "Carol2".into(),
                    age: 41,
                },
            ),
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

    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<User>("users").unwrap();
        t.bulk_load(BulkLoadInput::Replace(BulkSource::sorted_vec(vec![
            (
                1,
                User {
                    name: "Alice".into(),
                    age: 30,
                },
            ),
            (
                2,
                User {
                    name: "Bob".into(),
                    age: 25,
                },
            ),
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
            inserts: vec![(
                10,
                User {
                    name: "Eve".into(),
                    age: 22,
                },
            )],
            updates: vec![(
                1,
                User {
                    name: "Alice2".into(),
                    age: 31,
                },
            )],
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
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent);

    // Seed.
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: "Alice".into(),
                age: 30,
            })
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

#[test]
fn standalone_wal_recovery_consistent_coalesced() {
    let dir = common::test_scratch::scratch_dir();
    let config = StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.path().to_path_buf(),
            Durability::Consistent,
            WalWrite::Coalesced,
        ))
        .build();

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap().insert(User { name: "Alice".into(), age: 30 }).unwrap();
        wtx.commit().unwrap();
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap().insert(User { name: "Bob".into(), age: 25 }).unwrap();
        wtx.commit().unwrap();
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.version(), 2);
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 2);
    assert_eq!(table.get(1).unwrap(), &User { name: "Alice".into(), age: 30 });
    assert_eq!(table.get(2).unwrap(), &User { name: "Bob".into(), age: 25 });
}

// ---------------------------------------------------------------------------
// Eventual × Coalesced: write→drop→reopen→recover round-trip
// ---------------------------------------------------------------------------

#[test]
fn standalone_wal_recovery_eventual_coalesced() {
    let dir = common::test_scratch::scratch_dir();
    let config = StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.path().to_path_buf(),
            Durability::Eventual,
            WalWrite::Coalesced,
        ))
        .build();

    {
        let store = open_store(config.clone());
        for (name, age) in [("Alice", 30u32), ("Bob", 25), ("Carol", 41)] {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users")
                .unwrap()
                .insert(User { name: name.into(), age })
                .unwrap();
            wtx.commit().unwrap();
        }
        // Eventual: dropping the store at the end of this scope joins the WAL
        // background thread, which fsyncs all pending writes before we reopen.
        // (Same guarantee exercised by `eventual_drop_flushes_all_pending_wal_writes`.)
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert_eq!(table.len(), 3);
    assert_eq!(table.get(3).unwrap(), &User { name: "Carol".into(), age: 41 });
}

// ---------------------------------------------------------------------------
// PerEntry default: sanity round-trip (WalWrite::PerEntry is unchanged)
// ---------------------------------------------------------------------------

#[test]
fn standalone_perentry_default_still_recovers() {
    // Sanity: the default WalWrite::PerEntry path behaves as before.
    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Consistent); // sets PerEntry
    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User { name: "Alice".into(), age: 30 })
            .unwrap();
        wtx.commit().unwrap();
    }
    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.open_table::<User>("users").unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Bulk loads vs WAL recovery
// ---------------------------------------------------------------------------

/// Commits that land *after* an uncheckpointed bulk load cannot be replayed
/// on recovery — they were computed against post-load state, but the WAL
/// only reaches pre-load state. Recovery must fail with a clear error
/// instead of silently producing a state no client ever observed.
#[test]
fn recovery_fails_cleanly_when_commits_follow_uncheckpointed_bulk_load() {
    use ultima_db::{BulkLoadInput, BulkLoadOptions, BulkSource, Error};

    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Eventual);

    {
        let store = open_store(config.clone());

        // Seed commit (WAL v1).
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: "seed".into(),
                age: 1,
            })
            .unwrap();
        wtx.commit().unwrap();

        // Bulk load without a checkpoint.
        let rows: Vec<(u64, User)> = (1..=3)
            .map(|i| {
                (
                    i,
                    User {
                        name: format!("bulk{i}"),
                        age: i as u32,
                    },
                )
            })
            .collect();
        store
            .bulk_load::<User>(
                "users",
                BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
                BulkLoadOptions {
                    checkpoint_after: false,
                    ..BulkLoadOptions::default()
                },
            )
            .unwrap();

        // A normal commit on top of the loaded state (WAL entry exists,
        // but its semantics assume the post-load table).
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: "after".into(),
                age: 99,
            })
            .unwrap();
        wtx.commit().unwrap();
        // Store dropped: WAL flushed to disk.
    }

    let store = Store::new(config).unwrap();
    store.register_table::<User>("users").unwrap();
    let res = store.recover();
    assert!(
        matches!(res, Err(Error::BulkLoadNotCheckpointed { .. })),
        "recovery across an uncheckpointed bulk load with later commits \
         must fail cleanly, got {res:?}"
    );
}

/// With no commits after the load, recovery falls back to the pre-load
/// state — the documented `checkpoint_after: false` contract ("crash loses
/// the load") — rather than failing.
#[test]
fn recovery_without_commits_after_uncheckpointed_bulk_load_loses_only_the_load() {
    use ultima_db::{BulkLoadInput, BulkLoadOptions, BulkSource};

    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Eventual);

    {
        let store = open_store(config.clone());
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: "seed".into(),
                age: 1,
            })
            .unwrap();
        wtx.commit().unwrap();

        let rows: Vec<(u64, User)> = vec![(
            1,
            User {
                name: "bulk".into(),
                age: 2,
            },
        )];
        store
            .bulk_load::<User>(
                "users",
                BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
                BulkLoadOptions {
                    checkpoint_after: false,
                    ..BulkLoadOptions::default()
                },
            )
            .unwrap();
    }

    let store = open_store(config);
    let rtx = store.begin_read(None).unwrap();
    let users = rtx.open_table::<User>("users").unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users.get(1).unwrap().name, "seed");
}

/// `checkpoint_after: true` makes the load durable: recovery returns the
/// post-load state and later commits replay cleanly on top of it.
#[test]
fn recovery_after_checkpointed_bulk_load_replays_later_commits() {
    use ultima_db::{BulkLoadInput, BulkLoadOptions, BulkSource};

    let dir = common::test_scratch::scratch_dir();
    let config = standalone_config(dir.path(), Durability::Eventual);

    {
        let store = open_store(config.clone());
        let rows: Vec<(u64, User)> = (1..=2)
            .map(|i| {
                (
                    i,
                    User {
                        name: format!("bulk{i}"),
                        age: i as u32,
                    },
                )
            })
            .collect();
        store
            .bulk_load::<User>(
                "users",
                BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
                BulkLoadOptions {
                    checkpoint_after: true,
                    ..BulkLoadOptions::default()
                },
            )
            .unwrap();

        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: "after".into(),
                age: 99,
            })
            .unwrap();
        wtx.commit().unwrap();
    }

    let store = open_store(config);
    let rtx = store.begin_read(None).unwrap();
    let users = rtx.open_table::<User>("users").unwrap();
    assert_eq!(users.len(), 3, "bulk rows + later commit expected");
    assert_eq!(users.get(1).unwrap().name, "bulk1");
    assert_eq!(users.get(3).unwrap().name, "after");
}

// ---------------------------------------------------------------------------
// Checkpoint / WAL-prune vs concurrent commits
// ---------------------------------------------------------------------------

/// Checkpointing (which prunes the WAL) concurrently with commits must never
/// destroy an acknowledged commit. The WAL rewrite must be serialized with
/// the background WAL writer: a read→truncate→rewrite racing live appends
/// destroys entries whose committers were already told they are durable.
/// Each round commits N rows (every commit() Ok = acknowledged durable in
/// Consistent mode) while a checkpoint loop runs, then recovers from disk
/// and verifies all N rows survived.
#[test]
fn checkpoint_concurrent_with_commits_loses_no_acknowledged_commit() {
    use std::sync::atomic::{AtomicBool, Ordering};

    const ROUNDS: usize = 12;
    const COMMITS: usize = 150;

    for round in 0..ROUNDS {
        let dir = common::test_scratch::scratch_dir();
        let config = standalone_config(dir.path(), Durability::Consistent);

        {
            let store = open_store(config.clone());

            let done = std::sync::Arc::new(AtomicBool::new(false));
            let ckpt_done = done.clone();
            let ckpt_store = store.clone();
            let checkpointer = std::thread::spawn(move || {
                let mut count = 0usize;
                while !ckpt_done.load(Ordering::Acquire) {
                    // Errors (e.g. transient conflicts) are fine; destroying
                    // acknowledged WAL entries is not.
                    let _ = ckpt_store.checkpoint();
                    count += 1;
                }
                count
            });

            for i in 0..COMMITS {
                let mut wtx = store.begin_write(None).unwrap();
                wtx.open_table::<User>("users")
                    .unwrap()
                    .insert(User {
                        name: format!("row{i}"),
                        age: i as u32,
                    })
                    .unwrap();
                // Ok(commit) in Consistent mode = fsync-acknowledged.
                wtx.commit().unwrap();
            }

            done.store(true, Ordering::Release);
            let ckpt_runs = checkpointer.join().unwrap();
            assert!(ckpt_runs > 0, "checkpoint loop never ran");
        }

        // Recover from disk: every acknowledged commit must be present.
        let store = open_store(config);
        let rtx = store.begin_read(None).unwrap();
        let users = rtx.open_table::<User>("users").unwrap();
        assert_eq!(
            users.len(),
            COMMITS,
            "round {round}: acknowledged commits lost after recovery \
             (checkpoint/prune raced the WAL writer)"
        );
    }
}

// ---------------------------------------------------------------------------
// Arbitrary primary keys: end-to-end WAL recovery of a String-keyed table
// ---------------------------------------------------------------------------

/// The end-to-end counterpart to
/// `src/wal.rs::variable_length_keys_roundtrip_through_the_wal_file`: a
/// `String`-keyed table written through the public transaction API, fsynced,
/// and read back from a fresh store after WAL replay.
#[test]
fn string_keyed_table_survives_wal_recovery() {
    let dir = common::test_scratch::scratch_dir();
    // Bound rather than inlined so the reads below can take `&key` — the
    // by-reference form a non-`Copy` key wants — without clippy flagging a
    // borrow of a throwaway temporary.
    let (alice, bob) = ("alice@x.com".to_string(), "bob@x.com".to_string());

    {
        let store = Store::new(
            StoreConfig::builder()
                .persistence(Persistence::standalone(
                    dir.path().to_path_buf(),
                    Durability::Consistent,
                    WalWrite::PerEntry,
                ))
                .build(),
        )
        .unwrap();
        store
            .register_table_keyed::<String, String>("emails")
            .unwrap();

        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table_keyed::<String, String>("emails").unwrap();
        t.put(alice.clone(), "Alice".to_string()).unwrap();
        t.put(bob.clone(), "Bob".to_string()).unwrap();
        drop(t);
        wtx.commit().unwrap();
    }

    let store = Store::new(
        StoreConfig::builder()
            .persistence(Persistence::standalone(
                dir.path().to_path_buf(),
                Durability::Consistent,
                WalWrite::PerEntry,
            ))
            .build(),
    )
    .unwrap();
    store
        .register_table_keyed::<String, String>("emails")
        .unwrap();
    store.recover().unwrap();

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table_keyed::<String, String>("emails").unwrap();
    assert_eq!(t.get(&alice), Some(&"Alice".to_string()));
    assert_eq!(t.get(&bob), Some(&"Bob".to_string()));
}

// ---------------------------------------------------------------------------
// Key-type identity: a directory reopened under a different `K` is refused
// ---------------------------------------------------------------------------

/// Write three rows into `dir` through a `u64`-keyed table, checkpointing (or
/// not) at the end. `checkpointed` selects which of the two on-disk formats
/// carries the data on the way back in: the checkpoint, or the WAL alone.
fn seed_u64_table(dir: &Path, checkpointed: bool) {
    let store = open_store(standalone_config(dir, Durability::Consistent));
    for name in ["Alice", "Bob", "Carol"] {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: name.into(),
                age: 30,
            })
            .unwrap();
        wtx.commit().unwrap();
    }
    if checkpointed {
        store.checkpoint().unwrap();
    }
}

/// Reopen `dir` with `users` registered under `K` and return recovery's error.
fn recover_users_as<K: ultima_db::PrimaryKey>(dir: &Path) -> Error {
    let store = Store::new(standalone_config(dir, Durability::Consistent)).unwrap();
    store.register_table_keyed::<User, K>("users").unwrap();
    match store.recover() {
        Ok(()) => panic!(
            "recovery accepted a table written under a different key type \
             (reopened as {})",
            std::any::type_name::<K>()
        ),
        Err(e) => e,
    }
}

fn assert_key_type_error(err: &Error, expected: &str, found: &str) {
    let msg = err.to_string();
    assert!(
        msg.contains("primary key type mismatch"),
        "expected a key-type error, got: {msg}"
    );
    assert!(msg.contains(expected), "message must name {expected}: {msg}");
    assert!(msg.contains(found), "message must name {found}: {msg}");
}

/// A `u64`-keyed directory reopened as `String`-keyed must be refused, through
/// both on-disk formats.
///
/// Before the key-type tag this recovered `Ok` with `len == 3` and the key
/// `"\0\0\0\0\0\0\0\u{1}"` — the eight bytes of `1u64`, which are valid UTF-8.
#[test]
fn a_u64_table_reopened_as_string_keyed_is_refused() {
    for checkpointed in [false, true] {
        let dir = common::test_scratch::scratch_dir();
        seed_u64_table(dir.path(), checkpointed);
        let err = recover_users_as::<String>(dir.path());
        assert_key_type_error(&err, "String", "u64");
    }
}

/// The equal-width case, which is the one nothing else can catch: `i64::decode`
/// accepts any eight bytes, and since both encodings are order-preserving the
/// reinterpreted keys pass the ascending-order validation too. Before the tag
/// this recovered `Ok` with keys `[-9223372036854775807, ...]`.
#[test]
fn a_u64_table_reopened_as_i64_keyed_is_refused() {
    for checkpointed in [false, true] {
        let dir = common::test_scratch::scratch_dir();
        seed_u64_table(dir.path(), checkpointed);
        let err = recover_users_as::<i64>(dir.path());
        assert_key_type_error(&err, "i64", "u64");
    }
}

/// `String` and `Vec<u8>` encode *identically* — the tag is the only thing
/// that distinguishes them anywhere in either format.
#[test]
fn a_string_table_reopened_as_vec_u8_keyed_is_refused() {
    for checkpointed in [false, true] {
        let dir = common::test_scratch::scratch_dir();
        {
            let store = Store::new(standalone_config(dir.path(), Durability::Consistent)).unwrap();
            store
                .register_table_keyed::<User, String>("emails")
                .unwrap();
            store.recover().unwrap();
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table_keyed::<User, String>("emails").unwrap();
            t.put(
                "a@x.com".to_string(),
                User {
                    name: "Alice".into(),
                    age: 30,
                },
            )
            .unwrap();
            drop(t);
            wtx.commit().unwrap();
            if checkpointed {
                store.checkpoint().unwrap();
            }
        }

        let store = Store::new(standalone_config(dir.path(), Durability::Consistent)).unwrap();
        store
            .register_table_keyed::<User, Vec<u8>>("emails")
            .unwrap();
        let Err(err) = store.recover() else {
            panic!("a String-keyed table must not recover as Vec<u8>-keyed");
        };
        assert_key_type_error(&err, "Vec<u8>", "String");

        // The matching key type still recovers, so the refusal is about the
        // type and not about the data.
        let store = Store::new(standalone_config(dir.path(), Durability::Consistent)).unwrap();
        store
            .register_table_keyed::<User, String>("emails")
            .unwrap();
        store.recover().unwrap();
        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table_keyed::<User, String>("emails").unwrap();
        assert_eq!(t.len(), 1);
    }
}

// ---------------------------------------------------------------------------
// Key length cap: refused at the mutation, never acknowledged by commit
// ---------------------------------------------------------------------------

/// An over-long key must be refused *before* `commit()` acknowledges it.
///
/// The cap used to be enforced only on read, so `commit()` returned `Ok(v2)`
/// for a key no reader would accept: under `PerEntry` recovery then failed
/// permanently, and under `CoalescedPrealloc` — the mode
/// `Persistence::standalone_fast` selects — the tail-tolerant scan stopped at
/// the bad record and silently dropped the whole transaction, taking the
/// ordinary rows committed alongside it.
#[test]
fn an_over_long_key_is_refused_at_the_mutation_in_every_wal_write_mode() {
    for wal_write in [
        WalWrite::PerEntry,
        WalWrite::Coalesced,
        WalWrite::CoalescedPrealloc,
    ] {
        let dir = common::test_scratch::scratch_dir();
        let config = StoreConfig::builder()
            .persistence(Persistence::standalone(
                dir.path().to_path_buf(),
                Durability::Consistent,
                wal_write,
            ))
            .build();
        let over_long = "k".repeat(70 * 1024);

        {
            let store = Store::new(config.clone()).unwrap();
            store.register_table_keyed::<User, String>("t").unwrap();
            store.recover().unwrap();

            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table_keyed::<User, String>("t").unwrap();
            t.put(
                "a".to_string(),
                User {
                    name: "Alice".into(),
                    age: 30,
                },
            )
            .unwrap();
            let err = t
                .put(
                    over_long.clone(),
                    User {
                        name: "Huge".into(),
                        age: 1,
                    },
                )
                .expect_err("an over-long key must be refused at the mutation");
            assert!(
                matches!(err, Error::KeyTooLong { len, max, .. } if len == 70 * 1024 && max == 64 * 1024),
                "expected KeyTooLong, got: {err} ({wal_write:?})"
            );
            // The failed write left the table alone, and the transaction is
            // still usable — the co-committed rows are not collateral damage.
            assert!(t.get(&over_long).is_none());
            t.put(
                "b".to_string(),
                User {
                    name: "Bob".into(),
                    age: 40,
                },
            )
            .unwrap();
            drop(t);
            wtx.commit().unwrap();
        }

        let store = Store::new(config).unwrap();
        store.register_table_keyed::<User, String>("t").unwrap();
        store.recover().unwrap();
        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table_keyed::<User, String>("t").unwrap();
        assert_eq!(
            t.len(),
            2,
            "both ordinary rows must survive recovery ({wal_write:?})"
        );
    }
}

/// `Persistence::smr` writes checkpoints and no WAL, so it is the path where a
/// checkpoint-only cap would have gone missing. Both persistence modes refuse
/// the same key.
#[test]
fn the_key_cap_is_the_same_for_smr_and_standalone() {
    let dir = common::test_scratch::scratch_dir();
    let store = Store::new(smr_config(dir.path())).unwrap();
    store.register_table_keyed::<User, String>("t").unwrap();
    store.recover().unwrap();

    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table_keyed::<User, String>("t").unwrap();
    t.put(
        "k".repeat(70 * 1024),
        User {
            name: "Huge".into(),
            age: 1,
        },
    )
    .unwrap();
    drop(t);
    wtx.commit().unwrap();

    // No WAL to refuse it at commit, so the checkpoint is the boundary — and
    // it refuses rather than writing a file `standalone` could never have.
    let err = store
        .checkpoint()
        .expect_err("an over-long key must not reach a checkpoint");
    assert!(
        matches!(err, Error::KeyTooLong { .. }),
        "expected KeyTooLong, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Pre-0.3.0 (v1) WAL rejection
// ---------------------------------------------------------------------------

/// CRC-32/ISO-HDLC, bitwise. The WAL uses `crc32fast`, which is byte-identical;
/// spelling it out here keeps this fixture independent of the crate's internals,
/// so it stays a genuine "bytes an operator has on disk" artifact.
fn crc32_iso_hdlc(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// One complete pre-0.3.0 WAL record: `[len u32 LE][payload][crc32 u32 LE]`
/// around a v1 payload — no format header, and the row key written as a
/// bincode varint `u64` id. Hand-assembled byte by byte so it cannot drift
/// with the current encoder.
fn v1_wal_bytes() -> Vec<u8> {
    let payload: Vec<u8> = vec![
        0x01, // entry version = 1 (bincode varint)
        0x01, // op count = 1 (bincode varint u32)
        0x01, // TAG_INSERT
        0x05, b'u', b's', b'e', b'r', b's', // table name "users"
        0x01, // row id = 1 (bincode varint u64) -- the v1 key encoding
        0x02, 10, 20, // record bytes (bincode length-prefixed slice)
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    out.extend_from_slice(&crc32_iso_hdlc(&payload).to_le_bytes());
    out
}

/// A WAL written before 0.3.0 must be refused with a message that names the
/// format version and a migration path — never silently misread, and never
/// silently treated as an empty log.
///
/// Both durability/write modes are covered because they scan differently:
/// `PerEntry`/`Coalesced` recovery is strict, while `CoalescedPrealloc` is
/// tail-tolerant (an undecodable record normally means "end of log"). The
/// tolerant path is the dangerous one: without an explicit version check it
/// would report zero entries and drop every committed transaction on the floor.
#[test]
fn recovery_rejects_a_pre_0_3_0_wal() {
    for wal_write in [WalWrite::PerEntry, WalWrite::CoalescedPrealloc] {
        let dir = common::test_scratch::scratch_dir();
        std::fs::write(dir.path().join("wal.bin"), v1_wal_bytes()).unwrap();

        let config = StoreConfig::builder()
            .persistence(Persistence::standalone(
                dir.path().to_path_buf(),
                Durability::Consistent,
                wal_write,
            ))
            .build();

        // Every sink now refuses at open; recovery is the belt to that
        // suspenders. Either surface is acceptable, silence is not.
        let err = match Store::new(config) {
            Err(e) => e,
            Ok(store) => {
                store.register_table::<User>("users").unwrap();
                store
                    .recover()
                    .expect_err("a v1 WAL must not recover silently")
            }
        };
        let msg = err.to_string();
        assert!(
            msg.contains("no v2 format marker"),
            "{wal_write:?}: message was: {msg}"
        );
        assert!(
            msg.contains("Store::bulk_load"),
            "{wal_write:?}: message was: {msg}"
        );
    }
}

/// Opening a store on a pre-0.3.0 directory must fail at `Store::new`, in
/// **every** write mode — not just the preallocating one.
///
/// The append-mode sinks (`PerEntry`, `Coalesced`) do not read the file they
/// open. Before this check they let the store construct, accepted a
/// `Durability::Consistent` commit, and returned `Ok` — telling the caller the
/// write was durable — while appending v2 records behind the v1 prefix. The
/// next `recover()` then failed permanently, and the only remedy the error
/// could offer was deleting the WAL: destroying exactly the commit that had
/// just been acknowledged. `CoalescedPrealloc` happened to be safe only
/// because it scans to rebuild its write head.
///
/// The assertions that matter are (1) the store never opens, so no commit can
/// be acknowledged, and (2) the v1 file is left byte-identical, so it is still
/// readable by the 0.2.x build the migration path tells the operator to use.
#[test]
fn opening_a_store_on_a_pre_0_3_0_wal_is_refused_in_every_write_mode() {
    let modes = [
        (Durability::Consistent, WalWrite::PerEntry),
        (Durability::Consistent, WalWrite::Coalesced),
        (Durability::Consistent, WalWrite::CoalescedPrealloc),
        (Durability::Eventual, WalWrite::PerEntry),
        // The inline-fsync path builds its sink through the same `open`
        // functions, so it must refuse too.
        (Durability::ConsistentInline, WalWrite::PerEntry),
        (Durability::ConsistentInline, WalWrite::CoalescedPrealloc),
    ];

    for (durability, wal_write) in modes {
        let dir = common::test_scratch::scratch_dir();
        let wal_path = dir.path().join("wal.bin");
        std::fs::write(&wal_path, v1_wal_bytes()).unwrap();
        let before = std::fs::read(&wal_path).unwrap();

        let config = StoreConfig::builder()
            .persistence(Persistence::standalone(
                dir.path().to_path_buf(),
                durability,
                wal_write,
            ))
            .build();

        let err = Store::new(config).err().unwrap_or_else(|| {
            panic!("{durability:?}/{wal_write:?}: opened a store on a pre-0.3.0 WAL")
        });
        let msg = err.to_string();
        assert!(
            msg.contains("no v2 format marker"),
            "{durability:?}/{wal_write:?}: message was: {msg}"
        );

        assert_eq!(
            std::fs::read(&wal_path).unwrap(),
            before,
            "{durability:?}/{wal_write:?}: the pre-0.3.0 WAL was modified; \
             an operator following the migration path would find it damaged"
        );
    }
}

/// The gate must not fire on the files a healthy store legitimately produces:
/// a directory with no WAL at all, an empty WAL, and — for the preallocating
/// sink — a WAL that is nothing but zeros. All three must open and commit.
#[test]
fn the_v1_gate_does_not_reject_a_healthy_or_empty_wal() {
    for wal_write in [
        WalWrite::PerEntry,
        WalWrite::Coalesced,
        WalWrite::CoalescedPrealloc,
    ] {
        for (label, seed) in [
            ("absent", None),
            ("empty", Some(Vec::new())),
            ("preallocated zeros", Some(vec![0u8; 8192])),
        ] {
            let dir = common::test_scratch::scratch_dir();
            if let Some(bytes) = seed {
                std::fs::write(dir.path().join("wal.bin"), bytes).unwrap();
            }
            let config = StoreConfig::builder()
                .persistence(Persistence::standalone(
                    dir.path().to_path_buf(),
                    Durability::Consistent,
                    wal_write,
                ))
                .build();

            let store = Store::new(config)
                .unwrap_or_else(|e| panic!("{wal_write:?}/{label}: {e}"));
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users")
                .unwrap()
                .insert(User {
                    name: "Alice".into(),
                    age: 30,
                })
                .unwrap();
            wtx.commit()
                .unwrap_or_else(|e| panic!("{wal_write:?}/{label}: commit refused: {e}"));
        }
    }
}

// ---------------------------------------------------------------------------
// Registration identity includes the key type
// ---------------------------------------------------------------------------

/// Registering a name twice with the same record type but a different key
/// type must be refused. Before the registry's identity included the key
/// type this was a silent no-op: the second call left `Table<R, u64>`
/// closures in place, every subsequent `open_table_keyed::<R, String>`
/// succeeded in memory, and the mismatch only surfaced at `checkpoint()` as
/// an opaque "table downcast failed".
#[test]
fn register_table_keyed_rejects_a_second_key_type_for_the_same_name() {
    let dir = common::test_scratch::scratch_dir();
    let store = Store::new(
        StoreConfig::builder()
            .persistence(Persistence::smr(dir.path().to_path_buf()))
            .build(),
    )
    .unwrap();

    store.register_table_keyed::<String, u64>("t").unwrap();
    // Idempotent repeat: fine.
    store.register_table_keyed::<String, u64>("t").unwrap();
    // Same record type, different key type: refused.
    assert!(matches!(
        store.register_table_keyed::<String, String>("t"),
        Err(Error::TypeMismatch(_))
    ));
    // `register_table` is the u64 spelling of the same thing.
    store.register_table::<String>("t").unwrap();
}

/// The mismatch is caught at `open_table*` time too, on the one path where
/// nothing else pins the key type: a table that does not yet exist in the
/// base snapshot. Without the check a fresh `Table<R, u64>` would be created,
/// committed, and only rejected later by the registry's serializer.
#[test]
fn opening_a_fresh_table_under_the_wrong_key_type_errors_at_open() {
    let dir = common::test_scratch::scratch_dir();
    let store = Store::new(
        StoreConfig::builder()
            .persistence(Persistence::smr(dir.path().to_path_buf()))
            .build(),
    )
    .unwrap();
    store
        .register_table_keyed::<String, String>("emails")
        .unwrap();

    let mut wtx = store.begin_write(None).unwrap();
    assert!(
        matches!(wtx.open_table::<String>("emails"), Err(Error::TypeMismatch(_))),
        "a u64 open of a String-registered table must fail at open time"
    );
    // The correctly-keyed open works, and checkpoints without complaint.
    let mut t = wtx.open_table_keyed::<String, String>("emails").unwrap();
    t.put("a@x.com".to_string(), "A".to_string()).unwrap();
    drop(t);
    wtx.commit().unwrap();
    store.checkpoint().unwrap();
}

/// The registry may not disagree with the live data. Registration does not
/// have to come first, so a table can already exist under one key type when
/// `register_table*` is called with another; recording that registration left
/// the registry and the snapshot pointing at different `K`, and everything
/// downstream that trusts the registry — notably the snapshot wire format —
/// then acted on the wrong type. Reproduces the emit-side corruption:
/// `register_table::<Row>` after a `String` create used to return `Ok(())`,
/// after which `snapshot_stream` happily emitted the eight bytes of the key
/// `"abcdefgh"` reinterpreted as the u64 `7017280452245743464`.
#[test]
fn register_table_after_a_differently_keyed_table_exists_is_refused() {
    let dir = common::test_scratch::scratch_dir();
    let store = Store::new(
        StoreConfig::builder()
            .persistence(Persistence::smr(dir.path().to_path_buf()))
            .build(),
    )
    .unwrap();

    // A String-keyed table created without ever being registered.
    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table_keyed::<String, String>("t").unwrap();
    t.put("abcdefgh".to_string(), "v".to_string()).unwrap();
    drop(t);
    wtx.commit().unwrap();

    // Registering it as u64 must now be refused, not silently accepted.
    assert!(matches!(
        store.register_table::<String>("t"),
        Err(Error::TypeMismatch(_))
    ));
    assert!(matches!(
        store.register_table_keyed::<String, u32>("t"),
        Err(Error::TypeMismatch(_))
    ));

    // The matching registration is accepted, and the store checkpoints —
    // the state the mismatched registration could never have reached.
    store.register_table_keyed::<String, String>("t").unwrap();
    store.checkpoint().unwrap();
}

/// The mirror of the above for a name that does *not* exist yet: registration
/// before creation is unconstrained, and it is `open_table_keyed` that then
/// holds the new table to the registration.
#[test]
fn registering_before_the_table_exists_still_pins_the_key_type() {
    let dir = common::test_scratch::scratch_dir();
    let store = Store::new(
        StoreConfig::builder()
            .persistence(Persistence::smr(dir.path().to_path_buf()))
            .build(),
    )
    .unwrap();
    store.register_table_keyed::<String, String>("t").unwrap();

    let mut wtx = store.begin_write(None).unwrap();
    assert!(matches!(
        wtx.open_table::<String>("t"),
        Err(Error::TypeMismatch(_))
    ));
    // Re-registering the same pair stays idempotent once the table exists.
    let mut t = wtx.open_table_keyed::<String, String>("t").unwrap();
    t.put("k".to_string(), "v".to_string()).unwrap();
    drop(t);
    wtx.commit().unwrap();
    store.register_table_keyed::<String, String>("t").unwrap();
}
