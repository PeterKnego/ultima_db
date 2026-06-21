#![cfg(feature = "persistence")]
use ultima_db::{Durability, Error, Persistence, Store, StoreConfig, WalWrite, WriterMode};
use serde::{Deserialize, Serialize};

// `Record` is blanket-impl'd for any Serialize + DeserializeOwned type — do NOT
// add `impl Record for Row {}` (conflicting impl).
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct Row { v: u64 }

fn cfg(dir: &std::path::Path, wm: WriterMode) -> StoreConfig {
    StoreConfig {
        writer_mode: wm,
        persistence: Persistence::Standalone {
            dir: dir.to_path_buf(),
            durability: Durability::ConsistentInline,
            wal_write: WalWrite::PerEntry,
        },
        ..StoreConfig::default()
    }
}

#[test]
fn inline_singlewriter_commits_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::new(cfg(dir.path(), WriterMode::SingleWriter)).unwrap();
        store.register_table::<Row>("rows").unwrap();
        for v in 1..=20u64 {
            let mut wtx = store.begin_write(None).unwrap();
            { let mut t = wtx.open_table::<Row>("rows").unwrap(); t.insert(Row { v }).unwrap(); }
            wtx.commit().unwrap();
        }
    } // drop = crash
    let store = Store::new(cfg(dir.path(), WriterMode::SingleWriter)).unwrap();
    store.register_table::<Row>("rows").unwrap();
    store.recover().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<Row>("rows").unwrap();
    for v in 1..=20u64 {
        assert_eq!(t.get(v).map(|r| r.v), Some(v), "acked inline commit {v} lost");
    }
}

/// Mirror of `recovery_fails_cleanly_when_commits_follow_uncheckpointed_bulk_load`
/// (persistence_integration.rs) for the ConsistentInline backend.
///
/// Before the fix, `bulk_load` discarded the `InlineSync` waiter, so the
/// `WalOp::BulkLoad` marker was never written under the inline backend.
/// Recovery would then replay the post-load commit on top of the pre-load
/// state instead of failing with `BulkLoadNotCheckpointed`.
#[test]
fn inline_bulk_load_marker_is_durable_recovery_fails_cleanly() {
    use ultima_db::{BulkLoadInput, BulkLoadOptions, BulkSource};

    let dir = tempfile::tempdir().unwrap();
    let config = cfg(dir.path(), WriterMode::SingleWriter);

    {
        let store = Store::new(config.clone()).unwrap();
        store.register_table::<Row>("rows").unwrap();

        // Seed commit (WAL v1).
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<Row>("rows").unwrap().insert(Row { v: 1 }).unwrap();
        wtx.commit().unwrap();

        // Bulk load without a checkpoint.
        let rows: Vec<(u64, Row)> = (1..=3).map(|i| (i, Row { v: i })).collect();
        store
            .bulk_load::<Row>(
                "rows",
                BulkLoadInput::Replace(BulkSource::sorted_vec(rows)),
                BulkLoadOptions { checkpoint_after: false, ..BulkLoadOptions::default() },
            )
            .unwrap();

        // A normal commit on top of the loaded state.
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<Row>("rows").unwrap().insert(Row { v: 99 }).unwrap();
        wtx.commit().unwrap();
        // Store dropped: WAL flushed (inline — already durable per commit).
    }

    let store = Store::new(config).unwrap();
    store.register_table::<Row>("rows").unwrap();
    let res = store.recover();
    assert!(
        matches!(res, Err(Error::BulkLoadNotCheckpointed { .. })),
        "recovery across an uncheckpointed inline bulk load with later commits \
         must fail cleanly, got {res:?}"
    );
}

#[test]
fn inline_multiwriter_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let err = Store::new(cfg(dir.path(), WriterMode::MultiWriter));
    match err {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("requires WriterMode::SingleWriter"),
                "expected SingleWriter-required error, got: {msg}"
            );
        }
        Ok(_) => panic!("ConsistentInline + MultiWriter must error"),
    }
}
