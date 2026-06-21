#![cfg(feature = "persistence")]
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite, WriterMode};
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
