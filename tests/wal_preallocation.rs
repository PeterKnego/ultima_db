#![cfg(feature = "persistence")]
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct Row { v: u64 }

fn cfg(dir: &std::path::Path) -> StoreConfig {
    StoreConfig {
        persistence: Persistence::Standalone {
            dir: dir.to_path_buf(),
            durability: Durability::Consistent,
            wal_write: WalWrite::CoalescedPrealloc,
        },
        ..StoreConfig::default()
    }
}

#[test]
fn prealloc_store_commits_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::new(cfg(dir.path())).unwrap();
        store.register_table::<Row>("rows").unwrap();
        let mut wtx = store.begin_write(None).unwrap();
        { let mut t = wtx.open_table::<Row>("rows").unwrap(); t.insert(Row { v: 42 }).unwrap(); }
        wtx.commit().unwrap();
    }
    // Reopen and recover.
    let store = Store::new(cfg(dir.path())).unwrap();
    store.register_table::<Row>("rows").unwrap();
    store.recover().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get(1).map(|r| r.v), Some(42));
}
