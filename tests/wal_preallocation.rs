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
fn prealloc_store_recovers_through_torn_preallocated_tail() {
    use std::io::{Seek, SeekFrom, Write};
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::new(cfg(dir.path())).unwrap();
        store.register_table::<Row>("rows").unwrap();
        let mut wtx = store.begin_write(None).unwrap();
        { let mut t = wtx.open_table::<Row>("rows").unwrap(); t.insert(Row { v: 7 }).unwrap(); }
        wtx.commit().unwrap();
    }
    // Append a garbage "record" into the preallocated zero tail to simulate a
    // torn write that looks complete (non-zero len prefix + junk + zeros).
    let wal = dir.path().join("wal.bin");
    let durable = ultima_db::wal_durable_len_for_test(&wal); // helper exposed below
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&wal).unwrap();
    f.seek(SeekFrom::Start(durable)).unwrap();
    f.write_all(&[16u8, 0, 0, 0]).unwrap(); // len=16
    f.write_all(&[0xABu8; 16]).unwrap();     // junk body (CRC will mismatch)
    f.write_all(&[0u8, 0, 0, 0]).unwrap();   // junk crc
    f.sync_all().unwrap();
    drop(f);

    let store = Store::new(cfg(dir.path())).unwrap();
    store.register_table::<Row>("rows").unwrap();
    store.recover().unwrap(); // must NOT error on the torn tail
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get(1).map(|r| r.v), Some(7));
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
