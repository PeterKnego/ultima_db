// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Integration test verifying the custom index public API.

use ultima_db::{BTree, CustomIndex, Table, Result, Store};

/// A simple ID-set index backed by the public BTree.
#[derive(Clone)]
struct IdSetIndex {
    ids: BTree<u64, ()>,
}

impl IdSetIndex {
    fn new() -> Self {
        Self { ids: BTree::new() }
    }

    fn contains(&self, id: u64) -> bool {
        self.ids.get(&id).is_some()
    }

    fn len(&self) -> usize {
        self.ids.len()
    }
}

impl CustomIndex<String> for IdSetIndex {
    fn on_insert(&mut self, id: u64, _record: &String) -> Result<()> {
        self.ids = self.ids.insert(id, ());
        Ok(())
    }

    fn on_update(&mut self, _id: u64, _old: &String, _new: &String) -> Result<()> {
        Ok(())
    }

    fn on_delete(&mut self, id: u64, _record: &String) {
        if let Ok(new_ids) = self.ids.remove(&id) {
            self.ids = new_ids;
        }
    }
}

#[test]
fn custom_index_public_api_with_btree() {
    let mut table = Table::<String>::new();
    table.define_custom_index("id_set", IdSetIndex::new()).unwrap();

    let id1 = table.insert("hello".to_string()).unwrap();
    let id2 = table.insert("world".to_string()).unwrap();

    let idx = table.custom_index::<IdSetIndex>("id_set").unwrap();
    assert!(idx.contains(id1));
    assert!(idx.contains(id2));
    assert!(!idx.contains(999));
    assert_eq!(idx.len(), 2);

    // Test resolve
    let results = table.resolve(&[id1, id2, 999]);
    assert_eq!(results.len(), 2);
}

#[test]
fn custom_index_through_write_tx() {
    let store = Store::default();

    // First transaction: create table with custom index and insert data
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<String>("docs").unwrap();
        table.define_custom_index("id_set", IdSetIndex::new()).unwrap();
        table.insert("hello".to_string()).unwrap();
        table.insert("world".to_string()).unwrap();
    }
    wtx.commit().unwrap();

    // Read transaction: verify custom index is accessible
    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<String>("docs").unwrap();
    let idx = table.custom_index::<IdSetIndex>("id_set").unwrap();
    assert_eq!(idx.len(), 2);
    assert!(idx.contains(1));
    assert!(idx.contains(2));
}

#[test]
fn custom_index_snapshot_isolation() {
    let store = Store::default();

    // Transaction 1: create table with custom index
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut table = wtx.open_table::<String>("docs").unwrap();
        table.define_custom_index("id_set", IdSetIndex::new()).unwrap();
        table.insert("v1".to_string()).unwrap();
    }
    wtx.commit().unwrap();

    // Take a read snapshot at version 1
    let rtx_v1 = store.begin_read(None).unwrap();

    // Transaction 2: insert more data
    let mut wtx2 = store.begin_write(None).unwrap();
    {
        let mut table = wtx2.open_table::<String>("docs").unwrap();
        table.insert("v2".to_string()).unwrap();
    }
    wtx2.commit().unwrap();

    // Read snapshot at v1 should still see only 1 record
    let table_v1 = rtx_v1.open_table::<String>("docs").unwrap();
    let idx_v1 = table_v1.custom_index::<IdSetIndex>("id_set").unwrap();
    assert_eq!(idx_v1.len(), 1);

    // Latest read should see 2 records
    let rtx_v2 = store.begin_read(None).unwrap();
    let table_v2 = rtx_v2.open_table::<String>("docs").unwrap();
    let idx_v2 = table_v2.custom_index::<IdSetIndex>("id_set").unwrap();
    assert_eq!(idx_v2.len(), 2);
}
