use ultima_db::{BulkLoadInput, BulkLoadOptions, BulkSource, Store};

#[test]
fn bulk_load_replace_on_empty_store_creates_table() {
    let store = Store::default();
    let rows: Vec<(u64, String)> = (1u64..=1000).map(|i| (i, format!("v{i}"))).collect();
    let input = BulkLoadInput::Replace(BulkSource::sorted_vec(rows));
    let v = store
        .bulk_load::<String>("t", input, BulkLoadOptions::default())
        .unwrap();
    assert!(v >= 1);

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.get(1).map(String::as_str), Some("v1"));
    assert_eq!(t.get(1000).map(String::as_str), Some("v1000"));
    assert_eq!(t.len(), 1000);
}
