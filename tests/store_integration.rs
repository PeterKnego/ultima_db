use ultima_db::{Error, Store};

#[test]
fn end_to_end_crud() {
    let mut store = Store::new();
    let table = store.open_table::<String>("notes").unwrap();

    // Insert
    let id1 = table.insert("first note".to_string());
    let id2 = table.insert("second note".to_string());
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);

    // Get
    assert_eq!(table.get(id1), Some(&"first note".to_string()));
    assert_eq!(table.get(id2), Some(&"second note".to_string()));
    assert_eq!(table.get(99), None);

    // Update
    table.update(id1, "updated first".to_string()).unwrap();
    assert_eq!(table.get(id1), Some(&"updated first".to_string()));
    assert!(matches!(
        table.update(99, "x".to_string()),
        Err(Error::KeyNotFound)
    ));

    // Delete
    table.delete(id2).unwrap();
    assert_eq!(table.get(id2), None);
    assert_eq!(table.len(), 1);
    assert!(matches!(table.delete(99), Err(Error::KeyNotFound)));

    // Range scan — id2 was deleted, so only id1 and id3 appear
    let id3 = table.insert("third note".to_string());
    let results: Vec<_> = table.range(id1..=id3).collect();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (id1, &"updated first".to_string()));
    assert_eq!(results[1], (id3, &"third note".to_string()));
}

#[test]
fn two_tables_are_independent() {
    let mut store = Store::new();

    // Use statement-level borrows (temporary &mut ends at each semicolon)
    store.open_table::<String>("users").unwrap().insert("alice".to_string());
    store.open_table::<u64>("posts").unwrap().insert(42u64);

    {
        let users = store.open_table::<String>("users").unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users.get(1), Some(&"alice".to_string()));
    }

    {
        let posts = store.open_table::<u64>("posts").unwrap();
        assert_eq!(posts.get(1), Some(&42u64));
    }
}
