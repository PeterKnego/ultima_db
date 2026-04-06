use ultima_db::Store;

fn main() {
    let store = Store::default();

    // Write transaction: insert, update, delete
    let version = {
        let mut wtx = store.begin_write(None).unwrap();
        let mut table = wtx.open_table::<String>("notes").unwrap();

        let id = table.insert("Hello, UltimaDB!".to_string()).unwrap();
        println!("Inserted note with id={id}");

        if let Some(note) = table.get(id) {
            println!("Retrieved: {note}");
        }

        table.update(id, "Hello, updated!".to_string()).unwrap();
        println!("Updated note: {}", table.get(id).unwrap());

        table.delete(id).unwrap();
        println!("Deleted. Table empty: {}", table.is_empty());

        wtx.commit().unwrap()
    };

    println!("Committed as version {version}");

    // Read transaction: verify state
    let rtx = store.begin_read(None).unwrap();
    let table = rtx.open_table::<String>("notes").unwrap();
    println!("Notes after commit: {} record(s)", table.len());
}
