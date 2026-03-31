use ultima_db::Store;

fn main() {
    let mut store = Store::new();
    let table = store.open_table::<String>("notes").unwrap();

    let id = table.insert("Hello, UltimaDB!".to_string());
    println!("Inserted note with id={id}");

    if let Some(note) = table.get(id) {
        println!("Retrieved: {note}");
    }

    table.update(id, "Hello, updated!".to_string()).unwrap();
    println!("Updated note: {}", table.get(id).unwrap());

    table.delete(id).unwrap();
    println!("Deleted. Table empty: {}", table.is_empty());
}
