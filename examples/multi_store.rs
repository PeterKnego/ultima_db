use ultima_db::Store;

fn main() {
    let store = Store::default();

    // Populate two typed tables in separate write transactions
    {
        let mut wtx = store.begin_write(None).unwrap();
        let users = wtx.open_table::<String>("users").unwrap();
        users.insert("alice".to_string()).unwrap();
        users.insert("bob".to_string()).unwrap();
        wtx.commit().unwrap();
    }
    {
        let mut wtx = store.begin_write(None).unwrap();
        let scores = wtx.open_table::<u32>("scores").unwrap();
        scores.insert(100u32).unwrap();
        scores.insert(200u32).unwrap();
        wtx.commit().unwrap();
    }

    // Read both tables from the latest snapshot
    let rtx = store.begin_read(None).unwrap();

    println!("users table:");
    for (id, name) in rtx.open_table::<String>("users").unwrap().range(..) {
        println!("  {id}: {name}");
    }

    println!("scores table:");
    for (id, score) in rtx.open_table::<u32>("scores").unwrap().range(..) {
        println!("  {id}: {score}");
    }
}
