use ultima_db::Store;

fn main() {
    let mut store = Store::new();

    store.open_table::<String>("users").unwrap().insert("alice".to_string());
    store.open_table::<String>("users").unwrap().insert("bob".to_string());
    store.open_table::<u32>("scores").unwrap().insert(100u32);
    store.open_table::<u32>("scores").unwrap().insert(200u32);

    println!("users table:");
    for (id, name) in store.open_table::<String>("users").unwrap().range(..) {
        println!("  {id}: {name}");
    }

    println!("scores table:");
    for (id, score) in store.open_table::<u32>("scores").unwrap().range(..) {
        println!("  {id}: {score}");
    }
}
