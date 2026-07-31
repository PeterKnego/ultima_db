// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Example: a table keyed by `String` instead of an auto-increment `u64`.
//!
//! Shows `open_table_keyed` end to end — write, point read, ordered
//! iteration, range scan, and a secondary index over a non-`u64` primary
//! key — plus a composite `(String, u64)`-keyed table, since tuple keys sort
//! element by element and make prefix scans natural.
//!
//! Run: `cargo run --example string_keyed_table`

use ultima_db::{IndexKind, Store};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
struct User {
    display_name: String,
    country: String,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
struct Event {
    kind: String,
}

fn user(display_name: &str, country: &str) -> User {
    User {
        display_name: display_name.to_string(),
        country: country.to_string(),
    }
}

fn main() {
    let store = Store::default();

    // ----- Write: the key is supplied, not generated -----
    //
    // A `String`-keyed table has no auto-increment counter, so there is no
    // `insert` on its writer — only `put(key, record)`.
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut users = wtx.open_table_keyed::<User, String>("users").unwrap();

        // A non-unique secondary index over a `String`-keyed table: the index
        // key is `String` (the country) and the *row* key it points back to is
        // also `String`. Neither has to be the table's primary key type.
        users
            .define_index("by_country", IndexKind::NonUnique, |u: &User| {
                u.country.clone()
            })
            .unwrap();

        users
            .put("carol@example.com".to_string(), user("Carol", "SI"))
            .unwrap();
        users
            .put("alice@example.com".to_string(), user("Alice", "SI"))
            .unwrap();
        users
            .put("bob@example.com".to_string(), user("Bob", "DE"))
            .unwrap();

        drop(users);
        let v = wtx.commit().unwrap();
        println!("committed 3 users at version {v}");
    }

    let rtx = store.begin_read(None).unwrap();
    let users = rtx.open_table_keyed::<User, String>("users").unwrap();

    // ----- Point read -----
    //
    // Note the `&`: reads take `impl Borrow<K>`, and `&str` does *not*
    // implement `Borrow<String>` (it is `String: Borrow<str>` that holds, the
    // other direction). So `users.get("alice@example.com")` does not compile —
    // pass a `&String`, or an owned `String`, instead. This is the inverse of
    // `HashMap<String, _>::get`, which takes `&str`.
    let alice = "alice@example.com".to_string();
    println!("get({alice}) -> {:?}", users.get(&alice).map(|u| &u.display_name));

    // ----- Ordered iteration -----
    //
    // Keys come back in `Ord` order, which for `String` is lexicographic by
    // UTF-8 bytes. The order is the same on disk: key encoding is
    // order-preserving, so WAL replay and bulk loads see the same sequence.
    println!("all users, in key order:");
    for (email, u) in users.iter() {
        println!("  {email} -> {} ({})", u.display_name, u.country);
    }

    // ----- Range scan over string keys -----
    let head = "a".to_string().."c".to_string();
    let in_range: Vec<String> = users.range(head).map(|(k, _)| k).collect();
    println!("keys in [\"a\", \"c\"): {in_range:?}");

    // ----- Secondary index lookup: index key -> primary keys -----
    let si = "SI".to_string();
    let mut slovenians: Vec<String> = users
        .get_by_index("by_country", &si)
        .unwrap()
        .into_iter()
        .map(|(email, _)| email)
        .collect();
    slovenians.sort();
    println!("by_country[\"SI\"] -> {slovenians:?}");
    drop(rtx);

    // ----- Composite keys -----
    //
    // A tuple key sorts element by element, so all of one user's events are
    // contiguous and a prefix range reads them in sequence order.
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut events = wtx
            .open_table_keyed::<Event, (String, u64)>("events")
            .unwrap();
        for (who, seq, kind) in [
            (&alice, 1u64, "login"),
            (&alice, 2, "purchase"),
            (&alice, 3, "logout"),
        ] {
            events
                .put(
                    (who.clone(), seq),
                    Event {
                        kind: kind.to_string(),
                    },
                )
                .unwrap();
        }
        drop(events);
        wtx.commit().unwrap();
    }

    let rtx = store.begin_read(None).unwrap();
    let events = rtx
        .open_table_keyed::<Event, (String, u64)>("events")
        .unwrap();
    let alice_events: Vec<String> = events
        .range((alice.clone(), 0u64)..(alice.clone(), u64::MAX))
        .map(|((_, seq), e)| format!("{seq}:{}", e.kind))
        .collect();
    println!("events for {alice}: {alice_events:?}");

    assert_eq!(users_len(&store), 3);
    assert_eq!(alice_events.len(), 3);
    println!("done");
}

fn users_len(store: &Store) -> usize {
    let rtx = store.begin_read(None).unwrap();
    rtx.open_table_keyed::<User, String>("users").unwrap().len()
}
