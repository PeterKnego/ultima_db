# How to use natural primary keys instead of auto-increment ids

If the real identifier of a row is an email address, a UUID string, a byte
key, or a `(tenant, id)` pair, key the table by it directly instead of
storing it in a unique secondary index next to a surrogate `u64`. Any
`PrimaryKey` type works: `u8`–`u128`, `i8`–`i128`, `String`, `Vec<u8>`, and
2-/3-tuples of those (see the
[key encoding reference](../reference/key-encoding-and-formats.md)).

## Open the table with an explicit key type

Use `open_table_keyed::<R, K>` on both transaction types. (`open_table`
stays `u64`-only for signature-compatibility reasons; it is the same table
machinery underneath.)

```rust
use ultima_db::Store;

#[derive(Clone)]
struct User {
    display_name: String,
    country: String,
}

let store = Store::default();
let mut wtx = store.begin_write(None)?;
let mut users = wtx.open_table_keyed::<User, String>("users")?;
```

## Write with `put`, not `insert`

A keyed table has no auto-increment counter — the store cannot invent the
next `String`. So `insert` and `insert_batch` exist only on `u64`-keyed
tables; on everything else, supply the key yourself:

```rust
users.put(
    "alice@example.com".to_string(),
    User { display_name: "Alice".into(), country: "SI".into() },
)?;
drop(users);
wtx.commit()?;
```

`put` upserts: writing an existing key replaces the record.

## Point reads take `&String`, not `&str`

Reads accept `impl Borrow<K>`, and the standard library provides
`String: Borrow<str>` — not the direction you need here. So
`users.get("alice@example.com")` does not compile; pass a `&String` or an
owned `String`. This is the inverse of `HashMap<String, _>::get` and the
most common surprise when moving off `u64` keys:

```rust
let rtx = store.begin_read(None)?;
let users = rtx.open_table_keyed::<User, String>("users")?;

let key = "alice@example.com".to_string();
let alice = users.get(&key);        // Some(&User)
// users.get("alice@example.com")  // does not compile
```

## Ordered iteration and range scans

`iter` and `range` come free and yield keys in `Ord` order — the key
encoding is order-preserving, so memory, WAL, and checkpoints all agree on
the order (see the
[key encoding reference](../reference/key-encoding-and-formats.md)):

```rust
for (email, user) in users.iter() {
    println!("{email}: {}", user.display_name);
}

// Everyone whose email sorts in ["a", "c"):
let head: Vec<String> = users
    .range("a".to_string().."c".to_string())
    .map(|(k, _)| k)
    .collect();
```

## Composite keys and prefix scans

If rows belong to a parent — events per user, rows per tenant — use a tuple
key. Tuples sort element by element, so a parent's rows are contiguous and
a range over the first element is a prefix scan:

```rust
#[derive(Clone)]
struct Event {
    kind: String,
}

let mut wtx = store.begin_write(None)?;
let mut events = wtx.open_table_keyed::<Event, (String, u64)>("events")?;
events.put((key.clone(), 1), Event { kind: "login".into() })?;
events.put((key.clone(), 2), Event { kind: "purchase".into() })?;
drop(events);
wtx.commit()?;

let rtx = store.begin_read(None)?;
let events = rtx.open_table_keyed::<Event, (String, u64)>("events")?;
let alices: Vec<(u64, String)> = events
    .range((key.clone(), 0)..=(key.clone(), u64::MAX))
    .map(|((_, seq), e)| (seq, e.kind.clone()))
    .collect();
```

## Secondary indexes work unchanged

Index key and row key are independent types, so a `String`-keyed table can
carry any index (see
[How to find records by fields other than the primary key](query-with-indexes.md)):

```rust
let mut wtx = store.begin_write(None)?;
let mut users = wtx.open_table_keyed::<User, String>("users")?;
users.define_index("by_country", ultima_db::IndexKind::NonUnique, |u: &User| {
    u.country.clone()
})?;
drop(users);
wtx.commit()?;

let rtx = store.begin_read(None)?;
let users = rtx.open_table_keyed::<User, String>("users")?;
let slovenians = users.get_by_index("by_country", &"SI".to_string())?;
```

## Under persistence, register with the key type

If the store persists, register each keyed table with
`register_table_keyed::<R, K>` before `recover()` or `checkpoint()` — the
plain `register_table` registers a `u64`-keyed table and recovery will
reject the key-type mismatch:

```rust
store.register_table_keyed::<User, String>("users")?;
store.recover()?;
```

See [How to set up durable persistence](set-up-durable-persistence.md).

## Limits and custom key types

- Encoded keys are capped at 64 KiB; oversized keys fail the mutation with
  `Error::KeyTooLong` (see the
  [key encoding reference](../reference/key-encoding-and-formats.md)).
- If you implement `PrimaryKey` for your own type, its encoding must be
  order-preserving and it must declare a unique, never-changing
  `KEY_TYPE_ID` — the reserved ranges are listed in the
  [key encoding reference](../reference/key-encoding-and-formats.md).
