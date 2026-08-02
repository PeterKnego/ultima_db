# How to find records by fields other than the primary key

If you look rows up by a field — an email, a region, a status — define a
secondary index on it. Indexes are maintained automatically on every
insert, update, and delete, and are versioned with the table, so a reader
on an old snapshot sees the matching index state.

## Define an index

`define_index` takes a name, an `IndexKind`, and a key-extractor closure.
Use `Unique` when the field identifies at most one row, `NonUnique`
otherwise:

```rust
use ultima_db::{IndexKind, Store};

#[derive(Clone)]
struct Account {
    email: String,
    region: String,
    balance: i64,
}

let store = Store::default();
let mut wtx = store.begin_write(None)?;
let mut accounts = wtx.open_table::<Account>("accounts")?;

accounts.define_index("by_email", IndexKind::Unique, |a: &Account| a.email.clone())?;
accounts.define_index("by_region", IndexKind::NonUnique, |a: &Account| a.region.clone())?;
```

If the table already has data, the index is backfilled when defined — no
separate rebuild step. Backfill fails with `Error::DuplicateKey` if
existing rows violate a unique constraint.

## Query

```rust
let rtx = store.begin_read(None)?;
let accounts = rtx.open_table::<Account>("accounts")?;

// Unique index: at most one hit.
let alice: Option<(u64, &Account)> =
    accounts.get_unique("by_email", &"alice@example.com".to_string())?;

// Non-unique index: all matching rows.
let eu: Vec<(u64, &Account)> =
    accounts.get_by_index("by_region", &"eu".to_string())?;

// Either kind, when the call site doesn't care which.
let hits = accounts.get_by_key("by_email", &"alice@example.com".to_string())?;

// Range scan over index keys (works on both kinds).
let a_to_m = accounts.index_range("by_email", "a".to_string().."n".to_string())?;
```

The index key type at the query site must match the extractor's return
type exactly — a mismatch is a runtime `IndexTypeMismatch`, not a compile
error.

## Unique constraint violations

Writing a row whose extracted key already maps to a *different* row fails
with `Error::DuplicateKey` and the mutation does not happen. Batch
operations (`insert_batch`, `update_batch`) roll back the whole batch —
either every row lands or none do.

## Full-text search

For token queries over free text, enable the `fulltext` cargo feature (see
the [cargo features reference](../reference/cargo-features.md)) and attach
the built-in `FullTextIndex`, which is a custom index whose extractor
returns the text to index:

```rust
use ultima_db::FullTextIndex;

#[derive(Clone)]
struct Article {
    title: String,
    body: String,
}

let mut articles = wtx.open_table::<Article>("articles")?;
articles.define_custom_index(
    "search",
    FullTextIndex::new(|a: &Article| format!("{} {}", a.title, a.body)),
)?;
```

Query it by downcasting the named index back to its concrete type; results
are BM25-ranked `SearchResult`s, best first:

```rust
let articles = rtx.open_table::<Article>("articles")?;
let idx = articles.custom_index::<FullTextIndex<Article>>("search")?;
for hit in idx.search("rust") {
    println!("id={} score={:.3}", hit.id, hit.score);
}
```

Scores are only comparable within one query's result list.

## Custom indexes

When neither shape fits — a bitmap, a computed aggregate, a filtered id
set — implement `CustomIndex` yourself, backed by the public `BTree` so
clones stay O(1) and snapshots stay cheap:

```rust
use ultima_db::{BTree, CustomIndex, Result};

/// Ids of accounts with a negative balance.
#[derive(Clone)]
struct Overdrawn {
    ids: BTree<u64, ()>,
}

impl CustomIndex<Account> for Overdrawn {
    fn on_insert(&mut self, id: u64, a: &Account) -> Result<()> {
        if a.balance < 0 {
            self.ids = self.ids.insert(id, ());
        }
        Ok(())
    }
    fn on_update(&mut self, id: u64, _old: &Account, new: &Account) -> Result<()> {
        self.on_delete(id, new);
        self.on_insert(id, new)
    }
    fn on_delete(&mut self, id: u64, _a: &Account) {
        if let Ok(rest) = self.ids.remove(&id) {
            self.ids = rest;
        }
    }
}
```

Register with `define_custom_index("overdrawn", Overdrawn { .. })` and read
back with `custom_index::<Overdrawn>("overdrawn")`. Returning `Err` from
`on_insert`/`on_update` vetoes the mutation, so custom indexes double as
constraints. `CustomIndex<R, K>` is generic over the row key, so this works
on non-`u64`-keyed tables too (see
[How to use natural primary keys](use-natural-primary-keys.md)).

## Index DDL under MultiWriter

If the store runs `WriterMode::MultiWriter`, define indexes in their own
transaction. A transaction that defines an index on a table another writer
concurrently committed to fails with `Error::IndexDdlConflict`; retry the
DDL on its own. See
[How to handle write conflicts](handle-write-conflicts.md).
