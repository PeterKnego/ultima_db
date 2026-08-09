# Format-compatibility fixtures

Golden checkpoint bytes produced by *actual released builds* of UltimaDB, not
reconstructions. A hand-rolled fixture encodes what we believe an old format
was — which is exactly the belief these fixtures exist to check.

## Rule

> A future on-disk format bump adds a fixture for the version it supersedes,
> **in the same commit that bumps it**. A compatibility guarantee with no
> committed old-format bytes silently lapses on the next refactor.

## Provenance

| Fixture | Source | Ref |
| --- | --- | --- |
| `v0_2_0/checkpoint_2.bin` | `ultima-db` 0.2.0 release tree | commit `5cfae9a198268b16d7ace13ad6bc8529923dd005` ("chore(release): 0.2.0") |
| `v0_3_0/checkpoint_2.bin` | `ultima-db` 0.3.0 release tree | tag `v0.3.0` (commit `6bd5c3f45343fdbe21658061c14762eb34c7b852`) |

Only `checkpoint_2.bin` is committed from each generated directory.
`wal.bin` is deliberately **not** included — WAL compatibility is out of
scope for this whole feature, and shipping a WAL fixture would imply
coverage that does not exist.

## Row shape

Both fixtures were produced against the same `User` record and the same two
writes, so later tests can assert against a fixed shape:

```rust
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct User { name: String, age: u32 }
```

Table name `"users"`, rows `(1, "alice", 30)` and `(2, "bob", 25)` (in that
insertion order), `next_id = 3`, checkpointed at snapshot version `2`
(hence the `checkpoint_2.bin` filename).

## Regenerating (only needed if a fixture is lost or a new old-version
fixture is required for a future format bump)

```bash
SC=$(mktemp -d)
git worktree add "$SC/v020" 5cfae9a
git worktree add "$SC/v030" v0.3.0
```

In **each** worktree write `examples/genfix.rs`:

```rust
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct User { name: String, age: u32 }

fn main() {
    let dir = std::env::args().nth(1).unwrap();
    let cfg = StoreConfig::builder()
        .persistence(Persistence::standalone(
            std::path::PathBuf::from(&dir), Durability::Consistent, WalWrite::PerEntry))
        .build();
    let store = Store::new(cfg).unwrap();
    store.register_table::<User>("users").unwrap();
    store.recover().unwrap();
    for (n, a) in [("alice", 30u32), ("bob", 25)] {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users").unwrap()
            .insert(User { name: n.into(), age: a }).unwrap();
        wtx.commit().unwrap();
    }
    let v = store.checkpoint().unwrap();
    println!("checkpointed v{v}");
}
```

Then, per worktree (separate `CARGO_TARGET_DIR` so the two builds cannot
share artifacts):

```bash
cd "$SC/v020" && CARGO_TARGET_DIR="$SC/t020" cargo run --features persistence --example genfix -- "$SC/fix020"
cd "$SC/v030" && CARGO_TARGET_DIR="$SC/t030" cargo run --features persistence --example genfix -- "$SC/fix030"
```

Copy **only** `checkpoint_2.bin` from each into the fixture directories
under `tests/fixtures/formats/`. Do not copy `wal.bin`.

Clean up: `git worktree remove "$SC/v020"` and likewise for `v030`.

## Expected leading bytes

Verify with `xxd`:

```bash
xxd tests/fixtures/formats/v0_2_0/checkpoint_2.bin | head -3
xxd tests/fixtures/formats/v0_3_0/checkpoint_2.bin | head -6
```

`v0_2_0/checkpoint_2.bin` must begin `554c 4442 0102 0105 7573 6572 7310 03…`
`v0_3_0/checkpoint_2.bin` must begin `554c 4442 0102 0105 7573 6572 7347 ff02…`

If a regenerated fixture differs from these, **stop and report** — it means
generation used the wrong commit or the record shape drifted. Do not
"reconcile" a mismatch by hand-editing bytes; that defeats the entire
purpose of the fixture.
