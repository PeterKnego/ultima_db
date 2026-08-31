# Paged pre-block-decode fixture

Real bytes produced by an actual pre-Task-4 build of the paged B-tree
(commit `33f59a8b66a9620bd79844609515c25a9ad31ca7`, the tip of
`feat/paged-value-blocks` immediately before `NodeCodec::decode` was
changed to build value blocks for `DataLeaf` pages) — not a
reconstruction. Same task62 discipline as `tests/fixtures/formats/`: a
hand-rolled fixture would encode what we *believe* the pre-change bytes
looked like, which is exactly the belief this fixture exists to check.

Used by `tests/paged_format_compat.rs::prechange_directory_recovers_and_reads`
to prove that decode-to-block (Task 4) reads a directory written by the
prior all-Arc-leaf decode/checkpoint path without any format change — the
wire format for `DataLeaf` payloads is unchanged; only the in-memory
representation the decoder builds from those bytes changes.

## Contents

`pages.bin`, `checkpoint_4.root`, `wal.bin` (empty — a paged checkpoint
prunes the WAL) — the whole directory a paged `Store` needs to recover.

## Provenance

Table `"rows"` (`register_table_paged::<Row>`, `struct Row { v: u64 }`),
200 rows inserted `1..=200` (`Row { v: id }`, so `v == k`) across 4
commits of 50 rows each, `PagedOptions::builder().prealloc_chunk_bytes(64
<< 10).build()` (small chunk to keep the fixture small — this fixture is
about format compat, not size or perf), `Persistence::standalone(dir,
Durability::Eventual, WalWrite::Coalesced)`, then one `checkpoint()`
(landed at version 4) and a clean drop.

## Regenerating

Only needed if the fixture is lost. Check out the commit above (or any
commit before Task 4's decode change) in a worktree and run the generator
that produced it — reconstructed from this README, since the generator
itself was not committed (see `tests/fixtures/formats/README.md` for the
same one-shot-generator-not-committed convention):

```rust
use ultima_db::{Durability, PagedOptions, Persistence, Store, StoreConfig, WalWrite};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Row { v: u64 }

fn main() {
    let dir = std::env::args().nth(1).unwrap();
    let p = Persistence::standalone(
        std::path::PathBuf::from(&dir), Durability::Eventual, WalWrite::Coalesced,
    ).paged(PagedOptions::builder().prealloc_chunk_bytes(64 << 10).build()).unwrap();
    let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s.register_table_paged::<Row>("rows").unwrap();
    let mut id = 0u64;
    for _ in 0..4u64 {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        for _ in 0..50u64 { id += 1; t.insert(Row { v: id }).unwrap(); }
        w.commit().unwrap();
    }
    s.checkpoint().unwrap();
}
```

```bash
cargo run --features persistence --example gen_paged_fixture -- <scratch dir>
```

Copy `pages.bin`, `checkpoint_4.root`, and `wal.bin` (all of them — unlike
`tests/fixtures/formats/`, WAL compat *is* in scope here, since a paged
recovery reads `wal.bin` even when it is empty) into this directory.
