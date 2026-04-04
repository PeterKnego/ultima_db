# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
cargo build                              # build
cargo test                               # all tests (unit + integration)
cargo test btree::tests::insert_single   # single unit test by name substring
cargo test --test store_integration      # integration tests only
cargo clippy -- -D warnings              # lint (must pass with zero warnings)
cargo bench                              # benchmarks (criterion)
cargo run --example basic_usage          # run examples
cargo run --example multi_store
```

## Architecture

UltimaDB is an in-memory MVCC store built on a persistent copy-on-write B-tree. The key insight: mutations create new tree roots sharing unchanged subtrees via `Arc`, so old versions stay alive for free.

**Data structure stack:**

`Store` → `HashMap<u64, Arc<Snapshot>>` → `HashMap<String, Arc<dyn Any>>` → `Table<R>` → `BTree<R>` → `Arc<BTreeNode<R>>`

- **`BTree<R>`** (`src/btree.rs`): Persistent B-tree (T=3, MAX_KEYS=5). `insert`/`remove` return a *new* `BTree`; `Clone` is O(1) (Arc bump on root). Values stored as `Arc<R>` — no `R: Clone` bound needed. Not re-exported; internal implementation detail.
- **`Table<R>`** (`src/table.rs`): Typed collection wrapping `BTree<R>` with auto-incrementing `u64` IDs and `&mut self` mutation API. `Clone` is O(1) and preserves `next_id`.
- **`Store`** (`src/store.rs`): Version history as `HashMap<u64, Arc<Snapshot>>`. Provides `begin_read(Option<u64>)` and `begin_write(Option<u64>)`.
- **`ReadTx`/`WriteTx`** (defined in `src/store.rs`, re-exported via `src/transaction.rs`): `ReadTx` holds `Arc<Snapshot>` — zero-copy reads. `WriteTx` lazily clones tables on first `open_table` (O(1) per table), commits atomically by building a new snapshot.
- **Type erasure**: Tables are `Arc<dyn Any>` in snapshots, `Box<dyn Any>` in WriteTx's dirty map. Downcast at `open_table` time; `Error::TypeMismatch` if wrong type.

**Isolation level:** Snapshot Isolation — prevents dirty reads, nonrepeatable reads, phantom reads. Does *not* prevent write skew. See `docs/isolation-levels.md` for details and what SSI would require.

**Current limitations:** Single-writer is a design convention (not runtime-enforced). No `Send + Sync` on snapshots. No snapshot GC. No disk persistence. See `ARCHITECTURE.md` for the full decision table and rationale.

## Code Conventions

- The B-tree uses bottom-up splitting (recursive `InsertResult::Split` propagation) and check-before-delete (call `get()` before entering the deletion path to avoid unnecessary CoW).
- `WriteTx`/`ReadTx`/`Snapshot` are all in `store.rs` to avoid a circular module dependency. `transaction.rs` is a pure re-export.
- `#![allow(clippy::arc_with_non_send_sync)]` is set at module level in `store.rs` — deferred to a future task that adds `Send + Sync` bounds.
