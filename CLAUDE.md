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

`Store` → `HashMap<u64, Arc<Snapshot>>` → `HashMap<String, Arc<dyn MergeableTable>>` → `Table<R>` → `BTree<u64, R>` → `Arc<BTreeNode<u64, R>>`

- **`BTree<K, V>`** (`src/btree.rs`): Persistent B-tree (T=32, MAX_KEYS=63). `insert`/`remove` return a *new* `BTree`; `Clone` is O(1) (Arc bump on root). Values stored as `Arc<V>` — no `V: Clone` bound needed. Not re-exported; internal implementation detail.
- **`Table<R>`** (`src/table.rs`): Typed collection wrapping `BTree<u64, R>` with auto-incrementing IDs, `&mut self` mutation API, secondary indexes (`define_index`, `get_unique`, `get_by_index`, `get_by_key`, `index_range`), and batch operations (`insert_batch`, `update_batch`, `delete_batch`). `R: Send + Sync + 'static`. `Clone` is O(1) and preserves `next_id`.
- **`index`** (`src/index.rs`): Secondary index infrastructure. `IndexKind` (Unique/NonUnique), `ManagedIndex<R, K, S>` with `KeyExtractor` trait for key extraction. `UniqueStorage<K>` backed by `BTree<K, u64>`, `NonUniqueStorage<K>` backed by `BTree<(K, u64), ()>` (composite key for multi-value). Indexes maintained automatically on insert/update/delete via `IndexMaintainer` trait. Clone is O(1) via CoW B-tree internals.
- **`Store`** (`src/store.rs`): Version history as `HashMap<u64, Arc<Snapshot>>`. `Store: Send + Sync + Clone` — share clones across threads. Constructed via `Store::new(StoreConfig)` (or `Store::default()` for defaults). Provides `begin_read(Option<u64>)`, `begin_write(Option<u64>)`, and `gc()`. `StoreConfig` controls `num_snapshots_retained` (default 10, how many recent snapshots `gc()` keeps), `auto_snapshot_gc` (default true), and `writer_mode` (`SingleWriter` default, or `MultiWriter` for concurrent writers with OCC).
- **`ReadTx`/`WriteTx`** (defined in `src/store.rs`, re-exported via `src/transaction.rs`): `ReadTx` holds `Arc<Snapshot>` — zero-copy reads. `WriteTx` lazily clones tables on first `open_table` (O(1) per table). Both are `!Send + !Sync` (`PhantomData<*const ()>` marker) — a transaction must be opened and committed on the same thread. To use concurrent threads, clone the `Store` and call `begin_write`/`begin_read` inside each thread.
- **Type erasure**: Tables are `Arc<dyn MergeableTable>` in snapshots, `Box<dyn MergeableTable>` in WriteTx's dirty map. `MergeableTable: Any + Send + Sync` — downcasts to `Table<R>` go through `.as_any().downcast_ref::<Table<R>>()`. The trait also exposes `boxed_clone` (O(1) CoW) and `merge_keys_from` (per-key merge used by commit).

**Isolation level:** Snapshot Isolation — prevents dirty reads, nonrepeatable reads, phantom reads. Does *not* prevent write skew. See `docs/isolation-levels.md` for details and what SSI would require.

**MultiWriter OCC (key-level):** In `WriterMode::MultiWriter`, two writers conflict at commit only when their modified-row sets overlap on the same table. Disjoint keys in the same table both commit — the second commit's per-key merge (see the `MergeableTable` trait in `src/table.rs`) pulls only the writer's edited keys onto the current latest snapshot's table, preserving the other writer's edits to different rows. Different-table writers never conflict. On key overlap, the loser gets `Error::WriteConflict`; retry rebases. See `examples/concurrent_writes.rs`. Still **SI**, not SSI — read-set tracking would be needed to prevent write skew.

## Code Conventions

- The B-tree uses bottom-up splitting (recursive `InsertResult::Split` propagation) and check-before-delete (call `get()` before entering the deletion path to avoid unnecessary CoW).
- `WriteTx`/`ReadTx`/`Snapshot` are all in `store.rs` to avoid a circular module dependency. `transaction.rs` is a pure re-export.
- `WriteTx::commit` rebases the new snapshot onto the current `latest_version` and merges per-table via `MergeableTable::merge_keys_from`. Fast path: if no concurrent committed writer touched this table since my base, install my dirty wholesale (single Arc swap). Slow path: clone the latest version of the table (O(1) CoW) and replay just my modified keys into it through `Table::upsert_arc`, which keeps secondary indexes consistent via the existing `on_update`/`on_insert` hooks.
- Auto-assigned commit versions (from `begin_write(None)` in MultiWriter mode) are bumped under the commit lock to `latest_version + 1` so version order matches commit order. Without this, pre-assigned versions can let a chronologically-later commit land at a lower version number and the rebase chain loses its updates. Explicit versions (SMR mode) are not bumped.
- Batch operations (`insert_batch`, `update_batch`, `delete_batch`) use snapshot-and-restore for atomic rollback: capture table state before the operation, restore on failure.
- Index mutation during insert/update uses raw pointers to avoid borrowing `self.indexes` mutably while iterating. This is safe because the HashMap is not structurally modified during the loop.
- New features are documented in `docs/tasks/taskXX_feature_name.md` with architectural decisions and implementation details.

## Feature Development Workflow

Using superpowers (brainstorming, writing-plans, executing-plans) during feature development is fine — the generated plans/notes under `docs/superpowers/` are working artifacts, not deliverables. Before finishing and committing the feature:

1. Consolidate the architectural decisions and implementation details into `docs/tasks/taskXX_feature_name.md` (the canonical per-feature doc).
2. Delete the corresponding superpowers artifacts (`docs/superpowers/plans/*.md`, etc.) for that feature. They must not be committed alongside the `taskXX_feature_name.md`.

Superpowers artifacts are ephemeral scaffolding; `docs/tasks/` is the permanent record.
