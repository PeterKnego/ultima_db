# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
cargo build                              # build
cargo test                               # all tests (unit + integration)
cargo test btree::tests::insert_single   # single unit test by name substring
cargo test --test store_integration      # integration tests only
cargo clippy -- -D warnings              # lint (must pass with zero warnings)
cargo bench                              # first-party benchmarks (criterion)
make bench/compare-engines               # competitor baselines (RocksDB/Fjall/ReDB), opt-in
make perf/check                          # perf regression gate (autobench baselines)
cargo run --example basic_usage          # run examples
cargo run --example multi_store
```

## Benchmarking & Performance Testing

Two tiers, and the choice is about *repeatability*, not convenience:

- **Local (`cargo bench`, `make bench/*`, `make perf/check`)** — fine for a quick dev smoke test, catching an obvious regression, or exercising a bench harness for correctness. But the dev/CI/sandbox environment is too noisy for trustworthy numbers: absolute results are meaningless across machines and even same-machine A/B noise floors run ±2x on the Claude sandbox (±2.5–9% on a real bench host). **Never draw a perf conclusion — "X is faster than Y", a new baseline, a competitor ratio — from a local run.**
- **Remote NVMe host (`bench-infra/`)** — use this whenever **comprehensive, repeatable, or publishable** performance results are required: competitor comparisons, WAL/durability A/B sweeps, autobench Gate-A baselines, or any number that will land in `docs/benchmarks/` or a task doc. It provisions one AWS local-NVMe host, OS-tunes it, rsyncs the working tree, runs the workload on ephemeral NVMe, and pulls results to `bench-out/dist/<ts>/`. See `bench-infra/README.md` and `docs/superpowers/specs/2026-07-08-bench-infra-carveout-design.md`.

Remote usage (all from `bench-infra/`):

```bash
make init                       # terraform init (once)
make up                         # provision + configure (cold build: several min)
make bench/competitor           # UltimaDB vs RocksDB/Fjall/ReDB, both durability tiers
make bench/wal-ab               # ultima-only WAL/durability A/B sweep
make bench/autobench            # autobench Gate-A microbenches + baseline record
make status                     # list host + uptime (cost guard)
make destroy                    # tear everything down
make bench-oneshot TARGET=competitor|wal-ab|autobench   # up -> bench -> destroy
```

**Guardrails — read before touching `make up`/`bench-oneshot`:**

- These commands spin up **real, billable AWS resources**. Get **explicit user authorization** before any `make up`/`bench-oneshot` — do not provision cloud infra on your own initiative.
- **Nothing auto-reaps** (`ttl_hours` is only a tag). Always `make destroy` when done, and run `make status` to confirm no host is left running. Prefer `make bench-oneshot` for one-off runs so teardown is automatic.
- **Same-host relative only** — compare engine ordering and ratios, never absolute numbers across machines. NVMe is ephemeral (instance store); never store data you want to keep.
- Gate B (needs `ultima_cluster`) is **not** run remotely — it is a local-only step.

## Architecture

UltimaDB is an MVCC store built on a persistent copy-on-write B-tree. The data lives in memory; durability is opt-in via the `persistence` cargo feature (WAL + checkpoints, or checkpoint-only for SMR deployments). The key insight: mutations create new tree roots sharing unchanged subtrees via `Arc`, so old versions stay alive for free.

**Data structure stack:**

`Store` → `HashMap<u64, Arc<Snapshot>>` → `HashMap<String, Arc<dyn MergeableTable>>` → `Table<R>` → `BTree<u64, R>` → `Arc<BTreeNode<u64, R>>`

- **`BTree<K, V>`** (`src/btree.rs`): Persistent B-tree (T=32, MAX_KEYS=63). `insert`/`remove` return a *new* `BTree`; `Clone` is O(1) (Arc bump on root). Values stored as `Arc<V>` — no `V: Clone` bound needed. Re-exported as `ultima_db::BTree` (also the recommended backing structure for `CustomIndex` internals).
- **`Table<R>`** (`src/table.rs`): Typed collection wrapping `BTree<u64, R>` with auto-incrementing IDs, `&mut self` mutation API, secondary indexes (`define_index`, `get_unique`, `get_by_index`, `get_by_key`, `index_range`), and batch operations (`insert_batch`, `update_batch`, `delete_batch`). `insert_batch` takes a bulk-append fast path (right-spine-seeded `BulkBuilder`, task51) since batch ids always append past the current max key. `R: Send + Sync + 'static`. `Clone` is O(1) and preserves `next_id`.
- **`index`** (`src/index.rs`): Secondary index infrastructure. `IndexKind` (Unique/NonUnique), `ManagedIndex<R, K, S>` with `KeyExtractor` trait for key extraction. `UniqueStorage<K>` backed by `BTree<K, u64>`, `NonUniqueStorage<K>` backed by `BTree<(K, u64), ()>` (composite key for multi-value). Indexes maintained automatically on insert/update/delete via `IndexMaintainer` trait. Clone is O(1) via CoW B-tree internals.
- **`bulk_load`** (`src/bulk_load.rs`): Fast path for full restores and incremental deltas. `Store::bulk_load` builds a fresh data tree (`BTree::from_sorted`, O(N)) and rebuilds indexes via `IndexMaintainer::rebuild_from_sorted_data` (the same primitive `Table::define_index` now uses), then atomically installs as a new snapshot. Optional `checkpoint_after` triggers `Store::checkpoint()`. See `docs/tasks/task23_bulk_load.md`. Multi-table atomic install via `Store::bulk_load_batch()` (see `docs/tasks/task24_vector_restore.md`) — `Store::bulk_load` is now a thin wrapper that delegates to a single-element batch. Installs are OCC-visible: SingleWriter refuses (`WriterBusy`) while a writer is live; MultiWriter records a synthetic committed write set so in-flight writers of the loaded table conflict at commit (delete+recreate semantics); installs are refused while Consistent-mode commits are parked in the fsync wait. In Standalone mode a `WalOp::BulkLoad` marker makes recovery fail cleanly (`BulkLoadNotCheckpointed`) if commits follow an un-checkpointed load.
- **`Store`** (`src/store.rs`): Version history as `HashMap<u64, Arc<Snapshot>>`. `Store: Send + Sync + Clone` — share clones across threads. Constructed via `Store::new(StoreConfig)` (or `Store::default()` for defaults); build the `StoreConfig` with `StoreConfig::builder()....build()` (it is `#[non_exhaustive]`, task44). Provides `begin_read(Option<u64>)`, `begin_write(Option<u64>)`, and `gc()`. `StoreConfig` controls `num_snapshots_retained` (default 10, how many recent snapshots `gc()` keeps), `auto_snapshot_gc` (default true), `writer_mode` (`SingleWriter` default, or `MultiWriter` for concurrent writers with OCC), and `isolation_level` (`SnapshotIsolation` default, or `Serializable` for read-set tracking that prevents write skew under MultiWriter — see `docs/tasks/task21_serializable_isolation.md`).
- **`ReadTx`/`WriteTx`** (defined in `src/store.rs`, re-exported via `src/transaction.rs`): `ReadTx` holds `Arc<Snapshot>` — zero-copy reads. `WriteTx` lazily clones tables on first `open_table` (O(1) per table). Both are `!Send + !Sync` (`PhantomData<*const ()>` marker) — a transaction must be opened and committed on the same thread. To use concurrent threads, clone the `Store` and call `begin_write`/`begin_read` inside each thread.
- **Type erasure**: Tables are `Arc<dyn MergeableTable>` in snapshots, `Box<dyn MergeableTable>` in WriteTx's dirty map. `MergeableTable: Any + Send + Sync` — downcasts to `Table<R>` go through `.as_any().downcast_ref::<Table<R>>()`. The trait also exposes `boxed_clone` (O(1) CoW) and `merge_keys_from` (per-key merge used by commit).
- **Persistence** (`src/persistence.rs`, `src/wal.rs`, `src/checkpoint.rs`, `src/registry.rs`): Optional, gated on the `persistence` cargo feature. `StoreConfig::persistence` selects one of `Persistence::None` (default; in-memory only), `Persistence::standalone(dir, durability, wal_write)` (UltimaDB owns durability via WAL + checkpoints), or `Persistence::smr(dir)` (checkpoint-only, for Raft/Paxos deployments where the consensus log provides durability). These types are `#[non_exhaustive]` (task44): build `StoreConfig` via `StoreConfig::builder()....build()` and `Persistence` via its constructors. With the feature enabled, `Record` adds `Serialize + DeserializeOwned` bounds.
  - **`Durability`** (commit→durable semantics): `Eventual` (async fsync, `commit()` returns immediately), `Consistent` (blocks until fsynced via the WAL background thread), or `ConsistentInline` (same guarantee as `Consistent`, but the committing thread does the fsync itself off the store lock — no bg-thread handoff; **SingleWriter only**, `Store::new` errors under MultiWriter; best for serial durable commits on fast disk, see `docs/tasks/task38_wal_inline_fsync.md`).
  - **`WalWrite`** (how a committed batch is written, orthogonal to `Durability`): `PerEntry` (default; one `write` per entry + `sync_all`), `Coalesced` (one `write` per batch + `sync_all`, see task31), or `CoalescedPrealloc` (positioned writes into a physically pre-zero-filled `wal.bin` so each fsync is a metadata-free `fdatasync`; recovery uses a tail-tolerant scan, see `docs/tasks/task37_wal_preallocation.md`). WAL/checkpoint CRC uses hardware-accelerated `crc32fast`.
  - **`Persistence::standalone_fast(dir)`** is a convenience preset bundling the fastest durable single-writer config (`ConsistentInline` + `CoalescedPrealloc`, ~3.8× the `PerEntry` default on NVMe). Not a default change; inherits the SingleWriter-only restriction. `Store::register_table::<R>("name")` must be called for each table type before `Store::recover()` (loads latest checkpoint + replays WAL) or `Store::checkpoint()` (writes checkpoint + prunes WAL in Standalone). `checkpoint()` serializes against itself; the WAL prune is executed by the WAL background thread between batches (never racing concurrent appends) via a tmp+rename rewrite, and `cleanup_old_checkpoints` never deletes checkpoints newer than the one being kept. See `docs/tasks/task13_persistence.md` and `docs/tasks/task15_three_phase_consistent_persistence.md`.
- **`ultima_vector`** (sibling crate): HNSW vector search with SIMD-accelerated f32 distance kernels. Distance metrics (`Cosine`, `L2`, `DotProduct`, opt-in `CosineNormalized`) dispatch through `pulp` 0.22 to AVX-512 / AVX2 / NEON / scalar at runtime via a cached `Arch`. `Distance::distance_many` batches per-query setup for the brute-force-fallback path. Public `normalize_in_place` / `normalize_many` helpers prepare inputs for the `CosineNormalized` fast path. Inputs are validated at collection boundaries: dim mismatch and non-finite values (NaN/±Inf) are rejected with `Error::DimMismatch`/`Error::NonFinite` on insert, update, search, and restore; the raw `Distance` impls panic on length mismatch in all build profiles (task40). See `docs/tasks/task22_vector_search.md` (HNSW), `docs/tasks/task24_vector_restore.md` (bulk restore), and `docs/tasks/task25_simd_distance.md` (SIMD kernels).
- **Bench crates**: `bench_workloads` (shared YCSB/SmallBank generators, lib) and `compare_benches` (RocksDB/Fjall/ReDB baselines, opt-in tier — keeps their deps out of the root crate). First-party benches stay in `benches/`. Cloud A/B provisioning lives in-repo at `bench-infra/` (AWS local-NVMe single node; terraform + ansible; targets `bench/competitor`, `bench/wal-ab`, `bench/autobench` Gate-A). Decoupled from `ultima_cluster`. See `docs/superpowers/specs/2026-07-08-bench-infra-carveout-design.md`.
- **`autobench/`**: autoresearch-style perf harness (`run-iter`, `journal-microbench`, `smr-apply-microbench`) with committed baselines (`make perf/check`) and frozen torture floors. See `docs/tasks/task35_autobench_perf_harness.md` and `autobench/CLAUDE.md`. Also hosts the `elle-history` bin: `make consistency/elle` generates Elle list-append histories under MultiWriter SI/SSI and checks them with the vendored elle-cli (`tools/elle-cli/`, needs java) — see `docs/tasks/task45_elle_consistency_harness.md`. `make consistency/elle-mutation` injects commit-path bugs to prove the checks have teeth (task47).

**Isolation level:** Snapshot Isolation by default — prevents dirty reads, nonrepeatable reads, phantom reads. Set `isolation_level: IsolationLevel::Serializable` on `StoreConfig` to opt into SSI (write-skew prevention via read-set tracking; see `docs/tasks/task21_serializable_isolation.md`). SI does *not* prevent write skew; SSI does. Both modes share the same SI guarantees for `ReadTx`.

**MultiWriter OCC (key-level):** In `WriterMode::MultiWriter`, two writers conflict at commit only when their modified-row sets overlap on the same table. Disjoint keys in the same table both commit — the second commit's per-key merge (see the `MergeableTable` trait in `src/table.rs`) pulls only the writer's edited keys onto the current latest snapshot's table, preserving the other writer's edits to different rows. Different-table writers never conflict. On key overlap, the loser gets `Error::WriteConflict`; retry rebases. See `examples/concurrent_writes.rs`. SSI is available as an opt-in via `IsolationLevel::Serializable` (see task21).

## Code Conventions

- The B-tree uses bottom-up splitting (recursive `InsertResult::Split` propagation) and check-before-delete (call `get()` before entering the deletion path to avoid unnecessary CoW).
- `WriteTx`/`ReadTx`/`Snapshot` are all in `store.rs` to avoid a circular module dependency. `transaction.rs` is a pure re-export.
- `WriteTx::commit` rebases the new snapshot onto the current `latest_version` and merges per-table via `MergeableTable::merge_keys_from`. Fast path: if no concurrent committed writer touched this table since my base, install my dirty wholesale (single Arc swap). Slow path: clone the latest version of the table (O(1) CoW) and replay just my modified keys into it through `Table::upsert_arc`, which keeps secondary indexes consistent via the existing `on_update`/`on_insert` hooks.
- Auto-assigned commit versions (from `begin_write(None)` in MultiWriter mode) are finalized under the commit lock at WAL-submission time: a version ≤ `max(latest_version, last_submitted_version)` is bumped to `next_version`, so versions are unique and strictly monotonic in submission order even while earlier commits are parked in the Consistent-durability fsync wait. Snapshot promotion then happens in submission order (FIFO `PromoteGate` tickets) and re-forks from the current latest, so a parked commit can never be erased by a later commit forking past it. Explicit versions (SMR mode) are not bumped. See `docs/tasks/task15_three_phase_consistent_persistence.md` ("Promotion ordering").
- Batch operations (`insert_batch`, `update_batch`, `delete_batch`) use snapshot-and-restore for atomic rollback: capture table state before the operation, restore on failure.
- Index DDL (`define_index`/`define_custom_index`) inside a MultiWriter transaction is not carried through the merge slow path; since task41 the commit fails with `Error::IndexDdlConflict` (instead of silently dropping the index) when the DDL'd table saw a concurrent commit. Define indexes in their own transaction and retry on that error (see task41, task21 "v1 limitations").
- Index mutation during insert/update uses raw pointers to avoid borrowing `self.indexes` mutably while iterating. This is safe because the HashMap is not structurally modified during the loop.
- New features are documented in `docs/tasks/taskXX_feature_name.md` with architectural decisions and implementation details.

## Feature Development Workflow

Using superpowers (brainstorming, writing-plans, executing-plans) during feature development is fine — the generated plans/notes under `docs/superpowers/` capture the design reasoning behind a feature. Before finishing and committing the feature:

1. Consolidate the architectural decisions and implementation details into `docs/tasks/taskXX_feature_name.md` (the canonical per-feature doc).
2. Keep the corresponding superpowers artifacts (`docs/superpowers/plans/*.md`, `docs/superpowers/specs/*.md`, etc.) for that feature. Commit them alongside the `taskXX_feature_name.md` — they are retained as the design-history record.

`docs/tasks/` is the canonical per-feature record; the `docs/superpowers/` plan/spec docs are kept as the supporting design history.
