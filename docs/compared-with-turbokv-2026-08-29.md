# UltimaDB compared with TurboKV (research note, 2026-08-29)

Feature-by-feature inventory of [TurboKV](https://github.com/kingroryg/turbokv)
against UltimaDB, from primary sources only. TurboKV facts come from the local checkout at `../exernal_db/turbokv`
(commit `b60a7049`, 2026-08-29, identical to a fresh clone of `main`), crate
version 0.6.0;
UltimaDB facts from this working tree at `93eff28` (0.3.0). Nothing here is a
performance conclusion — the two projects' benchmarks were run on different
hardware against different workloads and are not comparable (see "Benchmarks").

Citations are `T:` (turbokv path) or `U:` (this repo path).

## One-line summary

TurboKV is a **raw-bytes, async, disk-resident LSM-tree** key-value store
(memtable → SSTables → compaction) with atomic write batches and three
durability presets. UltimaDB is a **typed, synchronous, memory-resident MVCC
store** on a persistent copy-on-write B-tree with transactions, snapshot
time-travel, OCC/SSI, secondary/full-text/vector indexes, and opt-in WAL +
checkpoint durability. They occupy different corners of the design space; the
overlap is "embedded Rust KV with a WAL".

## Comparison table

| Dimension | TurboKV 0.6.0 | UltimaDB 0.3.0 |
|---|---|---|
| **Engine** | LSM tree: `crossbeam-skiplist` memtable, SSTables with Bloom filters + block cache, background flush and compaction, manifest (T: `src/storage/{memtable,sstable,compaction,manifest}.rs`, `Cargo.toml`) | Persistent CoW B-tree (T=32, `fanout-t8` option); every commit is a new root sharing unchanged subtrees via `Arc` (U: `src/btree.rs`, `CLAUDE.md`) |
| **Where data lives** | Disk; 64 MiB memtable default, mmap SSTable reads (T: `README.md` "All presets start with a 64 MiB memtable") | Process memory; disk only for WAL/checkpoints (U: `README.md` "Data lives in memory") |
| **Key / value model** | Arbitrary bytes via `AsRef<[u8]>`, lexicographic byte order, caller encodes; values returned as owned `Vec<u8>` (T: `README.md` "Database operations") | Typed `Table<R, K = u64>`; `K` ∈ integers u8–u128/i8–i128, `String`, `Vec<u8>`, 2-/3-tuples with order-preserving encoding; auto-increment ids for `u64` (U: `src/primary_key.rs`, `README.md`) |
| **Namespaces / tables** | Single keyspace per `Db`; no column families found (T: `src/lib.rs` exports) | Many named tables per store, each with its own record type and key type (U: `src/store.rs` `open_table`, `open_table_keyed`) |
| **API style** | Async (`tokio` required): `db.get(k).await?`; sync streaming iterators (T: `README.md` quick start, `Cargo.toml` deps) | Synchronous; on async runtimes wrap in `spawn_blocking` (U: `CLAUDE.md` ReadTx/WriteTx) |
| **Transactions** | None. `WriteBatch` publishes atomically; `insert_many` is explicitly *not* atomic; each scan captures a point-in-time view (T: `README.md` "Point, bulk, and batch operations") | `WriteTx`/`ReadTx`; commit = new immutable snapshot version; `begin_read(Some(v))` time-travels to any retained version (U: `README.md` Highlights) |
| **Isolation** | Not stated as a level; last-writer-wins on duplicate keys; batches serialized by a mutex (T: `src/storage/engine.rs:262` `batch_serialization`) | Snapshot Isolation default; opt-in Serializable (SSI, write-skew prevention) (U: `docs/reference/isolation-levels.md`) |
| **Concurrent writers** | Shared `Db` handle across Tokio tasks; no conflict detection (T: `examples/concurrent.rs`, `engine.rs` locks) | `SingleWriter` default, or `MultiWriter` with key-level OCC → `Error::WriteConflict` on overlapping rows (U: `CLAUDE.md` "MultiWriter OCC") |
| **Durability presets** | `fast()` no WAL · `durable()` WAL append, no per-write sync · `paranoid()` group `sync_all` before ack; `wal_enabled` / `sync_writes` fields (T: `README.md` "Durability presets") | `Persistence::None` · `standalone(dir, Durability, WalWrite)` with `Eventual` / `Consistent` / `ConsistentInline` × `PerEntry` / `Coalesced` / `CoalescedPrealloc` · `smr(dir)` checkpoint-only for Raft/Paxos (U: `docs/reference/configuration.md`, `CLAUDE.md`) |
| **WAL** | Segmented, format v5, CRC framing + tag-last commit marker, reserved shared mappings where supported, tail repair on recovery (T: `CHANGELOG.md` 0.6.0) | Single `wal.bin`, CRC32 (`crc32fast`), optional pre-zero-filled prealloc with tail-tolerant scan, group commit via background thread (U: `src/wal.rs`, task37/task38 docs) |
| **Checkpoints / flush** | Memtable flush → SSTable + manifest; WAL segments reclaimed after flush (T: `README.md` `flush()`) | Explicit `checkpoint()`; full or CoW-structural-diff incremental chain (`checkpoint_chain_max`); WAL pruned after checkpoint (U: `docs/tasks/task61_incremental_checkpoints.md`) |
| **Compaction / GC** | Background compaction, `compact()` reports reclaimed tombstones (T: `README.md`) | No on-disk compaction (tree is in memory); `gc()` evicts old snapshot versions, `pin_version` protects one (U: `CLAUDE.md` Store) |
| **Compression** | LZ4 default, Snappy, Zstd, None (T: `README.md` `DbOptions.compression`) | None — no compression dependency or option (U: `grep -i compress src Cargo.toml` → no hits) |
| **Bloom filter / cache** | Exact-key Bloom filter (hardware-AES hash, needs `+aes` target feature), 64 MiB decompressed block cache (T: `README.md` Installation) | N/A (in-memory tree; no disk read path to filter) |
| **Point ops** | `insert`, `get`, `remove` (tombstone), `contains_key`, `insert_many`, `write_batch` (T: `README.md`) | `insert`/`put`, `get`, `update`, `delete`, `insert_batch`/`update_batch`/`delete_batch` with atomic rollback (U: `src/table.rs`) |
| **Range / prefix scans** | `range(start,end)` eager, `scan_prefix`, streaming `range_iter`/`scan_prefix_iter` with `paginate`, lazy `EntryGuard` values (T: `README.md` "Range and prefix scans") | `Table::range`, `Table::iter`, `BTree::range_prefix` for tuple-prefix scans, `index_range` on secondary indexes (U: `src/table.rs:830,908,1081`; `src/btree.rs:627`) |
| **Secondary indexes** | None found | Unique / non-unique, user-defined `CustomIndex`, maintained automatically on write (U: `src/index.rs`) |
| **Full-text search** | None found | BM25 `FullTextIndex` behind `fulltext` feature (U: `README.md` feature table) |
| **Vector search** | None found | HNSW + SIMD kernels in sibling `ultima-vector` crate (U: `README.md`) |
| **Bulk load / restore** | `insert_many` (non-atomic bulk) (T: `README.md`) | `Store::bulk_load` O(N) sorted rebuild, multi-table atomic `bulk_load_batch`, index rebuild (U: `src/bulk_load.rs`) |
| **TTL / retention** | Removed in 0.6.0 ("TTL and time retention require a separate database contract") (T: `CHANGELOG.md`) | None (U: `grep -rli 'ttl\|expire' src` → only a false hit on "little") |
| **Replication hooks** | None found | SMR checkpoint-only mode + snapshot streaming wire format for replication (U: `src/snapshot_stream/`, `README.md`) |
| **Format compatibility** | v5 reads released v1–v4 WAL segments; older TurboKV cannot open v5; fixture-based `tests/format_compatibility.rs` with SHA256SUMS (T: `CHANGELOG.md`, `tests/fixtures/storage_formats/`) | ≥0.2.0 checkpoints readable (v1 container, both payload generations); pre-0.3.0 WALs hard-fail at open; `tests/format_compat.rs` (U: `docs/tasks/task62_persistence_format_compat.md`) |
| **Directory ownership** | One open `Db`/`Engine` exclusively owns its directory (`directory_lock.rs`); `close()` required, drop is not a clean shutdown (T: `README.md`) | No directory lock found in `src/persistence.rs` (U: `grep flock\|try_lock` → no hits) |
| **Observability** | `status()`, `logical_stats()`, `physical_stats()`, `tracing` (T: `README.md`) | `metrics`-crate instrumentation behind `metrics` feature (U: `README.md` feature table) |
| **Testing & verification** | proptest (4 `proptest!` blocks + regressions file), test-only `failpoints.rs` persistence fault injection, ASan + TSan CI jobs, CI on Linux/macOS/Windows, MSRV gate (T: `.github/workflows/{ci,soundness}.yml`, `CHANGELOG.md`) | Elle list-append consistency checks under SI/SSI with mutation-tested harness; Lean 4 / Aeneas machine-checked B-tree + key-encoding proofs; TLA+ WAL model; WAL fault-injection tests (fsync, torn tail, failed extend); proptest; CI Linux-only (U: `docs/explanation/how-ultimadb-is-verified.md`, `formal/`, `tests/wal_fault_*.rs`, `.github/workflows/ci.yml` `ubuntu-latest`) |
| **Benchmarks published** | vs fjall 2.11.2 and redb 2.6.3; 200k × (20 B key, 400 B value), one caller; Apple M4, macOS 15.3.2, APFS; raw JSON artifacts committed (T: `README.md` Benchmarks, `benchmarks/results/`) | YCSB A–F + SmallBank vs RocksDB/Fjall/ReDB; AWS local-NVMe 8 vCPU; per-run docs in `docs/benchmarks/` (U: `README.md`, `docs/benchmarks/competitor-nvme-*.md`) |
| **Language / runtime deps** | Rust 2021, MSRV 1.85; `tokio`, `memmap2`, `crossbeam-skiplist`, `zstd`/`snap`/`lz4_flex`, `gxhash`, `parking_lot` (T: `Cargo.toml`) | Rust 2024, MSRV 1.88; default build has no I/O and no serde; `persistence` adds `serde`/`bincode` (U: `Cargo.toml`, `README.md`) |
| **Size** | ~26.8k lines in `src/` (T: `find src -name '*.rs' \| xargs cat \| wc -l`) | ~32.0k lines in `src/` (U: same command) |
| **License** | Apache-2.0 (T: `Cargo.toml`) | Apache-2.0 (U: `Cargo.toml`) |
| **Release state** | 0.6.0 dated 2026-08-28 in CHANGELOG; single author; pre-1.0 with breaking removals in 0.6.0 (T: `CHANGELOG.md`) | 0.3.0 on crates.io; pre-1.0 (U: `README.md`) |

## Things TurboKV has that UltimaDB does not

1. Disk-resident dataset larger than RAM (LSM + mmap SSTables).
2. Async-native API on Tokio.
3. On-disk compression (LZ4/Snappy/Zstd).
4. Background compaction with tombstone reclamation and a `compact()` report.
5. Block cache and Bloom filters (relevant only because of 1).
6. Exclusive directory lock and a structured `close_with_status()` shutdown contract.
7. Streaming iterators with `paginate` and lazy value materialization (`EntryGuard`).
8. Cross-platform CI (Linux/macOS/Windows) and ASan/TSan sanitizer jobs.
9. `logical_stats()` / `physical_stats()` / write-stall counters out of the box.

## Things UltimaDB has that TurboKV does not

1. Transactions (`ReadTx`/`WriteTx`) and MVCC versioned snapshots with time travel.
2. Stated isolation levels: Snapshot Isolation, opt-in Serializable (SSI).
3. Concurrent writers with key-level OCC conflict detection.
4. Typed records and typed, order-preserving composite primary keys.
5. Multiple named tables per store.
6. Secondary indexes (unique / non-unique / custom), BM25 full-text, HNSW vector search.
7. O(N) bulk load with atomic multi-table install and index rebuild.
8. Incremental (CoW-diff) checkpoints; SMR checkpoint-only mode for consensus-log deployments; snapshot streaming format.
9. Finer durability matrix (3 `Durability` × 3 `WalWrite`, including inline fsync and preallocated WAL).
10. Formal verification (Lean/Aeneas proofs, TLA+), Elle consistency checking, WAL fault injection.
11. No-I/O, no-serde default build.

## Benchmarks — why the numbers are not comparable

TurboKV's table is single-caller ingest of 200k rows on an Apple M4 laptop
against fjall and redb; its "Durable" column is an *un-synced* WAL append
(process-crash-safe, not power-loss-safe), and its README notes redb's
`Eventual` mode pays a macOS barrier per transaction, so the single-key redb
rows are "architectural context rather than a like-for-like durability claim"
(T: `README.md`). UltimaDB's headline numbers are fsync-acknowledged YCSB on an
AWS NVMe host (U: `README.md`). A fair comparison would need TurboKV in this
repo's `compare_benches` harness on the remote NVMe rig (`bench-infra/`) —
which has not been done. Per `CLAUDE.md`, no perf conclusion may be drawn from
a local run.

## Not verified / unknown

- GitHub star count and crates.io download figures (GitHub API not queried).
- Whether TurboKV 0.6.0 is actually published on crates.io (only `Cargo.toml`
  metadata and the CHANGELOG date were read).
- TurboKV's `tests/database_soundness.rs` test count (its tests use an
  attribute form my grep did not match; the file exists and is wired into
  `soundness.yml`).
- TurboKV's read-path concurrency guarantees beyond "scans capture a coherent
  point-in-time view" — no isolation level is documented.
