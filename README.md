# UltimaDB

A high-performance transactional embedded database for Rust, built on a
persistent copy-on-write B-tree. Data lives in memory with opt-in
WAL/checkpoint durability; every commit produces a new immutable MVCC
snapshot that shares unchanged subtrees with its predecessors, so
point-in-time reads are zero-copy and old versions stay alive for free.

## Performance

Durable YCSB on an AWS local-NVMe host (8 vCPU), every operation its own
fsync-acknowledged transaction. UltimaDB is fastest on all six workloads —
1.8–5.4× ahead of the best of RocksDB, Fjall, and ReDB:

| Workload | UltimaDB | vs. fastest competitor |
|---|--:|--:|
| Read-only | 5.81M ops/s | 4.2× |
| Read-mostly (95/5) | 397k ops/s | 2.0× |
| Read-latest | 386k ops/s | 2.2× |
| Update-heavy (50/50) | 42.4k ops/s | 1.8× |
| Read-modify-write | 42.0k ops/s | 1.8× |

Single-host criterion medians from our benchmark — absolute numbers vary by
machine; compare ratios, not raw values. Full results (both durability
tiers, all four engines, ms and ops/sec, methodology):
[docs/benchmarks/competitor-nvme-2026-07-13.md](docs/benchmarks/competitor-nvme-2026-07-13.md).

## Highlights

- **MVCC snapshots** — `begin_read(None)` pins the latest snapshot;
  `begin_read(Some(v))` time-travels. Readers never block writers and vice
  versa.
- **Typed tables** — `Table<R>` with auto-incrementing ids, secondary
  indexes (unique, non-unique, and user-defined `CustomIndex`
  implementations such as the built-in BM25 full-text index), and atomic
  batch operations.
- **Concurrent writers** — opt-in `MultiWriter` mode with key-level
  optimistic concurrency control: writers conflict only when they touch the
  same rows of the same table. Serializable snapshot isolation (write-skew
  prevention) is available via `IsolationLevel::Serializable`.
- **Opt-in durability** (`persistence` feature) — group-committed WAL with
  `Consistent` (fsync-acknowledged commits) or `Eventual` durability,
  CRC-protected checkpoints, crash recovery, and a checkpoint-only SMR mode
  for Raft/Paxos deployments where the consensus log owns durability.
- **Bulk loads & snapshot streaming** — O(N) sorted rebuilds for restores
  and deltas, multi-table atomic installs, and a streaming wire format for
  replication.
- **Vector search** (`ultima-vector`) — HNSW with SIMD-accelerated distance
  kernels (AVX-512/AVX2/NEON via runtime dispatch), metadata filtering, and
  MVCC-consistent restores.
- **Fast batch writes** — auto-increment batches take an O(batch + height)
  bulk-append path; full restores build trees O(N) via `Store::bulk_load`.

## Quick example

```rust
use ultima_db::Store;

let store = Store::default();

// Write a snapshot.
let mut wtx = store.begin_write(None).unwrap();
let mut users = wtx.open_table::<String>("users").unwrap();
let id = users.insert("alice".to_string()).unwrap();
let v1 = wtx.commit().unwrap();

// Read it back — and keep reading it, even as later commits land.
let rtx = store.begin_read(Some(v1)).unwrap();
assert_eq!(rtx.open_table::<String>("users").unwrap().get(id),
           Some(&"alice".to_string()));
```

More in [`examples/`](examples/): basic usage, multiple stores, concurrent
writers with conflict retry, and bulk restore.

## Installation

```bash
cargo add ultima-db            # in-memory store
cargo add ultima-db --features persistence   # + WAL/checkpoint durability
cargo add ultima-vector        # HNSW vector search on top
```

| Feature | What it adds |
|---|---|
| *(default)* | In-memory MVCC store — no I/O, no serde |
| `persistence` | WAL + checkpoints, crash recovery, SMR mode (`serde`/`bincode`) |
| `fulltext` | BM25 full-text `CustomIndex` |
| `metrics` | `metrics`-crate instrumentation |

MSRV: Rust 1.88. Pre-1.0: minor versions may break API.

## Workspace

| Crate | What it is |
|---|---|
| `ultima-db` | The store: B-tree, tables, MVCC, OCC/SSI, WAL + checkpoints |
| `ultima-vector` | HNSW vector search over UltimaDB tables |

## Development

```bash
cargo test                       # unit + integration tests
cargo clippy -- -D warnings      # lint (zero warnings policy)
cargo bench                      # criterion benchmarks (YCSB, SmallBank, ...)
```

Design notes for every feature live in [`docs/tasks/`](docs/tasks/). The full configuration reference is [docs/configuration.md](docs/configuration.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
