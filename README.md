# UltimaDB

A high-performance transactional embedded database for Rust, built on a
persistent copy-on-write B-tree. Data lives in memory with opt-in
WAL/checkpoint durability; every commit produces a new immutable MVCC
snapshot that shares unchanged subtrees with its predecessors, so
point-in-time reads are zero-copy and old versions stay alive for free.

**Documentation:** [tutorial](docs/tutorials/getting-started.md) ·
[how-to guides](docs/how-to/README.md) ·
[reference](docs/reference/README.md) ·
[explanation](docs/explanation/README.md) ·
[how it is verified](docs/explanation/how-ultimadb-is-verified.md) ·
[API docs](https://docs.rs/ultima-db)

## Highlights

- **MVCC snapshots** — `begin_read(None)` pins the latest snapshot;
  `begin_read(Some(v))` time-travels. Readers never block writers and vice
  versa.
- **Typed tables** — `Table<R>` with auto-incrementing ids, unique and
  non-unique secondary indexes, and atomic batch operations.
- **Arbitrary primary keys** — `Table<R, K = u64>`: key a table by
  `String`, `Vec<u8>`, any integer width, or a tuple, via
  `open_table_keyed::<R, K>`, instead of stashing the natural key in a
  unique index beside a surrogate id. Key encoding is order-preserving, so
  range scans, bulk loads and WAL replay all see the same order.
- **Pluggable indexes** — `CustomIndex` is a public trait, not an internal
  one. Implement it and the store maintains your index transactionally,
  inside the same MVCC snapshot as the rows it indexes, so there is no
  second system to keep consistent and no separate crash-recovery story.
  The built-in [BM25 full-text index](docs/how-to/query-with-indexes.md#full-text-search) is one example implementation.
  For more see the
  [custom-index how-to](docs/how-to/query-with-indexes.md#custom-indexes).
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
- **Vector search** (`ultima-vector`) — [HNSW vector index](docs/explanation/vector-search.md#riding-on-mvcc) with SIMD-accelerated distance
  kernels (AVX-512/AVX2/NEON via runtime dispatch), metadata filtering, and
  MVCC-consistent restores. 
- **Fast batch writes** — auto-increment batches take an O(batch + height)
  bulk-append path; full restores build trees O(N) via `Store::bulk_load`.

## Performance

Durable YCSB on an AWS local-NVMe host (8 vCPU): a 10,000-record working
set, every operation its own fsync-acknowledged transaction. UltimaDB is
fastest on all six workloads, 1.8–5.4× ahead of the best of RocksDB, Fjall,
and ReDB:

| Workload | UltimaDB | vs. fastest competitor |
|---|--:|--:|
| Read-only | 5.81M ops/s | 4.2× |
| Read-mostly (95/5) | 397k ops/s | 2.0× |
| Read-latest | 386k ops/s | 2.2× |
| Short range scans | 336k ops/s | 5.4× |
| Update-heavy (50/50) | 42.4k ops/s | 1.8× |
| Read-modify-write | 42.0k ops/s | 1.8× |

This is a deliberately narrow comparison, and the numbers only mean
something inside it: working sets that fit in RAM, small transactions,
one fsync per commit. RocksDB's LSM design is paying for larger-than-memory
data and compaction-managed write amplification — capabilities UltimaDB
doesn't offer. If your data outgrows memory, this is the wrong engine and
the wrong benchmark. Relax durability to the engines' default no-fsync
paths and Fjall leads the write-heavy mixes; those rows are in the full
results too. Single-host criterion medians — compare ratios, not absolute
values: [docs/benchmarks/competitor-nvme-2026-07-13.md](docs/benchmarks/competitor-nvme-2026-07-13.md)
and [reading our benchmark numbers](docs/explanation/reading-our-benchmarks.md).

## Correctness & verification

Tests catch the bugs someone thought of. A database also has to survive the
ones nobody did, so UltimaDB adds checks that go beyond a test suite — each
one lives in this repo and runs in CI:

- **The single-threaded B-tree core is proven correct.** Every table and
  index sits on one B-tree. Its insert and delete code is proven in Lean 4
  to behave exactly like a plain map — what you wrote is what you read
  back, and nothing else changes — against a mechanical Rust→Lean
  translation of the extracted insert/delete kernel, produced by
  [Aeneas](https://github.com/AeneasVerif/aeneas), so the theorems are about
  translated algorithm code rather than a hand-written model of it (the
  kernel is pinned to the shipped code by differential tests). The
  order-preserving key encoding gets the same treatment. CI rebuilds the
  proofs and re-checks their axioms whenever the B-tree, key-encoding, or
  proof sources change on `main` (and weekly regardless); a cheaper drift
  guard on every PR fails the build if a verified source changes without a
  matching proof-side change.
- **Transaction isolation is checked by Elle**, the tool used in the
  published Jepsen analyses of PostgreSQL, MySQL, and CockroachDB. Many
  threads hammer the store concurrently and Elle searches the recorded
  history for any result that the promised isolation level forbids.
- **The checker is proven to be able to fail.** Three real bugs can be
  deliberately switched on in the commit path, and CI confirms Elle catches
  every one. A green check that can never go red is worthless. This harness
  has already found and fixed a real deadlock.
- **Crash recovery is tested the way disks actually fail.** Logs are torn,
  zero-filled, bit-flipped, and killed mid-write. A crash restores everything
  that was acknowledged; data damaged after the fact fails loudly rather
  than being silently dropped. A TLA+ model of the commit pipeline
  additionally explores crash interleavings a test could never enumerate.

Each layer has limits — the proofs cover the single-threaded core, not the
concurrent machinery; Elle samples rather than exhausts — chosen so one
layer's blind spot sits inside another's coverage. The full argument, with
what is and is not covered, is in
[How UltimaDB is verified](docs/explanation/how-ultimadb-is-verified.md);
the proof inventory is in [`formal/README.md`](formal/README.md).

## How this was built

UltimaDB was designed by me and pair-programmed with Claude. The specs that
drove each feature are in [`docs/tasks`](docs/tasks),
so the process is auditable rather than something you have to take my word for.

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

// Or key the table yourself. `put` replaces `insert`, since there is no
// counter for a key the store cannot generate.
let mut wtx = store.begin_write(None).unwrap();
let mut emails = wtx.open_table_keyed::<String, String>("by_email").unwrap();
emails.put("alice@example.com".to_string(), "alice".to_string()).unwrap();
drop(emails);
wtx.commit().unwrap();
```

More in [`examples/`](examples/): basic usage, multiple stores, concurrent
writers with conflict retry, bulk restore, and a `String`-keyed table.

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
make consistency/elle            # Elle isolation check (needs java)
make consistency/elle-mutation   # prove the Elle check has teeth
make test/wal-faults             # in-flight WAL fault injection
make formal/tla-model            # TLA+ WAL crash-safety model (TLC, needs java)
make test/formal-kernel          # Lean-kernel differential tests
```

Documentation lives in [`docs/`](docs/README.md): a [getting-started tutorial](docs/tutorials/getting-started.md), [how-to guides](docs/how-to/README.md), [reference pages](docs/reference/README.md) (configuration, formats, isolation, performance), and [explanations](docs/explanation/README.md) of the architecture and design. The API reference is on [docs.rs](https://docs.rs/ultima-db). Per-feature design records for contributors live in the repo's `docs/tasks/` directory (internal, unlinked from the user docs).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
