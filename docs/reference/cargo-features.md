# Cargo features

`ultima-db` has no default features. MSRV is Rust 1.88; edition 2024.

| Feature | Enables | Added dependencies |
|---|---|---|
| `persistence` | WAL + checkpoint durability: the `Persistence::Standalone`/`Smr` modes, `Store::register_table`, `Store::recover`, `Store::checkpoint`, `Store::durable_version`, `Store::wait_durable`, `Store::on_durable`, `Store::snapshot_stream`, `Store::list_checkpoints`, `Store::open_checkpoint_reader`, the `wal` module, and `SnapshotReader`. Widens the `Record` trait bound to `Serialize + DeserializeOwned`. | `serde`, `bincode` |
| `fulltext` | The `fulltext` module: BM25 `FullTextIndex` and `SearchResult`. | — |
| `metrics` | Emits counters to the `metrics` facade. The `MetricsSnapshot` API exists without this feature. | `metrics` |
| `fanout-t8` | B-tree fanout T=8 instead of the default T=32, for write-dominated SMR deployments. | — |
| `wal-iouring` | io_uring WAL sink. Implies `persistence`. | `io-uring` |
| `bench-internals` | Internal handles (e.g. `BenchWal`) for the benchmark crates. Implies `persistence`. **Not public API.** | `memmap2` |
| `mutation-testing` | Test-only fault injection, for the Elle consistency harness (isolation/merge logic switches, task47) and for the in-flight WAL fault tests (I/O faults — a partial `write`, a failing `fsync`, a torn frame; task60). Compiled into no normal build; inert unless the `ULTIMA_MUTATION` environment variable is set. **Not public API.** | — |

docs.rs builds the API documentation with `persistence`, `fulltext`, and
`metrics` enabled.

The companion crate `ultima-vector` has its own `persistence` feature, which
forwards to `ultima-db/persistence`.
