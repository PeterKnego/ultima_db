# Explanation

Pages for understanding rather than doing — the design, its reasons, its
trade-offs, and its honest limits. Read them away from the keyboard.

- [Architecture](architecture.md) — the copy-on-write B-tree at the heart of
  the engine, why MVCC snapshots are nearly free, how commits merge, and the
  design-decision record.
- [Isolation: what snapshot isolation gives you](isolation.md) — why
  SnapshotIsolation is the default, what write skew is, and what
  Serializable actually buys.
- [How UltimaDB is verified](how-ultimadb-is-verified.md) — Elle
  consistency checking, machine-checked proofs, the crash-recovery
  contract, and what each layer can and cannot catch.
- [Reading our benchmark numbers](reading-our-benchmarks.md) — what the
  performance tables can honestly claim, and why absolute numbers don't
  travel between machines.
- [Vector search: HNSW and its trade-offs](vector-search.md) — why
  approximate nearest-neighbor search is the right trade and what the knobs
  really do.
- [UltimaDB compared with LMDB and RocksDB](compared-with-lmdb-and-rocksdb.md)
  — where it sits in the embedded-database design space.
