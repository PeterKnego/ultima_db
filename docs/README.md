# UltimaDB documentation

UltimaDB is an embedded, transactional, MVCC store for Rust: data lives in
typed tables inside your process, every commit produces a new readable
version, and durability, concurrent writers, and serializable isolation are
opt-in. This documentation set is organized by what you are trying to do.

## Learn it

New to UltimaDB? The tutorial builds a small, working program from nothing
and touches everything you need for a first real project — tables,
transactions, versions, and an index.

- [Getting started: build a task tracker](tutorials/getting-started.md)

## Get something done

The [how-to guides](how-to/README.md) each solve one real problem — keeping
data across restarts, retrying write conflicts, replicating to a follower,
migrating between versions — for someone already comfortable with the
basics.

## Look something up

The API reference lives in rustdoc: [docs.rs/ultima-db](https://docs.rs/ultima-db)
(and [docs.rs/ultima-vector](https://docs.rs/ultima-vector) for the vector
crate). The [reference pages here](reference/README.md) cover what no single
API item can: configuration and its legal combinations, cargo features,
isolation guarantees, on-disk formats, and current performance numbers.

## Understand it

The [explanation pages](explanation/README.md) are for reading away from the
keyboard: how the copy-on-write architecture works and why it was chosen,
what the isolation levels really promise, how the engine is verified, and
how it compares with LMDB and RocksDB.

---

Contributors: per-feature design records live in the repo's `docs/tasks/` directory, dated
benchmark run records in [`benchmarks/`](benchmarks/), and the build/test
workflow in the repository root's `CLAUDE.md`. The record of how this
documentation set itself was written — which quadrants were covered, in which
run, and what was approved — is [`diataxis-plan.md`](diataxis-plan.md).
