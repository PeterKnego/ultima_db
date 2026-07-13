// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UltimaDB — an embedded, in-memory MVCC store built on a persistent
//! copy-on-write B-tree.
//!
//! Every commit produces a new immutable snapshot sharing unchanged
//! subtrees with its predecessors: [`ReadTx`] pins a snapshot for
//! zero-copy reads (latest or any retained historical version), while
//! [`WriteTx`] mutates lazily-copied tables and installs atomically on
//! [`WriteTx::commit`].
//!
//! Start at [`Store`] and [`StoreConfig`]:
//!
//! - [`WriterMode::MultiWriter`] enables concurrent writers with key-level
//!   optimistic concurrency control; [`IsolationLevel::Serializable`] adds
//!   write-skew prevention (SSI).
//! - The `persistence` cargo feature adds WAL + checkpoint durability
//!   (`Persistence::Standalone`) or checkpoint-only SMR mode
//!   (`Persistence::Smr`) — see `Store::register_table`, `Store::recover`,
//!   and `Store::checkpoint`.
//! - Bulk restores and deltas go through `Store::bulk_load` /
//!   `Store::bulk_load_batch`.
//!
//! Design documents for each subsystem live in `docs/tasks/` in the
//! repository.

#![warn(missing_docs)]

/// The persistent copy-on-write B-tree (`BTree<K, V>`) that backs every
/// `Table`. Mutations return a new tree sharing unchanged subtrees with the
/// original via `Arc`, so old versions stay alive at O(1) clone cost.
pub mod btree;
pub mod bulk_load;
#[cfg(feature = "persistence")]
pub(crate) mod checkpoint;
/// Crate-wide [`Error`] and [`Result`] types returned by fallible store,
/// table, and transaction operations.
pub mod error;
/// BM25 full-text search over a table's records, gated by the `fulltext`
/// cargo feature. See `docs/tasks/task43_fulltext_search.md`.
#[cfg(feature = "fulltext")]
pub mod fulltext;
/// Secondary index infrastructure: unique, non-unique, and custom indexes
/// maintained automatically on insert/update/delete.
pub mod index;
pub(crate) mod intents;
pub mod metrics;
#[cfg(feature = "mutation-testing")]
pub(crate) mod mutation;
pub mod persistence;
#[cfg(feature = "persistence")]
pub(crate) mod registry;
pub mod snapshot_stream;
/// [`Store`], `Snapshot`, and the [`ReadTx`]/[`WriteTx`] transaction types
/// that implement the MVCC commit protocol.
pub mod store;
/// [`Table<R>`], a typed collection wrapping `BTree<u64, R>` with
/// auto-incrementing ids, secondary indexes, and batch operations.
pub mod table;
/// Re-exports [`ReadTx`]/[`WriteTx`] (defined in `store` to avoid a circular
/// module dependency) under a semantically clearer import path.
pub mod transaction;
#[cfg(feature = "persistence")]
pub mod wal;

/// Real-disk scratch dirs for durability unit tests. Also re-used by
/// integration tests via `tests/common/mod.rs` (`#[path]`-included). Gated the
/// same as its only in-crate consumers (`wal`/`checkpoint` unit tests).
#[cfg(all(test, feature = "persistence"))]
mod test_scratch;

pub use btree::BTree;
pub use bulk_load::{
    AddOptions, BulkDelta, BulkLoadBatch, BulkLoadInput, BulkLoadOptions, BulkSource,
};
pub use error::{Error, Result};
#[cfg(feature = "fulltext")]
pub use fulltext::{FullTextIndex, SearchResult};
pub use index::{CustomIndex, IndexKind};
pub use intents::CommitWaiter;
pub use metrics::{IndexMetricsSnapshot, MetricsSnapshot, TableMetricsSnapshot};
pub use persistence::{Durability, Persistence, Record, WalWrite};
#[cfg(feature = "persistence")]
pub use snapshot_stream::SnapshotReader;
pub use snapshot_stream::{InstallOptions, OnExtra, OnUnknown, SnapshotStreamError};
pub use store::{IsolationLevel, Readable, Store, StoreConfig, WriterMode};
pub use table::{Table, TableDef, TableOpener};
pub use transaction::{ReadTx, TableReader, TableWriter, WriteTx};

#[cfg(feature = "persistence")]
#[doc(hidden)]
pub fn wal_durable_len_for_test(path: &std::path::Path) -> u64 {
    crate::wal::scan_wal(path, true).unwrap().1
}
