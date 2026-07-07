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

pub mod btree;
pub mod bulk_load;
#[cfg(feature = "persistence")]
pub(crate) mod checkpoint;
pub mod error;
#[cfg(feature = "fulltext")]
pub mod fulltext;
pub mod index;
pub(crate) mod intents;
pub mod metrics;
#[cfg(feature = "mutation-testing")]
pub(crate) mod mutation;
pub mod persistence;
#[cfg(feature = "persistence")]
pub(crate) mod registry;
pub mod snapshot_stream;
pub mod store;
pub mod table;
pub mod transaction;
#[cfg(feature = "persistence")]
pub mod wal;

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
