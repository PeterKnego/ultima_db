// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

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
