// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UltimaDB — a high-performance transactional embedded database built on
//! a persistent copy-on-write B-tree (in-memory, with opt-in durability).
//!
//! Every commit produces a new immutable snapshot sharing unchanged
//! subtrees with its predecessors: [`ReadTx`] pins a snapshot for
//! zero-copy reads (latest or any retained historical version), while
//! [`WriteTx`] mutates lazily-copied tables and installs atomically on
//! [`WriteTx::commit`].
//!
//! # Examples
//!
//! ```
//! use ultima_db::Store;
//!
//! let store = Store::default();
//!
//! // Write a snapshot.
//! let mut wtx = store.begin_write(None).unwrap();
//! let mut users = wtx.open_table::<String>("users").unwrap();
//! let id = users.insert("alice".to_string()).unwrap();
//! let v1 = wtx.commit().unwrap();
//!
//! // Read it back — and keep reading it, even as later commits land.
//! let rtx = store.begin_read(Some(v1)).unwrap();
//! assert_eq!(rtx.open_table::<String>("users").unwrap().get(id),
//!            Some(&"alice".to_string()));
//!
//! // Or key the table yourself. `put` replaces `insert`, since there is no
//! // counter for a key the store cannot generate.
//! let mut wtx = store.begin_write(None).unwrap();
//! let mut emails = wtx.open_table_keyed::<String, String>("by_email").unwrap();
//! emails.put("alice@example.com".to_string(), "alice".to_string()).unwrap();
//! drop(emails);
//! wtx.commit().unwrap();
//! ```
//!
//! # Where to start
//!
//! Start at [`Store`] and [`StoreConfig`]:
//!
//! - [`WriterMode::MultiWriter`] enables concurrent writers with key-level
//!   optimistic concurrency control; [`IsolationLevel::Serializable`] adds
//!   write-skew prevention (SSI).
//! - The `persistence` cargo feature adds WAL + checkpoint durability
//!   ([`Persistence::Standalone`]) or checkpoint-only SMR mode
//!   ([`Persistence::Smr`]) — see [`Store::register_table`],
//!   [`Store::recover`], and [`Store::checkpoint`]. Items behind the
//!   feature carry an "Available on crate feature `persistence` only"
//!   badge on docs.rs.
//! - Bulk restores and deltas go through [`Store::bulk_load`] /
//!   [`Store::bulk_load_batch`].
//!
//! # Correctness
//!
//! The engine's correctness case — Elle consistency checking of MultiWriter
//! histories, machine-checked proofs of the B-tree, the crash-recovery
//! contract, and what each layer does and does not cover — is laid out in
//! [How UltimaDB is verified](https://github.com/PeterKnego/ultima_db/blob/main/docs/explanation/how-ultimadb-is-verified.md).
//!
//! # Further reading
//!
//! Tutorials, how-to guides, reference pages, and design explanations live
//! in the repository under
//! [`docs/`](https://github.com/PeterKnego/ultima_db/blob/main/docs/README.md);
//! the architecture is explained in
//! [`docs/explanation/architecture.md`](https://github.com/PeterKnego/ultima_db/blob/main/docs/explanation/architecture.md).
//!
// Without the `persistence` feature the three `Store` methods linked above
// do not exist, so the shortcut links would be reported as broken by
// `rustdoc::broken_intra_doc_links`. Point them at the `persistence` module
// page instead in that configuration; with the feature on, the shortcut
// links resolve to the methods and these definitions are not needed. The
// blank `//!` line above is load-bearing: a CommonMark link reference
// definition cannot interrupt a paragraph.
#![cfg_attr(not(feature = "persistence"), doc = "[`Store::register_table`]: persistence")]
#![cfg_attr(not(feature = "persistence"), doc = "[`Store::recover`]: persistence")]
#![cfg_attr(not(feature = "persistence"), doc = "[`Store::checkpoint`]: persistence")]
#![cfg_attr(docsrs, feature(doc_cfg))]

#![warn(missing_docs)]

/// The persistent copy-on-write B-tree (`BTree<K, V>`) that backs every
/// `Table`. Mutations return a new tree sharing unchanged subtrees with the
/// original via `Arc`, so old versions stay alive at O(1) clone cost.
pub mod btree;
pub mod bulk_load;
mod child;
#[cfg(feature = "persistence")]
pub(crate) mod checkpoint;
/// Crate-wide [`Error`] and [`Result`] types returned by fallible store,
/// table, and transaction operations.
pub mod error;
/// BM25 full-text search over a table's records, gated by the `fulltext`
/// cargo feature. Tokenization is Unicode-aware (split on
/// `!char::is_alphanumeric`, lowercased): CJK runs without spaces stay a
/// single token, and NFC-normalized input is recommended. Usage is covered in
/// [the indexes how-to](https://github.com/PeterKnego/ultima_db/blob/main/docs/how-to/query-with-indexes.md).
#[cfg(feature = "fulltext")]
pub mod fulltext;
/// Secondary index infrastructure: unique, non-unique, and custom indexes
/// maintained automatically on insert/update/delete.
pub mod index;
pub(crate) mod intents;
pub mod metrics;
#[cfg(feature = "mutation-testing")]
pub(crate) mod mutation;
mod overlay;
#[cfg(feature = "persistence")]
mod pagecodec;
#[cfg(feature = "persistence")]
mod pagefile;
pub mod persistence;
pub mod primary_key;
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

pub use btree::{BTree, BTreeDiff, Change};
pub use bulk_load::{
    AddOptions, BulkDelta, BulkLoadBatch, BulkLoadInput, BulkLoadOptions, BulkSource,
};
pub use error::{Error, Result};
#[cfg(feature = "fulltext")]
pub use fulltext::{FullTextIndex, SearchResult};
pub use index::{CustomIndex, IndexKind};
#[cfg(feature = "persistence")]
pub use index::IndexDef;
pub use intents::CommitWaiter;
pub use metrics::{IndexMetricsSnapshot, MetricsSnapshot, TableMetricsSnapshot};
pub use persistence::{Durability, Persistence, Record, WalWrite};
pub use primary_key::{AutoKey, PrimaryKey};
#[cfg(feature = "persistence")]
pub use snapshot_stream::SnapshotReader;
pub use snapshot_stream::{InstallOptions, OnExtra, OnUnknown, SnapshotStreamError};
pub use store::{IsolationLevel, Readable, Store, StoreConfig, VersionPin, WriterMode};
pub use table::{Table, TableDef, TableOpener};
pub use transaction::{ReadTx, TableReader, TableWriter, WriteTx};

#[cfg(feature = "persistence")]
#[doc(hidden)]
pub fn wal_durable_len_for_test(path: &std::path::Path) -> u64 {
    crate::wal::scan_wal(path, true).unwrap().1
}

/// Is the checkpoint file at `path` a full checkpoint (`true`) or a delta
/// (`false`)? Test-only escape hatch so integration tests can assert a file's
/// kind through the crate's own header reader rather than a hard-coded byte
/// offset. Not part of the stable public API.
#[cfg(feature = "persistence")]
#[doc(hidden)]
pub fn checkpoint_is_full_for_test(path: &std::path::Path) -> Result<bool> {
    crate::checkpoint::is_full_checkpoint(path)
}
