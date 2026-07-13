// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Error type for `ultima_vector`.

use thiserror::Error;

/// Errors returned by `ultima_vector`'s public API.
#[derive(Debug, Error)]
pub enum Error {
    /// An embedding or query's length didn't match the collection's
    /// configured `HnswParams::dim`. Raised on insert, update, search, and
    /// restore.
    #[error("vector dim mismatch: expected {expected}, got {got}")]
    DimMismatch {
        /// The collection's configured dimensionality.
        expected: usize,
        /// The length actually supplied.
        got: usize,
    },

    /// An embedding or query contained a NaN or ±Infinity component.
    /// Rejected at every `VectorCollection` boundary so the graph can never
    /// contain a non-finite value (see `docs/tasks/task40`).
    #[error("non-finite value ({value}) at index {index} in vector")]
    NonFinite {
        /// Position of the offending component within the vector.
        index: usize,
        /// The non-finite value found there.
        value: f32,
    },

    /// No live node exists at the given id — it was never inserted, or was
    /// already deleted (tombstoned) and is now being referenced directly
    /// rather than through a re-scan.
    #[error("node id {0} not found")]
    NodeNotFound(u64),

    /// An underlying `ultima_db` operation (table open, commit, bulk load)
    /// failed; see the wrapped error for the cause.
    #[error("ultima_db error: {0}")]
    Storage(#[from] ultima_db::Error),

    /// `HnswParams` failed validation in [`VectorCollection::open`](crate::VectorCollection::open)
    /// (e.g. `dim == 0`, `m < 2`, `m_max0 < m`).
    #[error("invalid params: {0}")]
    InvalidParams(&'static str),

    /// A restored row's `HnswState` violated the layer-count invariant
    /// (`layers.len() == level + 1`) — a malformed backup or a bug in the
    /// data producing the restore stream. `restore_iter` validates every
    /// row before installing any of them.
    #[error("invalid HnswState at id {id}: level={level} but layers.len()={layers}")]
    InvalidHnswState {
        /// Row id of the malformed entry.
        id: u64,
        /// The node's recorded top level.
        level: u8,
        /// The actual number of layer-adjacency entries found.
        layers: usize,
    },
}

/// `Result` alias for fallible `ultima_vector` operations.
pub type Result<T> = std::result::Result<T, Error>;
