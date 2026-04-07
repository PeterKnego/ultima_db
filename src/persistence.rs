//! Persistence configuration and the [`Record`] trait.
//!
//! The `persistence` cargo feature gates Serde bounds on record types.
//! Without the feature, `Record` is just `Send + Sync + 'static`.
//! With it, `Serialize + DeserializeOwned` are additionally required.

use std::path::PathBuf;

/// Marker trait that centralises the bounds every record type must satisfy.
///
/// When the `persistence` feature is **disabled**, this is equivalent to
/// `Send + Sync + 'static`.
///
/// When the `persistence` feature is **enabled**, record types must also
/// implement `serde::Serialize` and `serde::de::DeserializeOwned` so that
/// they can be written to the WAL and checkpoints.
#[cfg(feature = "persistence")]
pub trait Record: Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static {}

#[cfg(feature = "persistence")]
impl<T: Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static> Record for T {}

#[cfg(not(feature = "persistence"))]
pub trait Record: Send + Sync + 'static {}

#[cfg(not(feature = "persistence"))]
impl<T: Send + Sync + 'static> Record for T {}

// ---------------------------------------------------------------------------
// Durability — controls WAL fsync behavior
// ---------------------------------------------------------------------------

/// Controls WAL fsync behavior (Standalone mode only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// `commit()` returns immediately. A background thread fsyncs the WAL
    /// asynchronously. Data may be lost on crash (last unflushed entries).
    Eventual,
    /// `commit()` blocks until the WAL entry is written and fsynced.
    /// No data loss on crash.
    Consistent,
}

// ---------------------------------------------------------------------------
// Persistence — persistence mode
// ---------------------------------------------------------------------------

/// Persistence mode for a [`Store`](crate::Store).
#[derive(Debug, Clone, Default)]
pub enum Persistence {
    /// In-memory only. No disk I/O. Default.
    #[default]
    None,
    /// UltimaDB owns durability. WAL for transaction durability, checkpoints
    /// for fast recovery. WAL is auto-pruned on checkpoint.
    Standalone {
        /// Directory for WAL and checkpoint files.
        dir: PathBuf,
        /// WAL fsync behavior.
        durability: Durability,
    },
    /// Consensus log owns durability. Checkpoints only — no WAL.
    /// Used in SMR deployments where the Raft/Paxos log provides durability.
    Smr {
        /// Directory for checkpoint files.
        dir: PathBuf,
    },
}

