// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

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
    /// Same guarantee as [`Consistent`](Durability::Consistent) (commit blocks
    /// until the entry is fsynced; no data loss on crash). Differs only in
    /// mechanism: the committing thread performs the fsync itself — no WAL
    /// background thread, no cross-thread handoff. **SingleWriter only**
    /// (`Store::new` errors otherwise). Best for serial durable commits on fast
    /// disk, where the bg-thread handoff (~20-35µs) dominates a cheap fsync.
    ConsistentInline,
}

// ---------------------------------------------------------------------------
// WalWrite — how the WAL writes a committed batch to disk
// ---------------------------------------------------------------------------

/// How the WAL writes a committed batch to disk (Standalone mode).
///
/// Orthogonal to [`Durability`], which controls *when* `commit()` returns. All
/// four combinations are valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalWrite {
    /// One `write` per entry, then `sync_all` per batch. The original behavior.
    #[default]
    PerEntry,
    /// The whole batch is coalesced into a single `write`, then `sync_all`. Same
    /// durability as `PerEntry` (full fsync); fewer syscalls per batch — better
    /// group-commit throughput under Eventual / high-concurrency loads.
    Coalesced,
    /// Coalesced batch write into a **preallocated** WAL file: positioned
    /// writes overwrite a physically zero-filled region, so each Consistent
    /// commit's fsync carries no ext4 metadata commit. Steady-state uses
    /// `sync_data`; the file grows in 16 MiB chunks and is re-preallocated on
    /// prune. Opt-in; recovery uses a tail-tolerant scan. See
    /// docs/superpowers/specs/2026-06-20-wal-preallocation-design.md.
    CoalescedPrealloc,
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
        /// How the WAL writes each committed batch to disk.
        wal_write: WalWrite,
    },
    /// Consensus log owns durability. Checkpoints only — no WAL.
    /// Used in SMR deployments where the Raft/Paxos log provides durability.
    Smr {
        /// Directory for checkpoint files.
        dir: PathBuf,
    },
}

impl Persistence {
    /// Recommended fast durable config for a **single-writer** store: inline-fsync
    /// ([`Durability::ConsistentInline`]) + preallocation ([`WalWrite::CoalescedPrealloc`])
    /// — the lowest durable-commit latency (validated ~3.8× vs the `PerEntry` default
    /// on NVMe; see `docs/tasks/task38_wal_inline_fsync.md`).
    ///
    /// Same durability guarantee as [`Durability::Consistent`] (commit blocks until
    /// fsynced; no data loss on crash). **SingleWriter only** — building a store with
    /// this and [`WriterMode::MultiWriter`](crate::WriterMode::MultiWriter) returns an
    /// error from [`Store::new`](crate::Store::new) (inline cannot guarantee WAL-append
    /// order under concurrent writers). For MultiWriter, construct
    /// [`Persistence::Standalone`] with [`Durability::Consistent`].
    pub fn standalone_fast(dir: impl Into<PathBuf>) -> Self {
        Persistence::Standalone {
            dir: dir.into(),
            durability: Durability::ConsistentInline,
            wal_write: WalWrite::CoalescedPrealloc,
        }
    }
}
