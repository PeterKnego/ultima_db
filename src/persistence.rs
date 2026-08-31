// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Persistence configuration and the [`Record`] trait.
//!
//! The `persistence` cargo feature gates Serde bounds on record types.
//! Without the feature, `Record` is just `Send + Sync + 'static`.
//! With it, `Serialize + DeserializeOwned` are additionally required.

use std::path::PathBuf;
use std::time::Duration;

use crate::{Error, Result};

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

/// Marker trait that centralises the bounds every record type must satisfy.
///
/// The `persistence` feature is **disabled** in this build, so this is
/// equivalent to `Send + Sync + 'static`. With the feature enabled, record
/// types must additionally implement `serde::Serialize` and
/// `serde::de::DeserializeOwned` so they can be written to the WAL and
/// checkpoints.
#[cfg(not(feature = "persistence"))]
pub trait Record: Send + Sync + 'static {}

#[cfg(not(feature = "persistence"))]
impl<T: Send + Sync + 'static> Record for T {}

// ---------------------------------------------------------------------------
// Durability — controls WAL fsync behavior
// ---------------------------------------------------------------------------

/// Controls WAL fsync behavior (Standalone mode only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
pub enum Persistence {
    /// In-memory only. No disk I/O. Default.
    #[default]
    None,
    /// UltimaDB owns durability. WAL for transaction durability, checkpoints
    /// for fast recovery. WAL is auto-pruned on checkpoint.
    ///
    /// Construct via [`Persistence::standalone`] / [`Persistence::standalone_fast`].
    #[non_exhaustive]
    Standalone {
        /// Directory for WAL and checkpoint files.
        dir: PathBuf,
        /// WAL fsync behavior.
        durability: Durability,
        /// How the WAL writes each committed batch to disk.
        wal_write: WalWrite,
        /// Paged-checkpoint options, if this mode was built with
        /// [`Persistence::paged`]. `None` (the default from
        /// [`Persistence::standalone`]) means row-format checkpoints.
        paged: Option<PagedOptions>,
    },
    /// Consensus log owns durability. Checkpoints only — no WAL.
    /// Used in SMR deployments where the Raft/Paxos log provides durability.
    ///
    /// Construct via [`Persistence::smr`].
    #[non_exhaustive]
    Smr {
        /// Directory for checkpoint files.
        dir: PathBuf,
        /// Paged-checkpoint options, if this mode was built with
        /// [`Persistence::paged`]. `None` (the default from
        /// [`Persistence::smr`]) means row-format checkpoints.
        paged: Option<PagedOptions>,
    },
}

impl Persistence {
    /// Recommended fast durable config for a **single-writer** store: inline-fsync
    /// ([`Durability::ConsistentInline`]) + preallocation ([`WalWrite::CoalescedPrealloc`])
    /// — the lowest durable-commit latency (validated ~3.8× vs the `PerEntry` default
    /// on NVMe; see
    /// [the task38 design notes](https://github.com/PeterKnego/ultima_db/blob/main/docs/tasks/task38_wal_inline_fsync.md)).
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
            paged: None,
        }
    }

    /// Construct a [`Persistence::Standalone`] config: UltimaDB owns
    /// durability via WAL + checkpoints in `dir`.
    pub fn standalone(
        dir: impl Into<PathBuf>,
        durability: Durability,
        wal_write: WalWrite,
    ) -> Self {
        Persistence::Standalone {
            dir: dir.into(),
            durability,
            wal_write,
            paged: None,
        }
    }

    /// Construct a [`Persistence::Smr`] config: checkpoint-only, for
    /// deployments where a consensus log provides durability.
    pub fn smr(dir: impl Into<PathBuf>) -> Self {
        Persistence::Smr { dir: dir.into(), paged: None }
    }

    /// This persistence mode's paged-checkpoint options, if it was built
    /// with [`Persistence::paged`]. `None` for a plain row-format-checkpoint
    /// store, and always `None` for [`Persistence::None`] (which cannot
    /// carry them at all — see [`Persistence::paged`]'s doc).
    ///
    /// Crate-private and only called from `Store::new`'s
    /// `persistence`-gated page-file setup — gated the same way so it does
    /// not go dead in a build without the feature (`PagedOptions` itself
    /// stays unconditional, matching `Persistence`'s other config types).
    #[cfg(feature = "persistence")]
    pub(crate) fn paged_opts(&self) -> Option<&PagedOptions> {
        match self {
            Persistence::Standalone { paged, .. } => paged.as_ref(),
            Persistence::Smr { paged, .. } => paged.as_ref(),
            Persistence::None => None,
        }
    }

    /// Opt this persistence mode into paged checkpoints: an on-disk B-tree
    /// node store (`pages.bin`) that a table pages against once attached,
    /// instead of a row-format checkpoint that always (de)serializes every
    /// row on every checkpoint.
    ///
    /// Only [`Persistence::Standalone`] and [`Persistence::Smr`] have a
    /// directory to put `pages.bin` in; [`Persistence::None`] has none, so
    /// this rejects it with [`Error::Persistence`] rather than silently
    /// discarding `opts`.
    pub fn paged(self, opts: PagedOptions) -> Result<Self> {
        match self {
            Persistence::Standalone {
                dir,
                durability,
                wal_write,
                ..
            } => Ok(Persistence::Standalone {
                dir,
                durability,
                wal_write,
                paged: Some(opts),
            }),
            Persistence::Smr { dir, .. } => Ok(Persistence::Smr {
                dir,
                paged: Some(opts),
            }),
            Persistence::None => Err(Error::Persistence(
                "paged checkpoints require Standalone or Smr persistence".into(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// PagedOptions — tuning knobs for the paged checkpoint path
// ---------------------------------------------------------------------------

/// Tuning knobs for a store's paged checkpoint path: the on-disk B-tree node
/// store (`pages.bin`) a table pages against once attached via
/// [`Persistence::paged`], instead of a row-format checkpoint.
///
/// Build with [`PagedOptions::builder`]. `Default` gives every field its
/// documented default.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PagedOptions {
    /// Soft cap on total resident (not-yet-demoted) leaf bytes across every
    /// paged table, past which the checkpointer's demote pass (a later
    /// task) starts evicting quiet leaves back to disk. `None` disables
    /// demotion entirely: every leaf, once faulted in, stays resident.
    /// Default: `None`.
    pub memory_budget_bytes: Option<u64>,
    /// A checkpoint runs once this many dirty bytes have accumulated since
    /// the last one — the background checkpointer's (a later task) volume
    /// trigger. Default: 256 MiB.
    pub checkpoint_dirty_bytes: u64,
    /// A checkpoint runs at least this often regardless of dirty volume —
    /// the background checkpointer's time trigger (task12). Default:
    /// `Some(60s)` — a backstop so no configuration leaves every trigger
    /// false (`checkpoint_dirty_bytes` alone can go unmet for a long time
    /// on a low-write workload, and `memory_budget_bytes` defaults to
    /// `None`); set `None` explicitly to disable time-based checkpoints
    /// and rely only on `checkpoint_dirty_bytes`/`memory_budget_bytes`/
    /// manual [`Store::checkpoint`](crate::Store::checkpoint) calls.
    pub checkpoint_interval: Option<Duration>,
    /// Leaf-parents processed per demote pass batch (a later task).
    /// Default: 1024.
    pub demote_batch: usize,
    /// First-read buffer size for a page fault — big enough that most
    /// pages are satisfied by one positioned read (see `PageFile::open`'s
    /// `prefetch` parameter). Default: 4096.
    pub page_prefetch_bytes: usize,
    /// Grow-ahead chunk size for the page file: its physical extent grows
    /// this much at a time, ahead of the write cursor (see
    /// `PageFile::open`'s `chunk` parameter). Default: 16 MiB.
    pub prealloc_chunk_bytes: u64,
    /// How many `checkpoint_{v}.root` files to retain — older ones are
    /// pruned after each successful checkpoint. Default: 2 (the current
    /// root plus one prior, so a crash mid-write of the newest root still
    /// leaves a complete, readable one).
    pub retained_checkpoints: usize,
}

impl Default for PagedOptions {
    fn default() -> Self {
        Self {
            memory_budget_bytes: None,
            checkpoint_dirty_bytes: 256 << 20,
            checkpoint_interval: Some(Duration::from_secs(60)),
            demote_batch: 1024,
            page_prefetch_bytes: 4096,
            prealloc_chunk_bytes: 16 << 20,
            retained_checkpoints: 2,
        }
    }
}

impl PagedOptions {
    /// Start building [`PagedOptions`]. Chain the setters you need, then
    /// call [`PagedOptionsBuilder::build`]. Unset fields take their
    /// [`Default`] values.
    pub fn builder() -> PagedOptionsBuilder {
        PagedOptionsBuilder::default()
    }
}

/// Builder for [`PagedOptions`]. Obtain via [`PagedOptions::builder`].
#[derive(Clone, Debug, Default)]
pub struct PagedOptionsBuilder {
    opts: PagedOptions,
}

impl PagedOptionsBuilder {
    /// See [`PagedOptions::memory_budget_bytes`].
    pub fn memory_budget_bytes(mut self, n: u64) -> Self {
        self.opts.memory_budget_bytes = Some(n);
        self
    }
    /// See [`PagedOptions::checkpoint_dirty_bytes`].
    pub fn checkpoint_dirty_bytes(mut self, n: u64) -> Self {
        self.opts.checkpoint_dirty_bytes = n;
        self
    }
    /// See [`PagedOptions::checkpoint_interval`].
    pub fn checkpoint_interval(mut self, d: Duration) -> Self {
        self.opts.checkpoint_interval = Some(d);
        self
    }
    /// Disable the time trigger entirely — the default is `Some(60s)`
    /// (see [`PagedOptions::checkpoint_interval`]'s doc for why), so this
    /// is how a caller who wants only the dirty-bytes/memory-budget
    /// triggers (or purely manual [`Store::checkpoint`](crate::Store::checkpoint)
    /// calls) opts back out of it.
    pub fn checkpoint_interval_disabled(mut self) -> Self {
        self.opts.checkpoint_interval = None;
        self
    }
    /// See [`PagedOptions::demote_batch`].
    pub fn demote_batch(mut self, n: usize) -> Self {
        self.opts.demote_batch = n;
        self
    }
    /// See [`PagedOptions::page_prefetch_bytes`].
    pub fn page_prefetch_bytes(mut self, n: usize) -> Self {
        self.opts.page_prefetch_bytes = n;
        self
    }
    /// See [`PagedOptions::prealloc_chunk_bytes`].
    pub fn prealloc_chunk_bytes(mut self, n: u64) -> Self {
        self.opts.prealloc_chunk_bytes = n;
        self
    }
    /// See [`PagedOptions::retained_checkpoints`].
    pub fn retained_checkpoints(mut self, n: usize) -> Self {
        self.opts.retained_checkpoints = n;
        self
    }
    /// Finalize the configuration.
    pub fn build(self) -> PagedOptions {
        self.opts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_constructor_matches_literal() {
        let c = Persistence::standalone("/tmp/x", Durability::Consistent, WalWrite::Coalesced);
        match c {
            Persistence::Standalone {
                dir,
                durability,
                wal_write,
                ..
            } => {
                assert_eq!(dir, PathBuf::from("/tmp/x"));
                assert_eq!(durability, Durability::Consistent);
                assert_eq!(wal_write, WalWrite::Coalesced);
            }
            _ => panic!("expected Standalone"),
        }
    }

    #[test]
    fn smr_constructor_matches_literal() {
        match Persistence::smr("/tmp/y") {
            Persistence::Smr { dir, .. } => assert_eq!(dir, PathBuf::from("/tmp/y")),
            _ => panic!("expected Smr"),
        }
    }

    #[test]
    fn paged_rejects_persistence_none() {
        let err = Persistence::None
            .paged(PagedOptions::builder().build())
            .unwrap_err();
        assert!(matches!(err, Error::Persistence(_)));
    }

    #[test]
    #[cfg(feature = "persistence")]
    fn paged_attaches_options_to_standalone_and_smr() {
        let p = Persistence::standalone("/tmp/x", Durability::Consistent, WalWrite::Coalesced)
            .paged(PagedOptions::builder().retained_checkpoints(5).build())
            .unwrap();
        assert_eq!(p.paged_opts().unwrap().retained_checkpoints, 5);

        let p = Persistence::smr("/tmp/y")
            .paged(PagedOptions::builder().build())
            .unwrap();
        assert!(p.paged_opts().is_some());
    }

    #[test]
    fn paged_options_builder_defaults() {
        let o = PagedOptions::builder().build();
        assert_eq!(o.memory_budget_bytes, None);
        assert_eq!(o.checkpoint_dirty_bytes, 256 << 20);
        assert_eq!(
            o.checkpoint_interval,
            Some(Duration::from_secs(60)),
            "task12 fix round 1: a backstop default, not None -- see the field's doc"
        );
        assert_eq!(o.demote_batch, 1024);
        assert_eq!(o.page_prefetch_bytes, 4096);
        assert_eq!(o.prealloc_chunk_bytes, 16 << 20);
        assert_eq!(o.retained_checkpoints, 2);

        assert_eq!(
            PagedOptions::builder().checkpoint_interval_disabled().build().checkpoint_interval,
            None,
            "checkpoint_interval_disabled must be able to opt back out of the backstop"
        );
    }
}
