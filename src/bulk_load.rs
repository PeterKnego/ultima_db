//! Bulk-load API.
//!
//! Provides a fast path for ingesting many rows at once, primarily for full
//! restores and incremental delta application. Builds a fresh data B-tree
//! and indexes from sorted input, then atomically installs the result as a
//! new MVCC snapshot. See `docs/tasks/task23_bulk_load.md`.

#![allow(dead_code)]

/// Top-level shape of a bulk load: replace the table or apply a delta.
pub enum BulkLoadInput<R> {
    /// Replace all table contents with the provided rows. Index *definitions*
    /// on an existing table are preserved; index *data* is rebuilt.
    Replace(BulkSource<R>),
    /// Apply a delta on top of current contents. Validated up front; any
    /// failure leaves the store unchanged.
    Delta(BulkDelta<R>),
}

/// Source of rows for a `Replace` bulk load.
pub enum BulkSource<R> {
    /// Strictly-ascending `(id, R)` pairs. Fastest path.
    Sorted(Box<dyn Iterator<Item = (u64, R)> + Send>),
    /// Arbitrary-order `(id, R)` pairs. Sorted internally.
    Unsorted(Box<dyn Iterator<Item = (u64, R)> + Send>),
    /// Records with no caller-supplied IDs. Auto-assigned `1..=N`.
    AutoId(Box<dyn Iterator<Item = R> + Send>),
}

impl<R: Send + 'static> BulkSource<R> {
    /// Build a `Sorted` source from an in-memory vector.
    pub fn sorted_vec(v: Vec<(u64, R)>) -> Self {
        Self::Sorted(Box::new(v.into_iter()))
    }
    /// Build an `Unsorted` source from an in-memory vector.
    pub fn unsorted_vec(v: Vec<(u64, R)>) -> Self {
        Self::Unsorted(Box::new(v.into_iter()))
    }
    /// Build an `AutoId` source from an in-memory vector.
    pub fn auto_id_vec(v: Vec<R>) -> Self {
        Self::AutoId(Box::new(v.into_iter()))
    }
}

/// Mixed-op delta. Each bucket is validated and applied independently.
pub struct BulkDelta<R> {
    /// Rows to insert. Caller asserts these IDs are not present.
    pub inserts: Vec<(u64, R)>,
    /// Rows to overwrite. Caller asserts these IDs are present.
    pub updates: Vec<(u64, R)>,
    /// IDs to remove.
    pub deletes: Vec<u64>,
}

impl<R> Default for BulkDelta<R> {
    fn default() -> Self {
        Self {
            inserts: vec![],
            updates: vec![],
            deletes: vec![],
        }
    }
}

/// Options controlling persistence and table creation.
pub struct BulkLoadOptions {
    /// If true and the target table doesn't exist, create it (Replace only).
    /// If false and the table is missing, returns `Error::TableNotFound`.
    pub create_if_missing: bool,
    /// If true, after install write a checkpoint and prune the WAL up to
    /// the new version. No-op for `Persistence::OffDisk`.
    pub checkpoint_after: bool,
}

impl Default for BulkLoadOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            checkpoint_after: true,
        }
    }
}

// Internal helpers used by Store::bulk_load are added in Phase 5/6.
