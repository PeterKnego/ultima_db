//! Bulk-load API.
//!
//! Provides a fast path for ingesting many rows at once, primarily for full
//! restores and incremental delta application. Builds a fresh data B-tree
//! and indexes from sorted input, then atomically installs the result as a
//! new MVCC snapshot. See `docs/tasks/task23_bulk_load.md`.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{Error, Result};

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

// ---------------------------------------------------------------------------
// Internal helpers used by Store::bulk_load
// ---------------------------------------------------------------------------

/// Result of materializing a `BulkSource` into sorted, deduped rows ready
/// for `Table::from_bulk`. `max_id` is the largest assigned id, used to
/// initialize the new table's `next_id`.
pub(crate) struct MaterializedRows<R> {
    pub rows: Vec<(u64, Arc<R>)>,
    pub max_id: Option<u64>,
}

/// Drain a `BulkSource` into a strictly-ascending `Vec<(u64, Arc<R>)>`.
/// Validates ordering and rejects duplicate ids; for `AutoId`, assigns
/// sequential ids starting at `1`.
pub(crate) fn materialize_source<R>(src: BulkSource<R>) -> Result<MaterializedRows<R>>
where
    R: Send + Sync + 'static,
{
    match src {
        BulkSource::Sorted(iter) => {
            let rows: Vec<(u64, Arc<R>)> = iter.map(|(id, r)| (id, Arc::new(r))).collect();
            for w in rows.windows(2) {
                if w[0].0 == w[1].0 {
                    return Err(Error::InvalidBulkLoadInput(format!(
                        "duplicate id {} in sorted source",
                        w[0].0
                    )));
                }
                if w[0].0 > w[1].0 {
                    return Err(Error::InvalidBulkLoadInput(format!(
                        "Sorted source not ascending: {} > {}",
                        w[0].0, w[1].0
                    )));
                }
            }
            let max_id = rows.last().map(|(id, _)| *id);
            Ok(MaterializedRows { rows, max_id })
        }
        BulkSource::Unsorted(iter) => {
            let mut rows: Vec<(u64, Arc<R>)> = iter.map(|(id, r)| (id, Arc::new(r))).collect();
            rows.sort_unstable_by_key(|(id, _)| *id);
            for w in rows.windows(2) {
                if w[0].0 == w[1].0 {
                    return Err(Error::InvalidBulkLoadInput(format!(
                        "duplicate id {}",
                        w[0].0
                    )));
                }
            }
            let max_id = rows.last().map(|(id, _)| *id);
            Ok(MaterializedRows { rows, max_id })
        }
        BulkSource::AutoId(iter) => {
            let rows: Vec<(u64, Arc<R>)> = iter
                .enumerate()
                .map(|(i, r)| ((i as u64) + 1, Arc::new(r)))
                .collect();
            let max_id = rows.last().map(|(id, _)| *id);
            Ok(MaterializedRows { rows, max_id })
        }
    }
}

/// Validate and materialize a delta against a captured base data tree,
/// producing the new sorted rows that the bulk loader will hand to
/// `Table::from_bulk`.
///
/// Validation order matters: bucket-internal duplicates first, then
/// cross-bucket overlap, then existence checks against `base`. Callers
/// receive the most-local error first.
pub(crate) fn materialize_delta<R>(
    delta: BulkDelta<R>,
    base: &crate::btree::BTree<u64, R>,
) -> Result<MaterializedRows<R>>
where
    R: Send + Sync + 'static,
{
    let BulkDelta {
        mut inserts,
        mut updates,
        mut deletes,
    } = delta;

    // 1. Within-bucket dedup checks.
    inserts.sort_by_key(|(id, _)| *id);
    updates.sort_by_key(|(id, _)| *id);
    deletes.sort_unstable();
    for w in inserts.windows(2) {
        if w[0].0 == w[1].0 {
            return Err(Error::InvalidBulkLoadInput(format!(
                "duplicate id {} in inserts",
                w[0].0
            )));
        }
    }
    for w in updates.windows(2) {
        if w[0].0 == w[1].0 {
            return Err(Error::InvalidBulkLoadInput(format!(
                "duplicate id {} in updates",
                w[0].0
            )));
        }
    }
    for w in deletes.windows(2) {
        if w[0] == w[1] {
            return Err(Error::InvalidBulkLoadInput(format!(
                "duplicate id {} in deletes",
                w[0]
            )));
        }
    }

    // 2. Cross-bucket overlap.
    let mut seen: HashSet<u64> = HashSet::new();
    for (id, _) in &inserts {
        if !seen.insert(*id) {
            return Err(Error::InvalidBulkLoadInput(format!(
                "id {id} appears in multiple buckets"
            )));
        }
    }
    for (id, _) in &updates {
        if !seen.insert(*id) {
            return Err(Error::InvalidBulkLoadInput(format!(
                "id {id} appears in multiple buckets"
            )));
        }
    }
    for id in &deletes {
        if !seen.insert(*id) {
            return Err(Error::InvalidBulkLoadInput(format!(
                "id {id} appears in multiple buckets"
            )));
        }
    }

    // 3. Existence checks against base.
    for (id, _) in &updates {
        if base.get(id).is_none() {
            return Err(Error::KeyNotFound);
        }
    }
    for id in &deletes {
        if base.get(id).is_none() {
            return Err(Error::KeyNotFound);
        }
    }
    for (id, _) in &inserts {
        if base.get(id).is_some() {
            return Err(Error::DuplicateKey(format!(
                "bulk_delta insert id {id}"
            )));
        }
    }

    // 4. Materialize the merged sorted row vector.
    //    Walk base in id-order; skip ids in `deletes`; replace ids in
    //    `updates`; sort-merge inserts into the stream.
    let updates_map: HashMap<u64, Arc<R>> =
        updates.into_iter().map(|(id, r)| (id, Arc::new(r))).collect();
    let deletes_set: HashSet<u64> = deletes.into_iter().collect();

    let mut base_iter = base.range(..).peekable();
    let mut insert_iter = inserts
        .into_iter()
        .map(|(id, r)| (id, Arc::new(r)))
        .peekable();

    let mut rows: Vec<(u64, Arc<R>)> = Vec::new();
    loop {
        // BTreeRange yields (&u64, &R), so peek returns Option<&(&u64, &R)>.
        let take_base = match (base_iter.peek(), insert_iter.peek()) {
            (Some((bid, _)), Some((iid, _))) => **bid < *iid,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_base {
            let (id_ref, _) = base_iter.next().unwrap();
            let id = *id_ref;
            if deletes_set.contains(&id) {
                continue;
            }
            if let Some(new_rec) = updates_map.get(&id) {
                rows.push((id, new_rec.clone()));
            } else {
                // Reuse the existing Arc by cloning out of the data tree.
                let arc = base.get_arc(&id).expect("present per peek");
                rows.push((id, arc));
            }
        } else {
            let (id, arc) = insert_iter.next().unwrap();
            rows.push((id, arc));
        }
    }

    let max_id = rows.last().map(|(id, _)| *id);
    Ok(MaterializedRows { rows, max_id })
}
