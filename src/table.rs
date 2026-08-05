// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::any::Any;
#[cfg(feature = "persistence")]
use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::btree::{BTree, BTreeRange};
use crate::index::{
    CustomIndex, CustomIndexAdapter, IndexKind, IndexMaintainer, ManagedIndex, NonUniqueStorage,
    UniqueStorage,
};
use crate::overlay::{MergedIter, Overlay, OverlayOp, TableIter};
use crate::persistence::Record;
use crate::primary_key::{AutoKey, PrimaryKey};
use crate::{Error, Result};

// ---------------------------------------------------------------------------
// MergeableTable — the trait object carried in Snapshot.tables and WriteTx.dirty
// ---------------------------------------------------------------------------
//
// Supertrait `Any + Send + Sync` keeps existing downcast machinery working
// via the explicit `as_any()` accessor. `boxed_clone` is an O(1) CoW clone
// used at commit to take the latest snapshot's table and layer the writer's
// edits on top. `merge_keys_from` walks the writer's write_set and upserts
// each modified record from `source` into `self`.
//
// The trait is deliberately NOT generic over the primary key: a `Snapshot`
// holds `HashMap<String, Arc<dyn MergeableTable>>` whose tables may each be
// keyed by a different type. The two methods that need to name a key work in
// erased terms instead — `merge_keys_from` takes a `&dyn Any` that the impl
// downcasts to `&BTreeSet<K>`, and `collect_serialized_rows` hands back
// order-preserving encoded key bytes.

pub(crate) trait MergeableTable: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// The `TypeId` of this table's primary key, read off the *live* table.
    ///
    /// The registry knows a key type too, but the two can disagree: a table
    /// can be created by `open_table_keyed` without ever being registered,
    /// and `register_table*` afterwards records whatever the caller asked
    /// for. Anything making a key-shaped decision about a concrete table —
    /// `register_table_keyed`'s guard, and the key type the snapshot wire
    /// format stamps on each table header — must ask the table, not the
    /// registry, or it will act on the wrong type and reinterpret the rows.
    ///
    /// Gated like `index_list` below: every caller (the registration guard
    /// and both ends of the snapshot wire format) is persistence-only.
    #[cfg(feature = "persistence")]
    fn key_type_id(&self) -> TypeId;

    /// `std::any::type_name` of this table's primary key. Used for error
    /// messages (`TypeId` has no printable form) and carried on the snapshot
    /// wire format so the receiving end can *name* a mismatch it detects.
    #[cfg(feature = "persistence")]
    fn key_type_name(&self) -> &'static str;

    /// [`PrimaryKey::KEY_TYPE_ID`](crate::primary_key::PrimaryKey::KEY_TYPE_ID)
    /// of this table's primary key: the persisted key-type identity, and the
    /// one the wire format's mismatch check is *decided* on.
    ///
    /// `key_type_name` is neither stable across compiler versions nor
    /// injective across crate versions of a third-party key type; the id is
    /// declared by the key type itself and is neither. Read off the live
    /// table for the same reason the name is — see `key_type_id`.
    #[cfg(feature = "persistence")]
    fn key_type_code(&self) -> u32;

    /// O(1)-CoW clone (Arc bumps on the BTree root and index internals).
    fn boxed_clone(&self) -> Box<dyn MergeableTable>;

    /// For each key in `keys`, take the writer's record at that key from
    /// `source` and apply it to `self`:
    /// - `source` has a record → upsert into self (maintains indexes)
    /// - `source` does not have a record → delete from self (if present)
    ///
    /// `keys` is a `&BTreeSet<K>` erased to `&dyn Any`, because a snapshot
    /// holds tables with heterogeneous key types and `K` cannot appear in
    /// this trait's signature. The impl, which knows `K`, downcasts it; a
    /// failed downcast is an internal bug, not a user error.
    ///
    /// OCC guarantees no concurrent committed writer touched any key in
    /// `keys`, so self's state at those keys matches source's base state
    /// and the writes never fight another committer. A unique-index
    /// violation is still possible (two writers assigning the same indexed
    /// value to different rows); that bubbles up as `Error::DuplicateKey`.
    fn merge_keys_from(&mut self, source: &dyn MergeableTable, keys: &dyn Any) -> Result<()>;

    /// List of (kind_byte, name) for each secondary index.
    /// `kind_byte`: 0 = Unique, 1 = NonUnique, 2 = Custom.
    /// Used by `SnapshotReader` to emit table headers.
    #[cfg(feature = "persistence")]
    fn index_list(&self) -> Vec<(u8, String)>;

    /// Serialize every row to (encoded-key-bytes, bincode-bytes) pairs using
    /// the provided type-erased serializer from the registry. Returns them in
    /// primary-key order; the key bytes come from
    /// [`PrimaryKey::encode`](crate::primary_key::PrimaryKey::encode), whose
    /// byte order matches key order. Gated on `persistence` because it
    /// requires `serde + bincode`.
    #[cfg(feature = "persistence")]
    fn collect_serialized_rows(
        &self,
        serialize_record: &(dyn Fn(&dyn Any) -> Result<Vec<u8>> + Send + Sync),
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

impl<R: Record, K: PrimaryKey> MergeableTable for Table<R, K> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[cfg(feature = "persistence")]
    fn key_type_id(&self) -> TypeId {
        TypeId::of::<K>()
    }

    #[cfg(feature = "persistence")]
    fn key_type_name(&self) -> &'static str {
        std::any::type_name::<K>()
    }

    #[cfg(feature = "persistence")]
    fn key_type_code(&self) -> u32 {
        K::KEY_TYPE_ID
    }

    fn boxed_clone(&self) -> Box<dyn MergeableTable> {
        Box::new(self.clone())
    }

    fn merge_keys_from(&mut self, source: &dyn MergeableTable, keys: &dyn Any) -> Result<()> {
        let keys = keys
            .downcast_ref::<BTreeSet<K>>()
            .ok_or_else(|| Error::TypeMismatch("merge key set".to_string()))?;
        let source = source
            .as_any()
            .downcast_ref::<Table<R, K>>()
            .ok_or_else(|| Error::TypeMismatch("merge source".to_string()))?;

        // The write overlay (task58) is a SingleWriter-only optimization.
        // This per-key merge is MultiWriter's commit-time reconciliation
        // path, and it reads/writes `self`/`source` through the tree
        // directly (`data.get_arc`, `upsert_arc`, `delete`) rather than
        // through `merged_get`/`merged_iter` — an overlay-resident write on
        // either side here would silently be missed.
        debug_assert!(
            self.overlay_is_empty() && source.overlay_is_empty(),
            "merge_keys_from: overlay must be empty on the MultiWriter merge path"
        );

        // BUG(task47): under the `drop-merge-key` mutation, silently skip the
        // writer's edit to the first key of this merge — a lost update the
        // commit merge is supposed to preserve. OCC/SSI validation already
        // passed, so this exercises the merge path itself. Off in normal builds.
        #[cfg(feature = "mutation-testing")]
        let mut drop_first = matches!(
            crate::mutation::active(),
            Some(crate::mutation::Mutation::DropMergeKey)
        );

        for key in keys {
            #[cfg(feature = "mutation-testing")]
            if drop_first {
                drop_first = false;
                continue;
            }
            match (source.data.get_arc(key), self.data.get_arc(key)) {
                (Some(new_arc), _) => self.upsert_arc(key.clone(), new_arc)?,
                (None, Some(_)) => {
                    // Writer deleted this key. `self` still has the prior
                    // value (OCC rules out concurrent deletion at this key).
                    let _ = self.delete(key)?;
                }
                (None, None) => {
                    // Writer inserted-then-deleted in the same tx — no-op.
                }
            }
        }
        // Ensure the merged table's next_id is at least as large as the
        // writer's, so future auto-assigned inserts don't collide with any
        // id the writer already used. (`None` — an explicitly-keyed table —
        // orders below every `Some`, so this is a no-op there.)
        if source.next_id > self.next_id {
            self.next_id = source.next_id.clone();
        }
        Ok(())
    }

    #[cfg(feature = "persistence")]
    fn index_list(&self) -> Vec<(u8, String)> {
        self.indexes
            .iter()
            .map(|(name, idx)| {
                let kind_byte = match idx.kind() {
                    IndexKind::Unique => 0u8,
                    IndexKind::NonUnique => 1u8,
                    IndexKind::Custom => 2u8,
                };
                (kind_byte, name.clone())
            })
            .collect()
    }

    #[cfg(feature = "persistence")]
    fn collect_serialized_rows(
        &self,
        serialize_record: &(dyn Fn(&dyn Any) -> Result<Vec<u8>> + Send + Sync),
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::with_capacity(self.len());
        for (key, record) in self.merged_iter(..) {
            let bytes = serialize_record(record as &dyn Any)?;
            out.push((key.encode(), bytes));
        }
        Ok(out)
    }
}

/// A compile-time table definition binding a name to a record type.
#[derive(Copy, Clone)]
pub struct TableDef<R: 'static, K = u64> {
    name: &'static str,
    _phantom: PhantomData<(R, K)>,
}

impl<R: 'static, K> TableDef<R, K> {
    /// Binds `name` to record type `R` as a `const`-constructible table
    /// definition, usable as a `static` handle for `open_table` call sites.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _phantom: PhantomData,
        }
    }

    /// Returns the bound table name.
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

/// Trait for types that can identify a table and its record type.
pub trait TableOpener<R> {
    /// Returns the table name to open.
    fn table_name(&self) -> &str;
}

impl<R> TableOpener<R> for &str {
    fn table_name(&self) -> &str {
        self
    }
}

impl<R: 'static, K> TableOpener<R> for TableDef<R, K> {
    fn table_name(&self) -> &str {
        self.name
    }
}

/// A typed collection wrapping `BTree<K, R>` with secondary indexes and batch
/// operations. `Clone` is O(1) (CoW via the backing B-tree's `Arc` sharing)
/// and preserves `next_id`.
///
/// `K` defaults to `u64`, which is the only key type the table can assign
/// itself: [`Table::insert`] and friends live behind an
/// [`AutoKey`] bound. Tables keyed by anything
/// else are built with [`Table::new_keyed`] and written with [`Table::put`].
pub struct Table<R, K = u64> {
    data: BTree<K, R>,
    /// The auto-increment counter: `Some` for a table built with
    /// [`Table::new`], `None` for an explicitly-keyed one built with
    /// [`Table::new_keyed`].
    ///
    /// `None` is reachable under an `AutoKey` bound. `u64` is both `AutoKey`
    /// and `PrimaryKey`, so `Table::<R, u64>::new_keyed()` compiles from safe
    /// public code and produces a `u64` table with no counter. The `AutoKey`
    /// methods therefore *unwrap* and panic on `None` rather than assume:
    /// silently starting the counter at 1 would hand out ids colliding with
    /// rows a `put` already placed at 1.., which is worse than a panic. See
    /// the `# Panics` sections on [`Table::insert`], [`Table::insert_batch`]
    /// and [`Table::next_id`].
    ///
    /// Writing a key through [`Table::put`] (or the internal `upsert_arc`)
    /// installs/advances the counter past that key via
    /// [`PrimaryKey::advance_auto_counter`], so a `u64` table created with
    /// `new_keyed` does support `insert` once it has been written to.
    next_id: Option<K>,
    indexes: BTreeMap<String, Box<dyn IndexMaintainer<R, K>>>,
    /// Bounded write-front absorbing recent mutations. Every construction
    /// site below builds it disabled (`cap == 0`); `Store`'s writer path
    /// enables it per table via [`Table::set_overlay_cap`] (SingleWriter
    /// only). See `src/overlay.rs`.
    overlay: Overlay<R, K>,
}

/// Captured table state for atomic batch rollback.
struct TableSnapshot<R, K = u64> {
    data: BTree<K, R>,
    next_id: Option<K>,
    indexes: BTreeMap<String, Box<dyn IndexMaintainer<R, K>>>,
    overlay: Overlay<R, K>,
}

impl<R: Record, K: PrimaryKey> Table<R, K> {
    /// Creates an empty table addressed by explicit keys. Unlike
    /// [`Table::new`], there is no id counter — rows are added with
    /// [`Table::put`].
    pub fn new_keyed() -> Self {
        Self {
            data: BTree::new(),
            next_id: None,
            indexes: BTreeMap::new(),
            overlay: Overlay::new(0),
        }
    }

    /// Build a table from sorted `(key, Arc<record>)` pairs and a list of
    /// pre-defined indexes. Builds the data tree via `BTree::from_sorted`,
    /// then backfills each index via `rebuild_from_sorted_data`. On any
    /// index-build failure, returns `Err`; the original table (if any) is
    /// untouched because we never mutate it.
    #[allow(dead_code)]
    pub(crate) fn from_bulk(
        sorted_rows: Vec<(K, Arc<R>)>,
        next_id: Option<K>,
        mut index_defs: Vec<Box<dyn IndexMaintainer<R, K>>>,
    ) -> Result<Self> {
        // Debug-assert ascending unique keys.
        debug_assert!(
            sorted_rows.windows(2).all(|w| w[0].0 < w[1].0),
            "from_bulk: rows must be strictly ascending by key"
        );

        let data: BTree<K, R> = BTree::from_sorted(sorted_rows);

        let mut indexes: BTreeMap<String, Box<dyn IndexMaintainer<R, K>>> = BTreeMap::new();
        for mut idx in index_defs.drain(..) {
            idx.rebuild_from_sorted_data(&data)?;
            indexes.insert(idx.name().to_string(), idx);
        }

        Ok(Self {
            data,
            next_id,
            indexes,
            overlay: Overlay::new(0),
        })
    }

    /// Clone each index's *definition* (extractor, name, kind, storage type)
    /// with empty storage. Used by bulk-load to rebuild indexes from new data.
    pub(crate) fn empty_index_defs(&self) -> Result<Vec<Box<dyn IndexMaintainer<R, K>>>> {
        self.indexes.values().map(|i| i.empty_clone()).collect()
    }

    /// Borrow the underlying data B-tree. Used by bulk-load Delta to walk
    /// the captured base in key order while materializing the merged rows.
    ///
    /// Callers see the tree *without* the overlay — only valid where the
    /// overlay is empty by construction (bulk paths never enable it).
    pub(crate) fn data_ref(&self) -> &BTree<K, R> {
        debug_assert!(self.overlay_is_empty(), "data_ref: overlay must be empty");
        &self.data
    }

    /// Whether the write overlay currently holds no entries. Load-bearing:
    /// the `debug_assert!`s that call it fence real invariants (the
    /// MultiWriter merge path and `data_ref()`'s tree-only view).
    fn overlay_is_empty(&self) -> bool {
        self.overlay.is_empty()
    }

    /// The auto-increment counter, or `None` for an explicitly-keyed table.
    /// Serialization needs the distinction; `next_id()` (AutoKey-only) is the
    /// public accessor.
    #[allow(dead_code)]
    pub(crate) fn next_id_opt(&self) -> Option<K> {
        self.next_id.clone()
    }

    /// An empty table carrying `next_id` as its auto-increment counter.
    ///
    /// Spelled without the [`AutoKey`] bound so the type-erased registry
    /// closures can build one for *any* key type: `None` for an explicitly-
    /// keyed table, `Some(K::first())` when `K` is the one `AutoKey`. Under an
    /// `AutoKey` bound, prefer [`Table::new`].
    #[allow(dead_code)]
    pub(crate) fn empty_with_counter(next_id: Option<K>) -> Self {
        Self {
            data: BTree::new(),
            next_id,
            indexes: BTreeMap::new(),
            overlay: Overlay::new(0),
        }
    }

    /// True when this mutation should go through the overlay. Also the flush
    /// trigger: a full overlay is drained into the tree before the caller
    /// proceeds, so the subsequent buffer insert always has room.
    fn overlay_write_ready(&mut self) -> bool {
        if !self.overlay.enabled() || !self.indexes.is_empty() {
            return false;
        }
        if self.overlay.is_full() {
            self.flush_overlay();
        }
        true
    }

    /// Enable, resize, or disable the write overlay for this table.
    ///
    /// Contract: same cap → buffered entries are preserved (a no-op), so
    /// re-opening a write transaction against the same table doesn't
    /// discard anything on every `open_table`. Any cap *change* — including
    /// the `cap == 0` disable case, and including a cap the buffered
    /// entries would still fit under — flushes first: entries are never
    /// dropped, only ever flushed to the tree before the overlay is
    /// replaced.
    pub(crate) fn set_overlay_cap(&mut self, cap: usize) {
        if !self.overlay.is_empty() && cap != self.overlay.cap() {
            self.flush_overlay();
        }
        if self.overlay.cap() == cap {
            return;
        }
        self.overlay = Overlay::new(cap);
    }

    /// Replay the buffered ops into the tree in one batched, sorted pass —
    /// the root and inner nodes CoW once for the whole batch instead of once
    /// per transaction. Infallible: Puts use `insert_arc_mut`, and every
    /// Tombstone shadows a tree-resident key by the overlay invariant.
    pub(crate) fn flush_overlay(&mut self) {
        if self.overlay.is_empty() {
            return;
        }
        let entries = self.overlay.take_entries();
        for (key, op) in entries.iter() {
            match op {
                OverlayOp::Put { rec, .. } => {
                    self.data.insert_arc_mut(key.clone(), Arc::clone(rec));
                }
                OverlayOp::Tombstone => {
                    let removed = self.data.remove_mut(key);
                    debug_assert!(removed, "tombstone shadowed a non-resident key");
                }
            }
        }
    }

    /// Insert-or-replace a record at an explicit key. Available for every key
    /// type — this is how explicitly-keyed tables are written.
    ///
    /// Secondary indexes are maintained (routing to the insert or the update
    /// path depending on whether a record already exists at `key`).
    ///
    /// On an auto-increment table this also advances the id counter past
    /// `key` (see [`PrimaryKey::advance_auto_counter`]), so a following
    /// [`Table::insert`] cannot reissue the id just written. Unlike
    /// [`Table::insert_with_id`], `put` replaces an existing row instead of
    /// returning [`Error::DuplicateKey`].
    pub fn put(&mut self, key: K, record: R) -> Result<()> {
        if self.overlay_write_ready() {
            let resident = self.data.get(&key).is_some();
            // Keep the auto-increment counter (if this key type has one)
            // past any explicitly written key, exactly as `upsert_arc`'s
            // direct path does — just applied before the row lands in the
            // overlay instead of the tree. No-op for every non-`AutoKey`
            // key.
            key.advance_auto_counter(&mut self.next_id);
            self.overlay.set_put(key, Arc::new(record), resident);
            return Ok(());
        }
        self.upsert_arc(key, Arc::new(record))
    }

    /// Look up a record by its key. Overlay-then-tree: a live overlay entry
    /// (if any) wins, a tombstone means "not found" regardless of the tree.
    pub fn get(&self, key: &K) -> Option<&R> {
        if !self.overlay.is_empty() {
            match self.overlay.get(key) {
                Some(OverlayOp::Put { rec, .. }) => return Some(rec.as_ref()),
                Some(OverlayOp::Tombstone) => return None,
                None => {}
            }
        }
        self.data.get(key)
    }

    /// Update a record by its key. Returns an error if the key does not exist
    /// or if a unique index constraint is violated.
    pub fn update(&mut self, key: &K, record: R) -> Result<()> {
        let old = self.merged_get_arc(key).ok_or(Error::KeyNotFound)?;

        // Update all indexes; rollback on failure.
        // SAFETY: Same invariants as `insert` — see comment there.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R, K>>> =
            self.indexes.values_mut().map(|v| v as *mut _).collect();
        for (applied, ptr) in ptrs.iter().enumerate() {
            let idx = unsafe { &mut **ptr };
            if let Err(e) = idx.on_update(key.clone(), &old, &record) {
                // Rollback previously applied indexes by reversing the update.
                for prev_ptr in &ptrs[..applied] {
                    let prev_idx = unsafe { &mut **prev_ptr };
                    // Reverse: update back from new -> old. This should never
                    // fail because we're restoring previously-valid values.
                    let rollback_result = prev_idx.on_update(key.clone(), &record, &old);
                    debug_assert!(
                        rollback_result.is_ok(),
                        "index rollback failed: {:?}",
                        rollback_result
                    );
                }
                return Err(e);
            }
        }

        if self.overlay_write_ready() {
            let resident = self.data.get(key).is_some();
            self.overlay.set_put(key.clone(), Arc::new(record), resident);
        } else {
            self.data.insert_mut(key.clone(), record);
        }
        Ok(())
    }

    /// Insert-or-replace at an explicit key, reusing an existing `Arc<R>`.
    /// Maintains secondary indexes (routing to `on_insert` or `on_update`
    /// depending on whether a prior record exists at the key), and advances
    /// the auto-increment counter past the key on `AutoKey` tables so a later
    /// `insert` cannot reissue it. Used at commit by the per-key merge path.
    pub(crate) fn upsert_arc(&mut self, key: K, arc: Arc<R>) -> Result<()> {
        // `upsert_arc` is the MultiWriter commit-merge helper
        // (`merge_keys_from`, which already debug_asserts the overlay is
        // empty before calling in) and `put`'s fallback for when the
        // overlay isn't write-ready (disabled, or an indexed table) — `put`
        // itself buffers into the overlay directly when it can (see `put`).
        // Flushing first keeps this direct tree write correct if either
        // caller ever reaches here with a non-empty overlay; today both
        // already guarantee that by construction, so this is a defensive
        // no-op.
        self.flush_overlay();

        let prior = self.data.get_arc(&key);
        let new_ref: &R = &arc;
        // SAFETY: same invariants as `insert` — see comment there.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R, K>>> =
            self.indexes.values_mut().map(|v| v as *mut _).collect();

        match &prior {
            Some(old_arc) => {
                let old_ref: &R = old_arc;
                for (applied, ptr) in ptrs.iter().enumerate() {
                    let idx = unsafe { &mut **ptr };
                    if let Err(e) = idx.on_update(key.clone(), old_ref, new_ref) {
                        // Roll back previously applied index updates by
                        // reversing them (new → old). Should not fail
                        // because we are restoring valid state.
                        for prev_ptr in &ptrs[..applied] {
                            let prev_idx = unsafe { &mut **prev_ptr };
                            let _ = prev_idx.on_update(key.clone(), new_ref, old_ref);
                        }
                        return Err(e);
                    }
                }
            }
            None => {
                for (applied, ptr) in ptrs.iter().enumerate() {
                    let idx = unsafe { &mut **ptr };
                    if let Err(e) = idx.on_insert(key.clone(), new_ref) {
                        for prev_ptr in &ptrs[..applied] {
                            let prev_idx = unsafe { &mut **prev_ptr };
                            prev_idx.on_delete(key.clone(), new_ref);
                        }
                        return Err(e);
                    }
                }
            }
        }

        // Keep the auto-increment counter (if this key type has one) past any
        // explicitly written key, so a later `insert` cannot reissue an id a
        // `put`/`upsert` already occupies. No-op for every non-`AutoKey` key.
        key.advance_auto_counter(&mut self.next_id);
        self.data.insert_arc_mut(key, arc);
        Ok(())
    }

    /// Delete a record by its key. Returns the deleted record, or an error if
    /// the key does not exist.
    pub fn delete(&mut self, key: &K) -> Result<Arc<R>> {
        let old = self.merged_get_arc(key).ok_or(Error::KeyNotFound)?;
        // Remove from all indexes before removing from data tree.
        for idx in self.indexes.values_mut() {
            idx.on_delete(key.clone(), &old);
        }
        if self.overlay_write_ready() {
            match self.overlay.get(key) {
                Some(OverlayOp::Put {
                    tree_resident: false,
                    ..
                }) => self.overlay.remove_entry(key),
                _ => self.overlay.set_tombstone(key.clone()),
            }
        } else {
            let removed = self.data.remove_mut(key);
            debug_assert!(removed, "delete: presence checked above");
        }
        Ok(old)
    }

    // -----------------------------------------------------------------------
    // Batch mutations — deferred index updates
    // -----------------------------------------------------------------------

    /// Capture current state for atomic rollback. O(1) for data BTree and
    /// O(1) per index thanks to CoW/Arc internals.
    fn snapshot(&self) -> TableSnapshot<R, K> {
        TableSnapshot {
            data: self.data.clone(),
            next_id: self.next_id.clone(),
            indexes: self
                .indexes
                .iter()
                .map(|(k, v)| (k.clone(), v.clone_box()))
                .collect(),
            overlay: self.overlay.clone(),
        }
    }

    /// Restore from a previously captured snapshot.
    fn restore(&mut self, snap: TableSnapshot<R, K>) {
        self.data = snap.data;
        self.next_id = snap.next_id;
        self.indexes = snap.indexes;
        self.overlay = snap.overlay;
    }

    /// Update multiple records by key. Returns an error if any key does not
    /// exist or if a unique index constraint is violated. On error, the
    /// table is unchanged (atomic rollback).
    ///
    /// If the same key appears multiple times, the last value wins.
    pub fn update_batch(&mut self, updates: Vec<(K, R)>) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        // Flush first: the snapshot/restore rollback below and the phase-0
        // existence probe (`self.data.get_arc`) then operate on a
        // merged-empty table, unchanged from today.
        self.flush_overlay();

        // Deduplicate: keep only the last value for each key.
        let mut seen = BTreeMap::new();
        for (i, (key, _)) in updates.iter().enumerate() {
            seen.insert(key.clone(), i);
        }
        let deduped_indices: Vec<usize> = {
            let mut indices: Vec<usize> = seen.values().copied().collect();
            indices.sort_unstable();
            indices
        };

        // Phase 0: Validate all unique keys exist and collect old records.
        let mut old_records: Vec<(K, Arc<R>)> = Vec::with_capacity(deduped_indices.len());
        for &i in &deduped_indices {
            let key = updates[i].0.clone();
            let old = self.data.get_arc(&key).ok_or(Error::KeyNotFound)?;
            old_records.push((key, old));
        }

        let snap = self.snapshot();

        // Phase 1: Mutate data BTree for all updates (in original order so
        // last-value-wins semantics are preserved).
        for (key, record) in updates {
            self.data.insert_mut(key, record);
        }

        // Phase 2: Update each index for all deduplicated records.
        // SAFETY: Same invariants as single-record `insert` — see comment there.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R, K>>> =
            self.indexes.values_mut().map(|v| v as *mut _).collect();
        for ptr in &ptrs {
            let idx = unsafe { &mut **ptr };
            for (key, old_arc) in &old_records {
                let new_record = self.data.get(key).unwrap();
                if let Err(e) = idx.on_update(key.clone(), old_arc.as_ref(), new_record) {
                    self.restore(snap);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Delete multiple records by key. Returns an error if any key does not
    /// exist. On error, the table is unchanged (atomic rollback).
    ///
    /// Duplicate keys in the input are handled gracefully (deduplicated).
    pub fn delete_batch(&mut self, keys: &[K]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        // Flush first — see the comment in `update_batch`.
        self.flush_overlay();

        // Deduplicate keys.
        let mut keys = keys.to_vec();
        keys.sort_unstable();
        keys.dedup();

        // Phase 0: Validate all keys exist and collect old records.
        let mut old_records: Vec<(K, Arc<R>)> = Vec::with_capacity(keys.len());
        for key in &keys {
            let old = self.data.get_arc(key).ok_or(Error::KeyNotFound)?;
            old_records.push((key.clone(), old));
        }

        let snap = self.snapshot();

        // Phase 1: Remove all records from data BTree.
        for key in &keys {
            if !self.data.remove_mut(key) {
                self.restore(snap);
                return Err(Error::KeyNotFound);
            }
        }

        // Phase 2: Clean indexes (on_delete is infallible).
        for idx in self.indexes.values_mut() {
            for (key, old_arc) in &old_records {
                idx.on_delete(key.clone(), old_arc.as_ref());
            }
        }

        Ok(())
    }

    /// Merge the write overlay with the backing tree over `range` — the
    /// choke point every multi-row read (`iter`, `range`, `first`, `last`,
    /// `collect_serialized_rows`) flows through. The overlay slice is
    /// pre-narrowed to `range` via two `partition_point` calls, so this
    /// stays O(log cap + items) rather than O(cap) per call.
    ///
    /// An empty overlay short-circuits to the bare tree iterator
    /// (`TableIter::Plain`): scans on a quiet table — every `ReadTx`, every
    /// MultiWriter store, every table between flushes — pay one `match` per
    /// step instead of the merge's peek/compare, which is what keeps
    /// checkpoint/snapshot-stream scan cost off the overlay's bill.
    fn merged_iter<'a, Rb>(&'a self, range: Rb) -> TableIter<'a, R, K, BTreeRange<'a, K, R>>
    where
        Rb: RangeBounds<K> + 'a,
    {
        if self.overlay.is_empty() {
            return TableIter::Plain(self.data.range(range));
        }
        let e = self.overlay.entries();
        let lo = match range.start_bound() {
            Bound::Included(k) => e.partition_point(|(ek, _)| ek < k),
            Bound::Excluded(k) => e.partition_point(|(ek, _)| ek <= k),
            Bound::Unbounded => 0,
        };
        let hi = match range.end_bound() {
            Bound::Included(k) => e.partition_point(|(ek, _)| ek <= k),
            Bound::Excluded(k) => e.partition_point(|(ek, _)| ek < k),
            Bound::Unbounded => e.len(),
        };
        // An empty or inverted range (`5..2`, `5..5`) leaves `lo > hi`, and
        // `e[lo..hi]` would panic — in release too, since a slice-index panic
        // is not debug-gated. `BTree::range` is total on such bounds (it
        // filters rather than descends), so before the overlay this returned an
        // empty iterator; clamping keeps that. A committed snapshot normally
        // carries a live overlay, so this is the ordinary path, not a corner.
        let hi = hi.max(lo);
        TableIter::Merged(MergedIter {
            overlay: e[lo..hi].iter().peekable(),
            tree: self.data.range(range).peekable(),
        })
    }

    /// Overlay-then-tree point lookup returning an owned `Arc`. Internal
    /// counterpart to `get` for call sites that need to hold the value past
    /// the table's borrow — `update`/`delete` use it as the merged existence
    /// probe so a row buffered in the overlay is found without a tree read.
    fn merged_get_arc(&self, key: &K) -> Option<Arc<R>> {
        if !self.overlay.is_empty() {
            match self.overlay.get(key) {
                Some(OverlayOp::Put { rec, .. }) => return Some(Arc::clone(rec)),
                Some(OverlayOp::Tombstone) => return None,
                None => {}
            }
        }
        self.data.get_arc(key)
    }

    /// Returns an iterator over records within the specified key range.
    pub fn range<'a>(
        &'a self,
        range: impl RangeBounds<K> + 'a,
    ) -> impl Iterator<Item = (&'a K, &'a R)> + 'a {
        self.merged_iter(range)
    }

    /// Returns the number of records in the table.
    #[must_use]
    pub fn len(&self) -> usize {
        (self.data.len() as i64 + self.overlay.len_delta()) as usize
    }

    /// Returns true if the table contains no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if the table contains a record with the given key.
    pub fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// Returns the first (lowest key) record, or `None` if empty.
    pub fn first(&self) -> Option<(&K, &R)> {
        self.merged_iter(..).next()
    }

    /// Returns the last (highest key) record, or `None` if empty.
    pub fn last(&self) -> Option<(&K, &R)> {
        if self.overlay.is_empty() {
            return self.data.range(..).next_back();
        }
        // `MergedIter` isn't `DoubleEndedIterator`, and its default
        // `.last()` drains the *tree* side (`self.data.range(..)`) to
        // exhaustion — an O(table size) scan, not O(cap). Instead, walk a
        // bounded reverse merge: the mirror of `MergedIter::next()`, using
        // the overlay's own tail (at most `OVERLAY_CAP` entries) and the
        // tree's `next_back()`. Cost is O(log n) for tree descent plus at
        // most `OVERLAY_CAP` steps on the overlay side.
        let entries = self.overlay.entries();
        let mut ov_idx = entries.len();
        let mut tree = self.data.range(..);
        let mut tree_tail: Option<(&K, &R)> = tree.next_back();
        loop {
            let take_overlay = match (ov_idx, &tree_tail) {
                (0, None) => return None,
                (0, Some(_)) => false,
                (_, None) => true,
                (_, Some((tk, _))) => {
                    let ok = &entries[ov_idx - 1].0;
                    match ok.cmp(tk) {
                        std::cmp::Ordering::Less => false,
                        std::cmp::Ordering::Greater => true,
                        std::cmp::Ordering::Equal => {
                            // Overlay shadows this tree entry; consume it
                            // and step the tree side back one further.
                            tree_tail = tree.next_back();
                            true
                        }
                    }
                }
            };
            if take_overlay {
                ov_idx -= 1;
                let (k, op) = &entries[ov_idx];
                match op {
                    OverlayOp::Put { rec, .. } => return Some((k, rec.as_ref())),
                    OverlayOp::Tombstone => continue, // its tree twin (if any) already skipped
                }
            } else {
                return tree_tail;
            }
        }
    }

    /// Iterate over all records in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &R)> + '_ {
        self.merged_iter(..)
    }

    /// Look up multiple records by key.
    pub fn get_many(&self, keys: &[K]) -> Vec<Option<&R>> {
        keys.iter().map(|key| self.get(key)).collect()
    }

    // -----------------------------------------------------------------------
    // Index management
    // -----------------------------------------------------------------------

    /// Define a secondary index. If the table already contains data, the index
    /// is backfilled. Returns an error if the index name is already taken or
    /// if backfilling hits a unique constraint violation.
    ///
    /// `IK` is the *index* key (the value the extractor pulls out of a
    /// record); `K` remains the table's primary key.
    ///
    /// # Examples
    ///
    /// ```
    /// use ultima_db::{Table, IndexKind};
    ///
    /// let mut table: Table<String> = Table::new();
    /// let id = table.insert("alice@example.com".to_string()).unwrap();
    /// table
    ///     .define_index("by_email", IndexKind::Unique, |email: &String| email.clone())
    ///     .unwrap();
    ///
    /// let found = table.get_unique("by_email", &"alice@example.com".to_string()).unwrap();
    /// assert_eq!(found, Some((id, &"alice@example.com".to_string())));
    /// ```
    pub fn define_index<IK: Ord + Clone + Send + Sync + 'static>(
        &mut self,
        name: &str,
        kind: IndexKind,
        extractor: impl Fn(&R) -> IK + Send + Sync + 'static,
    ) -> Result<()> {
        // Indexed tables use the direct path: the index reads below
        // (get_unique/get_by_index/get_by_key/index_range) read `self.data`
        // straight off the tree, so an indexed table's overlay must stay
        // *empty* from here on. What guarantees that is
        // `overlay_write_ready()`'s `!self.indexes.is_empty()` arm — once
        // this index is registered no write buffers again. (The cap itself
        // is zeroed here for clarity, but a later `open_table` re-applies
        // the store's cap via `set_overlay_cap`; the emptiness invariant is
        // the load-bearing one, not the cap.) Entries buffered *before* this
        // call still have to be flushed now: `get`/`merged_get_arc` consult
        // a nonempty overlay regardless of index state, so leaving them
        // would let them shadow the tree forever while `update`/`delete`
        // wrote around them.
        self.flush_overlay();
        self.overlay = Overlay::new(0);
        if let Some(existing) = self.indexes.get(name) {
            if existing.kind() == IndexKind::Custom || existing.kind() != kind {
                return Err(Error::IndexTypeMismatch(name.to_string()));
            }
            // Same name and kind — idempotent. (We can't verify the extractor
            // or key type are the same, so trust the caller.)
            return Ok(());
        }
        let extractor = Arc::new(extractor);
        let mut index: Box<dyn IndexMaintainer<R, K>> = match kind {
            IndexKind::Unique => Box::new(ManagedIndex::<R, IK, UniqueStorage<IK, K>>::new(
                name.to_string(),
                kind,
                extractor,
                UniqueStorage::new(),
            )),
            IndexKind::NonUnique => Box::new(ManagedIndex::<R, IK, NonUniqueStorage<IK, K>>::new(
                name.to_string(),
                kind,
                extractor,
                NonUniqueStorage::new(),
            )),
            IndexKind::Custom => {
                return Err(Error::IndexTypeMismatch(name.to_string()));
            }
        };
        // Backfill via fast bulk-build primitive (falls back to per-row for custom indexes).
        // Reads `self.data` directly: the overlay was just flushed and no
        // further write will buffer into it, so this is exactly today's
        // tree-only picture.
        index.rebuild_from_sorted_data(&self.data)?;
        self.indexes.insert(name.to_string(), index);
        Ok(())
    }

    /// Look up a single record by a unique index.
    pub fn get_unique<IK: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &IK,
    ) -> Result<Option<(K, &R)>> {
        let idx = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;
        let managed = idx
            .as_any()
            .downcast_ref::<ManagedIndex<R, IK, UniqueStorage<IK, K>>>()
            .ok_or_else(|| Error::IndexTypeMismatch(index_name.to_string()))?;
        // Not routed through `get`/`merged_iter`: an indexed table's overlay
        // is always empty — `overlay_write_ready()` refuses to buffer while
        // `!self.indexes.is_empty()`, and the DDL that created this index
        // flushed whatever predated it — so a direct tree read is safe.
        match managed.storage().get(key) {
            Some(row_key) => Ok(self.data.get(&row_key).map(|r| (row_key, r))),
            None => Ok(None),
        }
    }

    /// Look up records by a non-unique index key.
    pub fn get_by_index<IK: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &IK,
    ) -> Result<Vec<(K, &R)>> {
        let idx = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;
        let managed = idx
            .as_any()
            .downcast_ref::<ManagedIndex<R, IK, NonUniqueStorage<IK, K>>>()
            .ok_or_else(|| Error::IndexTypeMismatch(index_name.to_string()))?;
        // An indexed table's overlay is always empty — see the comment in
        // `get_unique`. Direct tree reads are safe here too.
        Ok(managed
            .storage()
            .get_ids(key)
            .filter_map(|row_key| self.data.get(&row_key).map(|r| (row_key, r)))
            .collect())
    }

    /// Look up records by index key (works for both unique and non-unique).
    pub fn get_by_key<IK: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        key: &IK,
    ) -> Result<Vec<(K, &R)>> {
        let idx = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        // Try unique first, then non-unique. An indexed table's overlay is
        // always empty (see `get_unique`); direct tree reads below are safe.
        if let Some(managed) = idx
            .as_any()
            .downcast_ref::<ManagedIndex<R, IK, UniqueStorage<IK, K>>>()
        {
            return Ok(managed
                .storage()
                .get(key)
                .into_iter()
                .filter_map(|row_key| self.data.get(&row_key).map(|r| (row_key, r)))
                .collect());
        }
        let managed = idx
            .as_any()
            .downcast_ref::<ManagedIndex<R, IK, NonUniqueStorage<IK, K>>>()
            .ok_or_else(|| Error::IndexTypeMismatch(index_name.to_string()))?;
        Ok(managed
            .storage()
            .get_ids(key)
            .filter_map(|row_key| self.data.get(&row_key).map(|r| (row_key, r)))
            .collect())
    }

    /// Range scan on an index (works for both unique and non-unique).
    pub fn index_range<IK: Ord + Clone + Send + Sync + 'static>(
        &self,
        index_name: &str,
        range: impl RangeBounds<IK>,
    ) -> Result<Vec<(K, &R)>> {
        let idx = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::IndexNotFound(index_name.to_string()))?;

        // Try unique first, then non-unique. An indexed table's overlay is
        // always empty (see `get_unique`); direct tree reads below are safe.
        if let Some(managed) = idx
            .as_any()
            .downcast_ref::<ManagedIndex<R, IK, UniqueStorage<IK, K>>>()
        {
            return Ok(managed
                .storage()
                .range_ids(range)
                .filter_map(|(_, row_key)| self.data.get(&row_key).map(|r| (row_key, r)))
                .collect());
        }
        let managed = idx
            .as_any()
            .downcast_ref::<ManagedIndex<R, IK, NonUniqueStorage<IK, K>>>()
            .ok_or_else(|| Error::IndexTypeMismatch(index_name.to_string()))?;
        Ok(managed
            .storage()
            .range_ids(range)
            .filter_map(|(_, row_key)| self.data.get(&row_key).map(|r| (row_key, r)))
            .collect())
    }

    /// Resolve a slice of primary keys to `(key, &record)` pairs.
    /// Keys that don't exist in the table are silently skipped.
    pub fn resolve(&self, keys: &[K]) -> Vec<(K, &R)> {
        keys.iter()
            .filter_map(|key| self.get(key).map(|r| (key.clone(), r)))
            .collect()
    }

    /// Define a custom index. If the table already contains data, the index
    /// is backfilled via [`CustomIndex::rebuild`]. Returns an error if any
    /// index (built-in or custom) with the same name already exists.
    pub fn define_custom_index<I: CustomIndex<R, K>>(
        &mut self,
        name: &str,
        mut index: I,
    ) -> Result<()> {
        // Indexed tables use the direct path; the index reads rely on the
        // overlay staying empty from here on, which
        // `overlay_write_ready()`'s `!self.indexes.is_empty()` arm enforces
        // — see the fuller comment in `define_index` (task58 T5).
        self.flush_overlay();
        self.overlay = Overlay::new(0);
        if self.indexes.contains_key(name) {
            return Err(Error::IndexAlreadyExists(name.to_string()));
        }
        // Reads `self.data` directly: the overlay was just flushed and no
        // further write will buffer into it, so this is exactly today's
        // tree-only picture.
        index.rebuild(self.data.range(..).map(|(id, r)| (id.clone(), r)))?;
        let adapter = CustomIndexAdapter::new(name.to_string(), index);
        self.indexes.insert(name.to_string(), Box::new(adapter));
        Ok(())
    }

    /// Retrieve a reference to a custom index by name, downcast to the
    /// concrete index type. Returns `IndexNotFound` if the name doesn't
    /// exist, or `IndexTypeMismatch` if the type doesn't match.
    pub fn custom_index<I: CustomIndex<R, K>>(&self, name: &str) -> Result<&I> {
        let idx = self
            .indexes
            .get(name)
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))?;
        let adapter = idx
            .as_any()
            .downcast_ref::<CustomIndexAdapter<R, K, I>>()
            .ok_or_else(|| Error::IndexTypeMismatch(name.to_string()))?;
        Ok(adapter.inner())
    }
}

// ---------------------------------------------------------------------------
// Auto-increment API — only for keys the table can assign itself (`u64`).
// ---------------------------------------------------------------------------

impl<R: Record, K: AutoKey> Table<R, K> {
    /// Creates a new, empty table with auto-incrementing IDs starting at 1.
    pub fn new() -> Self {
        Self {
            data: BTree::new(),
            next_id: Some(K::first()),
            indexes: BTreeMap::new(),
            overlay: Overlay::new(0),
        }
    }

    /// Insert a record. Returns the auto-assigned ID, or an error if a unique
    /// index constraint is violated.
    ///
    /// # Panics
    ///
    /// Panics if the table has no id counter — i.e. it was built with
    /// [`Table::new_keyed`] and no key has been written through
    /// [`Table::put`] yet (`u64` satisfies both `PrimaryKey` and `AutoKey`,
    /// so that combination is reachable). Panicking beats inventing a counter
    /// that would collide with keys `put` may already have placed. Also
    /// panics on id overflow.
    pub fn insert(&mut self, record: R) -> Result<K> {
        let id = self.next_id.clone().expect("AutoKey table has next_id");
        let next = id.next();
        assert!(next.is_some(), "Table ID overflow");

        // Update all indexes; rollback on failure.
        // SAFETY: We collect raw pointers to index values to avoid borrowing
        // `self.indexes` mutably while iterating. This is sound because:
        // 1. We hold `&mut self`, so no concurrent access is possible.
        // 2. The HashMap is not structurally modified (no insert/remove) during
        //    this loop — only the index values themselves are mutated in place.
        // 3. Each pointer is dereferenced at most once per loop iteration.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R, K>>> =
            self.indexes.values_mut().map(|v| v as *mut _).collect();
        for (applied, ptr) in ptrs.iter().enumerate() {
            let idx = unsafe { &mut **ptr };
            if let Err(e) = idx.on_insert(id.clone(), &record) {
                // Rollback previously applied indexes.
                for prev_ptr in &ptrs[..applied] {
                    let prev_idx = unsafe { &mut **prev_ptr };
                    prev_idx.on_delete(id.clone(), &record);
                }
                return Err(e);
            }
        }

        self.next_id = next;
        if self.overlay_write_ready() {
            // A fresh auto-assigned id is never already in the tree.
            self.overlay.set_put(id.clone(), Arc::new(record), false);
        } else {
            self.data.insert_mut(id.clone(), record);
        }
        Ok(id)
    }

    /// Insert multiple records. Returns the auto-assigned IDs, or an error
    /// if a unique index constraint is violated. On error, the table is
    /// unchanged (atomic rollback).
    ///
    /// Index updates are deferred until all records are inserted into the
    /// data tree, then applied in one pass per index.
    ///
    /// # Panics
    ///
    /// Same as [`Table::insert`]: no id counter (see [`Table::new_keyed`]) or
    /// id overflow.
    ///
    /// # Examples
    ///
    /// ```
    /// use ultima_db::Table;
    ///
    /// let mut table: Table<String> = Table::new();
    /// let ids = table.insert_batch(vec!["a".into(), "b".into(), "c".into()]).unwrap();
    /// assert_eq!(ids, vec![1, 2, 3]);
    /// ```
    pub fn insert_batch(&mut self, records: Vec<R>) -> Result<Vec<K>> {
        if records.is_empty() {
            return Ok(vec![]);
        }
        // Flush first: the `max_key` fast-path guard below reads the tree
        // directly, and the snapshot/restore rollback then captures a
        // merged-empty table — both unchanged from today once the overlay
        // is drained.
        self.flush_overlay();

        // Assign the whole id run up front, before touching any state, so an
        // overflow panics with the table untouched (as the old
        // `checked_add(len)` assert did).
        let start_id = self.next_id.clone().expect("AutoKey table has next_id");
        let mut ids: Vec<K> = Vec::with_capacity(records.len());
        let mut cursor = start_id.clone();
        for _ in 0..records.len() {
            ids.push(cursor.clone());
            let next = cursor.next();
            assert!(next.is_some(), "Table ID overflow");
            cursor = next.unwrap();
        }
        // `cursor` is now the id that follows the batch.
        let after_batch = cursor;

        let snap = self.snapshot();

        // Phase 1: Insert all records into the data BTree.
        //
        // Fast path (task51): batch ids are next_id.. and every existing key is
        // < next_id (auto-increment; merge_keys_from maxes next_id across
        // writers; bulk_load rebuilds it past the loaded max), so the batch is a
        // pure append past the current max key — build it through the dense
        // BulkBuilder in O(batch + height) instead of per-key descents. The
        // max_key guard makes the invariant load-bearing instead of assumed;
        // if it ever fails we take the legacy per-key path.
        if self.data.max_key().is_none_or(|k| *k < start_id) {
            self.data.extend_from_sorted(
                ids.iter()
                    .cloned()
                    .zip(records.into_iter().map(Arc::new)),
            );
        } else {
            // Defensive fallback — unreachable given the next_id invariant, but a
            // violated invariant must degrade to the legacy per-key path, not UB
            // in the packed builder. (No debug_assert here: the fallback test
            // exercises this branch in debug builds.)
            for (id, record) in ids.iter().cloned().zip(records) {
                self.data.insert_mut(id, record);
            }
        }
        self.next_id = Some(after_batch);

        // Phase 2: Update each index for all new records.
        // SAFETY: Same invariants as single-record `insert` — see comment there.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R, K>>> =
            self.indexes.values_mut().map(|v| v as *mut _).collect();
        for ptr in &ptrs {
            let idx = unsafe { &mut **ptr };
            for id in &ids {
                let record = self.data.get(id).unwrap();
                if let Err(e) = idx.on_insert(id.clone(), record) {
                    self.restore(snap);
                    return Err(e);
                }
            }
        }

        Ok(ids)
    }

    /// Returns the next auto-increment ID (the ID that the next `insert` will assign).
    ///
    /// # Panics
    ///
    /// Same as [`Table::insert`]: panics if the table has no id counter (see
    /// [`Table::new_keyed`]).
    pub fn next_id(&self) -> K {
        self.next_id.clone().expect("AutoKey table has next_id")
    }

    /// Insert a record with a specific ID, bypassing auto-increment.
    /// Used during recovery to reconstruct table state from WAL/checkpoint.
    /// Returns an error if the ID already exists or if a unique index
    /// constraint is violated.
    pub fn insert_with_id(&mut self, id: K, record: R) -> Result<K> {
        // Reads and writes `self.data` directly (duplicate probe below, then
        // `insert_mut`) — flush first so the tree is the whole truth.
        self.flush_overlay();
        if self.data.get(&id).is_some() {
            // `K` carries no `Display`/`Debug` bound; the order-preserving
            // encoding is the one printable form every key type has.
            let hex: String = id.encode().iter().map(|b| format!("{b:02x}")).collect();
            return Err(Error::DuplicateKey(format!("id 0x{hex}")));
        }

        // Update all indexes; rollback on failure.
        // SAFETY: Same invariants as `insert` — see comment there.
        let ptrs: Vec<*mut Box<dyn IndexMaintainer<R, K>>> =
            self.indexes.values_mut().map(|v| v as *mut _).collect();
        for (applied, ptr) in ptrs.iter().enumerate() {
            let idx = unsafe { &mut **ptr };
            if let Err(e) = idx.on_insert(id.clone(), &record) {
                for prev_ptr in &ptrs[..applied] {
                    let prev_idx = unsafe { &mut **prev_ptr };
                    prev_idx.on_delete(id.clone(), &record);
                }
                return Err(e);
            }
        }

        self.data.insert_mut(id.clone(), record);
        if self.next_id.as_ref().is_none_or(|n| id >= *n) {
            // `next()` is `None` only at the very last representable id;
            // leave the counter alone there rather than dropping the
            // `Some` invariant of an auto-increment table.
            if let Some(next) = id.next() {
                self.next_id = Some(next);
            }
        }
        Ok(id)
    }

    /// Set the next auto-increment ID. Used during recovery to restore the
    /// counter after deserializing table state.
    pub fn set_next_id(&mut self, next_id: K) {
        self.next_id = Some(next_id);
    }
}

impl<R, K: PrimaryKey> Clone for Table<R, K> {
    /// O(1) per index + O(1) for data tree.
    fn clone(&self) -> Self {
        let indexes = self
            .indexes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone_box()))
            .collect();
        Table {
            data: self.data.clone(),
            next_id: self.next_id.clone(),
            indexes,
            overlay: self.overlay.clone(),
        }
    }
}

impl<R: Record, K: AutoKey> Default for Table<R, K> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl<R, K: PrimaryKey> Table<R, K> {
    /// The number of buffered overlay entries. A `pub(crate)` fn (rather
    /// than inlining this into `overlay_len_for_test` below) so the
    /// `TableWriter`/`TableReader` pass-throughs in `store.rs` can call it
    /// by name instead of reaching into their private `table` field.
    /// Deliberately in this unconstrained-`R` impl block (no `Record`
    /// bound) rather than alongside `flush_overlay`, so it (and
    /// `overlay_len_for_test`, which delegates to it) compile regardless of
    /// whether `R` happens to satisfy `Record` in a given test.
    pub(crate) fn overlay_len_probe(&self) -> usize {
        self.overlay.entries().len()
    }
}

#[cfg(test)]
impl<R, K: PrimaryKey> Table<R, K> {
    /// Test-only escape hatch: enable the overlay at `cap` (a no-op if it's
    /// already enabled) and hand back a mutable handle so tests can drive
    /// overlay state directly instead of pushing OVERLAY_CAP-many writes
    /// through the not-yet-built flush path.
    pub(crate) fn overlay_mut_for_test(&mut self, cap: usize) -> &mut Overlay<R, K> {
        if !self.overlay.enabled() {
            self.overlay = Overlay::new(cap);
        }
        &mut self.overlay
    }

    /// Test-only probe: the number of buffered overlay entries.
    pub(crate) fn overlay_len_for_test(&self) -> usize {
        self.overlay_len_probe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::CustomIndex;

    // -----------------------------------------------------------------------
    // Explicitly-keyed tables (task56)
    // -----------------------------------------------------------------------

    #[test]
    fn string_keyed_table_crud() {
        let mut t: Table<String, String> = Table::new_keyed();

        t.put("alice@x.com".to_string(), "Alice".to_string()).unwrap();
        t.put("bob@x.com".to_string(), "Bob".to_string()).unwrap();

        assert_eq!(t.get(&"alice@x.com".to_string()), Some(&"Alice".to_string()));
        assert_eq!(t.get(&"nobody@x.com".to_string()), None);
        assert_eq!(t.len(), 2);

        // put on an existing key overwrites.
        t.put("alice@x.com".to_string(), "Alice B".to_string()).unwrap();
        assert_eq!(
            t.get(&"alice@x.com".to_string()),
            Some(&"Alice B".to_string())
        );
        assert_eq!(t.len(), 2);

        t.delete(&"alice@x.com".to_string()).unwrap();
        assert_eq!(t.get(&"alice@x.com".to_string()), None);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn string_keyed_table_iterates_in_key_order() {
        let mut t: Table<u32, String> = Table::new_keyed();
        t.put("c".to_string(), 3).unwrap();
        t.put("a".to_string(), 1).unwrap();
        t.put("b".to_string(), 2).unwrap();

        let keys: Vec<String> = t.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    }

    #[test]
    fn u64_table_still_auto_increments() {
        let mut t: Table<String> = Table::new();
        let a = t.insert("first".to_string()).unwrap();
        let b = t.insert("second".to_string()).unwrap();
        assert_eq!((a, b), (1, 2));
        assert_eq!(t.get(&1), Some(&"first".to_string()));
    }

    #[test]
    fn keyed_table_has_no_id_counter() {
        let mut t: Table<String, String> = Table::new_keyed();
        assert_eq!(t.next_id_opt(), None);
        t.put("k".to_string(), "v".to_string()).unwrap();
        assert_eq!(t.next_id_opt(), None);

        let auto: Table<String> = Table::new();
        assert_eq!(auto.next_id_opt(), Some(1));
    }

    // --- `put`/`insert` id-counter interaction (review Important 2) ---

    #[test]
    fn put_then_insert_does_not_collide() {
        // `put` at the id `insert` was about to hand out must not let the
        // next `insert` overwrite the put row.
        let mut t: Table<String> = Table::new();
        t.put(1, "put-value".to_string()).unwrap();
        let id = t.insert("insert-value".to_string()).unwrap();

        assert_eq!(id, 2, "insert must not reissue the key `put` occupied");
        assert_eq!(t.len(), 2);
        assert_eq!(t.get(&1), Some(&"put-value".to_string()));
        assert_eq!(t.get(&2), Some(&"insert-value".to_string()));
    }

    #[test]
    fn put_at_a_high_key_moves_the_counter_past_it() {
        let mut t: Table<String> = Table::new();
        t.insert("a".to_string()).unwrap(); // id 1
        t.put(500, "far".to_string()).unwrap();
        assert_eq!(t.next_id(), 501);
        assert_eq!(t.insert("b".to_string()).unwrap(), 501);

        // A put *below* the counter leaves it alone.
        t.put(2, "low".to_string()).unwrap();
        assert_eq!(t.next_id(), 502);
        assert_eq!(t.insert("c".to_string()).unwrap(), 502);
        assert_eq!(t.len(), 5);
    }

    #[test]
    fn put_on_a_keyed_u64_table_installs_the_counter() {
        // `Table::<R, u64>::new_keyed()` starts with no counter; writing a
        // key installs one past that key, so `insert` then works.
        let mut t: Table<String, u64> = Table::new_keyed();
        assert_eq!(t.next_id_opt(), None);
        t.put(7, "seven".to_string()).unwrap();
        assert_eq!(t.next_id_opt(), Some(8));
        assert_eq!(t.insert("eight".to_string()).unwrap(), 8);
    }

    #[test]
    #[should_panic(expected = "AutoKey table has next_id")]
    fn insert_on_an_unwritten_keyed_u64_table_panics() {
        // `u64` is both `PrimaryKey` and `AutoKey`, so `new_keyed` is
        // reachable for it and leaves no counter. Handing out id 1 here could
        // silently overwrite a row a later `put` places at 1 — panic instead.
        let mut t: Table<String, u64> = Table::new_keyed();
        let _ = t.insert("boom".to_string());
    }

    #[test]
    #[should_panic(expected = "AutoKey table has next_id")]
    fn insert_batch_on_an_unwritten_keyed_u64_table_panics() {
        let mut t: Table<String, u64> = Table::new_keyed();
        let _ = t.insert_batch(vec!["boom".to_string()]);
    }

    #[test]
    #[should_panic(expected = "AutoKey table has next_id")]
    fn next_id_on_an_unwritten_keyed_u64_table_panics() {
        let t: Table<String, u64> = Table::new_keyed();
        let _ = t.next_id();
    }

    #[test]
    fn keyed_table_maintains_secondary_indexes() {
        // Index maintenance has to route through the row key type, not `u64`.
        let mut t: Table<String, String> = Table::new_keyed();
        t.define_index("by_name", IndexKind::Unique, |name: &String| name.clone())
            .unwrap();
        t.put("alice@x.com".to_string(), "Alice".to_string()).unwrap();
        t.put("bob@x.com".to_string(), "Bob".to_string()).unwrap();

        assert_eq!(
            t.get_unique("by_name", &"Alice".to_string()).unwrap(),
            Some(("alice@x.com".to_string(), &"Alice".to_string()))
        );

        // A duplicate indexed value on a different row key is rejected.
        let err = t.put("carol@x.com".to_string(), "Alice".to_string());
        assert!(matches!(err, Err(Error::DuplicateKey(_))), "got {err:?}");

        t.delete(&"alice@x.com".to_string()).unwrap();
        assert_eq!(t.get_unique("by_name", &"Alice".to_string()).unwrap(), None);
    }

    #[test]
    fn keyed_table_index_key_differs_from_row_key() {
        // IK = u32 (age), K = String (email). With `IK == K` a swapped
        // `UniqueStorage<K, IK>` / `NonUniqueStorage<K, IK>` argument pair
        // would still typecheck *and* pass; here it cannot.
        let mut t: Table<User, String> = Table::new_keyed();
        t.define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
            .unwrap();
        t.define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone())
            .unwrap();

        for (email, age, name) in [
            ("a@x.com", 30u32, "Ann"),
            ("b@x.com", 30, "Bob"),
            ("c@x.com", 41, "Cat"),
        ] {
            t.put(
                email.to_string(),
                User {
                    email: email.to_string(),
                    age,
                    name: name.to_string(),
                },
            )
            .unwrap();
        }

        // Non-unique u32 index key -> String row keys, in row-key order.
        let thirty: Vec<String> = t
            .get_by_index("by_age", &30u32)
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(thirty, vec!["a@x.com".to_string(), "b@x.com".to_string()]);

        // Unique String index key -> String row key (different value space).
        let (row_key, rec) = t.get_unique("by_name", &"Cat".to_string()).unwrap().unwrap();
        assert_eq!(row_key, "c@x.com".to_string());
        assert_eq!(rec.age, 41);

        // Range over the u32 index key.
        let older: Vec<String> = t
            .index_range("by_age", 31u32..)
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(older, vec!["c@x.com".to_string()]);

        // Deleting by row key cleans the u32-keyed index.
        t.delete(&"a@x.com".to_string()).unwrap();
        let thirty: Vec<String> = t
            .get_by_index("by_age", &30u32)
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(thirty, vec!["b@x.com".to_string()]);
    }

    #[test]
    fn keyed_table_range_and_batches_use_explicit_keys() {
        let mut t: Table<u32, String> = Table::new_keyed();
        for (k, v) in [("a", 1u32), ("b", 2), ("c", 3), ("d", 4)] {
            t.put(k.to_string(), v).unwrap();
        }

        let in_range: Vec<u32> = t
            .range("b".to_string().."d".to_string())
            .map(|(_, v)| *v)
            .collect();
        assert_eq!(in_range, vec![2, 3]);

        t.update_batch(vec![("a".to_string(), 10), ("b".to_string(), 20)])
            .unwrap();
        assert_eq!(t.get(&"a".to_string()), Some(&10));
        assert_eq!(t.get(&"b".to_string()), Some(&20));

        t.delete_batch(&["a".to_string(), "d".to_string()]).unwrap();
        assert_eq!(t.len(), 2);
        assert_eq!(t.first().map(|(k, _)| k.clone()), Some("b".to_string()));
        assert_eq!(t.last().map(|(k, _)| k.clone()), Some("c".to_string()));
    }

    #[test]
    fn merge_keys_from_downcasts_the_erased_key_set() {
        // The commit-path merge over a non-`u64` key type.
        let mut base: Table<String, String> = Table::new_keyed();
        base.put("a".to_string(), "base-a".to_string()).unwrap();
        base.put("b".to_string(), "base-b".to_string()).unwrap();

        let mut writer = base.clone();
        writer.put("a".to_string(), "writer-a".to_string()).unwrap();
        writer.delete(&"b".to_string()).unwrap();

        let keys: BTreeSet<String> = ["a".to_string(), "b".to_string()].into_iter().collect();
        base.merge_keys_from(&writer, &keys as &dyn Any).unwrap();

        assert_eq!(base.get(&"a".to_string()), Some(&"writer-a".to_string()));
        assert_eq!(base.get(&"b".to_string()), None);
    }

    #[test]
    fn merge_keys_from_rejects_a_mismatched_key_set() {
        let mut base: Table<String, String> = Table::new_keyed();
        base.put("a".to_string(), "base-a".to_string()).unwrap();
        let writer = base.clone();

        // A `BTreeSet<u64>` against a `String`-keyed table is an internal bug.
        let wrong: BTreeSet<u64> = [1u64].into_iter().collect();
        let err = base.merge_keys_from(&writer, &wrong as &dyn Any);
        assert!(matches!(err, Err(Error::TypeMismatch(_))), "got {err:?}");
    }

    #[test]
    fn insert_returns_id_starting_at_one() {
        let mut table: Table<String> = Table::new();
        assert_eq!(table.insert("first".to_string()).unwrap(), 1);
    }

    #[test]
    fn insert_returns_incrementing_ids() {
        let mut table: Table<String> = Table::new();
        assert_eq!(table.insert("a".to_string()).unwrap(), 1);
        assert_eq!(table.insert("b".to_string()).unwrap(), 2);
        assert_eq!(table.insert("c".to_string()).unwrap(), 3);
    }

    #[test]
    fn get_returns_inserted_record() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("hello".to_string()).unwrap();
        assert_eq!(table.get(&id), Some(&"hello".to_string()));
    }

    #[test]
    fn get_on_absent_id_returns_none() {
        let table: Table<String> = Table::new();
        assert_eq!(table.get(&99), None);
    }

    #[test]
    fn update_replaces_record() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("original".to_string()).unwrap();
        table.update(&id, "updated".to_string()).unwrap();
        assert_eq!(table.get(&id), Some(&"updated".to_string()));
    }

    #[test]
    fn update_on_absent_id_returns_key_not_found() {
        let mut table: Table<String> = Table::new();
        let result = table.update(&99, "x".to_string());
        assert!(matches!(result, Err(crate::Error::KeyNotFound)));
    }

    #[test]
    fn delete_removes_record() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("bye".to_string()).unwrap();
        table.delete(&id).unwrap();
        assert_eq!(table.get(&id), None);
    }

    #[test]
    fn delete_on_absent_id_returns_key_not_found() {
        let mut table: Table<String> = Table::new();
        let result = table.delete(&99);
        assert!(matches!(result, Err(crate::Error::KeyNotFound)));
    }

    #[test]
    fn range_yields_records_in_order() {
        let mut table: Table<String> = Table::new();
        table.insert("a".into()).unwrap();
        table.insert("b".into()).unwrap();
        table.insert("c".into()).unwrap();
        let results: Vec<_> = table.range(1..=3).collect();
        assert_eq!(
            results,
            vec![
                (&1, &"a".to_string()),
                (&2, &"b".to_string()),
                (&3, &"c".to_string())
            ]
        );
    }

    #[test]
    fn range_with_partial_bounds() {
        let mut table: Table<String> = Table::new();
        table.insert("a".into()).unwrap();
        table.insert("b".into()).unwrap();
        table.insert("c".into()).unwrap();
        let results: Vec<_> = table.range(2..).collect();
        assert_eq!(results, vec![(&2, &"b".to_string()), (&3, &"c".to_string())]);
    }

    #[test]
    fn range_on_empty_table_yields_nothing() {
        let table: Table<String> = Table::new();
        let results: Vec<_> = table.range(..).collect();
        assert!(results.is_empty());
    }

    #[test]
    fn new_table_is_empty() {
        let table: Table<String> = Table::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn len_reflects_insert_and_delete() {
        let mut table: Table<String> = Table::new();
        assert_eq!(table.len(), 0);
        let id = table.insert("a".to_string()).unwrap();
        assert_eq!(table.len(), 1);
        table.delete(&id).unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn table_clone_is_independent() {
        let mut original: Table<String> = Table::new();
        original.insert("alice".to_string()).unwrap();
        let clone = original.clone();
        original.insert("bob".to_string()).unwrap(); // mutate original after clone
        // Clone is unaffected
        assert_eq!(clone.len(), 1);
        assert_eq!(clone.get(&2), None);
    }

    #[test]
    fn table_clone_preserves_next_id() {
        let mut original: Table<String> = Table::new();
        original.insert("a".to_string()).unwrap(); // id 1
        original.insert("b".to_string()).unwrap(); // id 2
        let mut clone = original.clone();
        // Next insert in clone should continue from id 3
        let id = clone.insert("c".to_string()).unwrap();
        assert_eq!(id, 3);
        // Verify no ID collision
        assert_eq!(clone.get(&1), Some(&"a".to_string()));
        assert_eq!(clone.get(&3), Some(&"c".to_string()));
    }

    #[derive(Debug, Clone, PartialEq)]
    #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
    struct User {
        email: String,
        age: u32,
        name: String,
    }

    #[test]
    fn define_index_idempotent_same_kind() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("idx", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        // Redefining with same name and same kind should be Ok (idempotent)
        table
            .define_index("idx", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();

        // The original index should still be there and be Unique
        table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        let res = table.insert(User {
            email: "a@x.com".into(),
            age: 25,
            name: "B".into(),
        });
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));
    }

    #[test]
    fn define_index_rejects_kind_mismatch() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("idx", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        // Redefining with same name but different kind should fail
        let res = table.define_index("idx", IndexKind::NonUnique, |u: &User| u.age);
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));
    }

    #[test]
    fn define_index_rejects_custom_kind() {
        let mut table: Table<User> = Table::new();
        let res = table.define_index("idx", IndexKind::Custom, |u: &User| u.email.clone());
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));
    }

    #[test]
    fn define_index_rejects_collision_with_custom_index() {
        let mut table: Table<User> = Table::new();
        table
            .define_custom_index("idx", SumIndex::new(|u| u.age as u64))
            .unwrap();
        // Trying to define a built-in index with the same name as a custom index
        let res = table.define_index("idx", IndexKind::Unique, |u: &User| u.email.clone());
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));
    }

    #[test]
    fn query_wrong_index_type_returns_error() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("unique", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .define_index("non_unique", IndexKind::NonUnique, |u: &User| u.age)
            .unwrap();

        // get_unique on non-unique index
        let res = table.get_unique::<u32>("non_unique", &30);
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));

        // get_by_index on unique index
        let res = table.get_by_index::<String>("unique", &"a@x.com".to_string());
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));
    }

    #[test]
    fn define_index_backfill_failure() {
        let mut table: Table<User> = Table::new();
        table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        table
            .insert(User {
                email: "a@x.com".into(),
                age: 25,
                name: "B".into(),
            })
            .unwrap();

        // Defining unique index on duplicate data should fail
        let res = table.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone());
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));

        // Index should NOT be registered
        let res = table.get_unique::<String>("by_email", &"a@x.com".to_string());
        assert!(matches!(res, Err(crate::Error::IndexNotFound(_))));
    }

    #[test]
    fn table_clone_indexes_are_independent() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("idx", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();

        let mut clone = table.clone();

        // Insert into original
        table
            .insert(User {
                email: "b@x.com".into(),
                age: 25,
                name: "B".into(),
            })
            .unwrap();

        // Clone should NOT have b@x.com in its index
        assert!(
            clone
                .get_unique::<String>("idx", &"b@x.com".to_string())
                .unwrap()
                .is_none()
        );

        // Insert into clone
        clone
            .insert(User {
                email: "c@x.com".into(),
                age: 20,
                name: "C".into(),
            })
            .unwrap();

        // Original should NOT have c@x.com in its index
        assert!(
            table
                .get_unique::<String>("idx", &"c@x.com".to_string())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn index_range_type_mismatch() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();

        // Querying String index with u32 range
        let res = table.index_range::<u32>("by_email", 20..30);
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));
    }

    // -------------------------------------------------------------------
    // Batch operation tests
    // -------------------------------------------------------------------

    #[test]
    fn insert_batch_returns_sequential_ids() {
        let mut table: Table<String> = Table::new();
        table.insert("pre".to_string()).unwrap(); // id 1
        let ids = table
            .insert_batch(vec!["a".into(), "b".into(), "c".into()])
            .unwrap();
        assert_eq!(ids, vec![2, 3, 4]);
        assert_eq!(table.get(&2), Some(&"a".to_string()));
        assert_eq!(table.get(&3), Some(&"b".to_string()));
        assert_eq!(table.get(&4), Some(&"c".to_string()));
        assert_eq!(table.len(), 4);
    }

    #[test]
    fn insert_batch_empty_is_noop() {
        let mut table: Table<String> = Table::new();
        table.insert("existing".to_string()).unwrap();
        let ids = table.insert_batch(vec![]).unwrap();
        assert!(ids.is_empty());
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn insert_batch_bulk_path_matches_per_key_semantics() {
        // Interleave singles and batches; ids must stay sequential and every
        // record retrievable — exercises repeated seeding of a grown tree.
        let mut table: Table<String> = Table::new();
        let mut expected_id = 1u64;
        for round in 0..20 {
            let n = 50 * (round % 4) + 1;
            let ids = table
                .insert_batch((0..n).map(|i| format!("r{round}-{i}")).collect())
                .unwrap();
            assert_eq!(ids.first().copied(), Some(expected_id));
            assert_eq!(ids.len(), n as usize);
            expected_id += n as u64;
            let single = table.insert(format!("s{round}")).unwrap();
            assert_eq!(single, expected_id);
            expected_id += 1;
        }
        assert_eq!(table.len() as u64, expected_id - 1);
        for id in 1..expected_id {
            assert!(table.get(&id).is_some(), "missing id {id}");
        }
    }

    #[test]
    fn insert_batch_falls_back_when_next_id_behind_max_key() {
        // Corrupt next_id below the max key (unreachable in production; the
        // guard exists exactly for this). The fallback must keep legacy
        // per-key semantics: replace at colliding ids, table stays coherent.
        let mut table: Table<String> = Table::new();
        table
            .insert_batch((0..100).map(|i| format!("v{i}")).collect())
            .unwrap(); // ids 1..=100
        table.next_id = Some(50);
        let ids = table.insert_batch(vec!["x".into(), "y".into()]).unwrap();
        assert_eq!(ids, vec![50, 51]);
        assert_eq!(table.get(&50), Some(&"x".to_string()));
        assert_eq!(table.get(&51), Some(&"y".to_string()));
        assert_eq!(table.len(), 100); // replaced, not added
    }

    #[test]
    fn insert_batch_unique_constraint_within_batch() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();

        let res = table.insert_batch(vec![
            User {
                email: "dup@x.com".into(),
                age: 30,
                name: "A".into(),
            },
            User {
                email: "dup@x.com".into(),
                age: 25,
                name: "B".into(),
            },
        ]);
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));
        // Table should be unchanged
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn insert_batch_unique_constraint_against_existing() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .insert(User {
                email: "taken@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();

        let res = table.insert_batch(vec![
            User {
                email: "new@x.com".into(),
                age: 25,
                name: "B".into(),
            },
            User {
                email: "taken@x.com".into(),
                age: 20,
                name: "C".into(),
            },
        ]);
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));
        // Table should still have only the original record
        assert_eq!(table.len(), 1);
        assert!(
            table
                .get_unique::<String>("by_email", &"new@x.com".to_string())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn insert_batch_updates_all_indexes() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
            .unwrap();

        let ids = table
            .insert_batch(vec![
                User {
                    email: "a@x.com".into(),
                    age: 30,
                    name: "A".into(),
                },
                User {
                    email: "b@x.com".into(),
                    age: 30,
                    name: "B".into(),
                },
            ])
            .unwrap();

        assert_eq!(
            table
                .get_unique::<String>("by_email", &"a@x.com".to_string())
                .unwrap()
                .map(|(id, _)| id),
            Some(ids[0])
        );
        assert_eq!(table.get_by_index::<u32>("by_age", &30).unwrap().len(), 2);
    }

    #[test]
    fn update_batch_modifies_records() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        let id1 = table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        let id2 = table
            .insert(User {
                email: "b@x.com".into(),
                age: 25,
                name: "B".into(),
            })
            .unwrap();

        table
            .update_batch(vec![
                (
                    id1,
                    User {
                        email: "a_new@x.com".into(),
                        age: 31,
                        name: "A".into(),
                    },
                ),
                (
                    id2,
                    User {
                        email: "b_new@x.com".into(),
                        age: 26,
                        name: "B".into(),
                    },
                ),
            ])
            .unwrap();

        assert_eq!(table.get(&id1).unwrap().email, "a_new@x.com");
        assert_eq!(table.get(&id2).unwrap().email, "b_new@x.com");
        // Old index entries gone, new ones present
        assert!(
            table
                .get_unique::<String>("by_email", &"a@x.com".to_string())
                .unwrap()
                .is_none()
        );
        assert!(
            table
                .get_unique::<String>("by_email", &"a_new@x.com".to_string())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn update_batch_missing_id_fails_fast() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("original".to_string()).unwrap();

        let res = table.update_batch(vec![
            (id, "updated".to_string()),
            (999, "missing".to_string()),
        ]);
        assert!(matches!(res, Err(crate::Error::KeyNotFound)));
        // Original should be unchanged
        assert_eq!(table.get(&id), Some(&"original".to_string()));
    }

    #[test]
    fn update_batch_unique_constraint_rolls_back() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        let id1 = table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        let id2 = table
            .insert(User {
                email: "b@x.com".into(),
                age: 25,
                name: "B".into(),
            })
            .unwrap();

        // Try to update both to the same email
        let res = table.update_batch(vec![
            (
                id1,
                User {
                    email: "same@x.com".into(),
                    age: 30,
                    name: "A".into(),
                },
            ),
            (
                id2,
                User {
                    email: "same@x.com".into(),
                    age: 25,
                    name: "B".into(),
                },
            ),
        ]);
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));
        // Both records and indexes should be unchanged
        assert_eq!(table.get(&id1).unwrap().email, "a@x.com");
        assert_eq!(table.get(&id2).unwrap().email, "b@x.com");
        assert!(
            table
                .get_unique::<String>("by_email", &"a@x.com".to_string())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn delete_batch_removes_records_and_indexes() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        let id1 = table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        let id2 = table
            .insert(User {
                email: "b@x.com".into(),
                age: 25,
                name: "B".into(),
            })
            .unwrap();
        let id3 = table
            .insert(User {
                email: "c@x.com".into(),
                age: 20,
                name: "C".into(),
            })
            .unwrap();

        table.delete_batch(&[id1, id3]).unwrap();

        assert_eq!(table.get(&id1), None);
        assert_eq!(table.get(&id3), None);
        assert_eq!(table.get(&id2).unwrap().email, "b@x.com");
        assert_eq!(table.len(), 1);
        assert!(
            table
                .get_unique::<String>("by_email", &"a@x.com".to_string())
                .unwrap()
                .is_none()
        );
        assert!(
            table
                .get_unique::<String>("by_email", &"c@x.com".to_string())
                .unwrap()
                .is_none()
        );
        assert!(
            table
                .get_unique::<String>("by_email", &"b@x.com".to_string())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn delete_batch_missing_id_fails_fast() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("keep".to_string()).unwrap();

        let res = table.delete_batch(&[id, 999]);
        assert!(matches!(res, Err(crate::Error::KeyNotFound)));
        // Original should be unchanged
        assert_eq!(table.get(&id), Some(&"keep".to_string()));
    }

    #[test]
    fn delete_batch_duplicate_ids() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("hello".to_string()).unwrap();

        table.delete_batch(&[id, id]).unwrap();
        assert_eq!(table.get(&id), None);
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn update_batch_empty_is_noop() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("original".to_string()).unwrap();
        table.update_batch(vec![]).unwrap();
        assert_eq!(table.get(&id), Some(&"original".to_string()));
    }

    #[test]
    fn delete_batch_empty_is_noop() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("original".to_string()).unwrap();
        table.delete_batch(&[]).unwrap();
        assert_eq!(table.get(&id), Some(&"original".to_string()));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn update_batch_non_unique_index() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
            .unwrap();
        let id1 = table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        let id2 = table
            .insert(User {
                email: "b@x.com".into(),
                age: 30,
                name: "B".into(),
            })
            .unwrap();

        table
            .update_batch(vec![
                (
                    id1,
                    User {
                        email: "a@x.com".into(),
                        age: 40,
                        name: "A".into(),
                    },
                ),
                (
                    id2,
                    User {
                        email: "b@x.com".into(),
                        age: 50,
                        name: "B".into(),
                    },
                ),
            ])
            .unwrap();

        assert!(table.get_by_index::<u32>("by_age", &30).unwrap().is_empty());
        assert_eq!(table.get_by_index::<u32>("by_age", &40).unwrap().len(), 1);
        assert_eq!(table.get_by_index::<u32>("by_age", &50).unwrap().len(), 1);
    }

    #[test]
    fn update_batch_unique_constraint_against_existing() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        let id1 = table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        let _id2 = table
            .insert(User {
                email: "b@x.com".into(),
                age: 25,
                name: "B".into(),
            })
            .unwrap();

        // Update id1's email to collide with the untouched id2
        let res = table.update_batch(vec![(
            id1,
            User {
                email: "b@x.com".into(),
                age: 30,
                name: "A".into(),
            },
        )]);
        assert!(matches!(res, Err(crate::Error::DuplicateKey(_))));
        // Original should be unchanged
        assert_eq!(table.get(&id1).unwrap().email, "a@x.com");
    }

    #[test]
    fn update_batch_duplicate_ids_last_wins() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("original".to_string()).unwrap();

        table
            .update_batch(vec![(id, "first".to_string()), (id, "second".to_string())])
            .unwrap();
        assert_eq!(table.get(&id), Some(&"second".to_string()));
    }

    #[test]
    fn insert_batch_rollback_restores_next_id() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .insert(User {
                email: "existing@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        // next_id is now 2

        let res = table.insert_batch(vec![
            User {
                email: "new@x.com".into(),
                age: 25,
                name: "B".into(),
            },
            User {
                email: "existing@x.com".into(),
                age: 20,
                name: "C".into(),
            }, // conflict
        ]);
        assert!(res.is_err());

        // After rollback, next insert should get id 2, not 4
        let id = table
            .insert(User {
                email: "ok@x.com".into(),
                age: 22,
                name: "D".into(),
            })
            .unwrap();
        assert_eq!(id, 2);
    }

    #[test]
    fn insert_batch_multi_index_rollback() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone())
            .unwrap();

        table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "Alice".into(),
            })
            .unwrap();

        // Second record conflicts on name (not email) — both indexes must roll back
        let res = table.insert_batch(vec![
            User {
                email: "b@x.com".into(),
                age: 25,
                name: "Bob".into(),
            },
            User {
                email: "c@x.com".into(),
                age: 20,
                name: "Alice".into(),
            }, // name conflict
        ]);
        assert!(res.is_err());
        assert_eq!(table.len(), 1);
        // The first record's entries in BOTH indexes should be rolled back
        assert!(
            table
                .get_unique::<String>("by_email", &"b@x.com".to_string())
                .unwrap()
                .is_none()
        );
        assert!(
            table
                .get_unique::<String>("by_name", &"Bob".to_string())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn update_batch_multi_index_rollback() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone())
            .unwrap();

        let id1 = table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "Alice".into(),
            })
            .unwrap();
        let id2 = table
            .insert(User {
                email: "b@x.com".into(),
                age: 25,
                name: "Bob".into(),
            })
            .unwrap();

        // Update both; second conflicts on name with first's NEW name
        let res = table.update_batch(vec![
            (
                id1,
                User {
                    email: "a_new@x.com".into(),
                    age: 30,
                    name: "Charlie".into(),
                },
            ),
            (
                id2,
                User {
                    email: "b_new@x.com".into(),
                    age: 25,
                    name: "Charlie".into(),
                },
            ),
        ]);
        assert!(res.is_err());
        // Both records and all indexes should be unchanged
        assert_eq!(table.get(&id1).unwrap().email, "a@x.com");
        assert_eq!(table.get(&id1).unwrap().name, "Alice");
        assert!(
            table
                .get_unique::<String>("by_email", &"a@x.com".to_string())
                .unwrap()
                .is_some()
        );
        assert!(
            table
                .get_unique::<String>("by_name", &"Alice".to_string())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn table_usable_after_failed_insert_batch() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();

        // Fail a batch
        let res = table.insert_batch(vec![
            User {
                email: "dup@x.com".into(),
                age: 30,
                name: "A".into(),
            },
            User {
                email: "dup@x.com".into(),
                age: 25,
                name: "B".into(),
            },
        ]);
        assert!(res.is_err());

        // Table should be fully functional
        let id = table
            .insert(User {
                email: "ok@x.com".into(),
                age: 20,
                name: "C".into(),
            })
            .unwrap();
        assert_eq!(table.get(&id).unwrap().email, "ok@x.com");
        table
            .update(
                &id,
                User {
                    email: "ok2@x.com".into(),
                    age: 21,
                    name: "C".into(),
                },
            )
            .unwrap();
        assert_eq!(table.get(&id).unwrap().email, "ok2@x.com");
        table.delete(&id).unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn table_usable_after_failed_update_batch() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        let id1 = table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        let id2 = table
            .insert(User {
                email: "b@x.com".into(),
                age: 25,
                name: "B".into(),
            })
            .unwrap();

        // Fail a batch update
        let res = table.update_batch(vec![
            (
                id1,
                User {
                    email: "same@x.com".into(),
                    age: 30,
                    name: "A".into(),
                },
            ),
            (
                id2,
                User {
                    email: "same@x.com".into(),
                    age: 25,
                    name: "B".into(),
                },
            ),
        ]);
        assert!(res.is_err());

        // Table should be fully functional — can do single-record ops
        table
            .update(
                &id1,
                User {
                    email: "a_new@x.com".into(),
                    age: 31,
                    name: "A".into(),
                },
            )
            .unwrap();
        assert_eq!(table.get(&id1).unwrap().email, "a_new@x.com");
        assert!(
            table
                .get_unique::<String>("by_email", &"a_new@x.com".to_string())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn delete_batch_non_unique_index() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
            .unwrap();
        let id1 = table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        let id2 = table
            .insert(User {
                email: "b@x.com".into(),
                age: 30,
                name: "B".into(),
            })
            .unwrap();
        let _id3 = table
            .insert(User {
                email: "c@x.com".into(),
                age: 25,
                name: "C".into(),
            })
            .unwrap();

        table.delete_batch(&[id1, id2]).unwrap();

        assert!(table.get_by_index::<u32>("by_age", &30).unwrap().is_empty());
        assert_eq!(table.get_by_index::<u32>("by_age", &25).unwrap().len(), 1);
    }

    // -------------------------------------------------------------------
    // Coverage: get_by_key, index_range, single-op index rollback
    // -------------------------------------------------------------------

    #[test]
    fn get_by_key_on_unique_index() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();

        let results = table
            .get_by_key::<String>("by_email", &"a@x.com".to_string())
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.email, "a@x.com");

        let results = table
            .get_by_key::<String>("by_email", &"missing@x.com".to_string())
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn get_by_key_on_non_unique_index() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
            .unwrap();
        table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        table
            .insert(User {
                email: "b@x.com".into(),
                age: 30,
                name: "B".into(),
            })
            .unwrap();
        table
            .insert(User {
                email: "c@x.com".into(),
                age: 25,
                name: "C".into(),
            })
            .unwrap();

        let results = table.get_by_key::<u32>("by_age", &30).unwrap();
        assert_eq!(results.len(), 2);

        let results = table.get_by_key::<u32>("by_age", &99).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn get_by_key_nonexistent_index() {
        let table: Table<User> = Table::new();
        let res = table.get_by_key::<String>("nope", &"x".to_string());
        assert!(matches!(res, Err(crate::Error::IndexNotFound(_))));
    }

    #[test]
    fn get_by_key_wrong_key_type() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        // Query String index with u32 key
        let res = table.get_by_key::<u32>("by_email", &42);
        assert!(matches!(res, Err(crate::Error::IndexTypeMismatch(_))));
    }

    #[test]
    fn index_range_unique() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_age", IndexKind::Unique, |u: &User| u.age)
            .unwrap();
        table
            .insert(User {
                email: "a@x.com".into(),
                age: 20,
                name: "A".into(),
            })
            .unwrap();
        table
            .insert(User {
                email: "b@x.com".into(),
                age: 30,
                name: "B".into(),
            })
            .unwrap();
        table
            .insert(User {
                email: "c@x.com".into(),
                age: 40,
                name: "C".into(),
            })
            .unwrap();

        let results = table.index_range::<u32>("by_age", 25..=35).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.age, 30);

        // Full range
        let results = table.index_range::<u32>("by_age", ..).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn index_range_non_unique() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
            .unwrap();
        table
            .insert(User {
                email: "a@x.com".into(),
                age: 20,
                name: "A".into(),
            })
            .unwrap();
        table
            .insert(User {
                email: "b@x.com".into(),
                age: 30,
                name: "B".into(),
            })
            .unwrap();
        table
            .insert(User {
                email: "c@x.com".into(),
                age: 30,
                name: "C".into(),
            })
            .unwrap();
        table
            .insert(User {
                email: "d@x.com".into(),
                age: 40,
                name: "D".into(),
            })
            .unwrap();

        let results = table.index_range::<u32>("by_age", 25..=35).unwrap();
        assert_eq!(results.len(), 2);

        let results = table.index_range::<u32>("by_age", ..30).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.age, 20);
    }

    #[test]
    fn index_range_nonexistent_index() {
        let table: Table<User> = Table::new();
        let res = table.index_range::<u32>("nope", ..);
        assert!(matches!(res, Err(crate::Error::IndexNotFound(_))));
    }

    #[test]
    fn single_insert_multi_index_rollback() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone())
            .unwrap();

        table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "Alice".into(),
            })
            .unwrap();

        // Conflict on one index — both indexes should roll back the failed insert
        let res = table.insert(User {
            email: "b@x.com".into(),
            age: 25,
            name: "Alice".into(),
        });
        assert!(res.is_err());
        assert_eq!(table.len(), 1);
        // The non-conflicting index should NOT contain the failed record's email
        assert!(
            table
                .get_unique::<String>("by_email", &"b@x.com".to_string())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn single_update_multi_index_rollback() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone())
            .unwrap();

        let id1 = table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "Alice".into(),
            })
            .unwrap();
        let _id2 = table
            .insert(User {
                email: "b@x.com".into(),
                age: 25,
                name: "Bob".into(),
            })
            .unwrap();

        // Update id1 to collide with id2 on name — should roll back email index change too
        let res = table.update(
            &id1,
            User {
                email: "a_new@x.com".into(),
                age: 30,
                name: "Bob".into(),
            },
        );
        assert!(res.is_err());
        // Email index should still have old value, not new
        assert!(
            table
                .get_unique::<String>("by_email", &"a@x.com".to_string())
                .unwrap()
                .is_some()
        );
        assert!(
            table
                .get_unique::<String>("by_email", &"a_new@x.com".to_string())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn single_delete_removes_index_entries() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
            .unwrap();

        let id = table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        table.delete(&id).unwrap();

        assert!(
            table
                .get_unique::<String>("by_email", &"a@x.com".to_string())
                .unwrap()
                .is_none()
        );
        assert!(table.get_by_index::<u32>("by_age", &30).unwrap().is_empty());
    }

    #[test]
    fn insert_batch_large() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        table
            .define_index("by_age", IndexKind::NonUnique, |u: &User| u.age)
            .unwrap();

        let records: Vec<User> = (0..500)
            .map(|i| User {
                email: format!("user{}@x.com", i),
                age: (i % 50) as u32,
                name: format!("User{}", i),
            })
            .collect();

        let ids = table.insert_batch(records).unwrap();
        assert_eq!(ids.len(), 500);
        assert_eq!(table.len(), 500);

        // Spot-check indexes
        assert!(
            table
                .get_unique::<String>("by_email", &"user0@x.com".to_string())
                .unwrap()
                .is_some()
        );
        assert!(
            table
                .get_unique::<String>("by_email", &"user499@x.com".to_string())
                .unwrap()
                .is_some()
        );
        // age 0 should have 10 records (0, 50, 100, ..., 450)
        assert_eq!(table.get_by_index::<u32>("by_age", &0).unwrap().len(), 10);
    }

    // -------------------------------------------------------------------
    // Convenience method tests
    // -------------------------------------------------------------------

    #[test]
    fn delete_returns_old_record() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("hello".to_string()).unwrap();
        let old = table.delete(&id).unwrap();
        assert_eq!(*old, "hello".to_string());
    }

    #[test]
    fn contains_true_for_existing_id() {
        let mut table: Table<String> = Table::new();
        let id = table.insert("x".to_string()).unwrap();
        assert!(table.contains(&id));
    }

    #[test]
    fn contains_false_for_absent_id() {
        let table: Table<String> = Table::new();
        assert!(!table.contains(&99));
    }

    #[test]
    fn first_returns_min_id_record() {
        let mut table: Table<String> = Table::new();
        table.insert("a".to_string()).unwrap();
        table.insert("b".to_string()).unwrap();
        let (id, val) = table.first().unwrap();
        assert_eq!(id, &1);
        assert_eq!(val, &"a".to_string());
    }

    #[test]
    fn first_on_empty_returns_none() {
        let table: Table<String> = Table::new();
        assert!(table.first().is_none());
    }

    #[test]
    fn last_returns_max_id_record() {
        let mut table: Table<String> = Table::new();
        table.insert("a".to_string()).unwrap();
        table.insert("b".to_string()).unwrap();
        let (id, val) = table.last().unwrap();
        assert_eq!(id, &2);
        assert_eq!(val, &"b".to_string());
    }

    #[test]
    fn last_on_empty_returns_none() {
        let table: Table<String> = Table::new();
        assert!(table.last().is_none());
    }

    #[test]
    fn iter_yields_all_in_order() {
        let mut table: Table<String> = Table::new();
        table.insert("a".into()).unwrap();
        table.insert("b".into()).unwrap();
        table.insert("c".into()).unwrap();
        let results: Vec<_> = table.iter().collect();
        assert_eq!(
            results,
            vec![
                (&1, &"a".to_string()),
                (&2, &"b".to_string()),
                (&3, &"c".to_string())
            ]
        );
    }

    #[test]
    fn get_many_returns_matching_records() {
        let mut table: Table<String> = Table::new();
        table.insert("a".to_string()).unwrap();
        table.insert("b".to_string()).unwrap();
        table.insert("c".to_string()).unwrap();
        let results = table.get_many(&[1, 3, 99]);
        assert_eq!(results[0], Some(&"a".to_string()));
        assert_eq!(results[1], Some(&"c".to_string()));
        assert_eq!(results[2], None);
    }

    /// Indexes are maintained in deterministic (alphabetical) order.
    /// This ensures identical error messages across replicas when a
    /// constraint violation occurs.
    #[test]
    fn index_maintenance_order_is_deterministic() {
        let mut table: Table<(String, String)> = Table::new();
        table
            .define_index("zzz_idx", IndexKind::Unique, |r: &(String, String)| {
                r.0.clone()
            })
            .unwrap();
        table
            .define_index("aaa_idx", IndexKind::Unique, |r: &(String, String)| {
                r.1.clone()
            })
            .unwrap();
        table.insert(("x".into(), "y".into())).unwrap();

        // This violates both indexes. With BTreeMap, "aaa_idx" is checked first.
        let err = table.insert(("x".into(), "y".into())).unwrap_err();
        assert!(
            matches!(err, Error::DuplicateKey(ref name) if name == "aaa_idx"),
            "expected DuplicateKey(aaa_idx), got: {err:?}"
        );
    }

    #[test]
    fn default_creates_empty_table() {
        let table: Table<String> = Table::default();
        assert_eq!(table.len(), 0);
        assert_eq!(table.next_id(), 1);
    }

    #[test]
    fn insert_with_id_duplicate_returns_error() {
        let mut table = Table::<String>::new();
        table.insert_with_id(1, "first".into()).unwrap();
        let err = table.insert_with_id(1, "duplicate".into()).unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(_)));
        // Original record is preserved.
        assert_eq!(table.get(&1).unwrap(), "first");
    }

    #[test]
    fn insert_with_id_unique_index_violation_rolls_back() {
        let mut table = Table::<(String, u32)>::new();
        table
            .define_index("name", IndexKind::Unique, |r: &(String, u32)| r.0.clone())
            .unwrap();

        table.insert_with_id(1, ("alice".into(), 30)).unwrap();

        // Insert with a different ID but duplicate index key.
        let err = table.insert_with_id(2, ("alice".into(), 25)).unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(_)));

        // ID 2 should not exist; original record preserved.
        assert_eq!(table.len(), 1);
        assert!(table.get(&2).is_none());
        assert_eq!(table.get(&1).unwrap().0, "alice");

        // A new unique key should still insert successfully (index wasn't corrupted).
        table.insert_with_id(3, ("bob".into(), 40)).unwrap();
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn insert_with_id_multi_index_partial_rollback() {
        // Two unique indexes. The second one fails, so the first must be rolled back.
        let mut table = Table::<(String, String)>::new();
        // BTreeMap iterates alphabetically, so "idx_a" is checked before "idx_b".
        table
            .define_index("idx_a", IndexKind::Unique, |r: &(String, String)| {
                r.0.clone()
            })
            .unwrap();
        table
            .define_index("idx_b", IndexKind::Unique, |r: &(String, String)| {
                r.1.clone()
            })
            .unwrap();

        table
            .insert_with_id(1, ("alice".into(), "x".into()))
            .unwrap();

        // Second insert: unique key for idx_a ("bob") but duplicate for idx_b ("x").
        // idx_a succeeds first, then idx_b fails → idx_a must be rolled back.
        let err = table
            .insert_with_id(2, ("bob".into(), "x".into()))
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(_)));
        assert_eq!(table.len(), 1);
        assert!(table.get(&2).is_none());

        // "bob" should NOT be in idx_a after rollback.
        table.insert_with_id(3, ("bob".into(), "y".into())).unwrap();
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn table_def_const_new() {
        const DEF: TableDef<String> = TableDef::new("my_table");
        assert_eq!(DEF.name(), "my_table");
    }

    /// update_batch with duplicate IDs and a unique index should succeed
    /// (last value wins), not false-fail with DuplicateKey.
    #[test]
    fn update_batch_duplicate_ids_with_unique_index() {
        let mut table: Table<User> = Table::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        let id = table
            .insert(User {
                email: "a@x.com".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();

        // Same ID appears twice — last value should win.
        table
            .update_batch(vec![
                (
                    id,
                    User {
                        email: "b@x.com".into(),
                        age: 31,
                        name: "A".into(),
                    },
                ),
                (
                    id,
                    User {
                        email: "c@x.com".into(),
                        age: 32,
                        name: "A".into(),
                    },
                ),
            ])
            .unwrap();

        assert_eq!(table.get(&id).unwrap().email, "c@x.com");
        assert_eq!(table.get(&id).unwrap().age, 32);
        // Index should reflect the final value only.
        assert!(
            table
                .get_unique::<String>("by_email", &"a@x.com".to_string())
                .unwrap()
                .is_none()
        );
        assert!(
            table
                .get_unique::<String>("by_email", &"b@x.com".to_string())
                .unwrap()
                .is_none()
        );
        assert!(
            table
                .get_unique::<String>("by_email", &"c@x.com".to_string())
                .unwrap()
                .is_some()
        );
    }

    // -------------------------------------------------------------------
    // Custom index tests
    // -------------------------------------------------------------------

    #[derive(Clone)]
    struct SumIndex {
        total: u64,
        field_extractor: Arc<dyn Fn(&User) -> u64 + Send + Sync>,
    }

    impl SumIndex {
        fn new(extractor: impl Fn(&User) -> u64 + Send + Sync + 'static) -> Self {
            Self {
                total: 0,
                field_extractor: Arc::new(extractor),
            }
        }

        fn total(&self) -> u64 {
            self.total
        }
    }

    impl CustomIndex<User> for SumIndex {
        fn on_insert(&mut self, _id: u64, record: &User) -> Result<()> {
            self.total += (self.field_extractor)(record);
            Ok(())
        }

        fn on_update(&mut self, _id: u64, old: &User, new: &User) -> Result<()> {
            self.total -= (self.field_extractor)(old);
            self.total += (self.field_extractor)(new);
            Ok(())
        }

        fn on_delete(&mut self, _id: u64, record: &User) {
            self.total -= (self.field_extractor)(record);
        }
    }

    // `K` defaults to `u64` on `CustomIndex`, but any `PrimaryKey` works —
    // this second impl, keyed by `String`, is what `custom_index_on_a_
    // string_keyed_table` below exercises.
    impl CustomIndex<User, String> for SumIndex {
        fn on_insert(&mut self, _key: String, record: &User) -> Result<()> {
            self.total += (self.field_extractor)(record);
            Ok(())
        }

        fn on_update(&mut self, _key: String, old: &User, new: &User) -> Result<()> {
            self.total -= (self.field_extractor)(old);
            self.total += (self.field_extractor)(new);
            Ok(())
        }

        fn on_delete(&mut self, _key: String, record: &User) {
            self.total -= (self.field_extractor)(record);
        }
    }

    #[test]
    fn define_custom_index_and_query() {
        let mut table = Table::<User>::new();
        table
            .define_custom_index("age_sum", SumIndex::new(|u| u.age as u64))
            .unwrap();

        table
            .insert(User {
                email: "a@x.com".to_string(),
                age: 30,
                name: "Alice".to_string(),
            })
            .unwrap();
        table
            .insert(User {
                email: "b@x.com".to_string(),
                age: 20,
                name: "Bob".to_string(),
            })
            .unwrap();

        let idx = table.custom_index::<SumIndex>("age_sum").unwrap();
        assert_eq!(idx.total(), 50);
    }

    #[test]
    fn define_custom_index_backfills_existing_data() {
        let mut table = Table::<User>::new();
        table
            .insert(User {
                email: "a@x.com".to_string(),
                age: 10,
                name: "A".to_string(),
            })
            .unwrap();
        table
            .insert(User {
                email: "b@x.com".to_string(),
                age: 20,
                name: "B".to_string(),
            })
            .unwrap();

        table
            .define_custom_index("age_sum", SumIndex::new(|u| u.age as u64))
            .unwrap();

        let idx = table.custom_index::<SumIndex>("age_sum").unwrap();
        assert_eq!(idx.total(), 30);
    }

    #[test]
    fn define_custom_index_rejects_duplicate_name() {
        let mut table = Table::<User>::new();
        table
            .define_custom_index("idx", SumIndex::new(|u| u.age as u64))
            .unwrap();
        let res = table.define_custom_index("idx", SumIndex::new(|u| u.age as u64));
        assert!(matches!(res, Err(Error::IndexAlreadyExists(_))));
    }

    #[test]
    fn define_custom_index_rejects_name_collision_with_builtin() {
        let mut table = Table::<User>::new();
        table
            .define_index("idx", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        let res = table.define_custom_index("idx", SumIndex::new(|u| u.age as u64));
        assert!(matches!(res, Err(Error::IndexAlreadyExists(_))));
    }

    #[test]
    fn custom_index_not_found() {
        let table = Table::<User>::new();
        let res = table.custom_index::<SumIndex>("nope");
        assert!(matches!(res, Err(Error::IndexNotFound(_))));
    }

    #[test]
    fn custom_index_type_mismatch() {
        let mut table = Table::<User>::new();
        table
            .define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
            .unwrap();
        let res = table.custom_index::<SumIndex>("by_email");
        assert!(matches!(res, Err(Error::IndexTypeMismatch(_))));
    }

    #[test]
    fn custom_index_on_a_string_keyed_table() {
        // `define_custom_index`/`custom_index` used to be pinned to
        // `Table<R, u64>` because `CustomIndex` hard-coded `id: u64`. Now
        // that `CustomIndex<R, K = u64>` carries the row-key type, a
        // `new_keyed` (non-`AutoKey`) table can define one too.
        let mut table: Table<User, String> = Table::new_keyed();
        table
            .define_custom_index("age_sum", SumIndex::new(|u| u.age as u64))
            .unwrap();

        table
            .put(
                "alice".to_string(),
                User {
                    email: "a@x.com".to_string(),
                    age: 30,
                    name: "Alice".to_string(),
                },
            )
            .unwrap();
        table
            .put(
                "bob".to_string(),
                User {
                    email: "b@x.com".to_string(),
                    age: 20,
                    name: "Bob".to_string(),
                },
            )
            .unwrap();

        let idx = table.custom_index::<SumIndex>("age_sum").unwrap();
        assert_eq!(idx.total(), 50);

        table.delete(&"bob".to_string()).unwrap();
        let idx = table.custom_index::<SumIndex>("age_sum").unwrap();
        assert_eq!(idx.total(), 30);
    }

    #[test]
    fn resolve_returns_matching_records() {
        let mut table = Table::<User>::new();
        let id1 = table
            .insert(User {
                email: "a@x.com".to_string(),
                age: 30,
                name: "A".to_string(),
            })
            .unwrap();
        let id2 = table
            .insert(User {
                email: "b@x.com".to_string(),
                age: 20,
                name: "B".to_string(),
            })
            .unwrap();

        let results = table.resolve(&[id1, id2, 999]);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, id1);
        assert_eq!(results[1].0, id2);
    }

    #[test]
    fn resolve_empty_ids() {
        let table = Table::<User>::new();
        let results = table.resolve(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn custom_index_tracks_updates() {
        let mut table = Table::<User>::new();
        table
            .define_custom_index("age_sum", SumIndex::new(|u| u.age as u64))
            .unwrap();

        let id = table
            .insert(User {
                email: "a@x.com".to_string(),
                age: 30,
                name: "A".to_string(),
            })
            .unwrap();
        assert_eq!(
            table.custom_index::<SumIndex>("age_sum").unwrap().total(),
            30
        );

        table
            .update(
                &id,
                User {
                    email: "a@x.com".to_string(),
                    age: 40,
                    name: "A".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            table.custom_index::<SumIndex>("age_sum").unwrap().total(),
            40
        );

        table.delete(&id).unwrap();
        assert_eq!(
            table.custom_index::<SumIndex>("age_sum").unwrap().total(),
            0
        );
    }

    #[test]
    fn custom_index_works_with_batch_insert() {
        let mut table = Table::<User>::new();
        table
            .define_custom_index("age_sum", SumIndex::new(|u| u.age as u64))
            .unwrap();

        let records = vec![
            User {
                email: "a@x.com".to_string(),
                age: 10,
                name: "A".to_string(),
            },
            User {
                email: "b@x.com".to_string(),
                age: 20,
                name: "B".to_string(),
            },
            User {
                email: "c@x.com".to_string(),
                age: 30,
                name: "C".to_string(),
            },
        ];
        table.insert_batch(records).unwrap();

        assert_eq!(
            table.custom_index::<SumIndex>("age_sum").unwrap().total(),
            60
        );
    }

    #[test]
    fn custom_index_works_with_batch_delete() {
        let mut table = Table::<User>::new();
        table
            .define_custom_index("age_sum", SumIndex::new(|u| u.age as u64))
            .unwrap();

        let ids = table
            .insert_batch(vec![
                User {
                    email: "a@x.com".to_string(),
                    age: 10,
                    name: "A".to_string(),
                },
                User {
                    email: "b@x.com".to_string(),
                    age: 20,
                    name: "B".to_string(),
                },
                User {
                    email: "c@x.com".to_string(),
                    age: 30,
                    name: "C".to_string(),
                },
            ])
            .unwrap();

        table.delete_batch(&[ids[0], ids[2]]).unwrap();
        assert_eq!(
            table.custom_index::<SumIndex>("age_sum").unwrap().total(),
            20
        );
    }

    #[test]
    fn custom_index_clone_is_independent() {
        let mut table = Table::<User>::new();
        table
            .define_custom_index("age_sum", SumIndex::new(|u| u.age as u64))
            .unwrap();
        table
            .insert(User {
                email: "a@x.com".to_string(),
                age: 30,
                name: "A".to_string(),
            })
            .unwrap();

        let clone = table.clone();

        table
            .insert(User {
                email: "b@x.com".to_string(),
                age: 20,
                name: "B".to_string(),
            })
            .unwrap();

        assert_eq!(
            clone.custom_index::<SumIndex>("age_sum").unwrap().total(),
            30
        );
        assert_eq!(
            table.custom_index::<SumIndex>("age_sum").unwrap().total(),
            50
        );
    }

    /// A custom index that rejects inserts when total would exceed a limit.
    #[derive(Clone)]
    struct CappedSumIndex {
        total: u64,
        cap: u64,
    }

    impl CappedSumIndex {
        fn new(cap: u64) -> Self {
            Self { total: 0, cap }
        }
    }

    impl CustomIndex<User> for CappedSumIndex {
        fn on_insert(&mut self, _id: u64, record: &User) -> Result<()> {
            let new_total = self.total + record.age as u64;
            if new_total > self.cap {
                return Err(Error::DuplicateKey("cap exceeded".to_string()));
            }
            self.total = new_total;
            Ok(())
        }

        fn on_update(&mut self, _id: u64, old: &User, new: &User) -> Result<()> {
            let new_total = self.total - old.age as u64 + new.age as u64;
            if new_total > self.cap {
                return Err(Error::DuplicateKey("cap exceeded".to_string()));
            }
            self.total = new_total;
            Ok(())
        }

        fn on_delete(&mut self, _id: u64, record: &User) {
            self.total -= record.age as u64;
        }
    }

    #[test]
    fn custom_index_veto_rejects_insert() {
        let mut table = Table::<User>::new();
        table
            .define_custom_index("capped", CappedSumIndex::new(50))
            .unwrap();

        table
            .insert(User {
                email: "a@x.com".to_string(),
                age: 30,
                name: "A".to_string(),
            })
            .unwrap();
        table
            .insert(User {
                email: "b@x.com".to_string(),
                age: 15,
                name: "B".to_string(),
            })
            .unwrap();

        // This would push total to 55, exceeding cap of 50
        let res = table.insert(User {
            email: "c@x.com".to_string(),
            age: 10,
            name: "C".to_string(),
        });
        assert!(res.is_err());

        // Total should still be 45 (rollback)
        assert_eq!(
            table
                .custom_index::<CappedSumIndex>("capped")
                .unwrap()
                .total,
            45
        );
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn custom_index_veto_rejects_update() {
        let mut table = Table::<User>::new();
        table
            .define_custom_index("capped", CappedSumIndex::new(50))
            .unwrap();

        let id = table
            .insert(User {
                email: "a@x.com".to_string(),
                age: 30,
                name: "A".to_string(),
            })
            .unwrap();
        table
            .insert(User {
                email: "b@x.com".to_string(),
                age: 15,
                name: "B".to_string(),
            })
            .unwrap();

        // Update age 30 -> 40 would push total to 55
        let res = table.update(
            &id,
            User {
                email: "a@x.com".to_string(),
                age: 40,
                name: "A".to_string(),
            },
        );
        assert!(res.is_err());

        // Total should still be 45
        assert_eq!(
            table
                .custom_index::<CappedSumIndex>("capped")
                .unwrap()
                .total,
            45
        );
    }

    #[test]
    fn from_bulk_data_only_no_indexes() {
        let rows: Vec<(u64, std::sync::Arc<String>)> = (1u64..=5)
            .map(|i| (i, std::sync::Arc::new(format!("v{i}"))))
            .collect();
        let t = Table::<String>::from_bulk(rows, Some(6), vec![]).unwrap();
        assert_eq!(t.len(), 5);
        assert_eq!(t.get(&1).map(String::as_str), Some("v1"));
        assert_eq!(t.get(&5).map(String::as_str), Some("v5"));
        // Inserting after bulk should continue from next_id.
        let mut t2 = t;
        let id = t2.insert("v6".to_string()).unwrap();
        assert_eq!(id, 6);
    }

    #[test]
    fn from_bulk_with_indexes() {
        use std::sync::Arc;
        #[derive(Clone)]
        #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
        struct U {
            email: String,
            age: u32,
        }

        // Build empty index defs to hand to from_bulk.
        let unique_idx: Box<dyn IndexMaintainer<U, u64>> =
            Box::new(ManagedIndex::<U, String, UniqueStorage<String, u64>>::new(
                "by_email".into(),
                IndexKind::Unique,
                Arc::new(|u: &U| u.email.clone()),
                UniqueStorage::new(),
            ));
        let nonunique_idx: Box<dyn IndexMaintainer<U, u64>> =
            Box::new(ManagedIndex::<U, u32, NonUniqueStorage<u32, u64>>::new(
                "by_age".into(),
                IndexKind::NonUnique,
                Arc::new(|u: &U| u.age),
                NonUniqueStorage::new(),
            ));

        let rows: Vec<(u64, Arc<U>)> = (1u64..=5)
            .map(|i| {
                (
                    i,
                    Arc::new(U {
                        email: format!("u{i}@x"),
                        age: 10 * (i as u32 % 3),
                    }),
                )
            })
            .collect();

        let t = Table::<U>::from_bulk(rows, Some(6), vec![unique_idx, nonunique_idx]).unwrap();
        assert_eq!(t.len(), 5);
        let (id, _) = t
            .get_unique("by_email", &"u3@x".to_string())
            .unwrap()
            .unwrap();
        assert_eq!(id, 3);
        let ids: Vec<u64> = t
            .get_by_index("by_age", &10u32)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(
            ids.len()
                + t.get_by_index("by_age", &20u32).unwrap().len()
                + t.get_by_index("by_age", &0u32).unwrap().len(),
            5
        );
    }

    #[test]
    fn from_bulk_unique_collision_errors() {
        use std::sync::Arc;
        #[derive(Clone)]
        #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
        struct U {
            email: String,
        }

        let unique_idx: Box<dyn IndexMaintainer<U, u64>> =
            Box::new(ManagedIndex::<U, String, UniqueStorage<String, u64>>::new(
                "by_email".into(),
                IndexKind::Unique,
                Arc::new(|u: &U| u.email.clone()),
                UniqueStorage::new(),
            ));
        let rows: Vec<(u64, Arc<U>)> = vec![
            (
                1,
                Arc::new(U {
                    email: "dup@x".into(),
                }),
            ),
            (
                2,
                Arc::new(U {
                    email: "dup@x".into(),
                }),
            ),
        ];
        let res = Table::<U>::from_bulk(rows, Some(3), vec![unique_idx]);
        assert!(matches!(res, Err(Error::DuplicateKey(_))));
    }

    #[test]
    fn custom_index_on_update_runs_when_record_changes() {
        // Exercises CustomIndex::on_update — increments-then-decrements
        // through the index whenever a record's age changes.
        let mut table = Table::<User>::new();
        table
            .define_custom_index("capped", CappedSumIndex::new(100))
            .unwrap();
        let id = table
            .insert(User {
                email: "a@x".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        assert_eq!(
            table
                .custom_index::<CappedSumIndex>("capped")
                .unwrap()
                .total,
            30
        );
        table
            .update(
                &id,
                User {
                    email: "a@x".into(),
                    age: 50,
                    name: "A".into(),
                },
            )
            .unwrap();
        assert_eq!(
            table
                .custom_index::<CappedSumIndex>("capped")
                .unwrap()
                .total,
            50
        );
    }

    #[test]
    fn custom_index_on_delete_decrements_total() {
        // Exercises CustomIndex::on_delete — pulls the deleted record's
        // contribution back out of the running total.
        let mut table = Table::<User>::new();
        table
            .define_custom_index("capped", CappedSumIndex::new(100))
            .unwrap();
        let id = table
            .insert(User {
                email: "a@x".into(),
                age: 30,
                name: "A".into(),
            })
            .unwrap();
        table.delete(&id).unwrap();
        assert_eq!(
            table
                .custom_index::<CappedSumIndex>("capped")
                .unwrap()
                .total,
            0
        );
    }

    // -----------------------------------------------------------------------
    // Merged reads through the write overlay (task58 T2)
    // -----------------------------------------------------------------------

    #[test]
    fn merged_get_prefers_overlay_and_respects_tombstones() {
        let mut t: Table<String> = Table::new();
        let id = t.insert("tree".to_string()).unwrap();
        let ov = t.overlay_mut_for_test(8);
        ov.set_put(id, std::sync::Arc::new("overlay".to_string()), true);
        assert_eq!(t.get(&id).map(String::as_str), Some("overlay"));
        t.overlay_mut_for_test(8).set_tombstone(id);
        assert_eq!(t.get(&id), None);
        assert!(!t.contains(&id));
    }

    #[test]
    fn merged_len_and_iter_and_range_merge_both_sources() {
        let mut t: Table<String> = Table::new();
        let a = t.insert("a".to_string()).unwrap(); // key 1
        let b = t.insert("b".to_string()).unwrap(); // key 2
        let _c = t.insert("c".to_string()).unwrap(); // key 3
        {
            let ov = t.overlay_mut_for_test(8);
            ov.set_put(b, std::sync::Arc::new("b2".to_string()), true); // update via overlay
            ov.set_tombstone(a); // delete via overlay
            ov.set_put(10, std::sync::Arc::new("j".to_string()), false); // new row
        }
        assert_eq!(t.len(), 3); // 3 - 1 + 1
        let got: Vec<(u64, String)> = t.iter().map(|(k, v)| (*k, v.clone())).collect();
        assert_eq!(got, vec![(2, "b2".into()), (3, "c".into()), (10, "j".into())]);
        let ranged: Vec<u64> = t.range(2..=3).map(|(k, _)| *k).collect();
        assert_eq!(ranged, vec![2, 3]);
        assert_eq!(t.first().map(|(k, _)| *k), Some(2));
        assert_eq!(t.last().map(|(k, _)| *k), Some(10));
    }

    /// An empty or inverted range must stay empty, not panic.
    ///
    /// The overlay narrows `entries()` with two independent `partition_point`
    /// calls; for `5..2` the start bound lands above the end bound and the
    /// resulting `e[lo..hi]` is an out-of-order slice index — which panics in
    /// **release** as well, since that is not a `debug_assert`. `BTree::range`
    /// is total on these bounds, so before the overlay every one of these
    /// returned an empty iterator, and a committed snapshot normally carries a
    /// live overlay, so this is the ordinary read path.
    #[test]
    fn inverted_and_empty_ranges_stay_empty_with_a_live_overlay() {
        let mut t: Table<String> = Table::new();
        for i in 0..8 {
            t.insert(format!("v{i}")).unwrap();
        }
        {
            let ov = t.overlay_mut_for_test(8);
            ov.set_put(3, std::sync::Arc::new("three".to_string()), true);
            ov.set_put(20, std::sync::Arc::new("twenty".to_string()), false);
        }
        assert_eq!(t.overlay_len_for_test(), 2, "the overlay must be live for this to bite");

        // Bounds come through variables: these ranges are inverted, which is
        // exactly what a caller can pass at runtime, but written as literals
        // clippy's `reversed_empty_ranges` rejects them at compile time.
        // Only pairs that straddle an overlay key produce `lo > hi` and can
        // actually panic: with the overlay at {3, 20}, `(5,2)` gives lo=1,hi=0
        // and `(25,5)` gives lo=2,hi=1. The rest yield `lo == hi` and are here
        // to pin that ordinary empty ranges stay empty.
        for (lo, hi) in [(5u64, 2u64), (25, 5), (5, 5), (99, 30), (2, 1)] {
            let got: Vec<u64> = t.range(lo..hi).map(|(k, _)| *k).collect();
            assert!(got.is_empty(), "range({lo}..{hi}) must be empty, not panic");
            let got_incl: Vec<u64> = t.range(lo..=hi).map(|(k, _)| *k).collect();
            assert!(
                got_incl.iter().all(|k| *k >= lo && *k <= hi),
                "range({lo}..={hi}) leaked a key outside its bounds: {got_incl:?}"
            );
        }
        // A valid range still works after the clamp.
        let valid: Vec<u64> = t.range(2u64..=4u64).map(|(k, _)| *k).collect();
        assert_eq!(valid, vec![2, 3, 4]);
    }

    #[test]
    fn merged_last_falls_back_past_a_tombstoned_tree_max() {
        // Overlay tail is a tombstone shadowing the tree's max key; the true
        // last is an earlier, untouched tree key.
        let mut t: Table<String> = Table::new();
        let _a = t.insert("a".to_string()).unwrap(); // key 1
        let b = t.insert("b".to_string()).unwrap(); // key 2
        let c = t.insert("c".to_string()).unwrap(); // key 3 (tree max)
        t.overlay_mut_for_test(8).set_tombstone(c);
        assert_eq!(t.last(), Some((&b, &"b".to_string())));
    }

    #[test]
    fn merged_last_prefers_an_overlay_put_beyond_the_tree_max() {
        // Overlay tail is a Put for a key beyond anything in the tree.
        let mut t: Table<String> = Table::new();
        let _a = t.insert("a".to_string()).unwrap(); // key 1
        let _b = t.insert("b".to_string()).unwrap(); // key 2
        let ov = t.overlay_mut_for_test(8);
        ov.set_put(99, std::sync::Arc::new("z".to_string()), false);
        assert_eq!(t.last(), Some((&99, &"z".to_string())));
    }

    #[test]
    fn merged_last_walks_back_through_a_run_of_tombstones() {
        // Overlay tombstones kill every tree-resident key above 2, so the
        // backward walk must skip several shadowed entries before landing
        // on the first survivor.
        let mut t: Table<String> = Table::new();
        let _a = t.insert("a".to_string()).unwrap(); // key 1
        let b = t.insert("b".to_string()).unwrap(); // key 2 — survives
        let c = t.insert("c".to_string()).unwrap(); // key 3
        let d = t.insert("d".to_string()).unwrap(); // key 4
        let e = t.insert("e".to_string()).unwrap(); // key 5 (tree max)
        {
            let ov = t.overlay_mut_for_test(8);
            ov.set_tombstone(e);
            ov.set_tombstone(d);
            ov.set_tombstone(c);
        }
        assert_eq!(t.last(), Some((&b, &"b".to_string())));
    }

    // -----------------------------------------------------------------------
    // Write-path overlay routing (task58 T3)
    // -----------------------------------------------------------------------

    #[test]
    fn writes_land_in_overlay_and_flush_at_cap() {
        let mut t: Table<String> = Table::new();
        t.overlay_mut_for_test(4); // enable with tiny cap
        let mut ids = Vec::new();
        for i in 0..4 {
            ids.push(t.insert(format!("v{i}")).unwrap());
        }
        assert_eq!(t.overlay_len_for_test(), 4, "all four writes buffered");
        let id5 = t.insert("v4".to_string()).unwrap(); // fifth write: flush, then buffer
        assert_eq!(t.overlay_len_for_test(), 1);
        assert_eq!(t.len(), 5);
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(t.get(id).map(String::as_str), Some(format!("v{i}").as_str()));
        }
        assert_eq!(t.get(&id5).map(String::as_str), Some("v4"));
    }

    /// A cap change must never drop buffered entries, even when they'd
    /// still fit under the new cap — `set_overlay_cap`'s contract is flush
    /// on any change, preserve only on an unchanged cap (task58 T5 review
    /// finding: the original guard only flushed when shrinking below the
    /// current entry count, silently dropping entries on e.g. 128 -> 64
    /// with 10 buffered).
    #[test]
    fn set_overlay_cap_change_flushes_instead_of_dropping() {
        let mut t: Table<String> = Table::new();
        t.overlay_mut_for_test(8);
        let a = t.insert("a".to_string()).unwrap();
        let b = t.insert("b".to_string()).unwrap();
        assert_eq!(t.overlay_len_probe(), 2, "buffered, not yet flushed");

        t.set_overlay_cap(4); // cap change; both entries still fit under 4
        assert_eq!(t.overlay_len_probe(), 0, "cap change must flush, not drop");
        assert_eq!(t.len(), 2);
        assert_eq!(t.get(&a).map(String::as_str), Some("a"));
        assert_eq!(t.get(&b).map(String::as_str), Some("b"));

        let c = t.insert("c".to_string()).unwrap();
        assert_eq!(t.overlay_len_probe(), 1, "buffered under the new cap-4 overlay");
        t.set_overlay_cap(4); // same cap: must preserve, not flush
        assert_eq!(
            t.overlay_len_probe(),
            1,
            "same-cap re-open preserves buffered entries"
        );
        assert_eq!(t.get(&c).map(String::as_str), Some("c"));
    }

    #[test]
    fn overlay_delete_semantics_match_the_spec() {
        let mut t: Table<String> = Table::new();
        let resident = t.insert("old".to_string()).unwrap();
        t.overlay_mut_for_test(8);
        let born = t.insert("young".to_string()).unwrap(); // overlay-born
        // overlay-born delete: entry removed, no tombstone
        let removed = t.delete(&born).unwrap();
        assert_eq!(removed.as_str(), "young");
        assert_eq!(t.overlay_len_for_test(), 0);
        // tree-resident delete through the overlay: tombstone
        let removed = t.delete(&resident).unwrap();
        assert_eq!(removed.as_str(), "old");
        assert_eq!(t.overlay_len_for_test(), 1);
        assert_eq!(t.len(), 0);
        assert!(matches!(t.delete(&resident), Err(Error::KeyNotFound)));
        assert!(matches!(t.update(&resident, "x".into()), Err(Error::KeyNotFound)));
    }

    #[test]
    fn batch_ops_flush_first_and_stay_correct() {
        let mut t: Table<String> = Table::new();
        t.overlay_mut_for_test(8);
        let id = t.insert("solo".to_string()).unwrap();
        let ids = t.insert_batch((0..10).map(|i| format!("b{i}")).collect()).unwrap();
        assert_eq!(t.overlay_len_for_test(), 0, "insert_batch flushed the overlay");
        assert_eq!(t.len(), 11);
        assert_eq!(t.get(&id).map(String::as_str), Some("solo"));
        assert_eq!(ids.len(), 10);
    }

    #[test]
    fn rollback_restores_the_overlay() {
        let mut t: Table<String> = Table::new();
        t.overlay_mut_for_test(8);
        let id = t.insert("keep".to_string()).unwrap();
        // update_batch with a failing key rolls back wholesale
        let res = t.update_batch(vec![(id, "changed".into()), (9999, "nope".into())]);
        assert!(res.is_err());
        assert_eq!(t.get(&id).map(String::as_str), Some("keep"));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn put_lands_in_overlay_and_advances_the_counter() {
        let mut t: Table<String> = Table::new();
        t.overlay_mut_for_test(8);
        t.put(5, "five".to_string()).unwrap();
        assert_eq!(t.overlay_len_for_test(), 1, "put buffered, not flushed to the tree");
        assert_eq!(t.get(&5).map(String::as_str), Some("five"));
        // The auto-increment counter must advance past an explicitly
        // written key even though the row only landed in the overlay.
        let id = t.insert("six".to_string()).unwrap();
        assert_eq!(id, 6);
        assert_eq!(t.len(), 2);
    }

    /// Drive an overlay table and a plain table with the same op sequence;
    /// they must be observationally identical. Deterministic seeds; caps 1,
    /// 2, 3, 8 force flushes at every boundary alignment.
    #[test]
    fn overlay_table_is_observationally_identical_to_plain_table() {
        use rand::RngExt;
        use rand::SeedableRng;
        use rand::rngs::StdRng;

        for seed in 0..8u64 {
            for cap in [1usize, 2, 3, 8] {
                let mut rng = StdRng::seed_from_u64(seed);
                let mut plain: Table<String> = Table::new();
                let mut with_ov: Table<String> = Table::new();
                with_ov.overlay_mut_for_test(cap);
                let mut live_keys: Vec<u64> = Vec::new();
                for step in 0..400 {
                    match rng.random_range(0..7) {
                        0 | 1 => {
                            let v = format!("v{step}");
                            let a = plain.insert(v.clone()).unwrap();
                            let b = with_ov.insert(v).unwrap();
                            assert_eq!(a, b, "auto ids must stay in lockstep");
                            live_keys.push(a);
                        }
                        2 if !live_keys.is_empty() => {
                            let k = live_keys[rng.random_range(0..live_keys.len())];
                            let v = format!("u{step}");
                            assert_eq!(
                                plain.update(&k, v.clone()).is_ok(),
                                with_ov.update(&k, v).is_ok()
                            );
                        }
                        3 if !live_keys.is_empty() => {
                            let i = rng.random_range(0..live_keys.len());
                            let k = live_keys.swap_remove(i);
                            let a = plain.delete(&k);
                            let b = with_ov.delete(&k);
                            assert_eq!(a.is_ok(), b.is_ok());
                            if let (Ok(x), Ok(y)) = (a, b) {
                                assert_eq!(x, y);
                            }
                        }
                        4 => {
                            let k = rng.random_range(0..(live_keys.len() as u64 + 4));
                            assert_eq!(plain.get(&k), with_ov.get(&k), "seed {seed} cap {cap} step {step}");
                        }
                        5 => {
                            // Explicit-key `put` — the path the store's
                            // `TableWriter::put`/`upsert` drives. Keys
                            // deliberately overlap the auto-increment range
                            // so put-over-existing, put-over-tombstone and
                            // counter advancement all get exercised.
                            let k = rng.random_range(0..20u64);
                            let v = format!("p{step}");
                            plain.put(k, v.clone()).unwrap();
                            with_ov.put(k, v).unwrap();
                            assert_eq!(
                                plain.next_id(),
                                with_ov.next_id(),
                                "put must advance both counters alike"
                            );
                            if !live_keys.contains(&k) {
                                live_keys.push(k);
                            }
                        }
                        _ => {
                            let lo = rng.random_range(0..12u64);
                            let hi = lo + rng.random_range(0..12u64);
                            let a: Vec<(u64, String)> =
                                plain.range(lo..hi).map(|(k, v)| (*k, v.clone())).collect();
                            let b: Vec<(u64, String)> =
                                with_ov.range(lo..hi).map(|(k, v)| (*k, v.clone())).collect();
                            assert_eq!(a, b, "seed {seed} cap {cap} step {step}");
                        }
                    }
                    assert_eq!(plain.len(), with_ov.len());
                }
                // Full-iteration equivalence at the end, overlay still nonempty.
                let a: Vec<(u64, String)> = plain.iter().map(|(k, v)| (*k, v.clone())).collect();
                let b: Vec<(u64, String)> = with_ov.iter().map(|(k, v)| (*k, v.clone())).collect();
                assert_eq!(a, b);
            }
        }
    }
}
