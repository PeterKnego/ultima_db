// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::any::Any;
use std::sync::Arc;
#[cfg(feature = "persistence")]
use std::sync::atomic::Ordering;

use crate::btree::BTree;
#[cfg(feature = "persistence")]
use crate::btree::BTreeNode;
#[cfg(feature = "persistence")]
use crate::checkpoint::PagedIndexEntry;
#[cfg(feature = "persistence")]
use crate::child::{NO_PAGE, PageId};
#[cfg(feature = "persistence")]
use crate::pagecodec::{NodeCodec, PagedSource, PagedStats};
#[cfg(feature = "persistence")]
use crate::pagefile::{PageFile, PageKind};
use crate::persistence::Record;
use crate::primary_key::PrimaryKey;
#[cfg(feature = "persistence")]
use crate::table::PagedCtx;
use crate::{Error, Result};

/// Config for [`crate::table::Table::define_persisted_index`] — currently
/// just the generation number stamped into the index's [`PagedIndexEntry`],
/// bumped whenever the index's shape (extractor/key type) changes in a way
/// that would make an on-disk tree from an older generation unsafe to reuse.
/// `#[non_exhaustive]` so a later field can be added without breaking
/// source compatibility; build one with [`IndexDef::new`]. (A plain struct
/// literal — with or without `..Default::default()` — only compiles from
/// *inside* this crate: `#[non_exhaustive]` blocks struct-literal
/// construction entirely for external callers, functional-update syntax
/// included.)
#[cfg(feature = "persistence")]
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default)]
pub struct IndexDef {
    /// The index's shape generation — see the struct doc.
    pub generation: u32,
}

#[cfg(feature = "persistence")]
impl IndexDef {
    /// Builds an `IndexDef` with the given generation — the constructor
    /// external callers need, since `#[non_exhaustive]` blocks direct
    /// struct-literal construction from outside this crate (see the
    /// struct doc).
    pub fn new(generation: u32) -> Self {
        Self { generation }
    }
}

/// Whether an index enforces uniqueness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    /// Rejects a second record with the same key; backed by `BTree<IK, K>`.
    Unique,
    /// Allows multiple records to share a key; backed by
    /// `BTree<(IK, K), ()>` (row key folded into the key for multi-value storage).
    NonUnique,
    /// User-supplied index implementing [`CustomIndex`] (e.g. full-text);
    /// storage shape is opaque to the generic maintainer.
    Custom,
}

// ---------------------------------------------------------------------------
// IndexMaintainer trait — object-safe interface for type-erased indexes
// ---------------------------------------------------------------------------

pub(crate) trait IndexMaintainer<R, K: PrimaryKey>: Send + Sync {
    /// Called when a record is inserted into the table.
    fn on_insert(&mut self, key: K, record: &R) -> Result<()>;
    /// Called when a record in the table is updated.
    fn on_update(&mut self, key: K, old: &R, new: &R) -> Result<()>;
    /// Called when a record is deleted from the table.
    fn on_delete(&mut self, key: K, record: &R);
    /// Returns the kind of this index (Unique or NonUnique).
    fn kind(&self) -> IndexKind;
    /// Returns the name of this index.
    fn name(&self) -> &str;
    /// Returns a boxed clone of this index maintainer.
    fn clone_box(&self) -> Box<dyn IndexMaintainer<R, K>>;
    /// Clone the index *definition* (extractor, name, kind, storage shape)
    /// with empty storage. Used by bulk-load to rebuild the index against
    /// freshly-loaded data via [`rebuild_from_sorted_data`]. The returned
    /// box must be ready to receive a full backfill. Custom indexes have no
    /// generic "make empty" hook and return `Err` — bulk loads over them
    /// are rejected rather than silently dropping the index.
    fn empty_clone(&self) -> Result<Box<dyn IndexMaintainer<R, K>>>;
    /// Returns a reference to the underlying index as `Any`.
    fn as_any(&self) -> &dyn Any;

    /// Rebuild the index from a fully-built data tree using a sorted
    /// bottom-up build. Default impl falls back to per-row `on_insert`
    /// over an in-order walk; concrete implementations override for speed.
    fn rebuild_from_sorted_data(&mut self, data: &BTree<K, R>) -> Result<()> {
        for (key, record) in data.range(..) {
            self.on_insert(key.clone(), record)?;
        }
        Ok(())
    }

    /// Attach this index's storage to a page file for paged checkpoint
    /// write/read. A no-op unless the storage already carries a `persist`
    /// marker from `Table::define_persisted_index` — a plain
    /// (`Table::define_index`) index stays purely in-memory even once its
    /// table is paged.
    // No production caller yet — `Table::attach_paged_source` (Task 8+
    // wires it into `Store`). Used today by `table.rs`'s `paged` test
    // module.
    #[cfg(feature = "persistence")]
    #[allow(dead_code)]
    fn attach_paged_source(&mut self, file: Arc<PageFile>, stats: Arc<PagedStats>, table_name: &str);

    /// Write this index's dirty pages, if it is persisted. `None` when this
    /// index carries no `persist` marker.
    // No production caller yet — see `attach_paged_source` above.
    #[cfg(feature = "persistence")]
    #[allow(dead_code)]
    fn paged_write(&self, ctx: &PagedCtx) -> Result<Option<PagedIndexEntry>>;

    /// Page ids referenced by `prev`'s tree and not by `self`'s. `&[]` for
    /// an index that isn't persisted, or when `prev`'s concrete shape
    /// doesn't match `self`'s.
    // No production caller yet — see `attach_paged_source` above.
    #[cfg(feature = "persistence")]
    #[allow(dead_code)]
    fn paged_changed_pages(&self, prev: &dyn IndexMaintainer<R, K>) -> Vec<PageId>;

    /// The generation recorded by `Table::define_persisted_index`, or `0`
    /// for an index with no `persist` marker.
    // No production caller yet — see `attach_paged_source` above.
    #[cfg(feature = "persistence")]
    #[allow(dead_code)]
    fn paged_generation(&self) -> u32;

    /// This index's currently reachable page ids, read-only via
    /// [`crate::btree::BTree::for_each_page_id`] after forcing the tree
    /// fully resident with [`crate::btree::BTree::load_all`] first — see
    /// that call's note on the impl for why this is structural, not just
    /// an assumed invariant. `&[]` for a non-persisted index (no
    /// page-file presence at all) or an empty persisted one.
    ///
    /// Used by `Table::paged_changed_pages`'s still-pending persisted-index
    /// carry-forward path (Task 13) to learn the "new" side of a pending
    /// index that got attached, regenerated, or dropped this checkpoint:
    /// `prev`'s side of that diff has no live `IndexMaintainer` at all (it
    /// is only a [`crate::checkpoint::PagedIndexEntry`] in
    /// `pending_indexes`), so the diff walks `prev`'s pages with the
    /// type-erased [`crate::pagecodec::raw_reachable_page_ids`] instead —
    /// this method is only ever the "new" half of that comparison.
    #[cfg(feature = "persistence")]
    fn paged_reachable_ids(&self) -> Vec<PageId>;
}

/// Extracts an index key of type `IK` from a record of type `R`. Implemented
/// generically for any `Fn(&R) -> IK + Send + Sync`, so a plain closure
/// passed to `define_index` satisfies this trait.
pub trait KeyExtractor<R, IK>: Send + Sync {
    /// Computes the index key for `record`.
    fn extract(&self, record: &R) -> IK;
}

impl<R, IK, F> KeyExtractor<R, IK> for F
where
    F: Fn(&R) -> IK + Send + Sync,
{
    fn extract(&self, record: &R) -> IK {
        self(record)
    }
}

pub(crate) trait IndexStorage<IK, K>: Send + Sync {
    fn insert(&mut self, key: IK, row_key: K, name: &str) -> Result<()>;
    fn delete(&mut self, key: IK, row_key: K);
}

pub(crate) struct ManagedIndex<R, IK, S> {
    extractor: Arc<dyn KeyExtractor<R, IK>>,
    storage: S,
    name: String,
    kind: IndexKind,
}

impl<R, IK, S> ManagedIndex<R, IK, S>
where
    IK: Ord + Clone + 'static,
    S: 'static,
    R: 'static,
{
    /// Creates a managed index of the given `kind`, pairing `extractor`
    /// (computes the index key for each record) with its backing `storage`.
    /// `name` is reported in errors (e.g. duplicate keys on a unique index).
    pub fn new(
        name: String,
        kind: IndexKind,
        extractor: Arc<dyn KeyExtractor<R, IK>>,
        storage: S,
    ) -> Self {
        Self {
            name,
            kind,
            extractor,
            storage,
        }
    }

    /// Read access to the backing storage. The `Table` query paths
    /// (`get_unique`, `get_by_index`, `index_range`) downcast the type-erased
    /// maintainer to a concrete `ManagedIndex` and query through this.
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

impl<R, IK, K> IndexMaintainer<R, K> for ManagedIndex<R, IK, UniqueStorage<IK, K>>
where
    IK: Ord + Clone + Send + Sync + 'static,
    K: PrimaryKey,
    R: Record,
{
    fn on_insert(&mut self, key: K, record: &R) -> Result<()> {
        let idx_key = self.extractor.extract(record);
        self.storage.insert(idx_key, key, &self.name)
    }

    fn on_update(&mut self, key: K, old: &R, new: &R) -> Result<()> {
        let old_key = self.extractor.extract(old);
        let new_key = self.extractor.extract(new);
        if old_key != new_key {
            // Insert new key first — if it fails (e.g. unique constraint),
            // old_key is still intact and no rollback is needed.
            self.storage.insert(new_key, key.clone(), &self.name)?;
            self.storage.delete(old_key, key);
        }
        Ok(())
    }

    fn on_delete(&mut self, key: K, record: &R) {
        let idx_key = self.extractor.extract(record);
        self.storage.delete(idx_key, key);
    }

    fn kind(&self) -> IndexKind {
        self.kind
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn clone_box(&self) -> Box<dyn IndexMaintainer<R, K>> {
        Box::new(Self {
            extractor: Arc::clone(&self.extractor),
            storage: self.storage.clone(),
            name: self.name.clone(),
            kind: self.kind,
        })
    }

    fn empty_clone(&self) -> Result<Box<dyn IndexMaintainer<R, K>>> {
        Ok(Box::new(Self {
            extractor: Arc::clone(&self.extractor),
            // `empty_like`, not `UniqueStorage::new()`: bulk-load's
            // rebuild-from-empty path must not silently drop a
            // `define_persisted_index` storage's `persist` marker (and the
            // codec/attach closures it carries) just because it's rebuilding
            // from zero rows.
            storage: self.storage.empty_like(),
            name: self.name.clone(),
            kind: self.kind,
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn rebuild_from_sorted_data(&mut self, data: &BTree<K, R>) -> Result<()> {
        // 1. Extract (index key, row key) pairs in data order.
        let mut pairs: Vec<(IK, K)> = data
            .range(..)
            .map(|(key, rec)| (self.extractor.extract(rec), key.clone()))
            .collect();
        // 2. Sort by key. Detect collisions for unique storage.
        pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        for w in pairs.windows(2) {
            if w[0].0 == w[1].0 {
                return Err(Error::DuplicateKey(self.name.clone()));
            }
        }
        // 3. Bulk-build the index B-tree.
        let arc_pairs = pairs.into_iter().map(|(ik, key)| (ik, Arc::new(key)));
        let new_tree: BTree<IK, K> = BTree::from_sorted(arc_pairs);
        // Assign the tree field directly (same module, private field) rather
        // than replacing the whole `storage` value — a full replace would
        // silently drop any `codec`/`persist` marker `define_persisted_index`
        // already stamped on this storage.
        self.storage.tree = new_tree;
        Ok(())
    }

    #[cfg(feature = "persistence")]
    fn attach_paged_source(&mut self, file: Arc<PageFile>, stats: Arc<PagedStats>, table_name: &str) {
        let Some(p) = self.storage.persist.clone() else {
            return; // not a persisted index — stays purely in-memory
        };
        let name = format!("{table_name}.{}", self.name);
        (p.attach)(&mut self.storage.tree, file, stats, name);
    }

    #[cfg(feature = "persistence")]
    fn paged_write(&self, ctx: &PagedCtx) -> Result<Option<PagedIndexEntry>> {
        let Some(p) = &self.storage.persist else {
            return Ok(None);
        };
        if self.storage.tree.is_empty() {
            return Ok(Some(PagedIndexEntry {
                name: self.name.clone(),
                ik_type_id: p.ik_type_id,
                kind: 0,
                generation: p.generation,
                root_page: None,
                height: 0,
                len: 0,
            }));
        }
        let root_id = p.write(&self.storage.tree, ctx)?;
        Ok(Some(PagedIndexEntry {
            name: self.name.clone(),
            ik_type_id: p.ik_type_id,
            kind: 0,
            generation: p.generation,
            root_page: Some(root_id),
            height: self.storage.tree.height() as u32,
            len: self.storage.tree.len() as u64,
        }))
    }

    #[cfg(feature = "persistence")]
    fn paged_changed_pages(&self, prev: &dyn IndexMaintainer<R, K>) -> Vec<PageId> {
        let Some(prev) = prev.as_any().downcast_ref::<ManagedIndex<R, IK, UniqueStorage<IK, K>>>() else {
            return Vec::new();
        };
        self.storage.tree.changed_page_ids(&prev.storage.tree)
    }

    #[cfg(feature = "persistence")]
    fn paged_generation(&self) -> u32 {
        self.storage.persist.as_ref().map(|p| p.generation).unwrap_or(0)
    }

    #[cfg(feature = "persistence")]
    fn paged_reachable_ids(&self) -> Vec<PageId> {
        if self.storage.persist.is_none() {
            return Vec::new();
        }
        // Structural, not just documented: `for_each_page_id` only
        // descends into slots that are *already* loaded, so if this tree
        // were ever not fully resident (violating the invariant
        // `from_root_page`'s `load_all` establishes at attach time), it
        // would silently under-report rather than fault anything in.
        // Calling `load_all` here makes full residency an invariant this
        // method itself restores rather than merely assumes -- idempotent
        // and O(already-resident) (a pointer-only walk, no I/O) whenever
        // that invariant already holds, via `Child::load_quiet`'s fast
        // path.
        self.storage.tree.load_all();
        let mut out = Vec::new();
        self.storage.tree.for_each_page_id(&mut |id| out.push(id));
        out
    }
}

impl<R, IK, K> IndexMaintainer<R, K> for ManagedIndex<R, IK, NonUniqueStorage<IK, K>>
where
    IK: Ord + Clone + Send + Sync + 'static,
    K: PrimaryKey,
    R: Record,
{
    fn on_insert(&mut self, key: K, record: &R) -> Result<()> {
        let idx_key = self.extractor.extract(record);
        self.storage.insert(idx_key, key, &self.name)
    }

    fn on_update(&mut self, key: K, old: &R, new: &R) -> Result<()> {
        let old_key = self.extractor.extract(old);
        let new_key = self.extractor.extract(new);
        if old_key != new_key {
            // Insert new key first — if it fails (e.g. unique constraint),
            // old_key is still intact and no rollback is needed.
            self.storage.insert(new_key, key.clone(), &self.name)?;
            self.storage.delete(old_key, key);
        }
        Ok(())
    }

    fn on_delete(&mut self, key: K, record: &R) {
        let idx_key = self.extractor.extract(record);
        self.storage.delete(idx_key, key);
    }

    fn kind(&self) -> IndexKind {
        self.kind
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn clone_box(&self) -> Box<dyn IndexMaintainer<R, K>> {
        Box::new(Self {
            extractor: Arc::clone(&self.extractor),
            storage: self.storage.clone(),
            name: self.name.clone(),
            kind: self.kind,
        })
    }

    fn empty_clone(&self) -> Result<Box<dyn IndexMaintainer<R, K>>> {
        Ok(Box::new(Self {
            extractor: Arc::clone(&self.extractor),
            // See the comment in the `UniqueStorage` impl's `empty_clone`
            // above — same reasoning, `empty_like` over `::new()`.
            storage: self.storage.empty_like(),
            name: self.name.clone(),
            kind: self.kind,
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn rebuild_from_sorted_data(&mut self, data: &BTree<K, R>) -> Result<()> {
        // (index key, row key) is already strictly ordered when sorted
        // because the row key is unique.
        let mut pairs: Vec<((IK, K), ())> = data
            .range(..)
            .map(|(key, rec)| ((self.extractor.extract(rec), key.clone()), ()))
            .collect();
        pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let arc_pairs = pairs.into_iter().map(|(k, _)| (k, Arc::new(())));
        let new_tree: BTree<(IK, K), ()> = BTree::from_sorted(arc_pairs);
        // See the comment in the `UniqueStorage` impl above: assigning the
        // tree field directly preserves any `codec`/`persist` marker.
        self.storage.tree = new_tree;
        Ok(())
    }

    #[cfg(feature = "persistence")]
    fn attach_paged_source(&mut self, file: Arc<PageFile>, stats: Arc<PagedStats>, table_name: &str) {
        let Some(p) = self.storage.persist.clone() else {
            return; // not a persisted index — stays purely in-memory
        };
        let name = format!("{table_name}.{}", self.name);
        (p.attach)(&mut self.storage.tree, file, stats, name);
    }

    #[cfg(feature = "persistence")]
    fn paged_write(&self, ctx: &PagedCtx) -> Result<Option<PagedIndexEntry>> {
        let Some(p) = &self.storage.persist else {
            return Ok(None);
        };
        if self.storage.tree.is_empty() {
            return Ok(Some(PagedIndexEntry {
                name: self.name.clone(),
                ik_type_id: p.ik_type_id,
                kind: 1,
                generation: p.generation,
                root_page: None,
                height: 0,
                len: 0,
            }));
        }
        let root_id = p.write(&self.storage.tree, ctx)?;
        Ok(Some(PagedIndexEntry {
            name: self.name.clone(),
            ik_type_id: p.ik_type_id,
            kind: 1,
            generation: p.generation,
            root_page: Some(root_id),
            height: self.storage.tree.height() as u32,
            len: self.storage.tree.len() as u64,
        }))
    }

    #[cfg(feature = "persistence")]
    fn paged_changed_pages(&self, prev: &dyn IndexMaintainer<R, K>) -> Vec<PageId> {
        let Some(prev) = prev.as_any().downcast_ref::<ManagedIndex<R, IK, NonUniqueStorage<IK, K>>>() else {
            return Vec::new();
        };
        self.storage.tree.changed_page_ids(&prev.storage.tree)
    }

    #[cfg(feature = "persistence")]
    fn paged_generation(&self) -> u32 {
        self.storage.persist.as_ref().map(|p| p.generation).unwrap_or(0)
    }

    #[cfg(feature = "persistence")]
    fn paged_reachable_ids(&self) -> Vec<PageId> {
        if self.storage.persist.is_none() {
            return Vec::new();
        }
        // Structural, not just documented: `for_each_page_id` only
        // descends into slots that are *already* loaded, so if this tree
        // were ever not fully resident (violating the invariant
        // `from_root_page`'s `load_all` establishes at attach time), it
        // would silently under-report rather than fault anything in.
        // Calling `load_all` here makes full residency an invariant this
        // method itself restores rather than merely assumes -- idempotent
        // and O(already-resident) (a pointer-only walk, no I/O) whenever
        // that invariant already holds, via `Child::load_quiet`'s fast
        // path.
        self.storage.tree.load_all();
        let mut out = Vec::new();
        self.storage.tree.for_each_page_id(&mut |id| out.push(id));
        out
    }
}

// ---------------------------------------------------------------------------
// PersistedIndex — the paged bookkeeping a `define_persisted_index` storage
// carries; `None` on a plain `define_index` storage.
// ---------------------------------------------------------------------------

/// Encodes one node to its page payload+kind — see [`PersistedIndex`].
#[cfg(feature = "persistence")]
type EncodeFn<TK, TV> = Arc<dyn Fn(&BTreeNode<TK, TV>) -> Result<(PageKind, Vec<u8>)> + Send + Sync>;
/// Builds this index's `PagedSource` and attaches it to a tree — see
/// [`PersistedIndex`].
#[cfg(feature = "persistence")]
type AttachFn<TK, TV> = Arc<dyn Fn(&mut BTree<TK, TV>, Arc<PageFile>, Arc<PagedStats>, String) + Send + Sync>;

/// Paged bookkeeping for a persisted index's tree (`TK`/`TV` are that
/// tree's own key/value types — `(IK, K)`/`()` for `NonUniqueStorage`,
/// `IK`/`K` for `UniqueStorage`). Built once by
/// `UniqueStorage::new_persisted`/`NonUniqueStorage::new_persisted`, where
/// the index key `IK` is known to satisfy `PrimaryKey` (that constructor
/// carries its own `where IK: PrimaryKey`, same trick `NodeCodec` itself
/// uses for its `unique_index`/`non_unique_index` constructors).
///
/// `IndexMaintainer`'s trait methods (`attach_paged_source`, `paged_write`,
/// ...) are implemented once for `ManagedIndex<R, IK, UniqueStorage<IK,
/// K>>` under `ManagedIndex`'s own, *looser* bound
/// (`IK: Ord + Clone + Send + Sync + 'static`) — the same bound
/// `Table::define_index` promises its callers, which this crate cannot
/// tighten to `PrimaryKey` without breaking existing non-`PrimaryKey`
/// index keys (`usize`, for one — see `src/store.rs`'s `by_len` indexes).
/// So those trait bodies can never call `NodeCodec::encode`/`decode`
/// directly (those require `K: PrimaryKey`, per `pagecodec.rs`). Capturing
/// the codec inside plain closures here sidesteps that: a `Fn` trait
/// object needs no bound on what it closed over in order to be *called*,
/// only to be *built* — and it was built where the bound held.
// No production caller yet — `Table::define_persisted_index` (Task 8+
// wires it into `Store`). Used today by `table.rs`'s `paged` test module.
#[allow(dead_code)]
#[cfg(feature = "persistence")]
#[derive(Clone)]
struct PersistedIndex<TK, TV> {
    ik_type_id: u32,
    generation: u32,
    encode: EncodeFn<TK, TV>,
    attach: AttachFn<TK, TV>,
}

#[cfg(feature = "persistence")]
impl<TK: Ord + Clone, TV> PersistedIndex<TK, TV> {
    /// Write every dirty page of `tree` via the captured codec. Neither
    /// this nor `BTree::write_dirty` itself needs `TK: PrimaryKey` — only
    /// *building* the codec (inside the closure, at construction time)
    /// did.
    // No production caller yet — see the struct doc above.
    #[allow(dead_code)]
    fn write(&self, tree: &BTree<TK, TV>, ctx: &PagedCtx) -> Result<PageId> {
        let mut first_err: Option<Error> = None;
        let root_id = tree.write_dirty(&mut |node, _is_leaf| {
            match (self.encode)(node).and_then(|(kind, bytes)| ctx.file.append(kind, &bytes)) {
                Ok(id) => {
                    ctx.stats.pages_written.fetch_add(1, Ordering::Relaxed);
                    id
                }
                Err(e) => {
                    // `write_dirty`'s callback can't return `Result`;
                    // NO_PAGE aborts the walk (leaving the failed path
                    // dirty) and this carries the error out.
                    first_err.get_or_insert(e);
                    NO_PAGE
                }
            }
        });
        match first_err {
            Some(e) => Err(e),
            None => Ok(root_id),
        }
    }
}

// ---------------------------------------------------------------------------
// UniqueStorage — maps IK -> K (one primary key per index key)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct UniqueStorage<IK: Ord + Clone, K: PrimaryKey> {
    tree: BTree<IK, K>,
    /// Set by `Table::define_persisted_index`; `None` for a plain
    /// `Table::define_index` index, which `paged_write` then skips and
    /// `attach_paged_source` leaves without a page-file source.
    // No production caller yet — see `PersistedIndex`'s doc.
    #[allow(dead_code)]
    #[cfg(feature = "persistence")]
    persist: Option<PersistedIndex<IK, K>>,
}

impl<IK: Ord + Clone + 'static, K: PrimaryKey> UniqueStorage<IK, K> {
    /// Creates a new, empty unique index storage.
    pub fn new() -> Self {
        Self {
            tree: BTree::new(),
            #[cfg(feature = "persistence")]
            persist: None,
        }
    }

    /// A storage stamped for the paged path — see [`PersistedIndex`].
    #[cfg(feature = "persistence")]
    pub(crate) fn new_persisted(persist: (u32, u32)) -> Self
    where
        IK: PrimaryKey,
    {
        let (ik_type_id, generation) = persist;
        let codec = NodeCodec::<IK, K>::unique_index::<IK, K>();
        let codec_for_encode = codec.clone();
        Self {
            tree: BTree::new(),
            persist: Some(PersistedIndex {
                ik_type_id,
                generation,
                encode: Arc::new(move |node| codec_for_encode.encode(node)),
                attach: Arc::new(move |tree, file, stats, name| {
                    let source: Arc<PagedSource<IK, K>> =
                        Arc::new(PagedSource { file, codec: codec.clone(), name, stats });
                    tree.set_source(Some(source));
                }),
            }),
        }
    }

    /// A storage rebuilt from a recovered [`PagedIndexEntry`] (Task 13's
    /// attach path): the tree is reconstructed via `BTree::from_root_page`
    /// and immediately made fully resident via `BTree::load_all` — an
    /// index tree is never demoted (see `load_all`'s doc), so leaving any
    /// of it on disk here would just re-fault the same pages on the first
    /// query with nothing gained. `root_page: None` (an empty persisted
    /// index) needs neither: `BTree::new()` is already fully resident.
    ///
    /// Stamps a fresh `persist` marker under `ik_type_id`/`generation` the
    /// same way `new_persisted` does, so a later `paged_write` call on this
    /// storage knows its codec — the caller (`Table::define_persisted_index`)
    /// only reaches this once `ik_type_id`, `kind`, and `generation` have
    /// already been checked to match the pending entry, so re-stamping here
    /// (rather than trusting the pending entry's own fields verbatim) is
    /// just avoiding a second place that could drift from what was
    /// actually validated.
    #[cfg(feature = "persistence")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_root_page(
        root_page: Option<u64>,
        len: usize,
        height: usize,
        ik_type_id: u32,
        generation: u32,
        file: Arc<PageFile>,
        stats: Arc<PagedStats>,
        name: String,
    ) -> Self
    where
        IK: PrimaryKey,
    {
        let codec = NodeCodec::<IK, K>::unique_index::<IK, K>();
        let codec_for_encode = codec.clone();
        let tree = match root_page {
            Some(id) => {
                let source: Arc<PagedSource<IK, K>> =
                    Arc::new(PagedSource { file, codec: codec.clone(), name, stats });
                let t = BTree::from_root_page(id, len, height, source);
                t.load_all();
                t
            }
            None => BTree::new(),
        };
        Self {
            tree,
            persist: Some(PersistedIndex {
                ik_type_id,
                generation,
                encode: Arc::new(move |node| codec_for_encode.encode(node)),
                attach: Arc::new(move |tree, file, stats, name| {
                    let source: Arc<PagedSource<IK, K>> =
                        Arc::new(PagedSource { file, codec: codec.clone(), name, stats });
                    tree.set_source(Some(source));
                }),
            }),
        }
    }

    /// An empty storage preserving this one's paged-persistence marker (if
    /// any) — used by `empty_clone` (bulk-load's rebuild-from-empty path)
    /// so a `define_persisted_index` storage doesn't silently lose its
    /// `persist` marker (and the codec/attach closures it carries) just
    /// because it's being rebuilt from zero rows. Not itself feature-gated
    /// (`empty_clone` needs it unconditionally); only the field it copies
    /// is.
    pub(crate) fn empty_like(&self) -> Self {
        Self {
            tree: BTree::new(),
            #[cfg(feature = "persistence")]
            persist: self.persist.clone(),
        }
    }

    /// The row key indexed under `key`, if present.
    pub fn get(&self, key: &IK) -> Option<K> {
        self.tree.get(key).cloned()
    }

    /// `(index key, row key)` pairs whose index key falls in `range`,
    /// ascending by index key.
    pub fn range_ids<'a>(
        &'a self,
        range: impl std::ops::RangeBounds<IK> + 'a,
    ) -> impl Iterator<Item = (&'a IK, K)> + 'a {
        self.tree.range(range).map(|(k, v)| (k, v.clone()))
    }
}

impl<IK: Ord + Clone + Send + Sync + 'static, K: PrimaryKey> IndexStorage<IK, K>
    for UniqueStorage<IK, K>
{
    fn insert(&mut self, key: IK, row_key: K, name: &str) -> Result<()> {
        if self.tree.get(&key).is_some() {
            return Err(Error::DuplicateKey(name.to_string()));
        }
        self.tree = self.tree.insert(key, row_key);
        Ok(())
    }

    fn delete(&mut self, key: IK, _row_key: K) {
        match self.tree.remove(&key) {
            Ok(new_tree) => self.tree = new_tree,
            Err(_) => debug_assert!(false, "UniqueStorage::delete called for absent key"),
        }
    }
}

// ---------------------------------------------------------------------------
// NonUniqueStorage — maps (IK, K) -> () (composite key for multi-value)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct NonUniqueStorage<IK: Ord + Clone, K: PrimaryKey> {
    tree: BTree<(IK, K), ()>,
    /// See the field doc on `UniqueStorage` — same contract, just keyed by
    /// the composite `(IK, K)` this storage's tree actually uses.
    // No production caller yet — see `PersistedIndex`'s doc.
    #[allow(dead_code)]
    #[cfg(feature = "persistence")]
    persist: Option<PersistedIndex<(IK, K), ()>>,
}

impl<IK: Ord + Clone + Send + Sync + 'static, K: PrimaryKey> NonUniqueStorage<IK, K> {
    /// Creates a new, empty non-unique index storage.
    pub fn new() -> Self {
        Self {
            tree: BTree::new(),
            #[cfg(feature = "persistence")]
            persist: None,
        }
    }

    /// A storage stamped for the paged path — see [`PersistedIndex`].
    #[cfg(feature = "persistence")]
    pub(crate) fn new_persisted(persist: (u32, u32)) -> Self
    where
        IK: PrimaryKey,
    {
        let (ik_type_id, generation) = persist;
        let codec = NodeCodec::<(IK, K), ()>::non_unique_index::<IK, K>();
        let codec_for_encode = codec.clone();
        Self {
            tree: BTree::new(),
            persist: Some(PersistedIndex {
                ik_type_id,
                generation,
                encode: Arc::new(move |node| codec_for_encode.encode(node)),
                attach: Arc::new(move |tree, file, stats, name| {
                    let source: Arc<PagedSource<(IK, K), ()>> =
                        Arc::new(PagedSource { file, codec: codec.clone(), name, stats });
                    tree.set_source(Some(source));
                }),
            }),
        }
    }

    /// A storage rebuilt from a recovered [`PagedIndexEntry`] — see the
    /// doc on [`UniqueStorage::from_root_page`], the same contract applied
    /// to this storage's composite `(IK, K)` tree.
    #[cfg(feature = "persistence")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_root_page(
        root_page: Option<u64>,
        len: usize,
        height: usize,
        ik_type_id: u32,
        generation: u32,
        file: Arc<PageFile>,
        stats: Arc<PagedStats>,
        name: String,
    ) -> Self
    where
        IK: PrimaryKey,
    {
        let codec = NodeCodec::<(IK, K), ()>::non_unique_index::<IK, K>();
        let codec_for_encode = codec.clone();
        let tree = match root_page {
            Some(id) => {
                let source: Arc<PagedSource<(IK, K), ()>> =
                    Arc::new(PagedSource { file, codec: codec.clone(), name, stats });
                let t = BTree::from_root_page(id, len, height, source);
                t.load_all();
                t
            }
            None => BTree::new(),
        };
        Self {
            tree,
            persist: Some(PersistedIndex {
                ik_type_id,
                generation,
                encode: Arc::new(move |node| codec_for_encode.encode(node)),
                attach: Arc::new(move |tree, file, stats, name| {
                    let source: Arc<PagedSource<(IK, K), ()>> =
                        Arc::new(PagedSource { file, codec: codec.clone(), name, stats });
                    tree.set_source(Some(source));
                }),
            }),
        }
    }

    /// See the `empty_like` doc on `UniqueStorage` — same contract.
    pub(crate) fn empty_like(&self) -> Self {
        Self {
            tree: BTree::new(),
            #[cfg(feature = "persistence")]
            persist: self.persist.clone(),
        }
    }

    /// Row keys sharing the index key `key`, ascending. O(log n + k) — the
    /// prefix group is located by descent, not by filtering a wider scan.
    pub fn get_ids<'a>(&'a self, key: &'a IK) -> impl Iterator<Item = K> + 'a {
        self.tree.range_prefix(key).map(|((_, k), _)| k.clone())
    }

    /// Row keys whose index key falls in `range`, ascending by `(IK, K)`.
    ///
    /// The bounds are on `IK` alone, which cannot be expressed as a
    /// `RangeBounds<(IK, K)>` (that would need a min/max value for the
    /// arbitrary row-key type `K`), so this goes through `BTree::range_by`
    /// with a locator that looks only at the index-key half of the composite.
    pub fn range_ids<'a>(
        &'a self,
        range: impl std::ops::RangeBounds<IK> + 'a,
    ) -> impl Iterator<Item = (&'a IK, K)> + 'a {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        // Monotone: `(ik, _)` sorts by `ik` first, and `classify` on `ik` is
        // itself monotone — an entire `ik` group is classified alike, so
        // `Excluded` drops the whole group rather than part of it. Shares
        // `classify` with `BTree::range`'s own bound handling so the two
        // cannot drift apart.
        self.tree
            .range_by(move |(ik, _): &(IK, K)| crate::btree::classify(&start, &end, ik))
            .map(|((ik, id), _)| (ik, id.clone()))
    }
}

impl<IK: Ord + Clone + Send + Sync + 'static, K: PrimaryKey> IndexStorage<IK, K>
    for NonUniqueStorage<IK, K>
{
    fn insert(&mut self, key: IK, row_key: K, _name: &str) -> Result<()> {
        self.tree = self.tree.insert((key, row_key), ());
        Ok(())
    }

    fn delete(&mut self, key: IK, row_key: K) {
        match self.tree.remove(&(key, row_key)) {
            Ok(new_tree) => self.tree = new_tree,
            Err(_) => debug_assert!(false, "NonUniqueStorage::delete called for absent key"),
        }
    }
}

// ---------------------------------------------------------------------------
// CustomIndex — public trait for user-defined index structures
// ---------------------------------------------------------------------------

/// Trait for user-defined custom indexes.
///
/// Implementors have full control over their internal data structure and query
/// API. The `Clone` bound is required for CoW snapshot cloning — use
/// [`BTree<K, V>`](crate::btree::BTree) internally for O(1) clone.
pub trait CustomIndex<R: Record, K: PrimaryKey = u64>: Send + Sync + Clone + 'static {
    /// Called when a record is inserted. Return `Err` to veto the mutation.
    fn on_insert(&mut self, key: K, record: &R) -> Result<()>;

    /// Called when a record is updated. Return `Err` to veto the mutation.
    fn on_update(&mut self, key: K, old: &R, new: &R) -> Result<()>;

    /// Called when a record is deleted.
    fn on_delete(&mut self, key: K, record: &R);

    /// Rebuild the entire index from an iterator of `(key, record)` pairs.
    ///
    /// Used for backfilling when the index is defined on a non-empty table,
    /// and for recovery from persistence. The default implementation iterates
    /// and calls [`on_insert`](Self::on_insert) for each entry.
    fn rebuild<'a>(&mut self, data: impl Iterator<Item = (K, &'a R)>) -> Result<()>
    where
        R: 'a,
    {
        for (key, record) in data {
            self.on_insert(key, record)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CustomIndexAdapter — bridges CustomIndex into IndexMaintainer
// ---------------------------------------------------------------------------

pub(crate) struct CustomIndexAdapter<R: Record, K: PrimaryKey, I: CustomIndex<R, K>> {
    inner: I,
    name: String,
    _phantom: std::marker::PhantomData<(R, K)>,
}

impl<R: Record, K: PrimaryKey, I: CustomIndex<R, K>> CustomIndexAdapter<R, K, I> {
    /// Wraps `index` under registration name `name` so it can live in the
    /// table's type-erased maintainer map alongside the built-in indexes.
    pub fn new(name: String, index: I) -> Self {
        Self {
            inner: index,
            name,
            _phantom: std::marker::PhantomData,
        }
    }

    /// The wrapped user index. `Table::custom_index` downcasts the erased
    /// maintainer to this adapter and hands the caller this reference for
    /// queries.
    pub fn inner(&self) -> &I {
        &self.inner
    }
}

impl<R: Record, K: PrimaryKey, I: CustomIndex<R, K> + 'static> IndexMaintainer<R, K>
    for CustomIndexAdapter<R, K, I>
{
    fn on_insert(&mut self, key: K, record: &R) -> Result<()> {
        self.inner.on_insert(key, record)
    }

    fn on_update(&mut self, key: K, old: &R, new: &R) -> Result<()> {
        self.inner.on_update(key, old, new)
    }

    fn on_delete(&mut self, key: K, record: &R) {
        self.inner.on_delete(key, record)
    }

    fn kind(&self) -> IndexKind {
        IndexKind::Custom
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn clone_box(&self) -> Box<dyn IndexMaintainer<R, K>> {
        Box::new(CustomIndexAdapter {
            inner: self.inner.clone(),
            name: self.name.clone(),
            _phantom: std::marker::PhantomData,
        })
    }

    fn empty_clone(&self) -> Result<Box<dyn IndexMaintainer<R, K>>> {
        // Custom indexes have user-defined internal state with no generic
        // "make empty" hook; the bulk-load primitives don't yet support
        // them. Until a `CustomIndex::empty` requirement (or similar)
        // lands, the bulk-load Replace path can't preserve a custom-index
        // definition and rejects the load. See
        // `docs/tasks/task23_bulk_load.md`.
        Err(Error::InvalidBulkLoadInput(format!(
            "rebuilding custom index '{}' is not supported; \
             drop the index before bulk-loading and redefine it after",
            self.name
        )))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    #[cfg(feature = "persistence")]
    fn attach_paged_source(&mut self, _file: Arc<PageFile>, _stats: Arc<PagedStats>, _table_name: &str) {
        // A custom index's internal storage is opaque to this generic
        // maintainer (see `empty_clone` above) — never paged.
    }

    #[cfg(feature = "persistence")]
    fn paged_write(&self, _ctx: &PagedCtx) -> Result<Option<PagedIndexEntry>> {
        Ok(None)
    }

    #[cfg(feature = "persistence")]
    fn paged_changed_pages(&self, _prev: &dyn IndexMaintainer<R, K>) -> Vec<PageId> {
        Vec::new()
    }

    #[cfg(feature = "persistence")]
    fn paged_generation(&self) -> u32 {
        0
    }

    #[cfg(feature = "persistence")]
    fn paged_reachable_ids(&self) -> Vec<PageId> {
        // Never paged — see `attach_paged_source` above.
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
    struct User {
        email: String,
        age: u32,
    }

    #[test]
    fn unique_index_insert_and_lookup() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let user = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &user).unwrap();
        assert_eq!(idx.storage().get(&"alice@example.com".to_string()), Some(1));
        assert_eq!(idx.storage().get(&"bob@example.com".to_string()), None);
    }

    #[test]
    fn unique_index_rejects_duplicate() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let u1 = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        let u2 = User {
            email: "alice@example.com".to_string(),
            age: 25,
        };
        idx.on_insert(1u64, &u1).unwrap();
        assert!(matches!(idx.on_insert(2, &u2), Err(Error::DuplicateKey(_))));
    }

    #[test]
    fn unique_index_update_changes_key() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let old = User {
            email: "alice@old.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &old).unwrap();
        let new = User {
            email: "alice@new.com".to_string(),
            age: 30,
        };
        idx.on_update(1, &old, &new).unwrap();
        assert_eq!(idx.storage().get(&"alice@old.com".to_string()), None);
        assert_eq!(idx.storage().get(&"alice@new.com".to_string()), Some(1));
    }

    #[test]
    fn unique_index_update_rejects_conflict() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let u1 = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        let u2 = User {
            email: "bob@example.com".to_string(),
            age: 25,
        };
        idx.on_insert(1u64, &u1).unwrap();
        idx.on_insert(2, &u2).unwrap();
        // Try to update bob's email to alice's — should fail
        let u2_new = User {
            email: "alice@example.com".to_string(),
            age: 25,
        };
        assert!(matches!(
            idx.on_update(2, &u2, &u2_new),
            Err(Error::DuplicateKey(_))
        ));
        // Bob's old email should still be in the index (rollback)
        assert_eq!(idx.storage().get(&"bob@example.com".to_string()), Some(2));
    }

    #[test]
    fn unique_index_delete() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let user = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &user).unwrap();
        idx.on_delete(1, &user);
        assert_eq!(idx.storage().get(&"alice@example.com".to_string()), None);
    }

    #[test]
    fn non_unique_index_insert_and_lookup() {
        let ext: Arc<dyn KeyExtractor<User, u32>> = Arc::new(|u: &User| u.age);
        let mut idx = ManagedIndex::new(
            "by_age".to_string(),
            IndexKind::NonUnique,
            ext,
            NonUniqueStorage::new(),
        );
        let u1 = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        let u2 = User {
            email: "bob@example.com".to_string(),
            age: 30,
        };
        let u3 = User {
            email: "charlie@example.com".to_string(),
            age: 25,
        };
        idx.on_insert(1u64, &u1).unwrap();
        idx.on_insert(2, &u2).unwrap();
        idx.on_insert(3, &u3).unwrap();
        let ids_30: Vec<u64> = idx.storage().get_ids(&30).collect();
        assert_eq!(ids_30, vec![1, 2]);
        let ids_25: Vec<u64> = idx.storage().get_ids(&25).collect();
        assert_eq!(ids_25, vec![3]);
        let ids_99: Vec<u64> = idx.storage().get_ids(&99).collect();
        assert!(ids_99.is_empty());
    }

    #[test]
    fn non_unique_index_update() {
        let ext: Arc<dyn KeyExtractor<User, u32>> = Arc::new(|u: &User| u.age);
        let mut idx = ManagedIndex::new(
            "by_age".to_string(),
            IndexKind::NonUnique,
            ext,
            NonUniqueStorage::new(),
        );
        let old = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &old).unwrap();
        let new = User {
            email: "alice@example.com".to_string(),
            age: 31,
        };
        idx.on_update(1, &old, &new).unwrap();
        assert_eq!(idx.storage().get_ids(&30).count(), 0);
        let ids_31: Vec<u64> = idx.storage().get_ids(&31).collect();
        assert_eq!(ids_31, vec![1]);
    }

    #[test]
    fn non_unique_index_delete() {
        let ext: Arc<dyn KeyExtractor<User, u32>> = Arc::new(|u: &User| u.age);
        let mut idx = ManagedIndex::new(
            "by_age".to_string(),
            IndexKind::NonUnique,
            ext,
            NonUniqueStorage::new(),
        );
        let u1 = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        let u2 = User {
            email: "bob@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &u1).unwrap();
        idx.on_insert(2, &u2).unwrap();
        idx.on_delete(1, &u1);
        let ids_30: Vec<u64> = idx.storage().get_ids(&30).collect();
        assert_eq!(ids_30, vec![2]);
    }

    #[test]
    fn clone_box_produces_independent_copy() {
        let ext: Arc<dyn KeyExtractor<User, String>> = Arc::new(|u: &User| u.email.clone());
        let mut idx = ManagedIndex::new(
            "by_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let user = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &user).unwrap();

        let cloned = idx.clone_box();
        // Mutate original
        let user2 = User {
            email: "bob@example.com".to_string(),
            age: 25,
        };
        idx.on_insert(2u64, &user2).unwrap();

        // Clone should not see the new insert
        let cloned = cloned
            .as_any()
            .downcast_ref::<ManagedIndex<User, String, UniqueStorage<String, u64>>>()
            .unwrap();
        assert_eq!(cloned.storage().get(&"bob@example.com".to_string()), None);
        assert_eq!(
            cloned.storage().get(&"alice@example.com".to_string()),
            Some(1)
        );
    }

    #[test]
    fn index_kind_custom_variant() {
        let kind = IndexKind::Custom;
        assert_eq!(kind, IndexKind::Custom);
        assert_ne!(kind, IndexKind::Unique);
        assert_ne!(kind, IndexKind::NonUnique);
    }

    /// A minimal custom index that tracks the sum of a numeric field.
    #[derive(Clone)]
    struct SumIndex {
        total: u64,
    }

    impl SumIndex {
        fn new() -> Self {
            Self { total: 0 }
        }

        fn total(&self) -> u64 {
            self.total
        }
    }

    impl CustomIndex<User> for SumIndex {
        fn on_insert(&mut self, _id: u64, record: &User) -> Result<()> {
            self.total += record.age as u64;
            Ok(())
        }

        fn on_update(&mut self, _id: u64, old: &User, new: &User) -> Result<()> {
            self.total -= old.age as u64;
            self.total += new.age as u64;
            Ok(())
        }

        fn on_delete(&mut self, _id: u64, record: &User) {
            self.total -= record.age as u64;
        }
    }

    #[test]
    fn custom_index_adapter_lifecycle() {
        let sum = SumIndex::new();
        let mut adapter = CustomIndexAdapter::new("sum".to_string(), sum);

        let u1 = User {
            email: "a@x.com".to_string(),
            age: 30,
        };
        adapter.on_insert(1, &u1).unwrap();

        let inner = adapter
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, u64, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(inner.total(), 30);

        let u2 = User {
            email: "b@x.com".to_string(),
            age: 20,
        };
        adapter.on_insert(2, &u2).unwrap();

        let inner = adapter
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, u64, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(inner.total(), 50);

        let u1_new = User {
            email: "a@x.com".to_string(),
            age: 35,
        };
        adapter.on_update(1, &u1, &u1_new).unwrap();

        let inner = adapter
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, u64, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(inner.total(), 55);

        adapter.on_delete(2, &u2);

        let inner = adapter
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, u64, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(inner.total(), 35);

        assert_eq!(adapter.kind(), IndexKind::Custom);
    }

    #[test]
    fn custom_index_adapter_clone_box_independent() {
        let sum = SumIndex::new();
        let mut adapter = CustomIndexAdapter::new("sum".to_string(), sum);

        let u1 = User {
            email: "a@x.com".to_string(),
            age: 30,
        };
        adapter.on_insert(1, &u1).unwrap();

        let cloned = adapter.clone_box();

        let u2 = User {
            email: "b@x.com".to_string(),
            age: 20,
        };
        adapter.on_insert(2, &u2).unwrap();

        let cloned_inner = cloned
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, u64, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(cloned_inner.total(), 30);

        let orig_inner = adapter
            .as_any()
            .downcast_ref::<CustomIndexAdapter<User, u64, SumIndex>>()
            .unwrap()
            .inner();
        assert_eq!(orig_inner.total(), 50);
    }

    #[test]
    fn rebuild_from_sorted_data_unique_matches_incremental() {
        // Build the same index two ways: incrementally via on_insert vs.
        // bulk via rebuild_from_sorted_data. Assert identical lookups.
        use std::sync::Arc;

        #[derive(Clone)]
        #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
        struct Row {
            name: String,
        }

        let data: BTree<u64, Row> = {
            let mut t = BTree::new();
            for (id, name) in [(1u64, "a"), (2, "b"), (3, "c"), (4, "d")] {
                t = t.insert(
                    id,
                    Row {
                        name: name.to_string(),
                    },
                );
            }
            t
        };

        let extractor = Arc::new(|r: &Row| r.name.clone());
        let mut incr: ManagedIndex<Row, String, UniqueStorage<String, u64>> = ManagedIndex::new(
            "idx".into(),
            IndexKind::Unique,
            extractor.clone(),
            UniqueStorage::new(),
        );
        for (id, row) in data.range(..) {
            incr.on_insert(*id, row).unwrap();
        }

        let mut bulk: ManagedIndex<Row, String, UniqueStorage<String, u64>> = ManagedIndex::new(
            "idx".into(),
            IndexKind::Unique,
            extractor,
            UniqueStorage::new(),
        );
        bulk.rebuild_from_sorted_data(&data).unwrap();

        for key in ["a", "b", "c", "d"] {
            assert_eq!(
                incr.storage().get(&key.to_string()),
                bulk.storage().get(&key.to_string())
            );
        }
    }

    #[test]
    fn rebuild_from_sorted_data_non_unique_matches_incremental() {
        use std::sync::Arc;

        #[derive(Clone)]
        #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
        struct Row {
            age: u32,
        }

        let data: BTree<u64, Row> = {
            let mut t = BTree::new();
            for (id, age) in [(1u64, 10u32), (2, 20), (3, 10), (4, 30), (5, 20)] {
                t = t.insert(id, Row { age });
            }
            t
        };

        let extractor = Arc::new(|r: &Row| r.age);
        let mut incr: ManagedIndex<Row, u32, NonUniqueStorage<u32, u64>> = ManagedIndex::new(
            "idx".into(),
            IndexKind::NonUnique,
            extractor.clone(),
            NonUniqueStorage::new(),
        );
        for (id, row) in data.range(..) {
            incr.on_insert(*id, row).unwrap();
        }

        let mut bulk: ManagedIndex<Row, u32, NonUniqueStorage<u32, u64>> = ManagedIndex::new(
            "idx".into(),
            IndexKind::NonUnique,
            extractor,
            NonUniqueStorage::new(),
        );
        bulk.rebuild_from_sorted_data(&data).unwrap();

        for key in [10u32, 20, 30] {
            let mut a: Vec<u64> = incr.storage().get_ids(&key).collect();
            a.sort();
            let mut b: Vec<u64> = bulk.storage().get_ids(&key).collect();
            b.sort();
            assert_eq!(a, b);
        }
    }

    #[test]
    fn rebuild_from_sorted_data_unique_collision_errors() {
        use std::sync::Arc;
        #[derive(Clone)]
        #[cfg_attr(feature = "persistence", derive(serde::Serialize, serde::Deserialize))]
        struct Row {
            name: String,
        }

        let data: BTree<u64, Row> = {
            let mut t = BTree::new();
            t = t.insert(1, Row { name: "dup".into() });
            t = t.insert(2, Row { name: "dup".into() });
            t
        };
        let mut idx: ManagedIndex<Row, String, UniqueStorage<String, u64>> = ManagedIndex::new(
            "idx".into(),
            IndexKind::Unique,
            Arc::new(|r: &Row| r.name.clone()),
            UniqueStorage::new(),
        );
        let res = idx.rebuild_from_sorted_data(&data);
        assert!(matches!(res, Err(Error::DuplicateKey(_))));
    }

    #[test]
    fn unique_compound_index() {
        let ext: Arc<dyn KeyExtractor<User, (u32, String)>> =
            Arc::new(|u: &User| (u.age, u.email.clone()));
        let mut idx = ManagedIndex::new(
            "by_age_email".to_string(),
            IndexKind::Unique,
            ext,
            UniqueStorage::new(),
        );
        let user = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        idx.on_insert(1u64, &user).unwrap();

        assert_eq!(
            idx.storage().get(&(30, "alice@example.com".to_string())),
            Some(1)
        );
        assert_eq!(
            idx.storage().get(&(30, "bob@example.com".to_string())),
            None
        );

        // Reject duplicate (age, email)
        let user_dup = User {
            email: "alice@example.com".to_string(),
            age: 30,
        };
        assert!(matches!(
            idx.on_insert(2, &user_dup),
            Err(Error::DuplicateKey(_))
        ));
    }

    // -----------------------------------------------------------------------
    // task 2: index storage generic over the row key
    // -----------------------------------------------------------------------

    #[test]
    fn unique_index_over_string_primary_key() {
        // Index key is u32 (an age); row key is String (an email).
        let mut storage: UniqueStorage<u32, String> = UniqueStorage::new();
        storage.insert(30u32, "a@x.com".to_string(), "by_age").unwrap();
        storage.insert(40u32, "b@x.com".to_string(), "by_age").unwrap();

        assert_eq!(storage.get(&30), Some("a@x.com".to_string()));
        assert_eq!(storage.get(&40), Some("b@x.com".to_string()));
        assert_eq!(storage.get(&50), None);

        // A second row at the same index key is rejected.
        let err = storage
            .insert(30u32, "c@x.com".to_string(), "by_age")
            .unwrap_err();
        assert!(matches!(err, Error::DuplicateKey(_)), "got {err:?}");
    }

    #[test]
    fn non_unique_index_over_string_primary_key() {
        let mut storage: NonUniqueStorage<u32, String> = NonUniqueStorage::new();
        storage.insert(30u32, "a@x.com".to_string(), "by_age").unwrap();
        storage.insert(30u32, "b@x.com".to_string(), "by_age").unwrap();
        storage.insert(40u32, "c@x.com".to_string(), "by_age").unwrap();

        let mut at_30: Vec<String> = storage.get_ids(&30).collect();
        at_30.sort();
        assert_eq!(at_30, vec!["a@x.com".to_string(), "b@x.com".to_string()]);

        let at_40: Vec<String> = storage.get_ids(&40).collect();
        assert_eq!(at_40, vec!["c@x.com".to_string()]);
    }

    #[test]
    fn non_unique_range_ids_over_string_primary_key() {
        use std::ops::Bound;
        let mut storage: NonUniqueStorage<u32, String> = NonUniqueStorage::new();
        storage.insert(10u32, "a@x.com".to_string(), "by_age").unwrap();
        storage.insert(20u32, "b@x.com".to_string(), "by_age").unwrap();
        storage.insert(20u32, "c@x.com".to_string(), "by_age").unwrap();
        storage.insert(30u32, "d@x.com".to_string(), "by_age").unwrap();

        let mut got: Vec<String> = storage
            .range_ids((Bound::Included(20u32), Bound::Included(30u32)))
            .map(|(_, k)| k)
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec!["b@x.com".to_string(), "c@x.com".to_string(), "d@x.com".to_string()]
        );

        // Excluding the lower bound must drop that whole group, not part of it.
        let mut got: Vec<String> = storage
            .range_ids((Bound::Excluded(20u32), Bound::Unbounded))
            .map(|(_, k)| k)
            .collect();
        got.sort();
        assert_eq!(got, vec!["d@x.com".to_string()]);
    }
}
