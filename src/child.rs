// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The child slot: what a B-tree node holds instead of `Arc<BTreeNode>`.
//!
//! Three states — resident-dirty (`node` set, `NO_PAGE`), resident-clean
//! (`node` set, page id), on-disk (`node` null, page id). `null + NO_PAGE`
//! is a bug. The pointer is set once by CAS and never cleared, so a
//! `&BTreeNode` borrowed through `load` is valid for the slot's lifetime;
//! that is what keeps `Table::get -> Option<&R>` unchanged. Cloning an
//! on-disk slot copies one word and touches no child — the fix for the
//! sibling-refcount cost measured in
//! `docs/benchmarks/paging-baseline-local-2026-08-29.md`.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

use crate::btree::BTreeNode;
#[cfg(test)]
use crate::btree::Value;

/// Byte offset of a page in the (future) page file. Opaque outside `child`
/// and the paging layer added in later tasks.
pub(crate) type PageId = u64;
/// "Not on disk": all 63 low bits set. Real ids are byte offsets < 2^63, so
/// this value can never collide with a real page id.
pub(crate) const NO_PAGE: u64 = (1u64 << 63) - 1;
/// High bit of `meta`: clock-sweep "second chance" bit, set on every touch
/// and cleared by the (future) page evictor's sweep.
const ACCESSED: u64 = 1u64 << 63;
/// Low 63 bits of `meta`: either a `PageId` or `NO_PAGE`.
const ID_MASK: u64 = NO_PAGE;

/// Where a `Child` fetches a node it doesn't hold in memory. Implemented by
/// the page store; a `MockDisk` stand-in backs the unit tests below.
pub(crate) trait NodeSource<K, V>: Send + Sync {
    /// Read + decode one page into a fresh node. `Err` = I/O or CRC failure.
    fn read_node(&self, id: PageId) -> crate::Result<Arc<BTreeNode<K, V>>>;
    /// Called by `Child::make_mut` when it clones a *clean* node (dirty-bytes trigger).
    fn note_dirty(&self, _bytes: usize) {}
    /// Clone one value for a block-leaf CoW. `None` (the default) means
    /// this source cannot clone values — trees on such sources must never
    /// hold block leaves (I-B). `PagedSource` returns `Some` via the fn
    /// pointer captured at `register_table_paged` (Task 3).
    fn clone_value(&self, _v: &V) -> Option<V> {
        None
    }
    /// Name used in the fault-in panic message.
    fn name(&self) -> &str {
        "<unnamed>"
    }
}

/// A B-tree child slot: either a page id (not yet loaded) or a resident
/// node, in either case in 16 bytes with no `Mutex`/`RwLock`.
///
/// `node` is set at most once, by a single winning CAS in [`Self::fault_in`]
/// — every later read of a non-null pointer is safe to dereference for the
/// slot's whole lifetime, which is what lets [`Self::load`] hand back a
/// plain `&BTreeNode` instead of a guard. `meta` packs the accessed bit and
/// the page id (or [`NO_PAGE`] once the slot is dirty) into one word so both
/// can be read/written with a single atomic op.
pub(crate) struct Child<K, V> {
    meta: AtomicU64,
    node: AtomicPtr<BTreeNode<K, V>>,
    /// `AtomicPtr` is `Send + Sync` for any `T`; this restores the auto
    /// traits an owned `Arc<BTreeNode<K, V>>` would have had (and marks the
    /// slot as owning one strong count, for the benefit of `Drop`/`Clone`).
    _own: PhantomData<Arc<BTreeNode<K, V>>>,
}

impl<K, V> Child<K, V> {
    /// A slot that already holds its node in memory (the pre-paging state:
    /// every slot is `resident`, never `on_disk`, until later tasks add a
    /// page store).
    pub(crate) fn resident(node: Arc<BTreeNode<K, V>>) -> Self {
        Self {
            meta: AtomicU64::new(NO_PAGE),
            node: AtomicPtr::new(Arc::into_raw(node) as *mut _),
            _own: PhantomData,
        }
    }

    /// Like [`Self::resident`], but for a brand-new node joining a tree that
    /// may already have a [`NodeSource`] attached — every mutation-path
    /// creation site (a split's new sibling, a grown root, a delete-path
    /// rebuild, a live `extend_from_sorted` append) rather than a truly
    /// sourceless fresh tree (`BTree::new`, `BTree::from_sorted`'s initial
    /// build). Credits `src.note_dirty` when `src` is `Some` — a newly
    /// created node was never on disk, so it has no clean-to-dirty
    /// *transition* for [`Self::make_mut`] to notice later, but it is just
    /// as much a byte a checkpoint will have to write as one CoW'd from a
    /// clean page. `src: None` (an unattached tree) credits nothing, which
    /// is correct: there is no checkpoint to owe bytes to yet. See
    /// `docs/tasks/task12_background_checkpointer.md`.
    ///
    /// Task 8 (spec §4's dirty-bytes clause): a block leaf's real cost is
    /// `BTreeNode::leaf_bytes()` (`NODE_BYTES` plus its value block), not
    /// the flat `NODE_BYTES` a non-block node still credits — matching the
    /// demote-side debit and the fault-in credit (`PagedSource::read_node`,
    /// task 4) so all three speak the same unit.
    pub(crate) fn resident_new(node: Arc<BTreeNode<K, V>>, src: Option<&dyn NodeSource<K, V>>) -> Self {
        if let Some(s) = src {
            let bytes = if node.block.is_some() { node.leaf_bytes() } else { Self::NODE_BYTES };
            s.note_dirty(bytes);
        }
        Self::resident(node)
    }

    /// A slot that references a page but has not been faulted in yet.
    // Real (non-test) callers are all in `persistence`-gated code
    // (`NodeCodec::decode`, `BTree::from_root_page`/`write_dirty`) — this
    // module compiles unconditionally (`child` has no `#[cfg]` at its `mod`
    // declaration in lib.rs, unlike `pagecodec`/`pagefile`), so without the
    // feature this would otherwise warn as dead (final-review wave, I-4
    // re-check: an earlier pass of this doc sweep removed this allow on the
    // mistaken assumption that "has a real caller" was the same question as
    // "has a caller reachable in every feature combination" — `cargo check
    // --no-default-features --lib` caught the difference).
    #[allow(dead_code)]
    pub(crate) fn on_disk(id: PageId) -> Self {
        debug_assert!(id < NO_PAGE);
        Self {
            meta: AtomicU64::new(id),
            node: AtomicPtr::new(std::ptr::null_mut()),
            _own: PhantomData,
        }
    }

    /// `Some(id)` iff the slot is resident-clean or on-disk (the node's
    /// bytes on disk still match its in-memory contents, or it has none in
    /// memory yet). `None` means resident-dirty: no page reflects this node.
    pub(crate) fn page_id(&self) -> Option<PageId> {
        let id = self.meta.load(Ordering::Acquire) & ID_MASK;
        (id != NO_PAGE).then_some(id)
    }

    /// Whether the node is currently in memory (resident, clean or dirty).
    pub(crate) fn is_loaded(&self) -> bool {
        !self.node.load(Ordering::Acquire).is_null()
    }

    /// Record where a (previously dirty) node was just checkpointed to.
    /// Only legal on a dirty slot — going from `NO_PAGE` to a real id; a
    /// slot that already has a page id must go through `make_mut` (which
    /// resets it to `NO_PAGE`) before it can be reassigned.
    // Real callers (`BTree::write_dirty`, `restore_unchanged_ids`) are both
    // only reachable from `persistence`-gated code — see `on_disk`'s note
    // above for why that still needs `#[allow(dead_code)]` here.
    #[allow(dead_code)]
    pub(crate) fn set_page_id(&self, id: PageId) {
        debug_assert!(id < NO_PAGE);
        // Preserve the accessed bit; only the id part changes (NO_PAGE -> id).
        let mut cur = self.meta.load(Ordering::Acquire);
        loop {
            debug_assert_eq!(cur & ID_MASK, NO_PAGE, "set_page_id on a node that already has a page");
            match self.meta.compare_exchange_weak(cur, (cur & ACCESSED) | id, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(c) => cur = c,
            }
        }
    }

    /// Set the second-chance bit. A no-op (skips the RMW) once it's already set.
    pub(crate) fn mark_accessed(&self) {
        if self.meta.load(Ordering::Relaxed) & ACCESSED == 0 {
            self.meta.fetch_or(ACCESSED, Ordering::Relaxed);
        }
    }

    /// Read the second-chance bit and clear it — the evictor's sweep step.
    // Real caller (`BTree::demote_leaves`) is only reachable from
    // `persistence`-gated code — see `on_disk`'s note above.
    #[allow(dead_code)]
    pub(crate) fn take_accessed(&self) -> bool {
        self.meta.fetch_and(!ACCESSED, Ordering::Relaxed) & ACCESSED != 0
    }

    /// Borrow the node, faulting it in from `src` if it isn't resident yet.
    /// Panics if the slot is on-disk and either `src` is `None` or the read
    /// fails — a paged tree with no `NodeSource` (or a broken one) cannot
    /// answer, and there is no `Result`-returning path through the B-tree's
    /// existing `&V`-returning API to propagate that.
    pub(crate) fn load(&self, src: Option<&dyn NodeSource<K, V>>) -> &BTreeNode<K, V> {
        let p = self.node.load(Ordering::Acquire);
        if !p.is_null() {
            self.mark_accessed();
            // SAFETY: set once, never cleared; the Arc it came from is owned by this slot.
            return unsafe { &*p };
        }
        self.fault_in(src)
    }

    /// Like [`Self::load`], but for an already-resident slot it does not
    /// mark the accessed bit: a genuine fault still marks it (the page
    /// really was just brought in), but a maintenance walk that merely
    /// passes over an already-cached node — checkpointing it in
    /// `BTree::write_dirty`, or just probing tree shape in `BTree::height`
    /// — must not reset that node's place in the eviction sweep's clock
    /// just because it was looked at.
    pub(crate) fn load_quiet(&self, src: Option<&dyn NodeSource<K, V>>) -> &BTreeNode<K, V> {
        let p = self.node.load(Ordering::Acquire);
        if !p.is_null() {
            // SAFETY: same as `load`'s fast path — set once, never cleared.
            return unsafe { &*p };
        }
        self.fault_in(src)
    }

    #[cold]
    fn fault_in(&self, src: Option<&dyn NodeSource<K, V>>) -> &BTreeNode<K, V> {
        match self.try_load(src) {
            Ok(n) => n,
            Err(e) => panic!("{e}"),
        }
    }

    /// Like [`Self::fault_in`], but reports a corrupt slot, a missing
    /// [`NodeSource`], or a failed `read_node` as `Err` instead of
    /// panicking — the EAGER load paths (`recover()`'s inner-level faults,
    /// index attach) need a `Result` to propagate rather than crash the
    /// process on a corrupt page. [`Self::fault_in`] is now a thin panicking
    /// wrapper around this (same message text: `Display` on the returned
    /// error reproduces exactly what `fault_in` used to `panic!` directly).
    /// The LAZY path ([`Self::load`]/[`Self::load_quiet`]) keeps calling
    /// `fault_in` and stays panicking — there is no `Result`-returning path
    /// through the B-tree's existing `&V`-returning API for those (see
    /// `fault_in`'s original doc, preserved on [`Self::load`]).
    #[cold]
    pub(crate) fn try_load(&self, src: Option<&dyn NodeSource<K, V>>) -> crate::Result<&BTreeNode<K, V>> {
        // Mirror `load`'s fast path: a resident-dirty slot has no page id
        // (`page_id()` returns `None` for it), so the `page_id()` check
        // below alone would misreport it as a corrupt slot. Checking the
        // pointer first, exactly like `load`/`load_quiet` do, is what makes
        // this a true fallible equivalent of "get or fault" rather than
        // just a fallible `fault_in`.
        let p = self.node.load(Ordering::Acquire);
        if !p.is_null() {
            self.mark_accessed();
            // SAFETY: set once, never cleared; the Arc it came from is owned by this slot.
            return Ok(unsafe { &*p });
        }
        let id = self
            .page_id()
            .ok_or_else(|| crate::Error::Persistence("Child: null pointer and NO_PAGE (corrupt slot)".to_string()))?;
        let src = src.ok_or_else(|| {
            crate::Error::Persistence(format!("Child: page {id} referenced but the tree has no NodeSource"))
        })?;
        let fresh = src
            .read_node(id)
            .map_err(|e| crate::Error::Persistence(format!("ultima_db: cannot load page {id} of {}: {e}", src.name())))?;
        let raw = Arc::into_raw(fresh) as *mut BTreeNode<K, V>;
        match self.node.compare_exchange(std::ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => {
                self.mark_accessed();
                // SAFETY: this thread's CAS won; `raw` is the pointer now stored.
                Ok(unsafe { &*raw })
            }
            Err(winner) => {
                // Lost the race: another thread's read got there first. Drop
                // our own decode and use theirs.
                // SAFETY: we own `raw`; nobody else saw it.
                unsafe { drop(Arc::from_raw(raw)) };
                self.mark_accessed();
                // SAFETY: `winner` is non-null (we lost to a successful CAS) and set-once.
                Ok(unsafe { &*winner })
            }
        }
    }

    /// Like [`Self::load`], but hands back an owned `Arc` (bumping the
    /// refcount) instead of a borrow tied to `&self`.
    pub(crate) fn load_arc(&self, src: Option<&dyn NodeSource<K, V>>) -> Arc<BTreeNode<K, V>> {
        self.load(src); // ensure resident; discard the borrow
        let p = self.node.load(Ordering::Acquire) as *const BTreeNode<K, V>;
        // SAFETY: p came straight from the slot's AtomicPtr, i.e. from
        // Arc::into_raw; the slot still owns one count, so the allocation is
        // live. (Deriving the pointer from the `&BTreeNode` that `load`
        // returns instead — provenance read-only, bounded to the payload —
        // is UB: `increment_strong_count` writes through it to the
        // ArcInner's refcount at offset 0, which that borrow's provenance
        // does not cover. Miri rejects that version under both Stacked and
        // Tree Borrows.)
        unsafe {
            Arc::increment_strong_count(p);
            Arc::from_raw(p)
        }
    }

    /// `Some(n)` if resident (`n` = the underlying `Arc`'s strong count,
    /// including the slot's own share), `None` if still on-disk.
    // No production caller yet — a memory-pressure/eviction accounting path
    // is a later task. Used today by this module's own unit tests.
    #[allow(dead_code)]
    pub(crate) fn strong_count(&self) -> Option<usize> {
        let p = self.node.load(Ordering::Acquire);
        if p.is_null() {
            return None;
        }
        // SAFETY: as above; count read without taking ownership (no drop on `a`).
        let a = unsafe { std::mem::ManuallyDrop::new(Arc::from_raw(p)) };
        Some(Arc::strong_count(&a))
    }

    /// Same underlying node: same page id if both are on-disk/resident-clean
    /// (page id is stable identity there), else same `Arc` pointer. Two
    /// resident-dirty nodes are never equal to each other even if their
    /// contents happen to match — dirtiness means no shared identity to compare.
    pub(crate) fn same_node(a: &Self, b: &Self) -> bool {
        match (a.page_id(), b.page_id()) {
            (Some(x), Some(y)) => x == y,
            _ => {
                let (pa, pb) = (a.node.load(Ordering::Acquire), b.node.load(Ordering::Acquire));
                !pa.is_null() && pa == pb
            }
        }
    }

    /// Size estimate reported to `note_dirty`: one node's inline storage.
    pub(crate) const NODE_BYTES: usize = std::mem::size_of::<BTreeNode<K, V>>();
}

impl<K: Clone, V> Child<K, V> {
    /// Get a mutable node, cloning-on-write if it's shared and faulting it
    /// in first if it's on-disk. Always leaves the slot dirty (`NO_PAGE`):
    /// even an in-place edit of a uniquely-owned resident-clean node makes
    /// its contents diverge from the page it was loaded from.
    ///
    /// A block leaf is handed back **as a block leaf** (Task 6). Until the
    /// in-place (`_mut`) mutation family became block-aware this ran the
    /// Task 4 stopgap `BTreeNode::materialize` first, de-blocking every leaf
    /// a write touched; every `&mut` consumer in the tree now keeps
    /// `entries` and `block` in lockstep itself (I-A), so the stopgap — and
    /// with it the sibling `make_mut_keep_block` that existed only to skip
    /// it — is gone.
    pub(crate) fn make_mut(&mut self, src: Option<&dyn NodeSource<K, V>>) -> &mut BTreeNode<K, V> {
        self.load(src);
        self.make_mut_after_load(src)
    }

    /// Like [`Self::make_mut`], but for an already-resident node it does not
    /// touch the accessed bit — used by internal tree-shape bookkeeping
    /// (`BulkBuilder`'s right-spine tail fix-up) that CoWs a node while
    /// *constructing* a tree, not as a real workload access. A genuine
    /// fault (the node isn't resident yet) still marks accessed, same as
    /// any other fault — see [`Self::load_quiet`].
    pub(crate) fn make_mut_quiet(&mut self, src: Option<&dyn NodeSource<K, V>>) -> &mut BTreeNode<K, V> {
        self.load_quiet(src);
        self.make_mut_after_load(src)
    }

    /// Shared tail of [`Self::make_mut`]/[`Self::make_mut_quiet`]: the slot
    /// is already resident (by whichever load the caller used above); clone
    /// it on write and mark it dirty.
    fn make_mut_after_load(&mut self, src: Option<&dyn NodeSource<K, V>>) -> &mut BTreeNode<K, V> {
        let was_clean = self.page_id().is_some();
        let p = *self.node.get_mut();
        // SAFETY: caller already ensured residency (via `load`/`load_quiet`), so p is non-null.
        // Held in `ManuallyDrop` so an unwind out of either clone below cannot
        // release the strong count that `self.node`'s pointer (still `p` at
        // that point) nominally owns — dropping it here *and* again in
        // `Child::drop` would be a double free. `clone_with`'s block branch
        // can panic (I-B violation via a non-cloning source), and even the
        // non-block `Arc::make_mut` branch calls caller-supplied `K::Clone`,
        // which is not guaranteed panic-free.
        let mut arc = std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(p) });
        let raw = if arc.block.is_none() {
            // Non-block node: unchanged from the pre-Value<V> code — `Arc::make_mut`
            // clones (iff shared) directly into the fresh allocation instead of
            // building a whole `BTreeNode` on the stack first. `Arc::make_mut`
            // leaves `*arc` valid and still owning its count if the inner clone
            // panics, so it composes with the `ManuallyDrop` guard exactly like
            // the block branch below.
            Arc::make_mut(&mut arc); // clones iff shared; no-op (in place) if unique
            Arc::into_raw(std::mem::ManuallyDrop::into_inner(arc)) as *mut BTreeNode<K, V>
        } else if Arc::get_mut(&mut arc).is_some() {
            // Unique owner (block leaf included): mutate in place, pointer identity preserved.
            //
            // NOTE: `Arc::get_mut(..).is_some()` is not exactly `Arc::make_mut`'s
            // uniqueness condition — they diverge when `strong == 1 && weak > 1`
            // (`make_mut` moves into a fresh allocation without cloning; `get_mut`
            // returns `None` here, so this falls into the clone branch below).
            // Unobservable today: nothing in this crate ever creates a
            // `Weak<BTreeNode<K, V>>`. Worth revisiting if that ever changes.
            Arc::into_raw(std::mem::ManuallyDrop::into_inner(arc)) as *mut BTreeNode<K, V>
        } else {
            // Shared block leaf: plain `Clone` can't duplicate a value block
            // (no `V: Clone` bound on the tree) — go through `clone_with`,
            // which routes block values via the source.
            let fresh = Arc::new(arc.clone_with(src)); // may panic; `arc` is not dropped if it does
            drop(std::mem::ManuallyDrop::into_inner(arc)); // release the slot's old share, exactly once
            Arc::into_raw(fresh) as *mut BTreeNode<K, V>
        };
        *self.node.get_mut() = raw;
        // Whether cloned or edited in place, the contents now diverge from the page.
        *self.meta.get_mut() = (*self.meta.get_mut() & ACCESSED) | NO_PAGE;
        if was_clean
            && let Some(s) = src
        {
            // Task 8 (spec §4): a block leaf's real cost is
            // `leaf_bytes()`, not the flat `NODE_BYTES` a non-block node
            // still credits — `raw` is the post-mutation node, already in
            // hand. See the matching note on `resident_new` above.
            // SAFETY: `raw` is the pointer just stored in `self.node`, non-null.
            let node_ref = unsafe { &*raw };
            let bytes = if node_ref.block.is_some() { node_ref.leaf_bytes() } else { Self::NODE_BYTES };
            s.note_dirty(bytes);
        }
        // SAFETY: raw is the pointer just stored in `self.node`, non-null, uniquely owned by `arc`.
        //
        // Handed out exactly as it is: a block leaf stays a block leaf. Every
        // caller that edits one — `btree`'s in-place `_mut` family and the
        // shared rebalance path (`rotate_*`/`merge_*`/`absorb`) — rebuilds or
        // stores into the block itself and so keeps I-A (`entries` and
        // `block` in lockstep). Task 4's `materialize()` stopgap used to run
        // here for the benefit of the then-block-unaware `_mut` family; Task
        // 6 removed both it and the `make_mut_keep_block` variant that had to
        // opt out of it.
        unsafe { &mut *raw }
    }
}

impl<K, V> Clone for Child<K, V> {
    /// An on-disk slot clones by copying `meta` alone — no child is
    /// touched, no refcount bumped. A resident slot bumps the `Arc`'s
    /// strong count, same as cloning the `Arc` directly would.
    fn clone(&self) -> Self {
        let p = self.node.load(Ordering::Acquire);
        if !p.is_null() {
            // SAFETY: slot owns a count on p; bump it for the new slot's share.
            unsafe { Arc::increment_strong_count(p) };
        }
        Self {
            meta: AtomicU64::new(self.meta.load(Ordering::Acquire)),
            node: AtomicPtr::new(p),
            _own: PhantomData,
        }
    }
}

impl<K, V> Drop for Child<K, V> {
    fn drop(&mut self) {
        let p = *self.node.get_mut();
        if !p.is_null() {
            // SAFETY: slot owns exactly one count; dropping it releases that share.
            unsafe { drop(Arc::from_raw(p)) };
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::btree::BTreeNode;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Barrier, Mutex};

    /// In-memory "disk": page id -> node. Counts reads.
    pub(crate) struct MockDisk<K, V> {
        pub pages: Mutex<HashMap<PageId, Arc<BTreeNode<K, V>>>>,
        pub reads: AtomicUsize,
        pub dirty_bytes: AtomicUsize,
        /// Number of `clone_value` calls — the block-leaf CoW must clone
        /// exactly one value per entry, no more, no less.
        pub cloned: AtomicU64,
    }
    impl<K: Clone + Send + Sync, V: Send + Sync + Copy> NodeSource<K, V> for MockDisk<K, V> {
        fn read_node(&self, id: PageId) -> crate::Result<Arc<BTreeNode<K, V>>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let pages = self.pages.lock().unwrap();
            let n = pages.get(&id).ok_or_else(|| crate::Error::Persistence(format!("no page {id}")))?;
            // A fresh Arc, like a real decode. Goes through `clone_with`
            // (not plain `Clone`) so a block leaf can be faulted in from
            // "disk" too — a real decode never clones at all (it builds a
            // fresh node straight from bytes), but reusing `clone_with` is
            // the simplest faithful "fresh copy" this mock needs; for a
            // non-block node it's exactly the old plain-`Clone` behavior
            // (`clone_with`'s `None` branch is `self.clone()`).
            Ok(Arc::new(n.clone_with(Some(self))))
        }
        fn note_dirty(&self, bytes: usize) {
            self.dirty_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        fn clone_value(&self, v: &V) -> Option<V> {
            self.cloned.fetch_add(1, Ordering::Relaxed);
            Some(*v)
        }
    }
    impl<K, V> MockDisk<K, V> {
        pub fn new() -> Self {
            Self {
                pages: Mutex::new(HashMap::new()),
                reads: AtomicUsize::new(0),
                dirty_bytes: AtomicUsize::new(0),
                cloned: AtomicU64::new(0),
            }
        }
        pub fn put(&self, id: PageId, n: Arc<BTreeNode<K, V>>) {
            self.pages.lock().unwrap().insert(id, n);
        }
    }

    fn leaf(keys: &[u64]) -> Arc<BTreeNode<u64, u64>> {
        Arc::new(BTreeNode {
            entries: keys.iter().map(|k| (*k, Value::arc(Arc::new(*k * 10)))).collect(),
            children: Default::default(),
            block: None,
        })
    }

    /// A leaf whose values live in `block` rather than behind per-entry
    /// `Arc`s — the shape `clone_with`/`clone_value` exist to CoW.
    fn block_leaf(keys: &[u64]) -> Arc<BTreeNode<u64, u64>> {
        let values: Vec<u64> = keys.iter().map(|k| k * 10).collect();
        Arc::new(BTreeNode {
            entries: keys.iter().map(|k| (*k, Value::in_block())).collect(),
            children: Default::default(),
            block: Some(values.into_boxed_slice()),
        })
    }

    #[test]
    fn resident_slot_reports_no_page_and_loaded() {
        let c = Child::resident(leaf(&[1]));
        assert_eq!(c.page_id(), None);
        assert!(c.is_loaded());
        assert_eq!(c.strong_count(), Some(1));
    }

    /// `resident_new` credits `note_dirty` for a brand-new node when a
    /// source is attached (task12 fix round 1) — the case `resident` alone
    /// (and, before this fix, every btree.rs creation site) could never
    /// report: a newly split/rebuilt node was never on disk, so it has no
    /// clean-to-dirty *transition* for `make_mut` to notice.
    #[test]
    fn resident_new_credits_dirty_bytes_when_src_is_some() {
        let disk = MockDisk::new();
        let c: Child<u64, u64> = Child::resident_new(leaf(&[1]), Some(&disk));
        assert_eq!(c.page_id(), None);
        assert!(c.is_loaded());
        assert_eq!(
            disk.dirty_bytes.load(Ordering::Relaxed),
            Child::<u64, u64>::NODE_BYTES,
            "a brand-new node must be credited exactly once, at NODE_BYTES"
        );
    }

    /// The other half of the same fix: an unattached tree (`src: None`,
    /// e.g. a fresh `from_sorted` build with no checkpoint yet) must credit
    /// nothing — there is no checkpoint to owe these bytes to.
    #[test]
    fn resident_new_credits_nothing_when_src_is_none() {
        let c: Child<u64, u64> = Child::resident_new(leaf(&[1]), None);
        assert_eq!(c.page_id(), None);
        assert!(c.is_loaded());
        // No disk to check a counter on — the only assertion is that this
        // doesn't panic on a `None` source, which it would if `resident_new`
        // unwrapped `src` instead of checking it.
    }

    #[test]
    fn on_disk_slot_loads_once_and_counts_one_read() {
        let disk = MockDisk::new();
        disk.put(4096, leaf(&[7, 8]));
        let c: Child<u64, u64> = Child::on_disk(4096);
        assert!(!c.is_loaded());
        let n = c.load(Some(&disk));
        assert_eq!(n.entries.len(), 2);
        let _again = c.load(Some(&disk));
        assert_eq!(disk.reads.load(Ordering::Relaxed), 1, "second load must hit the pointer, not the disk");
        assert_eq!(c.page_id(), Some(4096), "loading keeps the page id (resident-clean)");
    }

    #[test]
    fn clone_of_on_disk_slot_touches_nothing() {
        let disk = MockDisk::new();
        disk.put(1, leaf(&[1]));
        let c: Child<u64, u64> = Child::on_disk(1);
        let d = c.clone();
        assert_eq!(disk.reads.load(Ordering::Relaxed), 0);
        assert!(!d.is_loaded());
        assert_eq!(d.page_id(), Some(1));
    }

    #[test]
    fn clone_of_loaded_slot_bumps_refcount() {
        let c = Child::resident(leaf(&[1]));
        let d = c.clone();
        assert_eq!(c.strong_count(), Some(2));
        drop(d);
        assert_eq!(c.strong_count(), Some(1));
    }

    #[test]
    fn make_mut_on_shared_node_clones_and_marks_dirty() {
        let disk = MockDisk::new();
        disk.put(1, leaf(&[1, 2]));
        let mut c: Child<u64, u64> = Child::on_disk(1);
        c.load(Some(&disk));
        let keep = c.load_arc(Some(&disk)); // second owner
        let n = c.make_mut(Some(&disk));
        n.entries.push((3, Value::arc(Arc::new(30))));
        assert_eq!(c.page_id(), None, "a CoW'd node is dirty");
        assert_eq!(keep.entries.len(), 2, "old owner unaffected");
        assert!(disk.dirty_bytes.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn make_mut_on_unique_node_mutates_in_place_and_stays_clean_until_edited() {
        // Uniquely owned resident-clean node: make_mut must still mark dirty
        // (the node's contents are about to diverge from its page).
        let disk = MockDisk::new();
        disk.put(1, leaf(&[1]));
        let mut c: Child<u64, u64> = Child::on_disk(1);
        c.load(Some(&disk));
        let before = c.load_arc(Some(&disk));
        let before_ptr = Arc::as_ptr(&before);
        drop(before);
        let n = c.make_mut(Some(&disk));
        assert_eq!(n as *const _, before_ptr, "unique owner: in place");
        assert_eq!(c.page_id(), None);
    }

    #[test]
    fn make_mut_unique_block_leaf_is_in_place() {
        // A uniquely-owned block leaf's make_mut must take the `Arc::get_mut`
        // fast path — no `clone_with`/`clone_value` call at all.
        let disk = MockDisk::new();
        let mut c: Child<u64, u64> = Child::resident(block_leaf(&[1, 2]));
        let before_ptr = c.load(Some(&disk)) as *const _;
        let n = c.make_mut(Some(&disk));
        assert_eq!(n as *const _, before_ptr, "unique owner: in place");
        for (i, k) in [1u64, 2].into_iter().enumerate() {
            assert_eq!(*n.value_at(i), k * 10, "block content unchanged by the in-place path");
        }
        // Task 6: `make_mut` hands the block back *as a block*. Until the
        // in-place (`_mut`) family became block-aware it ran the Task 4
        // stopgap `BTreeNode::materialize` here and de-blocked the leaf on
        // first write — right answers, no memory-honesty win.
        assert!(n.block.is_some(), "make_mut must not de-block a block leaf");
        // `n`'s mutable borrow of `c` ends at its last use above; only now
        // can `c` be borrowed again (immutably) below.
        assert_eq!(disk.cloned.load(Ordering::Relaxed), 0, "no clone on the unique-owner path");
        assert_eq!(c.page_id(), None);
    }

    #[test]
    fn make_mut_shared_block_leaf_clones_via_source() {
        // A block leaf shared with a second `Arc` owner can't take
        // `Arc::get_mut`'s fast path, and plain `Clone` can't duplicate a
        // block (no `V: Clone`) — make_mut must route through
        // `clone_with`/`NodeSource::clone_value`, once per block entry.
        let disk = MockDisk::new();
        let node = block_leaf(&[1, 2, 3]);
        let before_ptr = Arc::as_ptr(&node);
        let mut c: Child<u64, u64> = Child::resident(node.clone()); // `node` is the second owner
        let n = c.make_mut(Some(&disk));
        assert_ne!(n as *const _, before_ptr, "shared owner: cloned, not mutated in place");
        for (i, k) in [1u64, 2, 3].into_iter().enumerate() {
            assert_eq!(*n.value_at(i), k * 10, "cloned block preserves values and order");
            assert_eq!(*node.value_at(i), k * 10, "the original (surviving) node is unmutated");
        }
        assert_eq!(disk.cloned.load(Ordering::Relaxed), 3, "one clone_value call per block entry");
        assert!(c.load(Some(&disk)).block.is_some(), "the CoW'd copy is still a block leaf (Task 6)");
        assert_eq!(c.page_id(), None, "a CoW'd node is dirty");
        drop(node);
    }

    #[test]
    fn make_mut_shared_block_leaf_on_disk_marks_dirty() {
        // The `was_clean -> note_dirty` tail (already covered for a plain
        // node by `make_mut_on_shared_node_clones_and_marks_dirty`) must
        // fire for a block leaf too.
        let disk = MockDisk::new();
        disk.put(1, block_leaf(&[1, 2, 3]));
        let mut c: Child<u64, u64> = Child::on_disk(1);
        c.load(Some(&disk));
        // `MockDisk::read_node`'s own "fresh decode" simulation goes through
        // `clone_with` too (see its comment), so faulting in already cost 3
        // `clone_value` calls; only the delta from here on is the CoW's cost.
        let cloned_before_cow = disk.cloned.load(Ordering::Relaxed);
        let keep = c.load_arc(Some(&disk)); // second owner: forces the clone branch
        let n = c.make_mut(Some(&disk));
        for (i, k) in [1u64, 2, 3].into_iter().enumerate() {
            assert_eq!(*n.value_at(i), k * 10);
        }
        assert_eq!(keep.entries.len(), 3, "old owner unaffected");
        assert!(disk.dirty_bytes.load(Ordering::Relaxed) > 0, "was_clean -> note_dirty must fire for a block leaf");
        assert_eq!(
            disk.cloned.load(Ordering::Relaxed) - cloned_before_cow,
            3,
            "one clone_value call per block entry during the CoW"
        );
        assert_eq!(c.page_id(), None, "a CoW'd node is dirty");
    }

    #[test]
    fn make_mut_quiet_shared_block_leaf_clones_via_source() {
        // `make_mut_quiet` is used by `BulkBuilder`'s right-spine fix-up
        // (`btree.rs:2886`-ish) — it must also route a shared block leaf CoW
        // through `clone_with`, not `Arc::make_mut`, same as `make_mut`.
        let disk = MockDisk::new();
        let node = block_leaf(&[5, 6]);
        let before_ptr = Arc::as_ptr(&node);
        let mut c: Child<u64, u64> = Child::resident(node.clone()); // `node` is the second owner
        assert!(!c.take_accessed(), "resident starts with the accessed bit clear");
        let n = c.make_mut_quiet(Some(&disk));
        assert_ne!(n as *const _, before_ptr, "shared owner: cloned via source");
        for (i, k) in [5u64, 6].into_iter().enumerate() {
            assert_eq!(*n.value_at(i), k * 10);
        }
        assert_eq!(disk.cloned.load(Ordering::Relaxed), 2, "one clone_value call per block entry");
        assert!(
            !c.take_accessed(),
            "make_mut_quiet must not mark accessed on an already-resident node"
        );
        drop(node);
    }

    #[test]
    fn make_mut_panic_during_clone_with_does_not_double_free() {
        // Regression for the double-free the pre-`ManuallyDrop`
        // `make_mut_after_load` had (found in review of this task): a
        // `NodeSource` that violates I-B — holds a block leaf but its
        // `clone_value` returns `None` (the trait default, left unwired
        // here on purpose) — makes `clone_with` panic mid-clone. Before the
        // `ManuallyDrop` guard, that panic-unwind released the slot's strong
        // count early and the drops below then double-freed it (observed as
        // glibc "corrupted double-linked list", SIGABRT, killing the whole
        // test process rather than failing this one test). After the fix,
        // the panic is just a panic: both drops below run cleanly and this
        // test passes.
        struct NonCloningDisk;
        impl NodeSource<u64, u64> for NonCloningDisk {
            fn read_node(&self, _id: PageId) -> crate::Result<Arc<BTreeNode<u64, u64>>> {
                unreachable!("not exercised by this test")
            }
            // `clone_value` intentionally left at the trait default (`None`).
        }
        let disk = NonCloningDisk;
        let node = block_leaf(&[1, 2]);
        let mut c: Child<u64, u64> = Child::resident(node.clone()); // second owner: forces the clone branch
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            c.make_mut(Some(&disk));
        }));
        assert!(result.is_err(), "must panic: I-B violated (non-cloning source holds a block leaf)");
        // Neither drop below must double-free: `c`'s slot still owns exactly
        // its one legitimate share of the original node; `node` owns the other.
        drop(c);
        drop(node);
    }

    #[test]
    fn accessed_bit_is_second_chance() {
        let c = Child::resident(leaf(&[1]));
        assert!(!c.take_accessed());
        c.mark_accessed();
        assert!(c.take_accessed());
        assert!(!c.take_accessed());
    }

    #[test]
    fn concurrent_fault_in_leaves_exactly_one_arc() {
        let disk = Arc::new(MockDisk::new());
        disk.put(9, leaf(&[1]));
        let c: Arc<Child<u64, u64>> = Arc::new(Child::on_disk(9));
        // Line every thread up at the gate so all 8 race `fault_in` together —
        // without this, thread 0 can win (and publish the pointer) before
        // thread 7 is even spawned, and the loser-drop path never executes.
        let barrier = Arc::new(Barrier::new(8));
        let hs: Vec<_> = (0..8)
            .map(|_| {
                let c = c.clone();
                let d = disk.clone();
                let b = barrier.clone();
                std::thread::spawn(move || {
                    b.wait();
                    c.load(Some(&*d));
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(c.strong_count(), Some(1), "losers must drop their copies");
        assert!(disk.reads.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn set_page_id_preserves_accessed_bit() {
        // set_page_id only touches the id bits; a second-chance bit set
        // before the call must survive it.
        let c = Child::resident(leaf(&[1]));
        c.mark_accessed();
        c.set_page_id(4096);
        assert_eq!(c.page_id(), Some(4096));
        assert!(c.take_accessed(), "set_page_id must preserve a pre-existing accessed bit");
        assert!(!c.take_accessed());
    }

    #[test]
    fn set_page_id_terminates_under_concurrent_mark_accessed() {
        // set_page_id's CAS loop retries whenever `meta` changes underneath
        // it; a thread hammering mark_accessed (also a `meta` RMW) must not
        // starve it out.
        let c = Arc::new(Child::<u64, u64>::resident(leaf(&[1])));
        let stop = Arc::new(AtomicBool::new(false));
        let h = {
            let c = c.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    c.mark_accessed();
                }
            })
        };
        c.set_page_id(4096);
        stop.store(true, Ordering::Relaxed);
        h.join().unwrap();
        assert_eq!(c.page_id(), Some(4096), "the id must land despite the concurrent accessed-bit writer");
    }

    #[test]
    #[should_panic(expected = "page 77")]
    fn load_failure_panics_with_page_id() {
        let disk: MockDisk<u64, u64> = MockDisk::new();
        let c: Child<u64, u64> = Child::on_disk(77);
        c.load(Some(&disk));
    }

    #[test]
    fn same_node_by_page_or_pointer() {
        let a: Child<u64, u64> = Child::on_disk(5);
        let b: Child<u64, u64> = Child::on_disk(5);
        assert!(Child::same_node(&a, &b));
        let n = leaf(&[1]);
        let c = Child::resident(n.clone());
        let d = Child::resident(n);
        assert!(Child::same_node(&c, &d));
        let e = Child::resident(leaf(&[1]));
        assert!(!Child::same_node(&c, &e), "dirty nodes equal nothing but themselves");
    }
}
