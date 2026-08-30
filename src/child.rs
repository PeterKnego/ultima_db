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

// Not wired into `BTree`/`BTreeNode` yet — that's the next task in the
// paged-btree plan, which is the sole consumer of this module's API.
#![allow(dead_code)]

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

use crate::btree::BTreeNode;

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

    /// A slot that references a page but has not been faulted in yet.
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

    #[cold]
    fn fault_in(&self, src: Option<&dyn NodeSource<K, V>>) -> &BTreeNode<K, V> {
        let id = self.page_id().expect("Child: null pointer and NO_PAGE (corrupt slot)");
        let src = src.unwrap_or_else(|| panic!("Child: page {id} referenced but the tree has no NodeSource"));
        let fresh = match src.read_node(id) {
            Ok(n) => n,
            Err(e) => panic!("ultima_db: cannot load page {id} of {}: {e}", src.name()),
        };
        let raw = Arc::into_raw(fresh) as *mut BTreeNode<K, V>;
        match self.node.compare_exchange(std::ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => {
                self.mark_accessed();
                // SAFETY: this thread's CAS won; `raw` is the pointer now stored.
                unsafe { &*raw }
            }
            Err(winner) => {
                // Lost the race: another thread's read got there first. Drop
                // our own decode and use theirs.
                // SAFETY: we own `raw`; nobody else saw it.
                unsafe { drop(Arc::from_raw(raw)) };
                self.mark_accessed();
                // SAFETY: `winner` is non-null (we lost to a successful CAS) and set-once.
                unsafe { &*winner }
            }
        }
    }

    /// Like [`Self::load`], but hands back an owned `Arc` (bumping the
    /// refcount) instead of a borrow tied to `&self`.
    pub(crate) fn load_arc(&self, src: Option<&dyn NodeSource<K, V>>) -> Arc<BTreeNode<K, V>> {
        let p = self.load(src) as *const BTreeNode<K, V>;
        // SAFETY: p came from Arc::into_raw and the slot still owns one count.
        unsafe {
            Arc::increment_strong_count(p);
            Arc::from_raw(p)
        }
    }

    /// `Some(n)` if resident (`n` = the underlying `Arc`'s strong count,
    /// including the slot's own share), `None` if still on-disk.
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
    pub(crate) fn make_mut(&mut self, src: Option<&dyn NodeSource<K, V>>) -> &mut BTreeNode<K, V> {
        self.load(src);
        let was_clean = self.page_id().is_some();
        let p = *self.node.get_mut();
        // SAFETY: we hold &mut self, so no concurrent CAS can be racing; p is non-null after load.
        let mut arc = unsafe { Arc::from_raw(p) };
        Arc::make_mut(&mut arc); // clones iff shared; no-op (in place) if unique
        let raw = Arc::into_raw(arc) as *mut BTreeNode<K, V>;
        *self.node.get_mut() = raw;
        // Whether cloned or edited in place, the contents now diverge from the page.
        *self.meta.get_mut() = (*self.meta.get_mut() & ACCESSED) | NO_PAGE;
        if was_clean
            && let Some(s) = src
        {
            s.note_dirty(Self::NODE_BYTES);
        }
        // SAFETY: raw is the pointer just stored in `self.node`, non-null, uniquely owned by `arc`.
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    /// In-memory "disk": page id -> node. Counts reads.
    pub(crate) struct MockDisk<K, V> {
        pub pages: Mutex<HashMap<PageId, Arc<BTreeNode<K, V>>>>,
        pub reads: AtomicUsize,
        pub dirty_bytes: AtomicUsize,
    }
    impl<K: Clone + Send + Sync, V: Send + Sync> NodeSource<K, V> for MockDisk<K, V> {
        fn read_node(&self, id: PageId) -> crate::Result<Arc<BTreeNode<K, V>>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let pages = self.pages.lock().unwrap();
            let n = pages.get(&id).ok_or_else(|| crate::Error::Persistence(format!("no page {id}")))?;
            // A fresh Arc, like a real decode.
            Ok(Arc::new((**n).clone()))
        }
        fn note_dirty(&self, bytes: usize) {
            self.dirty_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }
    impl<K, V> MockDisk<K, V> {
        pub fn new() -> Self {
            Self { pages: Mutex::new(HashMap::new()), reads: AtomicUsize::new(0), dirty_bytes: AtomicUsize::new(0) }
        }
        pub fn put(&self, id: PageId, n: Arc<BTreeNode<K, V>>) {
            self.pages.lock().unwrap().insert(id, n);
        }
    }

    fn leaf(keys: &[u64]) -> Arc<BTreeNode<u64, u64>> {
        Arc::new(BTreeNode { entries: keys.iter().map(|k| (*k, Arc::new(*k * 10))).collect(), children: Default::default() })
    }

    #[test]
    fn resident_slot_reports_no_page_and_loaded() {
        let c = Child::resident(leaf(&[1]));
        assert_eq!(c.page_id(), None);
        assert!(c.is_loaded());
        assert_eq!(c.strong_count(), Some(1));
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
        n.entries.push((3, Arc::new(30)));
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
        let hs: Vec<_> = (0..8)
            .map(|_| {
                let c = c.clone();
                let d = disk.clone();
                std::thread::spawn(move || {
                    c.load(Some(&*d));
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(c.strong_count(), Some(1), "losers must drop their copies");
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
