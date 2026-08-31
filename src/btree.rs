// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::cmp::Ordering;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::child::{Child, NO_PAGE, NodeSource, PageId};
use crate::{Error, Result};

// Minimum degree: every non-root node has at least T-1 keys, at most 2T-1 keys.
//
// T=32 default / T=8 behind `fanout-t8`, per the 2026-07-18 NVMe A/B
// (docs/benchmarks/smr-fanout-fixedvec-nvme-2026-07-18.md): with the FixedVec
// inline node layout, T=32 is the balanced point — vs the old T=64 default it
// gives 1.6x contended write throughput and better read-p99-under-load, at
// +11% on uncontended gets. Smaller T keeps winning contended writes (T=8 is
// 2.85x) because the CoW commit path clones the full root-to-leaf path and
// clone cost scales with node width, but T=8 costs ~2x on reads under load
// and +25% on uncontended gets — a specialist trade for write-dominated SMR
// deployments, so it ships as an opt-in feature, not the default.
#[cfg(feature = "fanout-t8")]
const T: usize = 8;
#[cfg(not(feature = "fanout-t8"))]
const T: usize = 32;
const MIN_KEYS: usize = T - 1;
const MAX_KEYS: usize = 2 * T - 1;

// ---------------------------------------------------------------------------
// Fixed-capacity inline vector (private) — backs BTreeNode's entries/children
// ---------------------------------------------------------------------------

/// Fixed-capacity, inline vector-like container: the backing storage
/// `[MaybeUninit<E>; N]` lives directly in the struct, so cloning/dropping
/// one never touches the heap on its own — no separate allocation the way a
/// `Vec<E>` field would need. Storing `MaybeUninit<E>` instead of `Option<E>`
/// matters for element types with no spare-bit niche: `Child<K, V>`
/// (`AtomicU64` + `AtomicPtr`, see `src/child.rs`) has none — `AtomicPtr`
/// wraps an `UnsafeCell`, so `Option<Child<K, V>>` costs a whole extra word
/// (24 B instead of 16 B) per slot, tripling the size of every inner node's
/// `children` array. `(K, Arc<V>)` already had a niche via `Arc`'s non-null
/// pointer, so this change costs that element type nothing.
///
/// Invariant, load-bearing for every method below: slots `[0, len)` are
/// always initialized `E`; slots `[len, N)` are always uninitialized and
/// must never be read (`assume_init*`) or dropped. Because `MaybeUninit`
/// itself has no `Drop` glue, `FixedVec` now needs a manual `Drop` impl (the
/// `Option` layout got this for free from `Option<E>: Drop`).
///
/// `N` carries one slot of headroom beyond the node's steady-state max
/// (`MAX_KEYS` entries / `MAX_KEYS + 1` children — see `Entries`/`Children`
/// below): the insert-then-maybe-split pattern used by `insert_into_node`
/// and, especially, the in-place `insert_into_node_mut` / `maybe_split_mut`
/// (which mutates the already-`Arc::make_mut`'d node directly, with no
/// separate scratch buffer) inserts one element before checking for
/// overflow, so it transiently holds `MAX_KEYS + 1` entries / `MAX_KEYS + 2`
/// children. Everywhere else `len` stays within the steady-state max.
pub(crate) struct FixedVec<E, const N: usize> {
    data: [std::mem::MaybeUninit<E>; N],
    len: u8,
}

impl<E, const N: usize> FixedVec<E, N> {
    // `len` is a u8, so capacities above 255 would silently wrap and corrupt
    // the tree (observed as bogus results in the T=128 fanout sweep, where
    // MAX_KEYS + 1 = 256). Reject them at compile time.
    const _CAP_FITS_U8: () = assert!(N <= u8::MAX as usize);

    fn new() -> Self {
        #[allow(clippy::let_unit_value)]
        let _ = Self::_CAP_FITS_U8;
        FixedVec {
            data: [const { std::mem::MaybeUninit::uninit() }; N],
            len: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len as usize
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The live prefix `[0, len)` as an initialized slice.
    fn as_slice(&self) -> &[E] {
        // SAFETY: invariant — [0, len) are always initialized `E`. A
        // `MaybeUninit<E>` slice prefix that is fully initialized may be
        // reinterpreted as `&[E]` (same layout, `MaybeUninit<E>` is
        // `#[repr(transparent)]`-equivalent to `E` for this purpose).
        unsafe { std::slice::from_raw_parts(self.data.as_ptr().cast::<E>(), self.len as usize) }
    }

    /// Mutable view of the live prefix, for `split_at_mut`-based disjoint
    /// access to two elements at once (see `rotate_right`/`rotate_left`,
    /// which need two adjacent sibling children mutably at the same time).
    fn as_mut_slice(&mut self) -> &mut [E] {
        // SAFETY: same invariant as `as_slice`; `&mut self` gives unique
        // access, so no aliasing with any other view of these slots.
        unsafe { std::slice::from_raw_parts_mut(self.data.as_mut_ptr().cast::<E>(), self.len as usize) }
    }

    fn last(&self) -> Option<&E> {
        self.as_slice().last()
    }

    pub(crate) fn push(&mut self, item: E) {
        let i = self.len as usize;
        // A release-mode capacity check, not just a debug one: `write`
        // below trusts i < N to stay in bounds — in release that would be a
        // write past the array (see the `insert`/`remove`/`split_off`
        // asserts above for the same release-UB class).
        assert!(i < N, "FixedVec::push: at capacity");
        // SAFETY: i == len < N (checked above), so slot i is the first
        // uninitialized slot; `write` overwrites it without dropping
        // whatever (uninitialized) bytes were there.
        self.data[i].write(item);
        self.len += 1;
    }

    fn pop(&mut self) -> Option<E> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        let i = self.len as usize;
        // SAFETY: slot i was live (i < old len) and `len` no longer covers
        // it, so it is read out exactly once here and never read or dropped
        // again.
        Some(unsafe { self.data[i].assume_init_read() })
    }

    /// Shift `[idx, len)` right by one slot and insert `item` at `idx`.
    fn insert(&mut self, idx: usize, item: E) {
        let n = self.len as usize;
        // A release-mode bounds/capacity check, not just a debug one: the
        // `ptr::copy`/`write` below trust `idx <= n < N` to stay in bounds
        // and to never read an uninitialized slot into the live range, so
        // this must actually hold outside debug builds too.
        assert!(idx <= n && n < N, "FixedVec::insert: out of bounds or at capacity");
        if idx < n {
            // SAFETY: the assert above establishes idx <= n < N. [idx, n)
            // are initialized and n < N, so [idx+1, n+1) is in bounds;
            // `ptr::copy` moves them as raw bytes (valid for
            // `MaybeUninit<E>`, which permits overlapping/uninitialized
            // regions) without invoking `E`'s `Clone`/`Drop`. The source and
            // destination ranges overlap by construction, hence `copy` (not
            // `copy_nonoverlapping`).
            unsafe {
                let base = self.data.as_mut_ptr();
                std::ptr::copy(base.add(idx), base.add(idx + 1), n - idx);
            }
        }
        // SAFETY: the assert above establishes idx <= n < N, so slot idx is
        // in bounds. It now holds either the old (already-relocated,
        // logically vacated) bytes of the shifted range or was already the
        // first uninitialized slot; `write` installs `item` there without
        // dropping either.
        self.data[idx].write(item);
        self.len += 1;
    }

    /// Remove and return the element at `idx`, shifting `(idx, len)` left by one.
    fn remove(&mut self, idx: usize) -> E {
        let n = self.len as usize;
        // A release-mode bounds check, not just a debug one: `assume_init_read`
        // below trusts idx < n to be reading an initialized slot, and the
        // `ptr::copy` after it trusts the same bound to stay in range.
        assert!(idx < n, "FixedVec::remove: out of bounds");
        // SAFETY: the assert above establishes idx < n, so slot idx is
        // initialized; read it out before the shift below overwrites its
        // bytes.
        let removed = unsafe { self.data[idx].assume_init_read() };
        if idx + 1 < n {
            // SAFETY: idx < n (asserted above), so [idx+1, n) are
            // initialized; shifting them left over the now-logically-vacated
            // slot idx is a raw-byte move (valid for `MaybeUninit<E>`) that
            // neither clones nor drops.
            unsafe {
                let base = self.data.as_mut_ptr();
                std::ptr::copy(base.add(idx + 1), base.add(idx), n - idx - 1);
            }
        }
        self.len -= 1;
        removed
    }

    /// Truncate to `[0, at)`, returning the removed `[at, len)` tail as a new
    /// `FixedVec`. Moves elements out (no cloning) — the in-place counterpart
    /// of the immutable path's old `to_vec()`-based split.
    fn split_off(&mut self, at: usize) -> Self {
        let n = self.len as usize;
        // A release-mode bound, not just a debug one: `count = n - at` below
        // wraps (usize underflow) if at > n, which would then feed a huge
        // length into `copy_nonoverlapping`.
        assert!(at <= n, "FixedVec::split_off: out of bounds");
        let mut out = Self::new();
        let count = n - at;
        // SAFETY: the assert above establishes at <= n, so `count` cannot
        // wrap. [at, n) are initialized in `self`; `out.data[0, count)` is
        // freshly allocated and uninitialized, and `self`/`out` are distinct
        // allocations (disjoint), so `copy_nonoverlapping` is valid. This
        // moves the elements' bytes into `out` without invoking `Clone`;
        // `self.len` is shrunk to `at` right after so `[at, n)` in `self` is
        // never read or dropped again (ownership transferred to `out`).
        unsafe {
            std::ptr::copy_nonoverlapping(self.data.as_ptr().add(at).cast::<E>(), out.data.as_mut_ptr().cast::<E>(), count);
        }
        out.len = count as u8;
        self.len = at as u8;
        out
    }

    fn extend<I: IntoIterator<Item = E>>(&mut self, iter: I) {
        for item in iter {
            self.push(item);
        }
    }

    /// Iterate the live prefix.
    fn iter(&self) -> impl Iterator<Item = &E> + '_ {
        self.as_slice().iter()
    }

    /// Mutably iterate the live prefix — used by the paging primitives to
    /// flip `Child` slots (e.g. residency in `demote_leaves`) in place
    /// without a full node rebuild.
    fn iter_mut(&mut self) -> impl Iterator<Item = &mut E> + '_ {
        self.as_mut_slice().iter_mut()
    }

    fn to_vec(&self) -> Vec<E>
    where
        E: Clone,
    {
        self.iter().cloned().collect()
    }

    fn binary_search_by<F>(&self, f: F) -> std::result::Result<usize, usize>
    where
        F: FnMut(&E) -> std::cmp::Ordering,
    {
        self.as_slice().binary_search_by(f)
    }

    fn partition_point<F>(&self, pred: F) -> usize
    where
        F: FnMut(&E) -> bool,
    {
        self.as_slice().partition_point(pred)
    }
}

// Bounded on `E: Clone` only (same shape as `BTreeNode`'s own manual `Clone`
// below). Built via a fresh `FixedVec` and `push` rather than a bulk array
// clone: if some element's `Clone` panics partway through, the partial
// `out` built so far is a well-formed `FixedVec` whose own `Drop` cleans up
// exactly the elements already cloned — no leak, no double-drop of `self`'s
// originals (which `clone` never touched).
impl<E: Clone, const N: usize> Clone for FixedVec<E, N> {
    fn clone(&self) -> Self {
        let mut out = Self::new();
        for item in self.as_slice() {
            out.push(item.clone());
        }
        out
    }
}

// An empty `FixedVec` — no bound on `E` needed, `new()` never touches an
// element. Lets a leaf `BTreeNode` be built with `children: Default::default()`
// without spelling out the capacity const generic.
impl<E, const N: usize> Default for FixedVec<E, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E, const N: usize> Drop for FixedVec<E, N> {
    fn drop(&mut self) {
        // SAFETY: invariant — exactly [0, len) are initialized; each is
        // dropped in place exactly once here. [len, N) is never touched.
        for slot in &mut self.data[..self.len as usize] {
            unsafe { std::ptr::drop_in_place(slot.as_mut_ptr()) };
        }
    }
}

impl<E, const N: usize> std::ops::Index<usize> for FixedVec<E, N> {
    type Output = E;
    fn index(&self, idx: usize) -> &E {
        assert!(idx < self.len as usize, "FixedVec: index out of live range");
        // SAFETY: idx < len, just asserted, so slot idx is initialized.
        unsafe { self.data[idx].assume_init_ref() }
    }
}

impl<E, const N: usize> std::ops::IndexMut<usize> for FixedVec<E, N> {
    fn index_mut(&mut self, idx: usize) -> &mut E {
        assert!(idx < self.len as usize, "FixedVec: index out of live range");
        // SAFETY: idx < len, just asserted, so slot idx is initialized.
        unsafe { self.data[idx].assume_init_mut() }
    }
}

impl<E, const N: usize> FromIterator<E> for FixedVec<E, N> {
    fn from_iter<I: IntoIterator<Item = E>>(iter: I) -> Self {
        let mut out = Self::new();
        out.extend(iter);
        out
    }
}

/// Owns a `FixedVec`'s storage and yields `[0, len)` by value; any elements
/// not yet yielded when this is dropped are dropped by `Drop` below (e.g. a
/// caller that does `.into_iter().next()` and drops the rest).
pub(crate) struct FixedVecIntoIter<E, const N: usize> {
    data: [std::mem::MaybeUninit<E>; N],
    idx: usize,
    len: usize,
}

impl<E, const N: usize> Iterator for FixedVecIntoIter<E, N> {
    type Item = E;
    fn next(&mut self) -> Option<E> {
        if self.idx >= self.len {
            return None;
        }
        let i = self.idx;
        self.idx += 1;
        // SAFETY: i < len <= N and [0, len) were initialized when this
        // iterator was built (see `IntoIterator::into_iter` below); `idx`
        // only increases, so each index is read out exactly once.
        Some(unsafe { self.data[i].assume_init_read() })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.len - self.idx;
        (rem, Some(rem))
    }
}

impl<E, const N: usize> Drop for FixedVecIntoIter<E, N> {
    fn drop(&mut self) {
        // SAFETY: [idx, len) are exactly the not-yet-yielded initialized
        // elements — [0, idx) were already moved out by `next` (and must
        // not be dropped again), [len, N) were never initialized.
        for slot in &mut self.data[self.idx..self.len] {
            unsafe { std::ptr::drop_in_place(slot.as_mut_ptr()) };
        }
    }
}

impl<E, const N: usize> IntoIterator for FixedVec<E, N> {
    type Item = E;
    type IntoIter = FixedVecIntoIter<E, N>;
    fn into_iter(self) -> Self::IntoIter {
        let len = self.len as usize;
        // `self` implements `Drop`, so its `data` field can't be moved out
        // by destructuring; suppress that `Drop` and move `data` out by raw
        // read instead.
        let this = std::mem::ManuallyDrop::new(self);
        // SAFETY: `this` is a `ManuallyDrop`, so `self`'s `Drop` impl (which
        // would drop `[0, len)` in `data`) never runs for this value;
        // reading `data` out bitwise-moves ownership of the whole array —
        // including the live `[0, len)` prefix — to the returned iterator,
        // which becomes solely responsible for dropping what it doesn't
        // yield.
        let data = unsafe { std::ptr::read(&this.data) };
        FixedVecIntoIter { data, idx: 0, len }
    }
}

/// `BTreeNode::entries` field type. Capacity `MAX_KEYS + 1` — see the
/// `FixedVec` doc comment above for the transient-overflow headroom
/// rationale.
pub(crate) type Entries<K, V> = FixedVec<(K, Arc<V>), { MAX_KEYS + 1 }>;
/// `BTreeNode::children` field type. Capacity `MAX_KEYS + 2`: one more than
/// `Entries`'s capacity, mirroring the steady-state invariant that an
/// internal node always carries one more child than entries. Holds `Child`
/// slots rather than `Arc<BTreeNode>` directly, so a node can be cloned
/// (siblings included) without touching — or even loading — every child;
/// see `src/child.rs`.
pub(crate) type Children<K, V> = FixedVec<Child<K, V>, { MAX_KEYS + 2 }>;

// ---------------------------------------------------------------------------
// Internal node type
// ---------------------------------------------------------------------------

pub(crate) struct BTreeNode<K, V> {
    /// Key-value pairs stored in sorted order, inline (no heap Vec).
    pub(crate) entries: Entries<K, V>,
    /// Children; empty for leaf nodes, len == entries.len() + 1 for internal nodes.
    pub(crate) children: Children<K, V>,
}

// Manual `Clone` bounded on `K: Clone` only. A `#[derive(Clone)]` would add a
// spurious `V: Clone` bound; here values live behind `Arc<V>` and children behind
// `Arc<BTreeNode>`, so cloning a node only clones the keys and bumps refcounts —
// `V` is never cloned. Since `entries`/`children` are inline `FixedVec`s (not
// `Vec`s), this clone is a plain array copy plus the fields' element clones —
// no heap allocation of its own; the only allocation on the clone path is the
// caller's enclosing `Arc::new`. This impl is what makes `Arc::make_mut` usable
// on the in-place insert path (`insert_mut`) without imposing `V: Clone` on
// callers.
impl<K: Clone, V> Clone for BTreeNode<K, V> {
    fn clone(&self) -> Self {
        BTreeNode {
            entries: self.entries.clone(),
            children: self.children.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Public BTree type
// ---------------------------------------------------------------------------

/// Persistent copy-on-write B-tree mapping keys of type `K` to values of type `V`.
///
/// All mutation methods return a **new** `BTree` sharing unchanged subtrees
/// with the original via `Arc`. `Clone` is O(1).  No `V: Clone` bound is
/// required.
pub struct BTree<K, V> {
    root: Child<K, V>,
    len: usize,
    /// Levels below the root; 0 = root is a leaf. Kept up to date by every
    /// mutation (root split/collapse is the only thing that ever changes
    /// it) rather than recomputed by descent, so `height()` never needs to
    /// touch — let alone fault — a single node, even on a tree that is
    /// otherwise entirely on disk.
    height: usize,
    /// Where an on-disk `Child` slot in this tree faults its node in from.
    /// `None` for every tree built so far — nothing here ever creates an
    /// `on_disk` slot, so nothing ever needs to fault one in. Paging lands
    /// in a later task; this field (and `source()`/`set_source()` below)
    /// exists now so descent/mutation can thread it through unconditionally
    /// rather than needing a follow-up signature change everywhere.
    source: Option<Arc<dyn NodeSource<K, V>>>,
}

// ---------------------------------------------------------------------------
// Internal enums for recursive helpers
// ---------------------------------------------------------------------------

enum InsertResult<K, V> {
    Fit(Child<K, V>, bool),
    Split {
        left: Child<K, V>,
        median: (K, Arc<V>),
        right: Child<K, V>,
        replaced: bool,
    },
}

enum DeleteResult<K, V> {
    NotFound,
    Removed {
        node: Child<K, V>,
        underfull: bool,
    },
}

/// Outcome of an in-place delete into a node (see `delete_from_node_mut`).
/// The mutated node flows back through the `&mut Child<K, V>` the caller
/// passed; only the found/underfull flags propagate up.
enum DeleteOutcome {
    NotFound,
    Removed { underfull: bool },
}

// ---------------------------------------------------------------------------
// BTree impl
// ---------------------------------------------------------------------------

impl<K: Ord + Clone, V> BTree<K, V> {
    /// Creates a new, empty B-tree.
    pub fn new() -> Self {
        BTree {
            root: Child::resident(Arc::new(BTreeNode {
                entries: Entries::new(),
                children: Children::new(),
            })),
            len: 0,
            height: 0,
            source: None,
        }
    }

    /// The tree's page source, if one is attached — `None` means every
    /// `Child` slot in the tree is resident (the only state this task
    /// produces). Exposed so the recursive descent/mutation helpers can be
    /// handed `src` without the tree storing it on their behalf.
    // No production caller yet: this task's internals read `self.source`
    // directly (it's a private field of the same struct) rather than through
    // this accessor. A later task's page-store wiring is the intended
    // external caller; kept now so that wiring is a pure addition.
    #[allow(dead_code)]
    pub(crate) fn source(&self) -> Option<&dyn NodeSource<K, V>> {
        self.source.as_deref()
    }

    /// Attach (or clear, via `None`) the tree's page source.
    // No production caller yet — see `source()` above.
    #[allow(dead_code)]
    pub(crate) fn set_source(&mut self, s: Option<Arc<dyn NodeSource<K, V>>>) {
        self.source = s;
    }

    /// Build a B-tree from a strictly-ascending iterator of `(K, Arc<V>)`
    /// pairs in O(N), packing leaves densely with `MAX_KEYS` entries each.
    ///
    /// Debug-asserts strict ascending order and rejects duplicate keys.
    /// Caller is responsible for sort and dedup.
    ///
    /// At exactly nested-cascade-aligned sizes (`m * (MAX_KEYS + 1)^3 +
    /// delta` for any multiple `m >= 1` and `1 <= delta < MIN_KEYS`), the
    /// tail leaf may be packed below `MIN_KEYS`; reads, writes, and ordering
    /// are unaffected, and a delete touching that leaf restores the floor
    /// (see `from_sorted_nested_cascade_two_million_benign`).
    pub(crate) fn from_sorted<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, Arc<V>)>,
    {
        let mut builder = BulkBuilder::<K, V>::new();
        for (k, v) in iter {
            builder.push(k, v);
        }
        builder.finish()
    }

    /// Append a strictly-ascending iterator of `(K, Arc<V>)` pairs, every key
    /// strictly greater than the current maximum, in O(batch + height) with
    /// leaves packed densely like `from_sorted`. Debug-asserts the ordering
    /// (including versus the existing max, via the builder's `last_key`);
    /// caller guarantees it — `Table::insert_batch` checks `max_key()` first.
    pub(crate) fn extend_from_sorted<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (K, Arc<V>)>,
    {
        let mut builder = BulkBuilder::seed_from_spine(self);
        for (k, v) in iter {
            builder.push(k, v);
        }
        *self = builder.finish();
    }

    /// Returns the number of elements in the tree.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the tree contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Look up a key. Returns a reference tied to the lifetime of `&self`.
    pub fn get(&self, key: &K) -> Option<&V> {
        let src = self.source.as_deref();
        get_in_node(self.root.load(src), key, src)
    }

    /// Look up a key and return a shared handle to the value.
    pub fn get_arc(&self, key: &K) -> Option<Arc<V>> {
        let src = self.source.as_deref();
        get_arc_in_node(self.root.load(src), key, src)
    }

    /// The largest key in the tree (rightmost leaf's last entry), or `None`
    /// if empty. O(height); used by `Table::insert_batch` to verify the
    /// append invariant before taking the bulk fast path.
    pub(crate) fn max_key(&self) -> Option<&K> {
        let src = self.source.as_deref();
        let mut node = self.root.load(src);
        loop {
            match node.children.last() {
                Some(c) => node = c.load(src),
                None => return node.entries.last().map(|(k, _)| k),
            }
        }
    }

    /// Insert or replace a key-value pair. Returns a new tree; `self` is
    /// unchanged.
    pub fn insert(&self, key: K, val: V) -> BTree<K, V> {
        self.insert_arc(key, Arc::new(val))
    }

    /// Insert or replace a key-value pair, reusing an existing `Arc<V>`
    /// (no clone of the payload). Used at commit time for per-key merge
    /// to carry records from one snapshot into another without forcing
    /// `V: Clone`.
    pub fn insert_arc(&self, key: K, val_arc: Arc<V>) -> BTree<K, V> {
        match insert_into_node(&self.root, key, val_arc, self.source.as_deref()) {
            InsertResult::Fit(new_root, replaced) => {
                let new_len = if replaced { self.len } else { self.len + 1 };
                BTree {
                    root: new_root,
                    len: new_len,
                    height: self.height,
                    source: self.source.clone(),
                }
            }
            InsertResult::Split {
                left,
                median,
                right,
                replaced,
            } => {
                let new_len = if replaced { self.len } else { self.len + 1 };
                let mut entries = Entries::new();
                entries.push(median);
                let mut children = Children::new();
                children.push(left);
                children.push(right);
                let new_root = Child::resident_new(Arc::new(BTreeNode { entries, children }), self.source.as_deref());
                BTree {
                    root: new_root,
                    len: new_len,
                    height: self.height + 1,
                    source: self.source.clone(),
                }
            }
        }
    }

    /// In-place variant of [`insert`](Self::insert). Mutates `self` rather than
    /// returning a new tree.
    ///
    /// # Why this exists (perf prototype, task: btree-insert-mut)
    ///
    /// The immutable `insert` allocates a fresh `Arc<BTreeNode>` for every node
    /// on the root→leaf path on *every* call, because it cannot know whether any
    /// node is shared with another snapshot. `insert_mut` descends with
    /// `Child::make_mut`, which clones a node **only** when it is actually shared
    /// (`strong_count > 1`) and otherwise mutates it in place. Copy-on-write, and
    /// therefore snapshot isolation, is preserved exactly: a node still visible to
    /// an older snapshot is cloned before mutation; a node uniquely owned by this
    /// tree (e.g. one just created by a previous insert in a batch) is reused.
    ///
    /// The upshot: a batch of inserts into a privately-owned tree drops from
    /// `O(height)` allocations + refcount traffic *per key* to near-zero when
    /// successive keys share path nodes.
    pub fn insert_mut(&mut self, key: K, val: V) {
        self.insert_arc_mut(key, Arc::new(val));
    }

    /// In-place variant of [`insert_arc`](Self::insert_arc), reusing an existing
    /// `Arc<V>`. See [`insert_mut`](Self::insert_mut) for the rationale.
    pub fn insert_arc_mut(&mut self, key: K, val_arc: Arc<V>) {
        let src = self.source.as_deref();
        match insert_into_node_mut(&mut self.root, key, val_arc, src) {
            InsertOutcome::Fit { replaced } => {
                if !replaced {
                    self.len += 1;
                }
            }
            InsertOutcome::Split {
                median,
                right,
                replaced,
            } => {
                if !replaced {
                    self.len += 1;
                }
                // Root split: the mutated `self.root` is now the left half.
                // Lift it under a fresh root alongside the promoted median.
                //
                // The placeholder handed to `mem::replace` below is a
                // throwaway: it lives only until `self.root` is reassigned
                // a few lines down and is never reachable from any
                // snapshot, so it deliberately stays a plain `resident` —
                // `resident_new` here would credit `dirty_bytes` for a node
                // no checkpoint will ever have to write.
                let left = std::mem::replace(
                    &mut self.root,
                    Child::resident(Arc::new(BTreeNode {
                        entries: Entries::new(),
                        children: Children::new(),
                    })),
                );
                let mut entries = Entries::new();
                entries.push(median);
                let mut children = Children::new();
                children.push(left);
                children.push(right);
                self.root = Child::resident_new(Arc::new(BTreeNode { entries, children }), src);
                self.height += 1;
            }
        }
    }

    /// Remove a key. Returns a new tree, or `Err(KeyNotFound)` if the key is
    /// absent. `self` is unchanged.
    pub fn remove(&self, key: &K) -> Result<BTree<K, V>> {
        let src = self.source.as_deref();
        match delete_from_node(&self.root, key, src) {
            DeleteResult::NotFound => Err(Error::KeyNotFound),
            DeleteResult::Removed { node: new_root, .. } => {
                // If the root is now an internal node with no entries but one
                // child, collapse the tree height by one.
                let new_root_node = new_root.load(src);
                let collapsed = new_root_node.entries.is_empty() && !new_root_node.children.is_empty();
                let actual_root = if collapsed { new_root_node.children[0].clone() } else { new_root };
                Ok(BTree {
                    root: actual_root,
                    len: self.len - 1,
                    height: if collapsed { self.height - 1 } else { self.height },
                    source: self.source.clone(),
                })
            }
        }
    }

    /// In-place variant of [`remove`](Self::remove). Deletes `key`, mutating
    /// `self`; returns `true` iff the key was present. Copy-on-write preserved:
    /// nodes shared with an older snapshot are cloned before mutation, so a
    /// snapshot never observes the deletion. See [`insert_mut`](Self::insert_mut)
    /// for the rationale.
    pub fn remove_mut(&mut self, key: &K) -> bool {
        let src = self.source.as_deref();
        match delete_from_node_mut(&mut self.root, key, src) {
            DeleteOutcome::NotFound => false,
            DeleteOutcome::Removed { .. } => {
                self.len -= 1;
                // Root collapse: an internal root left with no entries and one
                // child drops a level. Move that child up (no clone).
                let root = self.root.make_mut(src);
                if root.entries.is_empty() && !root.children.is_empty() {
                    let only = root.children.remove(0);
                    self.root = only;
                    self.height -= 1;
                }
                true
            }
        }
    }

    /// Iterate over `(&K, &V)` pairs in ascending key order within `range`.
    pub fn range<'a>(&'a self, range: impl RangeBounds<K> + 'a) -> BTreeRange<'a, K, V> {
        self.range_with(Locator::Bounds(
            range.start_bound().cloned(),
            range.end_bound().cloned(),
        ))
    }

    /// Changes that turn `base` into `self`, in ascending key order.
    ///
    /// Runs in time proportional to what changed, not to tree size: any
    /// subtree the two versions still share is a single `Arc` on both sides
    /// and is skipped whole. Equal keys bound to the same value `Arc` are not
    /// reported — a re-insert of an identical `Arc` is not a change.
    ///
    /// Both trees must be versions of the same logical tree. Diffing two
    /// unrelated trees is well-defined (it degenerates to a full ordered
    /// merge) but pointless.
    pub fn diff<'a>(&'a self, base: &'a BTree<K, V>) -> BTreeDiff<'a, K, V> {
        // Common no-op-checkpoint case: nothing changed since `base` at all,
        // so the roots are still the same node (no CoW clone ever
        // happened). Short-circuit to empty cursors instead of walking up to
        // MAX_KEYS root entries just to find every one of them unchanged.
        if Child::same_node(&self.root, &base.root) {
            return BTreeDiff {
                new: DiffCursor {
                    stack: Vec::new(),
                    src: self.source.as_deref(),
                    #[cfg(test)]
                    descends: 0,
                },
                base: DiffCursor {
                    stack: Vec::new(),
                    src: base.source.as_deref(),
                    #[cfg(test)]
                    descends: 0,
                },
            };
        }
        BTreeDiff {
            new: DiffCursor::new(&self.root, self.source.as_deref()),
            base: DiffCursor::new(&base.root, base.source.as_deref()),
        }
    }

    /// Iterate over every entry the monotone `locate` predicate reports as
    /// [`Ordering::Equal`], in ascending key order.
    ///
    /// # Contract
    ///
    /// `locate` **must be monotone with respect to key order**: `Less` for
    /// every key before the range, `Equal` for every key inside it, `Greater`
    /// for every key after it (formally, `a <= b` implies
    /// `locate(a) <= locate(b)`). The descent uses `partition_point` on that
    /// predicate, so a non-monotone `locate` *silently truncates* the scan
    /// instead of failing — it is not memory-unsafe, but it is a correctness
    /// bug in the caller that no assertion will catch.
    ///
    /// This exists because some scans cannot be expressed as a
    /// `RangeBounds<K>`: a prefix scan of a composite key `(A, B)` would need
    /// invented minimum and maximum values for `B`, which types like `String`
    /// do not have.
    pub(crate) fn range_by<'a>(
        &'a self,
        locate: impl Fn(&K) -> Ordering + Send + Sync + 'a,
    ) -> BTreeRange<'a, K, V> {
        self.range_with(Locator::Pred(Box::new(locate)))
    }

    /// Shared constructor for both range entry points: seed the forward and
    /// backward stacks by descending with `locate`.
    fn range_with<'a>(&'a self, locate: Locator<'a, K>) -> BTreeRange<'a, K, V> {
        let mut iter = BTreeRange {
            stack: vec![],
            back_stack: vec![],
            locate,
            done: false,
            last_forward: None,
            last_backward: None,
            src: self.source.as_deref(),
        };
        iter.descend_left_from(&self.root);
        iter.descend_right_from(&self.root);
        iter
    }
}

// ---------------------------------------------------------------------------
// Paging primitives — dirty-walk checkpointing, leaf demotion/eviction, and
// page-diffing, all exercised against an in-memory mock "disk"
// (`crate::child::tests::MockDisk`) until the real page store lands in a
// later task. Every primitive here works purely in terms of `Child` slot
// state (page id / residency / accessed bit); none of them assume a real
// page file exists yet.
// ---------------------------------------------------------------------------

impl<K: Ord + Clone, V> BTree<K, V> {
    /// Levels below the root; 0 = root is a leaf. A plain field read — no
    /// I/O, ever, not even on a tree that is otherwise entirely on disk
    /// (`from_root_page`'s caller supplies it, since a fresh attach has no
    /// other way to know). Every mutation that can change it (root
    /// split/collapse) keeps it in sync; see the field's doc comment on
    /// `BTree` for why this replaced descending the leftmost path.
    pub(crate) fn height(&self) -> usize {
        self.height
    }

    /// Post-order over `NO_PAGE` slots. `write(node, is_leaf)` returns the
    /// id it stored the node under; children are written before their
    /// parent so a parent's payload can name them. Returns the root's page
    /// id (writing it if dirty).
    ///
    /// `write` returning [`NO_PAGE`] aborts: descent stops, every slot on
    /// the aborted path is left dirty (its page id is never set), and
    /// `NO_PAGE` propagates back up as this call's own result — the
    /// convention a later task's checkpoint writer uses to signal a write
    /// failure through a callback that cannot itself return a `Result`.
    // No production caller yet — the (future) page-file writer is the
    // intended caller. Used today by this task's tests.
    #[allow(dead_code)]
    pub(crate) fn write_dirty(&self, write: &mut dyn FnMut(&BTreeNode<K, V>, bool) -> PageId) -> PageId {
        fn go<K, V>(
            slot: &Child<K, V>,
            src: Option<&dyn NodeSource<K, V>>,
            write: &mut dyn FnMut(&BTreeNode<K, V>, bool) -> PageId,
        ) -> PageId {
            if let Some(id) = slot.page_id() {
                return id;
            }
            // A dirty slot is always resident (see child.rs's invariant
            // note), so this never faults; unlike `load`, it must not mark
            // a node as freshly "accessed" just because a checkpoint pass
            // walked past it.
            let node = slot.load_quiet(src);
            for c in node.children.iter() {
                let id = go(c, src, write);
                if id == NO_PAGE {
                    return NO_PAGE; // abort: leave this path dirty.
                }
            }
            let id = write(node, node.children.is_empty());
            if id == NO_PAGE {
                return NO_PAGE; // abort: leave this slot dirty too.
            }
            slot.set_page_id(id);
            id
        }
        go(&self.root, self.source.as_deref(), write)
    }

    /// Build a new version in which leaf slots with a page id and a clear
    /// accessed bit are on-disk. Processes at most `budget` leaf-parents,
    /// starting after `cursor` (the max key of the last parent processed).
    /// Returns (new tree, leaves demoted, next cursor / `None` when done).
    ///
    /// Demotion never assigns a *new* page id — a leaf keeps whatever id it
    /// already had, it just stops being resident — so a demote pass never
    /// invalidates anything on disk. The walk is conservative about CoW: a
    /// leaf-parent (and every ancestor above it) is `make_mut`'d as soon as
    /// it looks like it *might* have something to demote, before the
    /// per-leaf accessed check runs — so a parent whose leaves all turn out
    /// to be accessed (nothing demoted under it) is still left dirty by the
    /// walk. [`restore_unchanged_ids`] undoes that afterward: it walks the
    /// CoW'd path bottom-up and gives back the old page id to any node whose
    /// children came out identical to the original's, so a pass that ends
    /// up (fully or partially) demoting nothing forces no rewrite.
    // No production caller yet — the (future) page evictor is the intended
    // caller. Used today by this task's tests.
    #[allow(dead_code)]
    pub(crate) fn demote_leaves(&self, cursor: Option<&K>, budget: usize) -> (BTree<K, V>, usize, Option<K>) {
        let src = self.source.as_deref();
        let h = self.height();
        if h == 0 {
            return (self.clone(), 0, None);
        }
        let mut out = self.clone();
        let mut demoted = 0usize;
        let mut left = budget;
        let mut last: Option<K> = None;

        // depth counts down; at depth 1 a node's children are leaves.
        // Returns whether the budget was exhausted (there is more to do).
        fn go<K: Ord + Clone, V>(
            slot: &mut Child<K, V>,
            depth: usize,
            src: Option<&dyn NodeSource<K, V>>,
            cursor: Option<&K>,
            left: &mut usize,
            demoted: &mut usize,
            last: &mut Option<K>,
        ) -> bool {
            if *left == 0 {
                return true;
            }
            if depth == 1 {
                let node_ref = slot.load(src);
                if let (Some(c), Some(maxk)) = (cursor, node_ref.entries.last().map(|e| &e.0))
                    && maxk <= c
                {
                    return false; // already processed in an earlier pass
                }
                let any = node_ref.children.iter().any(|c| c.is_loaded() && c.page_id().is_some());
                if !any {
                    return false;
                }
                // Conservative CoW: this parent is dirtied here even if
                // every child below turns out to be accessed (nothing
                // demoted). `None` so the CoW isn't reported as new dirty
                // data — it's bookkeeping, not a write — and
                // `restore_unchanged_ids` gives the id back afterward if
                // nothing actually changed.
                let n = slot.make_mut(None);
                for c in n.children.iter_mut() {
                    if let (true, Some(id)) = (c.is_loaded(), c.page_id()) {
                        if c.take_accessed() {
                            // second chance: bit cleared, stays resident
                        } else {
                            *c = Child::on_disk(id);
                            *demoted += 1;
                        }
                    }
                }
                *last = n.entries.last().map(|(k, _)| k.clone());
                *left -= 1;
                return *left == 0;
            }
            slot.load(src); // ensure resident; may fault a never-visited branch
            let n = slot.make_mut(None);
            for c in n.children.iter_mut() {
                if go(c, depth - 1, src, cursor, left, demoted, last) {
                    return true;
                }
            }
            false
        }
        let exhausted = go(&mut out.root, h, src, cursor, &mut left, &mut demoted, &mut last);
        restore_unchanged_ids(&out.root, &self.root, h, src);
        (out, demoted, if exhausted { last } else { None })
    }

    /// Page ids referenced by `prev` and not by `self`, walking only inner
    /// subtrees whose page id differs between the two (identical-id
    /// subtrees are skipped whole).
    ///
    /// # Precondition
    ///
    /// Both trees' inner levels must be resident — `inner_ids`'s own walk
    /// below treats a not-yet-loaded slot as a leaf, since a *leaf* is the
    /// only thing this task ever leaves on disk. A tree fresh off
    /// `from_root_page` violates that (its whole spine may still be on
    /// disk), and calling this without the fix below would report every
    /// page of two otherwise-identical trees as dead. So this calls
    /// `load_inner_levels` on both trees first — a no-op once they already
    /// are resident, so callers that already loaded them (or built them
    /// in-memory) pay nothing extra.
    ///
    /// Read-only otherwise: uses [`Child::load_quiet`] throughout, so a
    /// checkpoint-diff or GC walk never marks a leaf "recently used" just
    /// by looking at it — only a real workload touch (`get`, `insert_mut`,
    /// ...) should be able to give a leaf a second chance in
    /// [`BTree::demote_leaves`].
    // No production caller yet — the (future) GC/page-reclaim pass is the
    // intended caller. Used today by this task's tests.
    #[allow(dead_code)]
    pub(crate) fn changed_page_ids(&self, prev: &BTree<K, V>) -> Vec<PageId> {
        use std::collections::HashSet;

        self.load_inner_levels();
        prev.load_inner_levels();

        fn inner_ids<K, V>(t: &BTree<K, V>, out: &mut HashSet<PageId>) {
            fn go<K, V>(slot: &Child<K, V>, src: Option<&dyn NodeSource<K, V>>, out: &mut HashSet<PageId>) {
                if !slot.is_loaded() {
                    return; // an on-disk slot is a leaf (inner levels are resident — see the precondition above)
                }
                let n = slot.load_quiet(src);
                if n.children.is_empty() {
                    return;
                }
                if let Some(id) = slot.page_id() {
                    out.insert(id);
                }
                for c in n.children.iter() {
                    go(c, src, out);
                }
            }
            go(&t.root, t.source.as_deref(), out);
        }

        let (mut new_inner, mut prev_inner) = (HashSet::new(), HashSet::new());
        inner_ids(self, &mut new_inner);
        inner_ids(prev, &mut prev_inner);

        // Ids referenced under changed inner nodes of each side; a slot
        // whose id is unchanged between the two trees is an identical
        // subtree and is skipped whole rather than walked.
        fn collect<K, V>(t: &BTree<K, V>, other_inner: &HashSet<PageId>, out: &mut HashSet<PageId>) {
            fn go<K, V>(
                slot: &Child<K, V>,
                src: Option<&dyn NodeSource<K, V>>,
                other: &HashSet<PageId>,
                out: &mut HashSet<PageId>,
            ) {
                let id = slot.page_id();
                if let Some(i) = id
                    && other.contains(&i)
                {
                    return; // identical subtree on both sides
                }
                if let Some(i) = id {
                    out.insert(i);
                }
                if !slot.is_loaded() {
                    return;
                }
                let n = slot.load_quiet(src);
                for c in n.children.iter() {
                    go(c, src, other, out);
                }
            }
            go(&t.root, t.source.as_deref(), other_inner, out);
        }

        let (mut new_ids, mut prev_ids) = (HashSet::new(), HashSet::new());
        collect(self, &prev_inner, &mut new_ids);
        collect(prev, &new_inner, &mut prev_ids);
        prev_ids.difference(&new_ids).copied().collect()
    }

    /// A tree whose root is on disk. `len` and `height` come from the root
    /// record (the caller — the checkpoint/root format a later task adds —
    /// is expected to have persisted both alongside the root id; there is
    /// no way to discover `height` from an on-disk root without faulting
    /// something, which is exactly what caching it on `BTree` avoids).
    // No production caller yet — the (future) recovery/attach path is the
    // intended caller. Used today by this task's tests.
    #[allow(dead_code)]
    pub(crate) fn from_root_page(id: PageId, len: usize, height: usize, source: Arc<dyn NodeSource<K, V>>) -> Self {
        BTree {
            root: Child::on_disk(id),
            len,
            height,
            source: Some(source),
        }
    }

    /// Fault in every non-leaf node; leaves stay on disk.
    // No production caller yet — the (future) startup/attach path is the
    // intended caller. Used today by this task's tests.
    #[allow(dead_code)]
    pub(crate) fn load_inner_levels(&self) {
        let src = self.source.as_deref();
        let h = self.height();
        fn go<K, V>(slot: &Child<K, V>, depth: usize, src: Option<&dyn NodeSource<K, V>>) {
            if depth == 0 {
                return; // leaf slot: leave on disk
            }
            let n = slot.load(src);
            for c in n.children.iter() {
                go(c, depth - 1, src);
            }
        }
        go(&self.root, h, src)
    }

    /// Bytes of resident leaves, estimated as loaded-leaf-slots × NODE_BYTES.
    /// Read-only: uses [`Child::load_quiet`], so measuring residency never
    /// marks anything "recently used" — see the same note on
    /// [`Self::changed_page_ids`].
    // No production caller yet — the (future) page evictor's budget check is
    // the intended caller. Used today by this task's tests.
    #[allow(dead_code)]
    pub(crate) fn resident_leaf_estimate(&self) -> usize {
        let src = self.source.as_deref();
        let h = self.height();
        fn go<K, V>(slot: &Child<K, V>, depth: usize, src: Option<&dyn NodeSource<K, V>>) -> usize {
            if depth == 0 {
                return if slot.is_loaded() { Child::<K, V>::NODE_BYTES } else { 0 };
            }
            let n = slot.load_quiet(src);
            n.children.iter().map(|c| go(c, depth - 1, src)).sum()
        }
        go(&self.root, h, src)
    }

    /// Walk resident nodes, reporting every slot's page id — used by tests
    /// and by a later task's punch (hole-punch reclaim) bookkeeping.
    /// Read-only: uses [`Child::load_quiet`], so a punch-bookkeeping pass
    /// never marks a leaf "recently used" just by visiting it — see the
    /// same note on [`Self::changed_page_ids`].
    // No production caller yet — a later task's punch bookkeeping is the
    // intended caller. Used today by this task's tests.
    #[allow(dead_code)]
    pub(crate) fn for_each_page_id(&self, f: &mut dyn FnMut(PageId)) {
        fn go<K, V>(slot: &Child<K, V>, src: Option<&dyn NodeSource<K, V>>, f: &mut dyn FnMut(PageId)) {
            if let Some(id) = slot.page_id() {
                f(id);
            }
            if !slot.is_loaded() {
                return;
            }
            for c in slot.load_quiet(src).children.iter() {
                go(c, src, f);
            }
        }
        go(&self.root, self.source.as_deref(), f)
    }
}

/// After [`BTree::demote_leaves`] CoWs an inner path — conservatively,
/// before it knows whether anything under a given parent will actually end
/// up demoted (see that method's doc comment) — give back a node's old page
/// id wherever every one of its child slots still matches the corresponding
/// slot in `orig`, walking both trees in lockstep, bottom-up, down to (but
/// not including) the leaf level. Demotion never changes a page id (flipping
/// a leaf resident<->on-disk keeps its id, which is all `Child::same_node`
/// compares), so in practice this restores the entire touched path whenever
/// nothing above the leaf level structurally changed.
fn restore_unchanged_ids<K: Ord + Clone, V>(new: &Child<K, V>, orig: &Child<K, V>, depth: usize, src: Option<&dyn NodeSource<K, V>>) {
    if depth == 0 || new.page_id().is_some() {
        // Leaf level (demotion never touches a leaf's id), or a node this
        // pass never `make_mut`'d in the first place — nothing to restore.
        return;
    }
    // `load_quiet`: this is bookkeeping that runs after every demote_leaves
    // pass, not a workload read — it must not leave `orig` (the untouched
    // original tree) or `new`'s inner nodes looking freshly "recently used".
    let nn = new.load_quiet(src);
    let on = orig.load_quiet(src);
    for (nc, oc) in nn.children.iter().zip(on.children.iter()) {
        restore_unchanged_ids(nc, oc, depth - 1, src);
    }
    if nn.children.iter().zip(on.children.iter()).all(|(a, b)| Child::same_node(a, b))
        && let Some(id) = orig.page_id()
    {
        new.set_page_id(id);
    }
}

impl<A: Ord + Clone + Sync, B: Ord + Clone, V> BTree<(A, B), V> {
    /// Iterate over every entry whose first key component equals `prefix`,
    /// in ascending order, in O(log n + k).
    ///
    /// This exists because a prefix scan cannot be written as a
    /// `RangeBounds<(A, B)>` without inventing minimum and maximum values
    /// for `B`, which do not exist for types like `String`.
    pub fn range_prefix<'a>(
        &'a self,
        prefix: &'a A,
    ) -> impl Iterator<Item = (&'a (A, B), &'a V)> + 'a {
        // Monotone by construction: `(a, _)` sorts by `a` first, so comparing
        // only the first component is consistent with the full key order.
        self.range_by(move |k: &(A, B)| k.0.cmp(prefix))
    }
}

impl<K, V> Clone for BTree<K, V> {
    /// O(1): clones the root `Child` slot (an `Arc` bump if resident, a
    /// page-id copy if not) and bumps the source `Arc`, if any.
    fn clone(&self) -> Self {
        BTree {
            root: self.root.clone(),
            len: self.len,
            height: self.height,
            source: self.source.clone(),
        }
    }
}

impl<K: Ord + Clone, V> Default for BTree<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Range iterator
// ---------------------------------------------------------------------------

/// Describes where a key sits relative to the range being scanned: before it,
/// inside it, or after it. Monotone with respect to key order (see
/// [`BTree::range_by`]).
///
/// Queried through two *directional* predicates rather than one merged
/// three-way classification, because the descent and the per-item checks each
/// only ever care about one side. Merging them would cost a second key
/// comparison per yielded item, and would turn an `Unbounded` side from zero
/// comparisons into one.
///
/// The `RangeBounds` case is a concrete variant rather than just another
/// boxed closure so that [`BTree::range`] — every index lookup and table scan
/// in the crate — stays allocation-free and statically dispatched.
enum Locator<'a, K> {
    /// Start and end bound, as supplied to [`BTree::range`].
    Bounds(Bound<K>, Bound<K>),
    /// An arbitrary monotone predicate, as supplied to [`BTree::range_by`].
    Pred(Box<dyn Fn(&K) -> Ordering + Send + Sync + 'a>),
}

/// Is `x` before the range's start bound? One key comparison, or none when
/// the bound is `Unbounded`.
#[inline]
fn before_start<T: Ord>(start: &Bound<T>, x: &T) -> bool {
    match start {
        Bound::Unbounded => false,
        Bound::Included(s) => x < s,
        Bound::Excluded(s) => x <= s,
    }
}

/// Is `x` past the range's end bound? One key comparison, or none when the
/// bound is `Unbounded`.
#[inline]
fn after_end<T: Ord>(end: &Bound<T>, x: &T) -> bool {
    match end {
        Bound::Unbounded => false,
        Bound::Included(e) => x > e,
        Bound::Excluded(e) => x >= e,
    }
}

/// Classify `x` against a `Bound` pair: `Less` before the range, `Equal`
/// inside, `Greater` after. Monotone in `x` by construction, so it is a valid
/// [`BTree::range_by`] locator — see `NonUniqueStorage::range_ids`, which
/// applies it to one half of a composite key.
///
/// Callers that only need one side must use [`before_start`] / [`after_end`]
/// directly: this evaluates *both* bounds, and the iteration path is walked
/// once per yielded item.
#[inline]
pub(crate) fn classify<T: Ord>(start: &Bound<T>, end: &Bound<T>, x: &T) -> Ordering {
    if before_start(start, x) {
        Ordering::Less
    } else if after_end(end, x) {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

impl<K: Ord> Locator<'_, K> {
    /// Is `key` before the range? For `Bounds` this consults the *start* bound
    /// only — the whole point of keeping the two directions separate, since
    /// merging them would cost a second key comparison on every yielded item
    /// (and turn an `Unbounded` side from zero comparisons into one).
    #[inline]
    fn is_before_start(&self, key: &K) -> bool {
        match self {
            Locator::Bounds(start, _) => before_start(start, key),
            Locator::Pred(f) => f(key) == Ordering::Less,
        }
    }

    /// Is `key` past the range? For `Bounds` this consults the *end* bound only.
    #[inline]
    fn is_after_end(&self, key: &K) -> bool {
        match self {
            Locator::Bounds(_, end) => after_end(end, key),
            Locator::Pred(f) => f(key) == Ordering::Greater,
        }
    }
}

/// Iterator over key-value pairs in a `BTree<K, V>` within a key range.
///
/// Supports both forward (`Iterator`) and backward (`DoubleEndedIterator`) traversal,
/// as well as mixed forward/backward iteration.
pub struct BTreeRange<'a, K, V> {
    /// Stack of (node, next-entry-index-to-yield) frames for forward traversal.
    stack: Vec<(&'a BTreeNode<K, V>, usize)>,
    /// Stack of (node, one-past-last-entry-index) frames for backward traversal.
    back_stack: Vec<(&'a BTreeNode<K, V>, usize)>,
    /// Where a key sits relative to the range — replaces the start/end bound
    /// pair, so both bounded and prefix scans share one descent path.
    locate: Locator<'a, K>,
    /// Set to true when the forward and backward iterators have met, or a bound
    /// is violated — no further items should be yielded.
    done: bool,
    /// Last key yielded by `next()` — used for overlap detection with `next_back()`.
    last_forward: Option<&'a K>,
    /// Last key yielded by `next_back()` — used for overlap detection with `next()`.
    last_backward: Option<&'a K>,
    /// The source tree's page source, threaded through every `Child::load`
    /// this scan performs. `None` for every tree built so far.
    src: Option<&'a dyn NodeSource<K, V>>,
}

impl<'a, K: Ord + Clone, V> BTreeRange<'a, K, V> {
    /// Push stack frames for the leftmost path that is not before the range.
    fn descend_left_from(&mut self, node: &'a Child<K, V>) {
        let n = node.load(self.src);
        let entry_start = {
            let locate = &self.locate;
            n.entries
                .partition_point(|(ek, _)| locate.is_before_start(ek))
        };
        self.stack.push((n, entry_start));
        if !n.children.is_empty() && entry_start < n.children.len() {
            self.descend_left_from(&n.children[entry_start]);
        }
    }

    /// Push stack frames for the leftmost leaf of `node` (no range restriction).
    fn descend_leftmost(&mut self, node: &'a Child<K, V>) {
        let n = node.load(self.src);
        self.stack.push((n, 0));
        if !n.children.is_empty() {
            self.descend_leftmost(&n.children[0]);
        }
    }

    fn in_end_bound(&self, key: &K) -> bool {
        !self.locate.is_after_end(key)
    }

    /// Push back_stack frames for the rightmost path that is not past the range.
    fn descend_right_from(&mut self, node: &'a Child<K, V>) {
        let n = node.load(self.src);
        // `entry_end` = one past the last valid index for backward iteration.
        let entry_end = {
            let locate = &self.locate;
            n.entries
                .partition_point(|(ek, _)| !locate.is_after_end(ek))
        };
        self.back_stack.push((n, entry_end));
        if !n.children.is_empty() && entry_end < n.children.len() {
            self.descend_right_from(&n.children[entry_end]);
        }
    }

    /// Push back_stack frames for the rightmost leaf of `node` (no range restriction).
    fn descend_rightmost(&mut self, node: &'a Child<K, V>) {
        let n = node.load(self.src);
        self.back_stack.push((n, n.entries.len()));
        if !n.children.is_empty() {
            self.descend_rightmost(n.children.last().unwrap());
        }
    }

    fn in_start_bound(&self, key: &K) -> bool {
        !self.locate.is_before_start(key)
    }
}

impl<'a, K: Ord + Clone, V> Iterator for BTreeRange<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            let stack_len = self.stack.len();
            if stack_len == 0 {
                self.done = true;
                return None;
            }

            // Copy the top frame's values — both fields are Copy
            // (&'a BTreeNode<K, V> is Copy; usize is Copy).
            let (node, entry_idx) = self.stack[stack_len - 1];

            if entry_idx >= node.entries.len() {
                self.stack.pop();
                continue;
            }

            let key = &node.entries[entry_idx].0;
            let val = &*node.entries[entry_idx].1; // &'a V

            if !self.in_end_bound(key) {
                self.stack.clear();
                self.done = true;
                return None;
            }

            // Overlap detection: stop if we've reached or passed a key already
            // yielded by next_back().
            if let Some(bk) = self.last_backward
                && key >= bk
            {
                self.done = true;
                return None;
            }

            // Advance the current frame to the next entry.
            self.stack[stack_len - 1].1 = entry_idx + 1;

            // For internal nodes, after yielding entries[i] we must next visit
            // the left-spine of children[i+1] before entries[i+1].
            if !node.children.is_empty() {
                let rci = entry_idx + 1;
                if rci < node.children.len() {
                    // node lives for 'a, so &node.children[rci] is &'a Arc<...>
                    self.descend_leftmost(&node.children[rci]);
                }
            }

            self.last_forward = Some(key);
            return Some((key, val));
        }
    }
}

impl<'a, K: Ord + Clone, V> DoubleEndedIterator for BTreeRange<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            let stack_len = self.back_stack.len();
            if stack_len == 0 {
                self.done = true;
                return None;
            }

            let (node, entry_idx) = self.back_stack[stack_len - 1];

            if entry_idx == 0 {
                self.back_stack.pop();
                continue;
            }

            let actual_idx = entry_idx - 1;
            let key = &node.entries[actual_idx].0;
            let val = &*node.entries[actual_idx].1;

            if !self.in_start_bound(key) {
                self.back_stack.clear();
                self.done = true;
                return None;
            }

            // Overlap detection: stop if we've reached or passed a key already
            // yielded by next().
            if let Some(fk) = self.last_forward
                && key <= fk
            {
                self.done = true;
                return None;
            }

            // Retreat the current frame to point at `actual_idx`.
            self.back_stack[stack_len - 1].1 = actual_idx;

            // For internal nodes, before yielding entries[actual_idx] we must
            // visit the rightmost path of children[actual_idx].
            if !node.children.is_empty() && actual_idx < node.children.len() {
                self.descend_rightmost(&node.children[actual_idx]);
            }

            self.last_backward = Some(key);
            return Some((key, val));
        }
    }
}

// ---------------------------------------------------------------------------
// Recursive helpers
// ---------------------------------------------------------------------------

/// Recursively searches for a key in a node. Returns a reference to the value.
fn get_in_node<'a, K: Ord, V>(
    node: &'a BTreeNode<K, V>,
    key: &K,
    src: Option<&dyn NodeSource<K, V>>,
) -> Option<&'a V> {
    match node.entries.binary_search_by(|(k, _)| k.cmp(key)) {
        Ok(pos) => Some(&*node.entries[pos].1),
        Err(pos) => {
            if node.children.is_empty() {
                None
            } else {
                get_in_node(node.children[pos].load(src), key, src)
            }
        }
    }
}

/// Recursively searches for a key in a node. Returns a shared handle to the value.
fn get_arc_in_node<K: Ord, V>(
    node: &BTreeNode<K, V>,
    key: &K,
    src: Option<&dyn NodeSource<K, V>>,
) -> Option<Arc<V>> {
    match node.entries.binary_search_by(|(k, _)| k.cmp(key)) {
        Ok(pos) => Some(Arc::clone(&node.entries[pos].1)),
        Err(pos) => {
            if node.children.is_empty() {
                None
            } else {
                get_arc_in_node(node.children[pos].load(src), key, src)
            }
        }
    }
}

/// Recursively inserts a key-value pair into a node, potentially splitting it.
fn insert_into_node<K: Ord + Clone, V>(
    node: &Child<K, V>,
    key: K,
    val: Arc<V>,
    src: Option<&dyn NodeSource<K, V>>,
) -> InsertResult<K, V> {
    let node = node.load(src);
    let mut entries = node.entries.clone();

    match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
        Ok(pos) => {
            // Replace existing value.
            entries[pos] = (key, val);
            let children = node.children.clone();
            InsertResult::Fit(Child::resident_new(Arc::new(BTreeNode { entries, children }), src), true)
        }
        Err(pos) => {
            if node.children.is_empty() {
                // Leaf: insert and possibly split.
                entries.insert(pos, (key, val));
                maybe_split(entries, Children::new(), false, src)
            } else {
                // Internal: recurse into child[pos], then merge the result.
                let mut children = node.children.clone();
                match insert_into_node(&children[pos], key, val, src) {
                    InsertResult::Fit(new_child, replaced) => {
                        children[pos] = new_child;
                        InsertResult::Fit(
                            Child::resident_new(Arc::new(BTreeNode { entries, children }), src),
                            replaced,
                        )
                    }
                    InsertResult::Split {
                        left,
                        median,
                        right,
                        replaced,
                    } => {
                        entries.insert(pos, median);
                        children[pos] = left;
                        children.insert(pos + 1, right);
                        maybe_split(entries, children, replaced, src)
                    }
                }
            }
        }
    }
}

/// Wrap entries+children into a node, splitting if entries exceed MAX_KEYS.
///
/// Uses `split_off`/`pop` (moves elements, no cloning) rather than
/// slice-range `to_vec()` — matches the in-place `maybe_split_mut`'s
/// approach; with inline `FixedVec` fields there is no separate heap Vec
/// to "reuse", so both paths just move entries directly into their final
/// homes.
fn maybe_split<K: Clone, V>(
    mut entries: Entries<K, V>,
    mut children: Children<K, V>,
    replaced: bool,
    src: Option<&dyn NodeSource<K, V>>,
) -> InsertResult<K, V> {
    if entries.len() <= MAX_KEYS {
        InsertResult::Fit(Child::resident_new(Arc::new(BTreeNode { entries, children }), src), replaced)
    } else {
        // entries.len() == MAX_KEYS + 1; split at mid.
        let mid = entries.len() / 2;
        let right_entries = entries.split_off(mid + 1); // entries[mid+1..]
        let median = entries.pop().unwrap(); // entries[mid]
        // entries is now entries[..mid] (the left half).
        let right_children = if children.is_empty() {
            Children::new()
        } else {
            children.split_off(mid + 1) // children[mid+1..]
        };

        InsertResult::Split {
            left: Child::resident_new(
                Arc::new(BTreeNode {
                    entries,
                    children,
                }),
                src,
            ),
            median,
            right: Child::resident_new(
                Arc::new(BTreeNode {
                    entries: right_entries,
                    children: right_children,
                }),
                src,
            ),
            replaced,
        }
    }
}

/// A single difference between two versions of a `BTree`.
///
/// Yielded by [`BTree::diff`] in ascending key order. Lifetimes borrow from
/// *both* trees: `Removed` borrows its key from the base tree, the other two
/// from the newer tree.
///
/// `#[derive(Debug)]` bounds both `K: Debug` and `V: Debug` (the latter via
/// `&Arc<V>: Debug`, which requires it) — genuinely needed for the `Added`/
/// `Updated` variants, so this isn't over-restrictive despite `Removed`
/// alone not needing `V: Debug`: derive bounds per type parameter, not per
/// variant.
#[derive(Debug)]
pub enum Change<'a, K, V> {
    /// Key is present in the newer tree and absent from the base.
    Added(&'a K, &'a Arc<V>),
    /// Key is present in both, bound to a different value `Arc`.
    Updated(&'a K, &'a Arc<V>),
    /// Key is present in the base and absent from the newer tree.
    Removed(&'a K),
}

/// In-order cursor over a `BTree` that exposes *subtree* boundaries.
///
/// `BTreeRange` stores `&BTreeNode` frames, which cannot be compared for
/// shared identity; the diff needs the `Child` slot itself (via
/// `Child::same_node`) to detect shared subtrees, so it gets its own cursor.
///
/// A frame's `slot` interleaves children and entries in traversal order:
/// even `slot` means "child `slot / 2` has not been descended into yet",
/// odd `slot` means "entry `slot / 2` is the next entry to yield".
struct DiffCursor<'a, K, V> {
    stack: Vec<(&'a Child<K, V>, usize)>,
    /// The tree's page source, threaded through every `Child::load` this
    /// cursor performs. `None` for every tree built so far.
    src: Option<&'a dyn NodeSource<K, V>>,
    /// Subtrees this cursor has entered via `descend`. Test-only: it is the
    /// load-bearing counter for proving the `Child::same_node` skip in
    /// `BTreeDiff::next` actually fires, rather than just producing correct
    /// output while silently walking every node.
    #[cfg(test)]
    descends: usize,
}

impl<'a, K: Ord + Clone, V> DiffCursor<'a, K, V> {
    fn new(root: &'a Child<K, V>, src: Option<&'a dyn NodeSource<K, V>>) -> Self {
        DiffCursor {
            stack: vec![(root, 0)],
            src,
            #[cfg(test)]
            descends: 0,
        }
    }

    /// The subtree this cursor is about to descend into, if any.
    ///
    /// `None` for a leaf frame or when the next step is an entry rather than
    /// a child — that is the signal the diff loop uses to fall back to a
    /// key-wise comparison.
    fn peek_child(&self) -> Option<&'a Child<K, V>> {
        let (node, slot) = *self.stack.last()?;
        let node = node.load(self.src);
        if node.children.is_empty() || slot % 2 == 1 {
            return None;
        }
        let idx = slot / 2;
        // FixedVec has no `get`; index only within the live prefix.
        if idx < node.children.len() {
            Some(&node.children[idx])
        } else {
            None
        }
    }

    /// Descend into the pending child.
    ///
    /// Only ever called when `peek_entry` has already determined the top
    /// frame's next step is a child (even `slot`, non-leaf node), so
    /// `peek_child` returning `None` here means the node's `children` count
    /// doesn't match its `entries` count — a malformed tree, not a
    /// legitimate "no pending child" state. Silently no-op-ing on that would
    /// leave `slot` unchanged, so `peek_entry`'s loop would call `descend`
    /// again and spin forever instead of failing.
    fn descend(&mut self) {
        let child = self.peek_child();
        debug_assert!(
            child.is_some(),
            "DiffCursor::descend: no pending child on a non-leaf frame — \
             malformed node (children.len() != entries.len() + 1)"
        );
        if let Some(child) = child {
            let last = self.stack.len() - 1;
            self.stack[last].1 += 1;
            self.stack.push((child, 0));
            #[cfg(test)]
            {
                self.descends += 1;
            }
        }
    }

    /// Step over the pending child without visiting any of its keys.
    fn skip_child(&mut self) {
        if self.peek_child().is_some() {
            let last = self.stack.len() - 1;
            self.stack[last].1 += 1;
        }
    }

    /// Advance until the top frame's next step is an entry, then return it.
    ///
    /// Does not consume the entry; call `bump` to move past it.
    fn peek_entry(&mut self) -> Option<(&'a K, &'a Arc<V>)> {
        loop {
            let (node, slot) = *self.stack.last()?;
            let node = node.load(self.src);
            if node.children.is_empty() {
                // Leaf: slots are entries directly, no interleaving.
                if slot < node.entries.len() {
                    let (k, v) = &node.entries[slot];
                    return Some((k, v));
                }
                self.stack.pop();
                continue;
            }
            if slot % 2 == 0 {
                self.descend();
                continue;
            }
            let idx = slot / 2;
            if idx < node.entries.len() {
                let (k, v) = &node.entries[idx];
                return Some((k, v));
            }
            self.stack.pop();
        }
    }

    /// Consume the entry last returned by `peek_entry`.
    fn bump(&mut self) {
        if let Some(last) = self.stack.last_mut() {
            // Leaf frames advance one slot per entry; internal frames advance
            // from odd slot `2i+1` (the entry just yielded) to the next
            // pending child at even slot `2i+2`. Same `+= 1`, two different
            // meanings, so it is spelled out here rather than folded into
            // `peek_entry`.
            last.1 += 1;
        }
    }
}

/// Iterator returned by [`BTree::diff`].
pub struct BTreeDiff<'a, K, V> {
    new: DiffCursor<'a, K, V>,
    base: DiffCursor<'a, K, V>,
}

impl<'a, K: Ord + Clone, V> Iterator for BTreeDiff<'a, K, V> {
    type Item = Change<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Opportunistic skip: when both cursors are poised on the *same*
            // subtree, every key inside it is unchanged by CoW construction,
            // so neither side needs to walk it. This is where the O(changed)
            // behaviour comes from; correctness never depends on it firing.
            match (self.new.peek_child(), self.base.peek_child()) {
                (Some(a), Some(b)) if Child::same_node(a, b) => {
                    self.new.skip_child();
                    self.base.skip_child();
                    continue;
                }
                _ => {}
            }

            return match (self.new.peek_entry(), self.base.peek_entry()) {
                (None, None) => None,
                (Some((k, v)), None) => {
                    self.new.bump();
                    Some(Change::Added(k, v))
                }
                (None, Some((k, _))) => {
                    self.base.bump();
                    Some(Change::Removed(k))
                }
                (Some((nk, nv)), Some((bk, bv))) => match nk.cmp(bk) {
                    Ordering::Less => {
                        self.new.bump();
                        Some(Change::Added(nk, nv))
                    }
                    Ordering::Greater => {
                        self.base.bump();
                        Some(Change::Removed(bk))
                    }
                    Ordering::Equal => {
                        self.new.bump();
                        self.base.bump();
                        if Arc::ptr_eq(nv, bv) {
                            continue;
                        }
                        Some(Change::Updated(nk, nv))
                    }
                },
            };
        }
    }
}

#[cfg(test)]
impl<K: Ord + Clone, V> BTreeDiff<'_, K, V> {
    /// Subtrees descended into by either cursor, summed. Test-only: no
    /// oracle comparing `diff`'s output to a full scan can distinguish an
    /// implementation that skips shared subtrees from one that walks every
    /// node and happens to compute the same answer — this counter is the
    /// only thing that can.
    pub fn nodes_visited(&self) -> usize {
        self.new.descends + self.base.descends
    }
}

/// Outcome of an in-place insert into a node (see `insert_into_node_mut`).
///
/// Unlike `InsertResult`, this does not carry the mutated node — the node is
/// updated in place through the `&mut Child<K, V>` the caller passed. Only the
/// promoted median + new right sibling (on split) and the replace/insert flag
/// need to flow back up.
enum InsertOutcome<K, V> {
    Fit {
        replaced: bool,
    },
    Split {
        median: (K, Arc<V>),
        right: Child<K, V>,
        replaced: bool,
    },
}

/// In-place counterpart to `insert_into_node`. Descends through
/// `Child::make_mut`, so each node is cloned only if it is still shared with
/// another snapshot (copy-on-write preserved) and otherwise mutated directly.
fn insert_into_node_mut<K: Ord + Clone, V>(
    node: &mut Child<K, V>,
    key: K,
    val: Arc<V>,
    src: Option<&dyn NodeSource<K, V>>,
) -> InsertOutcome<K, V> {
    // The one place CoW happens on this path: clones iff `node` is shared.
    let n = node.make_mut(src);

    match n.entries.binary_search_by(|(k, _)| k.cmp(&key)) {
        Ok(pos) => {
            // Replace existing value.
            n.entries[pos] = (key, val);
            InsertOutcome::Fit { replaced: true }
        }
        Err(pos) => {
            if n.children.is_empty() {
                // Leaf: insert and possibly split.
                n.entries.insert(pos, (key, val));
                match maybe_split_mut(n) {
                    None => InsertOutcome::Fit { replaced: false },
                    Some((median, right)) => InsertOutcome::Split {
                        median,
                        right: Child::resident_new(right, src),
                        replaced: false,
                    },
                }
            } else {
                // Internal: recurse into child[pos], then absorb the result.
                match insert_into_node_mut(&mut n.children[pos], key, val, src) {
                    InsertOutcome::Fit { replaced } => InsertOutcome::Fit { replaced },
                    InsertOutcome::Split {
                        median,
                        right,
                        replaced,
                    } => {
                        n.entries.insert(pos, median);
                        n.children.insert(pos + 1, right);
                        match maybe_split_mut(n) {
                            None => InsertOutcome::Fit { replaced },
                            Some((median, right)) => InsertOutcome::Split {
                                median,
                                right: Child::resident_new(right, src),
                                replaced,
                            },
                        }
                    }
                }
            }
        }
    }
}

/// Split `n` in place if it overflowed `MAX_KEYS`. On split, `n` is truncated to
/// the left half and `(median, right_sibling)` is returned; otherwise `None`.
///
/// Uses `split_off`/`pop` so the left half is edited in place, in `n`'s own
/// inline storage — only the right sibling's `Arc::new` allocates.
#[allow(clippy::type_complexity)]
fn maybe_split_mut<K: Clone, V>(
    n: &mut BTreeNode<K, V>,
) -> Option<((K, Arc<V>), Arc<BTreeNode<K, V>>)> {
    if n.entries.len() <= MAX_KEYS {
        return None;
    }
    // entries.len() == MAX_KEYS + 1; split at mid, matching the immutable
    // path exactly so both produce identically-shaped trees.
    let mid = n.entries.len() / 2;
    let right_entries = n.entries.split_off(mid + 1); // entries[mid+1..]
    let median = n.entries.pop().unwrap(); // entries[mid]
    // n.entries is now entries[..mid] (the left half).
    let right_children = if n.children.is_empty() {
        Children::new()
    } else {
        n.children.split_off(mid + 1) // children[mid+1..]
    };
    let right = Arc::new(BTreeNode {
        entries: right_entries,
        children: right_children,
    });
    Some((median, right))
}

/// Recursively deletes a key from a node, potentially triggering rebalancing.
fn delete_from_node<K: Ord + Clone, V>(
    node: &Child<K, V>,
    key: &K,
    src: Option<&dyn NodeSource<K, V>>,
) -> DeleteResult<K, V> {
    let node = node.load(src);
    let pos = node.entries.binary_search_by(|(k, _)| k.cmp(key));

    if node.children.is_empty() {
        // Leaf node.
        match pos {
            Err(_) => DeleteResult::NotFound,
            Ok(i) => {
                let mut entries = node.entries.clone();
                entries.remove(i);
                let underfull = entries.len() < MIN_KEYS;
                DeleteResult::Removed {
                    node: Child::resident_new(
                        Arc::new(BTreeNode {
                            entries,
                            children: Children::new(),
                        }),
                        src,
                    ),
                    underfull,
                }
            }
        }
    } else {
        // Internal node.
        match pos {
            Ok(i) => {
                // Key is in this node: replace it with its in-order successor
                // (leftmost entry of children[i+1]) and delete that successor.
                let (succ, new_right, right_underfull) = remove_leftmost(&node.children[i + 1], src);
                let mut entries = node.entries.clone();
                let mut children = node.children.clone();
                entries[i] = succ;
                children[i + 1] = new_right;
                if right_underfull {
                    fix_underfull_child(&mut entries, &mut children, i + 1, src);
                }
                let underfull = entries.len() < MIN_KEYS;
                DeleteResult::Removed {
                    node: Child::resident_new(Arc::new(BTreeNode { entries, children }), src),
                    underfull,
                }
            }
            Err(child_idx) => {
                // Key is in a subtree.
                match delete_from_node(&node.children[child_idx], key, src) {
                    DeleteResult::NotFound => DeleteResult::NotFound,
                    DeleteResult::Removed {
                        node: new_child,
                        underfull,
                    } => {
                        let mut entries = node.entries.clone();
                        let mut children = node.children.clone();
                        children[child_idx] = new_child;
                        if underfull {
                            fix_underfull_child(&mut entries, &mut children, child_idx, src);
                        }
                        let node_underfull = entries.len() < MIN_KEYS;
                        DeleteResult::Removed {
                            node: Child::resident_new(Arc::new(BTreeNode { entries, children }), src),
                            underfull: node_underfull,
                        }
                    }
                }
            }
        }
    }
}

/// Remove and return the leftmost (minimum-key) entry from the subtree.
/// Returns `(entry, new_root, is_underfull)`.
#[allow(clippy::type_complexity)]
fn remove_leftmost<K: Ord + Clone, V>(
    node: &Child<K, V>,
    src: Option<&dyn NodeSource<K, V>>,
) -> ((K, Arc<V>), Child<K, V>, bool) {
    let node = node.load(src);
    if node.children.is_empty() {
        let mut entries = node.entries.clone();
        let first = entries.remove(0);
        let underfull = entries.len() < MIN_KEYS;
        (
            first,
            Child::resident_new(
                Arc::new(BTreeNode {
                    entries,
                    children: Children::new(),
                }),
                src,
            ),
            underfull,
        )
    } else {
        let (entry, new_first_child, child_underfull) = remove_leftmost(&node.children[0], src);
        let mut entries = node.entries.clone();
        let mut children = node.children.clone();
        children[0] = new_first_child;
        if child_underfull {
            fix_underfull_child(&mut entries, &mut children, 0, src);
        }
        let underfull = entries.len() < MIN_KEYS;
        (entry, Child::resident_new(Arc::new(BTreeNode { entries, children }), src), underfull)
    }
}

/// Rebalance an underfull child at `idx` by rotating from a sibling or merging.
fn fix_underfull_child<K: Ord + Clone, V>(
    entries: &mut Entries<K, V>,
    children: &mut Children<K, V>,
    idx: usize,
    src: Option<&dyn NodeSource<K, V>>,
) {
    if idx > 0 && children[idx - 1].load(src).entries.len() > MIN_KEYS {
        rotate_right(entries, children, idx, src);
    } else if idx + 1 < children.len() && children[idx + 1].load(src).entries.len() > MIN_KEYS {
        rotate_left(entries, children, idx, src);
    } else if idx > 0 {
        merge_with_left(entries, children, idx, src);
    } else {
        merge_with_right(entries, children, idx, src);
    }
}

/// Rotates an entry from the left sibling into the current child.
///
/// Mutates the two siblings in place via [`Child::make_mut`]: each is cloned
/// only if still shared with an older snapshot, otherwise edited directly.
/// `split_at_mut` (via `as_mut_slice`) yields disjoint `&mut` handles to the
/// two adjacent children at once.
fn rotate_right<K: Clone, V>(
    entries: &mut Entries<K, V>,
    children: &mut Children<K, V>,
    idx: usize,
    src: Option<&dyn NodeSource<K, V>>,
) {
    let (left_part, right_part) = children.as_mut_slice().split_at_mut(idx);
    let left = left_part[idx - 1].make_mut(src);
    let right = right_part[0].make_mut(src);

    // Steal the last entry (and trailing child) of the left sibling.
    let stolen = left.entries.pop().unwrap();
    let stolen_child = if left.children.is_empty() {
        None
    } else {
        Some(left.children.pop().unwrap())
    };
    // The stolen entry becomes the new separator; the old separator descends
    // into the front of the right child.
    let separator = std::mem::replace(&mut entries[idx - 1], stolen);
    right.entries.insert(0, separator);
    if let Some(sc) = stolen_child {
        right.children.insert(0, sc);
    }
}

/// Rotates an entry from the right sibling into the current child.
///
/// In-place counterpart of `rotate_right` — see its docs for the CoW reasoning.
fn rotate_left<K: Clone, V>(
    entries: &mut Entries<K, V>,
    children: &mut Children<K, V>,
    idx: usize,
    src: Option<&dyn NodeSource<K, V>>,
) {
    let (left_part, right_part) = children.as_mut_slice().split_at_mut(idx + 1);
    let left = left_part[idx].make_mut(src);
    let right = right_part[0].make_mut(src);

    // Steal the first entry (and leading child) of the right sibling.
    let stolen = right.entries.remove(0);
    let stolen_child = if right.children.is_empty() {
        None
    } else {
        Some(right.children.remove(0))
    };
    // The stolen entry becomes the new separator; the old separator descends
    // onto the end of the left child.
    let separator = std::mem::replace(&mut entries[idx], stolen);
    left.entries.push(separator);
    if let Some(sc) = stolen_child {
        left.children.push(sc);
    }
}

/// Merges an underfull child with its left sibling.
///
/// The absorbing (left) sibling is opened with [`Child::make_mut`] and edited
/// in place; see [`absorb`] for how the (right) sibling's contents are
/// moved/cloned into it.
fn merge_with_left<K: Clone, V>(
    entries: &mut Entries<K, V>,
    children: &mut Children<K, V>,
    idx: usize,
    src: Option<&dyn NodeSource<K, V>>,
) {
    let separator = entries.remove(idx - 1);
    let right = children.remove(idx);
    let left = children[idx - 1].make_mut(src);
    left.entries.push(separator);
    absorb(left, right, src);
}

/// Merges an underfull child with its right sibling.
///
/// In-place counterpart of `merge_with_left` — see its docs.
fn merge_with_right<K: Clone, V>(
    entries: &mut Entries<K, V>,
    children: &mut Children<K, V>,
    idx: usize,
    src: Option<&dyn NodeSource<K, V>>,
) {
    let separator = entries.remove(idx);
    let right = children.remove(idx + 1);
    let left = children[idx].make_mut(src);
    left.entries.push(separator);
    absorb(left, right, src);
}

/// Appends `right`'s entries and children onto `left`. `left` must already
/// carry the descended separator as its last entry.
///
/// `right` is faulted in (if needed) and moved out via `Arc::try_unwrap`,
/// falling back to a clone if it is still shared with a snapshot. The
/// `Child` slot itself owns one strong count on the loaded `Arc`, so it must
/// be dropped *before* `try_unwrap` — `load_arc` bumps the count to hand
/// back an owned `Arc`, and if the slot's own count is still live at that
/// point, `try_unwrap` always sees `strong_count >= 2` and always takes the
/// clone branch, even when no snapshot shares the node. `drop(right)` below
/// releases that count first, so the fast (move) path actually fires
/// whenever nothing else holds the node — see
/// `merge_moves_unshared_sibling_instead_of_cloning` for a regression test.
fn absorb<K: Clone, V>(left: &mut BTreeNode<K, V>, right: Child<K, V>, src: Option<&dyn NodeSource<K, V>>) {
    let arc = right.load_arc(src);
    drop(right); // release the slot's own count first, so try_unwrap can succeed
    let rn = Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone());
    left.entries.extend(rn.entries);
    left.children.extend(rn.children);
}

/// In-place counterpart to `delete_from_node`. Descends through
/// `Child::make_mut`, so each node is cloned only if it is still shared with
/// another snapshot (copy-on-write preserved) and otherwise mutated directly.
/// Reuses the existing rebalance helpers (`fix_underfull_child` et al.),
/// which already mutate the parent's `entries`/`children` in place.
fn delete_from_node_mut<K: Ord + Clone, V>(
    node: &mut Child<K, V>,
    key: &K,
    src: Option<&dyn NodeSource<K, V>>,
) -> DeleteOutcome {
    let n = node.make_mut(src);
    let pos = n.entries.binary_search_by(|(k, _)| k.cmp(key));

    if n.children.is_empty() {
        // Leaf.
        match pos {
            Err(_) => DeleteOutcome::NotFound,
            Ok(i) => {
                n.entries.remove(i);
                DeleteOutcome::Removed {
                    underfull: n.entries.len() < MIN_KEYS,
                }
            }
        }
    } else {
        match pos {
            Ok(i) => {
                // Key here: replace with in-order successor from child[i+1].
                let (succ, right_underfull) = remove_leftmost_mut(&mut n.children[i + 1], src);
                n.entries[i] = succ;
                if right_underfull {
                    fix_underfull_child(&mut n.entries, &mut n.children, i + 1, src);
                }
                DeleteOutcome::Removed {
                    underfull: n.entries.len() < MIN_KEYS,
                }
            }
            Err(child_idx) => match delete_from_node_mut(&mut n.children[child_idx], key, src) {
                DeleteOutcome::NotFound => DeleteOutcome::NotFound,
                DeleteOutcome::Removed { underfull } => {
                    if underfull {
                        fix_underfull_child(&mut n.entries, &mut n.children, child_idx, src);
                    }
                    DeleteOutcome::Removed {
                        underfull: n.entries.len() < MIN_KEYS,
                    }
                }
            },
        }
    }
}

/// In-place counterpart to `remove_leftmost`: removes and returns the
/// minimum-key entry from the subtree, mutating shared nodes only via CoW.
fn remove_leftmost_mut<K: Ord + Clone, V>(
    node: &mut Child<K, V>,
    src: Option<&dyn NodeSource<K, V>>,
) -> ((K, Arc<V>), bool) {
    let n = node.make_mut(src);
    if n.children.is_empty() {
        let first = n.entries.remove(0);
        (first, n.entries.len() < MIN_KEYS)
    } else {
        let (entry, child_underfull) = remove_leftmost_mut(&mut n.children[0], src);
        if child_underfull {
            fix_underfull_child(&mut n.entries, &mut n.children, 0, src);
        }
        (entry, n.entries.len() < MIN_KEYS)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Bulk-load builder (private)
//
// `BulkBuilder` walks a strictly-ascending iterator of `(K, Arc<V>)` pairs in a
// single O(N) pass, packing each level densely (`MAX_KEYS` per node) and
// promoting on overflow. `finish` walks the levels bottom-up, redistributing
// the rightmost (possibly underfull) node with its left sibling so every node
// satisfies `MIN_KEYS` after the build.
//
// `from_sorted` and these helpers are pure additions for Phase 1 of the
// bulk-load feature; Phase 2 (index backfill) and Phase 3 (table build) wire
// them into `src/index.rs` and `src/table.rs`. Until then they are exercised
// by tests only.
// ---------------------------------------------------------------------------

struct LevelBuilder<K, V> {
    entries: Vec<(K, Arc<V>)>,
    children: Vec<Child<K, V>>,
}

impl<K, V> LevelBuilder<K, V> {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(MAX_KEYS),
            children: Vec::with_capacity(MAX_KEYS + 1),
        }
    }
}

struct BulkBuilder<K, V> {
    levels: Vec<LevelBuilder<K, V>>,
    len: usize,
    last_key: Option<K>,
    /// Carried from the tree `extend_from_sorted` is appending to (`None`
    /// for a fresh `from_sorted` build). `seed_from_spine` clones — without
    /// faulting — the non-rightmost children at each spine level, so a
    /// sibling the builder later needs to read (`redistribute_tail`'s
    /// borrow, `fix_right_spine_tail`'s residual-underflow walk) can be an
    /// on-disk slot; this is what lets those reads fault instead of
    /// panicking, and what lets the tree `finish()` produces still fault in
    /// whatever it didn't touch.
    source: Option<Arc<dyn NodeSource<K, V>>>,
}

impl<K: Ord + Clone, V> BulkBuilder<K, V> {
    fn new() -> Self {
        Self {
            levels: vec![LevelBuilder::new()],
            len: 0,
            last_key: None,
            source: None,
        }
    }

    /// Reconstruct builder state as if it had just consumed `tree`'s entries,
    /// by unzipping the right spine: the spine node at each level becomes that
    /// level's under-construction node — its entries copied, its children all
    /// attached as shared `Arc`s EXCEPT the rightmost, whose slot stays open
    /// (that child *is* the next spine level down). This is exactly the
    /// builder's mid-build invariant (`children.len() == entries.len()` on
    /// internal levels; the open slot is filled by `attach_child` or the
    /// `finish()` carry).
    ///
    /// Snapshot isolation: spine nodes are only *read* here and `finish()`
    /// builds fresh replacements, so trees sharing nodes with `tree` are
    /// never touched. `redistribute_tail` is safe on seeded state for two
    /// independent reasons: (a) it copies the popped sibling (`to_vec`) and
    /// builds a fresh node, never mutating through the Arc; and (b) it can
    /// only ever pop a freshly-frozen FULL node anyway — a level that is
    /// underfull at finish() with a parent sibling present must have frozen
    /// during the build (a never-frozen seeded level keeps its original
    /// \>= MIN_KEYS entries and only grows), and freezes only happen at
    /// MAX_KEYS, which also keeps its `total > 2 * MIN_KEYS` split sound.
    fn seed_from_spine(tree: &BTree<K, V>) -> Self {
        if tree.is_empty() {
            // No spine to unzip, but the empty tree can still carry a
            // source (e.g. delete-all on a paged tree) — `Self::new()`
            // alone would silently drop it, leaving the tree `finish()`
            // produces unable to fault anything it doesn't itself build.
            return Self {
                source: tree.source.clone(),
                ..Self::new()
            };
        }
        let src = tree.source.as_deref();
        let mut spine: Vec<&BTreeNode<K, V>> = Vec::new();
        let mut node = tree.root.load(src);
        loop {
            spine.push(node);
            match node.children.last() {
                Some(c) => node = c.load(src),
                None => break,
            }
        }
        // spine is root->leaf; builder levels are leaf(0)->root, so reverse.
        let mut levels = Vec::with_capacity(spine.len());
        for (i, spine_node) in spine.iter().rev().enumerate() {
            let mut lv = LevelBuilder::new();
            lv.entries
                .extend(spine_node.entries.iter().map(|(k, v)| (k.clone(), Arc::clone(v))));
            if i > 0 {
                let n = spine_node.children.len();
                lv.children.extend(spine_node.children.iter().take(n - 1).cloned());
            }
            levels.push(lv);
        }
        let last_key = spine
            .last()
            .expect("non-empty tree has a spine leaf")
            .entries
            .last()
            .map(|(k, _)| k.clone());
        Self {
            levels,
            len: tree.len,
            last_key,
            source: tree.source.clone(),
        }
    }

    fn push(&mut self, k: K, v: Arc<V>) {
        if let Some(prev) = &self.last_key {
            debug_assert!(*prev < k, "from_sorted: input not strictly ascending");
        }
        self.last_key = Some(k.clone());
        self.len += 1;

        // If the leaf is already at capacity, freeze it and promote the new
        // entry up as the separator between this leaf and the next.
        if self.levels[0].entries.len() == MAX_KEYS {
            let frozen = freeze_leaf(&mut self.levels[0], self.source.as_deref());
            self.attach_child(1, frozen, k, v);
        } else {
            self.levels[0].entries.push((k, v));
        }
    }

    /// Attach `child` as the next child of `levels[level]`, using `(sep_k, sep_v)`
    /// as the separator placed *after* the previous child. If `levels[level]` is
    /// also at capacity, freeze and recurse to the level above.
    fn attach_child(&mut self, level: usize, child: Child<K, V>, sep_k: K, sep_v: Arc<V>) {
        if level >= self.levels.len() {
            self.levels.push(LevelBuilder::new());
        }
        let lv = &mut self.levels[level];
        // On entry, `children.len() == entries.len()` (after the prior freeze
        // pushed a child up without yet attaching the separator). We append the
        // new child first, then the separator; the steady mid-build state
        // after that is still `children.len() == entries.len()`, with the
        // rightmost child slot left open until the next freeze (or
        // finish()'s carry) fills it.
        lv.children.push(child);
        if lv.entries.len() == MAX_KEYS {
            let frozen = freeze_internal(lv, self.source.as_deref());
            self.attach_child(level + 1, frozen, sep_k, sep_v);
        } else {
            lv.entries.push((sep_k, sep_v));
        }
    }

    fn finish(mut self) -> BTree<K, V> {
        // Walk levels bottom-up. At each level, redistribute the rightmost
        // (partial) node with its left sibling if it would otherwise be
        // underfull, then freeze the partial node and attach it as the
        // rightmost child of the level above.
        let mut carry: Option<Child<K, V>> = None;
        // Entries popped from a level that ended up "unclosed" (see below),
        // to be re-inserted individually once the tree is otherwise valid.
        let mut pending_reinsert: Vec<(K, Arc<V>)> = Vec::new();
        // Tracks the height of whatever `carry` currently holds: 0 the
        // moment the leaf level produces its node, +1 every time a level
        // above genuinely *wraps* what it carried in a new node. The
        // "collapse" branch below (an unclosed topmost level popping down
        // to its one child) deliberately does *not* bump this — it forwards
        // a lower level's node without adding a level, exactly like the
        // root-collapse case `remove`/`remove_mut` track the same way.
        let mut computed_height: usize = 0;

        for level in 0..self.levels.len() {
            let is_leaf_level = level == 0;

            if let Some(child) = carry.take() {
                self.levels[level].children.push(child);
            }

            // Tail rebalance: if a parent level exists with a previously-frozen
            // sibling, and our partial node is underfull, redistribute. This
            // also covers the edge case where the leaf level was fully drained
            // by a promotion (e.g. exactly `MAX_KEYS + 1` entries) — we still
            // borrow from the sibling so the parent ends up with a proper
            // rightmost child. The topmost level has no parent sibling and is
            // allowed to be partial (it becomes the root).
            //
            // Skip *internal* levels that are completely empty (no entries
            // *and* no children — the state right before the `continue`
            // below): there is no "own" child here for redistribute_tail's
            // merge to fold back in, so it would try to re-split the popped
            // sibling's `entries + 1 separator` across zero children of ours,
            // leaving the result one child short of `entries.len() + 1`. This
            // is reachable from a plain `from_sorted` whose input lands
            // exactly on an internal-level freeze boundary (e.g. `T*T`
            // leaf-freezes' worth of entries).
            //
            // Leaves are exempt from this guard: a fully-drained leaf (e.g.
            // input landing exactly on `MAX_KEYS + 1` entries) still *must*
            // redistribute when it has a parent sibling, because it's the
            // parent's only way to obtain a valid (non-empty) rightmost
            // child — a leaf's `children` is always empty regardless, so the
            // merge never touches children and the empty-entries case is
            // safe (the sibling's entries simply get resplit between the two
            // leaves).
            let is_degenerate_empty_internal = !is_leaf_level
                && self.levels[level].entries.is_empty()
                && self.levels[level].children.is_empty();
            let has_parent_sibling = self
                .levels
                .get(level + 1)
                .is_some_and(|p| !p.children.is_empty());
            if has_parent_sibling
                && !is_degenerate_empty_internal
                && self.levels[level].entries.len() < MIN_KEYS
            {
                // An internal level can still reach here "unclosed"
                // (`children.len() == entries.len()`, the steady mid-build
                // state described in `attach_child`): its carry from the
                // level below (taken at the top of this iteration) was
                // `None`, because input ran out exactly on a nested cascade
                // boundary where the level below finished completely empty.
                // The level's last entry is then a dangling separator with
                // no right child. `redistribute_tail`'s arity math assumes a
                // *closed* level (`children.len() == entries.len() + 1`), so
                // pop that dangling entry into `pending_reinsert` first — the
                // sibling it borrows from is always a freshly-frozen FULL
                // node (see `seed_from_spine`'s doc comment), so the merged
                // total after the pop is always `>= MAX_KEYS + 1 >
                // 2 * MIN_KEYS`, keeping the post-redistribute split sound.
                if !is_leaf_level {
                    let lv = &mut self.levels[level];
                    if !lv.entries.is_empty() && lv.children.len() == lv.entries.len() {
                        pending_reinsert.push(lv.entries.pop().unwrap());
                    }
                }
                redistribute_tail::<K, V>(&mut self.levels, level, self.source.as_deref());
            }

            let lv = &mut self.levels[level];
            if lv.entries.is_empty() && lv.children.is_empty() {
                continue;
            }

            let node = if is_leaf_level {
                computed_height = 0;
                Child::resident_new(
                    Arc::new(BTreeNode {
                        entries: std::mem::take(&mut lv.entries).into_iter().collect(),
                        children: Children::new(),
                    }),
                    self.source.as_deref(),
                )
            } else {
                let mut entries = std::mem::take(&mut lv.entries);
                let mut children = std::mem::take(&mut lv.children);
                // "Unclosed": `children.len() == entries.len()` means the
                // level's reserved final child slot (see `attach_child`)
                // never got filled — nothing arrived from below to carry
                // into it, because input ran out exactly on a cascading
                // multi-level freeze boundary (e.g. `T*T` leaf-freezes'
                // worth of entries: the very last pushed key is promoted as
                // a separator all the way up through however many levels
                // simultaneously hit `MAX_KEYS` on that same push). That
                // last entry is real data (the single largest key seen so
                // far) with nowhere structurally valid to point a right
                // child at; pop it and re-insert it after the tree is
                // otherwise built, via the ordinary (self-balancing)
                // `insert_arc_mut` path.
                if !entries.is_empty() && children.len() == entries.len() {
                    pending_reinsert.push(entries.pop().unwrap());
                }
                debug_assert_eq!(children.len(), entries.len() + 1);
                if entries.is_empty() && level + 1 == self.levels.len() {
                    // Popping the dangling entry above can leave the
                    // *topmost* level with zero entries and its one
                    // remaining child. Collapse here exactly like the root
                    // collapse `remove`/`remove_mut` perform after a delete
                    // (dropping a level uniformly changes the whole tree's
                    // height, unlike collapsing an arbitrary intermediate
                    // level, which would desync that one branch's leaf
                    // depth from the rest of the tree).
                    //
                    // Known narrow limitation: an *intermediate* level can
                    // itself end up with zero entries and one child (not
                    // collapsed, by the above rule) if input ends just past
                    // a *nested* cascade — e.g. `(MAX_KEYS + 1)^3 + 1`, where
                    // levels 0-2 all freeze together on the second-to-last
                    // push and the final single push then pads through
                    // multiple empty levels with nothing to redistribute
                    // against. That leaves a genuinely underfull (though
                    // structurally well-formed) non-root node, caught by
                    // `check_invariants`'s `MIN_KEYS` check rather than
                    // silently mis-shaping the tree. Requires input on the
                    // order of `(MAX_KEYS + 1)^3` (~2M at T=64) to reach;
                    // out of scope here (see `from_sorted_exact_cascade_boundary`
                    // for the one- and two-level cascades this does handle).
                    debug_assert_eq!(children.len(), 1);
                    children.pop().unwrap()
                } else {
                    computed_height += 1;
                    Child::resident_new(
                        Arc::new(BTreeNode {
                            entries: entries.into_iter().collect(),
                            children: children.into_iter().collect(),
                        }),
                        self.source.as_deref(),
                    )
                }
            };
            carry = Some(node);
        }

        let mut root = carry.unwrap_or_else(|| {
            Child::resident_new(
                Arc::new(BTreeNode {
                    entries: Entries::new(),
                    children: Children::new(),
                }),
                self.source.as_deref(),
            )
        });
        fix_right_spine_tail(&mut root, self.source.as_deref());
        let len = self.len - pending_reinsert.len();
        let mut tree = BTree {
            root,
            len,
            height: computed_height,
            source: self.source.clone(),
        };
        for (k, v) in pending_reinsert {
            tree.insert_arc_mut(k, v);
        }
        tree
    }
}

/// Fix any residual underflow left on the tree's right spine after the
/// per-level tail rebalance above.
///
/// `redistribute_tail` only sees a sibling to borrow from if one is still
/// sitting in the *immediate* parent level's builder state. When an
/// intermediate level had already frozen and reset (its own sibling already
/// promoted further up, out of that level's bookkeeping) with no further
/// pushes landing in it before the build ended, `has_parent_sibling` is
/// correctly `false` for the level below — but the leaf (or node) built
/// there can still end up genuinely underfull, and no ancestor's
/// entries/children reshuffle can repair *its* entry count: only a real
/// leaf-to-leaf (or node-to-node) merge fixes that, which requires an actual
/// sibling pointer, not builder-level bookkeeping. This walks the *finished*
/// tree's right spine bottom-up and repairs any such underfull node with the
/// same rotate/merge machinery `remove_mut` uses on real sibling nodes,
/// independent of how the builder's per-level state got there. No-op if the
/// tail is already balanced. Safe to mutate in place: every node on this
/// spine was freshly built by this `finish()` call, so `Child::make_mut`
/// never clones.
///
/// Returns whether `node` itself is now underfull (ignored by the caller at
/// the root, which has no `MIN_KEYS` floor).
fn fix_right_spine_tail<K: Ord + Clone, V>(
    node: &mut Child<K, V>,
    src: Option<&dyn NodeSource<K, V>>,
) -> bool {
    // `make_mut_quiet`/`load_quiet`, not the marking variants: this walk is
    // internal tree-shape bookkeeping run while *constructing* the tree
    // (from `BulkBuilder::finish`), not a real workload access — it must
    // not leave the freshly-built right spine (down to and including the
    // rightmost leaf) looking "recently used" to a later `demote_leaves`
    // pass just because the builder touched it once at build time.
    let n = node.make_mut_quiet(src);
    if n.children.is_empty() {
        return n.entries.len() < MIN_KEYS;
    }
    let mut last = n.children.len() - 1;
    let child_underfull = fix_right_spine_tail(&mut n.children[last], src);
    // A single-child node (no sibling of its own to rotate/merge with) has
    // no fix available at this level; the underflow just propagates to our
    // own `entries.len() < MIN_KEYS` check below, for our own parent to
    // handle against our (real) sibling.
    if child_underfull {
        while n.children.len() > 1 && n.children[last].load_quiet(src).entries.len() < MIN_KEYS {
            fix_underfull_child(&mut n.entries, &mut n.children, last, src);
            last = n.children.len() - 1;
        }
    }
    n.entries.len() < MIN_KEYS
}

/// Rebalance the underfull partial node at `levels[level]` with its left
/// sibling — the most recently-frozen node sitting at the tail of
/// `levels[level + 1].children`. Pops the sibling and the separator that
/// linked them, merges everything in order, then splits the merged sequence
/// in half so both resulting nodes satisfy `MIN_KEYS`. The new left node and
/// separator go back into the parent; the partial node receives the right
/// half.
///
/// Dirty-bytes accounting note (task12 fix round 1): when `src` is `Some`
/// (a live `extend_from_sorted` append), the popped `sibling` may itself
/// have already been `Child::resident_new`-credited — by an earlier
/// `freeze_leaf`/`freeze_internal` call in this same build — and is
/// discarded here (only its *contents* survive, folded into `new_left`).
/// That credit is not un-charged: nothing here can tell "a node this exact
/// build round just froze" apart from "a `seed_from_spine` clone of an
/// unrelated already-dirty node" using only the `Child`'s own state. The
/// result is a small, bounded (at most one `Child::NODE_BYTES` per
/// rebalanced level, not per row) permanent over-credit into `dirty_bytes`
/// — see `tests/paged_checkpointer.rs`'s
/// `dirty_bytes_trigger_fires_for_newly_created_nodes_on_a_live_attached_table`
/// for where this was found and why it's an accepted imprecision rather
/// than a bug to chase further.
fn redistribute_tail<K: Clone, V>(levels: &mut [LevelBuilder<K, V>], level: usize, src: Option<&dyn NodeSource<K, V>>) {
    let is_leaf_level = level == 0;
    let (lower, upper) = levels.split_at_mut(level + 1);
    let lv = &mut lower[level];
    let parent = &mut upper[0]; // levels[level + 1]

    let sibling = parent
        .children
        .pop()
        .expect("redistribute_tail: no sibling");
    let separator = parent
        .entries
        .pop()
        .expect("redistribute_tail: no separator");
    // `sibling` is either one the builder built itself (`Child::resident`,
    // always in-memory) or a clone of a non-rightmost spine node from
    // `seed_from_spine` — which, since the builder now carries the input
    // tree's `source` (see `BulkBuilder::source`), can be an on-disk slot;
    // `src` is what lets this fault it in instead of panicking.
    let sibling = sibling.load(src);

    // Reconstruct the full ordered sequence: sibling.entries ++ separator ++ lv.entries.
    let mut merged_entries: Vec<(K, Arc<V>)> = sibling.entries.to_vec();
    merged_entries.push(separator);
    merged_entries.append(&mut lv.entries);

    let merged_children: Vec<Child<K, V>> = if is_leaf_level {
        vec![]
    } else {
        let mut c: Vec<Child<K, V>> = sibling.children.to_vec();
        c.append(&mut lv.children);
        c
    };

    // Split the merged sequence around a new separator. We promote the entry
    // at index `split_at`. The left node gets entries [0..split_at), the right
    // node gets entries (split_at..total). For internal nodes, the children
    // list is split at `split_at + 1` so the left node owns one more child
    // than its entry count.
    let total = merged_entries.len();
    debug_assert!(total > 2 * MIN_KEYS);
    let split_at = total / 2;
    let mut right_entries = merged_entries.split_off(split_at);
    let new_separator = right_entries.remove(0);
    let new_left_entries = merged_entries;

    let (new_left_children, new_right_children) = if is_leaf_level {
        (vec![], vec![])
    } else {
        let mut left_c = merged_children;
        let right_c = left_c.split_off(split_at + 1);
        (left_c, right_c)
    };

    debug_assert!(new_left_entries.len() >= MIN_KEYS);
    debug_assert!(new_left_entries.len() <= MAX_KEYS);
    debug_assert!(right_entries.len() >= MIN_KEYS);
    debug_assert!(right_entries.len() <= MAX_KEYS);
    if !is_leaf_level {
        debug_assert_eq!(new_left_children.len(), new_left_entries.len() + 1);
        debug_assert_eq!(new_right_children.len(), right_entries.len() + 1);
    }

    let new_left = Child::resident_new(
        Arc::new(BTreeNode {
            entries: new_left_entries.into_iter().collect(),
            children: new_left_children.into_iter().collect(),
        }),
        src,
    );

    parent.children.push(new_left);
    parent.entries.push(new_separator);
    lv.entries = right_entries;
    lv.children = new_right_children;
}

/// `src` is `BulkBuilder::source`: `None` for a from-scratch `from_sorted`
/// build (nothing to credit — there is no checkpoint yet to owe bytes to),
/// `Some` when `extend_from_sorted` is appending onto an already-attached
/// live tree (the frozen node is exactly as new to the checkpoint as any
/// other freshly split node on the ordinary insert path).
fn freeze_leaf<K, V>(lv: &mut LevelBuilder<K, V>, src: Option<&dyn NodeSource<K, V>>) -> Child<K, V> {
    Child::resident_new(
        Arc::new(BTreeNode {
            entries: std::mem::take(&mut lv.entries).into_iter().collect(),
            children: Children::new(),
        }),
        src,
    )
}

/// See [`freeze_leaf`]'s doc for `src`.
fn freeze_internal<K, V>(lv: &mut LevelBuilder<K, V>, src: Option<&dyn NodeSource<K, V>>) -> Child<K, V> {
    let entries = std::mem::take(&mut lv.entries);
    let children = std::mem::take(&mut lv.children);
    debug_assert_eq!(children.len(), entries.len() + 1);
    Child::resident_new(
        Arc::new(BTreeNode {
            entries: entries.into_iter().collect(),
            children: children.into_iter().collect(),
        }),
        src,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn insert_range(start: u64, end: u64) -> BTree<u64, u64> {
        let mut t = BTree::new();
        for i in start..=end {
            t = t.insert(i, i * 10);
        }
        t
    }

    #[test]
    fn empty_tree_has_len_zero_and_is_empty() {
        let t: BTree<u64, u64> = BTree::new();
        assert_eq!(t.len(), 0);
        assert!(t.is_empty());
    }

    #[test]
    fn get_on_empty_returns_none() {
        let t: BTree<u64, u64> = BTree::new();
        assert_eq!(t.get(&1), None);
    }

    #[test]
    fn insert_single_key_get_returns_some() {
        let t = BTree::new().insert(1u64, 100u64);
        assert_eq!(t.get(&1), Some(&100));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn insert_many_keys_in_order_all_readable() {
        let t = insert_range(1, 20);
        assert_eq!(t.len(), 20);
        for i in 1u64..=20 {
            assert_eq!(t.get(&i), Some(&(i * 10)));
        }
    }

    #[test]
    fn insert_many_keys_reverse_order_all_readable() {
        let mut t = BTree::new();
        for i in (1u64..=20).rev() {
            t = t.insert(i, i * 10);
        }
        for i in 1u64..=20 {
            assert_eq!(t.get(&i), Some(&(i * 10)));
        }
    }

    #[test]
    fn get_absent_key_returns_none() {
        let t = insert_range(1, 5);
        assert_eq!(t.get(&99), None);
    }

    #[test]
    fn insert_replaces_existing_value() {
        let t1 = BTree::new().insert(1u64, 100u64);
        let t2 = t1.insert(1, 999);
        assert_eq!(t2.get(&1), Some(&999));
        assert_eq!(t2.len(), 1); // len unchanged on replace
    }

    #[test]
    fn insert_is_structurally_immutable() {
        let t1 = insert_range(1, 5);
        let t2 = t1.insert(10, 100);
        // t1 is unchanged
        assert_eq!(t1.get(&10), None);
        assert_eq!(t1.len(), 5);
        // t2 has the new key
        assert_eq!(t2.get(&10), Some(&100));
        assert_eq!(t2.len(), 6);
    }

    #[test]
    fn clone_is_independent_of_original() {
        let t1 = insert_range(1, 5);
        let t2 = t1.clone();
        let t3 = t1.insert(6, 60); // modify original
        // t2 (the clone) is unaffected
        assert_eq!(t2.get(&6), None);
        assert_eq!(t2.len(), 5);
        // t3 has the new key, t1 does not
        assert_eq!(t3.get(&6), Some(&60));
        assert_eq!(t1.get(&6), None);
    }

    #[test]
    fn root_split_insert_100_keys_all_readable() {
        // With MAX_KEYS=63 a split occurs around 64 inserts.
        let t = insert_range(1, 100);
        assert_eq!(t.len(), 100);
        for i in 1u64..=100 {
            assert_eq!(t.get(&i), Some(&(i * 10)));
        }
    }

    #[test]
    fn remove_existing_key_original_unchanged() {
        let t1 = insert_range(1, 5);
        let t2 = t1.remove(&3).unwrap();
        assert_eq!(t1.get(&3), Some(&30)); // original unchanged
        assert_eq!(t2.get(&3), None);
        assert_eq!(t2.len(), 4);
    }

    #[test]
    fn remove_absent_key_returns_key_not_found() {
        let t = insert_range(1, 5);
        assert!(matches!(t.remove(&99), Err(Error::KeyNotFound)));
    }

    #[test]
    fn remove_all_keys_tree_becomes_empty() {
        let mut t = insert_range(1, 10);
        for i in 1u64..=10 {
            t = t.remove(&i).unwrap();
        }
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn remove_triggers_rebalance_tree_stays_correct() {
        // Insert enough keys to force multi-level tree, then delete many to
        // trigger merges and rotations.
        let mut t = insert_range(1, 200);
        let keys_to_remove: Vec<u64> = (1..=150).collect();
        for &k in &keys_to_remove {
            t = t.remove(&k).unwrap();
        }
        assert_eq!(t.len(), 200 - keys_to_remove.len());
        for &k in &keys_to_remove {
            assert_eq!(t.get(&k), None);
        }
        for i in 1u64..=200 {
            if !keys_to_remove.contains(&i) {
                assert_eq!(t.get(&i), Some(&(i * 10)));
            }
        }
    }

    #[test]
    fn len_tracks_insert_and_remove() {
        let t0: BTree<u64, u64> = BTree::new();
        assert_eq!(t0.len(), 0);
        let t1 = t0.insert(1, 10);
        assert_eq!(t1.len(), 1);
        let t2 = t1.insert(2, 20);
        assert_eq!(t2.len(), 2);
        let t3 = t2.remove(&1).unwrap();
        assert_eq!(t3.len(), 1);
    }

    #[test]
    fn range_empty_tree_yields_nothing() {
        let t: BTree<u64, u64> = BTree::new();
        let results: Vec<_> = t.range(..).collect();
        assert!(results.is_empty());
    }

    #[test]
    fn range_full_yields_all_keys_in_order() {
        let t = insert_range(1, 10);
        let results: Vec<_> = t.range(..).collect();
        assert_eq!(results.len(), 10);
        for (i, &(k, v)) in results.iter().enumerate() {
            assert_eq!(*k, (i + 1) as u64);
            assert_eq!(*v, ((i + 1) as u64) * 10);
        }
    }

    #[test]
    fn range_inclusive_bounds() {
        let t = insert_range(1, 10);
        let results: Vec<_> = t.range(3u64..=7).collect();
        assert_eq!(results.len(), 5);
        assert_eq!(*results[0].0, 3);
        assert_eq!(*results[4].0, 7);
    }

    #[test]
    fn range_exclusive_end() {
        let t = insert_range(1, 10);
        let results: Vec<_> = t.range(3u64..7).collect();
        assert_eq!(results.len(), 4);
        assert_eq!(*results[0].0, 3);
        assert_eq!(*results[3].0, 6);
    }

    #[test]
    fn range_after_insert_and_remove() {
        let t = insert_range(1, 10);
        let t = t.remove(&5).unwrap();
        let results: Vec<u64> = t.range(..).map(|(k, _)| *k).collect();
        assert_eq!(results, vec![1, 2, 3, 4, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn range_across_split_boundary() {
        // Force multiple splits and verify range still works correctly.
        let t = insert_range(1, 20);
        let results: Vec<u64> = t.range(5u64..=15).map(|(k, _)| *k).collect();
        assert_eq!(results, (5u64..=15).collect::<Vec<_>>());
    }

    #[test]
    fn range_exclusive_start() {
        let t = insert_range(1, 10);
        let results: Vec<u64> = t
            .range((Bound::Excluded(5u64), Bound::Included(8u64)))
            .map(|(k, _)| *k)
            .collect();
        assert_eq!(results, vec![6, 7, 8]);
    }

    #[test]
    fn range_with_both_excluded() {
        let t = insert_range(1, 10);
        let results: Vec<u64> = t
            .range((Bound::Excluded(5u64), Bound::Excluded(8u64)))
            .map(|(k, _)| *k)
            .collect();
        assert_eq!(results, vec![6, 7]);
    }

    #[test]
    fn range_prefix_returns_exactly_the_matching_group() {
        let mut t: BTree<(u32, String), ()> = BTree::new();
        for (a, b) in [
            (1u32, "x"),
            (2, "a"),
            (2, "b"),
            (2, "c"),
            (3, "y"),
        ] {
            t = t.insert((a, b.to_string()), ());
        }

        let got: Vec<(u32, String)> = t.range_prefix(&2).map(|(k, _)| k.clone()).collect();
        assert_eq!(
            got,
            vec![
                (2, "a".to_string()),
                (2, "b".to_string()),
                (2, "c".to_string())
            ]
        );
    }

    #[test]
    fn range_prefix_handles_first_last_absent_and_empty() {
        let mut t: BTree<(u32, String), ()> = BTree::new();
        for (a, b) in [(1u32, "p"), (1, "q"), (5, "r")] {
            t = t.insert((a, b.to_string()), ());
        }

        // First group.
        assert_eq!(t.range_prefix(&1).count(), 2);
        // Last group.
        assert_eq!(t.range_prefix(&5).count(), 1);
        // Absent prefix between two present ones.
        assert_eq!(t.range_prefix(&3).count(), 0);
        // Absent prefix below and above everything.
        assert_eq!(t.range_prefix(&0).count(), 0);
        assert_eq!(t.range_prefix(&9).count(), 0);
        // Empty tree.
        let empty: BTree<(u32, String), ()> = BTree::new();
        assert_eq!(empty.range_prefix(&1).count(), 0);
    }

    /// The scan must not degrade to O(n): a prefix group of 3 in a tree of
    /// 10_000 must not visit the whole tree. Asserted via correctness at
    /// scale plus the group boundary, which a full scan would still pass —
    /// so this test guards correctness, and the O(log n + k) claim rests on
    /// the descent being bound-driven rather than filtered.
    #[test]
    fn range_prefix_is_correct_at_scale() {
        let mut t: BTree<(u32, u32), ()> = BTree::new();
        for i in 0..10_000u32 {
            t = t.insert((i % 1000, i), ());
        }
        let got: Vec<u32> = t.range_prefix(&500).map(|(k, _)| k.1).collect();
        assert_eq!(got, vec![500, 1500, 2500, 3500, 4500, 5500, 6500, 7500, 8500, 9500]);
    }

    /// `range()` must be unchanged by the locator refactor.
    #[test]
    #[allow(clippy::type_complexity)]
    fn range_still_honors_every_bound_combination() {
        use std::ops::Bound;
        let mut t: BTree<u32, ()> = BTree::new();
        for i in [10u32, 20, 30, 40] {
            t = t.insert(i, ());
        }
        let cases: Vec<((Bound<u32>, Bound<u32>), Vec<u32>)> = vec![
            ((Bound::Unbounded, Bound::Unbounded), vec![10, 20, 30, 40]),
            ((Bound::Included(20), Bound::Included(30)), vec![20, 30]),
            ((Bound::Excluded(20), Bound::Included(40)), vec![30, 40]),
            ((Bound::Included(20), Bound::Excluded(40)), vec![20, 30]),
            ((Bound::Excluded(10), Bound::Excluded(40)), vec![20, 30]),
            ((Bound::Included(25), Bound::Unbounded), vec![30, 40]),
            ((Bound::Unbounded, Bound::Excluded(10)), vec![]),
        ];
        for ((s, e), want) in cases {
            let got: Vec<u32> = t.range((s, e)).map(|(k, _)| *k).collect();
            assert_eq!(got, want, "bounds ({s:?}, {e:?})");
        }
    }

    #[test]
    fn max_key_empty_and_single() {
        let t: BTree<u64, u64> = BTree::new();
        assert_eq!(t.max_key(), None);
        let mut t = BTree::new();
        t.insert_mut(7u64, 70u64);
        assert_eq!(t.max_key(), Some(&7));
    }

    #[test]
    fn max_key_multi_level() {
        let t = insert_range(1, 100_000);
        assert_eq!(t.max_key(), Some(&100_000));
    }

    // -------------------------------------------------------------------
    // Deep-tree tests for internal-node deletion paths and rebalancing.
    // With T=32, a 3-level tree requires ~64*32 = 2048+ keys so that
    // the root has children that themselves have children.
    // -------------------------------------------------------------------

    #[test]
    fn deep_tree_get_arc_traverses_internal_nodes() {
        // get_arc recursion into children (line 264)
        let t = insert_range(1, 5000);
        // Keys in the middle are guaranteed to be in non-root nodes
        assert_eq!(t.get_arc(&2500).map(|v| *v), Some(25000));
        assert!(t.get_arc(&9999).is_none());
    }

    #[test]
    fn deep_tree_delete_internal_node_key() {
        // Forces the Ok(i) branch in delete_from_node for internal nodes
        // (lines 355-370) + remove_leftmost (lines 398-415).
        // Strategy: build a large tree, then find keys that are internal
        // separators by checking the tree structure indirectly — deleting
        // keys near the middle of the range exercises internal-node hits.
        let mut t = insert_range(1, 5000);
        let before_len = t.len();

        // Delete keys spread across the range to hit internal separators.
        // With T=32, root separators are roughly evenly spaced.
        let keys_to_delete: Vec<u64> = (1..=5000).step_by(64).collect();
        for &k in &keys_to_delete {
            t = t.remove(&k).unwrap();
        }
        assert_eq!(t.len(), before_len - keys_to_delete.len());

        // Verify remaining keys are intact
        for i in 1..=5000 {
            if keys_to_delete.contains(&i) {
                assert!(t.get(&i).is_none());
            } else {
                assert_eq!(t.get(&i), Some(&(i * 10)));
            }
        }
    }

    #[test]
    fn deep_tree_heavy_deletion_triggers_all_rebalance_paths() {
        // Insert enough to build 3+ levels, then delete in patterns that
        // trigger rotate_right, rotate_left, merge_with_left, merge_with_right.
        let mut t = insert_range(1, 5000);

        // Delete from the left side heavily to force right-to-left rebalancing
        for i in 1..=2000 {
            t = t.remove(&i).unwrap();
        }
        assert_eq!(t.len(), 3000);

        // Verify range still works (exercises descend_leftmost for internal nodes)
        let all: Vec<u64> = t.range(..).map(|(k, _)| *k).collect();
        assert_eq!(all.len(), 3000);
        assert_eq!(*all.first().unwrap(), 2001);
        assert_eq!(*all.last().unwrap(), 5000);

        // Now delete from the right side
        for i in (4001..=5000).rev() {
            t = t.remove(&i).unwrap();
        }
        assert_eq!(t.len(), 2000);

        // Delete alternating keys from what remains to trigger merges
        let remaining: Vec<u64> = (2001..=4000).collect();
        for &k in remaining.iter().step_by(2) {
            t = t.remove(&k).unwrap();
        }
        assert_eq!(t.len(), 1000);

        // Verify tree integrity
        let final_keys: Vec<u64> = t.range(..).map(|(k, _)| *k).collect();
        assert_eq!(final_keys.len(), 1000);
        for k in &final_keys {
            assert_eq!(t.get(k), Some(&(k * 10)));
        }
    }

    #[test]
    fn deep_tree_delete_all_exercises_merge_paths() {
        // Delete all 5000 keys in forward order — this heavily exercises
        // the left-side merge/rotate paths as the leftmost children
        // repeatedly become underfull.
        let mut t = insert_range(1, 5000);
        for i in 1..=5000 {
            t = t.remove(&i).unwrap();
        }
        assert!(t.is_empty());
    }

    #[test]
    fn deep_tree_delete_all_reverse_exercises_right_merge_paths() {
        // Delete all keys in reverse order — exercises right-side
        // merge/rotate paths as rightmost children become underfull.
        let mut t = insert_range(1, 5000);
        for i in (1..=5000).rev() {
            t = t.remove(&i).unwrap();
        }
        assert!(t.is_empty());
    }

    #[test]
    fn deep_tree_range_unbounded_start() {
        // Exercises descend_leftmost (line 176-182) via range(..)
        // on a multi-level tree — the Unbounded start case in
        // descend_left_from also works, but descend_leftmost is only
        // called during iteration when advancing to the next subtree.
        let t = insert_range(1, 5000);
        let all: Vec<u64> = t.range(..).map(|(k, _)| *k).collect();
        assert_eq!(all.len(), 5000);
        assert_eq!(all[0], 1);
        assert_eq!(all[4999], 5000);
    }

    #[test]
    fn default_creates_empty_tree() {
        let t: BTree<u64, u64> = BTree::default();
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn string_keys_work() {
        let mut t: BTree<String, u64> = BTree::new();
        t = t.insert("banana".to_string(), 1);
        t = t.insert("apple".to_string(), 2);
        t = t.insert("cherry".to_string(), 3);
        assert_eq!(t.get(&"apple".to_string()), Some(&2));
        assert_eq!(t.get(&"banana".to_string()), Some(&1));
        assert_eq!(t.get(&"cherry".to_string()), Some(&3));
        // Range should yield alphabetical order
        let keys: Vec<&String> = t.range(..).map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["apple", "banana", "cherry"]);
    }

    #[test]
    fn tuple_keys_work() {
        let mut t: BTree<(String, u64), ()> = BTree::new();
        t = t.insert(("alice".to_string(), 1), ());
        t = t.insert(("alice".to_string(), 2), ());
        t = t.insert(("bob".to_string(), 1), ());
        assert_eq!(t.len(), 3);
        // Range scan for all "alice" entries
        let results: Vec<_> = t
            .range(("alice".to_string(), 0u64)..=("alice".to_string(), u64::MAX))
            .collect();
        assert_eq!(results.len(), 2);
    }

    // -------------------------------------------------------------------
    // DoubleEndedIterator / reverse iteration tests
    // -------------------------------------------------------------------

    #[test]
    fn range_full_reverse_yields_all_keys_descending() {
        let t = insert_range(1, 10);
        let results: Vec<u64> = t.range(..).rev().map(|(k, _)| *k).collect();
        assert_eq!(results, vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
    }

    #[test]
    fn range_bounded_reverse() {
        let t = insert_range(1, 10);
        let results: Vec<u64> = t.range(3u64..=7).rev().map(|(k, _)| *k).collect();
        assert_eq!(results, vec![7, 6, 5, 4, 3]);
    }

    #[test]
    fn range_reverse_empty_tree() {
        let t: BTree<u64, u64> = BTree::new();
        let results: Vec<u64> = t.range(..).rev().map(|(k, _)| *k).collect();
        assert!(results.is_empty());
    }

    #[test]
    fn range_reverse_single_element() {
        let t = BTree::new().insert(5u64, 50u64);
        let results: Vec<u64> = t.range(..).rev().map(|(k, _)| *k).collect();
        assert_eq!(results, vec![5]);
    }

    #[test]
    fn range_reverse_across_split_boundary() {
        let t = insert_range(1, 200);
        let results: Vec<u64> = t.range(50u64..=150).rev().map(|(k, _)| *k).collect();
        let expected: Vec<u64> = (50..=150).rev().collect();
        assert_eq!(results, expected);
    }

    #[test]
    fn range_mixed_forward_and_reverse() {
        let t = insert_range(1, 10);
        let mut iter = t.range(3u64..=8);
        assert_eq!(iter.next().map(|(k, _)| *k), Some(3));
        assert_eq!(iter.next_back().map(|(k, _)| *k), Some(8));
        assert_eq!(iter.next().map(|(k, _)| *k), Some(4));
        assert_eq!(iter.next_back().map(|(k, _)| *k), Some(7));
        assert_eq!(iter.next().map(|(k, _)| *k), Some(5));
        assert_eq!(iter.next_back().map(|(k, _)| *k), Some(6));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
    }

    #[test]
    fn range_reverse_large_tree() {
        let t = insert_range(1, 5000);
        let results: Vec<u64> = t.range(..).rev().map(|(k, _)| *k).collect();
        let expected: Vec<u64> = (1..=5000).rev().collect();
        assert_eq!(results, expected);
    }

    // -------------------------------------------------------------------
    // Bulk-load constructor (`BTree::from_sorted`)
    // -------------------------------------------------------------------

    #[test]
    fn from_sorted_empty() {
        let t: BTree<u64, String> = BTree::from_sorted(std::iter::empty());
        assert_eq!(t.len(), 0);
        assert!(t.is_empty());
        assert_eq!(t.range(..).count(), 0);
    }

    #[test]
    fn from_sorted_single_entry() {
        let t = BTree::<u64, &str>::from_sorted(std::iter::once((1u64, Arc::new("a"))));
        assert_eq!(t.len(), 1);
        assert_eq!(t.get(&1), Some(&"a"));
    }

    #[test]
    fn from_sorted_exact_max_keys() {
        let entries: Vec<_> = (0..MAX_KEYS as u64).map(|i| (i, Arc::new(i))).collect();
        let t = BTree::<u64, u64>::from_sorted(entries);
        assert_eq!(t.len(), MAX_KEYS);
        for i in 0..MAX_KEYS as u64 {
            assert_eq!(t.get(&i).copied(), Some(i));
        }
    }

    #[test]
    fn from_sorted_max_keys_plus_one() {
        let n = MAX_KEYS as u64 + 1;
        let entries: Vec<_> = (0..n).map(|i| (i, Arc::new(i))).collect();
        let t = BTree::<u64, u64>::from_sorted(entries);
        assert_eq!(t.len(), n as usize);
        for i in 0..n {
            assert_eq!(t.get(&i).copied(), Some(i));
        }
    }

    #[test]
    fn from_sorted_1k_matches_insert() {
        let n = 1_000u64;
        let entries: Vec<_> = (0..n).map(|i| (i, Arc::new(i * 10))).collect();
        let by_bulk = BTree::<u64, u64>::from_sorted(entries.clone());
        let mut by_insert = BTree::<u64, u64>::new();
        for (k, v) in entries {
            by_insert = by_insert.insert(k, *v);
        }
        let walked_bulk: Vec<(u64, u64)> = by_bulk.range(..).map(|(k, v)| (*k, *v)).collect();
        let walked_insert: Vec<(u64, u64)> = by_insert.range(..).map(|(k, v)| (*k, *v)).collect();
        assert_eq!(walked_bulk, walked_insert);
        assert_eq!(by_bulk.len(), by_insert.len());
    }

    #[test]
    fn from_sorted_100k() {
        let n = 100_000u64;
        let entries: Vec<_> = (0..n).map(|i| (i, Arc::new(i))).collect();
        let t = BTree::<u64, u64>::from_sorted(entries);
        assert_eq!(t.len(), n as usize);
        assert_eq!(t.get(&0).copied(), Some(0));
        assert_eq!(t.get(&(n / 2)).copied(), Some(n / 2));
        assert_eq!(t.get(&(n - 1)).copied(), Some(n - 1));
        let walked: usize = t.range(..).count();
        assert_eq!(walked, n as usize);
    }

    #[test]
    fn from_sorted_tail_underfull() {
        // Exactly MAX_KEYS + 1 entries: the leaf overflow drains the leaf
        // builder, leaving a partial level above with one child but expecting
        // two. Tail redistribution must split the merged sequence so both
        // resulting leaves have >= MIN_KEYS entries.
        let n = MAX_KEYS as u64 + 1;
        let entries: Vec<_> = (0..n).map(|i| (i, Arc::new(i))).collect();
        let t = BTree::<u64, u64>::from_sorted(entries);
        assert_eq!(t.len(), n as usize);
        let walked: Vec<u64> = t.range(..).map(|(k, _)| *k).collect();
        let expected: Vec<u64> = (0..n).collect();
        assert_eq!(walked, expected);

        // Insert/remove around the boundary — the existing CoW algorithms
        // would panic on a malformed tree.
        let t2 = t.insert(n, n);
        assert_eq!(t2.get(&n).copied(), Some(n));
        let t3 = t2.remove(&0).unwrap();
        assert_eq!(t3.get(&0), None);
        assert_eq!(t3.len(), n as usize);
    }

    #[test]
    fn from_sorted_level1_drain() {
        // N = MAX_KEYS * (MAX_KEYS + 1) + 1 — the smallest input that drains
        // the level-1 internal node during finish, forcing cascading
        // redistribute_tail from level 1 borrowing from level 2.
        let n = (MAX_KEYS as u64) * (MAX_KEYS as u64 + 1) + 1;
        let entries: Vec<_> = (0..n).map(|i| (i, std::sync::Arc::new(i))).collect();
        let t = BTree::<u64, u64>::from_sorted(entries);
        assert_eq!(t.len(), n as usize);
        // Round-trip via insert + remove proves the tree is well-formed —
        // the existing CoW algorithms would panic on a malformed tree.
        let t2 = t.insert(n, n);
        assert_eq!(t2.get(&n).copied(), Some(n));
        let t3 = t2.remove(&0).unwrap();
        assert_eq!(t3.get(&0), None);
        assert_eq!(t3.len(), n as usize);
    }

    // -----------------------------------------------------------------------
    // In-place insert (`insert_mut`) — prototype for task: btree-insert-mut
    // -----------------------------------------------------------------------

    /// Collect the full tree as a Vec for structural comparison.
    fn dump(t: &BTree<u64, u64>) -> Vec<(u64, u64)> {
        t.range(..).map(|(&k, &v)| (k, v)).collect()
    }

    /// `insert_mut` must yield the exact same logical tree as `insert` across a
    /// large key count that forces many splits (multiple levels).
    #[test]
    fn insert_mut_matches_insert_ascending() {
        let n = 10_000u64;
        let immutable = insert_range(0, n - 1);

        let mut in_place = BTree::new();
        for i in 0..n {
            in_place.insert_mut(i, i * 10);
        }

        assert_eq!(in_place.len(), immutable.len());
        assert_eq!(dump(&in_place), dump(&immutable));
    }

    /// Same equivalence under a non-sequential insertion order and with
    /// replacements of existing keys (the `Ok(pos)` path).
    #[test]
    fn insert_mut_matches_insert_scrambled_with_replaces() {
        // A cheap deterministic scramble (LCG) — no rng dependency, no Date/rand
        // (which are unavailable here anyway).
        let n = 5000u64;
        let mut lcg = 12345u64;
        let mut next = || {
            lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            lcg % n
        };

        let mut immutable = BTree::new();
        let mut in_place = BTree::new();
        for _ in 0..(n * 3) {
            let k = next();
            let v = k.wrapping_mul(7).wrapping_add(1);
            immutable = immutable.insert(k, v);
            in_place.insert_mut(k, v);
        }

        assert_eq!(in_place.len(), immutable.len());
        assert_eq!(dump(&in_place), dump(&immutable));
    }

    /// The load-bearing correctness property: `insert_mut` must preserve
    /// copy-on-write. A previously-cloned handle (a "snapshot") must NOT observe
    /// mutations applied to the tree afterwards — `Arc::make_mut` has to clone the
    /// shared nodes rather than mutate them in place.
    #[test]
    fn insert_mut_preserves_snapshot_isolation() {
        let mut t = BTree::new();
        for i in 0..2000u64 {
            t.insert_mut(i, i);
        }

        // Take a snapshot (O(1) Arc bump). Its root is now shared.
        let snapshot = t.clone();
        let snapshot_dump = dump(&snapshot);
        assert_eq!(snapshot.len(), 2000);

        // Mutate the live tree in place: overwrite existing keys, add new ones,
        // all of which walk paths shared with `snapshot`.
        for i in 0..2000u64 {
            t.insert_mut(i, i + 1_000_000); // overwrite
        }
        for i in 2000..4000u64 {
            t.insert_mut(i, i); // grow
        }

        // Snapshot is completely unaffected.
        assert_eq!(dump(&snapshot), snapshot_dump);
        assert_eq!(snapshot.len(), 2000);
        assert_eq!(snapshot.get(&0).copied(), Some(0));
        assert_eq!(snapshot.get(&1999).copied(), Some(1999));
        assert_eq!(snapshot.get(&3000), None);

        // Live tree reflects every mutation.
        assert_eq!(t.len(), 4000);
        assert_eq!(t.get(&0).copied(), Some(1_000_000));
        assert_eq!(t.get(&1999).copied(), Some(1_001_999));
        assert_eq!(t.get(&3000).copied(), Some(3000));
    }

    /// Repeated snapshot-then-mutate cycles: chained snapshots must each retain
    /// the value they saw at capture time (structural sharing across versions).
    #[test]
    fn insert_mut_chained_snapshots_independent() {
        let mut t = BTree::new();
        let mut snaps = Vec::new();
        for round in 0..50u64 {
            for i in 0..200u64 {
                t.insert_mut(i, round * 1000 + i);
            }
            snaps.push((round, t.clone()));
        }
        for (round, snap) in &snaps {
            assert_eq!(snap.get(&0).copied(), Some(round * 1000));
            assert_eq!(snap.get(&199).copied(), Some(round * 1000 + 199));
            assert_eq!(snap.len(), 200);
        }
    }

    // -----------------------------------------------------------------------
    // In-place delete (`remove_mut`) — prototype for task: btree-remove-mut
    // -----------------------------------------------------------------------

    /// `remove_mut` must yield the exact same logical tree as the immutable
    /// `remove`, across interleaved insert/remove churn that forces rotations,
    /// merges, and root collapse — and must track `std::BTreeMap`.
    #[test]
    fn remove_mut_matches_remove_scrambled() {
        use std::collections::BTreeMap;
        let n = 800u64;
        let mut lcg = 0x9E3779B97F4A7C15u64;
        // Returns `(k, lcg)` — the branch decision below needs the raw LCG
        // state's parity, but `next` already holds `lcg` by unique borrow for
        // its own lifetime, so the state must flow out through the return
        // value rather than being re-read from the outer binding.
        let mut next = || {
            lcg = lcg
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (lcg % n, lcg)
        };

        let mut immutable = BTree::new();
        let mut in_place = BTree::new();
        let mut model = BTreeMap::new();

        // Seed both trees identically.
        for _ in 0..(n * 4) {
            let (k, _) = next();
            immutable = immutable.insert(k, k);
            in_place.insert_mut(k, k);
            model.insert(k, k);
        }
        assert_eq!(dump(&in_place), dump(&immutable));

        // Interleave inserts and removes (present + absent keys).
        for _ in 0..(n * 8) {
            let (k, state) = next();
            if state & 1 == 0 {
                immutable = immutable.insert(k, k);
                in_place.insert_mut(k, k);
                model.insert(k, k);
            } else {
                let imm_had = immutable.remove(&k);
                let inp_had = in_place.remove_mut(&k);
                let model_had = model.remove(&k).is_some();
                assert_eq!(inp_had, model_had, "remove_mut presence disagrees with std for {k}");
                if let Ok(t) = imm_had {
                    immutable = t;
                }
                assert_eq!(dump(&in_place), dump(&immutable), "trees diverged at key {k}");
            }
        }
        assert_eq!(in_place.len(), immutable.len());
        assert_eq!(dump(&in_place), dump(&immutable));
    }

    /// A previously-cloned snapshot must NOT observe deletions applied afterwards.
    #[test]
    fn remove_mut_preserves_snapshot_isolation() {
        let mut t = BTree::new();
        for i in 0..4000u64 {
            t.insert_mut(i, i);
        }
        let snapshot = t.clone();
        let snapshot_dump = dump(&snapshot);

        // Delete half the keys from the live tree in place.
        for i in 0..2000u64 {
            assert!(t.remove_mut(&i));
        }

        // Snapshot is completely unaffected.
        assert_eq!(dump(&snapshot), snapshot_dump);
        assert_eq!(snapshot.len(), 4000);
        assert_eq!(snapshot.get(&0).copied(), Some(0));
        assert_eq!(snapshot.get(&1999).copied(), Some(1999));

        // Live tree reflects every deletion.
        assert_eq!(t.len(), 2000);
        assert_eq!(t.get(&0), None);
        assert_eq!(t.get(&1999), None);
        assert_eq!(t.get(&2000).copied(), Some(2000));
    }

    /// Chained snapshot-then-delete cycles: each snapshot retains what it saw.
    #[test]
    fn remove_mut_chained_snapshots_independent() {
        let mut t = BTree::new();
        for i in 0..4000u64 {
            t.insert_mut(i, i);
        }
        let mut snaps = Vec::new();
        for round in 0..20u64 {
            // Delete a distinct 100-key window each round.
            for i in 0..100u64 {
                t.remove_mut(&(round * 100 + i));
            }
            snaps.push((round, t.clone(), t.len()));
        }
        for (round, snap, len) in &snaps {
            assert_eq!(snap.len(), *len);
            // The window deleted in this round is absent in this snapshot.
            assert_eq!(snap.get(&(round * 100)), None);
            // A key past all deletions is still present.
            assert_eq!(snap.get(&3999).copied(), Some(3999));
        }
    }

    /// Deleting an absent key returns false and leaves the tree unchanged.
    #[test]
    fn remove_mut_absent_key_is_noop() {
        let mut t = BTree::new();
        for i in 0..500u64 {
            t.insert_mut(i, i);
        }
        let before = dump(&t);
        assert!(!t.remove_mut(&1000));
        assert_eq!(t.len(), 500);
        assert_eq!(dump(&t), before);
    }

    /// In-place rebalancing (rotate/merge) opens sibling nodes via
    /// `Child::make_mut` / `absorb`'s `Arc::try_unwrap`. When a sibling is
    /// still shared with an older snapshot it MUST be cloned, never
    /// mutated/moved in place — otherwise a merge would corrupt the
    /// snapshot. Deleting a long contiguous run from the low end forces
    /// repeated merges and rotations at every level while a snapshot holds
    /// those very siblings.
    #[test]
    fn remove_mut_merge_under_snapshot_preserves_isolation() {
        use std::collections::BTreeMap;
        let mut t = BTree::new();
        for i in 0..4000u64 {
            t.insert_mut(i, i);
        }
        let snapshot = t.clone();
        let snapshot_dump = dump(&snapshot);

        // Delete a large contiguous prefix — guarantees underflow-driven merges
        // and rotations touching siblings shared with `snapshot`.
        let mut model: BTreeMap<u64, u64> = (0..4000u64).map(|i| (i, i)).collect();
        for i in 0..3000u64 {
            assert!(t.remove_mut(&i));
            model.remove(&i);
        }

        // Snapshot is byte-for-byte what it was at capture time.
        assert_eq!(dump(&snapshot), snapshot_dump);
        assert_eq!(snapshot.len(), 4000);
        assert_eq!(snapshot.get(&0).copied(), Some(0));
        assert_eq!(snapshot.get(&2999).copied(), Some(2999));

        // Live tree exactly tracks the model after all the merges.
        assert_eq!(t.len(), model.len());
        assert_eq!(dump(&t), model.into_iter().collect::<Vec<_>>());
    }

    /// A key type that counts every `Clone::clone` call, so a test can prove
    /// a code path took the zero-clone move branch rather than the
    /// deep-clone fallback branch, without any access to `absorb`'s private
    /// locals.
    struct CountedKey {
        v: u64,
        clones: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Clone for CountedKey {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            CountedKey {
                v: self.v,
                clones: std::sync::Arc::clone(&self.clones),
            }
        }
    }
    impl PartialEq for CountedKey {
        fn eq(&self, other: &Self) -> bool {
            self.v == other.v
        }
    }
    impl Eq for CountedKey {}
    impl PartialOrd for CountedKey {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for CountedKey {
        fn cmp(&self, other: &Self) -> Ordering {
            self.v.cmp(&other.v)
        }
    }

    /// Regression test for the `absorb` fix: `right.load_arc(src)` bumps the
    /// node's strong count to hand back an owned `Arc`, and the `Child`
    /// slot itself still owns a count on top of that — `Arc::try_unwrap`
    /// only succeeds once the slot's own count is released first
    /// (`drop(right)`). Without that `drop`, `try_unwrap` always observes
    /// `strong_count >= 2` and *always* takes the clone fallback, even for a
    /// sibling nothing else references.
    ///
    /// Proof, not inference: build a tree that is never cloned and never
    /// shares a snapshot (so every `Child::make_mut` on its way is an
    /// in-place edit, and the *only* place a key could possibly get cloned
    /// during the whole build+delete run is `absorb`'s fallback branch), use
    /// a key type that counts its own `Clone::clone` calls, force the same
    /// merge-heavy contiguous-prefix delete used by
    /// `remove_mut_merge_under_snapshot_preserves_isolation` (minus the
    /// snapshot), and assert the clone count is still zero afterward. If
    /// `drop(right)` is removed from `absorb`, every merge clones every
    /// entry of the absorbed sibling and this count goes strictly positive.
    #[test]
    fn merge_moves_unshared_sibling_instead_of_cloning() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering as AtoOrd};

        let clones = Arc::new(AtomicUsize::new(0));
        let key = |v: u64| CountedKey {
            v,
            clones: Arc::clone(&clones),
        };

        let mut t: BTree<CountedKey, u64> = BTree::new();
        for i in 0..4000u64 {
            t.insert_mut(key(i), i);
        }
        // `insert_mut` on a never-cloned tree should also be clone-free (every
        // node is uniquely owned, so `Child::make_mut` never CoW-clones), but
        // reset here anyway so the assertion below measures only the deletes.
        clones.store(0, AtoOrd::Relaxed);

        // Same shape as remove_mut_merge_under_snapshot_preserves_isolation's
        // delete, but with no snapshot ever taken — nothing shares any node.
        for i in 0..3000u64 {
            assert!(t.remove_mut(&key(i)));
        }

        assert_eq!(
            clones.load(AtoOrd::Relaxed),
            0,
            "absorb cloned an unshared sibling instead of moving it -- \
             the Child slot's own strong count must be dropped before \
             Arc::try_unwrap, or try_unwrap can never see strong_count == 1"
        );
    }

    /// Structural invariant check: arity bounds (root exempt from MIN_KEYS),
    /// uniform leaf depth, internal children == entries + 1, per-node key
    /// order, global in-order key order, and len consistency.
    fn check_invariants<K: Ord + Clone + std::fmt::Debug, V>(t: &BTree<K, V>) {
        fn walk<K: Ord + std::fmt::Debug, V>(
            node: &BTreeNode<K, V>,
            is_root: bool,
            depth: usize,
            leaf_depth: &mut Option<usize>,
            count: &mut usize,
        ) {
            if node.children.is_empty() {
                match *leaf_depth {
                    Some(d) => assert_eq!(d, depth, "non-uniform leaf depth"),
                    None => *leaf_depth = Some(depth),
                }
            } else {
                assert_eq!(
                    node.children.len(),
                    node.entries.len() + 1,
                    "internal arity: {} children for {} entries",
                    node.children.len(),
                    node.entries.len()
                );
                if is_root {
                    assert!(!node.entries.is_empty(), "internal root with no entries");
                }
                for c in node.children.iter() {
                    walk(c.load(None), false, depth + 1, leaf_depth, count);
                }
            }
            if !is_root {
                assert!(
                    node.entries.len() >= MIN_KEYS,
                    "underfull non-root: {} < MIN_KEYS",
                    node.entries.len()
                );
            }
            assert!(node.entries.len() <= MAX_KEYS, "overfull node");
            for i in 1..node.entries.len() {
                assert!(node.entries[i - 1].0 < node.entries[i].0, "unsorted entries within node");
            }
            *count += node.entries.len();
        }
        let mut leaf_depth = None;
        let mut count = 0;
        walk(t.root.load(None), true, 0, &mut leaf_depth, &mut count);
        assert_eq!(count, t.len(), "len does not match entry count");
        let keys: Vec<&K> = t.range(..).map(|(k, _)| k).collect();
        assert!(
            keys.windows(2).all(|w| w[0] < w[1]),
            "in-order walk not strictly ascending"
        );
    }

    #[test]
    fn check_invariants_accepts_existing_constructors() {
        check_invariants(&BTree::<u64, u64>::new());
        check_invariants(&insert_range(1, 10_000));
        let entries: Vec<_> = (0..10_000u64).map(|i| (i, Arc::new(i))).collect();
        check_invariants(&BTree::from_sorted(entries));
    }

    #[test]
    #[should_panic(expected = "len does not match")]
    fn check_invariants_catches_bad_len() {
        let mut t = insert_range(1, 100);
        t.len = 99; // deliberately corrupt (private field, same module)
        check_invariants(&t);
    }

    // -------------------------------------------------------------------
    // Bulk append (`BTree::extend_from_sorted`, task51)
    // -------------------------------------------------------------------

    /// extend_from_sorted must be observationally identical to per-key
    /// insert_mut of the same entries (mapping + len; node packing differs).
    fn assert_extend_matches_insert_mut(base: &BTree<u64, u64>, batch: &[(u64, u64)]) {
        let mut by_ext = base.clone();
        by_ext.extend_from_sorted(batch.iter().map(|&(k, v)| (k, Arc::new(v))));
        let mut by_mut = base.clone();
        for &(k, v) in batch {
            by_mut.insert_mut(k, v);
        }
        let walked_ext: Vec<(u64, u64)> = by_ext.range(..).map(|(k, v)| (*k, *v)).collect();
        let walked_mut: Vec<(u64, u64)> = by_mut.range(..).map(|(k, v)| (*k, *v)).collect();
        assert_eq!(walked_ext, walked_mut);
        assert_eq!(by_ext.len(), by_mut.len());
        check_invariants(&by_ext);
    }

    #[test]
    fn extend_from_sorted_empty_tree_and_empty_batch() {
        let empty: BTree<u64, u64> = BTree::new();
        let batch: Vec<(u64, u64)> = (0..1000).map(|i| (i, i * 10)).collect();
        assert_extend_matches_insert_mut(&empty, &batch); // == from_sorted case
        let base = insert_range(1, 1000);
        assert_extend_matches_insert_mut(&base, &[]); // no-op append
    }

    #[test]
    fn extend_from_sorted_tail_shapes() {
        // Underfull tail leaf, exactly-full tail leaf, and one-past-full:
        // sizes chosen relative to the packing consts, not hardcoded.
        for base_n in [
            MIN_KEYS as u64,              // root-leaf, underfull by non-root standards
            MAX_KEYS as u64,               // root-leaf exactly full
            MAX_KEYS as u64 + 1,           // first split just happened
            (MAX_KEYS * MAX_KEYS) as u64,  // multi-level
        ] {
            let base = insert_range(1, base_n);
            for batch_n in [1u64, 2, MIN_KEYS as u64, MAX_KEYS as u64 + 5, 5000] {
                let batch: Vec<(u64, u64)> =
                    (base_n + 1..=base_n + batch_n).map(|k| (k, k)).collect();
                assert_extend_matches_insert_mut(&base, &batch);
            }
        }
    }

    /// Regression test for a `from_sorted`/`finish()` bug found while
    /// developing `extend_from_sorted` (task51): when the input size lands
    /// exactly on a cascading freeze boundary (every level from the leaf up
    /// simultaneously hits `MAX_KEYS` on the very last push), the topmost
    /// level used to end up either failing an internal debug assertion, or
    /// (before that) attempting an invalid `redistribute_tail` on a level
    /// with no child of its own. Covers the single- and double-level
    /// cascade boundaries (`(MAX_KEYS + 1)^2` and `(MAX_KEYS + 1)^2 * 2`);
    /// see `pending_reinsert`/the root-collapse branch in `finish()`.
    #[test]
    fn from_sorted_exact_cascade_boundary() {
        let boundary = (MAX_KEYS as u64 + 1) * (MAX_KEYS as u64 + 1);
        for n in [
            boundary - 1,
            boundary,
            boundary + 1,
            boundary + MIN_KEYS as u64,
            2 * boundary,
            2 * boundary + 1,
        ] {
            let entries: Vec<_> = (0..n).map(|i| (i, Arc::new(i))).collect();
            let t = BTree::from_sorted(entries);
            check_invariants(&t);
            assert_eq!(t.len(), n as usize);
        }
    }

    /// Regression test for the narrow, verified-benign packing deviation
    /// documented at the collapse site in `finish()`: for `n = (MAX_KEYS +
    /// 1)^3 + d` with `1 <= d < MIN_KEYS`, an *intermediate* level can end up
    /// with zero entries and one child — only the topmost level is collapsed
    /// on a dangling reinsert, so an intermediate one that hits this is left
    /// underfull (one tail leaf below `MIN_KEYS`) rather than collapsed.
    ///
    /// This pins the SAFETY claims at that scale — data correctness,
    /// ordering, len, and structural well-formedness enough to support point
    /// lookups and a full in-order walk — and NOT the strict `MIN_KEYS`
    /// packing floor: `check_invariants` is deliberately *not* called on the
    /// freshly-built tree, because it would fail on the underfull tail leaf.
    /// It self-heals on delete: removing the top `2 * MAX_KEYS` keys touches
    /// that leaf (and its ancestors), and the tree then passes the strict
    /// invariant check.
    #[test]
    fn from_sorted_nested_cascade_two_million_benign() {
        let base = MAX_KEYS as u64 + 1;
        let cube = base * base * base;
        let n = cube + 30; // (MAX_KEYS + 1)^3 + 30, i.e. 1 <= 30 < MIN_KEYS

        let mut t = BTree::<u64, u64>::from_sorted((0..n).map(|i| (i, Arc::new(i))));
        assert_eq!(t.len(), n as usize);

        // Point lookups across the range, including several past the
        // (MAX_KEYS + 1)^3 cascade boundary where the underfull tail leaf
        // lives.
        for k in [0, 1, n / 2, cube - 1, cube, cube + 1, cube + 15, cube + 29, n - 1] {
            assert_eq!(t.get(&k).copied(), Some(k), "lookup mismatch at key {k}");
        }
        assert_eq!(t.get(&n), None);

        // Full range(..) walk: exactly n entries, strictly ascending, 0..n.
        // Track count + a running previous key rather than materializing a
        // ~2M-entry Vec of tuples.
        let mut count = 0usize;
        let mut prev: Option<u64> = None;
        for (k, v) in t.range(..) {
            if let Some(p) = prev {
                assert!(*k > p, "range walk not strictly ascending at key {k}");
            }
            assert_eq!(*v, *k);
            prev = Some(*k);
            count += 1;
        }
        assert_eq!(count, n as usize);
        assert_eq!(prev, Some(n - 1));

        // Self-heal: removing the top 2 * MAX_KEYS keys touches the
        // underfull tail leaf (and its ancestors); the tree must then
        // satisfy the strict MIN_KEYS packing floor.
        for k in (n - 2 * MAX_KEYS as u64)..n {
            assert!(t.remove_mut(&k), "expected key {k} to be present before removal");
        }
        assert_eq!(t.len(), (n - 2 * MAX_KEYS as u64) as usize);
        check_invariants(&t);
    }

    #[test]
    fn extend_from_sorted_fuzz_alternating_batches_and_removes() {
        // Grow a tree with random-size appends; between rounds remove random
        // keys (varying spine occupancy). extend path and per-key path must
        // agree after every round, and every intermediate tree must satisfy
        // the invariants. Deterministic LCG — no external RNG.
        let mut x: u64 = 0x5EED_5EED_5EED_5EED;
        let mut lcg = move || {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            x
        };
        let mut by_ext: BTree<u64, u64> = BTree::new();
        let mut by_mut: BTree<u64, u64> = BTree::new();
        let mut next_key = 0u64;
        for _round in 0..60 {
            let batch_n = lcg() % 800 + 1;
            let batch: Vec<(u64, u64)> =
                (next_key..next_key + batch_n).map(|k| (k, k ^ 0xABCD)).collect();
            next_key += batch_n;
            by_ext.extend_from_sorted(batch.iter().map(|&(k, v)| (k, Arc::new(v))));
            for &(k, v) in &batch {
                by_mut.insert_mut(k, v);
            }
            for _ in 0..(lcg() % 200) {
                let k = lcg() % next_key;
                by_ext.remove_mut(&k);
                by_mut.remove_mut(&k);
            }
            check_invariants(&by_ext);
            assert_eq!(by_ext.len(), by_mut.len(), "len diverged");
        }
        let walked_ext: Vec<(u64, u64)> = by_ext.range(..).map(|(k, v)| (*k, *v)).collect();
        let walked_mut: Vec<(u64, u64)> = by_mut.range(..).map(|(k, v)| (*k, *v)).collect();
        assert_eq!(walked_ext, walked_mut);
    }

    #[test]
    fn extend_from_sorted_preserves_snapshots() {
        let mut t: BTree<u64, u64> = BTree::new();
        for i in 0..(MAX_KEYS as u64 * MAX_KEYS as u64) {
            t.insert_mut(i, i);
        }
        let snap = t.clone(); // simulates an older MVCC snapshot
        let before: Vec<(u64, u64)> = snap.range(..).map(|(k, v)| (*k, *v)).collect();
        let start = MAX_KEYS as u64 * MAX_KEYS as u64;
        t.extend_from_sorted((start..start + 10_000).map(|i| (i, Arc::new(i))));
        let after: Vec<(u64, u64)> = snap.range(..).map(|(k, v)| (*k, *v)).collect();
        assert_eq!(before, after, "older snapshot observed the append");
        assert_eq!(snap.len() as u64, start);
        check_invariants(&snap);
        check_invariants(&t);
    }

    #[test]
    fn extend_from_sorted_fuzz_snapshot_per_round() {
        // Stronger than a single constructed case: snapshot before EVERY
        // append (many spine shapes, incl. tail-redistribute rounds) and
        // verify each snapshot afterwards. Catches any mutation of shared
        // nodes anywhere in seed/push/finish/redistribute.
        let mut x: u64 = 0xB16B_00B5_CAFE_F00D;
        let mut lcg = move || {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            x
        };
        let mut t: BTree<u64, u64> = BTree::new();
        let mut next_key = 0u64;
        for _round in 0..40 {
            let snap = t.clone();
            let before: Vec<(u64, u64)> = snap.range(..).map(|(k, v)| (*k, *v)).collect();
            let batch_n = lcg() % 300 + 1; // small batches maximize tail-redistribute hits
            t.extend_from_sorted(
                (next_key..next_key + batch_n).map(|k| (k, Arc::new(k))),
            );
            next_key += batch_n;
            let after: Vec<(u64, u64)> = snap.range(..).map(|(k, v)| (*k, *v)).collect();
            assert_eq!(before, after, "snapshot mutated by append");
            check_invariants(&t);
        }
    }

    /// Regression test for a second `from_sorted`/`finish()` debug-assert
    /// family found in final review of task51 (distinct from
    /// `from_sorted_exact_cascade_boundary`'s single-/double-level cascade
    /// and from the benign underfull-tail-leaf deviation in
    /// `from_sorted_nested_cascade_two_million_benign`): for
    /// `n = m * (MAX_KEYS + 1)^3 + j * (MAX_KEYS + 1)^2` with `m >= 1` and
    /// `1 <= j < MIN_KEYS`, an internal level reaches `finish()` "unclosed"
    /// (`children.len() == entries.len()`, a dangling separator with no
    /// right child — see `attach_child`'s doc comment) *and* underfull with
    /// a parent sibling present, so `redistribute_tail` used to run its
    /// arity-assuming split math on a level one child short and trip its own
    /// `debug_assert_eq!` (src/btree.rs, `redistribute_tail`). Fixed by
    /// popping the dangling separator into `pending_reinsert` before
    /// redistributing (see the comment at the `redistribute_tail` call site
    /// in `finish()`). All four sizes below were confirmed to build with
    /// zero underfull nodes post-fix, so this asserts full strict
    /// `check_invariants` rather than falling back to the benign-deviation
    /// safety checks.
    #[test]
    fn from_sorted_unclosed_level_debug_assert_family() {
        let base = MAX_KEYS as u64 + 1;
        let cube = base * base * base;
        let sq = base * base;
        for n in [
            cube + sq,
            cube + 2 * sq,
            cube + (MIN_KEYS as u64 - 1) * sq,
            2 * cube + sq,
        ] {
            let entries: Vec<_> = (0..n).map(|i| (i, Arc::new(i))).collect();
            let t = BTree::from_sorted(entries);
            assert_eq!(t.len(), n as usize);
            check_invariants(&t);
        }
    }

    #[test]
    fn diff_reports_added_updated_and_removed() {
        let base = BTree::<u64, String>::new()
            .insert(1, "one".into())
            .insert(2, "two".into())
            .insert(3, "three".into());
        let new = base
            .insert(2, "TWO".into())            // update
            .insert(4, "four".into())           // add
            .remove(&1)
            .unwrap();                          // remove

        let changes: Vec<_> = new
            .diff(&base)
            .map(|c| match c {
                Change::Added(k, v) => (*k, Some(v.to_string()), "add"),
                Change::Updated(k, v) => (*k, Some(v.to_string()), "upd"),
                Change::Removed(k) => (*k, None, "rem"),
            })
            .collect();

        assert_eq!(
            changes,
            vec![
                (1, None, "rem"),
                (2, Some("TWO".to_string()), "upd"),
                (4, Some("four".to_string()), "add"),
            ]
        );
    }

    #[test]
    fn diff_of_a_tree_against_itself_is_empty() {
        let t = BTree::<u64, u64>::new().insert(1, 10).insert(2, 20);
        assert_eq!(t.diff(&t).count(), 0);
        assert_eq!(t.clone().diff(&t).count(), 0);
    }

    #[test]
    fn diff_reports_no_change_when_the_same_value_arc_is_reinserted() {
        let base = BTree::<u64, u64>::new().insert(1, 10);
        let arc = base.get_arc(&1).unwrap();
        let new = base.insert_arc(1, arc);
        assert_eq!(new.diff(&base).count(), 0);
    }

    #[test]
    fn diff_skips_shared_subtrees_instead_of_walking_them() {
        // Deep enough to be several levels tall under both T=32 (63-key,
        // 64-way nodes) and T=8 (`fanout-t8`, 15-key, 16-way nodes) — see
        // `diff_oracle_tree_height` above for why a shallow tree can't
        // exercise the multi-level skip at all.
        let mut base = BTree::<u64, u64>::new();
        for i in 0..20_000u64 {
            base = base.insert(i, i);
        }
        let new = base.insert(10_000, 999_999);

        let mut d = new.diff(&base);
        let changes: Vec<_> = (&mut d).collect();
        assert_eq!(changes.len(), 1, "exactly one key changed");

        // A single changed key touches only the root-to-leaf path on each
        // side (roughly 2 * height descends, one path per cursor). Both
        // construction and traversal are deterministic (no hashing, no
        // randomized ops), so the measured counts below are exact, not a
        // typical case: 6 descends at T=32 (63-key, 64-way nodes, height 3),
        // 20 at T=8 (`fanout-t8`, 15-key, 16-way nodes, height 5 — narrower
        // fan-out means a taller tree for the same key count). The ceilings
        // give ~4x headroom over the measured value on each side — enough
        // that an incidental one-level height change doesn't trip the test,
        // nowhere close to the low thousands of descends a full walk of a
        // 20,000-key tree would cost if the `Arc::ptr_eq` skip in
        // `BTreeDiff::next` regressed to always-false.
        let ceiling = if cfg!(feature = "fanout-t8") { 64 } else { 24 };
        assert!(
            d.nodes_visited() <= ceiling,
            "diff visited {} nodes for a single changed key (ceiling {}) — \
             subtree skip is not firing",
            d.nodes_visited(),
            ceiling
        );
    }

    // -----------------------------------------------------------------
    // BTree::diff proptest oracle, deep-tree case.
    //
    // `tests/btree_diff_oracle.rs` covers narrow-key, high-collision
    // histories (small key space, so inserts/removes/updates collide
    // constantly) — but under the default T=32 (MAX_KEYS=63, 64-way
    // fan-out) that key space caps the tree at height 2, so it never
    // builds an internal node whose children are themselves internal:
    // exactly the case the multi-frame `stack.pop()` chain in
    // `DiffCursor::peek_entry`/`peek_child` and a non-leaf `Arc::ptr_eq`
    // subtree skip exist for. This case lives here instead of in the
    // integration test because proving it actually reached that depth
    // needs `root`/`children`, which aren't public API.
    //
    // Deterministic seed (`DIFF_ORACLE_DEEP_SEED` sequential inserts)
    // guarantees height >= 3 regardless of what the randomized ops layered
    // on top do, so update/remove coverage at depth isn't left to chance:
    // widening the key space alone (without the seed) would make collisions
    // — and therefore Updated/Removed coverage — vanishingly rare within a
    // few hundred random ops.
    // -----------------------------------------------------------------

    /// Levels from root to leaf, inclusive — a single-leaf (empty or small)
    /// tree is height 1. `height(t) >= 3` means some node's children are
    /// themselves internal nodes, not leaves.
    fn diff_oracle_tree_height<K, V>(t: &BTree<K, V>) -> usize {
        fn go<K, V>(node: &BTreeNode<K, V>) -> usize {
            if node.children.is_empty() {
                1
            } else {
                1 + go(node.children[0].load(None))
            }
        }
        go(t.root.load(None))
    }

    #[derive(Debug, Clone)]
    enum DiffOracleOp {
        Insert(u64, u64),
        Remove(u64),
    }

    // 64-way fan-out means a single overflowing child of the root (>63
    // entries) is already enough to reach height 3; this is a wide margin
    // over that, so the assertion in the proptest below is not a near thing.
    const DIFF_ORACLE_DEEP_SEED: u64 = 5_000;

    fn diff_oracle_deep_ops() -> impl Strategy<Value = Vec<DiffOracleOp>> {
        prop::collection::vec(
            prop_oneof![
                (0u64..DIFF_ORACLE_DEEP_SEED, 0u64..1000)
                    .prop_map(|(k, v)| DiffOracleOp::Insert(k, v)),
                (0u64..DIFF_ORACLE_DEEP_SEED).prop_map(DiffOracleOp::Remove),
            ],
            0..300,
        )
    }

    // Same generation-counter trick as the integration test's oracle (see
    // its `apply` doc comment): `diff` compares `Arc::ptr_eq`, not value
    // equality, and `BTree::insert` allocates a fresh value `Arc` on every
    // call, so the model must track per-key identity, not just the value.
    fn diff_oracle_apply(
        tree: &BTree<u64, u64>,
        model: &mut std::collections::BTreeMap<u64, (u64, u64)>,
        generation: &mut u64,
        op: &DiffOracleOp,
    ) -> BTree<u64, u64> {
        match op {
            DiffOracleOp::Insert(k, v) => {
                *generation += 1;
                model.insert(*k, (*v, *generation));
                tree.insert(*k, *v)
            }
            DiffOracleOp::Remove(k) => {
                model.remove(k);
                tree.remove(k).unwrap_or_else(|_| tree.clone())
            }
        }
    }

    fn diff_oracle_expected(
        new: &std::collections::BTreeMap<u64, (u64, u64)>,
        base: &std::collections::BTreeMap<u64, (u64, u64)>,
    ) -> Vec<(u64, Option<u64>, &'static str)> {
        let mut out = Vec::new();
        let mut keys: Vec<u64> = new.keys().chain(base.keys()).copied().collect();
        keys.sort_unstable();
        keys.dedup();
        for k in keys {
            match (new.get(&k), base.get(&k)) {
                (Some((_, ng)), Some((_, bg))) if ng == bg => {}
                (Some((nv, _)), Some(_)) => out.push((k, Some(*nv), "upd")),
                (Some((nv, _)), None) => out.push((k, Some(*nv), "add")),
                (None, Some(_)) => out.push((k, None, "rem")),
                (None, None) => unreachable!("key came from one of the two maps"),
            }
        }
        out
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        #[test]
        fn diff_matches_full_scan_oracle_deep_tree(
            rand_base in diff_oracle_deep_ops(),
            rand_then in diff_oracle_deep_ops(),
        ) {
            let mut model = std::collections::BTreeMap::new();
            let mut generation = 0u64;
            let mut tree = BTree::<u64, u64>::new();

            // Deterministic seed first: forces height >= 3 no matter what
            // the randomized ops (which may remove far more than they add)
            // do afterward.
            for k in 0..DIFF_ORACLE_DEEP_SEED {
                tree = diff_oracle_apply(
                    &tree, &mut model, &mut generation, &DiffOracleOp::Insert(k, k),
                );
            }
            for op in &rand_base {
                tree = diff_oracle_apply(&tree, &mut model, &mut generation, op);
            }
            let base_tree = tree.clone();
            let base_model = model.clone();

            // Confirm the deep path is actually reached, rather than assume
            // the seed size is adequate.
            let base_height = diff_oracle_tree_height(&base_tree);
            prop_assert!(
                base_height >= 3,
                "deep-tree proptest case only reached height {} — \
                 DIFF_ORACLE_DEEP_SEED needs raising",
                base_height
            );

            for op in &rand_then {
                tree = diff_oracle_apply(&tree, &mut model, &mut generation, op);
            }

            let got: Vec<(u64, Option<u64>, &'static str)> = tree
                .diff(&base_tree)
                .map(|c| match c {
                    Change::Added(k, v) => (*k, Some(**v), "add"),
                    Change::Updated(k, v) => (*k, Some(**v), "upd"),
                    Change::Removed(k) => (*k, None, "rem"),
                })
                .collect();

            prop_assert_eq!(got, diff_oracle_expected(&model, &base_model));
        }
    }

    // -----------------------------------------------------------------
    // task-2b: FixedVec storage without the Option discriminant
    // -----------------------------------------------------------------

    #[test]
    fn child_slots_are_sixteen_bytes() {
        use crate::child::Child;
        assert_eq!(std::mem::size_of::<Child<u64, u64>>(), 16);
        // 65 slots + len (padded to alignment). Must not regress to the Option layout (1560 + 8).
        assert!(std::mem::size_of::<Children<u64, u64>>() <= 65 * 16 + 8, "{}", std::mem::size_of::<Children<u64, u64>>());
    }

    mod fixed_vec {
        use super::super::*;
        use proptest::prelude::*;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        const CAP: usize = 8;
        type FV = FixedVec<Elem, CAP>;

        /// Drop-counting element. `live` is a per-test counter shared (via
        /// `Arc`) by every clone/copy of this value; `Clone` bumps it,
        /// `Drop` decrements it, so at any point `live.load()` is exactly
        /// the number of `Elem` instances currently alive — including ones
        /// still sitting inside a `FixedVec` and ones already moved out of
        /// one (e.g. by `pop`/`remove`/`into_iter`).
        struct Elem {
            val: u32,
            live: Arc<AtomicUsize>,
        }
        impl Elem {
            fn new(val: u32, live: &Arc<AtomicUsize>) -> Self {
                live.fetch_add(1, Ordering::SeqCst);
                Elem { val, live: live.clone() }
            }
        }
        impl Clone for Elem {
            fn clone(&self) -> Self {
                self.live.fetch_add(1, Ordering::SeqCst);
                Elem { val: self.val, live: self.live.clone() }
            }
        }
        impl Drop for Elem {
            fn drop(&mut self) {
                self.live.fetch_sub(1, Ordering::SeqCst);
            }
        }
        impl PartialEq for Elem {
            fn eq(&self, other: &Self) -> bool {
                self.val == other.val
            }
        }
        impl std::fmt::Debug for Elem {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "Elem({})", self.val)
            }
        }

        #[test]
        fn drop_releases_exactly_the_live_prefix() {
            let live = Arc::new(AtomicUsize::new(0));
            {
                let mut v: FV = FixedVec::new();
                for i in 0..5 {
                    v.push(Elem::new(i, &live));
                }
                assert_eq!(live.load(Ordering::SeqCst), 5);
            }
            assert_eq!(live.load(Ordering::SeqCst), 0, "Drop must release exactly [0, len)");
        }

        #[test]
        fn clone_duplicates_live_prefix_only() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            for i in 0..4 {
                v.push(Elem::new(i, &live));
            }
            assert_eq!(live.load(Ordering::SeqCst), 4);
            let v2 = v.clone();
            assert_eq!(live.load(Ordering::SeqCst), 8, "clone duplicates the live prefix");
            assert_eq!(v2.len(), 4);
            drop(v2);
            assert_eq!(live.load(Ordering::SeqCst), 4);
            drop(v);
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn clone_panic_mid_clone_drops_only_what_was_already_cloned() {
            // A `Clone` that panics on a specific value, so we can see that
            // FixedVec::clone doesn't leak or double-drop the partial result.
            struct PanicOnThird {
                val: u32,
                live: Arc<AtomicUsize>,
            }
            impl Clone for PanicOnThird {
                fn clone(&self) -> Self {
                    if self.val == 2 {
                        panic!("intentional clone panic");
                    }
                    self.live.fetch_add(1, Ordering::SeqCst);
                    PanicOnThird { val: self.val, live: self.live.clone() }
                }
            }
            impl Drop for PanicOnThird {
                fn drop(&mut self) {
                    self.live.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FixedVec<PanicOnThird, CAP> = FixedVec::new();
            for val in 0..4u32 {
                live.fetch_add(1, Ordering::SeqCst);
                v.push(PanicOnThird { val, live: live.clone() });
            }
            assert_eq!(live.load(Ordering::SeqCst), 4);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| v.clone()));
            assert!(result.is_err(), "clone of element index 2 must panic");
            // The two already-cloned elements (index 0, 1) must have been
            // dropped along with the aborted partial FixedVec, not leaked.
            assert_eq!(live.load(Ordering::SeqCst), 4, "only the original 4 elements remain live");
            drop(v);
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn into_iter_partial_consumption_drops_remainder() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            for i in 0..6 {
                v.push(Elem::new(i, &live));
            }
            let mut it = v.into_iter();
            let first = it.next().unwrap();
            assert_eq!(first.val, 0);
            assert_eq!(live.load(Ordering::SeqCst), 6, "yielded element still alive, owned by caller");
            drop(first);
            assert_eq!(live.load(Ordering::SeqCst), 5);
            drop(it); // must drop the remaining 5 (indices 1..6)
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn remove_drops_only_the_removed_element() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            for i in 0..5 {
                v.push(Elem::new(i, &live));
            }
            let removed = v.remove(2);
            assert_eq!(removed.val, 2);
            assert_eq!(live.load(Ordering::SeqCst), 5, "removed value still owned by the caller");
            drop(removed);
            assert_eq!(live.load(Ordering::SeqCst), 4);
            let vals: Vec<u32> = v.iter().map(|e| e.val).collect();
            assert_eq!(vals, vec![0, 1, 3, 4]);
            drop(v);
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn pop_drops_nothing_extra_and_returns_owned() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            for i in 0..3 {
                v.push(Elem::new(i, &live));
            }
            let popped = v.pop().unwrap();
            assert_eq!(popped.val, 2);
            assert_eq!(live.load(Ordering::SeqCst), 3);
            drop(popped);
            assert_eq!(live.load(Ordering::SeqCst), 2);
            drop(v);
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn split_off_moves_the_tail_without_dropping_or_cloning() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            for i in 0..6 {
                v.push(Elem::new(i, &live));
            }
            assert_eq!(live.load(Ordering::SeqCst), 6);
            let tail = v.split_off(2);
            assert_eq!(live.load(Ordering::SeqCst), 6, "moved, not cloned or dropped");
            assert_eq!(v.len(), 2);
            assert_eq!(tail.len(), 4);
            assert_eq!(v.iter().map(|e| e.val).collect::<Vec<_>>(), vec![0, 1]);
            assert_eq!(tail.iter().map(|e| e.val).collect::<Vec<_>>(), vec![2, 3, 4, 5]);
            drop(v);
            assert_eq!(live.load(Ordering::SeqCst), 4);
            drop(tail);
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn insert_shifts_without_dropping_or_cloning() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            for i in [0, 1, 3, 4] {
                v.push(Elem::new(i, &live));
            }
            v.insert(2, Elem::new(2, &live));
            assert_eq!(live.load(Ordering::SeqCst), 5);
            assert_eq!(v.iter().map(|e| e.val).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);
            drop(v);
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn extend_pushes_all_without_dropping_existing() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            v.push(Elem::new(0, &live));
            v.extend([Elem::new(1, &live), Elem::new(2, &live)]);
            assert_eq!(live.load(Ordering::SeqCst), 3);
            assert_eq!(v.iter().map(|e| e.val).collect::<Vec<_>>(), vec![0, 1, 2]);
            drop(v);
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }

        #[test]
        #[should_panic(expected = "index out of live range")]
        fn index_out_of_live_range_panics() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            v.push(Elem::new(0, &live));
            let _ = &v[5]; // len == 1, 5 < N == CAP == 8: within capacity, outside live range
        }

        // -------------------------------------------------------------
        // Release-mode preconditions on insert/remove/split_off: the checks
        // guarding the raw ptr::copy/assume_init calls must be real `assert!`s
        // (not `debug_assert!`), since in release a violated precondition
        // there is a write past the array, a read of uninitialized memory, or
        // a wrapped `usize` length fed straight into a memcpy.
        // -------------------------------------------------------------

        #[test]
        #[should_panic(expected = "FixedVec::push: at capacity")]
        fn push_at_capacity_panics() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            for i in 0..CAP as u32 {
                v.push(Elem::new(i, &live));
            }
            v.push(Elem::new(99, &live)); // len == N == CAP: no room left
        }

        #[test]
        #[should_panic(expected = "FixedVec::insert: out of bounds or at capacity")]
        fn insert_at_capacity_panics() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            for i in 0..CAP as u32 {
                v.push(Elem::new(i, &live));
            }
            v.insert(0, Elem::new(99, &live)); // len == N == CAP: no room left
        }

        #[test]
        #[should_panic(expected = "FixedVec::remove: out of bounds")]
        fn remove_at_len_panics() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            v.push(Elem::new(0, &live));
            v.remove(1); // idx == len: within capacity, nothing live there
        }

        #[test]
        #[should_panic(expected = "FixedVec::split_off: out of bounds")]
        fn split_off_past_len_panics() {
            let live = Arc::new(AtomicUsize::new(0));
            let mut v: FV = FixedVec::new();
            v.push(Elem::new(0, &live));
            let _ = v.split_off(2); // at == len + 1
        }

        // -------------------------------------------------------------
        // A real, non-`Freeze`, refcount-releasing element type (`Child`,
        // the whole reason this task exists) moved through every FixedVec
        // path, with `Arc::strong_count` checked before/after each step.
        // Deterministic, no proptest — cheap enough to run under Miri too
        // (unlike `matches_vec_oracle` below).
        // -------------------------------------------------------------

        #[test]
        fn child_moves_through_every_fixedvec_path_with_correct_refcount() {
            use crate::child::Child;

            let mut leaf_entries: Entries<u64, u64> = FixedVec::new();
            leaf_entries.push((1u64, Arc::new(10u64)));
            let leaf: Arc<BTreeNode<u64, u64>> = Arc::new(BTreeNode { entries: leaf_entries, children: Default::default() });
            assert_eq!(Arc::strong_count(&leaf), 1);

            // push (x3)
            let mut v: FixedVec<Child<u64, u64>, 8> = FixedVec::new();
            for _ in 0..3 {
                v.push(Child::resident(Arc::clone(&leaf)));
            }
            assert_eq!(v.len(), 3);
            assert_eq!(Arc::strong_count(&leaf), 1 + 3, "push must bump the refcount exactly once per element");

            // insert
            v.insert(1, Child::resident(Arc::clone(&leaf)));
            assert_eq!(v.len(), 4);
            assert_eq!(Arc::strong_count(&leaf), 1 + 4, "insert must shift by raw copy, not clone the shifted elements");

            // remove
            let removed = v.remove(0);
            assert_eq!(Arc::strong_count(&leaf), 1 + 4, "removed Child is still owned by the caller, not yet dropped");
            drop(removed);
            assert_eq!(Arc::strong_count(&leaf), 1 + 3, "dropping the removed Child releases exactly its one share");
            assert_eq!(v.len(), 3);

            // split_off (moves the tail, no clone/drop)
            let tail = v.split_off(1);
            assert_eq!(Arc::strong_count(&leaf), 1 + 3, "split_off moves Children, it must not clone or drop any");
            assert_eq!(v.len(), 1);
            assert_eq!(tail.len(), 2);

            // pop
            let popped = v.pop().unwrap();
            assert_eq!(Arc::strong_count(&leaf), 1 + 3, "popped Child is still owned by the caller");
            drop(popped);
            assert_eq!(Arc::strong_count(&leaf), 1 + 2);
            assert_eq!(v.len(), 0);

            // Clone (bumps refcount per live element, same as Child::clone would)
            let tail2 = tail.clone();
            assert_eq!(Arc::strong_count(&leaf), 1 + 2 + 2, "FixedVec::clone must clone every live Child, bumping the refcount");
            drop(tail2);
            assert_eq!(Arc::strong_count(&leaf), 1 + 2);

            // into_iter, partially consumed: yielded element stays alive in
            // the caller's hand; dropping the iterator must drop exactly the
            // un-yielded remainder, not double-drop or leak.
            let mut it = tail.into_iter();
            let first = it.next().unwrap();
            assert_eq!(Arc::strong_count(&leaf), 1 + 2, "yielded Child still alive, owned by the caller");
            drop(first);
            assert_eq!(Arc::strong_count(&leaf), 1 + 1);
            drop(it); // must drop the one un-yielded remaining Child
            assert_eq!(Arc::strong_count(&leaf), 1, "IntoIterator's Drop must release exactly the un-yielded tail");

            drop(v); // already empty; must not touch the refcount
            assert_eq!(Arc::strong_count(&leaf), 1);
        }

        // -------------------------------------------------------------
        // Vec<u32> oracle proptest over random op sequences (N = 8).
        // -------------------------------------------------------------

        #[derive(Debug, Clone)]
        enum Op {
            Push(u32),
            Pop,
            Insert(usize, u32),
            Remove(usize),
            SplitOffRejoin(usize),
            Extend(Vec<u32>),
        }

        fn op_strategy() -> impl Strategy<Value = Op> {
            prop_oneof![
                any::<u32>().prop_map(Op::Push),
                Just(Op::Pop),
                (0..=CAP, any::<u32>()).prop_map(|(idx, v)| Op::Insert(idx, v)),
                (0..CAP).prop_map(Op::Remove),
                (0..=CAP).prop_map(Op::SplitOffRejoin),
                prop::collection::vec(any::<u32>(), 0..4).prop_map(Op::Extend),
            ]
        }

        proptest! {
            // Full case count normally; Miri interprets every op (including
            // the ptr::copy/assume_init_* calls), so 200 cases here cost
            // ~53 minutes per borrow model. 8 cases is still a meaningful
            // Miri smoke check, and the real proptest+op-mix coverage still
            // runs at full strength under `cargo test`.
            #![proptest_config(ProptestConfig { cases: if cfg!(miri) { 8 } else { 200 }, ..ProptestConfig::default() })]
            #[test]
            fn matches_vec_oracle(ops in prop::collection::vec(op_strategy(), 0..60)) {
                let mut fv: FixedVec<u32, CAP> = FixedVec::new();
                let mut model: Vec<u32> = Vec::new();
                for op in ops {
                    match op {
                        Op::Push(v) => {
                            if model.len() < CAP {
                                fv.push(v);
                                model.push(v);
                            }
                        }
                        Op::Pop => {
                            let a = fv.pop();
                            let b = model.pop();
                            prop_assert_eq!(a, b);
                        }
                        Op::Insert(idx, v) => {
                            if model.len() < CAP {
                                let idx = idx.min(model.len());
                                fv.insert(idx, v);
                                model.insert(idx, v);
                            }
                        }
                        Op::Remove(idx) => {
                            if !model.is_empty() {
                                let idx = idx % model.len();
                                let a = fv.remove(idx);
                                let b = model.remove(idx);
                                prop_assert_eq!(a, b);
                            }
                        }
                        Op::SplitOffRejoin(at) => {
                            let at = at.min(model.len());
                            let tail_fv = fv.split_off(at);
                            let tail_model = model.split_off(at);
                            prop_assert_eq!(tail_fv.to_vec(), tail_model.clone());
                            fv.extend(tail_fv);
                            model.extend(tail_model);
                        }
                        Op::Extend(vals) => {
                            let room = CAP - model.len();
                            let vals: Vec<u32> = vals.into_iter().take(room).collect();
                            fv.extend(vals.clone());
                            model.extend(vals);
                        }
                    }
                    prop_assert_eq!(fv.to_vec(), model.clone());
                    prop_assert_eq!(fv.len(), model.len());
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // Paging primitives: height, write_dirty, demote_leaves,
    // changed_page_ids, from_root_page, load_inner_levels,
    // resident_leaf_estimate, for_each_page_id — all exercised against
    // `crate::child::tests::MockDisk`, an in-memory mock "disk".
    // -----------------------------------------------------------------
    mod paged {
        use super::*;
        use crate::child::tests::MockDisk;
        use std::sync::Arc;

        /// Write every dirty node into `disk`, assigning sequential ids
        /// 4096 apart (page-sized, though `MockDisk` doesn't care). Returns
        /// the root id.
        ///
        /// Stores a *detached* image, not `node.clone()`: `Child::clone`
        /// preserves an already-resident child's live pointer (that's the
        /// point of it — a CoW node clone must not evict anything), so a
        /// plain structural clone of an inner node would carry its whole
        /// resident subtree into the "page" via shared `Arc`s. A real disk
        /// page holds only the entries and each child's id (`write_dirty`
        /// visits post-order, so every child already has one); rebuilding
        /// `children` as fresh `Child::on_disk` slots is what makes reading
        /// this page back later actually fault its children instead of
        /// reusing the writer's own live copies.
        fn flush(t: &BTree<u64, u64>, disk: &MockDisk<u64, u64>, next: &mut u64) -> u64 {
            t.write_dirty(&mut |node, _leaf| {
                *next += 4096;
                let detached = BTreeNode {
                    entries: node.entries.clone(),
                    children: node
                        .children
                        .iter()
                        .map(|c| Child::on_disk(c.page_id().expect("write_dirty: child written before its parent")))
                        .collect(),
                };
                disk.put(*next, Arc::new(detached));
                *next
            })
        }
        fn tree(n: u64) -> BTree<u64, u64> {
            BTree::from_sorted((1..=n).map(|k| (k, Arc::new(k))))
        }

        #[test]
        fn height_of_bulk_tree() {
            assert_eq!(tree(10).height(), 0);
            assert_eq!(tree(10_000).height(), 2, "10k rows at MAX_KEYS=63: leaves, one inner level, root");
        }

        #[test]
        fn write_dirty_is_post_order_and_marks_clean() {
            let disk = MockDisk::new();
            let t = tree(5_000);
            let mut next = 0;
            let mut order = Vec::new();
            let root = t.write_dirty(&mut |node, leaf| {
                next += 1;
                order.push((next, leaf));
                disk.put(next, Arc::new(node.clone()));
                next
            });
            assert_eq!(order.last().unwrap(), &(root, false), "root written last");
            assert!(order.iter().take_while(|(_, l)| *l).count() > 0, "leaves before their parent");
            // Nothing dirty remains: a second walk writes zero pages.
            let mut writes = 0;
            t.write_dirty(&mut |_, _| {
                writes += 1;
                0
            });
            assert_eq!(writes, 0);
        }

        #[test]
        fn write_dirty_after_one_insert_writes_exactly_one_path() {
            let disk = MockDisk::new();
            let mut t = tree(5_000);
            let mut next = 0;
            flush(&t, &disk, &mut next);
            t.set_source(Some(Arc::new(MockDisk::<u64, u64>::new())));
            t.insert_mut(2_500, 1);
            let mut writes = 0;
            t.write_dirty(&mut |_, _| {
                writes += 1;
                next += 4096;
                next
            });
            assert_eq!(writes, t.height() + 1, "one node per level on the CoW path");
        }

        #[test]
        fn demote_then_read_faults_exactly_touched_leaves() {
            let disk = Arc::new(MockDisk::new());
            let mut t = tree(20_000);
            let mut next = 0;
            flush(&t, &disk, &mut next);
            t.set_source(Some(disk.clone()));
            let (t2, demoted, cursor) = t.demote_leaves(None, usize::MAX);
            assert!(cursor.is_none());
            assert!(demoted > 300, "20k rows ≈ 318 leaves, all quiet");
            assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), 0, "demotion loads nothing");
            for k in [1u64, 2, 3, 10_000, 19_999] {
                assert_eq!(t2.get(&k), Some(&k));
            }
            // 1,2,3 share a leaf; 10_000 and 19_999 are two more: 3 leaves.
            assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), 3);
            // Old version untouched and fully resident.
            assert!(t.resident_leaf_estimate() > t2.resident_leaf_estimate());
        }

        #[test]
        fn demote_gives_accessed_leaves_a_second_chance() {
            // Controller ruling: `get()` marking every resident hit accessed
            // (including a call made purely to *observe* residency) is the
            // spec's second-chance semantics working as intended, not a bug
            // — a page re-touched between sweeps legitimately survives. So
            // this test observes pass-1 survival via `resident_leaf_estimate`
            // (`is_loaded()`-based, never marks) instead of a `get()`, which
            // would itself re-arm the bit it's trying to check.
            //
            // Deliberately tracks key 5, in the *leftmost* leaf, not an
            // arbitrary interior one: `resident_leaf_estimate` calls
            // `height()` to find its depth, and `height()` walks — and, if
            // what it finds isn't resident, faults — the leftmost path to do
            // that (its own doc comment: "Loads only the leftmost path").
            // With the accessed leaf being anywhere *else*, that leftmost
            // walk would find the (unrelated, correctly-demoted) leftmost
            // leaf on disk and fault it back in as a side effect of the
            // residency check itself — corrupting the very count being
            // observed with a leaf nobody asked about. Tracking the leftmost
            // leaf sidesteps that for the pass-1 check: it's exactly the one
            // leaf still resident, so `height()`'s walk down to it is a
            // free peek, not a fault.
            //
            // That same fact is why there's no analogous `resident_leaf_estimate() == 0`
            // check after pass 2: once the *tracked* (leftmost) leaf itself
            // is genuinely on disk, asking "is anything resident" via
            // `resident_leaf_estimate` would unavoidably fault it straight
            // back in to answer the question — there is no way to observe
            // "nothing is resident" through this primitive without
            // resurrecting the one thing being checked. `get()` + the
            // read-count below proves the same fact honestly instead: if the
            // leaf were still resident, `get` would cost zero additional
            // reads; it costs exactly one, so it wasn't.
            let disk = Arc::new(MockDisk::new());
            let mut t = tree(20_000);
            let mut next = 0;
            flush(&t, &disk, &mut next);
            t.set_source(Some(disk.clone()));
            t.get(&5); // marks the leftmost leaf accessed
            let before = t.resident_leaf_estimate(); // is_loaded()-based, does not mark
            let (t2, _d1, _) = t.demote_leaves(None, usize::MAX);
            assert_eq!(
                t2.resident_leaf_estimate(),
                Child::<u64, u64>::NODE_BYTES,
                "exactly the accessed leaf survived pass 1"
            );
            assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), 0);
            let (t3, d2, _) = t2.demote_leaves(None, usize::MAX);
            assert_eq!(d2, 1, "second pass takes it");
            assert_eq!(t3.get(&5), Some(&5));
            assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), 1, "and now it faults");
            let _ = before;
        }

        #[test]
        fn demote_respects_budget_and_resumes_from_cursor() {
            let disk = Arc::new(MockDisk::new());
            let mut t = tree(20_000);
            let mut next = 0;
            flush(&t, &disk, &mut next);
            t.set_source(Some(disk.clone()));
            let (t2, d1, c1) = t.demote_leaves(None, 2);
            assert!(c1.is_some() && d1 <= 2 * 64);
            let (t3, d2, c2) = t2.demote_leaves(c1.as_ref(), usize::MAX);
            assert!(c2.is_none());
            assert!(d1 + d2 > 300);
            assert_eq!(t3.len(), 20_000);
        }

        #[test]
        fn demote_leaves_with_no_quiet_leaves_leaves_write_dirty_with_zero_writes() {
            let disk = Arc::new(MockDisk::new());
            let mut t = tree(5_000);
            let mut next = 0;
            flush(&t, &disk, &mut next);
            t.set_source(Some(disk.clone()));
            // Touch every leaf so nothing is quiet when we demote.
            for k in 1..=5_000u64 {
                t.get(&k);
            }
            let (t2, demoted, _) = t.demote_leaves(None, usize::MAX);
            assert_eq!(demoted, 0, "every leaf was accessed; none should be demoted");
            let mut writes = 0;
            t2.write_dirty(&mut |_, _| {
                writes += 1;
                0
            });
            assert_eq!(writes, 0, "restore_unchanged_ids must give every parent back its old page id");
        }

        #[test]
        fn changed_page_ids_is_the_replaced_path() {
            let disk = Arc::new(MockDisk::new());
            let mut t = tree(20_000);
            let mut next = 0;
            flush(&t, &disk, &mut next);
            let prev = t.clone();
            t.set_source(Some(disk.clone()));
            t.insert_mut(7, 70);
            flush(&t, &disk, &mut next);
            let dead = t.changed_page_ids(&prev);
            assert_eq!(dead.len(), t.height() + 1, "old leaf + old inner path + old root");
            assert!(dead.iter().all(|id| prev_has(&prev, *id)));
            fn prev_has(p: &BTree<u64, u64>, id: u64) -> bool {
                let mut found = false;
                p.for_each_page_id(&mut |x| found |= x == id);
                found
            }
        }

        #[test]
        fn from_root_page_and_load_inner_levels() {
            let disk = Arc::new(MockDisk::new());
            let t = tree(20_000);
            let mut next = 0;
            let root = flush(&t, &disk, &mut next);
            // `height` comes from the fully-resident source tree — exactly
            // what a real root record would have persisted alongside the
            // root id, and what lets `from_root_page` skip the (now-gone)
            // leftmost-leaf fault to learn it.
            let t2: BTree<u64, u64> = BTree::from_root_page(root, 20_000, t.height(), disk.clone());
            assert_eq!(t2.height(), t.height());
            assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), 0, "height is a cached field: attaching a cold tree costs nothing");
            t2.load_inner_levels();
            let inner = disk.reads.load(std::sync::atomic::Ordering::Relaxed);
            assert!(inner < 20, "root + one inner level (~6 nodes); no leaf fault to learn depth any more");
            assert_eq!(t2.get(&123), Some(&123));
            assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), inner + 1);
        }

        #[test]
        fn load_inner_levels_stops_exactly_at_leaves() {
            // Ruling (b): every slot `load_inner_levels` stops descending
            // at (depth 0, relative to the root) must actually be a leaf —
            // faulting it here, in the test only, to check `children.is_empty()`
            // is how we verify that without teaching production code to
            // fault leaves just to assert about them.
            let disk = Arc::new(MockDisk::new());
            let t = tree(20_000);
            let mut next = 0;
            let root = flush(&t, &disk, &mut next);
            let t2: BTree<u64, u64> = BTree::from_root_page(root, 20_000, t.height(), disk.clone());
            t2.load_inner_levels();

            fn check(slot: &Child<u64, u64>, depth: usize, src: Option<&dyn NodeSource<u64, u64>>) {
                if depth == 0 {
                    assert!(!slot.is_loaded(), "load_inner_levels must leave every leaf on disk");
                    let n = slot.load(src); // test-only fault: verify it's a real leaf
                    assert!(n.children.is_empty(), "the slot load_inner_levels stopped at must actually be a leaf");
                    return;
                }
                assert!(slot.is_loaded(), "every inner level must be resident after load_inner_levels");
                let n = slot.load(src);
                for c in n.children.iter() {
                    check(c, depth - 1, src);
                }
            }
            check(&t2.root, t2.height(), t2.source.as_deref());
        }

        // -------------------------------------------------------------
        // Review findings, fix round 1.
        //
        // IMPORTANT #1: `changed_page_ids`/`for_each_page_id` used the
        // marking `Child::load`, so a checkpoint diff or punch-bookkeeping
        // walk could mark a leaf "recently used" purely by looking at it —
        // giving it an undeserved second chance the next `demote_leaves`
        // pass. `load_quiet` everywhere in those two functions (and
        // `restore_unchanged_ids`/`resident_leaf_estimate`, for the same
        // reason) fixes it; this test is built to fail against the old
        // marking `load`.
        // -------------------------------------------------------------
        #[test]
        fn changed_page_ids_and_for_each_page_id_do_not_re_arm_accessed() {
            let disk = Arc::new(MockDisk::new());
            let mut t = tree(20_000);
            let mut next = 0;
            flush(&t, &disk, &mut next);
            t.set_source(Some(disk.clone()));
            // Two leaves get a *real* workload touch.
            t.get(&1);
            t.get(&19_999);
            // Pass A: everything else (never touched) demotes; the two
            // touched leaves survive on their second chance — which clears
            // their accessed bit. The tree is now effectively fully
            // demoted except those two survivors.
            let (t2, da, _) = t.demote_leaves(None, usize::MAX);
            assert!(da > 300, "everything but the two touched leaves demotes");
            // Walk it exactly the way a checkpoint diff / punch pass would:
            // compare against a deliberately unrelated tree (so
            // `changed_page_ids` can't skip via matching ids and is forced
            // to walk what it reaches) and separately via `for_each_page_id`.
            // Neither may re-arm the two leaves' just-cleared accessed bit.
            // `prev` is deliberately unrelated (empty): `changed_page_ids`
            // reports "referenced by prev, not by self" (dead-page GC
            // candidates), and an empty `prev` never references anything —
            // so the returned list is legitimately empty here. What matters
            // for this test is the *side effect*: comparing against an
            // empty `prev` means no id anywhere can match via the
            // "identical subtree, skip whole" optimization, forcing
            // `collect` to actually walk every node it can reach in `t2`
            // (exactly the shape a checkpoint diff or GC pass takes in
            // practice, where a lot has changed).
            let prev: BTree<u64, u64> = BTree::new();
            let _dead = t2.changed_page_ids(&prev);
            let mut count = 0;
            t2.for_each_page_id(&mut |_| count += 1);
            assert!(count > 0);
            // Pass B, the pass following the walks: with nothing having
            // re-armed them, the two leaves' second chance is used up and
            // they demote now — and only they, since everything else was
            // already on disk (free `!any` skip, nothing left to consider).
            let (t3, db, _) = t2.demote_leaves(None, usize::MAX);
            assert_eq!(db, 2, "exactly the two leaves the walks must not have re-armed");
            let _ = t3;
        }

        // -------------------------------------------------------------
        // IMPORTANT #2: `changed_page_ids` assumed inner levels are
        // resident on both sides ("an on-disk slot is a leaf"). A tree
        // fresh off `from_root_page` violates that — its whole spine can
        // still be on disk — and the old implementation would then treat
        // every inner id as a leaf id, never match anything, and report
        // every page of an otherwise-identical tree as dead (a live-page
        // hole-punch, for a later task's GC). Calling `load_inner_levels`
        // on both trees at entry fixes it.
        // -------------------------------------------------------------
        #[test]
        fn changed_page_ids_loads_inner_levels_for_a_cold_tree() {
            let disk = Arc::new(MockDisk::new());
            let t = tree(20_000);
            let mut next = 0;
            let root = flush(&t, &disk, &mut next);
            // Cold: only the root id is known, nothing faulted yet.
            let cold: BTree<u64, u64> = BTree::from_root_page(root, 20_000, t.height(), disk.clone());
            // Warm twin: `t` itself, still fully resident, referencing the
            // exact same pages (it's what got flushed).
            let dead = cold.changed_page_ids(&t);
            assert!(dead.is_empty(), "same pages, only residency differs — nothing is actually dead");
        }

        // -------------------------------------------------------------
        // Controller ruling 1: BulkBuilder's source-threading gap. A tree
        // with a source and some on-disk leaves, `extend_from_sorted` with
        // keys beyond its max, then `get` a key in an on-disk leaf that
        // `extend_from_sorted` never touched (it only ever rewrites the
        // right spine) — must not panic, and must fault exactly once.
        // -------------------------------------------------------------
        #[test]
        fn bulk_builder_carries_source_through_extend() {
            let disk = Arc::new(MockDisk::new());
            let mut t = tree(1_000);
            let mut next = 0;
            flush(&t, &disk, &mut next);
            t.set_source(Some(disk.clone()));
            let (mut t2, demoted, _) = t.demote_leaves(None, usize::MAX);
            assert!(demoted > 0, "need at least one on-disk leaf for this test to mean anything");
            t2.extend_from_sorted((1_001..=1_010).map(|k| (k, Arc::new(k))));
            // Key 1 lives in the leftmost leaf, untouched by
            // `extend_from_sorted` (which only ever rewrites the right
            // spine): must fault (not panic), exactly once.
            let reads_before = disk.reads.load(std::sync::atomic::Ordering::Relaxed);
            assert_eq!(t2.get(&1), Some(&1));
            assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), reads_before + 1);
        }

        #[test]
        fn seed_from_spine_empty_tree_keeps_source() {
            // Ruling 1, fourth site: `seed_from_spine`'s empty-tree branch
            // returned `Self::new()`, silently dropping the tree's source.
            // Delete everything, then extend — the rebuilt tree must still
            // be able to fault.
            let disk = Arc::new(MockDisk::new());
            let mut t = tree(10);
            t.set_source(Some(disk.clone()));
            for k in 1..=10u64 {
                t.remove_mut(&k);
            }
            assert!(t.is_empty());
            t.extend_from_sorted((1..=5u64).map(|k| (k, Arc::new(k))));
            assert!(t.source().is_some());
        }

        /// Independent height oracle: descends the leftmost path counting
        /// levels — the technique `BTree::height()` itself used before the
        /// cached-`height`-field ruling replaced it. Test-only, used to
        /// catch a stale cached height after a random insert/remove
        /// sequence (splits and merges are exactly what could desync a
        /// cache like this) — `resident` never carries a source, so this
        /// never faults.
        fn walk_height(t: &BTree<u64, u64>) -> usize {
            let src = t.source.as_deref();
            let mut h = 0;
            let mut n = t.root.load(src);
            while !n.children.is_empty() {
                n = n.children[0].load(src);
                h += 1;
            }
            h
        }

        proptest! {
            #[test]
            fn on_disk_twin_agrees_and_faults_once_per_leaf(ops in proptest::collection::vec((0u64..5_000, 0u8..3), 1..300)) {
                let disk = Arc::new(MockDisk::new());
                let mut resident = tree(5_000);
                let mut next = 0;
                flush(&resident, &disk, &mut next);
                let mut paged = { let mut p = resident.clone(); p.set_source(Some(disk.clone())); p.demote_leaves(None, usize::MAX).0 };
                for (k, op) in ops {
                    match op {
                        0 => prop_assert_eq!(resident.get(&k), paged.get(&k)),
                        1 => { resident.insert_mut(k, k + 1); paged.insert_mut(k, k + 1); }
                        _ => { resident.remove_mut(&k); paged.remove_mut(&k); }
                    }
                }
                prop_assert_eq!(resident.height(), walk_height(&resident), "cached height must track real splits/merges");
                let a: Vec<_> = resident.range(..).map(|(k, v)| (*k, *v)).collect();
                let b: Vec<_> = paged.range(..).map(|(k, v)| (*k, *v)).collect();
                prop_assert_eq!(a, b);
            }
        }
    }
}
