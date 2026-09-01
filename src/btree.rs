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
/// Steady-state max entries per node (`FixedVec` capacity is one more than
/// this — see `Entries`/`Children`'s docs). `pub(crate)` so `pagecodec.rs`'s
/// `NodeCodec::decode` can bound-check an untrusted on-disk entry count
/// against this build's actual fanout before it ever reaches `FixedVec::push`
/// (I-2(a)): a `fanout-t8` build reading a `pages.bin` written under the
/// default T=32 fanout must reject the mismatch as `CheckpointCorrupted`,
/// not panic on the first node wide enough to overflow T=8's capacity.
pub(crate) const MAX_KEYS: usize = 2 * T - 1;

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

/// A leaf entry's value slot: `Arc` = shared heap value (inner nodes,
/// non-paged trees — byte-identical to the old `Arc<V>` via the NonNull
/// niche); `in_block` = the value lives at this entry's own position in
/// the node's `block`. Spec §3 (I-A/I-B).
pub(crate) struct Value<V>(Option<Arc<V>>);

impl<V> Value<V> {
    pub(crate) fn arc(a: Arc<V>) -> Self {
        Value(Some(a))
    }
    /// Production caller: `NodeCodec::decode` (`DataLeaf` only, task 4) and
    /// block-leaf CoW rebuilds.
    pub(crate) fn in_block() -> Self {
        Value(None)
    }
    pub(crate) fn as_arc(&self) -> Option<&Arc<V>> {
        self.0.as_ref()
    }
    pub(crate) fn is_in_block(&self) -> bool {
        self.0.is_none()
    }
    /// Take the slot's `Arc` by value. `None` for an in-block slot, whose
    /// value lives in the node's `block` and has no per-entry `Arc` at all.
    /// Handing the `Arc` out *by value* (rather than cloning it out of the
    /// entry) is what lets `take_value`'s `Arc::try_unwrap` fast path fire
    /// when this node held the last reference.
    pub(crate) fn into_arc(self) -> Option<Arc<V>> {
        self.0
    }
}
impl<V> Clone for Value<V> {
    fn clone(&self) -> Self {
        Value(self.0.clone())
    }
}

/// `BTreeNode::entries` field type. Capacity `MAX_KEYS + 1` — see the
/// `FixedVec` doc comment above for the transient-overflow headroom
/// rationale.
pub(crate) type Entries<K, V> = FixedVec<(K, Value<V>), { MAX_KEYS + 1 }>;
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
    /// Some only on paged data-tree leaves ("block leaves"). `None` for
    /// every other node: inner nodes and index trees stay all-Arc always
    /// (I-B). +8B per node.
    ///
    /// Constructed by `NodeCodec::decode` (task 4, fault-in) and by every
    /// leaf mutation on **both** paths — the immutable one
    /// (`rebuild_block_leaf`, `split_block`, Task 5) and the in-place `_mut`
    /// one (`insert_into_block_leaf_mut`, `remove_from_block_leaf_mut`,
    /// `split_block_mut`, Task 6) — plus the rebalance code they share
    /// (`absorb`, `put_entry_from_arc`, `take_entry_as_arc`). So a block leaf
    /// that is updated, inserted into, deleted from, split, rotated, or
    /// merged comes back out block-shaped rather than silently de-blocking.
    ///
    /// **Nothing de-blocks a leaf any more.** Task 4's `BTreeNode::materialize`
    /// stopgap — which `Child::make_mut` ran before handing out a `&mut` node,
    /// because the `_mut` family could not yet reason about a block — was
    /// removed by Task 6 along with the `make_mut_keep_block` variant that
    /// existed only to skip it. The invariant every `&mut` holder must now
    /// keep is I-A itself: `entries` and `block` change together, in lockstep.
    ///
    /// Values still leave a block one at a time at exactly two boundaries,
    /// both of them "this value is moving into an *inner* node, whose entries
    /// are always Arc-backed (I-B)": a promoted split median, and an entry
    /// lifted into a parent separator by a rotation or by `remove_leftmost*`.
    pub(crate) block: Option<Box<[V]>>,
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
        debug_assert!(
            self.block.is_none(),
            "plain Clone must never see a block leaf (I-B)"
        );
        BTreeNode {
            entries: self.entries.clone(),
            children: self.children.clone(),
            block: None,
        }
    }
}

impl<K, V> BTreeNode<K, V> {
    /// The one legal clone of a block leaf: values duplicated through the
    /// source's `clone_value`. Non-block nodes take the plain-Clone path.
    pub(crate) fn clone_with(&self, src: Option<&dyn NodeSource<K, V>>) -> BTreeNode<K, V>
    where
        K: Clone,
    {
        match &self.block {
            None => self.clone(),
            Some(b) => {
                // I-B says block leaves exist only on a *cloning* source, not
                // merely that this call site passed one — a caller (e.g.
                // `demote_leaves`) may legitimately pass `src: None` on a
                // sourced tree when it knows the node it's CoW-ing can't be a
                // block leaf. Landing here with `None` (or a `Some` whose
                // `clone_value` returns `None`) means that promise was broken
                // for *this* node specifically.
                let src = src.expect("block leaf requires a source to clone values");
                let block: Box<[V]> = b
                    .iter()
                    .map(|v| src.clone_value(v).expect("block leaf requires a source to clone values"))
                    .collect();
                BTreeNode { entries: self.entries.clone(), children: self.children.clone(), block: Some(block) }
            }
        }
    }

    /// The value of entry `i`, from either representation. Panics on an
    /// in-block entry with no block — impossible under I-A.
    pub(crate) fn value_at(&self, i: usize) -> &V {
        match self.entries[i].1.as_arc() {
            Some(a) => a,
            None => &self.block.as_ref().expect("I-A: in-block entry requires a block")[i],
        }
    }

    /// This leaf's real resident byte cost: the fixed node cost plus one
    /// `size_of::<V>()` per entry — honest for a block leaf (values are
    /// inline) and, since it's driven by `entries.len()` rather than
    /// `block`, an equally correct estimate for an all-Arc leaf (the `Arc`
    /// control block plus the pointee is itself roughly `size_of::<V>()`
    /// worth of heap, just allocated separately rather than in one block).
    /// Consumed by `PagedSource::read_node`'s fault-in credit (this task)
    /// and, from Task 8 on, the demote-side debit — see spec §5.
    /// **Documented limit** (spec §5): heap bytes *inside* `V` (e.g. a
    /// `String` field's own buffer) are not counted — exact for the flat
    /// small-row target, an undercount for heap-carrying records.
    pub(crate) fn leaf_bytes(&self) -> usize {
        Child::<K, V>::NODE_BYTES + self.entries.len() * std::mem::size_of::<V>()
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
        median: (K, Value<V>),
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
                block: None,
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
    // Real callers (`Table::attach_paged_source`, index `from_root_page`
    // constructors) are all in `persistence`-gated code; `btree` itself
    // compiles unconditionally, so this would otherwise warn as dead in a
    // `--no-default-features` build (final-review wave, I-4 re-check — see
    // `Child::on_disk`'s note in `child.rs` for the general shape of this
    // gotcha).
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
    ///
    /// **Value-block leaves (task: paged-leaf-value-blocks, Task 7 ruling).**
    /// Every leaf `from_sorted`/`extend_from_sorted` freeze — `freeze_leaf`
    /// below — is built `block: None`, i.e. plain per-entry `Arc<V>`, no
    /// exceptions: the caller already owns each `V` behind an `Arc` (this
    /// takes `(K, Arc<V>)` pairs and moves them in), so there is nothing to
    /// gain from packing a block here, and every real call site (`Store::
    /// bulk_load` via `Table::from_bulk`, and `Table::insert_batch`'s
    /// task51 fast path) runs *before* any page-file source is attached to
    /// the tree being built — `Store::checkpoint_impl_paged` attaches
    /// lazily, at checkpoint time (`registry.rs`'s `attach_paged` closure),
    /// not at registration. A freshly bulk-built tree is therefore all-Arc
    /// end to end and stays that way until the store checkpoints it to
    /// `pages.bin` and something later faults a leaf back in through
    /// `NodeCodec::decode` (the only place a leaf ever becomes block-shaped)
    /// — one demote/fault cycle after the bulk build, never during it. The
    /// zero-`clone_value`-calls claim for the bulk path therefore holds
    /// trivially: there is no block to clone out of, by construction.
    /// (`extend_from_sorted` appending onto an *already-attached, already
    /// block-shaped* tree is the one case that does call `clone_value` —
    /// see `seed_from_spine`/`redistribute_tail` below — but only to *read*
    /// a block-leaf spine/sibling it did not itself build; the fresh nodes
    /// this produces are still `block: None`.)
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
                let new_root = Child::resident_new(
                    Arc::new(BTreeNode { entries, children, block: None }),
                    self.source.as_deref(),
                );
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
                        block: None,
                    })),
                );
                let mut entries = Entries::new();
                entries.push(median);
                let mut children = Children::new();
                children.push(left);
                children.push(right);
                self.root = Child::resident_new(
                    Arc::new(BTreeNode { entries, children, block: None }),
                    src,
                );
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
    // Real callers (`PersistedIndex::write`, `Table::paged_write_tree`) are
    // both in `persistence`-gated code — see `set_source`'s note above.
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
    /// Returns (new tree, leaves demoted, demoted bytes, next cursor / `None`
    /// when done). The bytes figure is `Σ BTreeNode::leaf_bytes()` of every
    /// leaf actually demoted this call — computed from the demoted leaf
    /// itself, symmetric by construction with the fault-in credit
    /// (`PagedSource::read_node`, task 4) and the dirty-bytes credit
    /// (`Child::resident_new`/`make_mut`, task 8) that both already speak
    /// `leaf_bytes()`. Task 8, spec §5.
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
    // Real caller (`Table::paged_demote`, via `Store::demote_pass`) is in
    // `persistence`-gated code — see `set_source`'s note above.
    #[allow(dead_code)]
    pub(crate) fn demote_leaves(&self, cursor: Option<&K>, budget: usize) -> (BTree<K, V>, usize, usize, Option<K>) {
        let src = self.source.as_deref();
        let h = self.height();
        if h == 0 {
            return (self.clone(), 0, 0, None);
        }
        let mut out = self.clone();
        let mut demoted = 0usize;
        let mut demoted_bytes = 0usize;
        let mut left = budget;
        let mut last: Option<K> = None;

        // depth counts down; at depth 1 a node's children are leaves.
        // Returns whether the budget was exhausted (there is more to do).
        #[allow(clippy::too_many_arguments)]
        fn go<K: Ord + Clone, V>(
            slot: &mut Child<K, V>,
            depth: usize,
            src: Option<&dyn NodeSource<K, V>>,
            cursor: Option<&K>,
            left: &mut usize,
            demoted: &mut usize,
            demoted_bytes: &mut usize,
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
                // `None` is safe here specifically because `depth == 1` means
                // `slot` is a leaf's *parent* (an inner node) — the loop below
                // only ever demotes its `children` (the leaves) by page id,
                // never CoWs a leaf itself. `make_mut(None)` must never reach
                // a block leaf (see I-B / `BTreeNode::clone_with`).
                let n = slot.make_mut(None);
                for c in n.children.iter_mut() {
                    if let (true, Some(id)) = (c.is_loaded(), c.page_id()) {
                        if c.take_accessed() {
                            // second chance: bit cleared, stays resident
                        } else {
                            // Bytes before the slot is overwritten (task 8):
                            // `c` is already loaded, so this is a plain peek
                            // (`load_quiet`, no fault, no accessed bump) at
                            // the exact leaf about to be dropped.
                            *demoted_bytes += c.load_quiet(src).leaf_bytes();
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
            // `None` is safe here for the same reason as the depth == 1 arm
            // above: `depth > 1` means `slot` is an internal node, never a
            // leaf, so this CoW can never reach a block leaf.
            let n = slot.make_mut(None);
            for c in n.children.iter_mut() {
                if go(c, depth - 1, src, cursor, left, demoted, demoted_bytes, last) {
                    return true;
                }
            }
            false
        }
        let exhausted = go(&mut out.root, h, src, cursor, &mut left, &mut demoted, &mut demoted_bytes, &mut last);
        restore_unchanged_ids(&out.root, &self.root, h, src);
        (out, demoted, demoted_bytes, if exhausted { last } else { None })
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
    // Real caller (`Table::paged_changed_pages`, via `Store::checkpoint_impl_paged`)
    // is in `persistence`-gated code — see `set_source`'s note above.
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
    // Real callers (`Table::from_paged_entry`, index `from_root_page`
    // constructors) are all in `persistence`-gated code — see
    // `set_source`'s note above.
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
    ///
    /// Production caller: [`Self::changed_page_ids`] (its own doc explains
    /// why it needs both trees' inner levels resident before it walks).
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

    /// Fault in every node — inner levels *and* leaves — leaving nothing on
    /// disk. Unlike [`Self::load_inner_levels`] (which the data tree uses:
    /// a leaf there is meant to stay on disk until a read actually touches
    /// it), this is for a persisted *index* tree being attached after
    /// recovery (`Table::define_persisted_index`'s attach branch, Task 13):
    /// an index tree is never demoted (see `Residency`'s doc — demotion is
    /// a data-tree-only concept), so leaving any of it on disk after attach
    /// would just re-fault the same pages on the very first query with
    /// nothing ever gained back. Uses [`Child::load_quiet`] throughout —
    /// same reasoning as `changed_page_ids`'s note: this is startup
    /// bookkeeping, not a workload read, and an index tree's accessed bits
    /// never matter anyway (it is never demoted), but staying `load_quiet`
    /// keeps this consistent with every other non-workload walk in this
    /// file.
    // Called from `index.rs`'s `paged_reachable_ids` methods (which
    // re-assert the same full-residency invariant on every call, since
    // `for_each_page_id` depends on it structurally — see that method's
    // doc: a post-recovery path, where a corrupt page has already been
    // caught by the EAGER `try_load_all` at attach time), plus this
    // module's own test below. All persistence-feature call sites, so this
    // is dead code under a build without that feature, same as
    // `load_inner_levels` above.
    #[allow(dead_code)]
    pub(crate) fn load_all(&self) {
        let src = self.source.as_deref();
        let h = self.height();
        fn go<K, V>(slot: &Child<K, V>, depth: usize, src: Option<&dyn NodeSource<K, V>>) {
            let n = slot.load_quiet(src);
            if depth == 0 {
                return; // leaf slot: loaded above, nothing further to descend into
            }
            for c in n.children.iter() {
                go(c, depth - 1, src);
            }
        }
        go(&self.root, h, src)
    }

    /// Like [`Self::load_inner_levels`], but reports a corrupt/unreadable
    /// page as `Err` instead of panicking (via [`Child::try_load`]) — the
    /// EAGER load `recover()` performs while rebuilding a paged table's data
    /// tree (`Table::from_paged_entry`) must not crash the process on a
    /// truncated or bit-flipped inner page; a spec-conformance requirement
    /// (I-1), unlike `load_inner_levels`'s own callers, which all run
    /// *after* a paged store has already recovered successfully and so can
    /// keep the panicking behavior. Same walk, same stopping rule (leaves
    /// stay on disk) — only the fault-in call and its error path differ.
    // Real caller (`Table::from_paged_entry`) is in `persistence`-gated
    // code — see `set_source`'s note above.
    #[allow(dead_code)]
    pub(crate) fn try_load_inner_levels(&self) -> crate::Result<()> {
        let src = self.source.as_deref();
        let h = self.height();
        fn go<K, V>(slot: &Child<K, V>, depth: usize, src: Option<&dyn NodeSource<K, V>>) -> crate::Result<()> {
            if depth == 0 {
                return Ok(()); // leaf slot: leave on disk
            }
            let n = slot.try_load(src)?;
            for c in n.children.iter() {
                go(c, depth - 1, src)?;
            }
            Ok(())
        }
        go(&self.root, h, src)
    }

    /// Like [`Self::load_all`], but reports a corrupt/unreadable page as
    /// `Err` instead of panicking (via [`Child::try_load`]) — used at index
    /// attach time (`UniqueStorage`/`NonUniqueStorage::from_root_page`, the
    /// EAGER load Task 13's attach path performs), so `define_persisted_index`
    /// can return `Err` on a corrupt index page instead of crashing the
    /// process (I-1). `load_all`'s own other callers (`paged_reachable_ids`)
    /// run post-recovery, once this invariant is already known to hold, so
    /// they keep the panicking version.
    // Real callers (`UniqueStorage`/`NonUniqueStorage::from_root_page`) are
    // both in `persistence`-gated code — see `set_source`'s note above.
    #[allow(dead_code)]
    pub(crate) fn try_load_all(&self) -> crate::Result<()> {
        let src = self.source.as_deref();
        let h = self.height();
        fn go<K, V>(slot: &Child<K, V>, depth: usize, src: Option<&dyn NodeSource<K, V>>) -> crate::Result<()> {
            let n = slot.try_load(src)?;
            if depth == 0 {
                return Ok(()); // leaf slot: loaded above, nothing further to descend into
            }
            for c in n.children.iter() {
                go(c, depth - 1, src)?;
            }
            Ok(())
        }
        go(&self.root, h, src)
    }

    /// Bytes of resident leaves, summed as `BTreeNode::leaf_bytes()` over
    /// every loaded leaf slot — the same unit the fault-in credit
    /// (`PagedSource::read_node`, task 4), the demote-side debit
    /// ([`Self::demote_leaves`], task 8), and the dirty-bytes credit
    /// (`Child::resident_new`/`make_mut`, task 8) all speak, so this walk's
    /// total lines up exactly with what those bump/subtract at runtime. The
    /// checkpoint-end F1 reconciliation (`Store::checkpoint_impl_paged`)
    /// re-bases the live `resident_leaf_bytes` counter from this walk every
    /// checkpoint. Read-only: uses [`Child::load_quiet`], so measuring
    /// residency never marks anything "recently used" — see the same note
    /// on [`Self::changed_page_ids`].
    // No longer a production caller as of Task 9 (review I-3): the F1
    // reconciliation walk now goes through `Self::resident_leaf_bytes_dedup`
    // (`Table::paged_resident_leaf_bytes_dedup`). Kept for its direct
    // unit-test callers in this file (a plain, non-deduped resident-bytes
    // walk is occasionally the simpler thing to assert against) — see
    // `#[allow(dead_code)]` below.
    #[allow(dead_code)]
    pub(crate) fn resident_leaf_estimate(&self) -> usize {
        let src = self.source.as_deref();
        let h = self.height();
        fn go<K, V>(slot: &Child<K, V>, depth: usize, src: Option<&dyn NodeSource<K, V>>) -> usize {
            if depth == 0 {
                return if slot.is_loaded() { slot.load_quiet(src).leaf_bytes() } else { 0 };
            }
            let n = slot.load_quiet(src);
            n.children.iter().map(|c| go(c, depth - 1, src)).sum()
        }
        go(&self.root, h, src)
    }

    /// Like [`Self::resident_leaf_estimate`], but deduped against `seen` — a
    /// slot already counted (by another tree walked earlier against the
    /// same `seen` set) contributes `0` the second time, and — Task 9
    /// review I-2 — an already-`seen` INNER slot prunes its whole subtree
    /// rather than re-descending it: every leaf beneath a shared inner node
    /// was necessarily counted (or skipped) already by whichever earlier
    /// walk first reached that same inner node, since the whole subtree
    /// beneath a shared `Child` is shared too (same argument
    /// [`Self::diff`]'s changed-page walk relies on). This turns an older
    /// snapshot's walk from O(its whole tree) into O(its divergent
    /// subtrees) — most of a CoW-sharing snapshot's walk is one shared
    /// pointer at the root, pruned immediately.
    ///
    /// Task 9's pin-aware reconciliation: retained snapshots CoW-share
    /// pre-demotion leaves with the latest snapshot (and with each other),
    /// so walking each snapshot's data tree independently and summing
    /// would double-count every shared leaf. The dedup key is a node's
    /// `Child` pointer ([`Child::resident_ptr`]) — the same ptr-identity
    /// trick [`Child::same_node`] and [`Self::diff`]'s changed-page walk
    /// use — read WITHOUT faulting and WITHOUT touching the accessed bit,
    /// so an unloaded (on-disk) slot contributes `0` and is never faulted
    /// in by this walk, exactly like [`Self::resident_leaf_estimate`] (an
    /// inner slot found on-disk is defensive-only: today's architecture
    /// keeps every data-tree inner level always resident, so this arm is
    /// never reached in practice, but the walk must not fault even if that
    /// ever changed). Caller decides `seen`'s lifetime and walk order;
    /// walking newest-snapshot first and reusing one `seen` set across all
    /// retained snapshots is what makes "resident" (latest's own total) and
    /// "pinned" (everything else newly counted after that) fall out of the
    /// same walk — see `Store::checkpoint_impl_paged`'s F1 reconciliation.
    // Production caller: the F1 reconciliation walk in
    // `Store::checkpoint_impl_paged` (via
    // `Table::paged_resident_leaf_bytes_dedup`). Also exercised directly by
    // this task's tests, including the visited-slot-count variant just
    // below (test-only: proves the pruning above actually happens).
    #[allow(dead_code)]
    pub(crate) fn resident_leaf_bytes_dedup(&self, seen: &mut std::collections::HashSet<*const ()>) -> usize {
        let mut visited = 0usize;
        Self::resident_leaf_bytes_dedup_go(&self.root, self.height(), self.source.as_deref(), seen, &mut visited)
    }

    /// Test-only twin of [`Self::resident_leaf_bytes_dedup`] that also
    /// reports how many slots the walk actually visited — the review-I-2
    /// regression guard: on a re-walk of an already-fully-`seen` tree, a
    /// pruning walk visits O(1) slots (just the root, immediately pruned)
    /// while a non-pruning one would still visit every slot.
    #[cfg(test)]
    pub(crate) fn resident_leaf_bytes_dedup_with_visits(
        &self,
        seen: &mut std::collections::HashSet<*const ()>,
    ) -> (usize, usize) {
        let mut visited = 0usize;
        let bytes =
            Self::resident_leaf_bytes_dedup_go(&self.root, self.height(), self.source.as_deref(), seen, &mut visited);
        (bytes, visited)
    }

    /// Shared recursive body for [`Self::resident_leaf_bytes_dedup`] and
    /// [`Self::resident_leaf_bytes_dedup_with_visits`] — one implementation
    /// so the test-only visit count can never drift from what production
    /// actually walks.
    fn resident_leaf_bytes_dedup_go(
        slot: &Child<K, V>,
        depth: usize,
        src: Option<&dyn NodeSource<K, V>>,
        seen: &mut std::collections::HashSet<*const ()>,
        visited: &mut usize,
    ) -> usize {
        *visited += 1;
        if depth == 0 {
            return match slot.resident_ptr() {
                Some(ptr) if seen.insert(ptr) => slot.load_quiet(src).leaf_bytes(),
                _ => 0, // not resident, or already counted from another snapshot's walk
            };
        }
        match slot.resident_ptr() {
            // Not resident: nothing beneath an on-disk inner slot can be
            // resident either (see the doc above) — 0, no fault, no descend.
            None => 0,
            // Already seen: the whole subtree below this inner node was
            // already walked (or pruned) once elsewhere — skip it, the
            // I-2 pruning step.
            Some(p) if !seen.insert(p) => 0,
            Some(_) => {
                let n = slot.load_quiet(src);
                n.children
                    .iter()
                    .map(|c| Self::resident_leaf_bytes_dedup_go(c, depth - 1, src, seen, visited))
                    .sum()
            }
        }
    }

    /// Report every slot's own page id, in document order — a slot's id is
    /// known without a fault (`Child::page_id()`), so this reports it
    /// whether or not the slot is loaded. It only *descends* into an inner
    /// slot's children when that slot is already resident: an unfaulted
    /// inner node's children are simply invisible to this walk (nothing
    /// here ever faults anything in). Correct and complete only when the
    /// tree is already fully resident — load-bearing for
    /// `IndexMaintainer::paged_reachable_ids` (Task 13), which calls
    /// [`Self::load_all`] immediately before this for exactly that reason
    /// (see that call site's note). Read-only otherwise: uses
    /// [`Child::load_quiet`], so a walk never marks a leaf "recently used"
    /// just by visiting it — see the same note on [`Self::changed_page_ids`].
    // Production caller: `IndexMaintainer::paged_reachable_ids` (Task 13),
    // always immediately after `load_all` -- both are `persistence`-gated,
    // so this is still dead code under a build without that feature (same
    // situation as `load_all` itself). Also exercised directly by this
    // file's punch-bookkeeping tests.
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
            let val = node.value_at(entry_idx); // &'a V

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
            let val = node.value_at(actual_idx);

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
        Ok(pos) => Some(node.value_at(pos)),
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
        Ok(pos) => match node.entries[pos].1.as_arc() {
            Some(a) => Some(a.clone()),
            // In-block entry: no per-entry `Arc` exists to hand back, so
            // build a fresh one by cloning the value out via the source
            // (`clone_value`, guaranteed `Some` for any tree that could
            // hold a block leaf — I-B). `Table::delete`/`update`'s
            // existence probe (`merged_get_arc` -> `get_arc`) routes
            // through here, so this must return the real value, not
            // silently report "not found" the way the pre-block-decode
            // `Task 5 wires it` stub used to (task 4: decode now legitimately
            // builds block leaves, so this path is live). `src`/`clone_value`
            // being absent here would itself be an I-B breach (a block leaf
            // with no cloning source) — every other I-B breach in this file
            // panics loudly (`value_at`, `clone_with`, `absorb`,
            // `seed_from_spine`), so this asserts too (fix round 1, Important
            // 1) rather than quietly degrading into the exact spurious
            // `KeyNotFound` this branch exists to fix.
            None => Some(Arc::new(
                src.and_then(|s| s.clone_value(node.value_at(pos)))
                    .expect("I-B: block leaf requires a cloning source"),
            )),
        },
        Err(pos) => {
            if node.children.is_empty() {
                None
            } else {
                get_arc_in_node(node.children[pos].load(src), key, src)
            }
        }
    }
}

/// One edit applied to a block leaf by [`rebuild_block_leaf`].
///
/// Spec §4: a block-leaf mutation "builds the new block in one allocation
/// pass — never clone-then-mutate". A `Box<[V]>` can neither grow, shrink,
/// nor have an element replaced while the node is only held by shared
/// reference, which is all the immutable path ever has, so every immutable
/// block-leaf mutation is expressed as one of these three edits and
/// streamed into a fresh node in a single pass.
pub(crate) enum LeafEdit<K, V> {
    /// Overwrite `entries[i]` with a new key and value. The key is carried
    /// (rather than reused from the node) so the block path stores the
    /// *incoming* key exactly like the all-Arc path's
    /// `entries[pos] = (key, ..)` does — the two are equivalent for every
    /// `K` whose `Ord` agrees with equality, which is all of them here, but
    /// the oracle is supposed to prove these paths identical and an
    /// unpinned divergence between them is not worth keeping. It also saves
    /// a `K::clone` (review round 1, Warning 4 / Minor 3).
    Replace(usize, K, V),
    /// Insert `(key, value)` at index `i`, shifting `[i, len)` right.
    Insert(usize, K, V),
    /// Drop `entries[i]`, shifting `(i, len)` left.
    Remove(usize),
}

/// Turn an incoming `Arc<V>` into an owned `V` bound for a block slot.
///
/// The common insert/update carries a freshly allocated `Arc` nobody else
/// holds, so `try_unwrap` moves the value straight into the block with no
/// copy at all; a genuinely shared `Arc` (a commit-merge `upsert_arc`, an
/// overlay flush replaying a value an older snapshot still references)
/// falls back to the source's `clone_value`. A missing `src` (or one whose
/// `clone_value` returns `None`) here is an I-B breach — a block leaf on a
/// non-cloning source — and panics, the same way every other I-B check in
/// this file does.
fn take_value<K, V>(val: Arc<V>, src: Option<&dyn NodeSource<K, V>>) -> V {
    match Arc::try_unwrap(val) {
        Ok(v) => v,
        Err(a) => src
            .and_then(|s| s.clone_value(&a))
            .expect("I-B: block leaf requires a cloning source"),
    }
}

/// Build a fresh block leaf from `node` with `edit` applied.
///
/// One pass, one allocation for the new block: values the edit does not
/// touch are cloned straight from the old block into their final slot via
/// `NodeSource::clone_value`, and the edit's own value moves in. The result
/// is always block-shaped (`block: Some`, every entry `in_block`), so an
/// immutable-path mutation of a block leaf yields a block leaf rather than
/// silently de-blocking it — that is the whole point of Task 5.
///
/// `Insert` may return `MAX_KEYS + 1` entries (the `Entries` capacity
/// headroom); the caller hands such a node to [`split_block`].
fn rebuild_block_leaf<K: Clone, V>(
    node: &BTreeNode<K, V>,
    edit: LeafEdit<K, V>,
    src: Option<&dyn NodeSource<K, V>>,
) -> BTreeNode<K, V> {
    debug_assert!(node.children.is_empty(), "I-A: only leaves carry a value block");
    let old = node.block.as_ref().expect("rebuild_block_leaf: not a block leaf");
    debug_assert_eq!(
        old.len(),
        node.entries.len(),
        "I-A: block length must match entries length"
    );
    let s = src.expect("I-B: block leaf requires a cloning source");
    let n = node.entries.len();
    let mut entries: Entries<K, V> = Entries::new();
    let mut block: Vec<V> = Vec::with_capacity(match &edit {
        LeafEdit::Insert(..) => n + 1,
        LeafEdit::Remove(_) => n - 1,
        LeafEdit::Replace(..) => n,
    });
    // Carry entry `i` across untouched: its key is cloned and its value is
    // duplicated out of the old block directly into the new one.
    let carry = |entries: &mut Entries<K, V>, block: &mut Vec<V>, i: usize| {
        entries.push((node.entries[i].0.clone(), Value::in_block()));
        block.push(
            s.clone_value(&old[i])
                .expect("I-B: block leaf requires a cloning source"),
        );
    };
    match edit {
        LeafEdit::Replace(pos, key, v) => {
            debug_assert!(pos < n, "Replace out of range");
            let mut kv = Some((key, v));
            for i in 0..n {
                if i == pos {
                    let (key, v) = kv.take().expect("Replace visits pos exactly once");
                    entries.push((key, Value::in_block()));
                    block.push(v);
                } else {
                    carry(&mut entries, &mut block, i);
                }
            }
        }
        LeafEdit::Insert(pos, key, v) => {
            debug_assert!(pos <= n, "Insert out of range");
            for i in 0..pos {
                carry(&mut entries, &mut block, i);
            }
            entries.push((key, Value::in_block()));
            block.push(v);
            for i in pos..n {
                carry(&mut entries, &mut block, i);
            }
        }
        LeafEdit::Remove(pos) => {
            debug_assert!(pos < n, "Remove out of range");
            for i in 0..n {
                if i != pos {
                    carry(&mut entries, &mut block, i);
                }
            }
        }
    }
    BTreeNode {
        entries,
        children: Children::new(),
        block: Some(block.into_boxed_slice()),
    }
}

/// Split an over-full block leaf (`MAX_KEYS + 1` entries, straight out of
/// [`rebuild_block_leaf`]) into two block leaves plus the promoted median.
///
/// Takes the node **by value**: it was just built by this thread and is not
/// shared, so each half's block is filled by *moving* values out of the
/// original — one fresh `Box<[V]>` per half, zero `clone_value` calls. The
/// median's value cannot stay in a block: it becomes a separator entry in
/// an inner node, and inner nodes are always Arc-backed (I-B), so it is
/// re-homed into a fresh `Arc` at the boundary.
///
/// The split point (`mid = len / 2`, median promoted, `[..mid]` left,
/// `[mid+1..]` right) is exactly `maybe_split`'s, so block and all-Arc
/// leaves produce identically-shaped trees.
#[allow(clippy::type_complexity)]
fn split_block<K: Clone, V>(node: BTreeNode<K, V>) -> (BTreeNode<K, V>, (K, Value<V>), BTreeNode<K, V>) {
    let BTreeNode {
        mut entries,
        children,
        block,
    } = node;
    debug_assert!(children.is_empty(), "I-A: only leaves carry a value block");
    let block = block.expect("split_block: not a block leaf");
    debug_assert_eq!(
        block.len(),
        entries.len(),
        "I-A: block length must match entries length"
    );
    let mid = entries.len() / 2;
    let right_entries = entries.split_off(mid + 1);
    // Drops an `in_block` marker slot (no `Arc`, nothing to release); the
    // real value comes out of the block below.
    let median_key = entries.pop().expect("entries[mid] exists").0;
    // `entries` is now `[..mid]`; the block still runs in entry order, so
    // draining it front-to-back lines the halves up with their keys.
    let mut vals = Vec::from(block).into_iter();
    let left_block: Vec<V> = vals.by_ref().take(entries.len()).collect();
    let median_val = vals.next().expect("I-A: block length must match entries length");
    let right_block: Vec<V> = vals.collect();
    debug_assert_eq!(left_block.len(), entries.len(), "I-A");
    debug_assert_eq!(right_block.len(), right_entries.len(), "I-A");
    (
        BTreeNode {
            entries,
            children: Children::new(),
            block: Some(left_block.into_boxed_slice()),
        },
        (median_key, Value::arc(Arc::new(median_val))),
        BTreeNode {
            entries: right_entries,
            children: Children::new(),
            block: Some(right_block.into_boxed_slice()),
        },
    )
}

/// Immutable-path insert into a **block leaf**: one `rebuild_block_leaf`
/// pass, then `split_block` if that overflowed. The block-shaped
/// counterpart of `insert_into_node`'s leaf arms + `maybe_split`.
fn insert_into_block_leaf<K: Ord + Clone, V>(
    node: &BTreeNode<K, V>,
    key: K,
    val: Arc<V>,
    src: Option<&dyn NodeSource<K, V>>,
) -> InsertResult<K, V> {
    debug_assert!(node.children.is_empty(), "I-A: only leaves carry a value block");
    match node.entries.binary_search_by(|(k, _)| k.cmp(&key)) {
        Ok(pos) => {
            // Replace stores the *incoming* key, matching `insert_into_node`'s
            // all-Arc arm exactly (review round 1, Warning 4).
            let n = rebuild_block_leaf(node, LeafEdit::Replace(pos, key, take_value(val, src)), src);
            InsertResult::Fit(Child::resident_new(Arc::new(n), src), true)
        }
        Err(pos) => {
            let n = rebuild_block_leaf(node, LeafEdit::Insert(pos, key, take_value(val, src)), src);
            if n.entries.len() <= MAX_KEYS {
                InsertResult::Fit(Child::resident_new(Arc::new(n), src), false)
            } else {
                // The overflowing rebuild deliberately materialises one
                // `MAX_KEYS + 1`-slot block that `split_block` immediately
                // drains into two halves *by move* (review round 1, Minor 2).
                // The obvious "optimisation" — deciding the split up front and
                // filling two blocks directly — would have to read the source
                // node's values twice or buffer them, i.e. clone-then-mutate,
                // which is exactly what spec §4 forbids. One extra allocation,
                // zero extra `clone_value` calls; leave it alone.
                let (left, median, right) = split_block(n);
                InsertResult::Split {
                    left: Child::resident_new(Arc::new(left), src),
                    median,
                    right: Child::resident_new(Arc::new(right), src),
                    replaced: false,
                }
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
    if node.block.is_some() {
        // Block leaf (always a leaf — I-A): rebuild the block in one pass
        // instead of shifting `entries` out from under it. Before Task 5
        // this fell through to the all-Arc code below, which cloned the
        // `in_block` marker slots into a `block: None` node and left every
        // value dangling for `value_at` to panic on.
        return insert_into_block_leaf(node, key, val, src);
    }
    let mut entries = node.entries.clone();

    match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
        Ok(pos) => {
            // Replace existing value.
            entries[pos] = (key, Value::arc(val));
            let children = node.children.clone();
            InsertResult::Fit(
                Child::resident_new(Arc::new(BTreeNode { entries, children, block: None }), src),
                true,
            )
        }
        Err(pos) => {
            if node.children.is_empty() {
                // Leaf: insert and possibly split.
                entries.insert(pos, (key, Value::arc(val)));
                maybe_split(entries, Children::new(), false, src)
            } else {
                // Internal: recurse into child[pos], then merge the result.
                let mut children = node.children.clone();
                match insert_into_node(&children[pos], key, val, src) {
                    InsertResult::Fit(new_child, replaced) => {
                        children[pos] = new_child;
                        InsertResult::Fit(
                            Child::resident_new(
                                Arc::new(BTreeNode { entries, children, block: None }),
                                src,
                            ),
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
        InsertResult::Fit(
            Child::resident_new(Arc::new(BTreeNode { entries, children, block: None }), src),
            replaced,
        )
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
                    block: None,
                }),
                src,
            ),
            median,
            right: Child::resident_new(
                Arc::new(BTreeNode {
                    entries: right_entries,
                    children: right_children,
                    block: None,
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
                    // Unlike `seed_from_spine`/`redistribute_tail` (fix round
                    // 1, Critical 2), this `expect` is genuinely unreachable
                    // in production, not merely untested (fix round 1, Minor
                    // 2): `diff` is only ever called from
                    // `registry.rs::diff_table`, the row-format incremental-
                    // checkpoint delta path, and `StoreConfig::checkpoint_chain_max`
                    // is documented inert in paged mode (a paged root is
                    // always self-contained, never part of a delta chain —
                    // see `Store::checkpoint_impl`) — so a block leaf never
                    // reaches this iterator. Left as `expect` (not fixed to
                    // clone via `clone_value`) since there is no live paged
                    // call site to regression-test against.
                    //
                    // Re-confirmed at Task 7 (boundaries/bulk/MultiWriter):
                    // `diff_table` (the only production caller of `BTree::
                    // diff`) is invoked from exactly one call site,
                    // `serialize_delta` in `checkpoint.rs`, which is in turn
                    // reachable only from `write_delta_checkpoint`, called
                    // only inside `Store::checkpoint_impl` — the row-format
                    // path `checkpoint_impl` itself early-returns out of
                    // (`if inner.paged.is_some() { return self.
                    // checkpoint_impl_paged(); }`) before ever reaching
                    // `write_delta_checkpoint`. Both Task 5 (immutable) and
                    // Task 6 (in-place) mutation paths now produce block
                    // leaves, but neither changes which checkpoint path runs
                    // on a paged store, so the claim is unchanged: no
                    // production call graph can hand a block leaf to this
                    // cursor.
                    return Some((k, v.as_arc().expect("diff over block leaf not yet supported (I-A)")));
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
                // See the leaf-arm comment above: unreachable in production
                // (paged mode never calls `diff` — `checkpoint_chain_max` is
                // inert there), not merely untested.
                return Some((k, v.as_arc().expect("diff over block leaf not yet supported (I-A)")));
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

// ---------------------------------------------------------------------------
// Block-leaf edits on the IN-PLACE (`_mut`) path — Task 6, spec §4.
//
// The immutable path's `rebuild_block_leaf`/`split_block` build a new node
// out of a `&` borrow, so every surviving value has to be duplicated through
// `NodeSource::clone_value`. The in-place path is handed a `&mut` node that
// `Child::make_mut` has already made uniquely owned, so the survivors are
// simply **moved** into the new block: the same one-pass, one-allocation
// discipline spec §4 requires, at **zero** `clone_value` calls.
//
// Each of these vacates `n`'s `entries` and `block` into locals up front and
// commits both back with infallible assignments at the end. `n` is already
// installed in its parent's `children` — i.e. reachable — so an unwind out
// of the middle (a `FixedVec` capacity assert) must never leave it holding
// `in_block` entries with no block behind them; between the two assignments
// `n` is a well-formed *empty* leaf instead. Same discipline as
// `put_entry_from_arc` (Task 5, review round 1 Important 2).
//
// There is deliberately no in-place counterpart to `LeafEdit::Replace`: an
// in-place update of an existing key is a single `block[pos] = v` store, not
// a rebuild, and lives inline in `insert_into_node_mut`.
// ---------------------------------------------------------------------------

/// Insert `(key, v)` at `pos` of a uniquely-owned **block leaf**, moving the
/// existing values into one fresh, exactly-sized block (`Box<[V]>` cannot
/// grow in place).
///
/// May leave `MAX_KEYS + 1` entries — the `Entries` capacity headroom; the
/// caller hands such a node straight to [`maybe_split_mut`].
fn insert_into_block_leaf_mut<K, V>(n: &mut BTreeNode<K, V>, pos: usize, key: K, v: V) {
    debug_assert!(n.children.is_empty(), "I-A: only leaves carry a value block");
    let old = Vec::from(n.block.take().expect("insert_into_block_leaf_mut: not a block leaf"));
    let mut entries = std::mem::take(&mut n.entries); // `n` is now `{[], None}`: I-A holds
    debug_assert_eq!(old.len(), entries.len(), "I-A: block length must match entries length");
    debug_assert!(pos <= old.len(), "Insert out of range");
    let mut block: Vec<V> = Vec::with_capacity(old.len() + 1);
    let mut it = old.into_iter();
    block.extend(it.by_ref().take(pos));
    block.push(v);
    block.extend(it);
    entries.insert(pos, (key, Value::in_block()));
    n.entries = entries;
    n.block = Some(block.into_boxed_slice());
}

/// Remove entry `pos` from a uniquely-owned **block leaf**: its value is
/// dropped (never cloned — it simply is not carried) and the survivors move
/// into one fresh, exactly-sized block.
fn remove_from_block_leaf_mut<K, V>(n: &mut BTreeNode<K, V>, pos: usize) {
    debug_assert!(n.children.is_empty(), "I-A: only leaves carry a value block");
    let old = Vec::from(n.block.take().expect("remove_from_block_leaf_mut: not a block leaf"));
    let mut entries = std::mem::take(&mut n.entries); // `n` is now `{[], None}`: I-A holds
    debug_assert_eq!(old.len(), entries.len(), "I-A: block length must match entries length");
    debug_assert!(pos < old.len(), "Remove out of range");
    let mut block: Vec<V> = Vec::with_capacity(old.len() - 1);
    for (j, v) in old.into_iter().enumerate() {
        if j != pos {
            block.push(v);
        }
    }
    entries.remove(pos);
    n.entries = entries;
    n.block = Some(block.into_boxed_slice());
}

/// In-place counterpart to [`split_block`]: split an over-full **block
/// leaf** (`MAX_KEYS + 1` entries, straight out of
/// [`insert_into_block_leaf_mut`]) into two block leaves plus the promoted
/// median. `n` becomes the left half; the right half is returned.
///
/// Values are moved, never cloned, and the split point (`mid = len / 2`,
/// median promoted, `[..mid]` left, `[mid+1..]` right) is exactly
/// `maybe_split`/`split_block`/`maybe_split_mut`'s, so every path produces
/// identically-shaped trees. The median's value cannot stay in a block — it
/// becomes a separator in an inner node, and those are always Arc-backed
/// (I-B) — so it is re-homed into a fresh `Arc` at the boundary.
#[allow(clippy::type_complexity)]
fn split_block_mut<K, V>(n: &mut BTreeNode<K, V>) -> ((K, Value<V>), Arc<BTreeNode<K, V>>) {
    debug_assert!(n.children.is_empty(), "I-A: only leaves carry a value block");
    let old = Vec::from(n.block.take().expect("split_block_mut: not a block leaf"));
    let mut entries = std::mem::take(&mut n.entries); // `n` is now `{[], None}`: I-A holds
    debug_assert_eq!(old.len(), entries.len(), "I-A: block length must match entries length");
    let mid = entries.len() / 2;
    let right_entries = entries.split_off(mid + 1);
    // Drops an `in_block` marker slot (no `Arc`, nothing to release); the
    // real value comes out of the block below.
    let median_key = entries.pop().expect("entries[mid] exists").0;
    let mut vals = old.into_iter();
    let left_block: Vec<V> = vals.by_ref().take(entries.len()).collect();
    let median_val = vals.next().expect("I-A: block length must match entries length");
    let right_block: Vec<V> = vals.collect();
    // BOTH halves, matching `split_block` (review round 1, Minor 3): the
    // left-half assert is what catches a `take(entries.len())` that ran on
    // the wrong side of the median `pop`.
    debug_assert_eq!(left_block.len(), entries.len(), "I-A");
    debug_assert_eq!(right_block.len(), right_entries.len(), "I-A");
    let right = Arc::new(BTreeNode {
        entries: right_entries,
        children: Children::new(),
        block: Some(right_block.into_boxed_slice()),
    });
    n.entries = entries;
    n.block = Some(left_block.into_boxed_slice());
    ((median_key, Value::arc(Arc::new(median_val))), right)
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
        median: (K, Value<V>),
        right: Child<K, V>,
        replaced: bool,
    },
}

/// In-place counterpart to `insert_into_node`. Descends through
/// `Child::make_mut`, so each node is cloned only if it is still shared with
/// another snapshot (copy-on-write preserved) and otherwise mutated directly.
///
/// Block-aware since Task 6: a leaf whose values live in a `block` is edited
/// through the block (an update is a single slot store; an insert rebuilds by
/// move, because `Box<[V]>` cannot grow in place) and stays block-shaped.
/// Before Task 6 `Child::make_mut` ran the `BTreeNode::materialize` stopgap
/// and handed this function an all-Arc leaf instead, silently de-blocking
/// every leaf a `Table` write touched.
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
            if let Some(block) = n.block.as_mut() {
                // THE O(1) HOT PATH (spec §4). `make_mut` above already made
                // this leaf uniquely owned, so the incoming value moves
                // straight into its slot — no rebuild, no reallocation, and
                // no `clone_value` at all (contrast the immutable path's
                // n-1, which is inherent to editing through a `&` borrow).
                // The displaced value drops in place.
                //
                // `take_value` runs FIRST because it is the one fallible
                // step (an I-B breach panics): until it returns, `n` is
                // completely untouched. The key is stored after the value,
                // once `block`'s borrow of `n` has ended — the incoming key,
                // matching the all-Arc arm below and
                // `insert_into_block_leaf`'s `Replace` (Task 5 review,
                // warning 4).
                let v = take_value(val, src);
                block[pos] = v;
                n.entries[pos].0 = key;
            } else {
                n.entries[pos] = (key, Value::arc(val));
            }
            InsertOutcome::Fit { replaced: true }
        }
        Err(pos) => {
            if n.children.is_empty() {
                // Leaf: insert and possibly split.
                if n.block.is_some() {
                    // A block leaf's length changes, so this rebuilds — by
                    // move, not by clone (the node is uniquely owned).
                    insert_into_block_leaf_mut(n, pos, key, take_value(val, src));
                } else {
                    n.entries.insert(pos, (key, Value::arc(val)));
                }
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
/// inline storage — only the right sibling's `Arc::new` allocates. A block
/// leaf hands off to [`split_block_mut`], which splits the value block the
/// same way and keeps both halves block-shaped (Task 6).
#[allow(clippy::type_complexity)]
fn maybe_split_mut<K: Clone, V>(
    n: &mut BTreeNode<K, V>,
) -> Option<((K, Value<V>), Arc<BTreeNode<K, V>>)> {
    if n.entries.len() <= MAX_KEYS {
        return None;
    }
    if n.block.is_some() {
        // Block leaf (always a leaf — I-A).
        return Some(split_block_mut(n));
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
        block: None,
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
                // A block leaf rebuilds its block in one pass and stays
                // block-shaped; an all-Arc leaf shifts its entries as before.
                let new = if node.block.is_some() {
                    rebuild_block_leaf(node, LeafEdit::Remove(i), src)
                } else {
                    let mut entries = node.entries.clone();
                    entries.remove(i);
                    BTreeNode {
                        entries,
                        children: Children::new(),
                        block: None,
                    }
                };
                let underfull = new.entries.len() < MIN_KEYS;
                DeleteResult::Removed {
                    node: Child::resident_new(Arc::new(new), src),
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
                    node: Child::resident_new(
                        Arc::new(BTreeNode { entries, children, block: None }),
                        src,
                    ),
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
                            node: Child::resident_new(
                                Arc::new(BTreeNode { entries, children, block: None }),
                                src,
                            ),
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
) -> ((K, Value<V>), Child<K, V>, bool) {
    let node = node.load(src);
    if node.children.is_empty() {
        if node.block.is_some() {
            // The removed entry is promoted into the *parent* (an inner
            // node), whose entries are always Arc-backed (I-B) — so the
            // value has to leave the block entirely, not just move slots.
            // Before Task 5 this handed the parent a bare `in_block` marker
            // with no block behind it, an I-A violation that surfaced as a
            // `value_at` panic on the next read of that separator.
            let key = node.entries[0].0.clone();
            let val = src
                .and_then(|s| s.clone_value(node.value_at(0)))
                .expect("I-B: block leaf requires a cloning source");
            let new = rebuild_block_leaf(node, LeafEdit::Remove(0), src);
            let underfull = new.entries.len() < MIN_KEYS;
            return (
                (key, Value::arc(Arc::new(val))),
                Child::resident_new(Arc::new(new), src),
                underfull,
            );
        }
        let mut entries = node.entries.clone();
        let first = entries.remove(0);
        let underfull = entries.len() < MIN_KEYS;
        (
            first,
            Child::resident_new(
                Arc::new(BTreeNode {
                    entries,
                    children: Children::new(),
                    block: None,
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
        (
            entry,
            Child::resident_new(Arc::new(BTreeNode { entries, children, block: None }), src),
            underfull,
        )
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

/// Take entry `i` out of a uniquely-owned node, handing it back with its
/// value in an `Arc`.
///
/// A rotation lifts an entry into the *parent*, and an inner node's entries
/// are always Arc-backed (I-B), so a value coming out of a block has to
/// leave the block entirely rather than just change slots. The surviving
/// values are moved into one exactly-sized fresh block — never
/// clone-then-mutate (spec §4), and never a `Box<[V]>` shrink-realloc.
fn take_entry_as_arc<K, V>(n: &mut BTreeNode<K, V>, i: usize) -> (K, Value<V>) {
    let (k, slot) = n.entries.remove(i);
    match slot.into_arc() {
        Some(a) => (k, Value::arc(a)),
        None => {
            let old = Vec::from(n.block.take().expect("I-A: in-block entry requires a block"));
            debug_assert_eq!(old.len(), n.entries.len() + 1, "I-A: block length must match entries length");
            let mut kept: Vec<V> = Vec::with_capacity(old.len() - 1);
            let mut taken = None;
            for (j, v) in old.into_iter().enumerate() {
                if j == i {
                    taken = Some(v);
                } else {
                    kept.push(v);
                }
            }
            n.block = Some(kept.into_boxed_slice());
            (k, Value::arc(Arc::new(taken.expect("i is in range"))))
        }
    }
}

/// Insert an Arc-backed entry at index `i` of a uniquely-owned node — the
/// inverse of [`take_entry_as_arc`]: a separator descending out of the
/// parent into a child. If the child is block-backed the value is re-homed
/// into a fresh, exactly-sized block; otherwise the `Arc` is stored as-is.
///
/// **Unwind order matters here** (review round 1, Important 2). `n` is
/// already installed in its parent's `children`, so it is *reachable*: if
/// this function panicked after taking `n.block` but before putting a new
/// one back, it would leave a live node with `block: None` and all-`in_block`
/// entries — an I-A violation with the parent's separator slot already
/// overwritten by the rotation. So the one fallible step (`take_value`,
/// which can panic on an I-B breach) runs FIRST, while `n` is still
/// untouched, and `n`'s own storage is then vacated wholesale before the
/// rebuild: from that point on `n` is either intact or a well-formed empty
/// leaf, never a half-rebuilt one.
fn put_entry_from_arc<K, V>(
    n: &mut BTreeNode<K, V>,
    i: usize,
    key: K,
    val: Value<V>,
    src: Option<&dyn NodeSource<K, V>>,
) {
    if n.block.is_none() {
        n.entries.insert(i, (key, val));
        return;
    }
    // Fallible, and deliberately before any mutation of `n`.
    let v = take_value(
        val.into_arc()
            .expect("I-B: a separator lives in an inner node and is always Arc-backed"),
        src,
    );
    let old = Vec::from(n.block.take().expect("checked non-None above"));
    // Vacating `entries` too keeps I-A true (0 entries, no block) across the
    // one remaining panic site below, `FixedVec::insert`'s capacity assert.
    let mut entries = std::mem::take(&mut n.entries);
    debug_assert_eq!(old.len(), entries.len(), "I-A: block length must match entries length");
    let mut fresh: Vec<V> = Vec::with_capacity(old.len() + 1);
    let mut it = old.into_iter();
    for _ in 0..i {
        fresh.push(it.next().expect("i <= len"));
    }
    fresh.push(v);
    fresh.extend(it);
    entries.insert(i, (key, Value::in_block()));
    n.entries = entries;
    n.block = Some(fresh.into_boxed_slice());
}

/// Rotates an entry from the left sibling into the current child.
///
/// Mutates the two siblings in place via [`Child::make_mut`]: each is
/// cloned only if still shared with an older snapshot, otherwise edited
/// directly. `split_at_mut` (via `as_mut_slice`) yields disjoint `&mut`
/// handles to the two adjacent children at once.
///
/// A rotation between block leaves must leave both of them block-backed
/// (spec §4) — which is why this code, not `make_mut`, owns the block
/// bookkeeping (Task 5's `make_mut_keep_block` was folded back into
/// `make_mut` when Task 6 removed the materializing stopgap). The one
/// migrating value
/// crosses the Arc boundary twice — out of the left leaf's block into the
/// parent's separator slot ([`take_entry_as_arc`]), and out of the parent's
/// old separator into the right leaf's block ([`put_entry_from_arc`]) —
/// while every other value stays exactly where it is.
fn rotate_right<K: Clone, V>(
    entries: &mut Entries<K, V>,
    children: &mut Children<K, V>,
    idx: usize,
    src: Option<&dyn NodeSource<K, V>>,
) {
    // TODO(perf, review round 1 Minor 1): on a *shared* block leaf
    // `make_mut` builds a whole fresh `Box<[V]>` via `clone_with`
    // that `take_entry_as_arc`/`put_entry_from_arc` then discard for another
    // one. The `clone_value` count is optimal either way (every value must
    // be duplicated out of the shared node); the second *allocation* is
    // pure waste, and fusing it needs a `Child`-level "CoW straight into the
    // edited shape" entry point. Same at `rotate_left`/`merge_with_*`.
    let (left_part, right_part) = children.as_mut_slice().split_at_mut(idx);
    let left = left_part[idx - 1].make_mut(src);
    let right = right_part[0].make_mut(src);

    // Steal the last entry (and trailing child) of the left sibling.
    let stolen = take_entry_as_arc(left, left.entries.len() - 1);
    let stolen_child = if left.children.is_empty() {
        None
    } else {
        Some(left.children.pop().unwrap())
    };
    // The stolen entry becomes the new separator; the old separator descends
    // into the front of the right child.
    let separator = std::mem::replace(&mut entries[idx - 1], stolen);
    put_entry_from_arc(right, 0, separator.0, separator.1, src);
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
    // TODO(perf, review round 1 Minor 1): see `rotate_right` — a shared
    // block leaf's block is allocated twice, once by `clone_with` and once
    // by the rebuild.
    let (left_part, right_part) = children.as_mut_slice().split_at_mut(idx + 1);
    let left = left_part[idx].make_mut(src);
    let right = right_part[0].make_mut(src);

    // Steal the first entry (and leading child) of the right sibling.
    let stolen = take_entry_as_arc(right, 0);
    let stolen_child = if right.children.is_empty() {
        None
    } else {
        Some(right.children.remove(0))
    };
    // The stolen entry becomes the new separator; the old separator descends
    // onto the end of the left child.
    let separator = std::mem::replace(&mut entries[idx], stolen);
    let at = left.entries.len();
    put_entry_from_arc(left, at, separator.0, separator.1, src);
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
    // `make_mut` hands a block leaf back block-shaped: a block-leaf `left` must stay
    // block-shaped so `absorb` can fold both sides into ONE fresh block
    // (spec §4) instead of de-blocking the merged result. The separator is
    // handed to `absorb` rather than pushed here, because pushing an
    // Arc-backed entry onto a block leaf would break I-A in between.
    // TODO(perf, review round 1 Minor 1): see `rotate_right` — a shared
    // block leaf's block is allocated twice, once by `clone_with` and once
    // by `absorb`'s merged rebuild.
    let left = children[idx - 1].make_mut(src);
    absorb(left, separator, right, src);
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
    // See `merge_with_left` for why the block bookkeeping lives here and why
    // the separator travels into `absorb` instead of being pushed here.
    // TODO(perf, review round 1 Minor 1): see `rotate_right`.
    let left = children[idx].make_mut(src);
    absorb(left, separator, right, src);
}

/// Appends the descended `separator` and then `right`'s entries and
/// children onto `left`.
///
/// If either side is a block leaf, the merged node is a block leaf too:
/// both sides' values (and the separator's, converted out of its `Arc`) are
/// folded into **one** fresh block in a single pass, rather than the merge
/// silently de-blocking the result (spec §4). Values already living in a
/// block move across untouched; an all-Arc side's values are taken out of
/// their entry slots first, so `take_value`'s `Arc::try_unwrap` fast path
/// can still fire when this node held the last reference.
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
fn absorb<K: Clone, V>(
    left: &mut BTreeNode<K, V>,
    separator: (K, Value<V>),
    right: Child<K, V>,
    src: Option<&dyn NodeSource<K, V>>,
) {
    let arc = right.load_arc(src);
    drop(right); // release the slot's own count first, so try_unwrap can succeed
    // `right` is consumed *by value* here, never through `make_mut`.
    // `clone_with` (not plain `Clone`, which would assert/corrupt on a block
    // leaf — see its own I-B guard) is what duplicates the shared branch;
    // Task 4 then de-blocked the result to keep the old entry-appending code
    // below safe. Task 5 folds the sibling into the merged block directly
    // instead, which is both correct and block-preserving.
    let mut rn = Arc::try_unwrap(arc).unwrap_or_else(|a| a.clone_with(src));
    // The merged entry count. Guaranteed `<= MAX_KEYS` by
    // `fix_underfull_child`'s contract (a merge only happens when both
    // siblings sit at `MIN_KEYS` or below, and `2 * MIN_KEYS + 1 ==
    // MAX_KEYS`), not by anything local — which is why it is asserted here
    // rather than assumed, now that the block path also sizes an allocation
    // from it (review round 1, Minor 6).
    let merged_len = left.entries.len() + 1 + rn.entries.len();
    debug_assert!(
        merged_len <= MAX_KEYS,
        "merge would overflow a node: {merged_len} > MAX_KEYS"
    );
    if left.block.is_none() && rn.block.is_none() {
        // Neither side is block-shaped (every inner-node merge, and every
        // leaf merge on a non-paged tree — or on a paged one whose leaves
        // have not been through a checkpoint/fault-in round trip yet): the
        // pre-block code, unchanged.
        left.entries.push(separator);
        left.entries.extend(rn.entries);
        left.children.extend(rn.children);
        return;
    }
    // At least one side is block-shaped, so the merged node is too. Blocks
    // only ever live at leaf depth, so neither side has children (I-A).
    debug_assert!(
        left.children.is_empty() && rn.children.is_empty(),
        "I-A: only leaves carry a value block"
    );
    // `left` is reachable from its parent's `children`, and the conversions
    // below (`take_value`) can panic on an I-B breach — so vacate `left`
    // wholesale first and build the merged node in locals, committing it in
    // two infallible assignments at the end. An unwind then leaves `left` a
    // well-formed empty leaf rather than a half-rebuilt I-A violation
    // (review round 1, Important 2). `rn` is a local the caller already
    // removed from the tree, so mutating it needs no such care.
    let left_entries = std::mem::take(&mut left.entries);
    let left_block = left.block.take();
    let mut entries: Entries<K, V> = Entries::new();
    let mut block: Vec<V> = Vec::with_capacity(merged_len);
    match left_block {
        // Already block-backed: entries are already `in_block` markers and
        // the values move straight across, no clone.
        Some(b) => {
            block.extend(Vec::from(b));
            entries.extend(left_entries.into_iter().map(|(k, _)| (k, Value::in_block())));
        }
        // All-Arc side: each slot's `Arc` leaves its entry *by value* (not
        // as a clone of it) so `take_value` can move the value out when this
        // node held the last reference.
        None => {
            for (k, v) in left_entries {
                block.push(take_value(
                    v.into_arc().expect("I-A: a non-block leaf's entries are Arc-backed"),
                    src,
                ));
                entries.push((k, Value::in_block()));
            }
        }
    }
    let (sk, sv) = separator;
    block.push(take_value(
        sv.into_arc()
            .expect("I-B: a separator lives in an inner node and is always Arc-backed"),
        src,
    ));
    entries.push((sk, Value::in_block()));
    match rn.block.take() {
        Some(b) => {
            block.extend(Vec::from(b));
            entries.extend(rn.entries.into_iter().map(|(k, _)| (k, Value::in_block())));
        }
        None => {
            for (k, v) in rn.entries {
                block.push(take_value(
                    v.into_arc().expect("I-A: a non-block leaf's entries are Arc-backed"),
                    src,
                ));
                entries.push((k, Value::in_block()));
            }
        }
    }
    left.entries = entries;
    left.block = Some(block.into_boxed_slice());
}

/// In-place counterpart to `delete_from_node`. Descends through
/// `Child::make_mut`, so each node is cloned only if it is still shared with
/// another snapshot (copy-on-write preserved) and otherwise mutated directly.
/// Reuses the existing rebalance helpers (`fix_underfull_child` et al.),
/// which already mutate the parent's `entries`/`children` in place — and
/// which have been block-aware since Task 5, so the rotations and merges an
/// underflow triggers keep their leaves block-shaped on this path too.
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
                // A block leaf rebuilds its block (by move — the node is
                // uniquely owned) and stays block-shaped; an all-Arc leaf
                // shifts its entries as before.
                if n.block.is_some() {
                    remove_from_block_leaf_mut(n, i);
                } else {
                    n.entries.remove(i);
                }
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
///
/// The entry it hands back is promoted into an *inner* node (it replaces the
/// deleted separator), and inner-node entries are always Arc-backed (I-B) —
/// so on a block leaf the value has to leave the block entirely rather than
/// just change slots. `take_entry_as_arc` (Task 5, shared with the rotation
/// path) is exactly that boundary, and is a plain `entries.remove` on an
/// all-Arc leaf.
fn remove_leftmost_mut<K: Ord + Clone, V>(
    node: &mut Child<K, V>,
    src: Option<&dyn NodeSource<K, V>>,
) -> ((K, Value<V>), bool) {
    let n = node.make_mut(src);
    if n.children.is_empty() {
        let first = take_entry_as_arc(n, 0);
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
            // The spine's leaf level (the last iteration, `i == spine.len() -
            // 1`) can be a block leaf on a recovered paged tree — dead at
            // task 3, live since task 4's decode-to-block (fix round 1,
            // Critical 2). Reads only (this never mutates through the
            // `Arc`, so no `make_mut` CoW applies here): an
            // in-block entry has no per-entry `Arc` to clone, so build a
            // fresh one via the source's `clone_value` (I-B: guaranteed
            // `Some` for any tree that could hold a block leaf) instead of
            // the old "not yet supported" stub.
            lv.entries.extend(spine_node.entries.iter().enumerate().map(|(idx, (k, v))| {
                let arc = match v.as_arc() {
                    Some(a) => Arc::clone(a),
                    None => Arc::new(
                        src.and_then(|s| s.clone_value(spine_node.value_at(idx)))
                            .expect("I-B: block leaf requires a cloning source"),
                    ),
                };
                (k.clone(), arc)
            }));
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
                        entries: std::mem::take(&mut lv.entries)
                            .into_iter()
                            .map(|(k, v)| (k, Value::arc(v)))
                            .collect(),
                        children: Children::new(),
                        block: None,
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
                            entries: entries.into_iter().map(|(k, v)| (k, Value::arc(v))).collect(),
                            children: children.into_iter().collect(),
                            block: None,
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
                    block: None,
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
/// tail is already balanced.
///
/// Every node on the *spine itself* was freshly built by this `finish()`
/// call, so the `make_mut_quiet` walk below never clones. That is **not**
/// true of the siblings `fix_underfull_child` reaches: `seed_from_spine`
/// pushes `Child` clones of the input tree's non-rightmost spine children
/// into the builder's levels, so a sibling can still be shared with that
/// tree — and, on a recovered paged tree, can be an on-disk block leaf.
/// `rotate_*`/`merge_*` open those through `Child::make_mut`, which CoWs
/// them via `clone_with` and keeps them block-shaped, so this is
/// correct; the old blanket "never clones" claim was simply wrong about
/// them (review round 1, Minor 5).
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
    // `sibling` at the leaf level can be a block leaf on a recovered paged
    // tree — dead at task 3, live since task 4's decode-to-block (fix round
    // 1, Critical 2, same shape as `seed_from_spine` above). Reads only, no
    // `make_mut` CoW applies: an in-block entry has no per-entry
    // `Arc`, so clone one out via the source (I-B) instead of the old
    // "not yet supported" stub.
    let mut merged_entries: Vec<(K, Arc<V>)> = sibling
        .entries
        .iter()
        .enumerate()
        .map(|(idx, (k, v))| {
            let arc = match v.as_arc() {
                Some(a) => a.clone(),
                None => Arc::new(
                    src.and_then(|s| s.clone_value(sibling.value_at(idx)))
                        .expect("I-B: block leaf requires a cloning source"),
                ),
            };
            (k.clone(), arc)
        })
        .collect();
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
            entries: new_left_entries.into_iter().map(|(k, v)| (k, Value::arc(v))).collect(),
            children: new_left_children.into_iter().collect(),
            block: None,
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
            entries: std::mem::take(&mut lv.entries)
                .into_iter()
                .map(|(k, v)| (k, Value::arc(v)))
                .collect(),
            children: Children::new(),
            block: None,
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
            entries: entries.into_iter().map(|(k, v)| (k, Value::arc(v))).collect(),
            children: children.into_iter().collect(),
            block: None,
        }),
        src,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Representation walk over a whole tree, for the block-leaf tests.
///
/// Returns `(leaves, block_leaves)` and asserts the representation
/// invariants on the way: I-A (every entry's slot kind agrees with its
/// node's `block`, and the block is exactly as long as `entries`), blocks
/// only at leaf depth, and uniform leaf depth.
///
/// Generic and crate-visible on purpose. Whether a leaf is block-backed is
/// invisible from outside the crate — no public API exposes it — so the
/// *store*-level version of "a `Table` write keeps its leaves block-backed"
/// (Task 5 review, warning 3; Task 6) has to be asserted from a test module
/// inside the crate. `store.rs`'s
/// `paged_table_leaves_stay_block_backed_across_a_mixed_table_workload` is
/// that test; `btree`'s own `tests::block_leaves` module wraps this in its
/// `walk_representation` helper. The integration file
/// `tests/paged_block_leaves.rs` holds the behavioural oracle instead.
#[cfg(test)]
impl<K, V> BTree<K, V> {
    pub(crate) fn leaf_representation(&self) -> (usize, usize) {
        fn go<K, V>(
            c: &Child<K, V>,
            src: Option<&dyn NodeSource<K, V>>,
            depth: usize,
            leaf_depth: &mut Option<usize>,
            out: &mut (usize, usize),
        ) {
            let n = c.load(src);
            for i in 0..n.entries.len() {
                assert_eq!(
                    n.entries[i].1.is_in_block(),
                    n.block.is_some(),
                    "I-A: entry {i} slot kind disagrees with the node's block"
                );
            }
            if let Some(b) = &n.block {
                assert_eq!(b.len(), n.entries.len(), "I-A: block length must match entries length");
                assert!(n.children.is_empty(), "blocks live only at leaf depth");
            }
            if n.children.is_empty() {
                out.0 += 1;
                if n.block.is_some() {
                    out.1 += 1;
                }
                match *leaf_depth {
                    Some(d) => assert_eq!(d, depth, "non-uniform leaf depth"),
                    None => *leaf_depth = Some(depth),
                }
            } else {
                for i in 0..n.children.len() {
                    go(&n.children[i], src, depth + 1, leaf_depth, out);
                }
            }
        }
        let mut out = (0, 0);
        go(&self.root, self.source.as_deref(), 0, &mut None, &mut out);
        out
    }
}

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

    #[test]
    fn value_slot_is_pointer_sized_and_entry_layout_unchanged() {
        use std::mem::size_of;
        assert_eq!(size_of::<Value<u64>>(), size_of::<Arc<u64>>(), "niche lost");
        assert_eq!(size_of::<(u64, Value<u64>)>(), size_of::<(u64, Arc<u64>)>());
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
            leaf_entries.push((1u64, Value::arc(Arc::new(10u64))));
            let leaf: Arc<BTreeNode<u64, u64>> = Arc::new(BTreeNode {
                entries: leaf_entries,
                children: Default::default(),
                block: None,
            });
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
    // Block-leaf mutations on the IMMUTABLE path (Task 5, spec §4).
    //
    // A block leaf (`block: Some`, every entry an `in_block` marker) is
    // what `NodeCodec::decode` builds for a `DataLeaf` page. Before Task 5
    // the immutable path (`BTree::insert`/`remove` -> `insert_into_node` /
    // `delete_from_node` / `remove_leftmost` / `absorb`) cloned such a
    // leaf's `entries` into a `block: None` node, leaving every untouched
    // entry an `in_block` marker with no block behind it — an I-A
    // violation that surfaces as a `value_at` panic on the next read.
    //
    // These tests assert both halves of the fix: the *values* are right
    // (behavioural), and the mutated leaves are still **block-backed**
    // (representational — the point of the task; materialising them would
    // give the right answers while silently throwing the memory-honesty
    // win away).
    // -----------------------------------------------------------------
    mod block_leaves {
        use super::*;
        use crate::child::tests::MockDisk;
        use std::sync::Arc;

        /// Rebuild `c`'s subtree with every leaf in **block** form — the
        /// shape `NodeCodec::decode` hands back for a `DataLeaf` page.
        /// Inner nodes stay all-Arc (I-B).
        fn blockify_child(c: &Child<u64, u64>) -> Child<u64, u64> {
            let n = c.load(None);
            if n.children.is_empty() {
                let mut entries: Entries<u64, u64> = Entries::new();
                let mut block: Vec<u64> = Vec::with_capacity(n.entries.len());
                for i in 0..n.entries.len() {
                    entries.push((n.entries[i].0, Value::in_block()));
                    block.push(*n.value_at(i));
                }
                Child::resident(Arc::new(BTreeNode {
                    entries,
                    children: Children::new(),
                    block: Some(block.into_boxed_slice()),
                }))
            } else {
                let mut children: Children<u64, u64> = Children::new();
                for i in 0..n.children.len() {
                    children.push(blockify_child(&n.children[i]));
                }
                Child::resident(Arc::new(BTreeNode {
                    entries: n.entries.clone(),
                    children,
                    block: None,
                }))
            }
        }

        /// `t` with every leaf block-backed and `disk` attached as the
        /// value-cloning source (I-B: a block leaf may only exist on a
        /// source that can clone values).
        fn blockify(t: &BTree<u64, u64>, disk: &Arc<MockDisk<u64, u64>>) -> BTree<u64, u64> {
            let mut out = BTree {
                root: blockify_child(&t.root),
                len: t.len,
                height: t.height,
                source: None,
            };
            out.set_source(Some(disk.clone()));
            out
        }

        /// Counted result of [`walk_representation`].
        #[derive(Debug, Default, PartialEq)]
        struct Repr {
            leaves: usize,
            block_leaves: usize,
        }

        /// Walk every reachable node asserting the representation
        /// invariants: I-A (every entry's slot kind agrees with the node's
        /// `block`, and the block is exactly as long as `entries`) and
        /// "blocks only at leaf depth". Counts leaves and block leaves.
        ///
        /// A thin named wrapper over `BTree::leaf_representation`, which is
        /// generic and crate-visible so `store.rs`'s paged tests can make
        /// the same assertion against a real `Table`'s row tree.
        fn walk_representation(t: &BTree<u64, u64>) -> Repr {
            let (leaves, block_leaves) = t.leaf_representation();
            Repr { leaves, block_leaves }
        }

        /// Row counts for the two "deep tree" tests, capped hard under
        /// Miri (whose interpreter makes a 5k-row build minutes of work)
        /// while staying well past a single leaf so the multi-level shapes
        /// these tests exist for still form.
        const DEEP: u64 = if cfg!(miri) { 300 } else { 5_000 };

        /// `NodeSource::clone_value` calls so far. The instrument for spec
        /// §4's ONE-PASS property: a clone-then-mutate rewrite of any of
        /// these builders would still produce correct values and correct
        /// representation — this counter is the only thing that would
        /// notice (review round 1, Important 1).
        fn cloned(disk: &MockDisk<u64, u64>) -> u64 {
            disk.cloned.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn read_all(t: &BTree<u64, u64>, keys: impl Iterator<Item = u64>) {
            for k in keys {
                assert_eq!(t.get(&k), Some(&(k * 10)), "key {k}");
            }
        }

        /// Baseline: `blockify` really does produce block leaves, and a
        /// block tree reads exactly like the all-Arc tree it came from.
        /// Without this the "still block-backed" assertions below could
        /// pass vacuously on a tree that never had a block at all.
        #[test]
        fn blockify_produces_block_leaves_that_read_correctly() {
            let disk = Arc::new(MockDisk::new());
            let t = blockify(&insert_range(1, DEEP), &disk);
            let r = walk_representation(&t);
            assert!(r.leaves > 1, "a deep tree must span more than one leaf");
            assert_eq!(r.block_leaves, r.leaves, "every leaf blockified");
            read_all(&t, 1..=DEEP);
            check_invariants(&t);
        }

        /// `insert` over an existing key: the leaf is rebuilt in one pass
        /// and stays block-backed; every *other* value in that leaf still
        /// reads (pre-Task-5 they became dangling `in_block` markers in a
        /// `block: None` node).
        #[test]
        fn immutable_replace_keeps_the_leaf_block_backed() {
            let disk = Arc::new(MockDisk::new());
            let t = blockify(&insert_range(1, 40), &disk);
            let before = cloned(&disk);
            let t2 = t.insert(7, 999);
            // ONE PASS: exactly the 39 values the edit does NOT touch are
            // duplicated out of the old block. The replaced value moves in
            // from the caller's `Arc` (`take_value`'s `try_unwrap`) and the
            // displaced one is dropped, never cloned — a clone-then-mutate
            // rebuild would read 40 here.
            assert_eq!(cloned(&disk) - before, 39, "Replace must clone exactly n-1 values");
            assert_eq!(t2.get(&7), Some(&999));
            for k in 1..=40u64 {
                if k != 7 {
                    assert_eq!(t2.get(&k), Some(&(k * 10)), "key {k}");
                }
            }
            let r = walk_representation(&t2);
            assert_eq!(r, Repr { leaves: 1, block_leaves: 1 }, "a replaced block leaf must stay block-backed");
            // The base tree is untouched (CoW): still block-backed, old value.
            assert_eq!(t.get(&7), Some(&70));
            assert_eq!(walk_representation(&t), Repr { leaves: 1, block_leaves: 1 });
        }

        /// `insert` of a new key that fits: block-backed in, block-backed out.
        #[test]
        fn immutable_insert_keeps_the_leaf_block_backed() {
            let disk = Arc::new(MockDisk::new());
            let t = blockify(&insert_range(1, 40), &disk);
            let before = cloned(&disk);
            let t2 = t.insert(0, 0);
            // ONE PASS: all 40 existing values carried across, the new one
            // moved in from the caller's `Arc`.
            assert_eq!(cloned(&disk) - before, 40, "Insert must clone exactly n values");
            assert_eq!(t2.len(), 41);
            assert_eq!(t2.get(&0), Some(&0));
            read_all(&t2, 1..=40);
            assert_eq!(walk_representation(&t2), Repr { leaves: 1, block_leaves: 1 });
        }

        /// An overflowing insert splits through `split_block`: BOTH halves
        /// come out block-backed and the promoted median is Arc-backed
        /// (separators live in inner nodes, which are never block-backed —
        /// I-B). `walk_representation` asserts that second half directly.
        #[test]
        fn immutable_insert_splits_a_block_leaf_into_two_block_leaves() {
            let disk = Arc::new(MockDisk::new());
            let base = insert_range(1, MAX_KEYS as u64);
            assert_eq!(base.height(), 0, "MAX_KEYS rows must still be a single leaf");
            let t = blockify(&base, &disk);
            let before = cloned(&disk);
            let t2 = t.insert(0, 0);
            // `split_block` itself clones NOTHING: the whole delta is the
            // preceding `rebuild_block_leaf(Insert)`'s n carries, and the
            // split then drains that block into two halves by move.
            assert_eq!(
                cloned(&disk) - before,
                MAX_KEYS as u64,
                "split_block must add zero clones on top of the rebuild"
            );
            assert_eq!(t2.height(), 1, "the leaf must have split");
            assert_eq!(t2.len(), MAX_KEYS + 1);
            assert_eq!(t2.get(&0), Some(&0));
            read_all(&t2, 1..=MAX_KEYS as u64);
            assert_eq!(
                walk_representation(&t2),
                Repr { leaves: 2, block_leaves: 2 },
                "both halves of a split block leaf must be block-backed"
            );
            check_invariants(&t2);
        }

        /// `remove` on a block leaf rebuilds the block without that slot.
        #[test]
        fn immutable_delete_keeps_the_leaf_block_backed() {
            let disk = Arc::new(MockDisk::new());
            let t = blockify(&insert_range(1, 40), &disk);
            let before = cloned(&disk);
            let t2 = t.remove(&7).unwrap();
            // ONE PASS: the 39 survivors are carried; the removed value is
            // never cloned, it simply is not carried.
            assert_eq!(cloned(&disk) - before, 39, "Remove must clone exactly n-1 values");
            assert_eq!(t2.len(), 39);
            assert_eq!(t2.get(&7), None);
            for k in (1..=40u64).filter(|k| *k != 7) {
                assert_eq!(t2.get(&k), Some(&(k * 10)), "key {k}");
            }
            assert_eq!(walk_representation(&t2), Repr { leaves: 1, block_leaves: 1 });
        }

        /// Deleting a key that sits in an *inner* node replaces it with the
        /// in-order successor lifted out of a block leaf
        /// (`remove_leftmost`). That entry lands in an inner node, so its
        /// value must leave the block and be re-homed into an `Arc` —
        /// before Task 5 the parent got a bare `in_block` marker with no
        /// block behind it, and reading the separator panicked.
        #[test]
        fn immutable_delete_of_an_inner_separator_rehomes_the_successor() {
            let disk = Arc::new(MockDisk::new());
            let base = insert_range(1, DEEP);
            let sep = {
                // A key that really is stored in the root, not in a leaf.
                let root = base.root.load(None);
                assert!(!root.children.is_empty(), "a deep tree's root is internal");
                root.entries[0].0
            };
            let t = blockify(&base, &disk);
            let t2 = t.remove(&sep).unwrap();
            assert_eq!(t2.get(&sep), None);
            assert_eq!(t2.len() as u64, DEEP - 1);
            read_all(&t2, (1..=DEEP).filter(move |k| *k != sep));
            let r = walk_representation(&t2);
            assert_eq!(r.block_leaves, r.leaves, "every leaf stays block-backed");
            check_invariants(&t2);
        }

        /// A delete that underflows a leaf and merges it with its sibling
        /// must produce ONE fresh block leaf holding both sides' values
        /// plus the descended separator (spec §4) — not a materialised
        /// all-Arc leaf, which is what the Task 4 stopgap produced.
        #[test]
        fn merging_two_block_leaves_yields_one_block_leaf() {
            let disk = Arc::new(MockDisk::new());
            // Two leaves under one root: MAX_KEYS + 1 ascending keys split
            // into halves of `MAX_KEYS / 2` and `MAX_KEYS - MAX_KEYS / 2`.
            let base = insert_range(1, MAX_KEYS as u64 + 1);
            assert_eq!(base.height(), 1);
            let mut t = blockify(&base, &disk);
            assert_eq!(walk_representation(&t).block_leaves, 2);
            // Delete from the left leaf until it underflows and merges.
            let mut alive: Vec<u64> = (1..=MAX_KEYS as u64 + 1).collect();
            let mut k = 1u64;
            while walk_representation(&t).leaves > 1 {
                t = t.remove(&k).unwrap();
                alive.retain(|x| *x != k);
                k += 1;
                assert!(k <= MAX_KEYS as u64, "the leaves must merge before running out of keys");
            }
            assert_eq!(
                walk_representation(&t),
                Repr { leaves: 1, block_leaves: 1 },
                "a merge of block leaves must yield a block leaf, not a materialised one"
            );
            assert_eq!(t.len(), alive.len());
            for key in alive {
                assert_eq!(t.get(&key), Some(&(key * 10)), "key {key}");
            }
            check_invariants(&t);
        }

        /// Equivalence oracle at the `BTree` level: a long mixed
        /// insert/replace/delete sequence run against a block-backed tree
        /// and against a plain all-Arc tree must agree key for key, the
        /// block tree must keep every structural invariant, and its leaves
        /// must still be block-backed at the end.
        ///
        /// This is the `BTree`-level counterpart of the store-level oracle
        /// in `tests/paged_block_leaves.rs`; it lives here because the
        /// *representation* assertion needs crate internals, and because
        /// the immutable path (`BTree::insert`/`remove`) is not what
        /// `Table` drives — the store-level oracle exercises the shared
        /// rebalance/merge code, this exercises the immutable path itself.
        #[test]
        fn immutable_mixed_workload_matches_a_plain_tree_and_stays_block_backed() {
            let disk = Arc::new(MockDisk::new());
            // Miri caps (see `DEEP`): still multi-level and still merge/
            // rotate-heavy, just small enough for the interpreter.
            let (rows, rounds, space) = if cfg!(miri) { (200u64, 150u64, 250u64) } else { (2_000, 1_500, 2_500) };
            let mut blocked = blockify(&insert_range(1, rows), &disk);
            let mut plain = insert_range(1, rows);
            // Deterministic xorshift: a failing run is reproducible from
            // the source alone, and it needs no dev-dependency under Miri.
            let mut x: u64 = 0x9E3779B97F4A7C15;
            let mut next = || {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x
            };
            for _ in 0..rounds {
                let r = next();
                let key = r % space;
                match r % 3 {
                    0 => {
                        blocked = blocked.insert(key, key * 10);
                        plain = plain.insert(key, key * 10);
                    }
                    1 => {
                        blocked = blocked.insert(key, key * 10 + 1);
                        plain = plain.insert(key, key * 10 + 1);
                    }
                    _ => {
                        if plain.get(&key).is_some() {
                            blocked = blocked.remove(&key).unwrap();
                            plain = plain.remove(&key).unwrap();
                        } else {
                            assert!(blocked.remove(&key).is_err());
                        }
                    }
                }
            }
            assert_eq!(blocked.len(), plain.len());
            let a: Vec<(u64, u64)> = blocked.range(..).map(|(k, v)| (*k, *v)).collect();
            let b: Vec<(u64, u64)> = plain.range(..).map(|(k, v)| (*k, *v)).collect();
            assert_eq!(a, b, "block-backed tree diverged from the all-Arc oracle");
            check_invariants(&blocked);
            let r = walk_representation(&blocked);
            assert_eq!(
                r.block_leaves, r.leaves,
                "every leaf must still be block-backed after a mixed immutable workload"
            );
        }


        // -------------------------------------------------------------
        // Direct coverage for the rebalance helpers' representation
        // sub-branches (review round 1, Important 1).
        //
        // `absorb` and `rotate_*` each branch on EACH sibling's own `block`
        // independently, so a merge or rotation can see any of the four
        // (block, all-Arc) x (block, all-Arc) pairings. The mixed pairings
        // are exactly what the store-level `_mut` path produces today (the
        // target leaf is materialised by `make_mut`, its sibling is not),
        // and until now the only evidence they ran was a temporary `panic!`
        // probe. These drive the helpers directly, with unique (unshared)
        // nodes so the clone counter reads the helper's own cost and
        // nothing else.
        // -------------------------------------------------------------

        /// A leaf whose values live behind per-entry `Arc`s. Value = key*10.
        fn arc_leaf(keys: &[u64]) -> BTreeNode<u64, u64> {
            BTreeNode {
                entries: keys.iter().map(|k| (*k, Value::arc(Arc::new(k * 10)))).collect(),
                children: Children::new(),
                block: None,
            }
        }

        /// A leaf whose values live inline in `block`. Value = key*10.
        fn block_leaf(keys: &[u64]) -> BTreeNode<u64, u64> {
            BTreeNode {
                entries: keys.iter().map(|k| (*k, Value::in_block())).collect(),
                children: Children::new(),
                block: Some(keys.iter().map(|k| k * 10).collect::<Vec<_>>().into_boxed_slice()),
            }
        }

        fn leaf(block: bool, keys: &[u64]) -> BTreeNode<u64, u64> {
            if block { block_leaf(keys) } else { arc_leaf(keys) }
        }

        /// Every `(key, value)` of a leaf, read through whichever
        /// representation it uses, plus a per-node I-A check.
        fn dump_leaf(n: &BTreeNode<u64, u64>) -> Vec<(u64, u64)> {
            for i in 0..n.entries.len() {
                assert_eq!(
                    n.entries[i].1.is_in_block(),
                    n.block.is_some(),
                    "I-A: entry {i} slot kind disagrees with the node's block"
                );
            }
            if let Some(b) = &n.block {
                assert_eq!(b.len(), n.entries.len(), "I-A: block length");
            }
            (0..n.entries.len()).map(|i| (n.entries[i].0, *n.value_at(i))).collect()
        }

        /// All four representation pairings of a leaf merge. Whichever
        /// sides are block-backed, the merged node holds every value in
        /// order; it is block-backed iff either side was; and because both
        /// siblings and the separator are uniquely owned here, the merge
        /// costs **zero** `clone_value` calls in every pairing — values
        /// move, they are never duplicated (spec §4).
        #[test]
        fn absorb_covers_all_four_representation_pairings_without_cloning() {
            for (left_block, right_block) in [(true, true), (true, false), (false, true), (false, false)] {
                let disk = MockDisk::<u64, u64>::new();
                let mut left = leaf(left_block, &[1, 2, 3]);
                let right = Child::resident(Arc::new(leaf(right_block, &[5, 6, 7])));
                let separator = (4u64, Value::arc(Arc::new(40u64)));

                let before = cloned(&disk);
                absorb(&mut left, separator, right, Some(&disk));
                assert_eq!(
                    cloned(&disk) - before,
                    0,
                    "merging uniquely-owned leaves ({left_block}, {right_block}) must move values, not clone them"
                );

                assert_eq!(
                    left.block.is_some(),
                    left_block || right_block,
                    "({left_block}, {right_block}): a merge involving a block leaf must yield a block leaf"
                );
                assert_eq!(
                    dump_leaf(&left),
                    (1..=7u64).map(|k| (k, k * 10)).collect::<Vec<_>>(),
                    "({left_block}, {right_block}): merged contents"
                );
            }
        }

        /// The same four pairings for `rotate_right` (steal the left
        /// sibling's last entry). Each sibling keeps its OWN representation
        /// — I-A is per-node — the migrating value crosses the `Arc`
        /// boundary exactly once in each direction, and uniquely-owned
        /// siblings cost zero clones.
        #[test]
        fn rotate_right_covers_all_four_representation_pairings_without_cloning() {
            for (left_block, right_block) in [(true, true), (true, false), (false, true), (false, false)] {
                let disk = MockDisk::<u64, u64>::new();
                let mut entries: Entries<u64, u64> = Entries::new();
                entries.push((5u64, Value::arc(Arc::new(50u64))));
                let mut children: Children<u64, u64> = Children::new();
                children.push(Child::resident(Arc::new(leaf(left_block, &[1, 2, 3, 4]))));
                children.push(Child::resident(Arc::new(leaf(right_block, &[6, 7]))));

                let before = cloned(&disk);
                rotate_right(&mut entries, &mut children, 1, Some(&disk));
                assert_eq!(
                    cloned(&disk) - before,
                    0,
                    "rotating between uniquely-owned leaves ({left_block}, {right_block}) must not clone"
                );

                // The stolen entry became the new separator, Arc-backed
                // because it now lives in an inner node (I-B).
                assert_eq!(entries[0].0, 4);
                assert_eq!(**entries[0].1.as_arc().expect("separators are Arc-backed"), 40);
                let l = children[0].load(Some(&disk));
                let r = children[1].load(Some(&disk));
                assert_eq!(l.block.is_some(), left_block, "each sibling keeps its own representation");
                assert_eq!(r.block.is_some(), right_block, "each sibling keeps its own representation");
                assert_eq!(dump_leaf(l), vec![(1, 10), (2, 20), (3, 30)]);
                assert_eq!(dump_leaf(r), vec![(5, 50), (6, 60), (7, 70)]);
            }
        }

        /// `rotate_left`'s four pairings (steal the right sibling's first
        /// entry) — the mirror of the test above.
        #[test]
        fn rotate_left_covers_all_four_representation_pairings_without_cloning() {
            for (left_block, right_block) in [(true, true), (true, false), (false, true), (false, false)] {
                let disk = MockDisk::<u64, u64>::new();
                let mut entries: Entries<u64, u64> = Entries::new();
                entries.push((3u64, Value::arc(Arc::new(30u64))));
                let mut children: Children<u64, u64> = Children::new();
                children.push(Child::resident(Arc::new(leaf(left_block, &[1, 2]))));
                children.push(Child::resident(Arc::new(leaf(right_block, &[4, 5, 6, 7]))));

                let before = cloned(&disk);
                rotate_left(&mut entries, &mut children, 0, Some(&disk));
                assert_eq!(
                    cloned(&disk) - before,
                    0,
                    "rotating between uniquely-owned leaves ({left_block}, {right_block}) must not clone"
                );

                assert_eq!(entries[0].0, 4);
                assert_eq!(**entries[0].1.as_arc().expect("separators are Arc-backed"), 40);
                let l = children[0].load(Some(&disk));
                let r = children[1].load(Some(&disk));
                assert_eq!(l.block.is_some(), left_block, "each sibling keeps its own representation");
                assert_eq!(r.block.is_some(), right_block, "each sibling keeps its own representation");
                assert_eq!(dump_leaf(l), vec![(1, 10), (2, 20), (3, 30)]);
                assert_eq!(dump_leaf(r), vec![(5, 50), (6, 60), (7, 70)]);
            }
        }

        /// A merge whose sides are still SHARED with another snapshot: the
        /// CoW is what clones (one `clone_value` per block entry, via
        /// `clone_with` inside `make_mut`), and `absorb` itself
        /// still adds none on top. Pins the cost split the report describes,
        /// so a regression that made `absorb` clone would be visible even
        /// though the total is non-zero here.
        #[test]
        fn merging_shared_block_leaves_clones_only_in_the_cow_not_in_absorb() {
            let disk = MockDisk::<u64, u64>::new();
            let left_node = Arc::new(block_leaf(&[1, 2, 3]));
            let right_node = Arc::new(block_leaf(&[5, 6, 7]));
            // A second strong count each: these leaves are "still in an
            // older snapshot", so `make_mut` must CoW them.
            let _snapshot = (Arc::clone(&left_node), Arc::clone(&right_node));

            let mut entries: Entries<u64, u64> = Entries::new();
            entries.push((4u64, Value::arc(Arc::new(40u64))));
            let mut children: Children<u64, u64> = Children::new();
            children.push(Child::resident(left_node));
            children.push(Child::resident(right_node));

            let before = cloned(&disk);
            merge_with_left(&mut entries, &mut children, 1, Some(&disk));
            assert_eq!(
                cloned(&disk) - before,
                6,
                "exactly one clone_value per shared entry (3 + 3), all of it in the CoW"
            );
            assert_eq!(children.len(), 1);
            assert!(entries.is_empty(), "the separator descended into the merged leaf");
            let merged = children[0].load(Some(&disk));
            assert!(merged.block.is_some(), "the merged leaf stays block-backed");
            assert_eq!(dump_leaf(merged), (1..=7u64).map(|k| (k, k * 10)).collect::<Vec<_>>());
        }

        /// `redistribute_tail`'s block-leaf branch (`src/btree.rs`) — the
        /// `BulkBuilder` tail rebalance reading a block-shaped sibling by
        /// shared reference and cloning its values out via
        /// `NodeSource::clone_value`.
        ///
        /// Task 4 added that branch defensively and left it uncovered; a
        /// store-level route to it looks unreachable in practice (the
        /// sibling `redistribute_tail` pops is the level's most recently
        /// *frozen* node, which the builder built itself and is therefore
        /// always all-Arc — a seeded on-disk sibling is only ever popped
        /// when the level never froze, which in turn can only happen when
        /// the seeded level is underfull, and a well-formed tree's
        /// rightmost non-root leaf never is). Rather than leave the branch
        /// untested on that argument, this drives `redistribute_tail`
        /// directly with hand-built level state whose sibling IS a block
        /// leaf, and pins both the values it recovers and the fact that it
        /// recovered them through `clone_value` (the `cloned` counter is
        /// the proof the block branch, not the `as_arc` branch, ran).
        #[test]
        fn redistribute_tail_reads_a_block_leaf_sibling_through_clone_value() {
            let disk = MockDisk::<u64, u64>::new();
            // Sibling: a block leaf holding keys 0..MAX_KEYS.
            let sib_keys: Vec<u64> = (0..MAX_KEYS as u64).collect();
            let sibling = Arc::new(BTreeNode {
                entries: sib_keys.iter().map(|k| (*k, Value::in_block())).collect(),
                children: Children::new(),
                block: Some(sib_keys.iter().map(|k| k * 10).collect::<Vec<_>>().into_boxed_slice()),
            });
            // levels[0]: the underfull partial leaf the builder is holding.
            let mut lv0 = LevelBuilder::<u64, u64>::new();
            let tail_keys: Vec<u64> = (MAX_KEYS as u64 + 1..MAX_KEYS as u64 + 1 + MIN_KEYS as u64).collect();
            lv0.entries = tail_keys.iter().map(|k| (*k, Arc::new(k * 10))).collect();
            // levels[1]: the parent, holding the sibling and the separator
            // that linked it to the partial node.
            let mut lv1 = LevelBuilder::<u64, u64>::new();
            lv1.children = vec![Child::resident(sibling)];
            lv1.entries = vec![(MAX_KEYS as u64, Arc::new(MAX_KEYS as u64 * 10))];

            let before = disk.cloned.load(std::sync::atomic::Ordering::Relaxed);
            let mut levels = vec![lv0, lv1];
            redistribute_tail::<u64, u64>(&mut levels, 0, Some(&disk));
            let cloned = disk.cloned.load(std::sync::atomic::Ordering::Relaxed) - before;
            assert_eq!(
                cloned, MAX_KEYS as u64,
                "the block branch must clone exactly one value per in-block sibling entry"
            );

            // Every key, in order, survives the redistribution with the
            // right value: the popped sibling's, the separator's, and the
            // partial node's.
            let expected: Vec<u64> = (0..MAX_KEYS as u64 + 1)
                .chain(tail_keys.iter().copied())
                .collect();
            let mut got: Vec<(u64, u64)> = Vec::new();
            for c in &levels[1].children {
                let n = c.load(Some(&disk));
                for i in 0..n.entries.len() {
                    got.push((n.entries[i].0, *n.value_at(i)));
                }
            }
            for (k, v) in &levels[1].entries {
                got.push((*k, **v));
            }
            for (k, v) in &levels[0].entries {
                got.push((*k, **v));
            }
            got.sort_unstable();
            assert_eq!(got.iter().map(|(k, _)| *k).collect::<Vec<_>>(), expected);
            assert!(got.iter().all(|(k, v)| *v == k * 10), "values must survive the fold");
            assert!(levels[0].entries.len() >= MIN_KEYS, "the partial node is no longer underfull");
        }

        // -------------------------------------------------------------
        // Block-leaf mutations on the IN-PLACE (`_mut`) path — Task 6,
        // spec §4.
        //
        // `Table` — i.e. every production write — drives
        // `BTree::insert_arc_mut`/`remove_mut`, not the immutable path
        // Task 5 covered. Until Task 6 those descended through
        // `Child::make_mut`, which ran the Task 4 stopgap
        // `BTreeNode::materialize` and de-blocked every leaf it handed
        // out: correct values, but the block (and with it the whole
        // memory-honesty win of the slice) was thrown away on first
        // write.
        //
        // These pin both halves, exactly like the immutable-path tests
        // above: the values are right, and the mutated leaves are STILL
        // block-backed. The clone counter additionally pins the in-place
        // path's own cost model, which is *better* than the immutable
        // one: a uniquely-owned block leaf is edited by moving values
        // (or, for a replace, by a single O(1) slot store), so it costs
        // **zero** `clone_value` calls; only a leaf still shared with an
        // older snapshot pays, and it pays exactly once per entry, in
        // the CoW.
        // -------------------------------------------------------------

        /// Entry counts of every leaf, left to right — lets the rebalance
        /// tests below assert the *pre*-state they depend on instead of
        /// silently going vacuous if the tree shape ever changes.
        fn leaf_sizes(t: &BTree<u64, u64>) -> Vec<usize> {
            fn go(c: &Child<u64, u64>, src: Option<&dyn NodeSource<u64, u64>>, out: &mut Vec<usize>) {
                let n = c.load(src);
                if n.children.is_empty() {
                    out.push(n.entries.len());
                } else {
                    for i in 0..n.children.len() {
                        go(&n.children[i], src, out);
                    }
                }
            }
            let mut out = Vec::new();
            go(&t.root, t.source(), &mut out);
            out
        }

        /// The O(1) hot path: an update of an existing key on a
        /// uniquely-owned block leaf stores straight into `block[pos]` —
        /// no rebuild, no reallocation, and **zero** clones (contrast the
        /// immutable path's n-1, which is inherent to CoW-ing a node it
        /// only has a `&` to).
        #[test]
        fn in_place_replace_on_a_unique_block_leaf_is_zero_clone() {
            let disk = Arc::new(MockDisk::new());
            let mut t = blockify(&insert_range(1, 40), &disk);
            let before = cloned(&disk);
            let leaf_ptr = t.root.load(t.source()) as *const BTreeNode<u64, u64>;
            t.insert_mut(7, 999);
            assert_eq!(cloned(&disk) - before, 0, "an in-place replace must not clone any value");
            assert_eq!(
                t.root.load(t.source()) as *const BTreeNode<u64, u64>,
                leaf_ptr,
                "a uniquely-owned leaf is edited in place, not reallocated"
            );
            assert_eq!(t.get(&7), Some(&999));
            assert_eq!(t.len(), 40);
            for k in (1..=40u64).filter(|k| *k != 7) {
                assert_eq!(t.get(&k), Some(&(k * 10)), "key {k}");
            }
            assert_eq!(
                walk_representation(&t),
                Repr { leaves: 1, block_leaves: 1 },
                "an in-place replace must leave the leaf block-backed"
            );
        }

        /// The same replace against a leaf still SHARED with an older
        /// snapshot: the CoW clones the block once per entry (that is
        /// `clone_with`, not the edit), the old snapshot keeps its own
        /// values, and both trees stay block-backed.
        #[test]
        fn in_place_replace_on_a_shared_block_leaf_cows_via_the_source() {
            let disk = Arc::new(MockDisk::new());
            let mut t = blockify(&insert_range(1, 40), &disk);
            let snapshot = t.clone(); // second owner of the leaf
            let before = cloned(&disk);
            t.insert_mut(7, 999);
            assert_eq!(
                cloned(&disk) - before,
                40,
                "CoW of a shared block leaf clones exactly one value per entry"
            );
            assert_eq!(t.get(&7), Some(&999));
            assert_eq!(snapshot.get(&7), Some(&70), "the older snapshot must not see the write");
            assert_eq!(walk_representation(&t), Repr { leaves: 1, block_leaves: 1 });
            assert_eq!(walk_representation(&snapshot), Repr { leaves: 1, block_leaves: 1 });
        }

        /// An in-place insert of a new key changes the block's length, so
        /// it rebuilds — but from a uniquely-owned node, so the surviving
        /// values are *moved*, not cloned.
        #[test]
        fn in_place_insert_keeps_the_leaf_block_backed() {
            let disk = Arc::new(MockDisk::new());
            let mut t = blockify(&insert_range(1, 40), &disk);
            let before = cloned(&disk);
            t.insert_mut(0, 0);
            assert_eq!(cloned(&disk) - before, 0, "an in-place insert must move values, not clone them");
            assert_eq!(t.len(), 41);
            assert_eq!(t.get(&0), Some(&0));
            read_all(&t, 1..=40);
            assert_eq!(walk_representation(&t), Repr { leaves: 1, block_leaves: 1 });
        }

        /// The overflow case: `maybe_split_mut` splits a block leaf into
        /// two block leaves, promoting an Arc-backed median (separators
        /// live in inner nodes — I-B).
        #[test]
        fn in_place_insert_splits_a_block_leaf_into_two_block_leaves() {
            let disk = Arc::new(MockDisk::new());
            let base = insert_range(1, MAX_KEYS as u64);
            assert_eq!(base.height(), 0, "MAX_KEYS rows must still be a single leaf");
            let mut t = blockify(&base, &disk);
            let before = cloned(&disk);
            t.insert_mut(0, 0);
            assert_eq!(cloned(&disk) - before, 0, "an in-place split must move values, not clone them");
            assert_eq!(t.height(), 1, "the leaf must have split");
            assert_eq!(t.len(), MAX_KEYS + 1);
            assert_eq!(t.get(&0), Some(&0));
            read_all(&t, 1..=MAX_KEYS as u64);
            assert_eq!(
                walk_representation(&t),
                Repr { leaves: 2, block_leaves: 2 },
                "both halves of an in-place split must be block-backed"
            );
            check_invariants(&t);
        }

        /// An in-place delete that does not underflow: one rebuild by
        /// move, still block-backed.
        #[test]
        fn in_place_delete_keeps_the_leaf_block_backed() {
            let disk = Arc::new(MockDisk::new());
            let mut t = blockify(&insert_range(1, 40), &disk);
            let before = cloned(&disk);
            assert!(t.remove_mut(&7));
            assert_eq!(cloned(&disk) - before, 0, "an in-place delete must move values, not clone them");
            assert_eq!(t.len(), 39);
            assert_eq!(t.get(&7), None);
            for k in (1..=40u64).filter(|k| *k != 7) {
                assert_eq!(t.get(&k), Some(&(k * 10)), "key {k}");
            }
            assert_eq!(walk_representation(&t), Repr { leaves: 1, block_leaves: 1 });
        }

        /// An in-place delete that underflows a leaf whose sibling has a
        /// surplus: `fix_underfull_child` -> `rotate_right`. Both leaves
        /// survive (no merge) and both stay block-backed.
        #[test]
        fn in_place_delete_underflow_rotates_between_block_leaves() {
            let disk = Arc::new(MockDisk::new());
            let base = insert_range(1, MAX_KEYS as u64 + 1);
            assert_eq!(base.height(), 1, "two leaves under one root");
            let mut t = blockify(&base, &disk);
            let sizes = leaf_sizes(&t);
            assert_eq!(sizes.len(), 2);
            assert!(sizes[0] > MIN_KEYS, "the left sibling must have a surplus to rotate from: {sizes:?}");
            assert_eq!(sizes[1], MIN_KEYS, "the right leaf must underflow on one delete: {sizes:?}");
            assert_eq!(walk_representation(&t).block_leaves, 2);

            // Delete the max key: the right leaf drops below MIN_KEYS and
            // steals the left sibling's last entry through the parent.
            let max = MAX_KEYS as u64 + 1;
            assert!(t.remove_mut(&max));
            assert_eq!(
                walk_representation(&t),
                Repr { leaves: 2, block_leaves: 2 },
                "a rotation must keep two leaves, both block-backed"
            );
            assert_eq!(leaf_sizes(&t), vec![sizes[0] - 1, MIN_KEYS], "one entry rotated across");
            assert_eq!(t.len(), MAX_KEYS);
            read_all(&t, 1..max);
            check_invariants(&t);
        }

        /// An in-place delete that underflows a leaf whose sibling has no
        /// surplus: `fix_underfull_child` -> `merge_with_*` -> `absorb`.
        /// One fresh block leaf holding both sides plus the descended
        /// separator.
        #[test]
        fn in_place_delete_underflow_merges_block_leaves() {
            let disk = Arc::new(MockDisk::new());
            let base = insert_range(1, MAX_KEYS as u64 + 1);
            assert_eq!(base.height(), 1);
            let mut t = blockify(&base, &disk);
            assert_eq!(walk_representation(&t).block_leaves, 2);
            let mut alive: Vec<u64> = (1..=MAX_KEYS as u64 + 1).collect();
            let mut k = 1u64;
            loop {
                // Checked EVERY round, not just at the end (review round 1,
                // Minor 1). The final assertion alone is insensitive: the
                // pre-Task-6 behaviour de-blocked only the leaf being
                // deleted from, and `absorb` re-blocks a merge whose OTHER
                // side is still block-shaped — so a whole run of de-blocking
                // deletes sails through a check made after the merge. Here
                // the very first delete's de-blocking is caught.
                let r = walk_representation(&t);
                assert_eq!(
                    r.block_leaves, r.leaves,
                    "every leaf must stay block-backed while deleting towards the merge (after deleting {} keys)",
                    k - 1
                );
                if r.leaves == 1 {
                    break;
                }
                assert!(t.remove_mut(&k));
                alive.retain(|x| *x != k);
                k += 1;
                assert!(k <= MAX_KEYS as u64, "the leaves must merge before running out of keys");
            }
            assert_eq!(
                walk_representation(&t),
                Repr { leaves: 1, block_leaves: 1 },
                "an in-place merge of block leaves must yield a block leaf"
            );
            assert_eq!(t.len(), alive.len());
            for key in alive {
                assert_eq!(t.get(&key), Some(&(key * 10)), "key {key}");
            }
            check_invariants(&t);
        }

        /// Deleting a key stored in an *inner* node lifts the in-order
        /// successor out of a block leaf through `remove_leftmost_mut`;
        /// it lands in an inner node, so its value has to leave the block
        /// and be re-homed into an `Arc` (I-B).
        #[test]
        fn in_place_delete_of_an_inner_separator_rehomes_the_successor() {
            let disk = Arc::new(MockDisk::new());
            let base = insert_range(1, DEEP);
            let sep = {
                let root = base.root.load(None);
                assert!(!root.children.is_empty(), "a deep tree's root is internal");
                root.entries[0].0
            };
            let mut t = blockify(&base, &disk);
            assert!(t.remove_mut(&sep));
            assert_eq!(t.get(&sep), None);
            assert_eq!(t.len() as u64, DEEP - 1);
            read_all(&t, (1..=DEEP).filter(move |k| *k != sep));
            let r = walk_representation(&t);
            assert_eq!(r.block_leaves, r.leaves, "every leaf stays block-backed");
            check_invariants(&t);
        }

        /// The in-place counterpart of
        /// `immutable_mixed_workload_matches_a_plain_tree_and_stays_block_backed`:
        /// a long `insert_mut`/`remove_mut` sequence — the same calls
        /// `Table` makes — against a block-backed tree and a plain
        /// all-Arc tree must agree key for key, keep every structural
        /// invariant, and leave every leaf block-backed.
        #[test]
        fn in_place_mixed_workload_matches_a_plain_tree_and_stays_block_backed() {
            let disk = Arc::new(MockDisk::new());
            let (rows, rounds, space) = if cfg!(miri) { (200u64, 150u64, 250u64) } else { (2_000, 1_500, 2_500) };
            let mut blocked = blockify(&insert_range(1, rows), &disk);
            let mut plain = insert_range(1, rows);
            let mut x: u64 = 0x9E3779B97F4A7C15;
            let mut next = || {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x
            };
            for _ in 0..rounds {
                let r = next();
                let key = r % space;
                match r % 3 {
                    0 => {
                        blocked.insert_mut(key, key * 10);
                        plain = plain.insert(key, key * 10);
                    }
                    1 => {
                        blocked.insert_mut(key, key * 10 + 1);
                        plain = plain.insert(key, key * 10 + 1);
                    }
                    _ => {
                        let hit = plain.get(&key).is_some();
                        assert_eq!(blocked.remove_mut(&key), hit, "remove_mut disagreed on key {key}");
                        if hit {
                            plain = plain.remove(&key).unwrap();
                        }
                    }
                }
            }
            assert_eq!(blocked.len(), plain.len());
            let a: Vec<(u64, u64)> = blocked.range(..).map(|(k, v)| (*k, *v)).collect();
            let b: Vec<(u64, u64)> = plain.range(..).map(|(k, v)| (*k, *v)).collect();
            assert_eq!(a, b, "block-backed tree diverged from the all-Arc oracle");
            check_invariants(&blocked);
            let r = walk_representation(&blocked);
            assert_eq!(
                r.block_leaves, r.leaves,
                "every leaf must still be block-backed after a mixed in-place workload"
            );
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
                    block: None,
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
            let (t2, demoted, demoted_bytes, cursor) = t.demote_leaves(None, usize::MAX);
            assert!(cursor.is_none());
            assert!(demoted > 300, "20k rows ≈ 318 leaves, all quiet");
            assert!(demoted_bytes > 0, "a real demote pass must report nonzero bytes");
            assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), 0, "demotion loads nothing");
            for k in [1u64, 2, 3, 10_000, 19_999] {
                assert_eq!(t2.get(&k), Some(&k));
            }
            // 1,2,3 share a leaf; 10_000 and 19_999 are two more: 3 leaves.
            assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), 3);
            // Old version untouched and fully resident.
            assert!(t.resident_leaf_estimate() > t2.resident_leaf_estimate());
        }

        /// Task 9 review I-2: an already-`seen` INNER slot must prune its
        /// whole subtree, not just skip leaves one at a time — the cost
        /// property spec §5 claims ("CoW sharing means most of an older
        /// snapshot's walk retraces pointers the newer walk already marked
        /// seen"). A fully in-memory tree (no `NodeSource` attached) has
        /// every `Child` always resident, so `resident_ptr()` is `Some`
        /// everywhere — no paged store needed to exercise this.
        #[test]
        fn resident_leaf_bytes_dedup_prunes_already_seen_subtrees() {
            let t = tree(20_000);
            let mut seen: std::collections::HashSet<*const ()> = std::collections::HashSet::new();
            let (bytes1, visited1) = t.resident_leaf_bytes_dedup_with_visits(&mut seen);
            assert!(bytes1 > 0, "a real tree must report nonzero resident bytes");
            assert!(visited1 > 300, "20k rows visits every leaf (≈318) plus every inner level at least once");

            // Re-walk the SAME tree against the SAME (now fully populated)
            // `seen` set: every node, leaf and inner, was already seen on
            // the first walk, so a pruning walk must stop at the root
            // without descending into a single child.
            let (bytes2, visited2) = t.resident_leaf_bytes_dedup_with_visits(&mut seen);
            assert_eq!(bytes2, 0, "everything already counted once");
            assert_eq!(visited2, 1, "the root is `seen`; pruned before any child is even loaded");
            assert!(
                visited2 * 100 < visited1,
                "second walk (visited={visited2}) must visit far fewer slots than the first                  (visited={visited1}) -- a walk that only dedups leaves (no inner-node pruning)                  would still visit every inner node on the second pass too"
            );
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
            let (t2, _d1, _b1, _) = t.demote_leaves(None, usize::MAX);
            // Task 8: `resident_leaf_estimate` now sums `leaf_bytes()`
            // (`NODE_BYTES` + one `size_of::<V>()` per entry), not flat
            // `NODE_BYTES` — the leftmost leaf `from_sorted` packs densely
            // (see `from_sorted_tail_underfull`'s doc: only the *tail* leaf
            // is left underfull), so it holds exactly `MAX_KEYS` entries.
            assert_eq!(
                t2.resident_leaf_estimate(),
                Child::<u64, u64>::NODE_BYTES + MAX_KEYS * std::mem::size_of::<u64>(),
                "exactly the accessed leaf survived pass 1"
            );
            assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), 0);
            let (t3, d2, b2, _) = t2.demote_leaves(None, usize::MAX);
            assert_eq!(d2, 1, "second pass takes it");
            assert_eq!(
                b2,
                Child::<u64, u64>::NODE_BYTES + MAX_KEYS * std::mem::size_of::<u64>(),
                "the one demoted leaf's bytes match its own leaf_bytes(), same as the estimate above"
            );
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
            let (t2, d1, b1, c1) = t.demote_leaves(None, 2);
            assert!(c1.is_some() && d1 <= 2 * 64);
            assert!(b1 > 0, "a real demote pass must report nonzero bytes");
            let (t3, d2, b2, c2) = t2.demote_leaves(c1.as_ref(), usize::MAX);
            assert!(c2.is_none());
            assert!(d1 + d2 > 300);
            assert!(b1 + b2 > 0, "cumulative bytes must be nonzero too");
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
            let (t2, demoted, demoted_bytes, _) = t.demote_leaves(None, usize::MAX);
            assert_eq!(demoted, 0, "every leaf was accessed; none should be demoted");
            assert_eq!(demoted_bytes, 0, "nothing demoted, nothing debited");
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

        /// `load_all` (Task 13's attach path) faults in every node — inner
        /// levels *and* leaves — unlike `load_inner_levels`, which stops
        /// one level short. Every slot must be `is_loaded()` afterward, and
        /// a subsequent `get` must not fault anything further.
        #[test]
        fn load_all_faults_every_node_including_leaves() {
            let disk = Arc::new(MockDisk::new());
            let t = tree(20_000);
            let mut next = 0;
            let root = flush(&t, &disk, &mut next);
            let t2: BTree<u64, u64> = BTree::from_root_page(root, 20_000, t.height(), disk.clone());
            t2.load_all();

            fn check(slot: &Child<u64, u64>) {
                assert!(slot.is_loaded(), "load_all must leave nothing on disk, leaves included");
            }
            fn walk(slot: &Child<u64, u64>, depth: usize, src: Option<&dyn NodeSource<u64, u64>>) {
                check(slot);
                if depth == 0 {
                    return;
                }
                let n = slot.load_quiet(src);
                for c in n.children.iter() {
                    walk(c, depth - 1, src);
                }
            }
            walk(&t2.root, t2.height(), t2.source.as_deref());

            let reads_before = disk.reads.load(std::sync::atomic::Ordering::Relaxed);
            assert_eq!(t2.get(&123), Some(&123));
            assert_eq!(
                disk.reads.load(std::sync::atomic::Ordering::Relaxed),
                reads_before,
                "every node is already resident; a read after load_all must fault nothing"
            );
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
            let (t2, da, ba, _) = t.demote_leaves(None, usize::MAX);
            assert!(da > 300, "everything but the two touched leaves demotes");
            assert!(ba > 0, "a real demote pass must report nonzero bytes");
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
            let (t3, db, bb, _) = t2.demote_leaves(None, usize::MAX);
            assert_eq!(db, 2, "exactly the two leaves the walks must not have re-armed");
            assert!(bb > 0, "a real demote pass must report nonzero bytes");
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
            let (mut t2, demoted, demoted_bytes, _) = t.demote_leaves(None, usize::MAX);
            assert!(demoted > 0, "need at least one on-disk leaf for this test to mean anything");
            assert!(demoted_bytes > 0, "a real demote pass must report nonzero bytes");
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
