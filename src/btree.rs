// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::{Error, Result};

// Minimum degree: every non-root node has at least T-1 keys, at most 2T-1 keys.
// T=32: under the SMR apply regime (one op per commit, retained snapshots + live
// readers, so every commit clones the full root-to-leaf path and make_mut never
// fires) T=32 beat T=64 on apply p99 (-33%), apply throughput (+22%) and
// read-p99-under-load (-30%) in a same-day A/B (autoresearch/smr-apply-jul17,
// 2026-07-17). The opposite held on a 1M-key mixed workload where in-place
// rebalancing (task50 §5.1) amortizes clone costs — there T=64 won all axes
// (see docs/superpowers/specs/2026-07-08-btree-optimization-candidates.md §2);
// revisit if that regime becomes the priority.
const T: usize = 32;
const MIN_KEYS: usize = T - 1;
const MAX_KEYS: usize = 2 * T - 1;

// ---------------------------------------------------------------------------
// Internal node type
// ---------------------------------------------------------------------------

struct BTreeNode<K, V> {
    /// Key-value pairs stored in sorted order.
    entries: Vec<(K, Arc<V>)>,
    /// Children; empty for leaf nodes, len == entries.len() + 1 for internal nodes.
    children: Vec<Arc<BTreeNode<K, V>>>,
}

// Manual `Clone` bounded on `K: Clone` only. A `#[derive(Clone)]` would add a
// spurious `V: Clone` bound; here values live behind `Arc<V>` and children behind
// `Arc<BTreeNode>`, so cloning a node only clones the keys and bumps refcounts —
// `V` is never cloned. This impl is what makes `Arc::make_mut` usable on the
// in-place insert path (`insert_mut`) without imposing `V: Clone` on callers.
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
    root: Arc<BTreeNode<K, V>>,
    len: usize,
}

// ---------------------------------------------------------------------------
// Internal enums for recursive helpers
// ---------------------------------------------------------------------------

enum InsertResult<K, V> {
    Fit(Arc<BTreeNode<K, V>>, bool),
    Split {
        left: Arc<BTreeNode<K, V>>,
        median: (K, Arc<V>),
        right: Arc<BTreeNode<K, V>>,
        replaced: bool,
    },
}

enum DeleteResult<K, V> {
    NotFound,
    Removed {
        node: Arc<BTreeNode<K, V>>,
        underfull: bool,
    },
}

/// Outcome of an in-place delete into a node (see `delete_from_node_mut`).
/// The mutated node flows back through the `&mut Arc<BTreeNode>` the caller
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
            root: Arc::new(BTreeNode {
                entries: vec![],
                children: vec![],
            }),
            len: 0,
        }
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
        get_in_node(&self.root, key)
    }

    /// Look up a key and return a shared handle to the value.
    pub fn get_arc(&self, key: &K) -> Option<Arc<V>> {
        get_arc_in_node(&self.root, key)
    }

    /// The largest key in the tree (rightmost leaf's last entry), or `None`
    /// if empty. O(height); used by `Table::insert_batch` to verify the
    /// append invariant before taking the bulk fast path.
    pub(crate) fn max_key(&self) -> Option<&K> {
        let mut node = &self.root;
        loop {
            match node.children.last() {
                Some(c) => node = c,
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
        match insert_into_node(&self.root, key, val_arc) {
            InsertResult::Fit(new_root, replaced) => {
                let new_len = if replaced { self.len } else { self.len + 1 };
                BTree {
                    root: new_root,
                    len: new_len,
                }
            }
            InsertResult::Split {
                left,
                median,
                right,
                replaced,
            } => {
                let new_len = if replaced { self.len } else { self.len + 1 };
                let new_root = Arc::new(BTreeNode {
                    entries: vec![median],
                    children: vec![left, right],
                });
                BTree {
                    root: new_root,
                    len: new_len,
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
    /// [`Arc::make_mut`], which clones a node **only** when it is actually shared
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
        match insert_into_node_mut(&mut self.root, key, val_arc) {
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
                let left = std::mem::replace(
                    &mut self.root,
                    Arc::new(BTreeNode {
                        entries: vec![],
                        children: vec![],
                    }),
                );
                self.root = Arc::new(BTreeNode {
                    entries: vec![median],
                    children: vec![left, right],
                });
            }
        }
    }

    /// Remove a key. Returns a new tree, or `Err(KeyNotFound)` if the key is
    /// absent. `self` is unchanged.
    pub fn remove(&self, key: &K) -> Result<BTree<K, V>> {
        match delete_from_node(&self.root, key) {
            DeleteResult::NotFound => Err(Error::KeyNotFound),
            DeleteResult::Removed { node: new_root, .. } => {
                // If the root is now an internal node with no entries but one
                // child, collapse the tree height by one.
                let actual_root = if new_root.entries.is_empty() && !new_root.children.is_empty() {
                    Arc::clone(&new_root.children[0])
                } else {
                    new_root
                };
                Ok(BTree {
                    root: actual_root,
                    len: self.len - 1,
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
        match delete_from_node_mut(&mut self.root, key) {
            DeleteOutcome::NotFound => false,
            DeleteOutcome::Removed { .. } => {
                self.len -= 1;
                // Root collapse: an internal root left with no entries and one
                // child drops a level. Move that child up (no clone).
                if self.root.entries.is_empty() && !self.root.children.is_empty() {
                    let child = {
                        let root = Arc::make_mut(&mut self.root);
                        root.children.remove(0)
                    };
                    self.root = child;
                }
                true
            }
        }
    }

    /// Iterate over `(&K, &V)` pairs in ascending key order within `range`.
    pub fn range<'a>(&'a self, range: impl RangeBounds<K> + 'a) -> BTreeRange<'a, K, V> {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        let mut iter = BTreeRange {
            stack: vec![],
            back_stack: vec![],
            start: start.clone(),
            end: end.clone(),
            done: false,
            last_forward: None,
            last_backward: None,
        };
        iter.descend_left_from(&self.root, &start);
        iter.descend_right_from(&self.root, &end);
        iter
    }
}

impl<K, V> Clone for BTree<K, V> {
    /// O(1): increments the root `Arc` reference count.
    fn clone(&self) -> Self {
        BTree {
            root: Arc::clone(&self.root),
            len: self.len,
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

/// Iterator over key-value pairs in a `BTree<K, V>` within a key range.
///
/// Supports both forward (`Iterator`) and backward (`DoubleEndedIterator`) traversal,
/// as well as mixed forward/backward iteration.
pub struct BTreeRange<'a, K, V> {
    /// Stack of (node, next-entry-index-to-yield) frames for forward traversal.
    stack: Vec<(&'a BTreeNode<K, V>, usize)>,
    /// Stack of (node, one-past-last-entry-index) frames for backward traversal.
    back_stack: Vec<(&'a BTreeNode<K, V>, usize)>,
    /// Start bound (needed for backward bound checking).
    start: Bound<K>,
    /// End bound (needed for forward bound checking).
    end: Bound<K>,
    /// Set to true when the forward and backward iterators have met, or a bound
    /// is violated — no further items should be yielded.
    done: bool,
    /// Last key yielded by `next()` — used for overlap detection with `next_back()`.
    last_forward: Option<&'a K>,
    /// Last key yielded by `next_back()` — used for overlap detection with `next()`.
    last_backward: Option<&'a K>,
}

impl<'a, K: Ord + Clone, V> BTreeRange<'a, K, V> {
    /// Push stack frames for the leftmost path that is >= `start`.
    fn descend_left_from(&mut self, node: &'a Arc<BTreeNode<K, V>>, start: &Bound<K>) {
        let n = node.as_ref();
        let entry_start = match start {
            Bound::Unbounded => 0,
            Bound::Included(k) => n.entries.partition_point(|(ek, _)| ek < k),
            Bound::Excluded(k) => n.entries.partition_point(|(ek, _)| ek <= k),
        };
        self.stack.push((n, entry_start));
        if !n.children.is_empty() && entry_start < n.children.len() {
            self.descend_left_from(&n.children[entry_start], start);
        }
    }

    /// Push stack frames for the leftmost leaf of `node` (no range restriction).
    fn descend_leftmost(&mut self, node: &'a Arc<BTreeNode<K, V>>) {
        let n = node.as_ref();
        self.stack.push((n, 0));
        if !n.children.is_empty() {
            self.descend_leftmost(&n.children[0]);
        }
    }

    fn in_end_bound(&self, key: &K) -> bool {
        match &self.end {
            Bound::Unbounded => true,
            Bound::Included(k) => key <= k,
            Bound::Excluded(k) => key < k,
        }
    }

    /// Push back_stack frames for the rightmost path that is <= `end`.
    fn descend_right_from(&mut self, node: &'a Arc<BTreeNode<K, V>>, end: &Bound<K>) {
        let n = node.as_ref();
        // `entry_end` = one past the last valid index for backward iteration.
        let entry_end = match end {
            Bound::Unbounded => n.entries.len(),
            Bound::Included(k) => n.entries.partition_point(|(ek, _)| ek <= k),
            Bound::Excluded(k) => n.entries.partition_point(|(ek, _)| ek < k),
        };
        self.back_stack.push((n, entry_end));
        if !n.children.is_empty() && entry_end < n.children.len() {
            self.descend_right_from(&n.children[entry_end], end);
        }
    }

    /// Push back_stack frames for the rightmost leaf of `node` (no range restriction).
    fn descend_rightmost(&mut self, node: &'a Arc<BTreeNode<K, V>>) {
        let n = node.as_ref();
        self.back_stack.push((n, n.entries.len()));
        if !n.children.is_empty() {
            self.descend_rightmost(n.children.last().unwrap());
        }
    }

    fn in_start_bound(&self, key: &K) -> bool {
        match &self.start {
            Bound::Unbounded => true,
            Bound::Included(k) => key >= k,
            Bound::Excluded(k) => key > k,
        }
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
fn get_in_node<'a, K: Ord, V>(node: &'a BTreeNode<K, V>, key: &K) -> Option<&'a V> {
    match node.entries.binary_search_by(|(k, _)| k.cmp(key)) {
        Ok(pos) => Some(&*node.entries[pos].1),
        Err(pos) => {
            if node.children.is_empty() {
                None
            } else {
                get_in_node(&node.children[pos], key)
            }
        }
    }
}

/// Recursively searches for a key in a node. Returns a shared handle to the value.
fn get_arc_in_node<K: Ord, V>(node: &BTreeNode<K, V>, key: &K) -> Option<Arc<V>> {
    match node.entries.binary_search_by(|(k, _)| k.cmp(key)) {
        Ok(pos) => Some(Arc::clone(&node.entries[pos].1)),
        Err(pos) => {
            if node.children.is_empty() {
                None
            } else {
                get_arc_in_node(&node.children[pos], key)
            }
        }
    }
}

/// Recursively inserts a key-value pair into a node, potentially splitting it.
fn insert_into_node<K: Ord + Clone, V>(
    node: &Arc<BTreeNode<K, V>>,
    key: K,
    val: Arc<V>,
) -> InsertResult<K, V> {
    let mut entries = node.entries.clone();

    match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
        Ok(pos) => {
            // Replace existing value.
            entries[pos] = (key, val);
            let children = node.children.clone();
            InsertResult::Fit(Arc::new(BTreeNode { entries, children }), true)
        }
        Err(pos) => {
            if node.children.is_empty() {
                // Leaf: insert and possibly split.
                entries.insert(pos, (key, val));
                maybe_split(entries, vec![], false)
            } else {
                // Internal: recurse into child[pos], then merge the result.
                let mut children = node.children.clone();
                match insert_into_node(&children[pos], key, val) {
                    InsertResult::Fit(new_child, replaced) => {
                        children[pos] = new_child;
                        InsertResult::Fit(Arc::new(BTreeNode { entries, children }), replaced)
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
                        maybe_split(entries, children, replaced)
                    }
                }
            }
        }
    }
}

/// Wrap entries+children into a node, splitting if entries exceed MAX_KEYS.
fn maybe_split<K: Clone, V>(
    entries: Vec<(K, Arc<V>)>,
    children: Vec<Arc<BTreeNode<K, V>>>,
    replaced: bool,
) -> InsertResult<K, V> {
    if entries.len() <= MAX_KEYS {
        InsertResult::Fit(Arc::new(BTreeNode { entries, children }), replaced)
    } else {
        // entries.len() == MAX_KEYS + 1 == 64; split at mid == 32.
        let mid = entries.len() / 2;
        let median = entries[mid].clone();
        let right_entries = entries[mid + 1..].to_vec();
        let left_entries = entries[..mid].to_vec();

        let (left_children, right_children) = if children.is_empty() {
            (vec![], vec![])
        } else {
            (children[..mid + 1].to_vec(), children[mid + 1..].to_vec())
        };

        InsertResult::Split {
            left: Arc::new(BTreeNode {
                entries: left_entries,
                children: left_children,
            }),
            median,
            right: Arc::new(BTreeNode {
                entries: right_entries,
                children: right_children,
            }),
            replaced,
        }
    }
}

/// Outcome of an in-place insert into a node (see `insert_into_node_mut`).
///
/// Unlike `InsertResult`, this does not carry the mutated node — the node is
/// updated in place through the `&mut Arc<BTreeNode>` the caller passed. Only the
/// promoted median + new right sibling (on split) and the replace/insert flag
/// need to flow back up.
enum InsertOutcome<K, V> {
    Fit {
        replaced: bool,
    },
    Split {
        median: (K, Arc<V>),
        right: Arc<BTreeNode<K, V>>,
        replaced: bool,
    },
}

/// In-place counterpart to `insert_into_node`. Descends through
/// `Arc::make_mut`, so each node is cloned only if it is still shared with
/// another snapshot (copy-on-write preserved) and otherwise mutated directly.
fn insert_into_node_mut<K: Ord + Clone, V>(
    node: &mut Arc<BTreeNode<K, V>>,
    key: K,
    val: Arc<V>,
) -> InsertOutcome<K, V> {
    // The one place CoW happens on this path: clones iff `node` is shared.
    let n = Arc::make_mut(node);

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
                        right,
                        replaced: false,
                    },
                }
            } else {
                // Internal: recurse into child[pos], then absorb the result.
                match insert_into_node_mut(&mut n.children[pos], key, val) {
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
                                right,
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
/// Uses `split_off`/`pop` so the left half reuses `n`'s existing allocation —
/// only the right sibling allocates. (The immutable `maybe_split` allocates three
/// fresh Vecs via `to_vec`.)
#[allow(clippy::type_complexity)]
fn maybe_split_mut<K: Clone, V>(
    n: &mut BTreeNode<K, V>,
) -> Option<((K, Arc<V>), Arc<BTreeNode<K, V>>)> {
    if n.entries.len() <= MAX_KEYS {
        return None;
    }
    // entries.len() == MAX_KEYS + 1 == 64; split at mid == 32, matching the
    // immutable path exactly so both produce identically-shaped trees.
    let mid = n.entries.len() / 2;
    let right_entries = n.entries.split_off(mid + 1); // entries[mid+1..]
    let median = n.entries.pop().unwrap(); // entries[mid]
    // n.entries is now entries[..mid] (the left half).
    let right_children = if n.children.is_empty() {
        vec![]
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
fn delete_from_node<K: Ord + Clone, V>(node: &Arc<BTreeNode<K, V>>, key: &K) -> DeleteResult<K, V> {
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
                    node: Arc::new(BTreeNode {
                        entries,
                        children: vec![],
                    }),
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
                let (succ, new_right, right_underfull) = remove_leftmost(&node.children[i + 1]);
                let mut entries = node.entries.clone();
                let mut children = node.children.clone();
                entries[i] = succ;
                children[i + 1] = new_right;
                if right_underfull {
                    fix_underfull_child(&mut entries, &mut children, i + 1);
                }
                let underfull = entries.len() < MIN_KEYS;
                DeleteResult::Removed {
                    node: Arc::new(BTreeNode { entries, children }),
                    underfull,
                }
            }
            Err(child_idx) => {
                // Key is in a subtree.
                match delete_from_node(&node.children[child_idx], key) {
                    DeleteResult::NotFound => DeleteResult::NotFound,
                    DeleteResult::Removed {
                        node: new_child,
                        underfull,
                    } => {
                        let mut entries = node.entries.clone();
                        let mut children = node.children.clone();
                        children[child_idx] = new_child;
                        if underfull {
                            fix_underfull_child(&mut entries, &mut children, child_idx);
                        }
                        let node_underfull = entries.len() < MIN_KEYS;
                        DeleteResult::Removed {
                            node: Arc::new(BTreeNode { entries, children }),
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
    node: &Arc<BTreeNode<K, V>>,
) -> ((K, Arc<V>), Arc<BTreeNode<K, V>>, bool) {
    if node.children.is_empty() {
        let mut entries = node.entries.clone();
        let first = entries.remove(0);
        let underfull = entries.len() < MIN_KEYS;
        (
            first,
            Arc::new(BTreeNode {
                entries,
                children: vec![],
            }),
            underfull,
        )
    } else {
        let (entry, new_first_child, child_underfull) = remove_leftmost(&node.children[0]);
        let mut entries = node.entries.clone();
        let mut children = node.children.clone();
        children[0] = new_first_child;
        if child_underfull {
            fix_underfull_child(&mut entries, &mut children, 0);
        }
        let underfull = entries.len() < MIN_KEYS;
        (entry, Arc::new(BTreeNode { entries, children }), underfull)
    }
}

/// Rebalance an underfull child at `idx` by rotating from a sibling or merging.
fn fix_underfull_child<K: Ord + Clone, V>(
    entries: &mut Vec<(K, Arc<V>)>,
    children: &mut Vec<Arc<BTreeNode<K, V>>>,
    idx: usize,
) {
    if idx > 0 && children[idx - 1].entries.len() > MIN_KEYS {
        rotate_right(entries, children, idx);
    } else if idx + 1 < children.len() && children[idx + 1].entries.len() > MIN_KEYS {
        rotate_left(entries, children, idx);
    } else if idx > 0 {
        merge_with_left(entries, children, idx);
    } else {
        merge_with_right(entries, children, idx);
    }
}

/// Rotates an entry from the left sibling into the current child.
///
/// Mutates the two siblings in place via [`Arc::make_mut`]: each is cloned only
/// if still shared with an older snapshot, otherwise edited directly. `split_at_mut`
/// yields disjoint `&mut` handles to the two adjacent children at once.
fn rotate_right<K: Clone, V>(
    entries: &mut [(K, Arc<V>)],
    children: &mut [Arc<BTreeNode<K, V>>],
    idx: usize,
) {
    let (left_part, right_part) = children.split_at_mut(idx);
    let left = Arc::make_mut(&mut left_part[idx - 1]);
    let right = Arc::make_mut(&mut right_part[0]);

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
    entries: &mut [(K, Arc<V>)],
    children: &mut [Arc<BTreeNode<K, V>>],
    idx: usize,
) {
    let (left_part, right_part) = children.split_at_mut(idx + 1);
    let left = Arc::make_mut(&mut left_part[idx]);
    let right = Arc::make_mut(&mut right_part[0]);

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
/// The absorbing (left) sibling is opened with [`Arc::make_mut`] and edited in
/// place; the absorbed (right) node's contents are **moved** into it via
/// [`Arc::try_unwrap`] when it is uniquely owned, falling back to a clone only
/// when it is still shared with a snapshot.
fn merge_with_left<K: Clone, V>(
    entries: &mut Vec<(K, Arc<V>)>,
    children: &mut Vec<Arc<BTreeNode<K, V>>>,
    idx: usize,
) {
    let separator = entries.remove(idx - 1);
    let right = children.remove(idx);
    let left = Arc::make_mut(&mut children[idx - 1]);
    left.entries.push(separator);
    absorb(left, right);
}

/// Merges an underfull child with its right sibling.
///
/// In-place counterpart of `merge_with_left` — see its docs.
fn merge_with_right<K: Clone, V>(
    entries: &mut Vec<(K, Arc<V>)>,
    children: &mut Vec<Arc<BTreeNode<K, V>>>,
    idx: usize,
) {
    let separator = entries.remove(idx);
    let right = children.remove(idx + 1);
    let left = Arc::make_mut(&mut children[idx]);
    left.entries.push(separator);
    absorb(left, right);
}

/// Appends `right`'s entries and children onto `left`, moving them out of
/// `right` when it is uniquely owned (no snapshot shares it) and cloning
/// otherwise. `left` must already carry the descended separator as its last
/// entry.
fn absorb<K: Clone, V>(left: &mut BTreeNode<K, V>, right: Arc<BTreeNode<K, V>>) {
    match Arc::try_unwrap(right) {
        Ok(rn) => {
            left.entries.extend(rn.entries);
            left.children.extend(rn.children);
        }
        Err(shared) => {
            left.entries.extend(shared.entries.iter().cloned());
            left.children.extend(shared.children.iter().cloned());
        }
    }
}

/// In-place counterpart to `delete_from_node`. Descends through
/// `Arc::make_mut`, so each node is cloned only if it is still shared with
/// another snapshot (copy-on-write preserved) and otherwise mutated directly.
/// Reuses the existing rebalance helpers (`fix_underfull_child` et al.),
/// which already mutate the parent's `entries`/`children` in place.
fn delete_from_node_mut<K: Ord + Clone, V>(
    node: &mut Arc<BTreeNode<K, V>>,
    key: &K,
) -> DeleteOutcome {
    let n = Arc::make_mut(node);
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
                let (succ, right_underfull) = remove_leftmost_mut(&mut n.children[i + 1]);
                n.entries[i] = succ;
                if right_underfull {
                    fix_underfull_child(&mut n.entries, &mut n.children, i + 1);
                }
                DeleteOutcome::Removed {
                    underfull: n.entries.len() < MIN_KEYS,
                }
            }
            Err(child_idx) => match delete_from_node_mut(&mut n.children[child_idx], key) {
                DeleteOutcome::NotFound => DeleteOutcome::NotFound,
                DeleteOutcome::Removed { underfull } => {
                    if underfull {
                        fix_underfull_child(&mut n.entries, &mut n.children, child_idx);
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
    node: &mut Arc<BTreeNode<K, V>>,
) -> ((K, Arc<V>), bool) {
    let n = Arc::make_mut(node);
    if n.children.is_empty() {
        let first = n.entries.remove(0);
        (first, n.entries.len() < MIN_KEYS)
    } else {
        let (entry, child_underfull) = remove_leftmost_mut(&mut n.children[0]);
        if child_underfull {
            fix_underfull_child(&mut n.entries, &mut n.children, 0);
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
    children: Vec<Arc<BTreeNode<K, V>>>,
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
}

impl<K: Ord + Clone, V> BulkBuilder<K, V> {
    fn new() -> Self {
        Self {
            levels: vec![LevelBuilder::new()],
            len: 0,
            last_key: None,
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
            return Self::new();
        }
        let mut spine: Vec<&Arc<BTreeNode<K, V>>> = Vec::new();
        let mut node = &tree.root;
        loop {
            spine.push(node);
            match node.children.last() {
                Some(c) => node = c,
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
                lv.children.extend(spine_node.children[..n - 1].iter().cloned());
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
            let frozen = freeze_leaf(&mut self.levels[0]);
            self.attach_child(1, frozen, k, v);
        } else {
            self.levels[0].entries.push((k, v));
        }
    }

    /// Attach `child` as the next child of `levels[level]`, using `(sep_k, sep_v)`
    /// as the separator placed *after* the previous child. If `levels[level]` is
    /// also at capacity, freeze and recurse to the level above.
    fn attach_child(&mut self, level: usize, child: Arc<BTreeNode<K, V>>, sep_k: K, sep_v: Arc<V>) {
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
            let frozen = freeze_internal(lv);
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
        let mut carry: Option<Arc<BTreeNode<K, V>>> = None;
        // Entries popped from a level that ended up "unclosed" (see below),
        // to be re-inserted individually once the tree is otherwise valid.
        let mut pending_reinsert: Vec<(K, Arc<V>)> = Vec::new();

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
                redistribute_tail::<K, V>(&mut self.levels, level);
            }

            let lv = &mut self.levels[level];
            if lv.entries.is_empty() && lv.children.is_empty() {
                continue;
            }

            let node = if is_leaf_level {
                Arc::new(BTreeNode {
                    entries: std::mem::take(&mut lv.entries),
                    children: vec![],
                })
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
                    Arc::new(BTreeNode { entries, children })
                }
            };
            carry = Some(node);
        }

        let mut root = carry.unwrap_or_else(|| {
            Arc::new(BTreeNode {
                entries: vec![],
                children: vec![],
            })
        });
        fix_right_spine_tail(&mut root);
        let len = self.len - pending_reinsert.len();
        let mut tree = BTree { root, len };
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
/// spine was freshly built by this `finish()` call, so `Arc::make_mut` never
/// clones.
///
/// Returns whether `node` itself is now underfull (ignored by the caller at
/// the root, which has no `MIN_KEYS` floor).
fn fix_right_spine_tail<K: Ord + Clone, V>(node: &mut Arc<BTreeNode<K, V>>) -> bool {
    let n = Arc::make_mut(node);
    if n.children.is_empty() {
        return n.entries.len() < MIN_KEYS;
    }
    let mut last = n.children.len() - 1;
    let child_underfull = fix_right_spine_tail(&mut n.children[last]);
    // A single-child node (no sibling of its own to rotate/merge with) has
    // no fix available at this level; the underflow just propagates to our
    // own `entries.len() < MIN_KEYS` check below, for our own parent to
    // handle against our (real) sibling.
    if child_underfull {
        while n.children.len() > 1 && n.children[last].entries.len() < MIN_KEYS {
            fix_underfull_child(&mut n.entries, &mut n.children, last);
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
fn redistribute_tail<K: Clone, V>(levels: &mut [LevelBuilder<K, V>], level: usize) {
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

    // Reconstruct the full ordered sequence: sibling.entries ++ separator ++ lv.entries.
    let mut merged_entries: Vec<(K, Arc<V>)> = sibling.entries.to_vec();
    merged_entries.push(separator);
    merged_entries.append(&mut lv.entries);

    let merged_children: Vec<Arc<BTreeNode<K, V>>> = if is_leaf_level {
        vec![]
    } else {
        let mut c: Vec<Arc<BTreeNode<K, V>>> = sibling.children.to_vec();
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

    let new_left = Arc::new(BTreeNode {
        entries: new_left_entries,
        children: new_left_children,
    });

    parent.children.push(new_left);
    parent.entries.push(new_separator);
    lv.entries = right_entries;
    lv.children = new_right_children;
}

fn freeze_leaf<K, V>(lv: &mut LevelBuilder<K, V>) -> Arc<BTreeNode<K, V>> {
    Arc::new(BTreeNode {
        entries: std::mem::take(&mut lv.entries),
        children: vec![],
    })
}

fn freeze_internal<K, V>(lv: &mut LevelBuilder<K, V>) -> Arc<BTreeNode<K, V>> {
    let entries = std::mem::take(&mut lv.entries);
    let children = std::mem::take(&mut lv.children);
    debug_assert_eq!(children.len(), entries.len() + 1);
    Arc::new(BTreeNode { entries, children })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
    /// `Arc::make_mut` / `Arc::try_unwrap`. When a sibling is still shared with
    /// an older snapshot it MUST be cloned, never mutated/moved in place —
    /// otherwise a merge would corrupt the snapshot. Deleting a long contiguous
    /// run from the low end forces repeated merges and rotations at every level
    /// while a snapshot holds those very siblings.
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
                for c in &node.children {
                    walk(c, false, depth + 1, leaf_depth, count);
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
            for w in node.entries.windows(2) {
                assert!(w[0].0 < w[1].0, "unsorted entries within node");
            }
            *count += node.entries.len();
        }
        let mut leaf_depth = None;
        let mut count = 0;
        walk(&t.root, true, 0, &mut leaf_depth, &mut count);
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
}
