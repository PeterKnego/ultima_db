use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::{Error, Result};

// Minimum degree: every non-root node has at least T-1 keys, at most 2T-1 keys.
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
    Removed { node: Arc<BTreeNode<K, V>>, underfull: bool },
}

// ---------------------------------------------------------------------------
// BTree impl
// ---------------------------------------------------------------------------

impl<K: Ord + Clone, V> BTree<K, V> {
    /// Creates a new, empty B-tree.
    pub fn new() -> Self {
        BTree {
            root: Arc::new(BTreeNode { entries: vec![], children: vec![] }),
            len: 0,
        }
    }

    /// Build a B-tree from a strictly-ascending iterator of `(K, Arc<V>)`
    /// pairs in O(N), packing leaves densely with `MAX_KEYS` entries each.
    ///
    /// Debug-asserts strict ascending order and rejects duplicate keys.
    /// Caller is responsible for sort and dedup.
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
                BTree { root: new_root, len: new_len }
            }
            InsertResult::Split { left, median, right, replaced } => {
                let new_len = if replaced { self.len } else { self.len + 1 };
                let new_root = Arc::new(BTreeNode {
                    entries: vec![median],
                    children: vec![left, right],
                });
                BTree { root: new_root, len: new_len }
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
                let actual_root =
                    if new_root.entries.is_empty() && !new_root.children.is_empty() {
                        Arc::clone(&new_root.children[0])
                    } else {
                        new_root
                    };
                Ok(BTree { root: actual_root, len: self.len - 1 })
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
        BTree { root: Arc::clone(&self.root), len: self.len }
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
fn insert_into_node<K: Ord + Clone, V>(node: &Arc<BTreeNode<K, V>>, key: K, val: Arc<V>) -> InsertResult<K, V> {
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
                    InsertResult::Split { left, median, right, replaced } => {
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
            left: Arc::new(BTreeNode { entries: left_entries, children: left_children }),
            median,
            right: Arc::new(BTreeNode { entries: right_entries, children: right_children }),
            replaced,
        }
    }
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
                    node: Arc::new(BTreeNode { entries, children: vec![] }),
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
                    DeleteResult::Removed { node: new_child, underfull } => {
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
fn remove_leftmost<K: Ord + Clone, V>(node: &Arc<BTreeNode<K, V>>) -> ((K, Arc<V>), Arc<BTreeNode<K, V>>, bool) {
    if node.children.is_empty() {
        let mut entries = node.entries.clone();
        let first = entries.remove(0);
        let underfull = entries.len() < MIN_KEYS;
        (first, Arc::new(BTreeNode { entries, children: vec![] }), underfull)
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
fn rotate_right<K: Clone, V>(
    entries: &mut [(K, Arc<V>)],
    children: &mut [Arc<BTreeNode<K, V>>],
    idx: usize,
) {
    let left = &children[idx - 1];
    let right = &children[idx];

    // Steal the last entry of the left sibling.
    let stolen = left.entries.last().unwrap().clone();
    // The current separator goes down into the right child.
    let separator = entries[idx - 1].clone();
    // The stolen entry becomes the new separator.
    entries[idx - 1] = stolen;

    let new_left_entries = left.entries[..left.entries.len() - 1].to_vec();
    let mut new_left_children = left.children.clone();
    let stolen_child = if !new_left_children.is_empty() {
        Some(new_left_children.pop().unwrap())
    } else {
        None
    };

    let mut new_right_entries = right.entries.clone();
    new_right_entries.insert(0, separator);
    let mut new_right_children = right.children.clone();
    if let Some(sc) = stolen_child {
        new_right_children.insert(0, sc);
    }

    children[idx - 1] =
        Arc::new(BTreeNode { entries: new_left_entries, children: new_left_children });
    children[idx] =
        Arc::new(BTreeNode { entries: new_right_entries, children: new_right_children });
}

/// Rotates an entry from the right sibling into the current child.
fn rotate_left<K: Clone, V>(
    entries: &mut [(K, Arc<V>)],
    children: &mut [Arc<BTreeNode<K, V>>],
    idx: usize,
) {
    let left = &children[idx];
    let right = &children[idx + 1];

    let stolen = right.entries[0].clone();
    let separator = entries[idx].clone();
    entries[idx] = stolen;

    let new_right_entries = right.entries[1..].to_vec();
    let mut new_right_children = right.children.clone();
    let stolen_child = if !new_right_children.is_empty() {
        Some(new_right_children.remove(0))
    } else {
        None
    };

    let mut new_left_entries = left.entries.clone();
    new_left_entries.push(separator);
    let mut new_left_children = left.children.clone();
    if let Some(sc) = stolen_child {
        new_left_children.push(sc);
    }

    children[idx] =
        Arc::new(BTreeNode { entries: new_left_entries, children: new_left_children });
    children[idx + 1] =
        Arc::new(BTreeNode { entries: new_right_entries, children: new_right_children });
}

/// Merges an underfull child with its left sibling.
fn merge_with_left<K: Clone, V>(
    entries: &mut Vec<(K, Arc<V>)>,
    children: &mut Vec<Arc<BTreeNode<K, V>>>,
    idx: usize,
) {
    let separator = entries.remove(idx - 1);
    let right = children.remove(idx);

    let mut merged_entries = children[idx - 1].entries.clone();
    merged_entries.push(separator);
    merged_entries.extend(right.entries.iter().cloned());

    let mut merged_children = children[idx - 1].children.clone();
    merged_children.extend(right.children.iter().cloned());

    children[idx - 1] =
        Arc::new(BTreeNode { entries: merged_entries, children: merged_children });
}

/// Merges an underfull child with its right sibling.
fn merge_with_right<K: Clone, V>(
    entries: &mut Vec<(K, Arc<V>)>,
    children: &mut Vec<Arc<BTreeNode<K, V>>>,
    idx: usize,
) {
    let separator = entries.remove(idx);
    let right = children.remove(idx + 1);

    let mut merged_entries = children[idx].entries.clone();
    merged_entries.push(separator);
    merged_entries.extend(right.entries.iter().cloned());

    let mut merged_children = children[idx].children.clone();
    merged_children.extend(right.children.iter().cloned());

    children[idx] = Arc::new(BTreeNode { entries: merged_entries, children: merged_children });
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

#[allow(dead_code)]
struct LevelBuilder<K, V> {
    entries: Vec<(K, Arc<V>)>,
    children: Vec<Arc<BTreeNode<K, V>>>,
}

#[allow(dead_code)]
impl<K, V> LevelBuilder<K, V> {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(MAX_KEYS),
            children: Vec::with_capacity(MAX_KEYS + 1),
        }
    }
}

#[allow(dead_code)]
struct BulkBuilder<K, V> {
    levels: Vec<LevelBuilder<K, V>>,
    len: usize,
    last_key: Option<K>,
}

#[allow(dead_code)]
impl<K: Ord + Clone, V> BulkBuilder<K, V> {
    fn new() -> Self {
        Self { levels: vec![LevelBuilder::new()], len: 0, last_key: None }
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
    fn attach_child(
        &mut self,
        level: usize,
        child: Arc<BTreeNode<K, V>>,
        sep_k: K,
        sep_v: Arc<V>,
    ) {
        if level >= self.levels.len() {
            self.levels.push(LevelBuilder::new());
        }
        let lv = &mut self.levels[level];
        // On entry, `children.len() == entries.len()` (after the prior freeze
        // pushed a child up without yet attaching the separator). We append the
        // new child first, then the separator, restoring `children.len() == entries.len() + 1`.
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
            let has_parent_sibling = self
                .levels
                .get(level + 1)
                .is_some_and(|p| !p.children.is_empty());
            if has_parent_sibling && self.levels[level].entries.len() < MIN_KEYS {
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
                let entries = std::mem::take(&mut lv.entries);
                let children = std::mem::take(&mut lv.children);
                debug_assert_eq!(children.len(), entries.len() + 1);
                Arc::new(BTreeNode { entries, children })
            };
            carry = Some(node);
        }

        let root = carry
            .unwrap_or_else(|| Arc::new(BTreeNode { entries: vec![], children: vec![] }));
        BTree { root, len: self.len }
    }
}

/// Rebalance the underfull partial node at `levels[level]` with its left
/// sibling — the most recently-frozen node sitting at the tail of
/// `levels[level + 1].children`. Pops the sibling and the separator that
/// linked them, merges everything in order, then splits the merged sequence
/// in half so both resulting nodes satisfy `MIN_KEYS`. The new left node and
/// separator go back into the parent; the partial node receives the right
/// half.
#[allow(dead_code)]
fn redistribute_tail<K: Clone, V>(levels: &mut [LevelBuilder<K, V>], level: usize) {
    let is_leaf_level = level == 0;
    let (lower, upper) = levels.split_at_mut(level + 1);
    let lv = &mut lower[level];
    let parent = &mut upper[0]; // levels[level + 1]

    let sibling = parent.children.pop().expect("redistribute_tail: no sibling");
    let separator = parent.entries.pop().expect("redistribute_tail: no separator");

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

#[allow(dead_code)]
fn freeze_leaf<K, V>(lv: &mut LevelBuilder<K, V>) -> Arc<BTreeNode<K, V>> {
    Arc::new(BTreeNode {
        entries: std::mem::take(&mut lv.entries),
        children: vec![],
    })
}

#[allow(dead_code)]
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
        let results: Vec<u64> = t.range((Bound::Excluded(5u64), Bound::Included(8u64))).map(|(k, _)| *k).collect();
        assert_eq!(results, vec![6, 7, 8]);
    }

    #[test]
    fn range_with_both_excluded() {
        let t = insert_range(1, 10);
        let results: Vec<u64> = t.range((Bound::Excluded(5u64), Bound::Excluded(8u64))).map(|(k, _)| *k).collect();
        assert_eq!(results, vec![6, 7]);
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
            .range((
                "alice".to_string(),
                0u64,
            )..=(
                "alice".to_string(),
                u64::MAX,
            ))
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
        let walked_insert: Vec<(u64, u64)> =
            by_insert.range(..).map(|(k, v)| (*k, *v)).collect();
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
}
