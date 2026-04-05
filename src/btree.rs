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
    Fit(Arc<BTreeNode<K, V>>),
    Split {
        left: Arc<BTreeNode<K, V>>,
        median: (K, Arc<V>),
        right: Arc<BTreeNode<K, V>>,
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
        let replacing = self.get(&key).is_some();
        let val_arc = Arc::new(val);
        let new_len = if replacing { self.len } else { self.len + 1 };

        match insert_into_node(&self.root, key, val_arc) {
            InsertResult::Fit(new_root) => BTree { root: new_root, len: new_len },
            InsertResult::Split { left, median, right } => {
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
        let mut iter = BTreeRange { stack: vec![], end };
        iter.descend_left_from(&self.root, &start);
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
pub struct BTreeRange<'a, K, V> {
    /// Stack of (node, next-entry-index-to-yield) frames. Stack grows upward;
    /// the top frame is the current position.
    stack: Vec<(&'a BTreeNode<K, V>, usize)>,
    end: Bound<K>,
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
}

impl<'a, K: Ord + Clone, V> Iterator for BTreeRange<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let stack_len = self.stack.len();
            if stack_len == 0 {
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
            InsertResult::Fit(Arc::new(BTreeNode { entries, children }))
        }
        Err(pos) => {
            if node.children.is_empty() {
                // Leaf: insert and possibly split.
                entries.insert(pos, (key, val));
                maybe_split(entries, vec![])
            } else {
                // Internal: recurse into child[pos], then merge the result.
                let mut children = node.children.clone();
                match insert_into_node(&children[pos], key, val) {
                    InsertResult::Fit(new_child) => {
                        children[pos] = new_child;
                        InsertResult::Fit(Arc::new(BTreeNode { entries, children }))
                    }
                    InsertResult::Split { left, median, right } => {
                        entries.insert(pos, median);
                        children[pos] = left;
                        children.insert(pos + 1, right);
                        maybe_split(entries, children)
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
) -> InsertResult<K, V> {
    if entries.len() <= MAX_KEYS {
        InsertResult::Fit(Arc::new(BTreeNode { entries, children }))
    } else {
        // entries.len() == MAX_KEYS + 1 == 6; split at mid == 3.
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
        assert!(matches!(t.remove(&99), Err(crate::Error::KeyNotFound)));
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
}
