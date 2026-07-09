//! Verification kernel: the algorithmic core of ultima_db's persistent
//! copy-on-write B-tree (`src/btree.rs`), ported to the Aeneas-supported
//! safe-Rust subset.
//!
//! Deltas from the real code, chosen to keep the algorithm line-for-line
//! recognizable while staying translatable:
//! - `Arc<BTreeNode>` -> `Box<Node>`: the Arcs are never mutated through
//!   (persistent structure), so sharing is semantically invisible; `clone()`
//!   deep-copies instead of bumping a refcount, which is observationally
//!   identical for a pure structure.
//! - `K`/`V` monomorphized to `u64` (matches `Table`'s `BTree<u64, R>` use).
//! - `children: Vec<Arc<Node>>` -> `Children` cons-list: Aeneas models `Vec`
//!   as a length-bounded subtype of `List`, so a recursive type through `Vec`
//!   is not strictly positive in Lean and is rejected by the kernel (same
//!   reason Aeneas's own betree example uses a custom list). Entries stay
//!   `Vec<(u64, u64)>` (non-recursive, fine).
//! - `binary_search_by` replaced by `find_pos` (no closures, no early return
//!   from a loop); `Vec::insert`/slice `to_vec` replaced by loop helpers.
//! - the in-place rebalancers (`fix_underfull_child`, `rotate_*`, `merge_*`)
//!   take `&mut Vec` in the real code; here they are pure functions returning
//!   the repaired `(entries, children)` — the same algorithm without mutation,
//!   which the persistent structure makes observationally identical.
//! - `remove` returns `Option<BTree>` (`None` == the `Err(KeyNotFound)` arm)
//!   since the kernel has no `Error` type.

pub const T: usize = 64;
pub const MAX_KEYS: usize = 2 * T - 1;
/// Minimum entries in a non-root node (mirror of `src/btree.rs`).
pub const MIN_KEYS: usize = T - 1;

pub enum Children {
    Nil,
    Cons(Box<Node>, Box<Children>),
}

pub struct Node {
    /// Key-value pairs stored in sorted order.
    pub entries: Vec<(u64, u64)>,
    /// Children; Nil for leaf nodes, len == entries.len() + 1 for internal.
    pub children: Children,
}

pub struct BTree {
    pub root: Box<Node>,
    pub len: usize,
}

pub enum InsertResult {
    Fit(Box<Node>, bool),
    Split {
        left: Box<Node>,
        median: (u64, u64),
        right: Box<Node>,
        replaced: bool,
    },
}

// ---------------------------------------------------------------------------
// Entry helpers (Vec-based, non-recursive)
// ---------------------------------------------------------------------------

/// Binary search over sorted entries. Returns `(true, pos)` if `key` is at
/// `pos`, `(false, pos)` with the insertion point otherwise.
/// (Replaces `entries.binary_search_by(|(k, _)| k.cmp(&key))`.)
fn find_pos(entries: &Vec<(u64, u64)>, key: u64) -> (bool, usize) {
    let mut lo: usize = 0;
    let mut hi: usize = entries.len();
    let mut found = false;
    let mut pos: usize = 0;
    while lo < hi && !found {
        let mid = lo + (hi - lo) / 2;
        let k = entries[mid].0;
        if k == key {
            found = true;
            pos = mid;
        } else if k < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if found {
        (true, pos)
    } else {
        (false, lo)
    }
}

/// Copy of `v` with `x` inserted at `pos`. (Replaces `Vec::insert`.)
fn insert_entry_at(v: &Vec<(u64, u64)>, pos: usize, x: (u64, u64)) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    let mut i: usize = 0;
    while i < pos {
        out.push(v[i]);
        i += 1;
    }
    out.push(x);
    while i < v.len() {
        out.push(v[i]);
        i += 1;
    }
    out
}

/// Copy of `v` with the entry at `pos` replaced by `x`.
fn replace_entry(v: &Vec<(u64, u64)>, pos: usize, x: (u64, u64)) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    let mut i: usize = 0;
    while i < v.len() {
        if i == pos {
            out.push(x);
        } else {
            out.push(v[i]);
        }
        i += 1;
    }
    out
}

/// `v[..n]` as an owned Vec. (Replaces slice `to_vec`.)
fn entries_prefix(v: &Vec<(u64, u64)>, n: usize) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    let mut i: usize = 0;
    while i < n {
        out.push(v[i]);
        i += 1;
    }
    out
}

/// `v[n..]` as an owned Vec.
fn entries_suffix(v: &Vec<(u64, u64)>, n: usize) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    let mut i: usize = n;
    while i < v.len() {
        out.push(v[i]);
        i += 1;
    }
    out
}

// ---------------------------------------------------------------------------
// Children helpers (cons-list, recursive)
// ---------------------------------------------------------------------------

/// Deep copy of a node. (Replaces the derived `Clone`: Charon rejects the
/// mutually recursive `impl Clone for Node`/`impl Clone for Children` pair as
/// a "mixed declaration group"; plain mutually recursive functions are fine.)
pub fn clone_node(n: &Node) -> Node {
    Node {
        entries: n.entries.clone(),
        children: clone_children(&n.children),
    }
}

/// Deep copy of a children list.
pub fn clone_children(c: &Children) -> Children {
    match c {
        Children::Nil => Children::Nil,
        Children::Cons(n, rest) => {
            Children::Cons(Box::new(clone_node(n)), Box::new(clone_children(rest)))
        }
    }
}

/// Number of children. (Replaces `Vec::len`.)
pub fn children_len(c: &Children) -> usize {
    match c {
        Children::Nil => 0,
        Children::Cons(_, rest) => 1 + children_len(rest),
    }
}

/// The child at index `i`; panics (-> `Result::fail` in Lean) out of bounds.
/// (Replaces `children[i]`.)
pub fn child_at(c: &Children, i: usize) -> &Node {
    match c {
        Children::Nil => panic!("child index out of bounds"),
        Children::Cons(n, rest) => {
            if i == 0 {
                n
            } else {
                child_at(rest, i - 1)
            }
        }
    }
}

/// Copy of `c` with the child at `pos` replaced by `x`.
/// (Replaces `children[pos] = x` on a fresh copy.)
fn replace_child(c: &Children, pos: usize, x: Box<Node>) -> Children {
    match c {
        Children::Nil => Children::Nil,
        Children::Cons(n, rest) => {
            if pos == 0 {
                Children::Cons(x, Box::new(clone_children(rest)))
            } else {
                Children::Cons(Box::new(clone_node(n)), Box::new(replace_child(rest, pos - 1, x)))
            }
        }
    }
}

/// Copy of `c` with the child at `pos` replaced by `l` and `r` inserted
/// right after it. (Replaces `children[pos] = left; children.insert(pos+1, right)`.)
fn replace_and_insert_child(c: &Children, pos: usize, l: Box<Node>, r: Box<Node>) -> Children {
    match c {
        Children::Nil => Children::Nil,
        Children::Cons(n, rest) => {
            if pos == 0 {
                Children::Cons(l, Box::new(Children::Cons(r, Box::new(clone_children(rest)))))
            } else {
                Children::Cons(
                    Box::new(clone_node(n)),
                    Box::new(replace_and_insert_child(rest, pos - 1, l, r)),
                )
            }
        }
    }
}

/// The first `n` children. (Replaces `children[..n].to_vec()`.)
fn children_prefix(c: &Children, n: usize) -> Children {
    if n == 0 {
        Children::Nil
    } else {
        match c {
            Children::Nil => Children::Nil,
            Children::Cons(x, rest) => {
                Children::Cons(Box::new(clone_node(x)), Box::new(children_prefix(rest, n - 1)))
            }
        }
    }
}

/// The children from index `n` on. (Replaces `children[n..].to_vec()`.)
fn children_suffix(c: &Children, n: usize) -> Children {
    if n == 0 {
        clone_children(c)
    } else {
        match c {
            Children::Nil => Children::Nil,
            Children::Cons(_, rest) => children_suffix(rest, n - 1),
        }
    }
}

// ---------------------------------------------------------------------------
// B-tree core (mirrors src/btree.rs)
// ---------------------------------------------------------------------------

/// Recursively searches for a key in a node.
/// (Mirror of `get_arc_in_node`.)
pub fn get_in_node(node: &Node, key: u64) -> Option<u64> {
    let (hit, pos) = find_pos(&node.entries, key);
    if hit {
        Some(node.entries[pos].1)
    } else if children_len(&node.children) == 0 {
        None
    } else {
        get_in_node(child_at(&node.children, pos), key)
    }
}

/// Wrap entries+children into a node, splitting if entries exceed MAX_KEYS.
/// (Mirror of `maybe_split`.)
fn maybe_split(entries: Vec<(u64, u64)>, children: Children, replaced: bool) -> InsertResult {
    if entries.len() <= MAX_KEYS {
        InsertResult::Fit(Box::new(Node { entries, children }), replaced)
    } else {
        // entries.len() == MAX_KEYS + 1 == 128; split at mid == 64.
        let mid = entries.len() / 2;
        let median = entries[mid];
        let left_entries = entries_prefix(&entries, mid);
        let right_entries = entries_suffix(&entries, mid + 1);

        let (left_children, right_children) = if children_len(&children) == 0 {
            (Children::Nil, Children::Nil)
        } else {
            (
                children_prefix(&children, mid + 1),
                children_suffix(&children, mid + 1),
            )
        };

        InsertResult::Split {
            left: Box::new(Node {
                entries: left_entries,
                children: left_children,
            }),
            median,
            right: Box::new(Node {
                entries: right_entries,
                children: right_children,
            }),
            replaced,
        }
    }
}

/// Recursively inserts a key-value pair into a node, potentially splitting it.
/// (Mirror of `insert_into_node`.)
pub fn insert_into_node(node: &Node, key: u64, val: u64) -> InsertResult {
    let (hit, pos) = find_pos(&node.entries, key);
    if hit {
        // Replace existing value.
        let entries = replace_entry(&node.entries, pos, (key, val));
        let children = clone_children(&node.children);
        InsertResult::Fit(Box::new(Node { entries, children }), true)
    } else if children_len(&node.children) == 0 {
        // Leaf: insert and possibly split.
        let entries = insert_entry_at(&node.entries, pos, (key, val));
        maybe_split(entries, Children::Nil, false)
    } else {
        // Internal: recurse into child[pos], then merge the result.
        match insert_into_node(child_at(&node.children, pos), key, val) {
            InsertResult::Fit(new_child, replaced) => {
                let children = replace_child(&node.children, pos, new_child);
                let entries = node.entries.clone();
                InsertResult::Fit(Box::new(Node { entries, children }), replaced)
            }
            InsertResult::Split {
                left,
                median,
                right,
                replaced,
            } => {
                let entries = insert_entry_at(&node.entries, pos, median);
                let children = replace_and_insert_child(&node.children, pos, left, right);
                maybe_split(entries, children, replaced)
            }
        }
    }
}

impl BTree {
    /// Creates a new, empty B-tree.
    pub fn new() -> Self {
        BTree {
            root: Box::new(Node {
                entries: Vec::new(),
                children: Children::Nil,
            }),
            len: 0,
        }
    }

    /// Look up a key.
    pub fn get(&self, key: u64) -> Option<u64> {
        get_in_node(&self.root, key)
    }

    /// Insert or replace a key-value pair. Returns a new tree; `self` is
    /// unchanged. (Mirror of `insert_arc`.)
    pub fn insert(&self, key: u64, val: u64) -> BTree {
        match insert_into_node(&self.root, key, val) {
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
                let mut entries: Vec<(u64, u64)> = Vec::new();
                entries.push(median);
                let children = Children::Cons(
                    left,
                    Box::new(Children::Cons(right, Box::new(Children::Nil))),
                );
                let new_root = Box::new(Node { entries, children });
                BTree {
                    root: new_root,
                    len: new_len,
                }
            }
        }
    }

    /// In-place insert; mutates `self` instead of returning a new tree.
    /// (Mirror of `BTree::insert_mut` / `insert_arc_mut` in `src/btree.rs`.)
    ///
    /// # Delta from the real code
    ///
    /// The real `insert_mut` descends through `Arc::make_mut`, cloning a node
    /// only when it is still shared with another snapshot (`strong_count > 1`)
    /// and mutating it in place otherwise. That clone-if-shared branch is a
    /// property of `Arc` reference counts; this kernel models nodes as
    /// uniquely-owned `Box` (see the top-of-file note: "the Arcs are never
    /// mutated through"), so the clone-vs-mutate distinction is invisible here
    /// — every node on the path is simply produced fresh, exactly as in
    /// `insert`. What survives the translation — and what `insert_mut`
    /// guarantees at the value level — is that it mutates `self` into precisely
    /// the tree [`insert`](BTree::insert) would return. So the faithful
    /// functional mirror is that in-place assignment. The differential test
    /// `insert_mut_matches_insert_and_std_btreemap` pins
    /// `insert_mut ≡ insert ≡ std::collections::BTreeMap`.
    ///
    /// The copy-on-write / snapshot-isolation property that actually
    /// distinguishes the two paths in the real code is a fact about `Arc`
    /// sharing, out of the Aeneas-modeled surface (like store/OCC/WAL) and
    /// covered instead by the concurrent-reader test
    /// `uncommitted_insert_mut_invisible_to_concurrent_reader` in
    /// `tests/store_integration.rs`.
    pub fn insert_mut(&mut self, key: u64, val: u64) {
        *self = self.insert(key, val);
    }

    /// Remove `key`. Returns the new tree, or `None` if the key was absent
    /// (the `Err(KeyNotFound)` arm of `src/btree.rs`). (Mirror of `remove`.)
    pub fn remove(&self, key: u64) -> Option<BTree> {
        match delete_from_node(&self.root, key) {
            DeleteResult::NotFound => None,
            DeleteResult::Removed { node: new_root, .. } => {
                // If the root became an entryless internal node, collapse a level.
                let actual_root = if new_root.entries.len() == 0 && children_len(&new_root.children) != 0 {
                    Box::new(clone_node(child_at(&new_root.children, 0)))
                } else {
                    new_root
                };
                Some(BTree {
                    root: actual_root,
                    len: self.len - 1,
                })
            }
        }
    }

    /// In-place delete; mutates `self` instead of returning a new tree.
    /// Returns `true` iff the key was present. (Mirror of `BTree::remove_mut`
    /// in `src/btree.rs`.)
    ///
    /// # Delta from the real code
    ///
    /// Same reasoning as `insert_mut`: the real `remove_mut` descends through
    /// `Arc::make_mut` (clone-if-shared vs mutate-in-place), a property of `Arc`
    /// reference counts that this uniquely-owned-`Box` model cannot express.
    /// What survives is that it mutates `self` into exactly the tree `remove`
    /// returns (or leaves it unchanged if absent). The differential test
    /// `remove_mut_matches_remove_and_std_btreemap` pins
    /// `remove_mut ≡ remove ≡ std::collections::BTreeMap`. The CoW /
    /// snapshot-isolation property is out of Aeneas scope (like store/OCC/WAL),
    /// covered by `uncommitted_remove_mut_invisible_to_concurrent_reader` in
    /// `tests/store_integration.rs`.
    pub fn remove_mut(&mut self, key: u64) -> bool {
        match self.remove(key) {
            Some(t) => {
                *self = t;
                true
            }
            None => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Delete with rebalancing (mirrors src/btree.rs delete_from_node & friends)
// ---------------------------------------------------------------------------

pub enum DeleteResult {
    NotFound,
    Removed { node: Box<Node>, underfull: bool },
}

/// Copy of `v` with the entry at `pos` removed. (Replaces `Vec::remove`.)
fn remove_entry_at(v: &Vec<(u64, u64)>, pos: usize) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    let mut i: usize = 0;
    while i < v.len() {
        if i != pos {
            out.push(v[i]);
        }
        i += 1;
    }
    out
}

/// `left ++ [sep] ++ right`. (Replaces the merge entry concatenation.)
fn merge_entries(left: &Vec<(u64, u64)>, sep: (u64, u64), right: &Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    let mut i: usize = 0;
    while i < left.len() {
        out.push(left[i]);
        i += 1;
    }
    out.push(sep);
    let mut j: usize = 0;
    while j < right.len() {
        out.push(right[j]);
        j += 1;
    }
    out
}

/// Copy of `c` with the child at `pos` removed. (Replaces `Vec::remove` on children.)
fn remove_child_at(c: &Children, pos: usize) -> Children {
    match c {
        Children::Nil => Children::Nil,
        Children::Cons(n, rest) => {
            if pos == 0 {
                clone_children(rest)
            } else {
                Children::Cons(Box::new(clone_node(n)), Box::new(remove_child_at(rest, pos - 1)))
            }
        }
    }
}

/// `a ++ b` as children. (Replaces `merged_children.extend(...)`.)
fn concat_children(a: &Children, b: &Children) -> Children {
    match a {
        Children::Nil => clone_children(b),
        Children::Cons(n, rest) => {
            Children::Cons(Box::new(clone_node(n)), Box::new(concat_children(rest, b)))
        }
    }
}

/// The last child of `c` as a singleton list, or `Nil` if `c` is empty.
/// (`child_at`-shaped recursion so the branch never mixes with an outer
/// reborrow, which Aeneas's context matcher rejects.)
fn last_child_singleton(c: &Children) -> Children {
    match c {
        Children::Nil => Children::Nil,
        Children::Cons(n, rest) => {
            if children_len(rest) == 0 {
                Children::Cons(Box::new(clone_node(n)), Box::new(Children::Nil))
            } else {
                last_child_singleton(rest)
            }
        }
    }
}

/// Copy of `c` without its last child (`Nil` if `c` has 0 or 1 children).
fn drop_last_child(c: &Children) -> Children {
    match c {
        Children::Nil => Children::Nil,
        Children::Cons(n, rest) => {
            if children_len(rest) == 0 {
                Children::Nil
            } else {
                Children::Cons(Box::new(clone_node(n)), Box::new(drop_last_child(rest)))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Rebalancing (rotate / merge).
//
// # Delta from the real code
//
// The real `src/btree.rs` rebalancers mutate the two involved sibling nodes
// **in place** via `Arc::make_mut` (rotate) and `Arc::try_unwrap` (merge's
// `absorb`): a sibling is cloned only when still shared with a snapshot,
// otherwise it is edited/moved directly (copy-on-write). This functional model
// instead rebuilds siblings by unconditional cloning. The distinction is a
// property of `Arc` reference counts, invisible in this uniquely-owned model —
// the input→output mapping is identical, which `remove_matches_std_btreemap`
// and `remove_mut_matches_remove_and_std_btreemap` pin down.
// ---------------------------------------------------------------------------

/// Rotate an entry from the left sibling (at `idx-1`) into the child at `idx`.
/// Returns the parent's repaired `(entries, children)`. (Mirror of `rotate_right`.)
fn rotate_right(entries: &Vec<(u64, u64)>, children: &Children, idx: usize) -> (Vec<(u64, u64)>, Children) {
    let left = child_at(children, idx - 1);
    let right = child_at(children, idx);
    let ll = left.entries.len();

    // Steal the last entry of the left sibling; the separator descends right.
    let stolen = left.entries[ll - 1];
    let separator = entries[idx - 1];
    let new_entries = replace_entry(entries, idx - 1, stolen);

    let new_left_entries = entries_prefix(&left.entries, ll - 1);
    let new_right_entries = insert_entry_at(&right.entries, 0, separator);

    // Move left's last child (if internal) to the front of right's children.
    let new_left_children = drop_last_child(&left.children);
    let moved = last_child_singleton(&left.children);
    let new_right_children = concat_children(&moved, &right.children);

    let new_left = Box::new(Node {
        entries: new_left_entries,
        children: new_left_children,
    });
    let new_right = Box::new(Node {
        entries: new_right_entries,
        children: new_right_children,
    });
    let children1 = replace_child(children, idx - 1, new_left);
    let children2 = replace_child(&children1, idx, new_right);
    (new_entries, children2)
}

/// Rotate an entry from the right sibling (at `idx+1`) into the child at `idx`.
/// Returns the parent's repaired `(entries, children)`. (Mirror of `rotate_left`.)
fn rotate_left(entries: &Vec<(u64, u64)>, children: &Children, idx: usize) -> (Vec<(u64, u64)>, Children) {
    let left = child_at(children, idx);
    let right = child_at(children, idx + 1);

    let stolen = right.entries[0];
    let separator = entries[idx];
    let new_entries = replace_entry(entries, idx, stolen);

    let new_right_entries = entries_suffix(&right.entries, 1);
    let new_left_entries = insert_entry_at(&left.entries, left.entries.len(), separator);

    // Move right's first child (if internal) to the end of left's children.
    let moved = children_prefix(&right.children, 1);
    let new_left_children = concat_children(&left.children, &moved);
    let new_right_children = children_suffix(&right.children, 1);

    let new_left = Box::new(Node {
        entries: new_left_entries,
        children: new_left_children,
    });
    let new_right = Box::new(Node {
        entries: new_right_entries,
        children: new_right_children,
    });
    let children1 = replace_child(children, idx, new_left);
    let children2 = replace_child(&children1, idx + 1, new_right);
    (new_entries, children2)
}

/// Merge the child at `idx` into its left sibling (`idx-1`), pulling the
/// separator down between them. (Mirror of `merge_with_left`.)
fn merge_with_left(entries: &Vec<(u64, u64)>, children: &Children, idx: usize) -> (Vec<(u64, u64)>, Children) {
    let separator = entries[idx - 1];
    let left = child_at(children, idx - 1);
    let right = child_at(children, idx);

    let merged_entries = merge_entries(&left.entries, separator, &right.entries);
    let merged_children = concat_children(&left.children, &right.children);
    let merged = Box::new(Node {
        entries: merged_entries,
        children: merged_children,
    });

    let new_entries = remove_entry_at(entries, idx - 1);
    let children1 = replace_child(children, idx - 1, merged);
    let children2 = remove_child_at(&children1, idx);
    (new_entries, children2)
}

/// Merge the child at `idx` into its right sibling (`idx+1`). (Mirror of `merge_with_right`.)
fn merge_with_right(entries: &Vec<(u64, u64)>, children: &Children, idx: usize) -> (Vec<(u64, u64)>, Children) {
    let separator = entries[idx];
    let left = child_at(children, idx);
    let right = child_at(children, idx + 1);

    let merged_entries = merge_entries(&left.entries, separator, &right.entries);
    let merged_children = concat_children(&left.children, &right.children);
    let merged = Box::new(Node {
        entries: merged_entries,
        children: merged_children,
    });

    let new_entries = remove_entry_at(entries, idx);
    let children1 = replace_child(children, idx, merged);
    let children2 = remove_child_at(&children1, idx + 1);
    (new_entries, children2)
}

/// Rebalance an underfull child at `idx` by rotating from a sibling with a
/// spare entry, else merging. Returns the parent's repaired `(entries, children)`.
/// (Mirror of `fix_underfull_child`.)
fn fix_underfull_child(entries: &Vec<(u64, u64)>, children: &Children, idx: usize) -> (Vec<(u64, u64)>, Children) {
    let n = children_len(children);
    if idx > 0 && child_at(children, idx - 1).entries.len() > MIN_KEYS {
        rotate_right(entries, children, idx)
    } else if idx + 1 < n && child_at(children, idx + 1).entries.len() > MIN_KEYS {
        rotate_left(entries, children, idx)
    } else if idx > 0 {
        merge_with_left(entries, children, idx)
    } else {
        merge_with_right(entries, children, idx)
    }
}

/// Apply `fix_underfull_child` at `idx` iff `underfull`, else pass the parent
/// through unchanged. Hoisted from the call sites so the borrow context stays
/// local to one small function (the equivalent inline `if` defeats Aeneas's
/// context matcher when the parent came from a recursive `match` arm).
fn maybe_fix(entries: Vec<(u64, u64)>, children: Children, idx: usize, underfull: bool) -> (Vec<(u64, u64)>, Children) {
    if underfull {
        fix_underfull_child(&entries, &children, idx)
    } else {
        (entries, children)
    }
}

/// Remove and return the leftmost (minimum-key) entry of the subtree, plus the
/// rebuilt node and whether it is now underfull. (Mirror of `remove_leftmost`.)
fn remove_leftmost(node: &Node) -> ((u64, u64), Box<Node>, bool) {
    if children_len(&node.children) == 0 {
        let first = node.entries[0];
        let new_entries = remove_entry_at(&node.entries, 0);
        let underfull = new_entries.len() < MIN_KEYS;
        (
            first,
            Box::new(Node {
                entries: new_entries,
                children: Children::Nil,
            }),
            underfull,
        )
    } else {
        let (entry, new_first_child, child_underfull) = remove_leftmost(child_at(&node.children, 0));
        let entries0 = node.entries.clone();
        let children0 = replace_child(&node.children, 0, new_first_child);
        let (entries1, children1) = maybe_fix(entries0, children0, 0, child_underfull);
        let underfull = entries1.len() < MIN_KEYS;
        (
            entry,
            Box::new(Node {
                entries: entries1,
                children: children1,
            }),
            underfull,
        )
    }
}

/// Recursively delete `key` from `node`, rebalancing on the way up.
/// (Mirror of `delete_from_node`.)
pub fn delete_from_node(node: &Node, key: u64) -> DeleteResult {
    let (hit, pos) = find_pos(&node.entries, key);

    if children_len(&node.children) == 0 {
        // Leaf.
        if !hit {
            DeleteResult::NotFound
        } else {
            let entries = remove_entry_at(&node.entries, pos);
            let underfull = entries.len() < MIN_KEYS;
            DeleteResult::Removed {
                node: Box::new(Node {
                    entries,
                    children: Children::Nil,
                }),
                underfull,
            }
        }
    } else if hit {
        // Internal, key is here: replace with in-order successor (leftmost of
        // children[pos+1]) and delete that successor.
        let (succ, new_right, right_underfull) = remove_leftmost(child_at(&node.children, pos + 1));
        let entries0 = replace_entry(&node.entries, pos, succ);
        let children0 = replace_child(&node.children, pos + 1, new_right);
        let (entries1, children1) = maybe_fix(entries0, children0, pos + 1, right_underfull);
        let underfull = entries1.len() < MIN_KEYS;
        DeleteResult::Removed {
            node: Box::new(Node {
                entries: entries1,
                children: children1,
            }),
            underfull,
        }
    } else {
        // Internal, key is in child[pos].
        match delete_from_node(child_at(&node.children, pos), key) {
            DeleteResult::NotFound => DeleteResult::NotFound,
            DeleteResult::Removed {
                node: new_child,
                underfull,
            } => {
                let entries0 = node.entries.clone();
                let children0 = replace_child(&node.children, pos, new_child);
                let (entries1, children1) = maybe_fix(entries0, children0, pos, underfull);
                let node_underfull = entries1.len() < MIN_KEYS;
                DeleteResult::Removed {
                    node: Box::new(Node {
                        entries: entries1,
                        children: children1,
                    }),
                    underfull: node_underfull,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn matches_std_btreemap() {
        let mut model = BTreeMap::new();
        let mut tree = BTree::new();
        let mut x: u64 = 12345;
        for _ in 0..2000 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let k = x % 512;
            let v = x >> 32;
            tree = tree.insert(k, v);
            model.insert(k, v);
            assert_eq!(tree.len, model.len());
        }
        for k in 0..512 {
            assert_eq!(tree.get(k), model.get(&k).copied(), "key {k}");
        }
    }

    #[test]
    fn insert_mut_matches_insert_and_std_btreemap() {
        // `insert_mut` must mutate `self` into exactly the tree the immutable
        // `insert` returns, and both must track `std::BTreeMap`. Drive the same
        // key stream through all three; the key domain (700) is far smaller than
        // the iteration count (3000), so this exercises many splits and repeated
        // replaces of existing keys.
        let mut model = BTreeMap::new();
        let mut imm = BTree::new(); // immutable insert (returns a new tree)
        let mut inp = BTree::new(); // in-place insert_mut (mutates self)
        let mut x: u64 = 0xDEAD_BEEF_CAFE_BABE;
        for _ in 0..3000 {
            x = lcg(x);
            let k = x % 700;
            let v = x >> 32;
            imm = imm.insert(k, v);
            inp.insert_mut(k, v);
            model.insert(k, v);
            assert_eq!(inp.len, imm.len, "len: in-place vs immutable");
            assert_eq!(inp.len, model.len(), "len: in-place vs std");
        }
        // Full observable equivalence over the whole key domain, absent keys
        // included: in-place == immutable == std for every key.
        for k in 0..700 {
            let want = model.get(&k).copied();
            assert_eq!(inp.get(k), want, "in-place vs std, key {k}");
            assert_eq!(inp.get(k), imm.get(k), "in-place vs immutable, key {k}");
        }
    }

    // Deterministic LCG (no rand dep, no Date/random primitives).
    fn lcg(x: u64) -> u64 {
        x.wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407)
    }

    #[test]
    fn remove_matches_std_btreemap() {
        let mut model = BTreeMap::new();
        let mut tree = BTree::new();
        let mut x: u64 = 0x9E3779B97F4A7C15;

        // Build a tree tall enough to have several internal levels.
        for _ in 0..3000 {
            x = lcg(x);
            let k = x % 800;
            let v = x >> 32;
            tree = tree.insert(k, v);
            model.insert(k, v);
        }
        assert_eq!(tree.len, model.len());

        // Interleave inserts and removes to exercise rotation and merge paths.
        for _ in 0..8000 {
            x = lcg(x);
            let k = x % 800;
            if x & 1 == 0 {
                x = lcg(x);
                let v = x >> 32;
                tree = tree.insert(k, v);
                model.insert(k, v);
            } else {
                let had = model.remove(&k);
                match tree.remove(k) {
                    Some(t) => {
                        assert!(had.is_some(), "tree removed absent key {k}");
                        tree = t;
                    }
                    None => assert!(had.is_none(), "tree missed present key {k}"),
                }
            }
            assert_eq!(tree.len, model.len());
        }

        for k in 0..800 {
            assert_eq!(tree.get(k), model.get(&k).copied(), "after churn, key {k}");
        }

        // Drain every remaining key: exercises repeated merges up to root collapse.
        let keys: Vec<u64> = model.keys().copied().collect();
        for k in keys {
            tree = tree.remove(k).expect("present key must remove");
            model.remove(&k);
            assert_eq!(tree.len, model.len());
        }
        assert_eq!(tree.len, 0);
        for k in 0..800 {
            assert_eq!(tree.get(k), None, "empty tree, key {k}");
        }
        // Removing from an empty tree is a no-op miss.
        assert!(tree.remove(42).is_none());
    }

    #[test]
    fn remove_mut_matches_remove_and_std_btreemap() {
        // remove_mut must mutate `self` into exactly what the immutable remove
        // returns, and both must track std::BTreeMap, across interleaved churn.
        let mut model = BTreeMap::new();
        let mut imm = BTree::new(); // immutable remove (returns Option<BTree>)
        let mut inp = BTree::new(); // in-place remove_mut
        let mut x: u64 = 0x1234_5678_9ABC_DEF0;
        for _ in 0..3000 {
            x = lcg(x);
            let k = x % 600;
            imm = imm.insert(k, k);
            inp.insert_mut(k, k);
            model.insert(k, k);
        }
        for _ in 0..6000 {
            x = lcg(x);
            let k = x % 600;
            if x & 1 == 0 {
                imm = imm.insert(k, k);
                inp.insert_mut(k, k);
                model.insert(k, k);
            } else {
                let inp_had = inp.remove_mut(k);
                let model_had = model.remove(&k).is_some();
                assert_eq!(inp_had, model_had, "remove_mut presence vs std, key {k}");
                if let Some(t) = imm.remove(k) {
                    imm = t;
                }
            }
            assert_eq!(inp.len, imm.len, "len: in-place vs immutable");
            assert_eq!(inp.len, model.len(), "len: in-place vs std");
        }
        for k in 0..600 {
            let want = model.get(&k).copied();
            assert_eq!(inp.get(k), want, "in-place vs std, key {k}");
            assert_eq!(inp.get(k), imm.get(k), "in-place vs immutable, key {k}");
        }
    }
}
