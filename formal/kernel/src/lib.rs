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
//! - insert/get only; delete-with-rebalancing to be ported later.

pub const T: usize = 32;
pub const MAX_KEYS: usize = 2 * T - 1;

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
        // entries.len() == MAX_KEYS + 1 == 64; split at mid == 32.
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
}
