# Incremental Checkpoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `Store::checkpoint()` cost track the number of rows changed since the last checkpoint instead of the size of the dataset.

**Architecture:** UltimaDB is copy-on-write, so an untouched subtree is literally the same `Arc<BTreeNode>` across two snapshots. A new `BTree::diff` walks two versions with a dual cursor that skips any subtree whose `Arc` is pointer-identical on both sides, emitting only added/updated/removed keys. `checkpoint()` writes those changes as a *delta* file chained to a base full checkpoint; recovery replays the chain. Correctness comes from the ordered key merge, not from the skip — the skip is purely the performance win.

**Tech Stack:** Rust 2024 edition, `bincode` 2 (serde feature) for record encoding, `crc32fast` via `crate::wal::crc32`, `proptest` (new dev-dependency) for oracle tests, `criterion` for benches.

**Spec:** `docs/superpowers/specs/2026-08-08-incremental-checkpoints-design.md`

## Global Constraints

- `cargo clippy -- -D warnings` must pass with zero warnings. Clippy is CI-gated; rustfmt is not.
- **Do not run `cargo fmt`.** The repo has rustfmt-version drift and no fmt gate; running it rewrites unrelated lines. Match the indentation and style of the file you are editing (`btree.rs`, `checkpoint.rs`, `registry.rs`, `store.rs` use 4 spaces; `overlay.rs` uses tabs).
- All persistence code is behind `#[cfg(feature = "persistence")]`. Test with `cargo test --features persistence`.
- `StoreConfig` and `Persistence` are `#[non_exhaustive]` (task44). New config goes through `StoreConfig::builder()`.
- Every new public item needs a doc comment. Existing docs in this codebase explain *why*, not *what* — match that.
- **No local perf conclusions.** The sandbox noise floor is ±2x. Benchmarks in Task 9 are written locally but must be run on the `bench-infra` NVMe host before any number is recorded.
- Encoded keys are capped at 64 KiB by `check_encoded_key_len`. The delta path must enforce the same cap as the checkpoint and WAL paths, or a key one format accepts and another refuses becomes a row that survives one durability path and destroys the other (task56).
- Commit after every task. Branch: `feat/incremental-checkpoints` (already exists, spec committed at `4098b6b`).

---

### Task 1: `BTree::diff` — dual cursor with pointer-identity subtree skip

**Files:**
- Modify: `Cargo.toml` (add `proptest` dev-dependency)
- Modify: `src/btree.rs` (add `Change`, `DiffCursor`, `BTreeDiff`, `BTree::diff`)
- Test: `src/btree.rs` `#[cfg(test)] mod tests` (unit), `tests/btree_diff_oracle.rs` (proptest)

**Interfaces:**
- Consumes: nothing (first task)
- Produces:
  - `pub enum Change<'a, K, V> { Added(&'a K, &'a Arc<V>), Updated(&'a K, &'a Arc<V>), Removed(&'a K) }`
  - `pub fn BTree::<K, V>::diff<'a>(&'a self, base: &'a BTree<K, V>) -> BTreeDiff<'a, K, V>` where `BTreeDiff: Iterator<Item = Change<'a, K, V>>`
  - Semantics: yields the changes that turn `base` into `self`, in ascending key order.

**Background for the implementer:** `BTree<K, V>` (`src/btree.rs:272`) holds `root: Arc<BTreeNode<K, V>>`. `BTreeNode` (`src/btree.rs:238`) has `entries: FixedVec<(K, Arc<V>), MAX_KEYS>` sorted ascending and `children: FixedVec<Arc<BTreeNode<K, V>>, {MAX_KEYS + 2}>` — empty for a leaf, otherwise `entries.len() + 1` long. In-order traversal is `child[0], entry[0], child[1], entry[1], ..., child[n]`.

The existing `BTreeRange` iterator (`src/btree.rs:710`) cannot be reused: its stack frames hold `&BTreeNode`, and `Arc::ptr_eq` needs `&Arc<BTreeNode>`. Hence a separate cursor type.

- [ ] **Step 1: Add the proptest dev-dependency**

In `Cargo.toml`, under `[dev-dependencies]`, after the `rand = "0.10"` line:

```toml
proptest = "1"
```

Run `cargo build --tests` to vendor it. Rationale for a new dependency: the diff's failure mode is a silently dropped key, and shrinking a 200-operation counterexample to a 3-operation one is the difference between a debuggable failure and an unactionable one.

- [ ] **Step 2: Write the failing unit test**

Add to the `mod tests` block at the bottom of `src/btree.rs`:

```rust
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test --lib btree::tests::diff_`
Expected: FAIL — `no method named 'diff' found for struct 'BTree'`.

- [ ] **Step 4: Implement `Change` and the cursor**

Add near the other public types in `src/btree.rs` (after the `BTreeRange` block is fine):

```rust
/// A single difference between two versions of a `BTree`.
///
/// Yielded by [`BTree::diff`] in ascending key order. Lifetimes borrow from
/// *both* trees: `Removed` borrows its key from the base tree, the other two
/// from the newer tree.
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
/// `BTreeRange` stores `&BTreeNode` frames, which cannot be compared with
/// `Arc::ptr_eq`; the diff needs the `Arc` itself to detect shared subtrees,
/// so it gets its own cursor.
///
/// A frame's `slot` interleaves children and entries in traversal order:
/// even `slot` means "child `slot / 2` has not been descended into yet",
/// odd `slot` means "entry `slot / 2` is the next entry to yield".
struct DiffCursor<'a, K, V> {
    stack: Vec<(&'a Arc<BTreeNode<K, V>>, usize)>,
}

impl<'a, K: Ord + Clone, V> DiffCursor<'a, K, V> {
    fn new(root: &'a Arc<BTreeNode<K, V>>) -> Self {
        DiffCursor { stack: vec![(root, 0)] }
    }

    /// The subtree this cursor is about to descend into, if any.
    ///
    /// `None` for a leaf frame or when the next step is an entry rather than
    /// a child — that is the signal the diff loop uses to fall back to a
    /// key-wise comparison.
    fn peek_child(&self) -> Option<&'a Arc<BTreeNode<K, V>>> {
        let (node, slot) = *self.stack.last()?;
        if node.children.is_empty() || slot % 2 == 1 {
            return None;
        }
        node.children.get(slot / 2)
    }

    /// Descend into the pending child.
    fn descend(&mut self) {
        if let Some(child) = self.peek_child() {
            let last = self.stack.len() - 1;
            self.stack[last].1 += 1;
            self.stack.push((child, 0));
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
            let node = last.0;
            last.1 += if node.children.is_empty() { 1 } else { 1 };
        }
    }
}
```

Note on `bump`: leaf frames advance one slot per entry, internal frames advance from odd slot `2i+1` to even slot `2i+2` (the next pending child) — both are `+= 1`. The branch is written out because the two cases mean different things and a future reader will otherwise assume it is a bug.

- [ ] **Step 5: Implement the diff iterator**

```rust
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
                (Some(a), Some(b)) if Arc::ptr_eq(a, b) => {
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
```

Add the entry point in the `impl<K: Ord + Clone, V> BTree<K, V>` block, next to `range`:

```rust
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
        BTreeDiff {
            new: DiffCursor::new(&self.root),
            base: DiffCursor::new(&base.root),
        }
    }
```

`Ordering` is already imported in `btree.rs` (used by `Locator`). Verify the import before adding a duplicate.

- [ ] **Step 6: Run the unit tests**

Run: `cargo test --lib btree::tests::diff_`
Expected: PASS (3 tests).

- [ ] **Step 7: Write the proptest oracle**

Create `tests/btree_diff_oracle.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `BTree::diff` against a brute-force oracle.
//!
//! The failure mode this guards is a *silently dropped* change: a diff that
//! misses a key loses a row on checkpoint recovery, with no error anywhere.
//! Example tests cannot cover the structural cases (splits, merges, root
//! collapse) that make a key move between nodes, so this compares against a
//! full scan of both trees over randomly generated histories.

use std::collections::BTreeMap;

use proptest::prelude::*;
use ultima_db::BTree;

#[derive(Debug, Clone)]
enum Op {
    Insert(u64, u64),
    Remove(u64),
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    // Keys drawn from a small space so inserts and removes actually collide;
    // a wide key space would make almost every remove a no-op.
    prop::collection::vec(
        prop_oneof![
            (0u64..200, 0u64..1000).prop_map(|(k, v)| Op::Insert(k, v)),
            (0u64..200).prop_map(Op::Remove),
        ],
        0..300,
    )
}

fn apply(tree: &BTree<u64, u64>, model: &mut BTreeMap<u64, u64>, op: &Op) -> BTree<u64, u64> {
    match op {
        Op::Insert(k, v) => {
            model.insert(*k, *v);
            tree.insert(*k, *v)
        }
        Op::Remove(k) => {
            model.remove(k);
            tree.remove(k).unwrap_or_else(|_| tree.clone())
        }
    }
}

/// Expected diff, computed the slow way: full scan of both trees.
fn oracle(
    new: &BTreeMap<u64, u64>,
    base: &BTreeMap<u64, u64>,
) -> Vec<(u64, Option<u64>)> {
    let mut out = Vec::new();
    let mut keys: Vec<u64> = new.keys().chain(base.keys()).copied().collect();
    keys.sort_unstable();
    keys.dedup();
    for k in keys {
        match (new.get(&k), base.get(&k)) {
            (Some(nv), Some(bv)) if nv == bv => {}
            (Some(nv), _) => out.push((k, Some(*nv))),
            (None, Some(_)) => out.push((k, None)),
            (None, None) => unreachable!("key came from one of the two maps"),
        }
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn diff_matches_full_scan_oracle(base_ops in ops(), then_ops in ops()) {
        let mut model = BTreeMap::new();
        let mut tree = BTree::<u64, u64>::new();
        for op in &base_ops {
            tree = apply(&tree, &mut model, op);
        }
        let base_tree = tree.clone();
        let base_model = model.clone();

        for op in &then_ops {
            tree = apply(&tree, &mut model, op);
        }

        let got: Vec<(u64, Option<u64>)> = tree
            .diff(&base_tree)
            .map(|c| match c {
                ultima_db::Change::Added(k, v) | ultima_db::Change::Updated(k, v) => (*k, Some(**v)),
                ultima_db::Change::Removed(k) => (*k, None),
            })
            .collect();

        prop_assert_eq!(got, oracle(&model, &base_model));
    }
}
```

Note the value-identity subtlety the oracle encodes: the model compares values by `==`, while `diff` compares by `Arc::ptr_eq`. `Op::Insert(k, v)` with the same `v` as the current binding creates a *new* `Arc`, so `diff` reports an `Updated` the model does not. Handle it by making the oracle compare on `==` and the test *tolerate* extra `Updated` entries whose value equals the base's — do **not** paper over it by relaxing the assert. Concretely: make `apply` skip the insert when `model.get(k) == Some(v)` so the two notions of change coincide. Add that guard to `apply`:

```rust
        Op::Insert(k, v) => {
            if model.get(k) == Some(v) {
                return tree.clone();     // same value: keep the existing Arc
            }
            model.insert(*k, *v);
            tree.insert(*k, *v)
        }
```

- [ ] **Step 8: Export `Change` and `BTreeDiff`**

`BTree` is re-exported from `src/lib.rs`. Add `Change` and `BTreeDiff` to the same `pub use` so the integration test compiles. Find the existing `pub use crate::btree::BTree;` line and extend it:

```rust
pub use crate::btree::{BTree, BTreeDiff, Change};
```

- [ ] **Step 9: Run the proptest**

Run: `cargo test --test btree_diff_oracle`
Expected: PASS, 256 cases. If it fails, proptest prints a shrunk counterexample — fix the cursor, do not weaken the oracle.

- [ ] **Step 10: Clippy and commit**

```bash
cargo clippy --all-targets -- -D warnings
git add Cargo.toml Cargo.lock src/btree.rs src/lib.rs tests/btree_diff_oracle.rs
git commit -m "feat(btree): diff two versions by pointer identity

An untouched subtree is the same Arc across two CoW snapshots, so two
versions can be diffed in time proportional to what changed. The ordered
key merge is what makes it correct; the ptr_eq subtree skip is what makes
it fast, and the two are deliberately independent.

BTreeRange could not be reused: its frames hold &BTreeNode, and ptr_eq
needs the Arc."
```

---

### Task 2: Prove the skip actually fires

**Files:**
- Modify: `src/btree.rs` (`#[cfg(test)]` node-visit counter)
- Test: `src/btree.rs` `mod tests`

**Interfaces:**
- Consumes: `BTree::diff`, `BTreeDiff` (Task 1)
- Produces: `BTreeDiff::nodes_visited(&self) -> usize`, `#[cfg(test)]` only

**Why this task exists:** a diff that is correct but silently walks every node passes every test in Task 1 while delivering none of the benefit. This is the one property no oracle test can catch.

- [ ] **Step 1: Write the failing test**

```rust
    #[test]
    fn diff_skips_shared_subtrees_instead_of_walking_them() {
        // Large enough to be several levels deep at T=32 (MAX_KEYS = 63).
        let mut base = BTree::<u64, u64>::new();
        for i in 0..20_000u64 {
            base = base.insert(i, i);
        }
        let new = base.insert(10_000, 999_999);

        let mut d = new.diff(&base);
        let changes: Vec<_> = (&mut d).collect();
        assert_eq!(changes.len(), 1, "exactly one key changed");

        // One changed key touches only the root-to-leaf path on each side.
        // 64 is a generous ceiling: the real number is ~2 * height + fanout
        // slack. If this trips, the skip stopped firing.
        assert!(
            d.nodes_visited() <= 64,
            "diff visited {} nodes for a single changed key — subtree skip is not firing",
            d.nodes_visited()
        );
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --lib btree::tests::diff_skips_shared`
Expected: FAIL — `no method named 'nodes_visited'`.

- [ ] **Step 3: Add the counter**

In `DiffCursor`, behind `cfg(test)`:

```rust
struct DiffCursor<'a, K, V> {
    stack: Vec<(&'a Arc<BTreeNode<K, V>>, usize)>,
    #[cfg(test)]
    descends: usize,
}
```

Count on the cursor, not on the diff: give `DiffCursor` a `#[cfg(test)] descends: usize` field, increment it inside `descend()` (the single place a subtree is entered), and drop the `nodes_visited` field from `BTreeDiff` in favour of summing the two cursors:

```rust
#[cfg(test)]
impl<K: Ord + Clone, V> BTreeDiff<'_, K, V> {
    /// Subtrees descended into by either cursor. Test-only: this is the guard
    /// against a correct diff that quietly degenerates to a full walk.
    pub fn nodes_visited(&self) -> usize {
        self.new.descends + self.base.descends
    }
}
```

`DiffCursor::new` initialises `descends: 0` under the same `cfg`. `BTreeDiff` needs no new field.

- [ ] **Step 4: Run the test**

Run: `cargo test --lib btree::tests::diff_skips_shared --release`
Expected: PASS. (Use `--release`: 20k inserts in a debug build is slow but not prohibitive; if the test takes over ~10s in debug, drop the size to 5,000 and the ceiling to 48 rather than marking it `#[ignore]`.)

- [ ] **Step 5: Commit**

```bash
cargo clippy --all-targets -- -D warnings
git add src/btree.rs
git commit -m "test(btree): assert the diff skip fires, not just that it is correct

A diff that walks every node is indistinguishable from a good one by any
oracle test, and delivers none of the benefit. Count descents and bound
them for a single-key change."
```

---

### Task 3: Type-erased per-table diff (`TableInfo::diff_table`)

**Files:**
- Modify: `src/registry.rs` (add `DiffTableFn`, `TableInfo::diff_table`, `diff_table::<R, K>`)
- Test: `src/registry.rs` `mod tests`

**Interfaces:**
- Consumes: `BTree::diff`, `Change` (Task 1)
- Produces:
  - `pub type DiffTableFn = Box<dyn Fn(&dyn Any, &dyn Any) -> Result<Vec<u8>> + Send + Sync>;`
  - `TableInfo::diff_table: DiffTableFn` — args are `(new_table, base_table)` as `&dyn Any`
  - Payload format (consumed by Task 5's writer and Task 6's loader):
    ```text
    [DELTA_MAGIC: u8 = 0xD1][DELTA_FORMAT: u8 = 1][key_type_id: u32 BE]
    [has_next_id: u8][next_id_len: u32 BE][next_id encoded]   -- when has_next_id == 1
    [num_changes: u64 BE]
    per change: [klen: u32 BE][key][op: u8 = 0 Put | 1 Del]
                Put -> [rlen: u32 BE][record bytes]
    ```

**Background:** `TableInfo` (`src/registry.rs:60`) is the type-erasure vtable. `serialize_table::<R, K>` (`src/registry.rs:414`) is the closest existing model — copy its header discipline (magic, format byte, `K::KEY_TYPE_ID`, `check_encoded_key_len`) rather than inventing a second one. `K` must not leak into `MergeableTable`'s signature, which is exactly why this goes through a registry closure that downcasts both sides.

- [ ] **Step 1: Write the failing test**

Add to `src/registry.rs` `mod tests`:

```rust
    #[test]
    fn diff_table_encodes_only_changed_rows() {
        let mut registry = TableRegistry::new();
        registry.register::<TestRecord>("items").unwrap();
        let info = registry.get("items").unwrap();

        let mut base = Table::<TestRecord, u64>::new();
        base.put(1, TestRecord { name: "a".into() });
        base.put(2, TestRecord { name: "b".into() });
        let mut new = base.clone();
        new.put(2, TestRecord { name: "B".into() });
        new.delete(&1).unwrap();
        new.put(3, TestRecord { name: "c".into() });

        let delta = (info.diff_table)(&new as &dyn Any, &base as &dyn Any).unwrap();
        let full = (info.serialize_table)(&new as &dyn Any).unwrap();

        // Three changed rows out of a three-row table is not a size win; the
        // point of the assertion is the *count*, decoded below.
        assert_eq!(delta[0], 0xD1, "delta magic");
        assert!(!full.is_empty());

        let changes = decode_delta_changes(&delta);
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0], (1u64, None));                       // deleted
        assert_eq!(changes[1].0, 2u64);
        assert!(changes[1].1.is_some());                            // updated
        assert_eq!(changes[2].0, 3u64);
        assert!(changes[2].1.is_some());                            // added
    }

    #[test]
    fn diff_table_of_an_unchanged_table_is_empty() {
        let mut registry = TableRegistry::new();
        registry.register::<TestRecord>("items").unwrap();
        let info = registry.get("items").unwrap();

        let mut t = Table::<TestRecord, u64>::new();
        t.put(1, TestRecord { name: "a".into() });
        let clone = t.clone();

        let delta = (info.diff_table)(&clone as &dyn Any, &t as &dyn Any).unwrap();
        assert_eq!(decode_delta_changes(&delta).len(), 0);
    }

    #[test]
    fn diff_table_rejects_a_base_of_a_different_type() {
        let mut registry = TableRegistry::new();
        registry.register::<TestRecord>("items").unwrap();
        let info = registry.get("items").unwrap();

        let new = Table::<TestRecord, u64>::new();
        let wrong = Table::<TestRecord, String>::new();
        let err = (info.diff_table)(&new as &dyn Any, &wrong as &dyn Any).unwrap_err();
        assert!(
            matches!(err, Error::Persistence(ref m) if m.contains("base table")),
            "unexpected error: {err:?}"
        );
    }
```

Write `decode_delta_changes` as a test helper in the same `mod tests`, returning `Vec<(u64, Option<Vec<u8>>)>`, parsing the format above with the existing `take`/`take_u32`/`take_u64` helpers (`src/registry.rs:456`).

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib registry::tests::diff_table --features persistence`
Expected: FAIL — no field `diff_table` on `TableInfo`.

- [ ] **Step 3: Implement `diff_table::<R, K>`**

Add next to `serialize_table` in `src/registry.rs`:

```rust
const DELTA_MAGIC: u8 = 0xD1;
const DELTA_FORMAT_V1: u8 = 1;

/// Serialize the rows that changed between `base` and `new`.
///
/// Mirrors [`serialize_table`]'s header discipline — same magic/format/key-type
/// preamble, same 64 KiB encoded-key cap — because the two formats have to
/// agree on what a legal row is. A key one path accepts and the other refuses
/// is a row that survives one durability path and destroys the other (task56).
fn diff_table<R: Record, K: PrimaryKey>(
    new: &Table<R, K>,
    base: &Table<R, K>,
) -> Result<Vec<u8>> {
    let config = bincode::config::standard();
    let mut buf = Vec::new();

    buf.push(DELTA_MAGIC);
    buf.push(DELTA_FORMAT_V1);
    buf.extend_from_slice(&K::KEY_TYPE_ID.to_be_bytes());

    match new.next_id_opt() {
        Some(id) => {
            buf.push(1u8);
            let enc = id.encode();
            buf.extend_from_slice(&(enc.len() as u32).to_be_bytes());
            buf.extend_from_slice(&enc);
        }
        None => buf.push(0u8),
    }

    // Count is written up front, so it is collected before it can be emitted.
    let mut body = Vec::new();
    let mut count: u64 = 0;
    for change in new.data().diff(base.data()) {
        let (key, record) = match change {
            Change::Added(k, v) | Change::Updated(k, v) => (k, Some(v)),
            Change::Removed(k) => (k, None),
        };
        let kb = key.encode();
        check_encoded_key_len(kb.len(), "checkpoint delta payload")?;
        body.extend_from_slice(&(kb.len() as u32).to_be_bytes());
        body.extend_from_slice(&kb);
        match record {
            Some(rec) => {
                body.push(0u8);
                let rb = bincode::serde::encode_to_vec(rec.as_ref(), config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
                body.extend_from_slice(&(rb.len() as u32).to_be_bytes());
                body.extend_from_slice(&rb);
            }
            None => body.push(1u8),
        }
        count += 1;
    }

    buf.extend_from_slice(&count.to_be_bytes());
    buf.extend_from_slice(&body);
    Ok(buf)
}
```

Add `use crate::btree::Change;` to the imports at the top of `src/registry.rs`.

`Table::data()` — the accessor for the backing `BTree<K, R>` — may be private or absent. Check `src/table.rs`; if there is no such accessor, add `pub(crate) fn data(&self) -> &BTree<K, R>`. Do not make it public: the table's `BTree` is an implementation detail and exposing it invites callers to bypass index maintenance.

- [ ] **Step 4: Wire it into `TableInfo`**

Add the type alias next to the other `*Fn` aliases:

```rust
/// Diff two `Table<R, K>` values (as `&dyn Any`) into a delta payload.
/// Arguments are `(new, base)`, in that order.
pub type DiffTableFn = Box<dyn Fn(&dyn Any, &dyn Any) -> Result<Vec<u8>> + Send + Sync>;
```

Add the field to `TableInfo` after `serialize_table`, and populate it in the registration closure block (`src/registry.rs:168`), following the shape of the `serialize_table` closure directly above it:

```rust
                    diff_table: Box::new(|new_any, base_any| {
                        let new = new_any
                            .downcast_ref::<Table<R, K>>()
                            .ok_or_else(|| Error::Persistence(
                                "table downcast failed for delta".into(),
                            ))?;
                        // A base of a different concrete type means the table
                        // was dropped and recreated with a different R or K
                        // between checkpoints. The caller turns this into a
                        // full table entry rather than a delta.
                        let base = base_any
                            .downcast_ref::<Table<R, K>>()
                            .ok_or_else(|| Error::Persistence(
                                "base table type does not match the current table".into(),
                            ))?;
                        diff_table(new, base)
                    }),
```

- [ ] **Step 5: Run the tests**

Run: `cargo test --lib registry::tests::diff_table --features persistence`
Expected: PASS (3 tests).

- [ ] **Step 6: Clippy and commit**

```bash
cargo clippy --all-targets --features persistence -- -D warnings
git add src/registry.rs src/table.rs
git commit -m "feat(registry): type-erased per-table delta payload

K cannot appear in MergeableTable's signature, so the diff goes through a
registry closure that downcasts both sides — the same shape serialize_table
already uses. A base of a different concrete type is a dropped-and-recreated
table; it errors here and the caller falls back to a full entry."
```

---

### Task 4: Checkpoint format v2 — full checkpoints only, no behaviour change

**Files:**
- Modify: `src/checkpoint.rs:32-80` (header, `serialize_snapshot`), `src/checkpoint.rs:83-171` (`deserialize_snapshot`)
- Test: `src/checkpoint.rs` `mod tests`

**Interfaces:**
- Consumes: nothing from earlier tasks
- Produces:
  - `const FORMAT_VERSION: u32 = 2`
  - `#[repr(u8)] enum CheckpointKind { Full = 0, Delta = 1 }`
  - `enum TableEntryKind { Unchanged = 0, Delta = 1, Full = 2, Dropped = 3 }`
  - File layout after the version: `[kind: u8][snapshot_version: u64][base_version: u64 — Delta only][num_tables: u32]`, each table entry prefixed with its `TableEntryKind`.

**Why this is its own task:** it changes the on-disk format without changing behaviour, so it can be reviewed and reverted independently of the delta logic. A format change bundled with a feature is a format change nobody reviews.

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn v2_full_checkpoint_round_trips() {
        // ... build a Snapshot with two registered tables ...
        let bytes = serialize_snapshot(&snap, &registry).unwrap();
        assert_eq!(&bytes[0..4], MAGIC);
        let restored = deserialize_snapshot(&bytes, &registry).unwrap();
        assert_eq!(restored.version, snap.version);
        assert_eq!(restored.tables.len(), snap.tables.len());
    }

    #[test]
    fn a_v1_checkpoint_is_refused_with_a_named_version() {
        let mut v1 = Vec::new();
        v1.extend_from_slice(MAGIC);
        bincode::encode_into_std_write(1u32, &mut v1, bincode::config::standard()).unwrap();
        v1.extend_from_slice(&crc32(&v1).to_le_bytes());
        let registry = TableRegistry::new();
        let err = deserialize_snapshot(&v1, &registry).unwrap_err();
        assert!(
            matches!(err, Error::CheckpointCorrupted(ref m) if m.contains("unsupported format version: 1")),
            "unexpected error: {err:?}"
        );
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib checkpoint::tests --features persistence`
Expected: the v1-refusal test FAILS (v1 is currently the *accepted* version).

- [ ] **Step 3: Bump the format and add the kind bytes**

In `src/checkpoint.rs`:

```rust
const FORMAT_VERSION: u32 = 2;

/// What a `checkpoint_*.bin` file contains.
///
/// Deltas deliberately share the `checkpoint_{version}.bin` namespace with
/// full checkpoints. An older binary reading a delta-headed directory picks
/// the delta as "latest" and fails its format check loudly, instead of
/// silently loading an older full whose WAL tail has already been pruned —
/// which would lose committed data with no error anywhere.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum CheckpointKind {
    Full = 0,
    Delta = 1,
}

/// How one table appears inside a checkpoint file.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum TableEntryKind {
    /// Byte-identical to the base — no payload.
    Unchanged = 0,
    /// Changed rows only, as produced by `TableInfo::diff_table`.
    Delta = 1,
    /// Whole table inline, as produced by `TableInfo::serialize_table`.
    Full = 2,
    /// Present in the base, gone in this version — no payload.
    Dropped = 3,
}
```

Write `kind` after `format_version` and, for `Delta`, `base_version` after `snapshot_version`. Prefix every table entry with its `TableEntryKind`. In this task `serialize_snapshot` always writes `Full`/`Full`, so `deserialize_snapshot` only needs to handle those two — but write the `u8 -> enum` conversion as an exhaustive match that errors on unknown values now, so Task 6 only adds arms.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib checkpoint::tests --features persistence`
Expected: PASS.

- [ ] **Step 5: Run the full persistence suite for regressions**

Run: `cargo test --features persistence`
Expected: PASS. Any test with a hand-built v1 checkpoint fixture needs updating to v2 — that is expected churn, not a bug. Recovery tests that write and read within one run are unaffected.

- [ ] **Step 6: Commit**

```bash
cargo clippy --all-targets --features persistence -- -D warnings
git add src/checkpoint.rs
git commit -m "feat(checkpoint): format v2 with explicit file and table-entry kinds

No behaviour change: every file is still Full and every table entry inline.
Deltas will share this namespace so an old binary fails loudly on a
delta-headed directory rather than silently loading a stale full whose WAL
tail was already pruned."
```

---

### Task 5: Write delta checkpoints

**Files:**
- Modify: `src/checkpoint.rs` (add `write_delta_checkpoint`, refactor the shared body out of `serialize_snapshot`)
- Test: `src/checkpoint.rs` `mod tests`

**Interfaces:**
- Consumes: `TableInfo::diff_table` (Task 3), `CheckpointKind`/`TableEntryKind` (Task 4)
- Produces:
  ```rust
  pub(crate) fn write_delta_checkpoint(
      dir: &Path,
      snapshot: &Snapshot,
      base: &Snapshot,
      registry: &TableRegistry,
  ) -> Result<u64>
  ```
  Returns `snapshot.version`. Uses the same tmp+rename+`sync_dir` discipline as `write_checkpoint` (`src/checkpoint.rs:210`).

- [ ] **Step 1: Write the failing test**

```rust
    #[test]
    fn a_delta_records_only_changed_tables() {
        let dir = tempfile::tempdir().unwrap();
        // registry with tables "a" and "b"; base snapshot has rows in both.
        // new snapshot changes only "a" — clone "b"'s Arc into it unchanged.
        let v = write_delta_checkpoint(dir.path(), &new_snap, &base_snap, &registry).unwrap();
        assert_eq!(v, new_snap.version);

        let raw = std::fs::read(dir.path().join(format!("checkpoint_{v}.bin"))).unwrap();
        let entries = parse_table_entry_kinds(&raw);          // test helper
        assert_eq!(entries.get("a"), Some(&TableEntryKind::Delta));
        assert_eq!(entries.get("b"), Some(&TableEntryKind::Unchanged));
    }

    #[test]
    fn a_table_absent_from_the_new_snapshot_is_recorded_as_dropped() {
        // base has "a" and "b"; new has only "a".
        let v = write_delta_checkpoint(dir.path(), &new_snap, &base_snap, &registry).unwrap();
        let raw = std::fs::read(dir.path().join(format!("checkpoint_{v}.bin"))).unwrap();
        assert_eq!(parse_table_entry_kinds(&raw).get("b"), Some(&TableEntryKind::Dropped));
    }

    #[test]
    fn a_table_absent_from_the_base_is_recorded_in_full() {
        // base has "a"; new has "a" and "b".
        let v = write_delta_checkpoint(dir.path(), &new_snap, &base_snap, &registry).unwrap();
        let raw = std::fs::read(dir.path().join(format!("checkpoint_{v}.bin"))).unwrap();
        assert_eq!(parse_table_entry_kinds(&raw).get("b"), Some(&TableEntryKind::Full));
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib checkpoint::tests::a_delta --features persistence`
Expected: FAIL — `cannot find function 'write_delta_checkpoint'`.

- [ ] **Step 3: Implement the entry-kind decision**

```rust
/// Serialize `snapshot` as a delta against `base`.
fn serialize_delta(
    snapshot: &Snapshot,
    base: &Snapshot,
    registry: &TableRegistry,
) -> Result<Vec<u8>> {
    // ... header: MAGIC, FORMAT_VERSION, CheckpointKind::Delta,
    //     snapshot.version, base.version ...

    for (name, table) in registered_tables(snapshot, registry) {
        let info = registry.get(name).ok_or_else(|| Error::TableNotRegistered(name.clone()))?;
        match base.tables.get(name) {
            // Same Arc: the table was not touched since the base checkpoint,
            // so there is provably nothing to write.
            Some(base_table) if Arc::ptr_eq(table, base_table) => {
                write_entry_kind(&mut buf, name, TableEntryKind::Unchanged);
            }
            Some(base_table) => {
                match (info.diff_table)(table.as_ref().as_any(), base_table.as_ref().as_any()) {
                    Ok(payload) => write_entry(&mut buf, name, TableEntryKind::Delta, &payload),
                    // Dropped and recreated with a different R or K: the base
                    // rows are not comparable, so the whole table goes inline.
                    Err(Error::Persistence(ref m)) if m.contains("base table") => {
                        let payload = (info.serialize_table)(table.as_ref().as_any())?;
                        write_entry(&mut buf, name, TableEntryKind::Full, &payload);
                    }
                    Err(e) => return Err(e),
                }
            }
            None => {
                let payload = (info.serialize_table)(table.as_ref().as_any())?;
                write_entry(&mut buf, name, TableEntryKind::Full, &payload);
            }
        }
    }

    // Tables in the base that are gone from this snapshot.
    for name in base.tables.keys() {
        if !snapshot.tables.contains_key(name) && registry.contains(name) {
            write_entry_kind(&mut buf, name, TableEntryKind::Dropped);
        }
    }

    // ... num_tables backfilled, CRC appended ...
}
```

`num_tables` is written before the entries are known, so either count first (two passes over the table map) or reserve four bytes and backfill. Prefer counting first — a backfilled length is the kind of thing that silently desynchronises when someone adds an entry kind later.

Match on the error string is fragile. Replace it with a dedicated variant instead: add `Error::TableTypeChanged { table: String }` to `src/error.rs`, return that from the `diff_table` closure's base downcast in Task 3, and match on it here. Update Task 3's third test to expect `Error::TableTypeChanged`.

- [ ] **Step 4: Implement `write_delta_checkpoint`**

Same body as `write_checkpoint` (`src/checkpoint.rs:210`) with `serialize_delta` in place of `serialize_snapshot`: `create_dir_all`, write to `{name}.tmp`, `sync_all`, `rename`, `sync_dir`. Factor the shared tail into `fn write_checkpoint_bytes(dir: &Path, version: u64, data: &[u8]) -> Result<u64>` and have both callers use it — two copies of the crash-safety dance is one copy too many.

- [ ] **Step 5: Run the tests**

Run: `cargo test --lib checkpoint::tests --features persistence`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo clippy --all-targets --features persistence -- -D warnings
git add src/checkpoint.rs src/registry.rs src/error.rs
git commit -m "feat(checkpoint): write delta checkpoints

A table whose Arc is unchanged since the base needs no payload at all —
that check is free and fires for every table an interval did not touch.
Created, dropped, and recreated-with-a-different-type tables each get an
explicit entry kind rather than being inferred at load time."
```

---

### Task 6: Chain discovery and recovery

**Files:**
- Modify: `src/checkpoint.rs` (`find_latest_checkpoint` → `find_head_chain`, `load_checkpoint` → `load_chain`)
- Modify: `src/error.rs` (add `CheckpointChainBroken`)
- Modify: `src/store.rs:1049-1054` (call the chain loader)
- Test: `src/checkpoint.rs` `mod tests`, `tests/store_integration.rs`

**Interfaces:**
- Consumes: everything from Tasks 3-5
- Produces:
  - `pub(crate) fn find_head_chain(dir: &Path) -> Result<Vec<PathBuf>>` — base-first, empty if no checkpoint exists
  - `pub(crate) fn load_chain(paths: &[PathBuf], registry: &TableRegistry) -> Result<Snapshot>`
  - `Error::CheckpointChainBroken { head: u64, missing: u64 }`

**Key decision to respect:** a broken chain is a hard failure. It cannot fall back to an older full, because the WAL has already been pruned to the head version — the older full plus the surviving WAL does not reconstruct committed state. An *absent* delta is different: tmp+rename makes a file atomically present or absent, so an absent one simply means the head is the previous file, which is a complete chain.

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn a_chain_recovers_to_the_same_state_as_a_full_checkpoint() {
        // full at v1, delta at v2, delta at v3
        let chain = find_head_chain(dir.path()).unwrap();
        assert_eq!(chain.len(), 3, "base + two deltas");
        let snap = load_chain(&chain, &registry).unwrap();
        assert_eq!(snap.version, 3);
        // same rows as a full checkpoint written at v3
    }

    #[test]
    fn a_corrupt_mid_chain_delta_fails_loudly() {
        // full at v1, delta at v2, delta at v3; corrupt v2's CRC
        let mut bytes = std::fs::read(dir.path().join("checkpoint_2.bin")).unwrap();
        let n = bytes.len();
        bytes[n - 1] ^= 0xFF;
        std::fs::write(dir.path().join("checkpoint_2.bin"), &bytes).unwrap();

        let chain = find_head_chain(dir.path()).unwrap();
        let err = load_chain(&chain, &registry).unwrap_err();
        assert!(matches!(err, Error::CheckpointCorrupted(_)), "unexpected: {err:?}");
    }

    #[test]
    fn a_missing_mid_chain_delta_fails_loudly_rather_than_recovering_stale_data() {
        // full at v1, delta at v2, delta at v3; delete v2
        std::fs::remove_file(dir.path().join("checkpoint_2.bin")).unwrap();
        let err = find_head_chain(dir.path()).unwrap_err();
        assert!(
            matches!(err, Error::CheckpointChainBroken { head: 3, missing: 2 }),
            "unexpected: {err:?}"
        );
    }

    #[test]
    fn a_dropped_table_does_not_reappear_after_chain_replay() {
        // full at v1 with tables a,b; delta at v2 dropping b
        let snap = load_chain(&find_head_chain(dir.path()).unwrap(), &registry).unwrap();
        assert!(!snap.tables.contains_key("b"));
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib checkpoint::tests::a_chain --features persistence`
Expected: FAIL — `cannot find function 'find_head_chain'`.

- [ ] **Step 3: Add the error variant**

In `src/error.rs`, next to `CheckpointCorrupted` (`src/error.rs:130`):

```rust
    /// A delta checkpoint's base is missing, so the head is unrecoverable.
    ///
    /// This is deliberately not recoverable by falling back to an older full
    /// checkpoint: the WAL has already been pruned to the head version, so an
    /// older full plus the surviving WAL does not reconstruct the committed
    /// state. Silently recovering less data than was committed is worse than
    /// refusing to start.
    #[error("checkpoint chain broken: head {head} needs base {missing}, which is missing")]
    CheckpointChainBroken { head: u64, missing: u64 },
```

Match the surrounding variants' attribute style (this crate uses `thiserror`; confirm by reading `src/error.rs:110-145` before copying the `#[error(...)]` line).

- [ ] **Step 4: Implement `find_head_chain`**

Reuse `find_latest_checkpoint`'s directory scan (`src/checkpoint.rs:182`) to find the highest version, then walk backwards: read each file's header only (`kind` and `base_version`), stopping at the first `Full`. Missing ancestor → `CheckpointChainBroken`. Return base-first.

Add `fn read_header(path: &Path) -> Result<(CheckpointKind, u64, Option<u64>)>` that reads just the fixed-size prefix rather than the whole file — a chain walk should not read N full checkpoints to learn their kinds.

- [ ] **Step 5: Implement `load_chain`**

Load the base with the existing full-checkpoint path, then for each delta in order apply table entries:

- `Unchanged` → keep the accumulated table.
- `Full` → replace via `info.deserialize_table`.
- `Dropped` → remove from the map.
- `Delta` → apply per-key ops onto the accumulated table using the **existing** `TableInfo::replay_insert` and `TableInfo::replay_delete` closures — the same ones WAL replay uses. There is no new per-key deserialization machinery to write; a `Put` is a `replay_insert` at an encoded key and a `Del` is a `replay_delete`. Verify the key-type code in the delta header against `info.key_type_code` first and error on mismatch, exactly as WAL replay does.

Set the resulting `Snapshot { version }` to the *head* version, not the base's.

- [ ] **Step 6: Point recovery at the chain**

In `src/store.rs:1049`, replace the `find_latest_checkpoint` / `load_checkpoint` pair with `find_head_chain` / `load_chain`, keeping the surrounding `latest_version` / `next_version` bookkeeping (`src/store.rs:1056-1062`) unchanged.

- [ ] **Step 7: Run the tests**

Run: `cargo test --features persistence`
Expected: PASS, whole suite.

- [ ] **Step 8: Commit**

```bash
cargo clippy --all-targets --features persistence -- -D warnings
git add src/checkpoint.rs src/error.rs src/store.rs
git commit -m "feat(checkpoint): chain-aware recovery

Delta application reuses replay_insert/replay_delete — the same closures WAL
replay drives — so there is no second per-key decode path to keep in sync.

A broken chain is a hard error. It cannot fall back to an older full: the WAL
is pruned to the head, so the older full plus surviving WAL recovers less than
was committed, silently."
```

---

### Task 7: `Store::checkpoint()` integration

**Files:**
- Modify: `src/store.rs:942-995` (`checkpoint`), `src/store.rs:194` area (builder), `StoreConfig` struct, `StoreInner` (base-snapshot field)
- Modify: `src/store.rs:1607` (`checkpoint_and_prune_after_bulk`)
- Modify: `src/checkpoint.rs:249` (`cleanup_old_checkpoints`)
- Test: `tests/store_integration.rs`

**Interfaces:**
- Consumes: `write_delta_checkpoint` (Task 5), `find_head_chain` (Task 6)
- Produces:
  - `StoreConfig::checkpoint_chain_max: usize` (default `1`)
  - `StoreConfigBuilder::checkpoint_chain_max(self, n: usize) -> Self`

**Base retention — a correction to the spec.** The spec says `checkpoint()` takes a `VersionPin`. Reading `src/store.rs:687` and `src/store.rs:2130`, `VersionPin` is a newtype over `Arc<Snapshot>`, and `checkpoint()` already holds exactly that `Arc<Snapshot>` (`src/store.rs:961`). Holding the `Arc` *is* the pin — `gc()` evicting the version from the map does not drop an `Arc` someone else holds. So store `Arc<Snapshot>` directly and skip the wrapper; the mechanism and the memory cost are identical.

- [ ] **Step 1: Write the failing tests**

These go in `tests/persistence_integration.rs`, which already has the harness: `common::test_scratch::scratch_dir()`, the `User` record, `standalone_config(dir, durability)`, and `open_store(config)` (which does `Store::new` + `register_table::<User>` + `recover`). Use them — do not introduce a second set of helpers.

Two new local helpers are needed; put them next to `standalone_config`:

```rust
/// `standalone_config` plus a chain length. Kept separate so the existing
/// tests keep exercising the default.
fn chained_config(dir: &Path, durability: Durability, chain_max: usize) -> StoreConfig {
    StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.to_path_buf(),
            durability,
            WalWrite::PerEntry,
        ))
        .checkpoint_chain_max(chain_max)
        .build()
}

fn checkpoint_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("checkpoint_") && n.ends_with(".bin"))
        .collect();
    names.sort();
    names
}

fn insert_user(store: &Store, name: &str) {
    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<User>("users")
        .unwrap()
        .insert(User { name: name.into(), age: 30 })
        .unwrap();
    wtx.commit().unwrap();
}
```

```rust
#[test]
fn chain_max_one_writes_only_full_checkpoints() {
    let dir = common::test_scratch::scratch_dir();
    let store = open_store(standalone_config(dir.path(), Durability::Consistent));
    for i in 0..3 {
        insert_user(&store, &format!("u{i}"));
        store.checkpoint().unwrap();
    }
    // Default chain_max is 1: every file is a full, so cleanup collapses to one.
    assert_eq!(checkpoint_files(dir.path()).len(), 1);
}

#[test]
fn a_delta_chain_recovers_every_committed_row() {
    let dir = common::test_scratch::scratch_dir();
    let config = chained_config(dir.path(), Durability::Consistent, 4);
    {
        let store = open_store(config.clone());
        for i in 0..10 {
            insert_user(&store, &format!("u{i}"));
            store.checkpoint().unwrap();
        }
        assert!(
            checkpoint_files(dir.path()).len() > 1,
            "chain_max(4) should have left deltas on disk"
        );
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.open_table::<User>("users").unwrap().len(), 10);
    assert_eq!(rtx.version(), 10);
}

#[test]
fn the_first_checkpoint_after_recovery_is_full() {
    let dir = common::test_scratch::scratch_dir();
    let config = chained_config(dir.path(), Durability::Consistent, 8);
    {
        let store = open_store(config.clone());
        insert_user(&store, "alice");
        store.checkpoint().unwrap();
    }

    // recover() leaves no in-memory base, so a delta is impossible here even
    // though chain_max would otherwise allow one.
    let store2 = open_store(config);
    insert_user(&store2, "bob");
    let v = store2.checkpoint().unwrap();
    let raw = std::fs::read(dir.path().join(format!("checkpoint_{v}.bin"))).unwrap();
    assert_eq!(raw[8], 0, "kind byte must be Full (0), got {}", raw[8]);
}

#[test]
fn a_checkpoint_after_bulk_load_is_full() {
    // bulk_load installs a wholly new tree, so a diff would rewrite every row
    // anyway — going straight to full is cheaper and simpler.
    let dir = common::test_scratch::scratch_dir();
    let config = chained_config(dir.path(), Durability::Consistent, 8);
    let store = open_store(config);
    insert_user(&store, "alice");
    store.checkpoint().unwrap();

    let rows = vec![(1u64, User { name: "loaded".into(), age: 1 })];
    store.bulk_load::<User>("users", rows, true).unwrap();

    let v = store.checkpoint().unwrap();
    let raw = std::fs::read(dir.path().join(format!("checkpoint_{v}.bin"))).unwrap();
    assert_eq!(raw[8], 0, "kind byte must be Full (0) after bulk_load");
}

#[test]
fn cleanup_never_deletes_an_ancestor_of_the_head() {
    let dir = common::test_scratch::scratch_dir();
    let store = open_store(chained_config(dir.path(), Durability::Consistent, 4));
    for i in 0..3 {
        insert_user(&store, &format!("u{i}"));
        store.checkpoint().unwrap();
    }
    // full@v1 + delta@v2 + delta@v3 — deleting any of the three makes the
    // head unrecoverable, and the WAL is already pruned past them.
    assert_eq!(checkpoint_files(dir.path()).len(), 3);
}
```

The `raw[8]` offset assumes `MAGIC` (4 bytes) + a bincode-varint `FORMAT_VERSION` of 1 byte + ... — **verify it against the Task 4 layout before relying on it**, and if the header is not fixed-width at that point, add a `pub(crate)` test-only `read_header` accessor rather than hard-coding an offset. Confirm `bulk_load`'s exact signature in `src/bulk_load.rs` before writing the call.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test store_integration --features persistence chain`
Expected: FAIL — no method `checkpoint_chain_max` on the builder.

- [ ] **Step 3: Add the config knob**

In the `StoreConfig` struct, with a doc comment stating the trade:

```rust
    /// Maximum length of a checkpoint chain: one full checkpoint followed by
    /// at most `checkpoint_chain_max - 1` deltas.
    ///
    /// `1` (the default) means every checkpoint is a full one — the behaviour
    /// before incremental checkpoints existed, and the only setting that
    /// retains no base snapshot. Higher values make `checkpoint()` cost track
    /// change volume instead of dataset size, at the cost of keeping the last
    /// checkpointed snapshot alive in memory and lengthening recovery.
    pub checkpoint_chain_max: usize,
```

Default `1` in the `Default` impl, plus the builder setter following the shape at `src/store.rs:194`. Reject `0` in `Store::new` with `Error::Persistence("checkpoint_chain_max must be >= 1")` — `0` has no sensible reading.

- [ ] **Step 4: Track the base snapshot**

Add to `StoreInner`:

```rust
    /// The snapshot the last checkpoint wrote, held so the next checkpoint can
    /// diff against it. Holding the Arc is what keeps it alive across `gc()`.
    /// `None` means the next checkpoint must be full: at startup, after
    /// recovery, and after a bulk load there is nothing to diff against.
    #[cfg(feature = "persistence")]
    checkpoint_base: Option<Arc<Snapshot>>,
```

Guard it with the chain-position counter (`checkpoint_chain_len: usize`) so a full is forced every `checkpoint_chain_max` files. Both live under the existing `checkpoint_lock` discipline — `checkpoint()` already serializes against itself (`src/store.rs:944`).

- [ ] **Step 5: Branch in `checkpoint()`**

Between the snapshot grab (`src/store.rs:961`) and `write_checkpoint` (`src/store.rs:964`):

```rust
        let base = if self.config_chain_max() > 1 {
            let inner = self.inner.read();
            inner.checkpoint_base.clone().filter(|b| {
                inner.checkpoint_chain_len < self.config_chain_max()
            })
        } else {
            None
        };

        let version = match &base {
            Some(base) => crate::checkpoint::write_delta_checkpoint(&dir, &snap, base, &registry)?,
            None => crate::checkpoint::write_checkpoint(&dir, &snap, &registry)?,
        };
```

Then update `checkpoint_base = Some(snap.clone())` and bump or reset `checkpoint_chain_len`. Leave the WAL prune (`src/store.rs:968-990`) exactly where it is: **prune only after the file is durable**, which the existing ordering already guarantees. Set `checkpoint_base = None` in `recover()` and in `checkpoint_and_prune_after_bulk` (`src/store.rs:1607`).

- [ ] **Step 6: Make cleanup chain-aware**

`cleanup_old_checkpoints` (`src/checkpoint.rs:249`) currently keeps its never-delete-newer rule; **add** the ancestor rule rather than replacing it. New signature:

```rust
pub(crate) fn cleanup_old_checkpoints(
    dir: &Path,
    keep_version: u64,
    chain: &[PathBuf],
) -> Result<()>
```

Skip any file in `chain` (the head's ancestors, from `find_head_chain`) and any file newer than `keep_version`. Update the doc comment: the existing one explains *why* newer files are spared, and the ancestor rule needs the same treatment.

The one call site is in `checkpoint()` (`src/store.rs:992`) and must now pass the chain. Call `find_head_chain(&dir)?` immediately after the write and before the cleanup — reading the directory once more is cheap next to the fsync that just happened, and it keeps cleanup driven by what is actually on disk rather than by the in-memory chain counter, which is the state that a crash can desynchronise.

- [ ] **Step 7: Run the tests**

Run: `cargo test --features persistence`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cargo clippy --all-targets --features persistence -- -D warnings
git add src/store.rs src/checkpoint.rs
git commit -m "feat(store): checkpoint_chain_max, base retention, chain-aware cleanup

Default 1 keeps today's behaviour exactly and holds no base snapshot; the
memory cost of incremental checkpointing is opt-in and bounded by the knob.

Not a VersionPin: checkpoint() already holds the Arc<Snapshot>, and holding
the Arc IS the pin — gc() evicting the version cannot drop it. VersionPin is
a newtype over the same Arc, so the wrapper would buy nothing.

Full checkpoints are forced where a diff is impossible or pointless: after
recovery (no in-memory base) and after bulk_load (wholly new tree)."
```

---

### Task 8: Chain-equivalence proptest and lifecycle coverage

**Files:**
- Create: `tests/checkpoint_chain_equivalence.rs`
- Test: itself

**Interfaces:**
- Consumes: everything above
- Produces: nothing consumed by later tasks

**Why:** Task 1's oracle proves `diff` is right about *trees*. This proves the whole pipeline — diff, encode, chain, replay — is right about *stores*. It is the test that would catch a key dropped anywhere between the B-tree and the recovered snapshot.

- [ ] **Step 1: Write the property**

```rust
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// A store recovered from a delta chain must be indistinguishable from
    /// one recovered from a full checkpoint taken at the same version.
    #[test]
    fn chain_recovery_matches_full_recovery(
        ops in prop::collection::vec(store_op(), 1..80),
        chain_max in 1usize..6,
        checkpoint_every in 1usize..7,
    ) {
        let chained = run_workload(&ops, chain_max, checkpoint_every)?;
        let full = run_workload(&ops, 1, checkpoint_every)?;
        prop_assert_eq!(chained.rows, full.rows);
        prop_assert_eq!(chained.version, full.version);
    }
}
```

`run_workload` builds a store in a fresh `tempfile::tempdir()`, applies the ops (insert / update / delete across two tables), checkpoints every `checkpoint_every` ops, drops the store, recovers into a new one, and returns the full row set plus `latest_version`. Case count is 64 rather than 256 because each case does real file I/O.

- [ ] **Step 2: Run it, expect it to pass**

Run: `cargo test --test checkpoint_chain_equivalence --features persistence`
Expected: PASS. If it fails, the shrunk counterexample is the bug report — fix the implementation, not the property.

- [ ] **Step 3: Add the lifecycle cases**

Deterministic tests, not properties (proptest will not generate DDL sequences densely enough to hit these). Same harness as Task 7 — `common::test_scratch::scratch_dir()`, `chained_config`, `open_store`, `insert_user`:

```rust
#[test]
fn a_table_created_between_checkpoints_appears_in_full_in_the_delta() {
    let dir = common::test_scratch::scratch_dir();
    let config = chained_config(dir.path(), Durability::Consistent, 8);
    let store = Store::new(config.clone()).unwrap();
    store.register_table::<User>("users").unwrap();
    store.register_table::<User>("admins").unwrap();
    store.recover().unwrap();

    insert_user(&store, "alice");                 // only "users" exists so far
    store.checkpoint().unwrap();

    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<User>("admins")
        .unwrap()
        .insert(User { name: "root".into(), age: 99 })
        .unwrap();
    wtx.commit().unwrap();
    store.checkpoint().unwrap();
    drop(store);

    let store2 = Store::new(config).unwrap();
    store2.register_table::<User>("users").unwrap();
    store2.register_table::<User>("admins").unwrap();
    store2.recover().unwrap();
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.open_table::<User>("admins").unwrap().len(), 1);
    assert_eq!(rtx.open_table::<User>("users").unwrap().len(), 1);
}

#[test]
fn an_index_defined_between_checkpoints_survives_chain_recovery() {
    // Checkpoints store data + next_id only; indexes are rebuilt on load via
    // rebuild_from_sorted_data. Deltas must not change that.
    let dir = common::test_scratch::scratch_dir();
    let config = chained_config(dir.path(), Durability::Consistent, 8);
    {
        let store = open_store(config.clone());
        insert_user(&store, "alice");
        store.checkpoint().unwrap();

        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone())
            .unwrap();
        wtx.commit().unwrap();

        insert_user(&store, "bob");
        store.checkpoint().unwrap();
    }

    let store2 = open_store(config);
    let rtx = store2.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert!(table.get_unique("by_name", &"bob".to_string()).unwrap().is_some());
    assert!(table.get_unique("by_name", &"alice".to_string()).unwrap().is_some());
}
```

Two cases from the spec need their reachability confirmed before they are written as assertions: **table drop** and **drop-then-recreate-with-a-different-key-type**. Read `docs/tasks/task59_table_lifecycle_races.md` and `tests/table_lifecycle_races.rs` first — if the store has no public table-drop API, the `Dropped` entry kind is only reachable through `register_table` differences across restarts, and the test must be written at the `serialize_delta` level (Task 5 already covers that shape) rather than through the `Store` API. Do not write a `Store`-level test for an interleaving the API cannot produce; note the finding in the task doc instead.

`define_index`'s exact signature and the `IndexKind` import are in `src/table.rs` and `tests/store_integration.rs:494` — copy from there rather than from this plan if they disagree.

- [ ] **Step 4: Add the downgrade test**

An old binary cannot be linked into this test, so simulate what it does: rewrite the head file's `format_version` field to `1` and assert the loader refuses it. That exercises the same code path an old binary would take — the version check — without needing the old binary.

```rust
#[test]
fn a_delta_headed_directory_is_refused_rather_than_read_as_stale() {
    let dir = common::test_scratch::scratch_dir();
    let config = chained_config(dir.path(), Durability::Consistent, 8);
    {
        let store = open_store(config.clone());
        for i in 0..3 {
            insert_user(&store, &format!("u{i}"));
            store.checkpoint().unwrap();
        }
    }
    // The head is a delta. Stamp it with the pre-incremental format version:
    // an old binary would pick this same file as "latest" and must fail on it,
    // not silently fall back to the full whose WAL tail is already pruned.
    let head = dir.path().join("checkpoint_3.bin");
    let mut raw = std::fs::read(&head).unwrap();
    raw[4] = 1; // format_version -> 1; see the Task 4 layout
    std::fs::write(&head, &raw).unwrap();

    let store2 = Store::new(config).unwrap();
    store2.register_table::<User>("users").unwrap();
    let err = store2.recover().unwrap_err();
    assert!(
        matches!(err, Error::CheckpointCorrupted(_)),
        "a downgraded read must fail loudly, got {err:?}"
    );
}
```

The `raw[4]` offset carries the same caveat as Task 7's `raw[8]`: verify it against the Task 4 layout, and prefer a test-only header accessor if the prefix is not fixed-width. Note that flipping the version byte also invalidates the CRC, so the error may surface as a CRC mismatch rather than a version mismatch — both are `CheckpointCorrupted`, and both are the loud failure this test is about, so the `matches!` above is deliberately on the variant and not the message.

- [ ] **Step 5: Commit**

```bash
cargo clippy --all-targets --features persistence -- -D warnings
git add tests/checkpoint_chain_equivalence.rs
git commit -m "test(checkpoint): chain recovery is indistinguishable from full recovery

Task 1's oracle proves diff is right about trees; this proves the whole
pipeline is right about stores. Lifecycle and downgrade cases are
deterministic — proptest will not generate DDL sequences densely enough."
```

---

### Task 9: Benchmark and task doc

**Files:**
- Create: `benches/checkpoint_delta.rs`
- Modify: `Cargo.toml` (`[[bench]]` entry)
- Create: `docs/tasks/task61_incremental_checkpoints.md`
- Modify: `CLAUDE.md` (persistence section: the new knob)

**Interfaces:**
- Consumes: everything above
- Produces: the canonical per-feature doc

- [ ] **Step 1: Write the benchmark**

Criterion bench over the crossover question, since that is what sets the default: fixed dataset (100k rows), vary the fraction of rows dirtied between checkpoints (1%, 10%, 50%, 100%), measure `checkpoint()` wall time and bytes written for `chain_max = 1` vs `chain_max = 8`. A 100%-dirty delta is expected to be *worse* than a full — it pays the diff plus per-key framing — and the bench should show where that crossover sits.

Second bench: recovery time vs chain length (1, 2, 4, 8, 16 files).

- [ ] **Step 2: Run it locally for correctness only**

Run: `cargo bench --bench checkpoint_delta --features persistence`
Expected: completes without panicking. **Record no numbers.** The sandbox noise floor is ±2x; a local A/B here is meaningless.

- [ ] **Step 3: Run it on the bench host**

This provisions real billable AWS resources — **get explicit authorization from Peter first**, and do not run it on your own initiative.

```bash
cd bench-infra && make bench-oneshot TARGET=autobench
make status     # confirm nothing is left running
```

Results land in `bench-out/dist/<ts>/`.

- [ ] **Step 4: Write the task doc**

`docs/tasks/task61_incremental_checkpoints.md`, following the shape of `docs/tasks/task37_wal_preallocation.md`. It must record: the CoW-diff insight and why it beats WiscKey-style KV separation here; the format; the base-retention memory trade; why a broken chain is fatal; the measured crossover; and the **recommended `checkpoint_chain_max` default**, justified by the measured recovery-time curve. If the numbers argue for a default above 1, say so and change it in a follow-up — do not change the default on the strength of an unmeasured guess.

- [ ] **Step 5: Update CLAUDE.md**

One sentence in the Persistence bullet naming `checkpoint_chain_max`, its default, and the doc reference. Match the density of the surrounding bullets.

- [ ] **Step 6: Final verification**

```bash
cargo test
cargo test --features persistence
cargo clippy --all-targets --features persistence -- -D warnings
cargo test -p ultima-vector
```

The workspace check matters: a root `cargo test` misses member crates.

- [ ] **Step 7: Commit and open the PR**

```bash
git add benches/checkpoint_delta.rs Cargo.toml docs/tasks/task61_incremental_checkpoints.md CLAUDE.md
git commit -m "docs(task61): incremental checkpoints — measurements and canonical doc"
git push -u origin feat/incremental-checkpoints
gh pr create --fill
```

---

## Notes for the reviewer

Three places where this plan deliberately departs from the spec:

1. **No `VersionPin`.** `checkpoint()` already holds the `Arc<Snapshot>`; holding it *is* the pin. `VersionPin` is a newtype over the same `Arc`, so using it would add a type without adding a guarantee (Task 7).
2. **`Error::TableTypeChanged` instead of a string match.** The spec described detecting a recreated-with-a-different-type table via the `diff_table` downcast failing; doing that by matching on an error *message* is fragile, so it gets a variant (Task 5, Step 3).
3. **`proptest` is a new dev-dependency.** The repo currently randomizes with `rand` and fixed seeds. Shrinking is worth the dependency for a diff whose failure mode is a silently dropped key, but it is a judgement call worth confirming (Task 1, Step 1).

The riskiest task is 7, because it touches the WAL prune ordering and the cleanup invariant — the two places where a mistake loses committed data rather than failing a test.
