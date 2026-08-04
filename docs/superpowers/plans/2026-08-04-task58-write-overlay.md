# Write Overlay (task58) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the per-transaction O(height×T) B-tree copy-on-write clone chain with a bounded flat overlay, so an eventual-tier commit costs a ~100 ns memcpy instead of ~2 µs of node cloning.

**Architecture:** `Table<R, K>` gains an `Overlay<R, K>` — a sorted `Arc<Vec<(K, OverlayOp<R>)>>` capped at `OVERLAY_CAP` (128) plus an `i64` length delta. Writes land in the overlay; when it fills, one batched pass replays it into the tree. Every read merges overlay-over-tree through a single choke point. The overlay is volatile acceleration only: WAL ops stay logical and unchanged, and no on-disk format changes.

**Tech Stack:** Rust, `Arc<Vec<_>>` with `Arc::make_mut`, the existing `BTree<K, V>` (`src/btree.rs`), criterion benches.

**Design spec:** `docs/superpowers/specs/2026-08-03-write-overlay-design.md` — read it before Task 1. This plan implements it; where the two disagree, the spec's *intent* governs and the discrepancy should be raised, but note the two additions in "Corrections to the spec" below.

## Global Constraints

- **SingleWriter stores only.** MultiWriter never enables overlays. `merge_keys_from` (`src/table.rs:166`) is untouched and carries a `debug_assert!` tripwire.
- **Non-indexed tables only.** `define_index`/`define_custom_index` flush the overlay and set `overlay_disabled`; indexed tables use today's write path verbatim.
- **No public API change and no config surface.** `OVERLAY_CAP` is an internal `const`, env-overridable for bench tuning only (task57 precedent).
- **No on-disk format change.** Checkpoints and WAL are byte-compatible with today.
- `cargo clippy --features persistence,fulltext --all-targets -- -D warnings` must pass with zero warnings.
- Run tests as `cargo test --features persistence,fulltext`. **CI also runs the `metrics` feature** — before the final commit, verify `cargo test --features persistence,fulltext,metrics` and `cargo clippy --features persistence,fulltext,metrics --all-targets -- -D warnings`.
- Do not run `cargo fmt` (repo-wide rustfmt-version drift; match surrounding style instead).
- `tests/store_integration.rs::concurrent_same_table_overlapping_keys_with_retry` is a known flake (~1 in 5, fails with `got 0` conflicts). Re-run it isolated before treating a failure as yours.

## Corrections to the spec, found by auditing the code

The spec says the correctness linchpin is that "no direct `self.data.iter()`/range remains outside this choke point." The audit found **17** such sites, two of which the spec does not mention. Both are handled in this plan:

1. **`collect_serialized_rows` (`src/table.rs:208-209`)** uses `self.data.len()` and `self.data.range(..)` — this is the *checkpoint serialization* path. Unmerged, a checkpoint taken with a non-empty overlay silently omits every overlay row. Task 3.
2. **`self.data.max_key()` (`src/table.rs:977`)** guards the task51 bulk-append fast path (`if self.data.max_key().is_none_or(|k| *k < start_id)`). With an overlay, the tree's max key is not the table's max key, so the fast path could seed a `BulkBuilder` below a live overlay row. Task 3 resolves it by flushing before the fast path rather than by merging the comparison.

There is also exactly one `pub(crate) fn data_ref(&self) -> &BTree<K, R>` (`src/table.rs:348`), with a single caller: `materialize_delta` at `src/store.rs:1393`. Task 3 covers it.

---

## File Structure

- **Create `src/overlay.rs`** — `OverlayOp<R>`, `Overlay<R, K>`, `OVERLAY_CAP`, and the `MergedRange` iterator. One responsibility: the overlay data structure and the merge. Isolated so it is unit-testable without a `Table`.
- **Modify `src/table.rs`** — `Table` and `TableSnapshot` gain the overlay fields; the read choke point and the write path.
- **Modify `src/store.rs:1393`** — the one `data_ref()` caller.
- **Modify `src/lib.rs`** — `mod overlay;`.
- **Create `tests/overlay_equivalence.rs`** — the differential property test (the centerpiece).
- **Modify `benches/`** — the `perf_decomp` overlay cell.
- **Create `docs/tasks/task58_write_overlay.md`** — the canonical feature record.

**Ordering principle:** Tasks 1–3 add the overlay and make the whole codebase overlay-aware **while the overlay is never populated**. Every existing test must pass unchanged throughout. Task 4 is the single commit that turns writes on. This means a bisect lands on Task 4 for any behavioural regression, and Tasks 1–3 are provably inert.

---

### Task 1: The `Overlay` type and the merged iterator

**Files:**
- Create: `src/overlay.rs`
- Modify: `src/lib.rs` (add `mod overlay;`)

**Interfaces:**
- Produces: `OVERLAY_CAP: usize`, `OverlayOp<R>`, `Overlay<R, K>` with `new()`, `is_empty()`, `len()`, `get(&K) -> Option<&OverlayOp<R>>`, `put(K, Arc<R>)`, `tombstone(K)`, `remove_entry(&K) -> bool`, `entries() -> &[(K, OverlayOp<R>)]`, `len_delta() -> i64`, `adjust_len(i64)`, `clear()`; and `MergedRange<'a, K, R>` implementing `Iterator<Item = (&'a K, &'a R)>`.

- [ ] **Step 1: Write the failing tests**

Create `src/overlay.rs` with only this test module at the bottom (the code above it comes in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn put_keeps_entries_sorted_and_replaces_in_place() {
        let mut o: Overlay<i32, u64> = Overlay::new();
        o.put(5, Arc::new(50));
        o.put(1, Arc::new(10));
        o.put(3, Arc::new(30));
        o.put(1, Arc::new(11)); // replace, not append
        let keys: Vec<u64> = o.entries().iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![1, 3, 5], "entries stay sorted with no duplicate key");
        match o.get(&1) {
            Some(OverlayOp::Put(r)) => assert_eq!(**r, 11, "replacement won"),
            other => panic!("expected Put(11), got {other:?}"),
        }
    }

    #[test]
    fn tombstone_shadows_and_remove_entry_erases() {
        let mut o: Overlay<i32, u64> = Overlay::new();
        o.put(2, Arc::new(20));
        o.tombstone(2);
        assert!(matches!(o.get(&2), Some(OverlayOp::Tombstone)));
        assert!(o.remove_entry(&2), "erasing a present key reports true");
        assert!(o.get(&2).is_none(), "erased outright, not tombstoned");
        assert!(!o.remove_entry(&2), "erasing an absent key reports false");
    }

    #[test]
    fn len_delta_tracks_adjustments_and_clear_resets() {
        let mut o: Overlay<i32, u64> = Overlay::new();
        o.put(1, Arc::new(10));
        o.adjust_len(1);
        o.tombstone(2);
        o.adjust_len(-1);
        assert_eq!(o.len_delta(), 0);
        o.clear();
        assert_eq!(o.len_delta(), 0);
        assert!(o.is_empty());
    }

    /// The merge is the correctness core: overlay wins ties, tombstones
    /// swallow the tree's row, and both sides may run out first.
    #[test]
    fn merged_range_overlays_ties_and_swallows_tombstones() {
        let tree: Vec<(u64, i32)> = vec![(1, 10), (2, 20), (3, 30), (5, 50)];
        let mut o: Overlay<i32, u64> = Overlay::new();
        o.put(2, Arc::new(222)); // shadows the tree's 20
        o.tombstone(3);          // hides the tree's 30
        o.put(4, Arc::new(40));  // interleaves between tree rows
        o.put(6, Arc::new(60));  // runs past the tree's end

        let got: Vec<(u64, i32)> = MergedRange::new(tree.iter().map(|(k, v)| (k, v)), o.entries())
            .map(|(k, v)| (*k, *v))
            .collect();
        assert_eq!(got, vec![(1, 10), (2, 222), (4, 40), (5, 50), (6, 60)]);
    }

    #[test]
    fn merged_range_with_empty_overlay_is_the_tree() {
        let tree: Vec<(u64, i32)> = vec![(1, 10), (2, 20)];
        let o: Overlay<i32, u64> = Overlay::new();
        let got: Vec<(u64, i32)> = MergedRange::new(tree.iter().map(|(k, v)| (k, v)), o.entries())
            .map(|(k, v)| (*k, *v))
            .collect();
        assert_eq!(got, vec![(1, 10), (2, 20)]);
    }

    #[test]
    fn merged_range_with_empty_tree_is_the_overlay_minus_tombstones() {
        let tree: Vec<(u64, i32)> = vec![];
        let mut o: Overlay<i32, u64> = Overlay::new();
        o.put(1, Arc::new(10));
        o.tombstone(2);
        let got: Vec<(u64, i32)> = MergedRange::new(tree.iter().map(|(k, v)| (k, v)), o.entries())
            .map(|(k, v)| (*k, *v))
            .collect();
        assert_eq!(got, vec![(1, 10)]);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --features persistence,fulltext --lib overlay::`
Expected: FAIL — `cannot find type Overlay in this scope` (nothing is implemented yet).

- [ ] **Step 3: Implement the overlay**

Put this **above** the test module in `src/overlay.rs`:

```rust
//! Bounded flat write overlay (task58).
//!
//! Every commit on a shared table used to pay an O(height x T) copy-on-write
//! clone chain, because the writer's tree shares nodes with the latest
//! snapshot (~2 us/txn measured against a ~250 ns tree-op floor). The overlay
//! absorbs writes into a small sorted vec instead: after a commit shares the
//! vec with the snapshot, the next write's `Arc::make_mut` copies at most
//! `OVERLAY_CAP` entries (~2 KB, ~100 ns) rather than cloning tree nodes.
//! Every `OVERLAY_CAP` writes, one batched pass replays the vec into the tree.
//!
//! Substituting a *bounded* copy for an unbounded-fanout node-clone chain is
//! the entire trick. See `docs/tasks/task58_write_overlay.md`.

use std::sync::Arc;

/// Maximum entries held before a write triggers a flush.
///
/// Bounds both the per-write `make_mut` copy (~2 KB at 128) and the worst-case
/// flush latency (~100-200 us for 128 warm inserts), which rides inside one
/// commit. Env-overridable for bench tuning only — never a public config knob
/// (task57 precedent).
pub(crate) fn overlay_cap() -> usize {
    static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("ULTIMA_OVERLAY_CAP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|c| *c > 0)
            .unwrap_or(128)
    })
}

/// What the overlay records for a key.
///
/// A `Tombstone` always shadows a row that really exists in the tree: deleting
/// a row that lives *only* in the overlay erases the entry outright (see
/// `Overlay::remove_entry`). That invariant is what makes flush's
/// `BTree::remove_mut` infallible — every tombstone has something to remove.
#[derive(Debug)]
pub(crate) enum OverlayOp<R> {
    Put(Arc<R>),
    Tombstone,
}

impl<R> Clone for OverlayOp<R> {
    fn clone(&self) -> Self {
        match self {
            OverlayOp::Put(a) => OverlayOp::Put(Arc::clone(a)),
            OverlayOp::Tombstone => OverlayOp::Tombstone,
        }
    }
}

/// Sorted, bounded write buffer sitting in front of a table's `BTree`.
#[derive(Debug)]
pub(crate) struct Overlay<R, K> {
    /// Sorted by `K`, no duplicate keys, `len() <= overlay_cap()`.
    entries: Arc<Vec<(K, OverlayOp<R>)>>,
    /// Merged row count is `tree.len() + len_delta`. Never makes it negative.
    len_delta: i64,
}

impl<R, K> Clone for Overlay<R, K> {
    /// O(1): shares the entry vec. The next mutation pays the bounded
    /// `make_mut` copy, which is the point of the design.
    fn clone(&self) -> Self {
        Overlay { entries: Arc::clone(&self.entries), len_delta: self.len_delta }
    }
}

impl<R, K> Default for Overlay<R, K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R, K: Ord + Clone> Overlay<R, K> {
    pub(crate) fn new() -> Self {
        Overlay { entries: Arc::new(Vec::new()), len_delta: 0 }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn entries(&self) -> &[(K, OverlayOp<R>)] {
        &self.entries
    }

    pub(crate) fn len_delta(&self) -> i64 {
        self.len_delta
    }

    pub(crate) fn adjust_len(&mut self, by: i64) {
        self.len_delta += by;
    }

    pub(crate) fn get(&self, key: &K) -> Option<&OverlayOp<R>> {
        self.entries
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
            .map(|i| &self.entries[i].1)
    }

    pub(crate) fn put(&mut self, key: K, val: Arc<R>) {
        self.set(key, OverlayOp::Put(val));
    }

    pub(crate) fn tombstone(&mut self, key: K) {
        self.set(key, OverlayOp::Tombstone);
    }

    fn set(&mut self, key: K, op: OverlayOp<R>) {
        let v = Arc::make_mut(&mut self.entries);
        match v.binary_search_by(|(k, _)| k.cmp(&key)) {
            Ok(i) => v[i].1 = op,
            Err(i) => v.insert(i, (key, op)),
        }
    }

    /// Erases an entry outright. Used when deleting a row that exists only in
    /// the overlay, so that tombstones always shadow a real tree row.
    pub(crate) fn remove_entry(&mut self, key: &K) -> bool {
        let v = Arc::make_mut(&mut self.entries);
        match v.binary_search_by(|(k, _)| k.cmp(key)) {
            Ok(i) => {
                v.remove(i);
                true
            }
            Err(_) => false,
        }
    }

    /// Drops every entry and re-bases the delta. Called after a flush has
    /// applied the entries to the tree.
    pub(crate) fn clear(&mut self) {
        self.entries = Arc::new(Vec::new());
        self.len_delta = 0;
    }
}

/// Two-pointer merge of a tree range and the sorted overlay slice.
///
/// Overlay wins ties; tombstones swallow the tree's row and yield nothing.
/// With an empty overlay this degenerates to the tree iterator plus one dead
/// branch per step, which is the quiet-store read path.
pub(crate) struct MergedRange<'a, K, R, I>
where
    I: Iterator<Item = (&'a K, &'a R)>,
    K: 'a,
    R: 'a,
{
    tree: std::iter::Peekable<I>,
    overlay: &'a [(K, OverlayOp<R>)],
    idx: usize,
}

impl<'a, K: Ord, R, I> MergedRange<'a, K, R, I>
where
    I: Iterator<Item = (&'a K, &'a R)>,
{
    pub(crate) fn new(tree: I, overlay: &'a [(K, OverlayOp<R>)]) -> Self {
        MergedRange { tree: tree.peekable(), overlay, idx: 0 }
    }
}

impl<'a, K: Ord, R, I> Iterator for MergedRange<'a, K, R, I>
where
    I: Iterator<Item = (&'a K, &'a R)>,
{
    type Item = (&'a K, &'a R);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ov = self.overlay.get(self.idx);
            match (self.tree.peek(), ov) {
                (None, None) => return None,
                // Overlay exhausted: the rest of the tree passes through.
                (Some(_), None) => return self.tree.next(),
                // Tree exhausted: emit the overlay's remaining puts.
                (None, Some((k, op))) => {
                    self.idx += 1;
                    if let OverlayOp::Put(r) = op {
                        return Some((k, r));
                    }
                    // A tombstone past the tree's end shadows nothing; skip.
                }
                (Some((tk, _)), Some((ok, op))) => match (*tk).cmp(ok) {
                    std::cmp::Ordering::Less => return self.tree.next(),
                    std::cmp::Ordering::Greater => {
                        self.idx += 1;
                        if let OverlayOp::Put(r) = op {
                            return Some((ok, r));
                        }
                    }
                    // Tie: the overlay decides, and the tree's row is consumed.
                    std::cmp::Ordering::Equal => {
                        self.tree.next();
                        self.idx += 1;
                        if let OverlayOp::Put(r) = op {
                            return Some((ok, r));
                        }
                    }
                },
            }
        }
    }
}
```

The test module's `MergedRange::new(...)` calls pass `tree.iter().map(...)`, which satisfies `I: Iterator<Item = (&u64, &i32)>`.

Add to `src/lib.rs`, beside the other `mod` declarations:

```rust
mod overlay;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --features persistence,fulltext --lib overlay::`
Expected: PASS, 6 tests.

- [ ] **Step 5: Lint**

Run: `cargo clippy --features persistence,fulltext --all-targets -- -D warnings`
Expected: zero warnings. If clippy objects to the `Clone` impls being manual, keep them — a `#[derive(Clone)]` would add an unwanted `R: Clone` bound.

- [ ] **Step 6: Commit**

```bash
git add src/overlay.rs src/lib.rs
git commit -m "feat(overlay): bounded sorted write overlay + merged iterator (task58)

The structure only, wired to nothing. Overlay wins ties and tombstones
swallow the tree's row; a tombstone always shadows a real tree row, since
deleting an overlay-only key erases the entry instead, which is what makes
flush's remove_mut infallible."
```

---

### Task 2: Rename `data` to `tree` and route every read through the choke point

**Files:**
- Modify: `src/table.rs` (the field at :267, `TableSnapshot` at :291, and all 41 `self.data` uses)

**Interfaces:**
- Consumes: `Overlay`, `OverlayOp`, `MergedRange` from Task 1.
- Produces: on `Table<R, K>` — private field `tree: BTree<K, R>`, private field `overlay: Overlay<R, K>`, private field `overlay_disabled: bool`; and the merged accessors `fn merged_get(&self, &K) -> Option<&R>`, `fn merged_get_arc(&self, &K) -> Option<Arc<R>>`, `fn merged_range<'a>(&'a self, impl RangeBounds<K> + 'a) -> impl Iterator<Item = (&'a K, &'a R)> + 'a`, `fn merged_len(&self) -> usize`.

**Why the rename:** the spec's stated linchpin is that no direct tree access survives outside the choke point. Renaming the field makes the compiler enumerate all 41 sites, so none can be missed by reading. This task is a **pure refactor** — the overlay is constructed empty and never written, so behaviour is identical and every existing test must pass untouched.

- [ ] **Step 1: Rename the field and add the overlay fields**

In `src/table.rs`, change the struct at :266-:287 so `data` becomes `tree`, and add two fields:

```rust
pub struct Table<R, K = u64> {
    tree: BTree<K, R>,
    /// Bounded write buffer in front of `tree` (task58). Always empty for
    /// indexed tables and for MultiWriter stores.
    overlay: crate::overlay::Overlay<R, K>,
    /// Set once an index is defined on this table: the index maintainers read
    /// through the write path, so overlaid rows would be invisible to them.
    /// Once true, never cleared — the table uses the pre-task58 write path.
    overlay_disabled: bool,
    next_id: Option<K>,
    indexes: BTreeMap<String, Box<dyn IndexMaintainer<R, K>>>,
    // ... remaining fields unchanged
}
```

Apply the same rename to `TableSnapshot` at :290-:294 and add the two fields:

```rust
struct TableSnapshot<R, K = u64> {
    tree: BTree<K, R>,
    overlay: crate::overlay::Overlay<R, K>,
    overlay_disabled: bool,
    next_id: Option<K>,
    indexes: BTreeMap<String, Box<dyn IndexMaintainer<R, K>>>,
}
```

- [ ] **Step 2: Run the build to enumerate every site**

Run: `cargo check --features persistence,fulltext 2>&1 | grep -c "no field \`data\`"`
Expected: a non-zero count — this is your work list. Every constructor also needs the two new fields.

- [ ] **Step 3: Add the merged accessors**

Add these to the `impl<R: Record, K: PrimaryKey> Table<R, K>` block:

```rust
    /// Overlay-then-tree lookup. One branch when the overlay is empty.
    fn merged_get(&self, key: &K) -> Option<&R> {
        if !self.overlay.is_empty() {
            match self.overlay.get(key) {
                Some(crate::overlay::OverlayOp::Put(r)) => return Some(r),
                Some(crate::overlay::OverlayOp::Tombstone) => return None,
                None => {}
            }
        }
        self.tree.get(key)
    }

    /// `Arc` flavour of [`Self::merged_get`], for the write paths that need to
    /// hand the old row to index maintainers.
    fn merged_get_arc(&self, key: &K) -> Option<Arc<R>> {
        if !self.overlay.is_empty() {
            match self.overlay.get(key) {
                Some(crate::overlay::OverlayOp::Put(r)) => return Some(Arc::clone(r)),
                Some(crate::overlay::OverlayOp::Tombstone) => return None,
                None => {}
            }
        }
        self.tree.get_arc(key)
    }

    /// THE choke point. Every multi-row read in the crate goes through here:
    /// `range`, `iter`, first/last, checkpoint serialization, index rebuild.
    /// Adding a caller of `self.tree.range` outside this method reintroduces
    /// the bug this design exists to avoid.
    fn merged_range<'a>(
        &'a self,
        range: impl RangeBounds<K> + 'a,
    ) -> impl Iterator<Item = (&'a K, &'a R)> + 'a {
        crate::overlay::MergedRange::new(self.tree.range(range), self.overlay.entries())
    }

    fn merged_len(&self) -> usize {
        let n = self.tree.len() as i64 + self.overlay.len_delta();
        debug_assert!(n >= 0, "merged len went negative: {n}");
        n.max(0) as usize
    }
```

**Note on `merged_range` and bounded ranges:** `MergedRange` walks the *whole* overlay slice, so a bounded `range(a..b)` would wrongly emit overlay rows outside `[a, b)`. Restrict the slice before constructing it:

```rust
    fn merged_range<'a>(
        &'a self,
        range: impl RangeBounds<K> + 'a,
    ) -> impl Iterator<Item = (&'a K, &'a R)> + 'a {
        let ov = self.overlay.entries();
        let start = match range.start_bound() {
            std::ops::Bound::Unbounded => 0,
            std::ops::Bound::Included(k) => ov.partition_point(|(ek, _)| ek < k),
            std::ops::Bound::Excluded(k) => ov.partition_point(|(ek, _)| ek <= k),
        };
        let end = match range.end_bound() {
            std::ops::Bound::Unbounded => ov.len(),
            std::ops::Bound::Included(k) => ov.partition_point(|(ek, _)| ek <= k),
            std::ops::Bound::Excluded(k) => ov.partition_point(|(ek, _)| ek < k),
        };
        crate::overlay::MergedRange::new(self.tree.range(range), &ov[start..end])
    }
```

- [ ] **Step 4: Route the read sites through the accessors**

Replace each site as follows. Sites are listed by their pre-rename line numbers.

| Site | Was | Becomes |
|---|---|---|
| :391 `get` | `self.data.get(key)` | `self.merged_get(key)` |
| :398 `update` | `self.data.get_arc(key)` | `self.merged_get_arc(key)` |
| :433 `upsert_arc` | `self.data.get_arc(&key)` | `self.merged_get_arc(&key)` |
| :481 `delete` | `self.data.get_arc(key)` | `self.merged_get_arc(key)` |
| :541, :588 batch ops | `self.data.get_arc(...)` | `self.merged_get_arc(...)` |
| :617 `range` | `self.data.range(range)` | `self.merged_range(range)` |
| :623 `len` | `self.data.len()` | `self.merged_len()` |
| :629 `is_empty` | `self.data.is_empty()` | `self.merged_len() == 0` |
| :639 first | `self.data.range(..).next()` | `self.merged_range(..).next()` |
| :644 last | `self.data.range(..).next_back()` | see Step 5 |
| :166 `merge_keys_from` | `source.data.get_arc` / `self.data.get_arc` | keep as `.tree.` — MultiWriter path, see Task 5 |
| :499, :1077 clones | `self.data.clone()` | `self.tree.clone()` plus the overlay, see Task 3 |
| :208-209, :847, :977 | | Task 3 |

Everything else (`insert_mut`, `remove_mut`, `insert_arc_mut`, `extend_from_sorted`) is a *write* into the tree and stays `self.tree.*` — those are Task 4's business.

- [ ] **Step 5: Handle `next_back` — `MergedRange` is forward-only**

Site :644 calls `.next_back()`, which `MergedRange` does not implement. Implementing `DoubleEndedIterator` for a two-pointer merge is fiddly and not needed anywhere else, so replace that site with a forward scan:

```rust
        // `MergedRange` is forward-only by design (a double-ended merge needs a
        // second pair of cursors for no other caller). The overlay is bounded
        // and this is not a hot path.
        self.merged_range(..).last()
```

If the surrounding method is documented as O(1), update its doc comment to say it is O(n) when an overlay is present and O(1) otherwise — do not leave a stale complexity claim.

- [ ] **Step 6: Construct the new fields in every constructor**

Every `Table { ... }` and `TableSnapshot { ... }` literal needs `overlay: crate::overlay::Overlay::new()` and `overlay_disabled: false`. The compiler lists them all.

- [ ] **Step 7: Verify the refactor is inert**

Run: `cargo test --features persistence,fulltext`
Expected: PASS, with the same counts as before this task. **No test file may be edited in this task.** If a test needs changing, the refactor is not inert and something is wrong.

Run: `cargo clippy --features persistence,fulltext --all-targets -- -D warnings`
Expected: zero warnings.

- [ ] **Step 8: Prove no direct tree read escaped**

Run: `grep -nE "self\.tree\.(range|get|get_arc|len|is_empty|max_key)" src/table.rs`
Expected: matches **only** inside `merged_get`, `merged_get_arc`, `merged_range`, `merged_len`, and the `merge_keys_from` MultiWriter path at :166. Any other match is a leak — route it through an accessor.

- [ ] **Step 9: Commit**

```bash
git add src/table.rs
git commit -m "refactor(table): rename data -> tree, route reads through merged accessors (task58)

Pure refactor: the overlay is constructed empty and never written, so
behaviour is identical and no test changes. Renaming the field makes the
compiler enumerate all 41 access sites rather than trusting a grep — the
spec's linchpin is that no direct tree read survives outside the choke
point, and this is how that gets enforced instead of asserted."
```

---

### Task 3: Make clone, rollback, serialization and the bulk-append guard overlay-aware

**Files:**
- Modify: `src/table.rs` (:208-209 `collect_serialized_rows`, :497-:515 `snapshot`/`restore`, :847 `define_index` rebuild, :977 bulk-append guard, :1077 `boxed_clone`, :348 `data_ref`)
- Modify: `src/store.rs:1393` (`materialize_delta`)

**Interfaces:**
- Consumes: the merged accessors from Task 2.
- Produces: `fn flush_overlay(&mut self)` on `Table<R, K>` (used here by `define_index` and the bulk-append guard; Task 4 also calls it from the write path).

Still inert: the overlay is never populated, so all of this is exercised only in the empty case until Task 4.

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` in `src/table.rs`:

```rust
    /// Flush must be a no-op on an empty overlay, and must leave the table
    /// observationally identical. Task 4 tests the populated case; this pins
    /// the inert case so Task 3 can land before writes are switched on.
    #[test]
    fn flush_of_empty_overlay_is_a_noop() {
        let mut t: Table<String, u64> = Table::new();
        t.put(1, "a".to_string()).unwrap();
        t.put(2, "b".to_string()).unwrap();
        let before: Vec<(u64, String)> =
            t.iter().map(|(k, v)| (*k, v.clone())).collect();
        let len_before = t.len();

        t.flush_overlay();

        let after: Vec<(u64, String)> = t.iter().map(|(k, v)| (*k, v.clone())).collect();
        assert_eq!(before, after);
        assert_eq!(len_before, t.len());
        assert!(t.overlay.is_empty());
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --features persistence,fulltext --lib table::tests::flush_of_empty_overlay_is_a_noop`
Expected: FAIL — `no method named flush_overlay`.

- [ ] **Step 3: Implement `flush_overlay`**

Add to `impl<R: Record, K: PrimaryKey> Table<R, K>`:

```rust
    /// Replays the overlay into the tree in one batched pass and empties it.
    ///
    /// Infallible by construction: entries were validated when written, the
    /// writer owns this table clone, and every `Tombstone` shadows a real tree
    /// row (see `Overlay::remove_entry`), so `remove_mut` always has a target.
    /// Sorted order gives the tree its best-case locality, and the batch clones
    /// the root and inner nodes once for all entries instead of once per
    /// transaction — which is where the win comes from.
    pub(crate) fn flush_overlay(&mut self) {
        if self.overlay.is_empty() {
            return;
        }
        let entries = std::mem::replace(
            &mut self.overlay,
            crate::overlay::Overlay::new(),
        );
        for (key, op) in entries.entries() {
            match op {
                crate::overlay::OverlayOp::Put(arc) => {
                    self.tree.insert_arc_mut(key.clone(), Arc::clone(arc));
                }
                crate::overlay::OverlayOp::Tombstone => {
                    let removed = self.tree.remove_mut(key);
                    debug_assert!(
                        removed,
                        "tombstone with no tree row — the overlay-only delete \
                         path should have erased the entry instead"
                    );
                }
            }
        }
    }
```

- [ ] **Step 4: Fix the five remaining tree-direct sites**

**a. `collect_serialized_rows` (:208-209) — the checkpoint path.** Unmerged, a checkpoint with a live overlay silently drops every overlay row:

```rust
        let mut out = Vec::with_capacity(self.merged_len());
        for (key, record) in self.merged_range(..) {
```

**b. `snapshot` (:497) and `restore` (:510) — batch rollback.** Both must carry the overlay, or a failed batch leaves rows that the rollback did not undo:

```rust
    fn snapshot(&self) -> TableSnapshot<R, K> {
        TableSnapshot {
            tree: self.tree.clone(),
            overlay: self.overlay.clone(),
            overlay_disabled: self.overlay_disabled,
            next_id: self.next_id.clone(),
            indexes: self.indexes.iter().map(|(k, v)| (k.clone(), v.boxed_clone())).collect(),
        }
    }
```

and `restore` assigns all five fields back.

**c. `boxed_clone` (:1077)** — same: clone `tree`, `overlay`, and `overlay_disabled`.

**d. `define_index` (:847)** — flush first, then disable, so the index sees merged data and later writes bypass the overlay:

```rust
        self.flush_overlay();
        self.overlay_disabled = true;
        index.rebuild(self.tree.range(..).map(|(id, r)| (id.clone(), r)))?;
```

Apply the same two leading lines to `define_custom_index` (:839).

**e. The bulk-append guard (:977)** — the tree's max key is not the table's max key when an overlay is live, so the fast path could seed a `BulkBuilder` below an overlay row. Flush rather than merge the comparison:

```rust
        // The task51 right-spine fast path reasons about the tree's right edge,
        // which an overlay can invalidate. Flushing is cheap (bounded) and
        // keeps that reasoning valid, rather than teaching the guard about the
        // overlay.
        self.flush_overlay();
        if self.tree.max_key().is_none_or(|k| *k < start_id) {
```

- [ ] **Step 5: Fix the one `data_ref` caller**

`data_ref` (:348) hands out `&BTree`, bypassing any overlay. Its single caller is `materialize_delta` at `src/store.rs:1393`. Rename it so the bypass is self-documenting and no new caller appears by accident:

```rust
    /// The raw tree, **without** the overlay merged in. Only valid where the
    /// overlay is known empty. `materialize_delta` qualifies: it runs against a
    /// committed snapshot table, and commit flushes (task58).
    pub(crate) fn tree_ref_unmerged(&self) -> &BTree<K, R> {
        debug_assert!(self.overlay.is_empty(), "tree_ref_unmerged with a live overlay");
        &self.tree
    }
```

Update `src/store.rs:1393` to call `tree_ref_unmerged()`.

- [ ] **Step 6: Run the tests**

Run: `cargo test --features persistence,fulltext`
Expected: PASS, same counts as Task 2 plus the one new test.

Run: `cargo clippy --features persistence,fulltext --all-targets -- -D warnings`
Expected: zero warnings.

- [ ] **Step 7: Commit**

```bash
git add src/table.rs src/store.rs
git commit -m "feat(table): overlay-aware clone, rollback, serialization and bulk guard (task58)

Still inert — nothing populates the overlay yet. Covers the two sites the
design spec did not name: collect_serialized_rows (a checkpoint with a live
overlay would silently omit every overlay row) and the task51 bulk-append
guard, whose right-spine reasoning an overlay invalidates. The guard flushes
rather than learning about the overlay, keeping that reasoning intact.

data_ref -> tree_ref_unmerged, with a debug_assert, so the one legitimate
bypass is self-documenting and a new caller cannot appear silently."
```

---

### Task 4: Turn on the write path, and the differential property test

**Files:**
- Modify: `src/table.rs` (`insert`, `put`, `update`, `delete`, and the batch variants)
- Create: `tests/overlay_equivalence.rs`

**Interfaces:**
- Consumes: `flush_overlay`, the merged accessors, `Overlay::{put, tombstone, remove_entry, adjust_len, len}`.
- Produces: no new public API. This is the commit that changes behaviour.

- [ ] **Step 1: Write the failing property test**

Create `tests/overlay_equivalence.rs`:

```rust
//! The centerpiece test for task58: an overlay-enabled table and a
//! flush-after-every-write table, driven by identical random op sequences,
//! must be observationally identical. This guards the merge, tombstone and
//! len_delta surface in one place rather than one assertion per method.

use ultima_db::Table;

#[derive(Debug, Clone)]
enum Op {
    Put(u64, String),
    Update(u64, String),
    Delete(u64),
    Get(u64),
    Len,
    Iter,
    Range(u64, u64),
}

/// Deterministic xorshift — no dev-dependency, and a failing seed is
/// reproducible from the assertion message alone.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn gen_ops(seed: u64, count: usize, key_space: u64) -> Vec<Op> {
    let mut rng = Rng(seed | 1);
    (0..count)
        .map(|i| {
            let k = rng.below(key_space);
            match rng.below(7) {
                0 => Op::Put(k, format!("v{i}")),
                1 => Op::Update(k, format!("u{i}")),
                2 => Op::Delete(k),
                3 => Op::Get(k),
                4 => Op::Len,
                5 => Op::Iter,
                _ => {
                    let b = rng.below(key_space);
                    Op::Range(k.min(b), k.max(b))
                }
            }
        })
        .collect()
}

/// Applies `ops`, returning one observation string per read op. Any divergence
/// between the two tables shows up as a differing observation.
fn run(ops: &[Op], flush_every_write: bool) -> Vec<String> {
    let mut t: Table<String, u64> = Table::new_keyed();
    let mut obs = Vec::new();
    for op in ops {
        match op {
            Op::Put(k, v) => {
                let r = t.put(*k, v.clone());
                obs.push(format!("put:{r:?}"));
            }
            Op::Update(k, v) => {
                let r = t.update(k, v.clone());
                obs.push(format!("update:{}", r.is_ok()));
            }
            Op::Delete(k) => {
                let r = t.delete(k);
                obs.push(format!("delete:{}", r.is_ok()));
            }
            Op::Get(k) => obs.push(format!("get:{:?}", t.get(k))),
            Op::Len => obs.push(format!("len:{}", t.len())),
            Op::Iter => {
                let rows: Vec<(u64, String)> =
                    t.iter().map(|(k, v)| (*k, v.clone())).collect();
                obs.push(format!("iter:{rows:?}"));
            }
            Op::Range(a, b) => {
                let rows: Vec<(u64, String)> =
                    t.range(*a..=*b).map(|(k, v)| (*k, v.clone())).collect();
                obs.push(format!("range:{rows:?}"));
            }
        }
        if flush_every_write {
            t.flush_overlay_for_test();
        }
    }
    obs
}

#[test]
fn overlay_table_is_observationally_identical_to_a_flushed_one() {
    // Small key space so keys collide often: that is where tombstones,
    // overlay-only deletes and in-place replacement actually get exercised.
    for seed in 1..=40u64 {
        for key_space in [4u64, 17, 200] {
            let ops = gen_ops(seed, 400, key_space);
            let overlaid = run(&ops, false);
            let flushed = run(&ops, true);
            assert_eq!(
                overlaid, flushed,
                "divergence at seed={seed} key_space={key_space}"
            );
        }
    }
}

/// The cap must not be observable. Runs the same sequence at caps that force
/// a flush on nearly every write (1, 2) and at caps that never fill.
#[test]
fn behaviour_is_independent_of_overlay_cap() {
    let ops = gen_ops(7, 600, 23);
    let baseline = run(&ops, true);
    for cap in ["1", "2", "3", "128", "100000"] {
        // Safety: single-threaded test process, set before any table is built.
        unsafe { std::env::set_var("ULTIMA_OVERLAY_CAP", cap) };
        assert_eq!(run(&ops, false), baseline, "cap={cap} changed behaviour");
    }
    unsafe { std::env::remove_var("ULTIMA_OVERLAY_CAP") };
}
```

**Note:** `overlay_cap()` memoizes in a `OnceLock`, so `behaviour_is_independent_of_overlay_cap` cannot vary the cap in-process. Change `overlay_cap()` to read the env var on each call in `#[cfg(test)]` builds:

```rust
pub(crate) fn overlay_cap() -> usize {
    #[cfg(test)]
    {
        return std::env::var("ULTIMA_OVERLAY_CAP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|c| *c > 0)
            .unwrap_or(128);
    }
    #[cfg(not(test))]
    {
        static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *CAP.get_or_init(|| {
            std::env::var("ULTIMA_OVERLAY_CAP")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|c| *c > 0)
                .unwrap_or(128)
        })
    }
}
```

Integration tests compile the crate without `cfg(test)`, so also add a test-only public hook on `Table` for the two things `tests/overlay_equivalence.rs` needs:

```rust
    /// Test-only: force a flush. Not part of the public API contract.
    #[doc(hidden)]
    pub fn flush_overlay_for_test(&mut self) {
        self.flush_overlay();
    }
```

and make the integration test set the cap via the env var before building tables — which requires `overlay_cap()` to not memoize in normal builds either. Simplest resolution, and the one to implement: **drop the `OnceLock` entirely** and read the env var on every call. It is one `std::env::var` per flush check, not per row, and the flush check happens once per write on a bounded path. Measure it in Task 7 and memoize only if it shows.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --features persistence,fulltext --test overlay_equivalence`
Expected: FAIL — `no method named flush_overlay_for_test`, and once that exists, the two runs diverge because writes do not yet use the overlay.

- [ ] **Step 3: Implement the overlay write path**

In each of `put`, `update`, `delete` (and the `insert` auto-increment path), branch at the top:

```rust
        if self.overlay_disabled || self.indexes.is_empty() == false {
            // pre-task58 path, unchanged
        }
```

Use this exact predicate in one helper so the three sites cannot drift:

```rust
    /// Overlays are V1-scoped to non-indexed tables. `overlay_disabled` is set
    /// by `define_index`; the `indexes` check covers a table that arrived with
    /// indexes already attached (via clone or snapshot restore).
    fn overlay_enabled(&self) -> bool {
        !self.overlay_disabled && self.indexes.is_empty()
    }
```

`put` becomes:

```rust
    pub fn put(&mut self, key: K, record: R) -> Result<()> {
        if !self.overlay_enabled() {
            return self.put_direct(key, record); // today's body, extracted verbatim
        }
        if self.overlay.len() >= crate::overlay::overlay_cap() {
            self.flush_overlay();
        }
        let existed = self.merged_get(&key).is_some();
        key.advance_auto_counter(&mut self.next_id);
        self.overlay.put(key, Arc::new(record));
        if !existed {
            self.overlay.adjust_len(1);
        }
        Ok(())
    }
```

`update` returns `Err(Error::KeyNotFound)` when `merged_get_arc` is `None`, otherwise writes `overlay.put` with no `adjust_len` (the row already counted).

`delete` is the subtle one — this asymmetry is what keeps flush infallible:

```rust
    pub fn delete(&mut self, key: &K) -> Result<R> {
        if !self.overlay_enabled() {
            return self.delete_direct(key);
        }
        if self.overlay.len() >= crate::overlay::overlay_cap() {
            self.flush_overlay();
        }
        let old = self.merged_get_arc(key).ok_or(Error::KeyNotFound)?;
        if self.tree.get(key).is_some() {
            // Shadows a real tree row: a tombstone has something to remove.
            self.overlay.tombstone(key.clone());
        } else {
            // Lives only in the overlay: erase it outright, so that every
            // tombstone in the overlay shadows a real tree row.
            self.overlay.remove_entry(key);
        }
        self.overlay.adjust_len(-1);
        Arc::try_unwrap(old).or_else(|arc| Ok((*arc).clone()))
    }
```

Match `delete`'s existing return type and cloning behaviour exactly — read the current body first and preserve it. If `R: Clone` is not bound, return whatever today's `delete` returns rather than inventing a clone.

WAL emission in `TableWriter` is **not touched**: the logical `WalOp` is produced exactly as today, before and regardless of where the row lands.

- [ ] **Step 4: Run the tests**

Run: `cargo test --features persistence,fulltext --test overlay_equivalence`
Expected: PASS, both tests.

Run: `cargo test --features persistence,fulltext`
Expected: PASS. A failure here is a real behavioural difference — chase it, do not adjust the test.

- [ ] **Step 5: Commit**

```bash
git add src/table.rs src/overlay.rs tests/overlay_equivalence.rs
git commit -m "feat(table): route non-indexed SingleWriter writes through the overlay (task58)

The commit that changes behaviour. Deleting a row that lives only in the
overlay erases the entry rather than writing a tombstone, so every tombstone
shadows a real tree row and flush's remove_mut is infallible.

The differential test drives an overlay table and a flush-after-every-write
table through identical random sequences and requires identical observations,
including at caps 1 and 2 where nearly every write flushes."
```

---

### Task 5: MultiWriter tripwire and index interaction

**Files:**
- Modify: `src/table.rs` (`merge_keys_from` at :166, `upsert_arc` at :432)
- Modify: `tests/overlay_equivalence.rs` (add the cases below)

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn defining_an_index_flushes_and_disables_the_overlay() {
    let mut t: Table<String, u64> = Table::new_keyed();
    for i in 0..10u64 {
        t.put(i, format!("v{i}")).unwrap();
    }
    t.define_index("by_val", |r: &String| r.clone()).unwrap();

    // Every row written before the index existed must be indexed.
    for i in 0..10u64 {
        assert_eq!(
            t.get_unique("by_val", &format!("v{i}")).unwrap().map(|r| r.clone()),
            Some(format!("v{i}")),
            "pre-index row {i} missing from the index"
        );
    }
    // And writes after it must stay visible to the index.
    t.put(99, "v99".to_string()).unwrap();
    assert!(t.get_unique("by_val", &"v99".to_string()).unwrap().is_some());
}
```

Adjust `define_index`'s and `get_unique`'s argument shapes to the real signatures at `src/table.rs:682` and `:721` before running — read them rather than trusting this sketch.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features persistence,fulltext --test overlay_equivalence defining_an_index`
Expected: FAIL if the flush-before-rebuild from Task 3 Step 4d is missing or ordered wrongly.

- [ ] **Step 3: Add the MultiWriter tripwire**

At the top of `merge_keys_from` (:166) — the OCC merge path, which must never see an overlay:

```rust
        debug_assert!(
            self.overlay.is_empty() && src.overlay.is_empty(),
            "merge_keys_from with a live overlay — overlays are SingleWriter-only (task58)"
        );
```

and the same assertion at the top of `upsert_arc` (:432), which the merge path calls per key.

- [ ] **Step 4: Add the store-level tripwire test**

In `tests/overlay_equivalence.rs`, build a `MultiWriter` store, run two concurrent writers with overlapping keys, and assert the retry path still produces a `WriteConflict` and a correct final state. The `debug_assert`s above fire in test builds if an overlay ever reaches that path.

- [ ] **Step 5: Run, lint, commit**

```bash
cargo test --features persistence,fulltext
cargo clippy --features persistence,fulltext --all-targets -- -D warnings
git add src/table.rs tests/overlay_equivalence.rs
git commit -m "feat(table): MultiWriter overlay tripwire + index-flush coverage (task58)"
```

---

### Task 6: Durability integration — WAL recovery, checkpoints, MVCC visibility

**Files:**
- Modify: `tests/store_integration.rs` (or a new `tests/overlay_durability.rs` if that file is already large — check first)

- [ ] **Step 1: Write the failing tests**

Four cases, each targeting a way the overlay could leak into durable state:

```rust
/// A checkpoint taken with a live overlay must contain the overlay's rows.
/// Before the collect_serialized_rows fix this silently dropped them.
#[test]
fn checkpoint_with_a_live_overlay_round_trips() { /* write N < cap rows, checkpoint, recover, assert all N present */ }

/// WAL ops are logical and unchanged, so replay rebuilds an overlay
/// through the normal table API.
#[test]
fn wal_replay_reproduces_overlaid_writes() { /* write, drop without checkpoint, recover, assert */ }

/// An old ReadTx sees its frozen overlay; a new one sees the new state.
#[test]
fn mvcc_visibility_holds_across_an_overlay_flush() { /* commit, hold ReadTx, write past the cap to force a flush, assert the old tx is unchanged */ }

/// A failed batch must roll back overlay rows too.
#[test]
fn batch_rollback_restores_the_overlay_exactly() { /* batch that fails partway, assert len and contents unchanged */ }
```

Fill in each body against the existing test helpers in that file — do not invent a store-construction helper that is not already there.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --features persistence,fulltext overlay`
Expected: they must fail against a build with Task 3's `collect_serialized_rows` fix reverted. Verify that by reverting it locally, seeing red, and restoring it — a test that cannot fail is not a test.

- [ ] **Step 3: Fix whatever they surface, then run the whole suite**

Run: `cargo test --features persistence,fulltext`
Run: `cargo test --features persistence,fulltext,metrics`
Expected: PASS. The `metrics` run matters — CI uses that combination and this repo has shipped a red `main` by testing only `persistence,fulltext`.

- [ ] **Step 4: Commit**

```bash
git add tests/
git commit -m "test(overlay): checkpoint, WAL replay, MVCC visibility and batch rollback (task58)"
```

---

### Task 7: Benchmark cell, docs, and the ship-gate package

**Files:**
- Modify: the `perf_decomp` bench under `benches/`
- Create: `docs/tasks/task58_write_overlay.md`
- Modify: `CLAUDE.md` (the `Table` bullet in the architecture section)

- [ ] **Step 1: Add the bench cell**

Add an overlay-vs-main arm to `perf_decomp`. Per this repo's rules, a local run gives **direction only** — never a number for a doc.

- [ ] **Step 2: Measure the `overlay_cap()` env read**

Task 4 dropped the `OnceLock`. Confirm with the bench that reading the env var per write is not measurable against the ~100 ns target; if it is, memoize behind a non-`cfg(test)` `OnceLock` and give the integration test a different way to vary the cap.

- [ ] **Step 3: Write the task doc**

Create `docs/tasks/task58_write_overlay.md` covering: the ~2 µs/txn tax and where it was measured; the bounded-copy trick; the tombstone invariant and why flush is infallible; the V1 restrictions and how they auto-degrade; the choke-point rule and that the field rename is what enforces it; the two sites the design spec missed (`collect_serialized_rows`, the bulk-append guard); and the rejected alternatives from the spec (frozen delta chains, in-place commit with snapshot-on-demand) with their reasons.

- [ ] **Step 4: Update `CLAUDE.md`**

Extend the `Table<R, K = u64>` bullet to mention the overlay, its V1 restrictions, and that all multi-row reads go through `merged_range`.

- [ ] **Step 5: Commit**

```bash
git add benches/ docs/tasks/task58_write_overlay.md CLAUDE.md
git commit -m "docs(task58): write-overlay feature record + bench cell"
```

- [ ] **Step 6: The ship gate — requires explicit user authorization**

**Do not provision AWS on your own initiative.** Present this to the user and wait:

> Ready for the fleet A/B ship gate: one `c6id.2xlarge`, main vs branch, eventual A/F plus C/E guardrail cells, interleaved ×2, shipped config, glibc. Ship iff **A beats Fjall same-host** and C/E are statistically unchanged, with reads-under-writes within ≤5%. Roughly `make bench-oneshot TARGET=competitor` from `bench-infra/`. Authorize?

If the first result is marginal, one extra arm may sweep `OVERLAY_CAP` at 64/128/256. Run `make status` afterwards to confirm nothing is left running.

---

## Self-Review

**Spec coverage.** V1 scope → Tasks 4 (SingleWriter/non-indexed predicate), 5 (tripwire); data structure → Task 1; write path incl. the tombstone asymmetry → Task 4; flush → Task 3; read paths and the choke point → Task 2; durability/recovery/composition → Tasks 3 and 6; the spec's 7 test cases → Task 4 (#1), Task 6 (#2, #3, #5), Task 5 (#4, #7), Task 6 (#6); validation plan → Task 7. No spec requirement is unassigned.

**Two gaps this plan adds beyond the spec**, both found by auditing the code and both load-bearing: `collect_serialized_rows` (silent checkpoint data loss) and the task51 bulk-append guard (a fast path that could seed below a live overlay row).

**Known soft spots, flagged rather than hidden.**

1. **`Task 4 Step 1`'s env-var/`OnceLock` tension is real** and I resolved it toward "no memoization, measure in Task 7." An implementer who finds the env read measurable should say so rather than quietly memoizing and breaking the cap test.
2. **`delete`'s return type** is sketched, not quoted — the plan tells the implementer to read the real body first. Same for `define_index`/`get_unique` signatures in Task 5.
3. **Task 2 is the largest task** (41 sites). It is mechanical and the compiler drives it, but it is the one most likely to need a second review pass.
4. **`MergedRange` is forward-only.** Task 2 Step 5 handles the single `next_back` caller by degrading it to a forward scan. If a later caller needs double-ended iteration, that is a design change, not a patch.
