# Write Overlay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bounded write overlay in front of `Table`'s B-tree so a commit's MVCC copy-on-write cost becomes a ~2 KB memcpy instead of an O(height×T) node-clone chain, targeting the eventual-tier YCSB A cell (spec: `docs/superpowers/specs/2026-08-03-write-overlay-design.md`).

**Architecture:** New `src/overlay.rs` holds `Overlay<R, K>` (sorted `Arc<Vec<(K, OverlayOp<R>)>>`, cap 128, tombstones, `len_delta`) and `MergedIter`. `Table` (src/table.rs) routes writes into the overlay and all reads through merged views; a full overlay flushes into the tree in one batched pass. Store-side wiring enables the overlay only for SingleWriter stores' tables; `define_index` flushes and disables per-table.

**Tech Stack:** Rust, existing `BTree` primitives (`insert_arc_mut`, `remove_mut`, `get_arc`, `range`), no new dependencies.

## Global Constraints

- V1 scope (from spec): SingleWriter stores only; tables with secondary indexes never use the overlay; no public API or on-disk format changes.
- Read budget: `get`/`iter`/`range` on an empty overlay must be one predictable branch away from today's code path.
- WAL emission, auto-increment (`next_id`), and error contracts (`update` missing key → `Error::KeyNotFound`; `delete` returns the removed `Arc<R>`) are unchanged.
- Invariant: a `Tombstone` always shadows a tree-resident key (overlay-born rows are deleted by removing their entry), so flush is infallible.
- Invariant: indexed tables have a permanently empty, disabled overlay — the index-path reads (`get_by_index` etc., src/table.rs:735–824) may keep reading `self.data` directly *because* of it.
- Repo rules: no `cargo fmt`; clippy must pass with `-D warnings` in `persistence` and `persistence bench-internals` configs; match surrounding comment style.
- All test/bench commands run with `--features persistence` unless noted.

---

### Task 1: `Overlay` data structure (`src/overlay.rs`)

**Files:**
- Create: `src/overlay.rs`
- Modify: `src/lib.rs` (add `mod overlay;` next to the other private modules)
- Test: unit tests inside `src/overlay.rs`

**Interfaces:**
- Consumes: nothing (leaf module; only `std::sync::Arc`).
- Produces (used by Tasks 2–3):
  ```rust
  pub(crate) const OVERLAY_CAP: usize = 128;

  pub(crate) enum OverlayOp<R> {
      Put { rec: Arc<R>, tree_resident: bool },
      Tombstone,
  }

  pub(crate) struct Overlay<R, K> { /* entries: Arc<Vec<(K, OverlayOp<R>)>>, len_delta: i64, cap: usize */ }

  impl<R, K: Ord + Clone> Overlay<R, K> {
      pub(crate) fn new(cap: usize) -> Self;           // cap 0 == permanently disabled
      pub(crate) fn is_empty(&self) -> bool;
      pub(crate) fn is_full(&self) -> bool;            // len == cap (false when cap 0)
      pub(crate) fn enabled(&self) -> bool;            // cap > 0
      pub(crate) fn len_delta(&self) -> i64;
      pub(crate) fn get(&self, key: &K) -> Option<&OverlayOp<R>>;
      pub(crate) fn set_put(&mut self, key: K, rec: Arc<R>, tree_resident: bool);
      pub(crate) fn set_tombstone(&mut self, key: K);  // caller guarantees tree-resident
      pub(crate) fn remove_entry(&mut self, key: &K);  // overlay-born delete
      pub(crate) fn entries(&self) -> &[(K, OverlayOp<R>)];
      pub(crate) fn take_entries(&mut self) -> Arc<Vec<(K, OverlayOp<R>)>>; // resets to empty, len_delta = 0
  }
  // Clone is Arc-bump + copies (derive-by-hand; R: Clone NOT required).
  ```
- `len_delta` transition table (implemented inside the setters):

  | previous entry | new op | tree_resident | Δ |
  |---|---|---|--:|
  | none | Put | false | +1 |
  | none | Put | true | 0 |
  | none | Tombstone | (true by contract) | −1 |
  | Put | Put | (unchanged bit) | 0 |
  | Tombstone | Put | (true, re-insert) | +1 |
  | Put{resident:false} | remove_entry | — | −1 |
  | Put{resident:true} | Tombstone | — | −1 |

- [ ] **Step 1: Write the failing tests**

In `src/overlay.rs`, module skeleton with only the test module first is awkward in Rust — instead write the full test module against the intended API and let the missing types be the failure:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn put(o: &mut Overlay<String, u64>, k: u64, v: &str, resident: bool) {
        o.set_put(k, Arc::new(v.to_string()), resident);
    }

    #[test]
    fn entries_stay_sorted_and_replace_in_place() {
        let mut o: Overlay<String, u64> = Overlay::new(8);
        put(&mut o, 5, "e", false);
        put(&mut o, 1, "a", false);
        put(&mut o, 3, "c", false);
        put(&mut o, 3, "c2", false); // replace, not duplicate
        let keys: Vec<u64> = o.entries().iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![1, 3, 5]);
        assert!(matches!(o.get(&3), Some(OverlayOp::Put { rec, .. }) if rec.as_str() == "c2"));
    }

    #[test]
    fn len_delta_follows_the_transition_table() {
        let mut o: Overlay<String, u64> = Overlay::new(8);
        put(&mut o, 1, "new", false);       // none -> Put(!resident): +1
        assert_eq!(o.len_delta(), 1);
        put(&mut o, 2, "upd", true);        // none -> Put(resident): 0
        assert_eq!(o.len_delta(), 1);
        o.set_tombstone(3);                 // none -> Tomb: -1
        assert_eq!(o.len_delta(), 0);
        put(&mut o, 3, "back", true);       // Tomb -> Put: +1
        assert_eq!(o.len_delta(), 1);
        o.remove_entry(&1);                 // overlay-born delete: -1
        assert_eq!(o.len_delta(), 0);
        o.set_tombstone(2);                 // Put(resident) -> Tomb: -1
        assert_eq!(o.len_delta(), -1);
        put(&mut o, 2, "again", true);      // Put stays Put after Tomb->Put
        assert_eq!(o.len_delta(), 0);
    }

    #[test]
    fn clone_is_cow_and_mutation_after_clone_does_not_leak() {
        let mut o: Overlay<String, u64> = Overlay::new(8);
        put(&mut o, 1, "a", false);
        let frozen = o.clone();
        put(&mut o, 2, "b", false);
        assert_eq!(frozen.entries().len(), 1, "frozen clone must not see later writes");
        assert_eq!(o.entries().len(), 2);
    }

    #[test]
    fn cap_zero_is_disabled_and_never_full() {
        let o: Overlay<String, u64> = Overlay::new(0);
        assert!(!o.enabled());
        assert!(!o.is_full());
        assert!(o.is_empty());
    }

    #[test]
    fn take_entries_resets_state() {
        let mut o: Overlay<String, u64> = Overlay::new(8);
        put(&mut o, 1, "a", false);
        o.set_tombstone(2);
        let drained = o.take_entries();
        assert_eq!(drained.len(), 2);
        assert!(o.is_empty());
        assert_eq!(o.len_delta(), 0);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib --features persistence overlay:: 2>&1 | tail -5`
Expected: compile error — `Overlay`/`OverlayOp` not found (add `mod overlay;` to `src/lib.rs` first so the test module is compiled).

- [ ] **Step 3: Implement `Overlay`**

```rust
// src/overlay.rs
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bounded write overlay: the small sorted write-front that absorbs a
//! table's mutations so each commit's copy-on-write is a bounded memcpy of
//! at most `OVERLAY_CAP` entries instead of a B-tree node-clone chain. See
//! docs/superpowers/specs/2026-08-03-write-overlay-design.md.

use std::sync::Arc;

/// Default capacity. Bench-tunable via `ULTIMA_OVERLAY_CAP` at table-enable
/// time (bench escape hatch, not a public knob — task57 precedent).
pub(crate) const OVERLAY_CAP: usize = 128;

pub(crate) enum OverlayOp<R> {
    /// A live row. `tree_resident` records whether the key also exists in
    /// the backing tree — it decides delete behavior (remove vs tombstone)
    /// and the `len_delta` bookkeeping.
    Put { rec: Arc<R>, tree_resident: bool },
    /// Shadows a tree-resident row. Never written for overlay-born rows,
    /// which is what makes flush's `remove_mut` infallible.
    Tombstone,
}

impl<R> Clone for OverlayOp<R> {
    fn clone(&self) -> Self {
        match self {
            OverlayOp::Put { rec, tree_resident } => OverlayOp::Put {
                rec: Arc::clone(rec),
                tree_resident: *tree_resident,
            },
            OverlayOp::Tombstone => OverlayOp::Tombstone,
        }
    }
}

pub(crate) struct Overlay<R, K> {
    /// Sorted by key. Shared with snapshots via the Arc; a write after a
    /// commit CoWs the whole Vec — bounded by `cap`, that IS the design.
    entries: Arc<Vec<(K, OverlayOp<R>)>>,
    len_delta: i64,
    cap: usize,
}

impl<R, K> Clone for Overlay<R, K> {
    fn clone(&self) -> Self {
        Overlay {
            entries: Arc::clone(&self.entries),
            len_delta: self.len_delta,
            cap: self.cap,
        }
    }
}

impl<R, K: Ord + Clone> Overlay<R, K> {
    pub(crate) fn new(cap: usize) -> Self {
        Overlay { entries: Arc::new(Vec::new()), len_delta: 0, cap }
    }

    pub(crate) fn enabled(&self) -> bool { self.cap > 0 }
    pub(crate) fn is_empty(&self) -> bool { self.entries.is_empty() }
    pub(crate) fn is_full(&self) -> bool { self.cap > 0 && self.entries.len() >= self.cap }
    pub(crate) fn len_delta(&self) -> i64 { self.len_delta }
    pub(crate) fn entries(&self) -> &[(K, OverlayOp<R>)] { &self.entries }

    pub(crate) fn get(&self, key: &K) -> Option<&OverlayOp<R>> {
        self.entries
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
            .map(|i| &self.entries[i].1)
    }

    pub(crate) fn set_put(&mut self, key: K, rec: Arc<R>, tree_resident: bool) {
        let entries = Arc::make_mut(&mut self.entries);
        match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
            Ok(i) => {
                if matches!(entries[i].1, OverlayOp::Tombstone) {
                    self.len_delta += 1; // Tomb -> Put: row comes back
                }
                // A replaced Put keeps its original residency: the tree's
                // state for this key hasn't changed since the first Put.
                let resident = match entries[i].1 {
                    OverlayOp::Put { tree_resident, .. } => tree_resident,
                    OverlayOp::Tombstone => true,
                };
                entries[i].1 = OverlayOp::Put { rec, tree_resident: resident };
            }
            Err(i) => {
                if !tree_resident {
                    self.len_delta += 1;
                }
                entries.insert(i, (key, OverlayOp::Put { rec, tree_resident }));
            }
        }
    }

    pub(crate) fn set_tombstone(&mut self, key: K) {
        let entries = Arc::make_mut(&mut self.entries);
        match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
            Ok(i) => {
                debug_assert!(
                    matches!(entries[i].1, OverlayOp::Put { tree_resident: true, .. }),
                    "tombstone over overlay-born Put — caller must remove_entry instead"
                );
                entries[i].1 = OverlayOp::Tombstone;
                self.len_delta -= 1;
            }
            Err(i) => {
                entries.insert(i, (key, OverlayOp::Tombstone));
                self.len_delta -= 1;
            }
        }
    }

    pub(crate) fn remove_entry(&mut self, key: &K) {
        let entries = Arc::make_mut(&mut self.entries);
        if let Ok(i) = entries.binary_search_by(|(k, _)| k.cmp(key)) {
            debug_assert!(
                matches!(entries[i].1, OverlayOp::Put { tree_resident: false, .. }),
                "remove_entry is only for overlay-born rows"
            );
            entries.remove(i);
            self.len_delta -= 1;
        }
    }

    /// Hand the frozen entries to a flush and reset to empty.
    pub(crate) fn take_entries(&mut self) -> Arc<Vec<(K, OverlayOp<R>)>> {
        self.len_delta = 0;
        std::mem::replace(&mut self.entries, Arc::new(Vec::new()))
    }
}
```

In `src/lib.rs`, next to the other internal modules (near `pub mod table;`), add:

```rust
mod overlay;
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test --lib --features persistence overlay:: 2>&1 | tail -3`
Expected: `test result: ok. 5 passed`

- [ ] **Step 5: Clippy + commit**

Run: `cargo clippy --all-targets --features "persistence bench-internals" -- -D warnings 2>&1 | tail -2`
Expected: clean.

```bash
git add src/overlay.rs src/lib.rs
git commit -m "feat(overlay): bounded sorted write-front with tombstones and len_delta (task58 T1)"
```

---

### Task 2: Merged read paths on `Table`

**Files:**
- Modify: `src/table.rs` — struct `Table` (near line 266), `get` (~391), `len`/`is_empty` (~622–630), `contains` (~633), `first`/`last` (~637–645), `get_many` (~650), `range` (~613), `iter` (~648), `collect_serialized_rows` (~204), `merge_keys_from` (~141), `data_ref` (~348), `Clone` impl (~1068), `TableSnapshot` (~290) and `snapshot`/`restore` (~497/510)
- Test: unit tests in `src/table.rs`

**Interfaces:**
- Consumes: `Overlay`, `OverlayOp`, `OVERLAY_CAP` from Task 1; existing `BTree::range`/`get`/`get_arc`.
- Produces: `Table::overlay` field plus a merged-read internal API later tasks build on:
  ```rust
  impl<R, K> Table<R, K> {
      fn merged_get_arc(&self, key: &K) -> Option<Arc<R>>;      // overlay-then-tree
      fn merged_iter(&self, range: impl RangeBounds<K> + Clone) // the choke point
          -> MergedIter<'_, R, K, /* tree iter type */>;
  }
  ```
  `MergedIter` lives in `src/overlay.rs`: a two-pointer merge yielding
  `(&K, &R)`; overlay wins ties; tombstones swallow the tree entry.
- **Every** multi-row read flows through `merged_iter` — after this task,
  `grep -n 'self\.data\.range' src/table.rs` must show only: the flush
  internals (none yet), `merged_iter` itself, and the index-path sites
  (735–847), which are covered by the indexed-tables-have-empty-overlays
  invariant (add a one-line comment at each index site saying so).

- [ ] **Step 1: Add the `overlay` field and keep everything compiling**

In `Table` (near line 266) and `TableSnapshot` (near 290) add `overlay: Overlay<R, K>`; construct with `Overlay::new(0)` (disabled) in `new`, `new_keyed`, `from_bulk`, `empty_with_counter`; clone it in the `Clone` impl (~1068) and capture/restore it in `snapshot()`/`restore()` (~497/510). Run `cargo test --lib --features persistence 2>&1 | grep '^test result'` — everything must still pass (overlay is inert at cap 0).

Commit: `git commit -am "refactor(table): carry an inert Overlay through Table, TableSnapshot, Clone (task58 T2a)"`

- [ ] **Step 2: Write the failing merged-read tests**

Add to `src/table.rs` tests (the existing `mod tests`). These construct the overlay state through a test-only helper — add to `impl Table` under `#[cfg(test)]`:

```rust
#[cfg(test)]
pub(crate) fn overlay_mut_for_test(&mut self, cap: usize) -> &mut Overlay<R, K> {
    if !self.overlay.enabled() {
        self.overlay = Overlay::new(cap);
    }
    &mut self.overlay
}
```

```rust
#[test]
fn merged_get_prefers_overlay_and_respects_tombstones() {
    let mut t: Table<String> = Table::new();
    let id = t.insert("tree".to_string()).unwrap();
    let ov = t.overlay_mut_for_test(8);
    ov.set_put(id, std::sync::Arc::new("overlay".to_string()), true);
    assert_eq!(t.get(&id).map(String::as_str), Some("overlay"));
    t.overlay_mut_for_test(8).set_tombstone(id);
    assert_eq!(t.get(&id), None);
    assert!(!t.contains(&id));
}

#[test]
fn merged_len_and_iter_and_range_merge_both_sources() {
    let mut t: Table<String> = Table::new();
    let a = t.insert("a".to_string()).unwrap(); // key 1
    let b = t.insert("b".to_string()).unwrap(); // key 2
    let _c = t.insert("c".to_string()).unwrap(); // key 3
    {
        let ov = t.overlay_mut_for_test(8);
        ov.set_put(b, std::sync::Arc::new("b2".to_string()), true); // update via overlay
        ov.set_tombstone(a);                                        // delete via overlay
        ov.set_put(10, std::sync::Arc::new("j".to_string()), false); // new row
    }
    assert_eq!(t.len(), 3); // 3 - 1 + 1
    let got: Vec<(u64, String)> = t.iter().map(|(k, v)| (*k, v.clone())).collect();
    assert_eq!(got, vec![(2, "b2".into()), (3, "c".into()), (10, "j".into())]);
    let ranged: Vec<u64> = t.range(2..=3).map(|(k, _)| *k).collect();
    assert_eq!(ranged, vec![2, 3]);
    assert_eq!(t.first().map(|(k, _)| *k), Some(2));
    assert_eq!(t.last().map(|(k, _)| *k), Some(10));
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test --lib --features persistence merged_ 2>&1 | tail -5`
Expected: FAIL — `get` returns the tree value / `len` ignores the delta (asserts fire).

- [ ] **Step 4: Implement `MergedIter` and rewire the read paths**

`MergedIter` in `src/overlay.rs`:

```rust
/// Two-pointer merge of the sorted overlay slice and the tree's range
/// iterator. Overlay wins key ties; tombstones swallow the tree's entry.
/// With an empty overlay this is the tree iterator plus one dead branch.
pub(crate) struct MergedIter<'a, R, K, T>
where
    T: Iterator<Item = (&'a K, &'a R)>,
{
    pub(crate) overlay: std::iter::Peekable<std::slice::Iter<'a, (K, OverlayOp<R>)>>,
    pub(crate) tree: std::iter::Peekable<T>,
}

impl<'a, R: 'a, K: Ord + 'a, T> Iterator for MergedIter<'a, R, K, T>
where
    T: Iterator<Item = (&'a K, &'a R)>,
{
    type Item = (&'a K, &'a R);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let take_overlay = match (self.overlay.peek(), self.tree.peek()) {
                (Some((ok, _)), Some((tk, _))) => match ok.cmp(tk) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Greater => false,
                    std::cmp::Ordering::Equal => {
                        self.tree.next(); // shadowed by the overlay entry
                        true
                    }
                },
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => return None,
            };
            if take_overlay {
                let (k, op) = self.overlay.next().unwrap();
                match op {
                    OverlayOp::Put { rec, .. } => return Some((k, rec.as_ref())),
                    OverlayOp::Tombstone => continue, // tree twin already skipped
                }
            } else {
                return self.tree.next();
            }
        }
    }
}
```

In `Table`, the choke point (the overlay slice is pre-narrowed to the range
with two `partition_point` calls so `range` stays O(log cap + items)):

```rust
fn merged_iter<'a, Rb>(&'a self, range: Rb) -> MergedIter<'a, R, K, /* btree range iter */>
where
    Rb: RangeBounds<K> + Clone,
{
    let e = self.overlay.entries();
    let lo = match range.start_bound() {
        Bound::Included(k) => e.partition_point(|(ek, _)| ek < k),
        Bound::Excluded(k) => e.partition_point(|(ek, _)| ek <= k),
        Bound::Unbounded => 0,
    };
    let hi = match range.end_bound() {
        Bound::Included(k) => e.partition_point(|(ek, _)| ek <= k),
        Bound::Excluded(k) => e.partition_point(|(ek, _)| ek < k),
        Bound::Unbounded => e.len(),
    };
    MergedIter {
        overlay: e[lo..hi].iter().peekable(),
        tree: self.data.range(range).peekable(),
    }
}

fn merged_get_arc(&self, key: &K) -> Option<Arc<R>> {
    if !self.overlay.is_empty() {
        match self.overlay.get(key) {
            Some(OverlayOp::Put { rec, .. }) => return Some(Arc::clone(rec)),
            Some(OverlayOp::Tombstone) => return None,
            None => {}
        }
    }
    self.data.get_arc(key)
}
```

Rewire (each is a small mechanical edit):
- `get` (~391): overlay probe first (same shape as `merged_get_arc` but returning `&R` — match on the overlay op, `Put` returns `rec.as_ref()`, `Tombstone` returns `None`, miss falls to `self.data.get(key)`).
- `len` (~622): `(self.data.len() as i64 + self.overlay.len_delta()) as usize`.
- `is_empty` (~628): `self.len() == 0` (the tree may be non-empty under net tombstones).
- `contains` (~633): via the `get` path.
- `first`/`last` (~637–645): `self.merged_iter(..).next()` / for `last`, merge `self.overlay` tail with `self.data.range(..).next_back()` — implement as: compare `data.range(..).next_back()` with the last non-tombstone overlay entry, resolving shadowing through `merged_get`-style logic; simplest correct form is `self.merged_iter(..).last()` for `last()` only when the overlay is nonempty, else the existing `next_back()` fast path.
- `get_many` (~650): map over the merged `get`.
- `iter` (~648) and `range` (~613): return `self.merged_iter(..)` / `self.merged_iter(range)`. The public signatures already return `impl Iterator` — unchanged.
- `collect_serialized_rows` (~204): iterate `self.merged_iter(..)` instead of `self.data.range(..)`; `with_capacity(self.len())`.
- `merge_keys_from` (~141, MultiWriter-only): add `debug_assert!(self.overlay.is_empty() && source_overlay_empty)` via a small `overlay_is_empty()` helper on the trait impl side — plus a comment citing the SingleWriter-only invariant.
- `data_ref` (~348): add a doc comment "callers see the tree WITHOUT the overlay; only valid where the overlay is empty by construction (bulk paths)" and `debug_assert!(self.overlay.is_empty())`.
- Index-path sites (735–824, 847): add the invariant comment, no rewiring.

- [ ] **Step 5: Run tests**

Run: `cargo test --lib --features persistence 2>&1 | grep '^test result'`
Expected: all pass (new merged tests + full suite).

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy --all-targets --features "persistence bench-internals" -- -D warnings
git add src/table.rs src/overlay.rs
git commit -m "feat(overlay): merged read paths — get/len/iter/range/serialize through one choke point (task58 T2)"
```

---

### Task 3: Write paths, flush, and batch-op flush-first

**Files:**
- Modify: `src/table.rs` — `insert` (~894), `put` (~386/1027/1049), `update` (~397), `delete` (~480), `upsert_arc` (~432), `insert_batch` (~977), `update_batch` (~541), `delete_batch` (~588)
- Test: unit tests in `src/table.rs`

**Interfaces:**
- Consumes: Task 1 setters, Task 2 merged reads.
- Produces:
  ```rust
  impl<R, K> Table<R, K> {
      pub(crate) fn flush_overlay(&mut self);          // batched replay into the tree
      fn overlay_write_ready(&mut self) -> bool;       // enabled && indexes.is_empty(); flushes when full
  }
  ```
  Write-path contract for Task 4/5: with the overlay enabled, single-row
  mutations land in the overlay; batch ops and index DDL flush first and
  use today's paths.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn writes_land_in_overlay_and_flush_at_cap() {
    let mut t: Table<String> = Table::new();
    t.overlay_mut_for_test(4); // enable with tiny cap
    let mut ids = Vec::new();
    for i in 0..4 {
        ids.push(t.insert(format!("v{i}")).unwrap());
    }
    assert_eq!(t.overlay_len_for_test(), 4, "all four writes buffered");
    let id5 = t.insert("v4".to_string()).unwrap(); // fifth write: flush, then buffer
    assert_eq!(t.overlay_len_for_test(), 1);
    assert_eq!(t.len(), 5);
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(t.get(id).map(String::as_str), Some(format!("v{i}").as_str()));
    }
    assert_eq!(t.get(&id5).map(String::as_str), Some("v4"));
}

#[test]
fn overlay_delete_semantics_match_the_spec() {
    let mut t: Table<String> = Table::new();
    let resident = t.insert("old".to_string()).unwrap();
    t.overlay_mut_for_test(8);
    let born = t.insert("young".to_string()).unwrap();  // overlay-born
    // overlay-born delete: entry removed, no tombstone
    let removed = t.delete(&born).unwrap();
    assert_eq!(removed.as_str(), "young");
    assert_eq!(t.overlay_len_for_test(), 0);
    // tree-resident delete through the overlay: tombstone
    let removed = t.delete(&resident).unwrap();
    assert_eq!(removed.as_str(), "old");
    assert_eq!(t.overlay_len_for_test(), 1);
    assert_eq!(t.len(), 0);
    assert!(matches!(t.delete(&resident), Err(Error::KeyNotFound)));
    assert!(matches!(t.update(&resident, "x".into()), Err(Error::KeyNotFound)));
}

#[test]
fn batch_ops_flush_first_and_stay_correct() {
    let mut t: Table<String> = Table::new();
    t.overlay_mut_for_test(8);
    let id = t.insert("solo".to_string()).unwrap();
    let ids = t.insert_batch((0..10).map(|i| format!("b{i}")).collect()).unwrap();
    assert_eq!(t.overlay_len_for_test(), 0, "insert_batch flushed the overlay");
    assert_eq!(t.len(), 11);
    assert_eq!(t.get(&id).map(String::as_str), Some("solo"));
    assert_eq!(ids.len(), 10);
}

#[test]
fn rollback_restores_the_overlay() {
    let mut t: Table<String> = Table::new();
    t.overlay_mut_for_test(8);
    let id = t.insert("keep".to_string()).unwrap();
    // update_batch with a failing key rolls back wholesale
    let res = t.update_batch(vec![(id, "changed".into()), (9999, "nope".into())]);
    assert!(res.is_err());
    assert_eq!(t.get(&id).map(String::as_str), Some("keep"));
    assert_eq!(t.len(), 1);
}
```

Add the test-only probe next to `overlay_mut_for_test`:

```rust
#[cfg(test)]
pub(crate) fn overlay_len_for_test(&self) -> usize {
    self.overlay.entries().len()
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib --features persistence overlay_delete_semantics writes_land batch_ops_flush rollback_restores 2>&1 | tail -5`
Expected: FAIL — writes go straight to the tree today (`overlay_len_for_test` is 0 where 4 expected).

- [ ] **Step 3: Implement the write routing and flush**

Gate + flush:

```rust
/// True when this mutation should go through the overlay. Also the flush
/// trigger: a full overlay is drained into the tree before the caller
/// proceeds, so the subsequent buffer insert always has room.
fn overlay_write_ready(&mut self) -> bool {
    if !self.overlay.enabled() || !self.indexes.is_empty() {
        return false;
    }
    if self.overlay.is_full() {
        self.flush_overlay();
    }
    true
}

/// Replay the buffered ops into the tree in one batched, sorted pass —
/// the root and inner nodes CoW once for the whole batch instead of once
/// per transaction. Infallible: Puts use `insert_arc_mut`, and every
/// Tombstone shadows a tree-resident key by the overlay invariant.
pub(crate) fn flush_overlay(&mut self) {
    if self.overlay.is_empty() {
        return;
    }
    let entries = self.overlay.take_entries();
    for (key, op) in entries.iter() {
        match op {
            OverlayOp::Put { rec, .. } => {
                self.data.insert_arc_mut(key.clone(), Arc::clone(rec));
            }
            OverlayOp::Tombstone => {
                let removed = self.data.remove_mut(key);
                debug_assert!(removed, "tombstone shadowed a non-resident key");
            }
        }
    }
}
```

Route the four single-row paths. Pattern for `update` (~397; same shape for
`put`'s two arms and the tail of `insert` at ~921/1049 — insert's id
allocation and `advance_auto_counter` stay exactly as they are, only the
final `self.data.insert_mut(...)` is replaced):

```rust
// update(): after the existence check
if self.overlay_write_ready() {
    let resident = self.data.get(key).is_some();
    self.overlay.set_put(key.clone(), Arc::new(record), resident);
} else {
    self.data.insert_mut(key.clone(), record);
}
```

(For `update` the existence check itself becomes the merged probe:
`self.merged_get_arc(key).ok_or(Error::KeyNotFound)?` replaces the
`self.data.get_arc` at ~398 — its return value is also the "old" record
where the current code uses it.)

`delete` (~480):

```rust
let old = self.merged_get_arc(key).ok_or(Error::KeyNotFound)?;
if self.overlay_write_ready() {
    match self.overlay.get(key) {
        Some(OverlayOp::Put { tree_resident: false, .. }) => self.overlay.remove_entry(key),
        _ => self.overlay.set_tombstone(key.clone()),
    }
} else {
    self.data.remove_mut(key);
}
Ok(old)
```

`upsert_arc` (~432, the MultiWriter merge helper): first line
`self.flush_overlay();` with a comment — SingleWriter-only scope means this
is a defensive no-op in practice, and it keeps the method correct if the
scope ever widens.

Batch ops (`insert_batch` ~977, `update_batch` ~541, `delete_batch` ~588):
first line `self.flush_overlay();` — their snapshot/restore rollback
(`snapshot()` already captures the overlay from Task 2) and the
`extend_from_sorted`/`max_key` fast path then operate on a merged-empty
table, unchanged.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features persistence 2>&1 | grep '^test result'`
Expected: all pass.

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy --all-targets --features "persistence bench-internals" -- -D warnings
git add src/table.rs
git commit -m "feat(overlay): route single-row writes through the overlay; batched flush; batch ops flush-first (task58 T3)"
```

---

### Task 4: Property test — overlay table ≡ plain table

**Files:**
- Test: `src/table.rs` (new test in the existing tests module)

**Interfaces:**
- Consumes: `overlay_mut_for_test(cap)` from Task 2, full write/read surface from Task 3.
- Produces: the regression net every later change runs against.

- [ ] **Step 1: Write the test (it should pass immediately — that is the point of this task; if it fails, Tasks 1–3 have a bug and this test is the debugging tool)**

```rust
/// Drive an overlay table and a plain table with the same op sequence;
/// they must be observationally identical. Deterministic seeds; caps 1,
/// 2, 3, 8 force flushes at every boundary alignment.
#[test]
fn overlay_table_is_observationally_identical_to_plain_table() {
    use rand::{Rng, RngExt, SeedableRng};
    for seed in 0..8u64 {
        for cap in [1usize, 2, 3, 8] {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let mut plain: Table<String> = Table::new();
            let mut with_ov: Table<String> = Table::new();
            with_ov.overlay_mut_for_test(cap);
            let mut live_keys: Vec<u64> = Vec::new();
            for step in 0..400 {
                match rng.random_range(0..6) {
                    0 | 1 => {
                        let v = format!("v{step}");
                        let a = plain.insert(v.clone()).unwrap();
                        let b = with_ov.insert(v).unwrap();
                        assert_eq!(a, b, "auto ids must stay in lockstep");
                        live_keys.push(a);
                    }
                    2 if !live_keys.is_empty() => {
                        let k = live_keys[rng.random_range(0..live_keys.len())];
                        let v = format!("u{step}");
                        assert_eq!(
                            plain.update(&k, v.clone()).is_ok(),
                            with_ov.update(&k, v).is_ok()
                        );
                    }
                    3 if !live_keys.is_empty() => {
                        let i = rng.random_range(0..live_keys.len());
                        let k = live_keys.swap_remove(i);
                        let a = plain.delete(&k);
                        let b = with_ov.delete(&k);
                        assert_eq!(a.is_ok(), b.is_ok());
                        if let (Ok(x), Ok(y)) = (a, b) {
                            assert_eq!(x, y);
                        }
                    }
                    4 => {
                        let k = rng.random_range(0..(live_keys.len() as u64 + 4));
                        assert_eq!(plain.get(&k), with_ov.get(&k), "seed {seed} cap {cap} step {step}");
                    }
                    _ => {
                        let lo = rng.random_range(0..12u64);
                        let hi = lo + rng.random_range(0..12u64);
                        let a: Vec<(u64, String)> =
                            plain.range(lo..hi).map(|(k, v)| (*k, v.clone())).collect();
                        let b: Vec<(u64, String)> =
                            with_ov.range(lo..hi).map(|(k, v)| (*k, v.clone())).collect();
                        assert_eq!(a, b, "seed {seed} cap {cap} step {step}");
                    }
                }
                assert_eq!(plain.len(), with_ov.len());
            }
            // Full-iteration equivalence at the end, overlay still nonempty.
            let a: Vec<(u64, String)> = plain.iter().map(|(k, v)| (*k, v.clone())).collect();
            let b: Vec<(u64, String)> = with_ov.iter().map(|(k, v)| (*k, v.clone())).collect();
            assert_eq!(a, b);
        }
    }
}
```

- [ ] **Step 2: Run it**

Run: `cargo test --lib --features persistence observationally_identical 2>&1 | tail -3`
Expected: PASS. If it fails, minimize by printing the seed/cap/step from the assert message and fix the Task 1–3 bug it found before proceeding.

- [ ] **Step 3: Commit**

```bash
git add src/table.rs
git commit -m "test(overlay): property test — overlay table observationally identical to plain table (task58 T4)"
```

---

### Task 5: Store wiring — enable, DDL disable, MVCC/persistence integration

**Files:**
- Modify: `src/store.rs` — the shared open-table helper used by `WriteTx::open_table`/`open_tables2`/`open_tables3` (search `fn open_table_dirty` or the `downcast a dirty entry` helper near the comment at ~2434); `define_index`/`define_custom_index` in `src/table.rs` (~682/839)
- Modify: `src/table.rs` — add `set_overlay_cap`
- Test: `src/store.rs` tests module

**Interfaces:**
- Consumes: everything above.
- Produces:
  ```rust
  impl<R, K> Table<R, K> {
      pub(crate) fn set_overlay_cap(&mut self, cap: usize); // flushes if shrinking below len
  }
  ```
  Wiring rule: the WriteTx open-table path calls
  `table.set_overlay_cap(overlay_cap_for(store))` on the dirty clone, where
  `overlay_cap_for` returns `OVERLAY_CAP` for SingleWriter stores (honoring
  the `ULTIMA_OVERLAY_CAP` env override, parsed once per store at
  `Store::new` into `StoreInner`) and `0` for MultiWriter.

- [ ] **Step 1: Write the failing tests (in `src/store.rs` tests)**

```rust
#[test]
fn single_writer_store_buffers_writes_in_the_overlay() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.insert("a".to_string()).unwrap();
    }
    wtx.commit().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.get(&1).map(String::as_str), Some("a"));
    assert!(t.overlay_len_probe() > 0, "SingleWriter write should be buffered");
}

#[test]
fn multi_writer_store_never_engages_the_overlay() {
    let store = Store::new(
        StoreConfig::builder().writer_mode(WriterMode::MultiWriter).build(),
    ).unwrap();
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.insert("a".to_string()).unwrap();
    }
    wtx.commit().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.overlay_len_probe(), 0);
    assert_eq!(t.get(&1).map(String::as_str), Some("a"));
}

#[test]
fn old_snapshot_keeps_its_frozen_overlay() {
    let store = Store::default();
    let mut w1 = store.begin_write(None).unwrap();
    w1.open_table::<String>("t").unwrap().insert("v1".to_string()).unwrap();
    w1.commit().unwrap();
    let old = store.begin_read(None).unwrap(); // pins version with v1 only
    let mut w2 = store.begin_write(None).unwrap();
    w2.open_table::<String>("t").unwrap().update(&1, "v2".to_string()).unwrap();
    w2.commit().unwrap();
    let old_t = old.open_table::<String>("t").unwrap();
    assert_eq!(old_t.get(&1).map(String::as_str), Some("v1"));
    let new_t = store.begin_read(None).unwrap();
    assert_eq!(
        new_t.open_table::<String>("t").unwrap().get(&1).map(String::as_str),
        Some("v2")
    );
}

#[test]
fn define_index_flushes_and_disables_the_overlay() {
    let store = Store::default();
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.insert("row".to_string()).unwrap();
        t.define_index("len", IndexKind::NonUnique, |r: &String| r.len() as u64).unwrap();
        assert_eq!(t.overlay_len_probe(), 0, "DDL must flush");
        t.insert("row2".to_string()).unwrap();
        assert_eq!(t.overlay_len_probe(), 0, "indexed table writes bypass the overlay");
        assert_eq!(t.get_by_index("len", &3).unwrap().len(), 1);
        assert_eq!(t.get_by_index("len", &4).unwrap().len(), 1);
    }
    wtx.commit().unwrap();
}
```

(`overlay_len_probe` is a `pub(crate)` non-test twin of
`overlay_len_for_test` — the store tests live in a different module, so
give `Table` a `pub(crate) fn overlay_len_probe(&self) -> usize` and have
the `#[cfg(test)]` helper delegate to it. `TableWriter` needs a
pass-through `overlay_len_probe()` too — one-line delegation.)

Adjust signatures to the real `define_index`/`get_by_index` shapes at
src/table.rs:682/~757 when writing the test — the shapes above follow
CLAUDE.md's documented API.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib --features persistence overlay 2>&1 | tail -6`
Expected: `single_writer_store_buffers...` FAILS (probe reads 0 — nothing enables the overlay yet); the MultiWriter test passes trivially; visibility/DDL tests fail on the probe asserts.

- [ ] **Step 3: Implement the wiring**

- `Table::set_overlay_cap(cap)`: if `cap == 0 && !self.overlay.is_empty()` → `self.flush_overlay()`; then `self.overlay = Overlay::new(cap)` **only when the cap actually changes** (preserve entries when re-opening with the same cap: if `self.overlay.enabled() == (cap > 0)` and unchanged cap, do nothing).
- `StoreInner` gains `overlay_cap: usize`, computed once in `Store::new`: `0` for MultiWriter, else `std::env::var("ULTIMA_OVERLAY_CAP").ok().and_then(|v| v.parse().ok()).unwrap_or(OVERLAY_CAP)`.
- In the shared dirty-open helper (the single place `WriteTx::open_table` and the tuple openers materialize/clone a table — near the Task-2-noted comment at src/store.rs:2434): after the clone/creation, call `table.set_overlay_cap(inner.overlay_cap)`.
- `define_index`/`define_custom_index` (src/table.rs:682/839): first line `self.flush_overlay(); self.overlay = Overlay::new(0);` with a comment citing the spec ("indexed tables use the direct path; index reads at 735–824 rely on this").
- Recovery/checkpoint installs and `from_bulk` need **no** wiring: they build tables with disabled overlays, and the first `open_table` in a write txn enables it via the helper.

- [ ] **Step 4: Run the full suites**

Run: `cargo test --features persistence 2>&1 | grep -E '^test result'` and `cargo test 2>&1 | grep -cE 'result: ok'`
Expected: all green, both configs.

- [ ] **Step 5: Persistence integration tests**

Append to the same store tests (both `#[cfg(feature = "persistence")]`):

```rust
#[cfg(feature = "persistence")]
#[test]
fn recovery_replays_into_an_equivalent_table_with_overlay_pending() {
    let dir = crate::test_scratch::scratch_dir();
    let mk = || {
        Store::new(
            StoreConfig::builder()
                .persistence(Persistence::standalone(
                    dir.path().to_path_buf(),
                    Durability::Consistent,
                    WalWrite::Coalesced,
                ))
                .build(),
        )
        .unwrap()
    };
    {
        let store = mk();
        store.register_table::<String>("t").unwrap();
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("a".to_string()).unwrap();
            t.insert("b".to_string()).unwrap();
            t.delete(&1).unwrap();
        }
        wtx.commit().unwrap(); // overlay entries never flushed — "crash" here
    }
    let store = mk();
    store.register_table::<String>("t").unwrap();
    store.recover().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.get(&1), None);
    assert_eq!(t.get(&2).map(String::as_str), Some("b"));
    assert_eq!(t.len(), 1);
}

#[cfg(feature = "persistence")]
#[test]
fn checkpoint_serializes_the_merged_view() {
    let dir = crate::test_scratch::scratch_dir();
    let store = Store::new(
        StoreConfig::builder()
            .persistence(Persistence::standalone(
                dir.path().to_path_buf(),
                Durability::Consistent,
                WalWrite::Coalesced,
            ))
            .build(),
    )
    .unwrap();
    store.register_table::<String>("t").unwrap();
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut t = wtx.open_table::<String>("t").unwrap();
        t.insert("a".to_string()).unwrap();
        t.insert("b".to_string()).unwrap();
        t.delete(&1).unwrap();
    }
    wtx.commit().unwrap();
    store.checkpoint().unwrap(); // serializes with the overlay nonempty

    let store2 = Store::new(
        StoreConfig::builder()
            .persistence(Persistence::standalone(
                dir.path().to_path_buf(),
                Durability::Consistent,
                WalWrite::Coalesced,
            ))
            .build(),
    )
    .unwrap();
    store2.register_table::<String>("t").unwrap();
    store2.recover().unwrap();
    let rtx = store2.begin_read(None).unwrap();
    let t = rtx.open_table::<String>("t").unwrap();
    assert_eq!(t.get(&2).map(String::as_str), Some("b"));
    assert_eq!(t.get(&1), None);
    assert_eq!(t.len(), 1);
}
```

Follow the existing persistence-test fixtures in `src/store.rs` for the
exact builder incantations if they differ (search `Persistence::standalone(`
in the tests module and mirror one).

Run: `cargo test --lib --features persistence recovery_replays checkpoint_serializes 2>&1 | tail -3` — expected PASS (these are integration confirmations; if either fails, the merged-iterator choke point missed a serialization path — fix there, not in the test).

- [ ] **Step 6: Clippy both configs + commit**

```bash
cargo clippy --all-targets --features "persistence bench-internals" -- -D warnings
cargo clippy --all-targets -- -D warnings
git add src/table.rs src/store.rs src/overlay.rs
git commit -m "feat(overlay): store wiring — SingleWriter enable, DDL flush+disable, MVCC/recovery/checkpoint integration (task58 T5)"
```

---

### Task 6: Perf cell, docs, and the ship-gate handoff

**Files:**
- Modify: `examples/perf_decomp.rs` (add an overlay-visible cell), `CLAUDE.md` (one paragraph in the Table bullet), `docs/tasks/task58_write_overlay.md` (create)
- Test: full suites + local direction run

**Interfaces:**
- Consumes: everything above.
- Produces: the task58 doc and the measurable artifacts the fleet A/B uses.

- [ ] **Step 1: Extend `perf_decomp`**

`store_eventual_update` already exercises the overlay automatically (the store is SingleWriter). Add one contrast cell right after it — same store loop with `ULTIMA_OVERLAY_CAP=0` in the env **before** the store is built (the cap is read at `Store::new`):

```rust
// (7) Same as (6) with the overlay disabled — isolates the overlay's effect.
unsafe { std::env::set_var("ULTIMA_OVERLAY_CAP", "0") };
let dir2 = tempfile::tempdir_in(bench_disk_dir()).unwrap();
let store_no_ov = make_store(Persistence::standalone(
    dir2.path().to_path_buf(),
    Durability::Eventual,
    WalWrite::Coalesced,
));
let c_no_ov = measure("store_eventual_no_overlay", &keys, |ks| {
    for &k in ks {
        let mut wtx = store_no_ov.begin_write(None).unwrap();
        let mut table = wtx.open_table::<YcsbRecord>("ycsb").unwrap();
        let _ = table.update(k, YcsbRecord::new(k.wrapping_add(1)));
        wtx.commit().unwrap();
    }
});
unsafe { std::env::remove_var("ULTIMA_OVERLAY_CAP") };
drop(store_no_ov);
println!("  overlay effect (no_ov - ov) {:8.0}", c_no_ov - c_ev);
```

Reorder note: build the overlay-enabled store (6) BEFORE setting the env var, or scope the env set/remove tightly as shown.

- [ ] **Step 2: Local direction run**

Run: `cargo run --release --features persistence --example perf_decomp 2>&1 | tail -12`
Expected direction (sandbox, direction-only): `store_eventual_update` < `store_eventual_no_overlay`, gap of very roughly 1–2 µs. Record the numbers in the task doc.

- [ ] **Step 3: Write `docs/tasks/task58_write_overlay.md`**

Contents: problem (MVCC tax, cite the decomposition doc), the overlay design summary (copy the spec's data-structure and invariants sections, condensed), v1 scope and auto-disable rules, the delete asymmetry, test inventory (name the property test), local direction numbers, and a "Validation" section stating the fleet gate verbatim: *"Ship iff eventual A beats Fjall same-host glibc, with C/E unchanged; A/B recipe: bench-oneshot competitor plus a branch-vs-main A/F run, OVERLAY_CAP arms 64/128/256 if marginal."* Also add one sentence to CLAUDE.md's `Table` bullet: "Since task58, SingleWriter stores buffer single-row writes in a bounded overlay (`src/overlay.rs`, cap 128, flushed batched); indexed tables and MultiWriter stores bypass it."

- [ ] **Step 4: Full verification sweep**

```bash
cargo test --features persistence 2>&1 | grep -E '^test result'
cargo test 2>&1 | grep -E '^test result' | tail -3
cargo clippy --all-targets --features "persistence bench-internals" -- -D warnings
make perf/check   # local perf gate — overlay must not regress the SMR-apply floors
```

If `make perf/check` regresses (SMR apply uses explicit-version SingleWriter commits — the overlay engages there), inspect whether the apply workload's batched cells now double-buffer; if so, record the delta in the task doc and flag it at the review gate rather than tuning blind.

- [ ] **Step 5: Commit**

```bash
git add examples/perf_decomp.rs docs/tasks/task58_write_overlay.md CLAUDE.md
git commit -m "perf+docs(overlay): perf_decomp overlay cell, task58 doc, CLAUDE.md note (task58 T6)"
```

- [ ] **Step 6: Ship gate (manual, billable — do NOT run without explicit user authorization)**

The fleet A/B per the spec: one c6id.2xlarge, arms = main vs branch (eventual A/F + C/E guardrails, interleaved ×2), `OVERLAY_CAP` 64/256 arms only if the first result is marginal. Use the custom-run recipe from `bench-infra` (rsync + ssh script, pidfile watcher — see the 2026-08-03 memory scars: no `pgrep -f` self-match watchers, no `| head` under pipefail). Ship iff **A beats Fjall same-host glibc** and C/E unchanged within noise.
