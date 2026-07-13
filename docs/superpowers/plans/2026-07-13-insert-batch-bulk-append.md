# insert_batch Bulk-Append Fast Path (task51) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `Table::insert_batch` build its appended keys through the dense O(N) `BulkBuilder` instead of per-key `insert_mut` descents, closing the measured ~2× `from_sorted` ↔ `insert_batch` gap (task49).

**Architecture:** A new `BulkBuilder::seed_from_spine` reconstructs builder state from an existing tree by unzipping its right spine (each spine node becomes that level's under-construction node with the rightmost-child slot open — the builder's exact mid-build invariant); `BTree::extend_from_sorted` seeds, pushes the batch, and re-`finish()`es. `Table::insert_batch` phase 1 calls it behind a release-mode `max_key < next_id` guard with the existing per-key loop as defensive fallback. Spec: `docs/superpowers/specs/2026-07-13-insert-batch-bulk-append-design.md`.

**Tech Stack:** Rust (MSRV 1.88), criterion benches, formal drift guard (`formal/scripts/check-drift.sh`).

## Global Constraints

- Work on branch `feat/insert-batch-bulk-append` in an isolated worktree (superpowers:using-git-worktrees).
- `cargo clippy -- -D warnings` must stay clean after every task.
- Do NOT run `cargo fmt` (repo has rustfmt drift); match surrounding style by hand.
- Tree constants are `T = 64`, `MAX_KEYS = 127`, `MIN_KEYS = 63` — never hardcode the numbers in tests, use the consts.
- `src/btree.rs` edits fire the formal drift guard; Task 6 (kernel mirror) must land in the same branch so CI passes without `[skip-formal-drift]`.
- Snapshot isolation is the product invariant: an older `BTree` clone must never observe an append.

**Key existing code (read before starting):**
- `src/btree.rs:1044-1172` — `LevelBuilder` (`entries: Vec<(K, Arc<V>)>`, `children: Vec<Arc<BTreeNode<K,V>>>`), `BulkBuilder { levels, len, last_key }`, `push`/`attach_child`/`finish`.
- `src/btree.rs:1182` — `redistribute_tail` (already CoW-safe: reads the popped sibling via `to_vec()`, builds a fresh node, never mutates through the `Arc`).
- `src/btree.rs:108-117` — `from_sorted` (the pattern `extend_from_sorted` generalizes).
- `src/table.rs:410-446` — `insert_batch` (phase 1 = the loop being replaced; phase 2 = index maintenance, unchanged).

---

### Task 1: `BTree::max_key`

**Files:**
- Modify: `src/btree.rs` (impl block near `get_arc`, ~line 137; tests at the bottom `mod tests`)

**Interfaces:**
- Produces: `pub(crate) fn max_key(&self) -> Option<&K>` on `BTree<K, V>` — rightmost key or `None` for an empty tree. Task 5 consumes it.

- [ ] **Step 1: Write the failing tests** (in `mod tests`, near the `range_*` tests)

```rust
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
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test max_key -- --nocapture`
Expected: FAIL to compile — `max_key` not found.

- [ ] **Step 3: Implement** (next to `get_arc`)

```rust
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
```

- [ ] **Step 4: Verify pass**

Run: `cargo test max_key`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/btree.rs && git commit -m "feat(btree): max_key accessor (task51 groundwork)"
```

---

### Task 2: Test-only structural invariant checker

**Files:**
- Modify: `src/btree.rs` (top of `mod tests`, near `insert_range`)

**Interfaces:**
- Produces: `fn check_invariants<K: Ord + std::fmt::Debug, V>(t: &BTree<K, V>)` (test helper) — panics on any violated B-tree invariant. Tasks 3–4 call it after every mutation.

- [ ] **Step 1: Write the helper plus a sanity test that it passes on existing constructors and catches a bad tree**

```rust
/// Structural invariant check: arity bounds (root exempt from MIN_KEYS),
/// uniform leaf depth, internal children == entries + 1, per-node key
/// order, global in-order key order, and len consistency.
fn check_invariants<K: Ord + std::fmt::Debug, V>(t: &BTree<K, V>) {
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
```

- [ ] **Step 2: Run both tests**

Run: `cargo test check_invariants`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add src/btree.rs && git commit -m "test(btree): structural invariant checker (task51 groundwork)"
```

---

### Task 3: `BulkBuilder::seed_from_spine` + `BTree::extend_from_sorted`

**Files:**
- Modify: `src/btree.rs` — `BulkBuilder` impl (~line 1067) and `BTree` impl (next to `from_sorted`, ~line 117); tests at the bottom.

**Interfaces:**
- Consumes: `check_invariants` (Task 2), `LevelBuilder`/`BulkBuilder` internals as they exist today.
- Produces: `pub(crate) fn extend_from_sorted<I: IntoIterator<Item = (K, Arc<V>)>>(&mut self, iter: I)` on `BTree<K, V>` — appends strictly-ascending keys all greater than the current max. Task 5 consumes it. Also `fn seed_from_spine(tree: &BTree<K, V>) -> BulkBuilder<K, V>` (private).

- [ ] **Step 1: Write the failing equivalence tests**

```rust
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
        MIN_KEYS as u64,            // root-leaf, underfull by non-root standards
        MAX_KEYS as u64,            // root-leaf exactly full
        MAX_KEYS as u64 + 1,        // first split just happened
        (MAX_KEYS * MAX_KEYS) as u64, // multi-level
    ] {
        let base = insert_range(1, base_n);
        for batch_n in [1u64, 2, MIN_KEYS as u64, MAX_KEYS as u64 + 5, 5000] {
            let batch: Vec<(u64, u64)> =
                (base_n + 1..=base_n + batch_n).map(|k| (k, k)).collect();
            assert_extend_matches_insert_mut(&base, &batch);
        }
    }
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
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test extend_from_sorted`
Expected: FAIL to compile — `extend_from_sorted` not found.

- [ ] **Step 3: Implement `seed_from_spine`** (in the `BulkBuilder` impl, after `new()`)

```rust
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
/// >= MIN_KEYS entries and only grows), and freezes only happen at
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
```

- [ ] **Step 4: Implement `extend_from_sorted`** (in the `BTree` impl, directly after `from_sorted`)

```rust
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
```

Also delete the now-stale `#[allow(dead_code)]` attributes on `BulkBuilder`/`LevelBuilder`/their impls if the compiler confirms they are no longer needed (they were there because only `from_sorted` used the builder; leave any that still are needed).

- [ ] **Step 5: Run the new tests + the whole btree suite**

Run: `cargo test extend_from_sorted && cargo test btree::`
Expected: all pass (fuzz test included).

- [ ] **Step 6: Clippy**

Run: `cargo clippy -- -D warnings`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add src/btree.rs && git commit -m "feat(btree): bulk append via right-spine-seeded BulkBuilder (task51)"
```

---

### Task 4: Snapshot-isolation tests for the append path

**Files:**
- Modify: `src/btree.rs` (`mod tests`)

**Interfaces:**
- Consumes: `extend_from_sorted` (Task 3), `check_invariants` (Task 2).

- [ ] **Step 1: Write the tests**

```rust
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
    let mut x: u64 = 0xB16_B00B5_CAFE_F00D;
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
```

- [ ] **Step 2: Run**

Run: `cargo test extend_from_sorted`
Expected: all pass (if the snapshot tests fail, the bug is a shared-node mutation in seed/finish — do NOT weaken the test).

- [ ] **Step 3: Commit**

```bash
git add src/btree.rs && git commit -m "test(btree): snapshot isolation under bulk append (task51)"
```

---

### Task 5: Wire `Table::insert_batch`

**Files:**
- Modify: `src/table.rs:410-446` (`insert_batch` phase 1) and `mod tests` in the same file.

**Interfaces:**
- Consumes: `BTree::extend_from_sorted`, `BTree::max_key` (Tasks 1, 3).
- Produces: unchanged public signature `pub fn insert_batch(&mut self, records: Vec<R>) -> Result<Vec<u64>>` — same ids, same rollback semantics.

- [ ] **Step 1: Write the failing table tests** (in `src/table.rs` `mod tests`)

```rust
#[test]
fn insert_batch_bulk_path_matches_per_key_semantics() {
    // Interleave singles and batches; ids must stay sequential and every
    // record retrievable — exercises repeated seeding of a grown tree.
    let mut table: Table<String> = Table::new();
    let mut expected_id = 1u64;
    for round in 0..20 {
        let n = 50 * (round % 4) + 1;
        let ids = table
            .insert_batch((0..n).map(|i| format!("r{round}-{i}")).collect())
            .unwrap();
        assert_eq!(ids.first().copied(), Some(expected_id));
        assert_eq!(ids.len(), n as usize);
        expected_id += n as u64;
        let single = table.insert(format!("s{round}")).unwrap();
        assert_eq!(single, expected_id);
        expected_id += 1;
    }
    assert_eq!(table.len() as u64, expected_id - 1);
    for id in 1..expected_id {
        assert!(table.get(id).is_some(), "missing id {id}");
    }
}

#[test]
fn insert_batch_falls_back_when_next_id_behind_max_key() {
    // Corrupt next_id below the max key (unreachable in production; the
    // guard exists exactly for this). The fallback must keep legacy
    // per-key semantics: replace at colliding ids, table stays coherent.
    let mut table: Table<String> = Table::new();
    table
        .insert_batch((0..100).map(|i| format!("v{i}")).collect())
        .unwrap(); // ids 1..=100
    table.next_id = 50;
    let ids = table.insert_batch(vec!["x".into(), "y".into()]).unwrap();
    assert_eq!(ids, vec![50, 51]);
    assert_eq!(table.get(50), Some(&"x".to_string()));
    assert_eq!(table.get(51), Some(&"y".to_string()));
    assert_eq!(table.len(), 100); // replaced, not added
}
```

- [ ] **Step 2: Run to verify the fallback test fails**

Run: `cargo test insert_batch -- --nocapture`
Expected: `insert_batch_bulk_path_matches_per_key_semantics` passes already (old path is correct, just slow); `insert_batch_falls_back_...` passes too on the OLD code. That is fine — these tests pin behavior across the swap. Confirm both pass BEFORE changing the implementation.

- [ ] **Step 3: Replace phase 1**

Replace the phase-1 block of `insert_batch` (the `let mut ids = ...` loop, keeping the empty-check, overflow assert, and `let snap = self.snapshot();` above it exactly as they are):

```rust
// Phase 1: Insert all records into the data BTree.
//
// Fast path (task51): batch ids are next_id.. and every existing key is
// < next_id (auto-increment; merge_keys_from maxes next_id across
// writers; bulk_load rebuilds it past the loaded max), so the batch is a
// pure append past the current max key — build it through the dense
// BulkBuilder in O(batch + height) instead of per-key descents. The
// max_key guard makes the invariant load-bearing instead of assumed;
// if it ever fails we take the legacy per-key path.
let start_id = self.next_id;
let n = records.len() as u64;
let ids: Vec<u64> = (start_id..start_id + n).collect();
if self.data.max_key().is_none_or(|k| *k < start_id) {
    self.data.extend_from_sorted(
        records
            .into_iter()
            .enumerate()
            .map(|(i, r)| (start_id + i as u64, Arc::new(r))),
    );
    self.next_id = start_id + n;
} else {
    // Defensive fallback — unreachable given the next_id invariant, but a
    // violated invariant must degrade to the legacy per-key path, not UB
    // in the packed builder. (No debug_assert here: the fallback test
    // exercises this branch in debug builds.)
    for (i, record) in records.into_iter().enumerate() {
        self.data.insert_mut(start_id + i as u64, record);
        self.next_id += 1;
    }
}
```

(Note the fallback keeps `self.next_id += 1` per record so a mid-loop panic can't leave `next_id` behind inserted keys — same as the old loop. Check the surrounding code compiles with `ids` now built up front; phase 2 is untouched.)

- [ ] **Step 4: Run the full table + store suites**

Run: `cargo test --features persistence,fulltext,metrics table:: && cargo test --features persistence,fulltext,metrics --test store_integration && cargo test insert_batch`
Expected: all pass — including the batch rollback tests (`snapshot()`/`restore` semantics unchanged) and the two new tests, with the fallback test exercising the slow branch.

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -- -D warnings
git add src/table.rs && git commit -m "feat(table): insert_batch takes the bulk-append fast path (task51)"
```

---

### Task 6: Formal kernel mirror (drift guard)

**Files:**
- Modify: `formal/kernel/src/lib.rs` (impl `BTree`, after `insert_mut` ~line 411; tests in `mod tests` ~line 793)

**Interfaces:**
- Consumes: kernel `BTree::insert_mut(&mut self, key: u64, val: u64)` (exists).
- Produces: kernel `pub fn extend_from_sorted(&mut self, items: &[(u64, u64)])` — the SPEC of the production fast path: a fold of the verified insert.

- [ ] **Step 1: Add the mirror**

```rust
/// Mirror of production `BTree::extend_from_sorted` (task51). The
/// production bulk-append (right-spine-seeded BulkBuilder) must be
/// observationally equal to folding the verified `insert` over the
/// batch — this fold IS the spec; the Lean proofs cover each `insert`.
/// Production-side equivalence is pinned by the extend_from_sorted
/// differential tests in src/btree.rs.
pub fn extend_from_sorted(&mut self, items: &[(u64, u64)]) {
    for &(k, v) in items {
        self.insert_mut(k, v);
    }
}
```

- [ ] **Step 2: Add the kernel differential test**

```rust
#[test]
fn extend_from_sorted_matches_insert_and_std_btreemap() {
    // Ascending appends atop a random base: the extend mirror, per-key
    // insert, and std::BTreeMap must agree exactly.
    let mut model = BTreeMap::new();
    let mut ext = BTree::new();
    let mut per_key = BTree::new();
    let mut x: u64 = 0xFEED_FACE_DEAD_BEEF;
    let mut next = 0u64;
    for _ in 0..50 {
        x = lcg(x);
        let n = x % 200 + 1;
        let batch: Vec<(u64, u64)> = (next..next + n).map(|k| (k, k ^ x)).collect();
        next += n;
        ext.extend_from_sorted(&batch);
        for &(k, v) in &batch {
            per_key.insert_mut(k, v);
            model.insert(k, v);
        }
        assert_eq!(ext.len, per_key.len);
        assert_eq!(ext.len, model.len());
    }
    for k in 0..next {
        assert_eq!(ext.get(k), model.get(&k).copied(), "key {k}");
        assert_eq!(ext.get(k), per_key.get(k), "key {k}");
    }
}
```

- [ ] **Step 3: Run kernel tests + the drift guard**

Run: `make test/formal-kernel && BASE=main formal/scripts/check-drift.sh`
Expected: kernel tests pass; drift guard passes (formal/ touched alongside src/btree.rs in the branch range).

- [ ] **Step 4: Commit**

```bash
git add formal/kernel/src/lib.rs && git commit -m "formal(kernel): extend_from_sorted mirror + differential test (task51)"
```

---

### Task 7: Bench smoke, full verification, task doc

**Files:**
- Create: `docs/tasks/task51_insert_batch_bulk_append.md`
- Modify: `CLAUDE.md` (one line in the `Table<R>` bullet noting the batch fast path)

- [ ] **Step 1: Full suite + lint**

Run: `make lint && cargo test --features persistence,fulltext,metrics && cargo test --lib && cargo test -p ultima-vector --features persistence`
Expected: all green (this is exactly what CI gates).

- [ ] **Step 2: Magnitude smoke via the existing gap bench** (sandbox numbers are magnitude-only, per bench methodology — never publish them)

Run: `cargo bench --bench bulk_load_bench -- --quick`
Expected: the `bulk_vs_insert_batch` group shows `insert_batch` within ~1.2× of `bulk_load_sorted` at 100k+ (was ~2×). Record the observed ratio in the task doc as "sandbox smoke", explicitly non-authoritative.

- [ ] **Step 3: Write `docs/tasks/task51_insert_batch_bulk_append.md`**

Follow the task48/task50 doc structure: Status / Motivation / Design (seed_from_spine unzip + the two-part redistribute_tail safety argument from the Task 3 comment) / What changed (files) / Correctness (list every test added, incl. why the spec's "shared-sibling redistribute regression test" became the stronger per-round-snapshot fuzz: the constructed case is unreachable — a popped sibling is always freshly-frozen-full — and the fuzz pins the property that matters regardless) / Bench (sandbox smoke ratio; authoritative number deferred to the next bench-infra run via task49's harness) / Related (spec, plan, task48/49/50).

- [ ] **Step 4: Add the CLAUDE.md line**

In the `Table<R>` bullet, extend the batch-operations mention: `insert_batch` takes a bulk-append fast path (right-spine-seeded `BulkBuilder`, task51) since batch ids always append past the current max key.

- [ ] **Step 5: Commit**

```bash
git add docs/tasks/task51_insert_batch_bulk_append.md CLAUDE.md
git commit -m "docs(task51): insert_batch bulk-append task doc + CLAUDE.md note"
```

---

## Self-review notes (done at plan time)

- Spec coverage: max_key guard+fallback (T5), seeding+extend (T3), snapshot isolation incl. redistribute audit (T3 comment + T4), formal mirror (T6), equivalence/invariant/fuzz tests (T2–T4), bench (T7), task doc (T7). Spec's single-constructed-case redistribute test is deliberately upgraded to the per-round-snapshot fuzz — documented in T7's task doc step.
- The spec's `benches/btree_insert_mut_bench.rs` batch arm is dropped: `extend_from_sorted` is `pub(crate)` (benches can't call it) and `bulk_load_bench.rs` already measures the exact gap end-to-end. YAGNI.
- Type consistency: `extend_from_sorted` takes `(K, Arc<V>)` everywhere; table wiring wraps records with `Arc::new`; kernel mirror is `&[(u64, u64)]` (kernel stores plain u64 values, no Arc — matches existing kernel style).
