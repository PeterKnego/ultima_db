# In-place B-tree delete (`BTree::remove_mut`) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an in-place `BTree::remove_mut` (mirror of `insert_mut`) that deletes via `Arc::make_mut`, and wire it into `Table`, cutting delete-heavy batches from `O(height)` allocations per key to near-zero.

**Architecture:** A new `make_mut` descent (`delete_from_node_mut` / `remove_leftmost_mut` / `remove_mut`) mutates uniquely-owned nodes in place and CoW-clones snapshot-shared ones. It **reuses the existing rebalance helpers** (`fix_underfull_child`, `rotate_right/left`, `merge_with_left/right`) verbatim — they already take `&mut Vec` of the parent's entries/children. The immutable `remove` stays. Snapshot isolation is preserved by construction, exactly as for `insert_mut`.

**Tech Stack:** Rust, `Arc::make_mut` (needs the manual `impl Clone for BTreeNode` already added by task48), criterion, the formal kernel differential test.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-08-btree-remove-mut-design.md` (authoritative).
- `remove_mut(&mut self, key: &K) -> bool` — returns `true` iff the key was present (and removed); decrements `self.len` iff `true`.
- Delete algorithm, `MIN_KEYS`, and rebalance semantics UNCHANGED — only allocation discipline differs. Reuse the existing rebalance helpers; do NOT fork in-place `rotate_*_mut`/`merge_*_mut` variants (rebalancing is a rare underflow path; its sibling clones are CoW-correct).
- CoW/snapshot isolation is the load-bearing property — `Arc::make_mut` must be applied on every node of the descent before any mutation.
- Copyright header on new files: `// SPDX-License-Identifier: Apache-2.0` then `// Copyright 2026 Peter Knego`.
- Formal drift guard: the remove path is verified, so this re-fires it — resolve by mirroring in `formal/kernel` (Task 4).
- `cargo clippy --all-targets` must pass with zero warnings (the gated lint). Do NOT run `cargo fmt` (repo has rustfmt-version drift, no fmt gate — match surrounding style).

---

### Task 1: `BTree::remove_mut` core + unit tests

**Files:**
- Modify: `src/btree.rs` (add `DeleteOutcome`, `delete_from_node_mut`, `remove_leftmost_mut`, `BTree::remove_mut`; add 4 unit tests in the existing `mod tests`)

**Interfaces:**
- Consumes: existing `BTreeNode`, `MIN_KEYS`, `fix_underfull_child`, `Arc::make_mut` (via the manual `impl Clone for BTreeNode`), and the test helpers `dump(&BTree<u64,u64>) -> Vec<(u64,u64)>` and `insert_range` already in `mod tests` (added by task48).
- Produces: `pub fn remove_mut(&mut self, key: &K) -> bool`.

- [ ] **Step 1: Write the failing unit tests**

Add to the `#[cfg(test)] mod tests` block in `src/btree.rs` (after the `insert_mut_*` tests). These reference `BTree::remove_mut`, which does not exist yet, so they fail to compile — that is the RED state.

```rust
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
    let mut next = || {
        lcg = lcg
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        lcg % n
    };

    let mut immutable = BTree::new();
    let mut in_place = BTree::new();
    let mut model = BTreeMap::new();

    // Seed both trees identically.
    for _ in 0..(n * 4) {
        let k = next();
        immutable = immutable.insert(k, k);
        in_place.insert_mut(k, k);
        model.insert(k, k);
    }
    assert_eq!(dump(&in_place), dump(&immutable));

    // Interleave inserts and removes (present + absent keys).
    for _ in 0..(n * 8) {
        let k = next();
        if lcg & 1 == 0 {
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
```

- [ ] **Step 2: Run the tests to verify they fail (RED)**

Run: `cargo test --lib btree::tests::remove_mut 2>&1 | tail -20`
Expected: compile error, `no method named remove_mut found for struct BTree`.

- [ ] **Step 3: Implement `remove_mut` and its helpers**

In `src/btree.rs`, add the `DeleteOutcome` enum near the existing `DeleteResult` (after `enum DeleteResult`), and the helpers near `delete_from_node`. Add the `remove_mut` method inside `impl<K: Ord + Clone, V> BTree<K, V>` (right after `remove`).

```rust
/// Outcome of an in-place delete into a node (see `delete_from_node_mut`).
/// The mutated node flows back through the `&mut Arc<BTreeNode>` the caller
/// passed; only the found/underfull flags propagate up.
enum DeleteOutcome {
    NotFound,
    Removed { underfull: bool },
}
```

```rust
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
            Err(child_idx) => {
                match delete_from_node_mut(&mut n.children[child_idx], key) {
                    DeleteOutcome::NotFound => DeleteOutcome::NotFound,
                    DeleteOutcome::Removed { underfull } => {
                        if underfull {
                            fix_underfull_child(&mut n.entries, &mut n.children, child_idx);
                        }
                        DeleteOutcome::Removed {
                            underfull: n.entries.len() < MIN_KEYS,
                        }
                    }
                }
            }
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
```

The `remove_mut` method (inside `impl<K: Ord + Clone, V> BTree<K, V>`, after `remove`):

```rust
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
```

- [ ] **Step 4: Run the tests to verify they pass (GREEN)**

Run: `cargo test --lib btree::tests::remove_mut 2>&1 | tail -12`
Expected: `remove_mut_matches_remove_scrambled`, `remove_mut_preserves_snapshot_isolation`, `remove_mut_chained_snapshots_independent`, `remove_mut_absent_key_is_noop` all pass.

- [ ] **Step 5: Full lib suite + clippy**

Run: `cargo test --lib 2>&1 | tail -3 && cargo clippy --lib --all-targets 2>&1 | tail -3`
Expected: all lib tests pass; clippy clean (no warnings). If clippy flags the `DeleteOutcome::Removed { .. }` unused-field pattern or similar, address minimally.

- [ ] **Step 6: Commit**

```bash
git add src/btree.rs
git commit -m "perf(btree): in-place remove_mut via Arc::make_mut"
```

---

### Task 2: Wire `remove_mut` into `Table`

**Files:**
- Modify: `src/table.rs` (`delete` at ~line 374; `delete_batch` at ~line 527)

**Interfaces:**
- Consumes: `BTree::remove_mut` (Task 1).
- Produces: `Table::delete` / `delete_batch` mutate the data tree in place.

- [ ] **Step 1: Rewire `Table::delete`**

In `src/table.rs`, in `pub fn delete(&mut self, id: u64) -> Result<Arc<R>>`, replace:

```rust
        self.data = self.data.remove(&id)?;
```
with (the preceding `get_arc` check already guarantees presence, so `remove_mut` returns `true`):
```rust
        let removed = self.data.remove_mut(&id);
        debug_assert!(removed, "delete: presence checked above");
```

- [ ] **Step 2: Rewire `Table::delete_batch`**

In `pub fn delete_batch(&mut self, ids: &[u64]) -> Result<()>`, the per-id loop currently does `match self.data.remove(&id) { ... }`. Read the surrounding lines first (`sed -n '506,545p' src/table.rs`) to see how it handles missing ids and rollback, then replace the immutable `remove` call with `remove_mut`, preserving the existing missing-id / rollback behavior. If the existing code matched on `Ok`/`Err(KeyNotFound)`, translate to the `bool` return: `false` is the missing-id case that triggers the existing rollback (`self.restore(snap)`); `true` is success. Keep the snapshot/restore rollback exactly as-is (it works unchanged — `snapshot()` clones the root Arc, so the first `remove_mut` CoW-clones and leaves the captured tree intact).

- [ ] **Step 3: Run the Table delete tests**

Run: `cargo test --lib table::tests::delete 2>&1 | tail -12`
Expected: `delete_removes_record`, `delete_on_absent_id_returns_key_not_found`, `delete_batch_removes_records_and_indexes`, `delete_batch_missing_id_fails_fast`, `delete_batch_duplicate_ids`, `delete_batch_empty_is_noop` all pass.

- [ ] **Step 4: Full lib + integration suite + clippy**

Run: `cargo test --lib --test store_integration 2>&1 | grep "test result:" && cargo clippy --all-targets 2>&1 | tail -2`
Expected: all pass, clippy clean.

- [ ] **Step 5: Commit**

```bash
git add src/table.rs
git commit -m "perf(table): use BTree::remove_mut in delete/delete_batch"
```

---

### Task 3: Concurrent-reader isolation integration test

**Files:**
- Modify: `tests/store_integration.rs` (add one test, mirroring `uncommitted_insert_mut_invisible_to_concurrent_reader`)

**Interfaces:**
- Consumes: `Store`, `Table::insert_batch`/`delete`, `remove_mut` (via `Table`).

- [ ] **Step 1: Write the test**

Append to `tests/store_integration.rs`:

```rust
// ---------------------------------------------------------------------------
// In-place delete (`BTree::remove_mut`) must not leak uncommitted deletes to
// concurrent reads. Mirror of the insert_mut isolation test.
// ---------------------------------------------------------------------------

#[test]
fn uncommitted_remove_mut_invisible_to_concurrent_reader() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let store = Store::default();
    const N: u64 = 1_000;

    // Base: commit N rows so the writer opens a populated table (shared spine).
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<u64>("t").unwrap();
        t.insert_batch((0..N).map(|i| i * 10).collect()).unwrap();
        wtx.commit().unwrap();
    }
    let base_val = |id: u64| (id - 1) * 10; // ids are 1..=N

    let rtx_before = store.begin_read(None).unwrap();

    let mutated = Arc::new(Barrier::new(2));
    let read_done = Arc::new(Barrier::new(2));

    let reader = {
        let store = store.clone();
        let mutated = Arc::clone(&mutated);
        let read_done = Arc::clone(&read_done);
        thread::spawn(move || {
            mutated.wait(); // writer has done its in-place deletes, uncommitted
            let rtx = store.begin_read(None).unwrap();
            let t = rtx.open_table::<u64>("t").unwrap();
            // Uncommitted deletes are invisible: all N rows still present.
            assert_eq!(t.len() as u64, N, "reader saw uncommitted deletes");
            assert_eq!(t.get(1), Some(&base_val(1)));
            assert_eq!(t.get(N), Some(&base_val(N)));
            read_done.wait();
        })
    };

    // Writer: delete the first half of the rows in place, uncommitted.
    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut t = wtx.open_table::<u64>("t").unwrap();
        for id in 1..=(N / 2) {
            t.delete(id).unwrap(); // -> BTree::remove_mut
        }
        assert_eq!(t.len() as u64, N / 2);
        assert_eq!(t.get(1), None);
    }
    mutated.wait();
    read_done.wait();
    wtx.commit().unwrap();

    reader.join().unwrap();

    // Reader opened before the writer is unchanged (repeatable read).
    {
        let t = rtx_before.open_table::<u64>("t").unwrap();
        assert_eq!(t.len() as u64, N);
        assert_eq!(t.get(1), Some(&base_val(1)));
    }

    // A fresh read sees the committed deletions.
    let rtx_after = store.begin_read(None).unwrap();
    let t = rtx_after.open_table::<u64>("t").unwrap();
    assert_eq!(t.len() as u64, N / 2);
    assert_eq!(t.get(1), None);
    assert_eq!(t.get(N), Some(&base_val(N)));
}
```

- [ ] **Step 2: Run it**

Run: `cargo test --test store_integration uncommitted_remove_mut_invisible_to_concurrent_reader 2>&1 | tail -6`
Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/store_integration.rs
git commit -m "test(btree): concurrent-reader isolation for in-place remove_mut"
```

---

### Task 4: Formal kernel mirror + differential test

**Files:**
- Modify: `formal/kernel/src/lib.rs` (add `BTree::remove_mut` + a differential test)

**Interfaces:**
- Consumes: the kernel's existing `BTree::remove` and `insert_mut` (added by the task48 follow-up).

- [ ] **Step 1: Add `remove_mut` to the kernel**

In `formal/kernel/src/lib.rs`, inside `impl BTree` (after `remove`), add — modeled as the in-place assignment of `remove`'s result, exactly as `insert_mut` was (the kernel's functional `Box` model makes the `Arc::make_mut` distinction invisible; the faithful mirror is that `remove_mut` mutates `self` into what `remove` returns, or leaves it unchanged if the key is absent):

```rust
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
```

- [ ] **Step 2: Add the differential test**

In the `#[cfg(test)] mod tests` block of `formal/kernel/src/lib.rs`, add (reuses the existing `lcg` helper):

```rust
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
```

- [ ] **Step 3: Run the kernel test + drift guard**

The kernel is excluded from the workspace; in a nested worktree `make test/formal-kernel` hits a spurious parent-workspace error, so test from a copy outside the repo tree:

```bash
SCRATCH=$(mktemp -d)/kernel && mkdir -p "$SCRATCH"
cp -r formal/kernel/* "$SCRATCH/" && (cd "$SCRATCH" && cargo test 2>&1 | tail -8)
```
Expected: all kernel tests pass, including `remove_mut_matches_remove_and_std_btreemap`.

Then the drift guard:
```bash
BASE=origin/main formal/scripts/check-drift.sh 2>&1 | tail -3
```
Expected: `src/btree.rs and formal/ both changed — ok.`

- [ ] **Step 4: Commit**

```bash
git add formal/kernel/src/lib.rs
git commit -m "formal(kernel): mirror remove_mut + differential test"
```

---

### Task 5: Delete-heavy A/B benchmark + task doc

**Files:**
- Modify: `benches/btree_remove_mut_bench.rs` (uncomment the `in_place` arms)
- Create: `docs/tasks/task50_btree_remove_mut.md`

**Interfaces:**
- Consumes: `BTree::remove_mut` (Task 1). The bench file + its `Cargo.toml` `[[bench]]` entry already exist (added during the perf-baseline step).

- [ ] **Step 1: Enable the `in_place` arms**

In `benches/btree_remove_mut_bench.rs`, uncomment the two `in_place` `group.bench_with_input(...)` blocks (one in `bench_delete_ascending`, one in `bench_delete_random`) and the explanatory NOTE about them being disabled. They call `t.remove_mut(k)` per key.

- [ ] **Step 2: Run the A/B**

Run: `cargo bench --bench btree_remove_mut_bench 2>&1 | grep -E "delete_(ascending|random)/(immutable|in_place)/[0-9]+|time:" | grep -v change`
Expected: both arms report for 100000 and 1000000; `in_place` should be multiples faster than `immutable` (baseline was ~3.11 s ascending / 3.66 s random @1M — expect in_place in the ~150–500 ms range, i.e. ~8–20×).

- [ ] **Step 3: Write the task doc**

Create `docs/tasks/task50_btree_remove_mut.md` following the shape of `task48_btree_insert_mut.md`: Status (with the measured A/B ratios from Step 2), Motivation (delete paid the same per-key spine clone `insert_mut` removed for inserts — cite the baseline from the spec), Design (`make_mut` descent reusing the existing rebalance helpers; `DeleteOutcome`; root collapse), Correctness (the 4 unit tests + the concurrent-reader integration test), `Table` wiring (delete/delete_batch, rollback unaffected), Benchmarks (the A/B table from Step 2, ratios-are-the-deliverable framing), and Notes (immutable `remove` retained; rebalance sibling-clone on the rare underflow path is a possible future micro-opt; CoW property covered by the integration test, out of Aeneas scope). Also move the two design docs' relevance: reference `docs/superpowers/specs/2026-07-08-btree-remove-mut-design.md`.

- [ ] **Step 4: Commit**

```bash
git add benches/btree_remove_mut_bench.rs docs/tasks/task50_btree_remove_mut.md
git commit -m "bench+docs(btree): remove_mut A/B results + task50"
```

---

## Self-Review

**Spec coverage:**
- §3 API (`remove_mut -> bool`, len decrement) → Task 1 Step 3. ✓
- §4 design (`DeleteOutcome`, `delete_from_node_mut`, `remove_leftmost_mut`, root collapse, reuse rebalance helpers) → Task 1 Step 3. ✓ (The spec floated splitting descent vs in-place-rebalance into two tasks; the code review of the actual helpers showed they're reusable verbatim, so no in-place-rebalance task is needed — recorded as a Global Constraint.)
- §5 correctness (4 unit tests + concurrent-reader integration test) → Task 1 Step 1, Task 3. ✓
- §6 Table wiring (delete + delete_batch, rollback unaffected) → Task 2. ✓
- §7 formal drift (kernel mirror + differential test) → Task 4. ✓
- §8 verification (lib/integration/clippy/kernel/A-B) → distributed across Tasks 1–5. ✓

**Placeholder scan:** Task 2 Step 2 says "read the surrounding lines then translate the missing-id/rollback behavior" rather than pasting a fixed diff — this is deliberate: the exact `delete_batch` match arms must be read first (the plan gives the precise translation rule: `false` → existing rollback path, `true` → success; keep snapshot/restore as-is). Not a vague placeholder. All code steps in Tasks 1, 3, 4 show complete code.

**Type consistency:** `remove_mut(&mut self, key: &K) -> bool` in `src/btree.rs`; kernel `remove_mut(&mut self, key: u64) -> bool` (kernel uses concrete `u64`, matching its `insert_mut`). `DeleteOutcome { NotFound, Removed { underfull: bool } }` used consistently in `delete_from_node_mut`. `remove_leftmost_mut -> ((K, Arc<V>), bool)`. `fix_underfull_child(&mut Vec<(K,Arc<V>)>, &mut Vec<Arc<BTreeNode>>, usize)` reused with its existing signature. Bench arms call `t.remove_mut(k)` where `k: &u64` (the loop yields `&u64` from `for k in keys`). ✓
