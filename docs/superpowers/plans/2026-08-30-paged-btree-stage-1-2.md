# Paged CoW B-tree (stages 1+2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A store whose data leaves live on disk and are loaded on touch, with indexes and inner levels resident and memory bounded by an operator budget — without changing the read API.

**Architecture:** Every `Arc<BTreeNode>` child pointer becomes a 16-byte `Child` slot: an atomic `meta` word (page id or `NO_PAGE`, plus an accessed bit) and an atomic pointer that is set once by CAS on fault-in and never cleared. Cloning a parent copies on-disk slots without touching the children — that single property removes the sibling-refcount cost the paging baseline measured. Pages are appended to one preallocated `pages.bin` at checkpoint (dirty = slot has `NO_PAGE`), a root record per checkpoint is the commit point, and demotion of quiet leaves is done at checkpoint by re-publishing the latest version with fresh on-disk slots, so old readers keep their nodes until `gc()`.

**Tech Stack:** Rust 2024, `bincode` 2 (serde) for record values, `PrimaryKey::encode` for keys, `crc32fast` via `crate::wal::crc32`, `std::os::unix::fs::FileExt` (`read_at`/`write_at`), `libc` 0.2 (already a transitive dependency in `Cargo.lock`; add it as an optional direct dep enabled by the `persistence` feature) for `fallocate(PUNCH_HOLE)` and `posix_fadvise`, `proptest` (existing dev-dep).

**Spec:** `docs/superpowers/specs/2026-08-30-paged-btree-stage-1-2-design.md`

## Global Constraints

- `cargo clippy -- -D warnings` must pass. Clippy is CI-gated; rustfmt is not.
- **Do not run `cargo fmt`.** Match the surrounding style (4 spaces in `btree.rs`, `checkpoint.rs`, `registry.rs`, `store.rs`, `table.rs`, `index.rs`; tabs in `overlay.rs`).
- All page-file, codec, root-record, checkpointer and index-persistence code is behind `#[cfg(feature = "persistence")]`. The `Child` slot and `NodeSource` trait are **not** feature-gated (the tree always uses slots). Test with `cargo test` and `cargo test --features persistence`; run both before every commit.
- `StoreConfig` and `Persistence` are `#[non_exhaustive]`; new config goes through builder methods.
- Read-API signatures (`get`, `range`, iterators, index lookups) do not change. A lazy fault that fails **panics** with table name, page id and cause (spec Q2).
- Encoded keys are capped at 64 KiB by `check_encoded_key_len` (registry.rs); page payloads enforce the same cap on keys.
- Spec correction carried into this plan: `BTreeNode` stores `(K, Arc<V>)` entries in **every** node, inner nodes included (`src/btree.rs:228`). Inner pages therefore carry `(key, value)` pairs plus child ids, not keys alone. Update the spec's §3 payload sketch in Task 6.
- No perf conclusions from local runs (±2× noise). The acceptance gate (Task 16) is shape-only locally.
- The formal cite guard (`check-cites.py`) breaks on line shifts in `src/store.rs`/`src/persistence.rs`/`src/wal.rs`; re-anchor cites by reading, in the task that shifts them (Tasks 11–14).
- Commit after every task on branch `bench/paging-baseline` (spec committed at `2b823d7`), or a new `feat/paged-btree` branched from it — executor's choice, state it in the first commit.

## File Structure

| file | responsibility |
|---|---|
| `src/child.rs` (new) | `Child<K,V>` slot, `NodeSource<K,V>` trait, `PageId`, `NO_PAGE`, accessed bit. No I/O, no feature gate. |
| `src/btree.rs` (modify) | Children become `Child` slots; `BTree` carries `root: Child` + `source`; descent/mutation thread the source; new primitives `height`, `write_dirty`, `demote_leaves`, `changed_page_ids`, `from_root_page`, `load_inner_levels`. |
| `src/pagefile.rs` (new, persistence) | `PageFile`: preallocated append-only file, `append`/`read`/`sync`/`punch`, `FADV_RANDOM`, page header + CRC. |
| `src/pagecodec.rs` (new, persistence) | `PageKind`, `NodeCodec<K,V>` (encode/decode a node payload), `PagedSource<K,V>: NodeSource`. |
| `src/checkpoint.rs` (modify, persistence) | `CheckpointKind::Paged`, root record (`PagedRoot`) read/write, `.root` discovery, cleanup. |
| `src/table.rs`, `src/index.rs` (modify) | `MergeableTable`/`IndexMaintainer` paged hooks; `define_persisted_index`; `Residency`. |
| `src/registry.rs` (modify, persistence) | `attach_paged` closure per `(R, K)`. |
| `src/store.rs`, `src/persistence.rs` (modify) | `PagedOptions`, `Persistence::paged`, `PagedState`, checkpoint phases, demotion install, recovery branch, checkpointer thread. |
| `src/error.rs` (modify) | `IndexDefinitionMismatch`, `PagedFormatRequired`. |
| `tests/paged_*.rs` (new) | integration tests per phase; `tests/checkpoint_chain_equivalence.rs` extended. |
| `compare_benches/src/bin/paging_matrix.rs`, `Makefile` | `ultima-paged` engine; `make paging/check`. |

---

## Phase A — the slot (no persistence dependency)

### Task 1: `Child<K,V>` slot and `NodeSource` trait

**Files:**
- Create: `src/child.rs`
- Modify: `src/lib.rs` (add `mod child;`), `src/btree.rs:238` (`BTreeNode` → `pub(crate)`, fields `pub(crate)`)
- Test: unit tests in `src/child.rs`

**Interfaces:**
- Produces:
  ```rust
  pub(crate) type PageId = u64;
  pub(crate) const NO_PAGE: u64 = (1u64 << 63) - 1;     // all low 63 bits set
  const ACCESSED: u64 = 1u64 << 63;
  pub(crate) trait NodeSource<K, V>: Send + Sync {
      /// Read + decode one page into a fresh node. `Err` = I/O or CRC failure.
      fn read_node(&self, id: PageId) -> crate::Result<Arc<BTreeNode<K, V>>>;
      /// Called by `Child::make_mut` when it clones a *clean* node (dirty-bytes trigger).
      fn note_dirty(&self, _bytes: usize) {}
      /// Name used in the fault-in panic message.
      fn name(&self) -> &str { "<unnamed>" }
  }
  pub(crate) struct Child<K, V> { meta: AtomicU64, node: AtomicPtr<BTreeNode<K, V>>, _own: PhantomData<Arc<BTreeNode<K, V>>> }
  impl<K, V> Child<K, V> {
      pub(crate) fn resident(node: Arc<BTreeNode<K, V>>) -> Self;      // meta = NO_PAGE
      pub(crate) fn on_disk(id: PageId) -> Self;                        // node = null
      pub(crate) fn page_id(&self) -> Option<PageId>;
      pub(crate) fn is_loaded(&self) -> bool;
      pub(crate) fn set_page_id(&self, id: PageId);                      // NO_PAGE -> id only (debug_assert)
      pub(crate) fn mark_accessed(&self);                                // fetch_or only if clear
      pub(crate) fn take_accessed(&self) -> bool;                        // read + clear
      pub(crate) fn load(&self, src: Option<&dyn NodeSource<K, V>>) -> &BTreeNode<K, V>;   // panics on failure
      pub(crate) fn load_arc(&self, src: Option<&dyn NodeSource<K, V>>) -> Arc<BTreeNode<K, V>>;
      pub(crate) fn strong_count(&self) -> Option<usize>;
      pub(crate) fn same_node(a: &Self, b: &Self) -> bool;               // same page id, or same Arc ptr
  }
  impl<K: Clone, V> Child<K, V> { pub(crate) fn make_mut(&mut self, src: Option<&dyn NodeSource<K, V>>) -> &mut BTreeNode<K, V>; }
  impl<K, V> Clone for Child<K, V>;   // on-disk: copy meta only. loaded: increment_strong_count.
  impl<K, V> Drop for Child<K, V>;
  ```

- [ ] **Step 1: Write the failing tests**

```rust
// src/child.rs (bottom)
#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::BTreeNode;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    /// In-memory "disk": page id -> node. Counts reads.
    pub(crate) struct MockDisk<K, V> {
        pub pages: Mutex<HashMap<PageId, Arc<BTreeNode<K, V>>>>,
        pub reads: AtomicUsize,
        pub dirty_bytes: AtomicUsize,
    }
    impl<K: Clone + Send + Sync, V: Send + Sync> NodeSource<K, V> for MockDisk<K, V> {
        fn read_node(&self, id: PageId) -> crate::Result<Arc<BTreeNode<K, V>>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let pages = self.pages.lock().unwrap();
            let n = pages.get(&id).ok_or_else(|| crate::Error::Persistence(format!("no page {id}")))?;
            // A fresh Arc, like a real decode.
            Ok(Arc::new((**n).clone()))
        }
        fn note_dirty(&self, bytes: usize) { self.dirty_bytes.fetch_add(bytes, Ordering::Relaxed); }
    }
    impl<K, V> MockDisk<K, V> {
        pub fn new() -> Self { Self { pages: Mutex::new(HashMap::new()), reads: AtomicUsize::new(0), dirty_bytes: AtomicUsize::new(0) } }
        pub fn put(&self, id: PageId, n: Arc<BTreeNode<K, V>>) { self.pages.lock().unwrap().insert(id, n); }
    }

    fn leaf(keys: &[u64]) -> Arc<BTreeNode<u64, u64>> {
        Arc::new(BTreeNode { entries: keys.iter().map(|k| (*k, Arc::new(*k * 10))).collect(), children: Default::default() })
    }

    #[test]
    fn resident_slot_reports_no_page_and_loaded() {
        let c = Child::resident(leaf(&[1]));
        assert_eq!(c.page_id(), None);
        assert!(c.is_loaded());
        assert_eq!(c.strong_count(), Some(1));
    }

    #[test]
    fn on_disk_slot_loads_once_and_counts_one_read() {
        let disk = MockDisk::new();
        disk.put(4096, leaf(&[7, 8]));
        let c: Child<u64, u64> = Child::on_disk(4096);
        assert!(!c.is_loaded());
        let n = c.load(Some(&disk));
        assert_eq!(n.entries.len(), 2);
        let _again = c.load(Some(&disk));
        assert_eq!(disk.reads.load(Ordering::Relaxed), 1, "second load must hit the pointer, not the disk");
        assert_eq!(c.page_id(), Some(4096), "loading keeps the page id (resident-clean)");
    }

    #[test]
    fn clone_of_on_disk_slot_touches_nothing() {
        let disk = MockDisk::new();
        disk.put(1, leaf(&[1]));
        let c: Child<u64, u64> = Child::on_disk(1);
        let d = c.clone();
        assert_eq!(disk.reads.load(Ordering::Relaxed), 0);
        assert!(!d.is_loaded());
        assert_eq!(d.page_id(), Some(1));
    }

    #[test]
    fn clone_of_loaded_slot_bumps_refcount() {
        let c = Child::resident(leaf(&[1]));
        let d = c.clone();
        assert_eq!(c.strong_count(), Some(2));
        drop(d);
        assert_eq!(c.strong_count(), Some(1));
    }

    #[test]
    fn make_mut_on_shared_node_clones_and_marks_dirty() {
        let disk = MockDisk::new();
        disk.put(1, leaf(&[1, 2]));
        let mut c: Child<u64, u64> = Child::on_disk(1);
        c.load(Some(&disk));
        let keep = c.load_arc(Some(&disk)); // second owner
        let n = c.make_mut(Some(&disk));
        n.entries.push((3, Arc::new(30)));
        assert_eq!(c.page_id(), None, "a CoW'd node is dirty");
        assert_eq!(keep.entries.len(), 2, "old owner unaffected");
        assert!(disk.dirty_bytes.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn make_mut_on_unique_node_mutates_in_place_and_stays_clean_until_edited() {
        // Uniquely owned resident-clean node: make_mut must still mark dirty
        // (the node's contents are about to diverge from its page).
        let disk = MockDisk::new();
        disk.put(1, leaf(&[1]));
        let mut c: Child<u64, u64> = Child::on_disk(1);
        c.load(Some(&disk));
        let before = c.load_arc(Some(&disk)); let before_ptr = Arc::as_ptr(&before); drop(before);
        let n = c.make_mut(Some(&disk));
        assert_eq!(n as *const _, before_ptr, "unique owner: in place");
        assert_eq!(c.page_id(), None);
    }

    #[test]
    fn accessed_bit_is_second_chance() {
        let c = Child::resident(leaf(&[1]));
        assert!(!c.take_accessed());
        c.mark_accessed();
        assert!(c.take_accessed());
        assert!(!c.take_accessed());
    }

    #[test]
    fn concurrent_fault_in_leaves_exactly_one_arc() {
        let disk = Arc::new(MockDisk::new());
        disk.put(9, leaf(&[1]));
        let c: Arc<Child<u64, u64>> = Arc::new(Child::on_disk(9));
        let hs: Vec<_> = (0..8).map(|_| { let c = c.clone(); let d = disk.clone(); std::thread::spawn(move || { c.load(Some(&*d)); }) }).collect();
        for h in hs { h.join().unwrap(); }
        assert_eq!(c.strong_count(), Some(1), "losers must drop their copies");
    }

    #[test]
    #[should_panic(expected = "page 77")]
    fn load_failure_panics_with_page_id() {
        let disk: MockDisk<u64, u64> = MockDisk::new();
        let c: Child<u64, u64> = Child::on_disk(77);
        c.load(Some(&disk));
    }

    #[test]
    fn same_node_by_page_or_pointer() {
        let a: Child<u64, u64> = Child::on_disk(5);
        let b: Child<u64, u64> = Child::on_disk(5);
        assert!(Child::same_node(&a, &b));
        let n = leaf(&[1]);
        let c = Child::resident(n.clone());
        let d = Child::resident(n);
        assert!(Child::same_node(&c, &d));
        let e = Child::resident(leaf(&[1]));
        assert!(!Child::same_node(&c, &e), "dirty nodes equal nothing but themselves");
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test child:: 2>&1 | tail -5`
Expected: compile error — `crate::child` does not exist.

- [ ] **Step 3: Implement `src/child.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The child slot: what a B-tree node holds instead of `Arc<BTreeNode>`.
//!
//! Three states — resident-dirty (`node` set, `NO_PAGE`), resident-clean
//! (`node` set, page id), on-disk (`node` null, page id). `null + NO_PAGE`
//! is a bug. The pointer is set once by CAS and never cleared, so a
//! `&BTreeNode` borrowed through `load` is valid for the slot's lifetime;
//! that is what keeps `Table::get -> Option<&R>` unchanged. Cloning an
//! on-disk slot copies one word and touches no child — the fix for the
//! sibling-refcount cost measured in
//! `docs/benchmarks/paging-baseline-local-2026-08-29.md`.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

use crate::btree::BTreeNode;

pub(crate) type PageId = u64;
/// "Not on disk": all 63 low bits set. Real ids are byte offsets < 2^63.
pub(crate) const NO_PAGE: u64 = (1u64 << 63) - 1;
const ACCESSED: u64 = 1u64 << 63;
const ID_MASK: u64 = NO_PAGE;

pub(crate) trait NodeSource<K, V>: Send + Sync {
    fn read_node(&self, id: PageId) -> crate::Result<Arc<BTreeNode<K, V>>>;
    fn note_dirty(&self, _bytes: usize) {}
    fn name(&self) -> &str { "<unnamed>" }
}

pub(crate) struct Child<K, V> {
    meta: AtomicU64,
    node: AtomicPtr<BTreeNode<K, V>>,
    /// `AtomicPtr` is `Send + Sync` for any `T`; this restores the auto
    /// traits an owned `Arc<BTreeNode<K, V>>` would have had.
    _own: PhantomData<Arc<BTreeNode<K, V>>>,
}

impl<K, V> Child<K, V> {
    pub(crate) fn resident(node: Arc<BTreeNode<K, V>>) -> Self {
        Self { meta: AtomicU64::new(NO_PAGE), node: AtomicPtr::new(Arc::into_raw(node) as *mut _), _own: PhantomData }
    }
    pub(crate) fn on_disk(id: PageId) -> Self {
        debug_assert!(id < NO_PAGE);
        Self { meta: AtomicU64::new(id), node: AtomicPtr::new(std::ptr::null_mut()), _own: PhantomData }
    }
    pub(crate) fn page_id(&self) -> Option<PageId> {
        let id = self.meta.load(Ordering::Acquire) & ID_MASK;
        (id != NO_PAGE).then_some(id)
    }
    pub(crate) fn is_loaded(&self) -> bool { !self.node.load(Ordering::Acquire).is_null() }
    pub(crate) fn set_page_id(&self, id: PageId) {
        debug_assert!(id < NO_PAGE);
        // Preserve the accessed bit; only the id part changes (NO_PAGE -> id).
        let mut cur = self.meta.load(Ordering::Acquire);
        loop {
            debug_assert_eq!(cur & ID_MASK, NO_PAGE, "set_page_id on a node that already has a page");
            match self.meta.compare_exchange_weak(cur, (cur & ACCESSED) | id, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(c) => cur = c,
            }
        }
    }
    pub(crate) fn mark_accessed(&self) {
        if self.meta.load(Ordering::Relaxed) & ACCESSED == 0 {
            self.meta.fetch_or(ACCESSED, Ordering::Relaxed);
        }
    }
    pub(crate) fn take_accessed(&self) -> bool { self.meta.fetch_and(!ACCESSED, Ordering::Relaxed) & ACCESSED != 0 }

    pub(crate) fn load(&self, src: Option<&dyn NodeSource<K, V>>) -> &BTreeNode<K, V> {
        let p = self.node.load(Ordering::Acquire);
        if !p.is_null() {
            self.mark_accessed();
            // SAFETY: set once, never cleared; the Arc it came from is owned by this slot.
            return unsafe { &*p };
        }
        self.fault_in(src)
    }
    #[cold]
    fn fault_in(&self, src: Option<&dyn NodeSource<K, V>>) -> &BTreeNode<K, V> {
        let id = self.page_id().expect("Child: null pointer and NO_PAGE (corrupt slot)");
        let src = src.unwrap_or_else(|| panic!("Child: page {id} referenced but the tree has no NodeSource"));
        let fresh = match src.read_node(id) {
            Ok(n) => n,
            Err(e) => panic!("ultima_db: cannot load page {id} of {}: {e}", src.name()),
        };
        let raw = Arc::into_raw(fresh) as *mut BTreeNode<K, V>;
        match self.node.compare_exchange(std::ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => { self.mark_accessed(); unsafe { &*raw } }
            Err(winner) => {
                // SAFETY: we own `raw`; nobody else saw it.
                unsafe { drop(Arc::from_raw(raw)) };
                self.mark_accessed();
                unsafe { &*winner }
            }
        }
    }
    pub(crate) fn load_arc(&self, src: Option<&dyn NodeSource<K, V>>) -> Arc<BTreeNode<K, V>> {
        self.load(src); // ensure resident; discard the borrow
        let p = self.node.load(Ordering::Acquire) as *const BTreeNode<K, V>;
        // SAFETY: p came straight from the slot's AtomicPtr, i.e. from Arc::into_raw;
        // the slot still owns one count, so the allocation is live. (Deriving the
        // pointer from the `&BTreeNode` `load` returns instead is UB — Miri rejects
        // it under both Stacked and Tree Borrows: that borrow's provenance is
        // read-only and bounded to the payload, but increment_strong_count writes
        // through it to the ArcInner's refcount.)
        unsafe { Arc::increment_strong_count(p); Arc::from_raw(p) }
    }
    pub(crate) fn strong_count(&self) -> Option<usize> {
        let p = self.node.load(Ordering::Acquire);
        if p.is_null() { return None; }
        // SAFETY: as above; count read without taking ownership.
        let a = unsafe { std::mem::ManuallyDrop::new(Arc::from_raw(p)) };
        Some(Arc::strong_count(&a))
    }
    pub(crate) fn same_node(a: &Self, b: &Self) -> bool {
        match (a.page_id(), b.page_id()) {
            (Some(x), Some(y)) => x == y,
            _ => {
                let (pa, pb) = (a.node.load(Ordering::Acquire), b.node.load(Ordering::Acquire));
                !pa.is_null() && pa == pb
            }
        }
    }
    /// Size estimate reported to `note_dirty`: one node's inline storage.
    pub(crate) const NODE_BYTES: usize = std::mem::size_of::<BTreeNode<K, V>>();
}

impl<K: Clone, V> Child<K, V> {
    pub(crate) fn make_mut(&mut self, src: Option<&dyn NodeSource<K, V>>) -> &mut BTreeNode<K, V> {
        self.load(src);
        let was_clean = self.page_id().is_some();
        let p = *self.node.get_mut();
        // SAFETY: we hold &mut self, so no concurrent CAS; p is non-null after load.
        let mut arc = unsafe { Arc::from_raw(p) };
        let r = Arc::make_mut(&mut arc); // clones iff shared
        let _ = r;
        let raw = Arc::into_raw(arc) as *mut BTreeNode<K, V>;
        *self.node.get_mut() = raw;
        // Whether cloned or edited in place, the contents now diverge from the page.
        *self.meta.get_mut() = (*self.meta.get_mut() & ACCESSED) | NO_PAGE;
        if was_clean && let Some(s) = src { s.note_dirty(Self::NODE_BYTES); }
        unsafe { &mut *raw }
    }
}

impl<K, V> Clone for Child<K, V> {
    fn clone(&self) -> Self {
        let p = self.node.load(Ordering::Acquire);
        if !p.is_null() {
            // SAFETY: slot owns a count on p.
            unsafe { Arc::increment_strong_count(p) };
        }
        Self { meta: AtomicU64::new(self.meta.load(Ordering::Acquire)), node: AtomicPtr::new(p), _own: PhantomData }
    }
}

impl<K, V> Drop for Child<K, V> {
    fn drop(&mut self) {
        let p = *self.node.get_mut();
        if !p.is_null() {
            // SAFETY: slot owns exactly one count.
            unsafe { drop(Arc::from_raw(p)) };
        }
    }
}
```

Also in `src/btree.rs`: make `struct BTreeNode` → `pub(crate) struct BTreeNode`, fields `pub(crate) entries`, `pub(crate) children`; `Entries`/`Children` aliases `pub(crate)`. Add `mod child;` to `src/lib.rs` (next to `mod btree;`). `FixedVec` needs `Default` (empty) for the test's `children: Default::default()` — add `impl<E, const N: usize> Default for FixedVec<E, N>` if missing (`Self::new()`).

- [ ] **Step 4: Run tests**

Run: `cargo test child:: && cargo test --features persistence child::`
Expected: all 10 pass.

- [ ] **Step 5: Commit**

```bash
git add src/child.rs src/lib.rs src/btree.rs
git commit -m "feat(btree): Child slot with set-once fault-in and refcount-free clone of on-disk children"
```

---

### Task 2: Thread `Child` slots and a `NodeSource` through `BTree`

**Files:**
- Modify: `src/btree.rs` — `Children` alias (`:232`), `BTree` struct (`:272`), every `Arc::make_mut`/`Arc::ptr_eq`/`&n.children[i]` site listed by `grep -n "make_mut\|ptr_eq\|children\[" src/btree.rs`
- Test: existing `cargo test btree::` must pass unchanged (source `None`); new tests added in Task 3.

**Interfaces:**
- Consumes: Task 1.
- Produces:
  ```rust
  pub(crate) type Children<K, V> = FixedVec<Child<K, V>, { MAX_KEYS + 2 }>;
  pub struct BTree<K, V> { root: Child<K, V>, len: usize, source: Option<Arc<dyn NodeSource<K, V>>> }
  impl<K, V> BTree<K, V> { pub(crate) fn source(&self) -> Option<&dyn NodeSource<K, V>>; pub(crate) fn set_source(&mut self, s: Option<Arc<dyn NodeSource<K, V>>>); }
  ```
  Every private recursive function gains a trailing `src: Option<&dyn NodeSource<K, V>>` parameter.

- [ ] **Step 1: Mechanical replacement**

Apply, in order, each keeping the file compiling before the next:

1. `type Children<K, V> = FixedVec<Child<K, V>, { MAX_KEYS + 2 }>;` and `use crate::child::{Child, NodeSource, PageId, NO_PAGE};`.
2. `BTree { root: Child<K, V>, len, source: Option<Arc<dyn NodeSource<K, V>>> }`. `BTree::new()` → `root: Child::resident(Arc::new(BTreeNode{..}))`, `source: None`. `Clone for BTree` clones all three (`Arc` clone for source). Add `source()`/`set_source()`.
3. Every `Arc::make_mut(&mut x)` where `x: &mut Arc<BTreeNode>` becomes `x.make_mut(src)`; every `&node.children[i]` passed to a recursive fn becomes `node.children[i].load(src)` (read paths) or `&mut node.children[i]` (write paths, which then call `.make_mut(src)` inside). Sites: `get_in_node`, `get_arc_in_node`, `insert_into_node` (immutable path builds new `Arc`s → wrap in `Child::resident`), `insert_into_node_mut` (`:1283`), `maybe_split_mut` (`right` → `Child::resident(right)` at the insertion site), `delete_from_node`/`delete_from_node_mut` (`:1586`), `remove_leftmost*`, `fix_underfull_child`, `rotate_left/right` (`:1486,1510`: `left_part[idx-1].make_mut(src)`), `merge_with_left/right`, `absorb(left, right: Child)` → `let arc = right.load_arc(src); drop(right); /* release the slot's own count first, or try_unwrap can never succeed */ let rn = Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone());`, `freeze_leaf`/`freeze_internal` (`:2071`: return `Child::resident(Arc::new(..))`), `fix_right_spine_tail`, `LevelBuilder.children: Vec<Child<K,V>>`.
4. `BTreeRange::descend_*` take `&'a Child<K, V>` and call `.load(src)` — `BTreeRange` gains `src: Option<&'a dyn NodeSource<K, V>>`.
5. `DiffCursor` stack holds `&'a Child<K, V>`; `Arc::ptr_eq(a, b)` → `Child::same_node(a, b)`; value identity `Arc::ptr_eq(nv, bv)` unchanged (values are still `Arc<V>`).
6. `BTree::diff` short-circuit `Arc::ptr_eq(&self.root, &base.root)` → `Child::same_node(&self.root, &base.root)`.
7. `remove_mut`'s root collapse (`:526`): `let root = self.root.make_mut(src); if root.entries.is_empty() && !root.children.is_empty() { let only = root.children.remove(0); self.root = only; }` — moving a `Child` out is a plain move.

Public method bodies pass `self.source.as_deref()` as `src`.

- [ ] **Step 2: Run the whole existing suite**

Run: `cargo test && cargo test --features persistence && cargo clippy -- -D warnings`
Expected: everything green with zero behaviour change (source is `None` everywhere). The `send_bounds` test (`tests/send_bounds.rs`) must still pass — `PhantomData<Arc<BTreeNode>>` in `Child` is what keeps `BTree<K, V>: Send + Sync` iff `K, V: Send + Sync`.

- [ ] **Step 3: Regression check on the in-memory path**

Run: `cargo bench --bench btree_get_bench -- --noplot 2>&1 | grep -E "time:" | head` and the same for `btree_insert_mut_bench` (`benches/`). Record the numbers in the commit message as "local, ±2×, sanity only". A 16-byte slot changes node size (`Children` 520 → 1040 B); a >2× local regression on `get` is a bug in the descent (an accidental `load_arc` where `load` was meant), not noise.

- [ ] **Step 4: Commit**

```bash
git add src/btree.rs
git commit -m "refactor(btree): children are Child slots; descent and mutation thread an optional NodeSource"
```

---

### Task 3: Tree primitives — `height`, `write_dirty`, `demote_leaves`, `changed_page_ids`, `from_root_page`, `load_inner_levels`

**Files:**
- Modify: `src/btree.rs` (new `impl<K: Ord + Clone, V> BTree<K, V>` block near `diff`)
- Test: `src/btree.rs` tests module (uses `crate::child::tests::MockDisk` — make that module `pub(crate)` under `#[cfg(test)]`)

**Interfaces:**
- Produces:
  ```rust
  impl<K: Ord + Clone, V> BTree<K, V> {
      /// Levels below the root; 0 = root is a leaf. Loads only the leftmost path.
      pub(crate) fn height(&self) -> usize;
      /// Post-order over NO_PAGE slots. `write(node, is_leaf)` returns the id it stored the node under;
      /// children are written before their parent so a parent's payload can name them.
      /// Returns the root's page id (writing it if dirty).
      pub(crate) fn write_dirty(&self, write: &mut dyn FnMut(&BTreeNode<K, V>, bool) -> PageId) -> PageId;
      /// Build a new version in which leaf slots with a page id and a clear accessed bit are
      /// on-disk. Processes at most `budget` leaf-parents, starting after `cursor` (the max key
      /// of the last parent processed). Returns (new tree, leaves demoted, next cursor / None when done).
      pub(crate) fn demote_leaves(&self, cursor: Option<&K>, budget: usize) -> (BTree<K, V>, usize, Option<K>);
      /// Page ids referenced by `prev` and not by `self`, walking only inner subtrees whose page
      /// id differs between the two (identical-id subtrees are skipped whole).
      pub(crate) fn changed_page_ids(&self, prev: &BTree<K, V>) -> Vec<PageId>;
      /// A tree whose root is on disk. `len` from the root record.
      pub(crate) fn from_root_page(id: PageId, len: usize, source: Arc<dyn NodeSource<K, V>>) -> Self;
      /// Fault in every non-leaf node; leaves stay on disk.
      pub(crate) fn load_inner_levels(&self);
      /// Bytes of resident leaves, estimated as loaded-leaf-slots × NODE_BYTES.
      pub(crate) fn resident_leaf_estimate(&self) -> usize;
  }
  ```

- [ ] **Step 1: Write the failing tests**

```rust
// in src/btree.rs tests module
mod paged {
    use super::*;
    use crate::child::tests::MockDisk;
    use std::sync::Arc;

    /// Write every dirty node into `disk`, assigning sequential ids. Returns the root id.
    fn flush(t: &BTree<u64, u64>, disk: &MockDisk<u64, u64>, next: &mut u64) -> u64 {
        t.write_dirty(&mut |node, _leaf| { *next += 4096; disk.put(*next, Arc::new(node.clone())); *next })
    }
    fn tree(n: u64) -> BTree<u64, u64> { BTree::from_sorted((1..=n).map(|k| (k, Arc::new(k)))) }

    #[test]
    fn height_of_bulk_tree() {
        assert_eq!(tree(10).height(), 0);
        assert_eq!(tree(10_000).height(), 2, "10k rows at MAX_KEYS=63: leaves, one inner level, root");
    }

    #[test]
    fn write_dirty_is_post_order_and_marks_clean() {
        let disk = MockDisk::new();
        let t = tree(5_000);
        let mut next = 0;
        let mut order = Vec::new();
        let root = t.write_dirty(&mut |node, leaf| { next += 1; order.push((next, leaf)); disk.put(next, Arc::new(node.clone())); next });
        assert_eq!(order.last().unwrap(), &(root, false), "root written last");
        assert!(order.iter().take_while(|(_, l)| *l).count() > 0, "leaves before their parent");
        // Nothing dirty remains: a second walk writes zero pages.
        let mut writes = 0;
        t.write_dirty(&mut |_, _| { writes += 1; 0 });
        assert_eq!(writes, 0);
    }

    #[test]
    fn write_dirty_after_one_insert_writes_exactly_one_path() {
        let disk = MockDisk::new();
        let mut t = tree(5_000);
        let mut next = 0;
        flush(&t, &disk, &mut next);
        t.set_source(Some(Arc::new(MockDisk::<u64, u64>::new())));
        t.insert_mut(2_500, 1);
        let mut writes = 0;
        t.write_dirty(&mut |_, _| { writes += 1; next += 4096; next });
        assert_eq!(writes, t.height() + 1, "one node per level on the CoW path");
    }

    #[test]
    fn demote_then_read_faults_exactly_touched_leaves() {
        let disk = Arc::new(MockDisk::new());
        let mut t = tree(20_000);
        let mut next = 0;
        flush(&t, &disk, &mut next);
        t.set_source(Some(disk.clone()));
        let (t2, demoted, cursor) = t.demote_leaves(None, usize::MAX);
        assert!(cursor.is_none());
        assert!(demoted > 300, "20k rows ≈ 318 leaves, all quiet");
        assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), 0, "demotion loads nothing");
        for k in [1u64, 2, 3, 10_000, 19_999] { assert_eq!(t2.get(&k), Some(&k)); }
        // 1,2,3 share a leaf; 10_000 and 19_999 are two more: 3 leaves.
        assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), 3);
        // Old version untouched and fully resident.
        assert_eq!(t.resident_leaf_estimate() > t2.resident_leaf_estimate(), true);
    }

    #[test]
    fn demote_gives_accessed_leaves_a_second_chance() {
        let disk = Arc::new(MockDisk::new());
        let mut t = tree(20_000);
        let mut next = 0;
        flush(&t, &disk, &mut next);
        t.set_source(Some(disk.clone()));
        t.get(&5); // marks that leaf accessed
        let (t2, d1, _) = t.demote_leaves(None, usize::MAX);
        assert_eq!(t2.get(&5), Some(&5));
        assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), 0, "accessed leaf survived one pass");
        let (t3, d2, _) = t2.demote_leaves(None, usize::MAX);
        assert_eq!(d2, 1, "second pass takes it");
        let _ = (t3, d1);
    }

    #[test]
    fn demote_respects_budget_and_resumes_from_cursor() {
        let disk = Arc::new(MockDisk::new());
        let mut t = tree(20_000);
        let mut next = 0;
        flush(&t, &disk, &mut next);
        t.set_source(Some(disk.clone()));
        let (t2, d1, c1) = t.demote_leaves(None, 2);
        assert!(c1.is_some() && d1 <= 2 * 64);
        let (t3, d2, c2) = t2.demote_leaves(c1.as_ref(), usize::MAX);
        assert!(c2.is_none());
        assert!(d1 + d2 > 300);
        assert_eq!(t3.len(), 20_000);
    }

    #[test]
    fn changed_page_ids_is_the_replaced_path() {
        let disk = Arc::new(MockDisk::new());
        let mut t = tree(20_000);
        let mut next = 0;
        flush(&t, &disk, &mut next);
        let prev = t.clone();
        t.set_source(Some(disk.clone()));
        t.insert_mut(7, 70);
        flush(&t, &disk, &mut next);
        let dead = t.changed_page_ids(&prev);
        assert_eq!(dead.len(), t.height() + 1, "old leaf + old inner path + old root");
        assert!(dead.iter().all(|id| prev_has(&prev, *id)));
        fn prev_has(p: &BTree<u64, u64>, id: u64) -> bool { let mut found = false; p.write_dirty(&mut |_, _| 0); /* no-op */ p.for_each_page_id(&mut |x| found |= x == id); found }
    }

    #[test]
    fn from_root_page_and_load_inner_levels() {
        let disk = Arc::new(MockDisk::new());
        let t = tree(20_000);
        let mut next = 0;
        let root = flush(&t, &disk, &mut next);
        let t2: BTree<u64, u64> = BTree::from_root_page(root, 20_000, disk.clone());
        t2.load_inner_levels();
        let inner = disk.reads.load(std::sync::atomic::Ordering::Relaxed);
        assert!(inner < 20, "root + one inner level (~6 nodes) + one leaf for height probing");
        assert_eq!(t2.get(&123), Some(&123));
        assert_eq!(disk.reads.load(std::sync::atomic::Ordering::Relaxed), inner + 1);
    }

    proptest! {
        #[test]
        fn on_disk_twin_agrees_and_faults_once_per_leaf(ops in proptest::collection::vec((0u64..5_000, 0u8..3), 1..300)) {
            let disk = Arc::new(MockDisk::new());
            let mut resident = tree(5_000);
            let mut next = 0;
            flush(&resident, &disk, &mut next);
            let mut paged = { let mut p = resident.clone(); p.set_source(Some(disk.clone())); p.demote_leaves(None, usize::MAX).0 };
            for (k, op) in ops {
                match op {
                    0 => prop_assert_eq!(resident.get(&k), paged.get(&k)),
                    1 => { resident.insert_mut(k, k + 1); paged.insert_mut(k, k + 1); }
                    _ => { resident.remove_mut(&k); paged.remove_mut(&k); }
                }
            }
            let a: Vec<_> = resident.range(..).map(|(k, v)| (*k, *v)).collect();
            let b: Vec<_> = paged.range(..).map(|(k, v)| (*k, *v)).collect();
            prop_assert_eq!(a, b);
        }
    }
}
```

Add a small helper `pub(crate) fn for_each_page_id(&self, f: &mut dyn FnMut(PageId))` (walk resident nodes, report every slot's page id) — it's used by tests and by Task 11's punch bookkeeping.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test btree::tests::paged 2>&1 | grep -E "error|not found" | head -5`
Expected: methods not found.

- [ ] **Step 3: Implement**

```rust
impl<K: Ord + Clone, V> BTree<K, V> {
    pub(crate) fn height(&self) -> usize {
        let src = self.source.as_deref();
        let mut h = 0;
        let mut n = self.root.load(src);
        while !n.children.is_empty() { n = n.children[0].load(src); h += 1; }
        h
    }

    pub(crate) fn write_dirty(&self, write: &mut dyn FnMut(&BTreeNode<K, V>, bool) -> PageId) -> PageId {
        fn go<K, V>(slot: &Child<K, V>, src: Option<&dyn NodeSource<K, V>>, write: &mut dyn FnMut(&BTreeNode<K, V>, bool) -> PageId) -> PageId {
            if let Some(id) = slot.page_id() { return id; }
            let node = slot.load(src); // resident-dirty: already loaded
            for c in node.children.iter() { go(c, src, write); }
            let id = write(node, node.children.is_empty());
            slot.set_page_id(id);
            id
        }
        go(&self.root, self.source.as_deref(), write)
    }

    pub(crate) fn demote_leaves(&self, cursor: Option<&K>, budget: usize) -> (BTree<K, V>, usize, Option<K>) {
        let src = self.source.as_deref();
        let h = self.height();
        if h == 0 { return (self.clone(), 0, None); }
        let mut out = self.clone();
        let mut demoted = 0usize;
        let mut left = budget;
        let mut last: Option<K> = None;
        // depth counts down; at depth 1 the node's children are leaves.
        fn go<K: Ord + Clone, V>(slot: &mut Child<K, V>, depth: usize, src: Option<&dyn NodeSource<K, V>>, cursor: Option<&K>, left: &mut usize, demoted: &mut usize, last: &mut Option<K>) -> bool /*exhausted*/ {
            if *left == 0 { return true; }
            let node_ref = slot.load(src);
            if depth == 1 {
                // Skip parents entirely ≤ cursor.
                if let (Some(c), Some(maxk)) = (cursor, node_ref.entries.last().map(|e| &e.0)) && maxk <= c { return false; }
                let any = node_ref.children.iter().any(|c| c.is_loaded() && c.page_id().is_some());
                if !any { return false; }
                let n = slot.make_mut(src); // CoW this parent only
                for c in n.children.iter_mut() {
                    match (c.is_loaded(), c.page_id()) {
                        (true, Some(id)) => { if c.take_accessed() { /* second chance */ } else { *c = Child::on_disk(id); *demoted += 1; } }
                        _ => {}
                    }
                }
                *last = n.entries.last().map(|e| e.0.clone());
                *left -= 1;
                return *left == 0;
            }
            let n = slot.make_mut(src);
            for c in n.children.iter_mut() { if go(c, depth - 1, src, cursor, left, demoted, last) { return true; } }
            false
        }
        // NOTE: make_mut on the path marks those inner nodes dirty (NO_PAGE). The next checkpoint
        // rewrites them — that is the "checkpoint CoWs the inner tree proportional to demoted
        // parents" cost the spec states. Restore their page ids if nothing changed: a parent whose
        // children slots are all unchanged after the pass is re-marked with its old id below.
        let exhausted = go(&mut out.root, h, src, cursor, &mut left, &mut demoted, &mut last);
        (out, demoted, if exhausted { last } else { None })
    }

    pub(crate) fn changed_page_ids(&self, prev: &BTree<K, V>) -> Vec<PageId> {
        use std::collections::HashSet;
        fn inner_ids<K, V>(t: &BTree<K, V>, out: &mut HashSet<PageId>) {
            fn go<K, V>(slot: &Child<K, V>, src: Option<&dyn NodeSource<K, V>>, out: &mut HashSet<PageId>) {
                if !slot.is_loaded() { return; } // an on-disk slot is a leaf (inner levels are resident)
                let n = slot.load(src);
                if n.children.is_empty() { return; }
                if let Some(id) = slot.page_id() { out.insert(id); }
                for c in n.children.iter() { go(c, src, out); }
            }
            go(&t.root, t.source.as_deref(), out);
        }
        let (mut new_inner, mut prev_inner) = (HashSet::new(), HashSet::new());
        inner_ids(self, &mut new_inner); inner_ids(prev, &mut prev_inner);
        // ids under changed inner nodes of each side
        fn collect<K, V>(t: &BTree<K, V>, other_inner: &HashSet<PageId>, out: &mut HashSet<PageId>) {
            fn go<K, V>(slot: &Child<K, V>, src: Option<&dyn NodeSource<K, V>>, other: &HashSet<PageId>, out: &mut HashSet<PageId>) {
                let id = slot.page_id();
                if let Some(i) = id && other.contains(&i) { return; } // identical subtree
                if let Some(i) = id { out.insert(i); }
                if !slot.is_loaded() { return; }
                let n = slot.load(src);
                for c in n.children.iter() { go(c, src, other, out); }
            }
            go(&t.root, t.source.as_deref(), other_inner, out);
        }
        let (mut new_ids, mut prev_ids) = (HashSet::new(), HashSet::new());
        collect(self, &prev_inner, &mut new_ids); collect(prev, &new_inner, &mut prev_ids);
        prev_ids.difference(&new_ids).copied().collect()
    }

    pub(crate) fn from_root_page(id: PageId, len: usize, source: Arc<dyn NodeSource<K, V>>) -> Self {
        BTree { root: Child::on_disk(id), len, source: Some(source) }
    }

    pub(crate) fn load_inner_levels(&self) {
        let src = self.source.as_deref();
        let h = self.height();
        fn go<K, V>(slot: &Child<K, V>, depth: usize, src: Option<&dyn NodeSource<K, V>>) {
            if depth == 0 { return; } // leaf slot: leave on disk
            let n = slot.load(src);
            for c in n.children.iter() { go(c, depth - 1, src); }
        }
        go(&self.root, h, src);
    }

    pub(crate) fn resident_leaf_estimate(&self) -> usize {
        let src = self.source.as_deref();
        let h = self.height();
        fn go<K, V>(slot: &Child<K, V>, depth: usize, src: Option<&dyn NodeSource<K, V>>) -> usize {
            if depth == 0 { return if slot.is_loaded() { Child::<K, V>::NODE_BYTES } else { 0 }; }
            let n = slot.load(src);
            n.children.iter().map(|c| go(c, depth - 1, src)).sum()
        }
        go(&self.root, h, src)
    }

    pub(crate) fn for_each_page_id(&self, f: &mut dyn FnMut(PageId)) {
        fn go<K, V>(slot: &Child<K, V>, src: Option<&dyn NodeSource<K, V>>, f: &mut dyn FnMut(PageId)) {
            if let Some(id) = slot.page_id() { f(id); }
            if !slot.is_loaded() { return; }
            for c in slot.load(src).children.iter() { go(c, src, f); }
        }
        go(&self.root, self.source.as_deref(), f)
    }
}
```

`demote_leaves` detail the note above points at: after the pass, walk the CoW'd path once more and, for each parent whose child slots are *all* `same_node` with the corresponding slots in `self` (nothing demoted under it), restore its page id with `set_page_id(old)`. Implement as a helper `restore_unchanged_ids(&self, out: &mut BTree)` walking both trees in lockstep at inner levels only; an `Ok` test: `demote_leaves` on a tree with no quiet leaves leaves `write_dirty` with zero writes.

`FixedVec` needs `iter_mut()` (add next to `iter`) and `remove(0)` already exists.

- [ ] **Step 4: Run tests**

Run: `cargo test btree::tests::paged -- --nocapture && cargo test`
Expected: all pass; the proptest runs 256 cases by default.

- [ ] **Step 5: Commit**

```bash
git add src/btree.rs src/child.rs
git commit -m "feat(btree): dirty walk, leaf demotion with second chance, changed-page diff, root-page attach"
```

---

## Phase B — page file, codec, root record (persistence feature)

### Task 4: `PageFile` — preallocated append-only page store

**Files:**
- Create: `src/pagefile.rs`
- Modify: `src/lib.rs` (`#[cfg(feature = "persistence")] mod pagefile;`), `src/wal.rs:632` (`preallocate_to` → `pub(crate)`)
- Test: unit tests in `src/pagefile.rs`

**Interfaces:**
- Consumes: `crate::wal::preallocate_to(file, from, to)`, `crate::wal::crc32(&[u8]) -> u32` (`src/wal.rs:581`, `pub(crate)`), `crate::child::PageId`.
- Produces:
  ```rust
  #[repr(u8)] #[derive(Clone, Copy, PartialEq, Eq, Debug)]
  pub(crate) enum PageKind { DataLeaf = 1, DataInner = 2, IndexLeaf = 3, IndexInner = 4 }
  pub(crate) const PAGE_HEADER_LEN: usize = 12;   // kind u8 | fmt u8 | flags u8 | pad u8 | payload_len u32 LE | crc32 u32 LE
  pub(crate) const PAGE_FMT_V1: u8 = 1;
  pub(crate) struct PageFile { file: File, w: Mutex<WriteHead { cursor: u64, capacity: u64 }>, chunk: u64, prefetch: usize }
  impl PageFile {
      pub(crate) fn open(path: &Path, cursor: u64, chunk: u64, prefetch: usize) -> Result<Self>;  // creates; fadvise RANDOM; capacity = file len
      pub(crate) fn append(&self, kind: PageKind, payload: &[u8]) -> Result<PageId>;              // grow-ahead, write_at, returns offset
      pub(crate) fn read(&self, id: PageId) -> Result<(PageKind, Vec<u8>)>;                        // prefetch read, second read if needed, CRC verified
      pub(crate) fn sync(&self) -> Result<()>;                                                     // sync_data
      pub(crate) fn file_end(&self) -> u64;                                                        // current cursor
      pub(crate) fn set_cursor(&self, at: u64);                                                    // recovery: file_end from root record
      pub(crate) fn punch(&self, ranges: &[(u64, u64)]) -> Result<()>;                            // fallocate PUNCH_HOLE|KEEP_SIZE per range
      pub(crate) fn page_len(payload_len: usize) -> u64 { (PAGE_HEADER_LEN + payload_len) as u64 }
  }
  pub(crate) fn page_file_path(dir: &Path) -> PathBuf { dir.join("pages.bin") }
  ```

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    fn tmp() -> (tempfile::TempDir, PageFile) {
        let d = tempfile::tempdir().unwrap();
        let pf = PageFile::open(&page_file_path(d.path()), 0, 1 << 20, 4096).unwrap();
        (d, pf)
    }
    #[test]
    fn append_read_roundtrip_all_kinds() {
        let (_d, pf) = tmp();
        for (i, kind) in [PageKind::DataLeaf, PageKind::DataInner, PageKind::IndexLeaf, PageKind::IndexInner].into_iter().enumerate() {
            let payload = vec![i as u8; 100 + i];
            let id = pf.append(kind, &payload).unwrap();
            assert_eq!(pf.read(id).unwrap(), (kind, payload));
        }
    }
    #[test]
    fn ids_are_byte_offsets_and_file_end_advances() {
        let (_d, pf) = tmp();
        let a = pf.append(PageKind::DataLeaf, &[1; 10]).unwrap();
        let b = pf.append(PageKind::DataLeaf, &[2; 10]).unwrap();
        assert_eq!(a, 0);
        assert_eq!(b, PageFile::page_len(10));
        assert_eq!(pf.file_end(), 2 * PageFile::page_len(10));
    }
    #[test]
    fn grow_ahead_zero_fills_in_chunks() {
        let d = tempfile::tempdir().unwrap();
        let pf = PageFile::open(&page_file_path(d.path()), 0, 8192, 4096).unwrap();
        pf.append(PageKind::DataLeaf, &[0; 100]).unwrap();
        assert_eq!(std::fs::metadata(page_file_path(d.path())).unwrap().len(), 8192, "physically zero-filled to one chunk");
        pf.append(PageKind::DataLeaf, &vec![0; 8000]).unwrap();
        assert_eq!(std::fs::metadata(page_file_path(d.path())).unwrap().len(), 16384);
    }
    #[test]
    fn prefetch_boundaries() {
        let (_d, pf) = tmp();
        for n in [4096 - PAGE_HEADER_LEN - 1, 4096 - PAGE_HEADER_LEN, 4096 - PAGE_HEADER_LEN + 1, 70_000] {
            let p: Vec<u8> = (0..n).map(|i| i as u8).collect();
            let id = pf.append(PageKind::DataLeaf, &p).unwrap();
            assert_eq!(pf.read(id).unwrap().1, p, "payload_len {n}");
        }
    }
    #[test]
    fn flipped_byte_fails_crc() {
        let (d, pf) = tmp();
        let id = pf.append(PageKind::DataLeaf, &[9; 50]).unwrap();
        pf.sync().unwrap();
        { use std::os::unix::fs::FileExt; let f = std::fs::OpenOptions::new().write(true).open(page_file_path(d.path())).unwrap(); f.write_at(&[8], id + PAGE_HEADER_LEN as u64 + 3).unwrap(); }
        let e = pf.read(id).unwrap_err();
        assert!(matches!(e, crate::Error::CheckpointCorrupted(_)), "{e:?}");
    }
    #[test]
    fn unknown_kind_or_fmt_rejected() {
        let (d, pf) = tmp();
        let id = pf.append(PageKind::DataLeaf, &[1; 8]).unwrap();
        { use std::os::unix::fs::FileExt; let f = std::fs::OpenOptions::new().write(true).open(page_file_path(d.path())).unwrap(); f.write_at(&[99], id).unwrap(); }
        assert!(pf.read(id).is_err());
    }
    #[test]
    fn punch_frees_blocks_but_keeps_offsets() {
        let (d, pf) = tmp();
        let big = vec![7u8; 1 << 20];
        let a = pf.append(PageKind::DataLeaf, &big).unwrap();
        let b = pf.append(PageKind::DataLeaf, &big).unwrap();
        pf.sync().unwrap();
        let blocks_before = { use std::os::unix::fs::MetadataExt; std::fs::metadata(page_file_path(d.path())).unwrap().blocks() };
        pf.punch(&[(a, PageFile::page_len(big.len()))]).unwrap();
        let blocks_after = { use std::os::unix::fs::MetadataExt; std::fs::metadata(page_file_path(d.path())).unwrap().blocks() };
        assert!(blocks_after < blocks_before, "hole punched ({blocks_before} -> {blocks_after})");
        assert_eq!(pf.read(b).unwrap().1, big, "neighbour intact at the same offset");
    }
    #[test]
    fn reopen_at_cursor_ignores_garbage_past_it() {
        let d = tempfile::tempdir().unwrap();
        let path = page_file_path(d.path());
        let end = { let pf = PageFile::open(&path, 0, 8192, 4096).unwrap(); pf.append(PageKind::DataLeaf, &[1; 10]).unwrap(); pf.sync().unwrap(); pf.file_end() };
        { use std::os::unix::fs::FileExt; let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap(); f.write_at(&[0xAB; 64], end).unwrap(); } // torn page
        let pf = PageFile::open(&path, end, 8192, 4096).unwrap();
        let id = pf.append(PageKind::DataLeaf, &[2; 10]).unwrap();
        assert_eq!(id, end, "next append overwrites the garbage");
        assert_eq!(pf.read(id).unwrap().1, vec![2; 10]);
    }
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test --features persistence pagefile::` → module missing.

- [ ] **Step 3: Implement `src/pagefile.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//! Append-only, physically preallocated page store (`pages.bin`). See spec §3.
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use parking_lot::Mutex;
use crate::child::PageId;
use crate::{Error, Result};

#[repr(u8)] #[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PageKind { DataLeaf = 1, DataInner = 2, IndexLeaf = 3, IndexInner = 4 }
impl TryFrom<u8> for PageKind { type Error = Error; fn try_from(b: u8) -> Result<Self> { Ok(match b { 1 => Self::DataLeaf, 2 => Self::DataInner, 3 => Self::IndexLeaf, 4 => Self::IndexInner, o => return Err(Error::CheckpointCorrupted(format!("unknown page kind {o}"))) }) } }

pub(crate) const PAGE_HEADER_LEN: usize = 12;
pub(crate) const PAGE_FMT_V1: u8 = 1;
/// Keys are capped at 64 KiB; a single node beyond this is a corruption signal, not a workload.
pub(crate) const MAX_PAGE_BYTES: usize = 64 << 20;

struct WriteHead { cursor: u64, capacity: u64 }
pub(crate) struct PageFile { file: File, w: Mutex<WriteHead>, chunk: u64, prefetch: usize }

pub(crate) fn page_file_path(dir: &Path) -> PathBuf { dir.join("pages.bin") }

impl PageFile {
    pub(crate) fn open(path: &Path, cursor: u64, chunk: u64, prefetch: usize) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path).map_err(|e| Error::Persistence(e.to_string()))?;
        let capacity = file.metadata().map_err(|e| Error::Persistence(e.to_string()))?.len();
        fadvise_random(&file);
        Ok(Self { file, w: Mutex::new(WriteHead { cursor, capacity }), chunk, prefetch: prefetch.max(PAGE_HEADER_LEN) })
    }
    pub(crate) fn page_len(payload_len: usize) -> u64 { (PAGE_HEADER_LEN + payload_len) as u64 }
    pub(crate) fn file_end(&self) -> u64 { self.w.lock().cursor }
    pub(crate) fn set_cursor(&self, at: u64) { self.w.lock().cursor = at; }

    pub(crate) fn append(&self, kind: PageKind, payload: &[u8]) -> Result<PageId> {
        let len = Self::page_len(payload.len());
        let mut w = self.w.lock();
        let id = w.cursor;
        let need = id + len;
        if need > w.capacity {
            let to = need.div_ceil(self.chunk) * self.chunk;
            let mut f = self.file.try_clone().map_err(|e| Error::Persistence(e.to_string()))?;
            crate::wal::preallocate_to(&mut f, w.capacity, to)?; // physical zero-fill + sync_all inside
            w.capacity = to;
        }
        let mut hdr = [0u8; PAGE_HEADER_LEN];
        hdr[0] = kind as u8; hdr[1] = PAGE_FMT_V1; hdr[2] = 0; hdr[3] = 0;
        hdr[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        let mut h = crc32fast::Hasher::new(); h.update(&hdr[..8]); h.update(payload);
        hdr[8..12].copy_from_slice(&h.finalize().to_le_bytes());
        self.file.write_all_at(&hdr, id).map_err(|e| Error::Persistence(e.to_string()))?;
        self.file.write_all_at(payload, id + PAGE_HEADER_LEN as u64).map_err(|e| Error::Persistence(e.to_string()))?;
        w.cursor = need;
        Ok(id)
    }

    pub(crate) fn read(&self, id: PageId) -> Result<(PageKind, Vec<u8>)> {
        let mut buf = vec![0u8; self.prefetch];
        let got = read_fully_at(&self.file, &mut buf, id)?;
        if got < PAGE_HEADER_LEN { return Err(Error::CheckpointCorrupted(format!("page {id}: short header"))); }
        let kind = PageKind::try_from(buf[0])?;
        if buf[1] != PAGE_FMT_V1 { return Err(Error::CheckpointCorrupted(format!("page {id}: unsupported page format {}", buf[1]))); }
        let plen = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let total = PAGE_HEADER_LEN + plen;
        // Bound BEFORE allocating: a bit-flipped payload_len can decode to ~4 GiB and
        // `Vec::resize` would abort the process instead of returning an error.
        if plen > MAX_PAGE_BYTES || id + total as u64 > self.w.lock().capacity {
            return Err(Error::CheckpointCorrupted(format!("page {id}: payload_len {plen} exceeds limits")));
        }
        if total > buf.len() { buf.resize(total, 0); let more = read_fully_at(&self.file, &mut buf[got..total], id + got as u64)?; if got + more < total { return Err(Error::CheckpointCorrupted(format!("page {id}: short payload"))); } }
        else if got < total { return Err(Error::CheckpointCorrupted(format!("page {id}: short payload"))); }
        let payload = buf[PAGE_HEADER_LEN..total].to_vec();
        let mut h = crc32fast::Hasher::new(); h.update(&buf[..8]); h.update(&payload);
        if h.finalize() != crc { return Err(Error::CheckpointCorrupted(format!("page {id}: crc mismatch"))); }
        Ok((kind, payload))
    }
    pub(crate) fn sync(&self) -> Result<()> { self.file.sync_data().map_err(|e| Error::Persistence(e.to_string())) }
    pub(crate) fn punch(&self, ranges: &[(u64, u64)]) -> Result<()> {
        use std::os::fd::AsRawFd;
        for &(off, len) in ranges {
            // SAFETY: plain syscall on our own fd.
            let r = unsafe { libc::fallocate(self.file.as_raw_fd(), libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE, off as i64, len as i64) };
            if r != 0 { return Err(Error::Persistence(format!("punch {off}+{len}: {}", std::io::Error::last_os_error()))); }
        }
        Ok(())
    }
}
fn read_fully_at(f: &File, buf: &mut [u8], mut off: u64) -> Result<usize> {
    let mut n = 0;
    while n < buf.len() { match f.read_at(&mut buf[n..], off) { Ok(0) => break, Ok(k) => { n += k; off += k as u64; } Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}, Err(e) => return Err(Error::Persistence(e.to_string())) } }
    Ok(n)
}
fn fadvise_random(f: &File) { use std::os::fd::AsRawFd; unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM); } }
```

Add `libc = { version = "0.2", optional = true }` and extend the feature: `persistence = ["dep:serde", "dep:bincode", "dep:libc"]`. `preallocate_to` in `wal.rs:632` must become `pub(crate)`; it ends with `file.sync_all()` (`wal.rs:669`), so the extension is durable before `append` writes into it — no extra sync needed.

- [ ] **Step 4: Run** — `cargo test --features persistence pagefile:: && cargo clippy --features persistence -- -D warnings` → 8 pass.

- [ ] **Step 5: Commit** — `git add src/pagefile.rs src/lib.rs src/wal.rs Cargo.toml Cargo.lock && git commit -m "feat(persistence): PageFile — preallocated append-only page store with CRC, prefetch reads, hole-punch reclaim"`

---

### Task 5: `NodeCodec` and `PagedSource`

**Files:**
- Create: `src/pagecodec.rs`
- Modify: `src/lib.rs`
- Test: unit tests in `src/pagecodec.rs`

**Interfaces:**
- Consumes: `PageFile`, `PageKind`, `BTreeNode` (pub(crate) fields), `PrimaryKey::{encode, decode}`, `Record` (`serde::Serialize + DeserializeOwned` under persistence), `crate::registry::check_encoded_key_len` (make `pub(crate)`).
- Produces:
  ```rust
  pub(crate) struct ValueCodec<V> { enc: fn(&V) -> Result<Vec<u8>>, dec: fn(&[u8]) -> Result<V> }
  pub(crate) struct NodeCodec<K, V> { value: ValueCodec<V>, index: bool /* selects Index* page kinds */, _k: PhantomData<K> }
  impl<K: PrimaryKey, V> NodeCodec<K, V> {
      pub(crate) fn records<R: Record>() -> NodeCodec<K, R>;                 // bincode values, data kinds
      pub(crate) fn unique_index<IK: PrimaryKey, RK: PrimaryKey>() -> NodeCodec<IK, RK>;   // value = row key via encode
      pub(crate) fn non_unique_index<IK: PrimaryKey, RK: PrimaryKey>() -> NodeCodec<(IK, RK), ()>;
      pub(crate) fn encode(&self, node: &BTreeNode<K, V>) -> Result<(PageKind, Vec<u8>)>;  // child ids from slots (all must have ids)
      pub(crate) fn decode(&self, kind: PageKind, payload: &[u8]) -> Result<BTreeNode<K, V>>;
  }
  pub(crate) struct PagedSource<K, V> { file: Arc<PageFile>, codec: NodeCodec<K, V>, name: String, stats: Arc<PagedStats> }
  pub(crate) struct PagedStats { pub page_faults: AtomicU64, pub dirty_bytes: AtomicU64, pub resident_leaf_bytes: AtomicI64, pub pages_written: AtomicU64, pub leaves_demoted: AtomicU64 }
  impl<K: PrimaryKey, V: Send + Sync + 'static> NodeSource<K, V> for PagedSource<K, V>;  // read_node = file.read + decode; counts page_faults; note_dirty adds to dirty_bytes
  ```
  Payload layout (both leaf and inner; inner appends child ids):
  `n u16 LE | (key_len u16 LE, key, val_len u32 LE, val)[n] | [child_id u64 LE](n+1 if inner)`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::BTreeNode;
    use crate::child::Child;
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Row { a: u64, s: String }

    fn leaf<K: Clone, V>(pairs: Vec<(K, V)>) -> BTreeNode<K, V> { BTreeNode { entries: pairs.into_iter().map(|(k, v)| (k, Arc::new(v))).collect(), children: Default::default() } }

    #[test]
    fn records_leaf_roundtrip() {
        let c = NodeCodec::<u64, Row>::records::<Row>();
        let n = leaf(vec![(1, Row { a: 1, s: "x".into() }), (2, Row { a: 2, s: "yy".into() })]);
        let (kind, bytes) = c.encode(&n).unwrap();
        assert_eq!(kind, PageKind::DataLeaf);
        let back = c.decode(kind, &bytes).unwrap();
        assert_eq!(back.entries.len(), 2);
        assert_eq!(*back.entries[1].1, Row { a: 2, s: "yy".into() });
    }
    #[test]
    fn inner_node_carries_values_and_child_ids() {
        let c = NodeCodec::<String, Row>::records::<Row>();
        let mut n = leaf(vec![("m".to_string(), Row { a: 9, s: "".into() })]);
        n.children.push(Child::on_disk(100)); n.children.push(Child::on_disk(200));
        let (kind, bytes) = c.encode(&n).unwrap();
        assert_eq!(kind, PageKind::DataInner);
        let back = c.decode(kind, &bytes).unwrap();
        assert_eq!(back.children.len(), 2);
        assert_eq!(back.children[0].page_id(), Some(100));
        assert_eq!(back.children[1].page_id(), Some(200));
        assert!(!back.children[0].is_loaded());
        assert_eq!(back.entries[0].1.a, 9);
    }
    #[test]
    fn encode_refuses_dirty_child() {
        let c = NodeCodec::<u64, Row>::records::<Row>();
        let mut n = leaf(vec![(1, Row { a: 1, s: "".into() })]);
        n.children.push(Child::resident(Arc::new(leaf(vec![])))); n.children.push(Child::on_disk(1));
        assert!(c.encode(&n).is_err(), "a child without a page id cannot be named");
    }
    #[test]
    fn index_kinds_and_every_key_type() {
        let u = NodeCodec::<String, u64>::unique_index::<String, u64>();
        let n = leaf(vec![("a".to_string(), 5u64)]);
        let (k, b) = u.encode(&n).unwrap(); assert_eq!(k, PageKind::IndexLeaf);
        assert_eq!(*u.decode(k, &b).unwrap().entries[0].1, 5);
        let nu = NodeCodec::<(i32, Vec<u8>), ()>::non_unique_index::<i32, Vec<u8>>();
        let n = leaf(vec![((-3, vec![1, 2]), ())]);
        let (k, b) = nu.encode(&n).unwrap();
        assert_eq!(nu.decode(k, &b).unwrap().entries[0].0, (-3, vec![1, 2]));
        // tuples, u128, i8 through the records codec with unit values
        let t = NodeCodec::<(u128, i8, String), ()>::records::<()>();
        let n = leaf(vec![((u128::MAX, -1, "z".into()), ())]);
        let (k, b) = t.encode(&n).unwrap(); assert_eq!(t.decode(k, &b).unwrap().entries[0].0.1, -1);
    }
    #[test]
    fn paged_source_reads_and_counts() {
        let d = tempfile::tempdir().unwrap();
        let pf = Arc::new(crate::pagefile::PageFile::open(&crate::pagefile::page_file_path(d.path()), 0, 1 << 16, 4096).unwrap());
        let codec = NodeCodec::<u64, Row>::records::<Row>();
        let n = leaf(vec![(1, Row { a: 1, s: "".into() })]);
        let (k, b) = codec.encode(&n).unwrap();
        let id = pf.append(k, &b).unwrap();
        let stats = Arc::new(PagedStats::default());
        let src = PagedSource { file: pf, codec, name: "t".into(), stats: stats.clone() };
        let back = crate::child::NodeSource::read_node(&src, id).unwrap();
        assert_eq!(back.entries.len(), 1);
        assert_eq!(stats.page_faults.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
```

- [ ] **Step 2: Run to verify failure** — module missing.

- [ ] **Step 3: Implement** — `encode`: for each entry `key.encode()` (enforce `check_encoded_key_len(len, "page payload")`), `(self.value.enc)(&*v)`; if `!node.children.is_empty()` append each `c.page_id().ok_or(Error::Persistence("dirty child"))?` LE. `decode`: parse, build `entries` with `Arc::new((self.value.dec)(bytes)?)`, `children` as `Child::on_disk(id)`. `records`: `enc = |v| bincode::serde::encode_to_vec(v, standard())`, `dec` likewise. `unique_index`: `enc = |k| Ok(k.encode())`, `dec = RK::decode`. `non_unique_index`: `enc = |_| Ok(vec![])`, `dec = |_| Ok(())`. `PagedSource::read_node`: `let (kind, bytes) = self.file.read(id)?; self.stats.page_faults.fetch_add(1); Ok(Arc::new(self.codec.decode(kind, &bytes)?))`; `note_dirty` → `stats.dirty_bytes += bytes`; `name` → `&self.name`. `PagedStats: Default`.

- [ ] **Step 4: Run** — `cargo test --features persistence pagecodec:: && cargo clippy --features persistence -- -D warnings`.

- [ ] **Step 5: Commit** — `git add src/pagecodec.rs src/lib.rs src/registry.rs && git commit -m "feat(persistence): NodeCodec for data/index pages and PagedSource with fault/dirty stats"`

---

### Task 6: Root record and `.root` discovery in `checkpoint.rs`

**Files:**
- Modify: `src/checkpoint.rs` (`CheckpointKind` `:50`, `find_latest_checkpoint` `:521`, `read_header` `:617`, `cleanup_old_checkpoints` `:1024`), spec §3 payload sketch
- Test: `src/checkpoint.rs` tests

**Interfaces:**
- Produces:
  ```rust
  enum CheckpointKind { Full = 0, Delta = 1, Paged = 2 }
  #[derive(Clone, Debug, PartialEq)] pub(crate) struct PagedIndexEntry { pub name: String, pub ik_type_id: u32, pub kind: u8 /*0 unique,1 nonunique*/, pub generation: u32, pub root_page: Option<u64>, pub len: u64 }
  #[derive(Clone, Debug, PartialEq)] pub(crate) struct PagedTableEntry { pub name: String, pub key_type_id: u32, pub root_page: Option<u64>, pub len: u64, pub next_id: Option<Vec<u8>>, pub indexes: Vec<PagedIndexEntry> }
  #[derive(Clone, Debug, PartialEq)] pub(crate) struct PagedRoot { pub version: u64, pub file_end: u64, pub tables: Vec<PagedTableEntry>, pub dead_pages: Vec<(u64, u64)> }
  pub(crate) fn root_path(dir: &Path, version: u64) -> PathBuf;            // checkpoint_{v}.root
  pub(crate) fn write_paged_root(dir: &Path, root: &PagedRoot) -> Result<()>;   // container framing + bincode body + trailing crc; tmp+rename+sync_dir
  pub(crate) fn read_paged_root(path: &Path) -> Result<PagedRoot>;
  pub(crate) enum LatestCheckpoint { Rows(PathBuf), Paged(PathBuf) }
  pub(crate) fn find_latest_checkpoint_any(dir: &Path) -> Result<Option<LatestCheckpoint>>;  // max version over .bin and .root
  pub(crate) fn list_paged_roots(dir: &Path) -> Result<Vec<(u64, PathBuf)>>;  // ascending
  ```

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn paged_root_roundtrip_and_discovery() {
    let d = tempfile::tempdir().unwrap();
    let root = PagedRoot { version: 42, file_end: 8192, tables: vec![PagedTableEntry { name: "t".into(), key_type_id: 3, root_page: Some(4096), len: 10, next_id: Some(vec![0, 0, 0, 0, 0, 0, 0, 11]), indexes: vec![PagedIndexEntry { name: "by_x".into(), ik_type_id: 11, kind: 1, generation: 0, root_page: None, len: 0 }] }], dead_pages: vec![(0, 4096)] };
    write_paged_root(d.path(), &root).unwrap();
    assert_eq!(read_paged_root(&root_path(d.path(), 42)).unwrap(), root);
    assert!(matches!(find_latest_checkpoint_any(d.path()).unwrap(), Some(LatestCheckpoint::Paged(_))));
    // An older rows checkpoint with a higher version wins discovery (version rules, not kind).
    let snap = Snapshot { version: 43, tables: Default::default() };
    write_checkpoint(d.path(), &snap, &TableRegistry::new()).unwrap();
    assert!(matches!(find_latest_checkpoint_any(d.path()).unwrap(), Some(LatestCheckpoint::Rows(_))));
}
#[test]
fn old_reader_rejects_paged_kind_cleanly() {
    let d = tempfile::tempdir().unwrap();
    write_paged_root(d.path(), &PagedRoot { version: 1, file_end: 0, tables: vec![], dead_pages: vec![] }).unwrap();
    // Simulate the pre-paged reader: `read_header` on a `.root` must not misparse; and a `.bin`
    // with kind byte 2 must be rejected by TryFrom in a build that lacks Paged (assert the error text here).
    let e = read_header(&root_path(d.path(), 1)).unwrap();
    assert_eq!(e.0, CheckpointKind::Paged);
}
#[test]
fn root_crc_detects_corruption() {
    let d = tempfile::tempdir().unwrap();
    write_paged_root(d.path(), &PagedRoot { version: 7, file_end: 0, tables: vec![], dead_pages: vec![] }).unwrap();
    let p = root_path(d.path(), 7);
    let mut b = std::fs::read(&p).unwrap(); let i = b.len() / 2; b[i] ^= 1; std::fs::write(&p, b).unwrap();
    assert!(matches!(read_paged_root(&p), Err(Error::CheckpointCorrupted(_))));
}
```

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement** — framing identical to `serialize_snapshot`'s prefix (`MAGIC`, `FORMAT_VERSION` varint, kind byte `2`, `version` varint), then the body via `bincode::encode_into_std_write` of a `#[derive(bincode::Encode, bincode::Decode)]` mirror struct, then the trailing CRC the other checkpoint files use (see `write_checkpoint_bytes` callers — the whole-file CRC lives in `serialize_snapshot`; replicate). `read_header` learns that a `.root` path has kind `Paged` and no base. `find_latest_checkpoint_any` scans both suffixes. `cleanup_old_checkpoints` gains a sibling `cleanup_old_roots(dir, keep: usize) -> Vec<PathBuf /*deleted*/>` (Task 11 uses the return). Amend spec §3's payload sketch: inner pages carry `(key, value)` pairs.

- [ ] **Step 4: Run** — `cargo test --features persistence checkpoint::tests::paged`.

- [ ] **Step 5: Commit** — `git add src/checkpoint.rs docs/superpowers/specs/2026-08-30-paged-btree-stage-1-2-design.md && git commit -m "feat(persistence): paged root record (CheckpointKind::Paged) with discovery and cleanup"`

---

## Phase C — table and store integration

### Task 7: Paged hooks on `MergeableTable` and `IndexMaintainer`; `Table` implementation

**Files:**
- Modify: `src/table.rs` (`MergeableTable` trait `:39`, `Table` struct `:295`, impl `:114`), `src/index.rs` (`IndexMaintainer` `:29`, `UniqueStorage`/`NonUniqueStorage` `:280,:334`, `ManagedIndex`)
- Test: `tests/paged_table.rs` (new; uses `PageFile` through `pub(crate)` — put the test as a unit test module in `table.rs` instead, since `PageFile` is crate-private)

**Interfaces:**
- Consumes: Tasks 3–6.
- Produces (all `#[cfg(feature = "persistence")]`):
  ```rust
  pub(crate) struct PagedCtx<'a> { pub file: &'a PageFile, pub stats: &'a PagedStats }
  pub(crate) enum Residency { Resident, Lazy }
  // MergeableTable additions
  fn paged_write(&self, ctx: &PagedCtx) -> Result<PagedTableEntry>;                      // write_dirty on data + each persisted index; returns entry
  fn paged_demote(&self, cursor: Option<&dyn Any>, budget: usize) -> (Box<dyn MergeableTable>, usize, Option<Box<dyn Any + Send>>);  // erased K cursor
  fn paged_changed_pages(&self, prev: &dyn MergeableTable) -> Vec<PageId>;
  fn paged_resident_leaf_bytes(&self) -> usize;
  fn residency(&self) -> Residency; fn set_residency(&mut self, r: Residency);
  // IndexMaintainer additions
  fn paged_write(&self, ctx: &PagedCtx) -> Result<Option<PagedIndexEntry>>;   // None if not persistable
  fn paged_changed_pages(&self, prev: &dyn IndexMaintainer<R, K>) -> Vec<PageId>;
  fn paged_generation(&self) -> u32;
  // Storage: UniqueStorage/NonUniqueStorage gain `codec: Option<NodeCodec<..>>` and `persist: Option<(u32 /*ik type*/, u32 /*generation*/)>`
  // Table gains: `residency: Residency`, `pending_indexes: Vec<PagedIndexEntry>` (filled by attach in Task 9)
  ```

- [ ] **Step 1: Write the failing tests** (unit module `paged` in `table.rs`)

```rust
#[test]
fn table_paged_write_then_demote_then_read_faults_one_leaf() {
    let d = tempfile::tempdir().unwrap();
    let file = Arc::new(PageFile::open(&page_file_path(d.path()), 0, 1 << 20, 4096).unwrap());
    let stats = Arc::new(PagedStats::default());
    let mut t: Table<u64, u64> = Table::new_keyed();
    for i in 1..=20_000u64 { t.put(i, i * 2).unwrap(); }
    t.attach_paged_source(file.clone(), stats.clone(), "rows");   // sets BTree source with records codec
    let entry = t.paged_write(&PagedCtx { file: &file, stats: &stats }).unwrap();
    assert_eq!(entry.len, 20_000); assert!(entry.root_page.is_some()); assert_eq!(entry.key_type_id, <u64 as PrimaryKey>::KEY_TYPE_ID);
    let written = stats.pages_written.load(Ordering::Relaxed);
    assert!(written > 300);
    let (t2, demoted, done) = t.paged_demote(None, usize::MAX);
    assert!(done.is_none() && demoted > 300);
    let t2 = t2.as_any().downcast_ref::<Table<u64, u64>>().unwrap();
    assert_eq!(t2.get(&777), Some(&1554));
    assert_eq!(stats.page_faults.load(Ordering::Relaxed), 1);
    // A second write after no changes writes nothing (demotion re-marked unchanged parents).
    let before = stats.pages_written.load(Ordering::Relaxed);
    t2.paged_write(&PagedCtx { file: &file, stats: &stats }).unwrap();
    assert_eq!(stats.pages_written.load(Ordering::Relaxed), before);
}
#[test]
fn persisted_index_is_written_and_unpersisted_index_is_not() {
    let d = tempfile::tempdir().unwrap();
    let file = Arc::new(PageFile::open(&page_file_path(d.path()), 0, 1 << 20, 4096).unwrap());
    let stats = Arc::new(PagedStats::default());
    let mut t: Table<u64, u64> = Table::new_keyed();
    for i in 1..=1_000u64 { t.put(i, i % 10).unwrap(); }
    t.define_persisted_index::<u64>("by_mod", IndexKind::NonUnique, IndexDef { generation: 3 }, |r| *r).unwrap();
    t.define_index::<u64>("plain", IndexKind::Unique, |r| *r + 1_000_000).unwrap();
    t.attach_paged_source(file.clone(), stats.clone(), "rows");
    let e = t.paged_write(&PagedCtx { file: &file, stats: &stats }).unwrap();
    assert_eq!(e.indexes.len(), 1);
    assert_eq!(e.indexes[0].name, "by_mod"); assert_eq!(e.indexes[0].generation, 3); assert_eq!(e.indexes[0].kind, 1); assert_eq!(e.indexes[0].len, 1_000);
}
#[test]
fn resident_table_never_demotes() {
    /* as first test, but `t.set_residency(Residency::Resident)` before demote → demoted == 0 */
}
```

`define_persisted_index` and `IndexDef` are introduced here with the minimal signature; Task 13 adds the attach path and error variants.

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement** — In `Table`: `attach_paged_source` builds `Arc<PagedSource<K, R>>` via `NodeCodec::records::<R>()` and `self.data.set_source(..)`, and for each index maintainer calls a new `IndexMaintainer::attach_paged_source(&mut self, file, stats, table_name)` (storages with a codec set their tree's source). `paged_write`: flush overlay on a clone (the `diff_table` precedent — `Table::clone` then `flush_overlay`), `data.write_dirty(|node, leaf| { let (kind, bytes) = codec.encode(node)?; let id = ctx.file.append(kind, &bytes)?; stats.pages_written += 1; id })` — the closure can't return `Result`; keep a `first_err: Option<Error>` captured and return it after the walk (`write_dirty` gets an early-out: if the closure returns `NO_PAGE`, stop descending and leave slots dirty). Index entries: `indexes.values().filter_map(|i| i.paged_write(ctx).transpose()).collect::<Result<_>>()?`. `paged_demote`: `data.demote_leaves(cursor.downcast_ref::<K>(), budget)` → new `Table` with the new tree, same indexes/next_id/overlay; `stats.leaves_demoted += n`. `paged_changed_pages`: `data.changed_page_ids(prev.data)` plus, per index name present in both, `idx.paged_changed_pages(prev_idx)`; indexes in `prev` absent in `self` contribute all their page ids (`for_each_page_id`). `IndexMaintainer::paged_write` for `ManagedIndex<_, _, UniqueStorage>`: if `storage.codec.is_none()` → `Ok(None)`, else `write_dirty` on `storage.tree` with the index codec and `PageKind::Index*` → `Some(PagedIndexEntry { kind: 0, ik_type_id, generation, root_page, len })`; same for `NonUniqueStorage` with `kind: 1`. `define_persisted_index<IK: PrimaryKey>` mirrors `define_index` but constructs storages with `codec: Some(NodeCodec::unique_index::<IK, K>())` / `non_unique_index`, `persist: Some((IK::KEY_TYPE_ID, def.generation))`.

- [ ] **Step 4: Run** — `cargo test --features persistence table::tests::paged && cargo test` (in-memory build must still compile: every new item is feature-gated).

- [ ] **Step 5: Commit** — `git commit -am "feat(table): paged write/demote/changed-pages hooks; define_persisted_index with codec-backed storages"`

---

### Task 8: `PagedOptions`, `Persistence::paged`, `PagedState`, and the paged checkpoint (phases 1–2, no demotion)

**Files:**
- Modify: `src/persistence.rs` (`Persistence` `:96`), `src/store.rs` (`StoreInner` `:425–470`, `Store::new` `~:600`, `checkpoint_impl` `:1030`), `src/registry.rs` (new `attach_paged` closure — used in Task 9, add the field now), `src/lib.rs` re-exports
- Test: `tests/paged_checkpoint.rs`

**Interfaces:**
- Produces:
  ```rust
  #[derive(Clone, Debug)] #[non_exhaustive]
  pub struct PagedOptions { pub memory_budget_bytes: Option<u64>, pub checkpoint_dirty_bytes: u64, pub checkpoint_interval: Option<Duration>, pub demote_batch: usize, pub page_prefetch_bytes: usize, pub prealloc_chunk_bytes: u64, pub retained_checkpoints: usize }
  impl PagedOptions { pub fn builder() -> PagedOptionsBuilder; }   // setters named after fields; defaults: None, 256 MiB, None, 1024, 4096, 16 MiB, 2
  impl Persistence { pub fn paged(self, opts: PagedOptions) -> Self; pub(crate) fn paged_opts(&self) -> Option<&PagedOptions>; }
  // Persistence::Standalone / Smr gain a field `paged: Option<PagedOptions>` (non_exhaustive, so additive)
  // StoreInner gains `paged: Option<PagedState>` where
  pub(crate) struct PagedState { pub file: Arc<PageFile>, pub stats: Arc<PagedStats>, pub opts: PagedOptions, pub last_root: Option<(Arc<Snapshot>, u64 /*version*/)>, pub last_checkpoint_at: Instant }
  ```
  `Store::new`: when `paged_opts()` is `Some`, open `PageFile` at `page_file_path(dir)` with cursor 0 (recovery sets it), store `PagedState`. Refuse `Persistence::None.paged(..)` at `Store::new` with `Error::Persistence("paged checkpoints require Standalone or Smr persistence")`.
  `checkpoint_impl` paged branch (replaces the `write_checkpoint`/`write_delta_checkpoint` call when `inner.paged.is_some()`):
  1. snapshot `snap` + registry as today (read lock, released);
  2. for each registered table in `snap`: `entry = table.paged_write(&PagedCtx{..})?` — tables without a source yet (first checkpoint after a legacy recover or a fresh table) get `attach_paged_source` first, done under `inner.write()` on a **clone** installed back into the snapshot map (the source is an `Arc` in the `BTree`; attaching to the live snapshot's table requires replacing the `Arc<dyn MergeableTable>` — do it as a re-publish of the same version, the mechanism Task 10 formalises; for this task implement `install_table_clone(inner, name, table)` that swaps the Arc in `snapshots[latest]` by building a new `Arc<Snapshot>` with the same version);
  3. `file.sync()`; `dead_pages = []` (Task 11 fills it); `write_paged_root(dir, &PagedRoot { version: snap.version, file_end: file.file_end(), tables, dead_pages })`;
  4. `inner.paged.last_root = Some((snap, version))`; existing WAL prune path unchanged (`prune up to version`); `cleanup_old_roots(dir, opts.retained_checkpoints)`.

- [ ] **Step 1: Write the failing test**

```rust
// tests/paged_checkpoint.rs
#![cfg(feature = "persistence")]
use ultima_db::{Durability, PagedOptions, Persistence, Store, StoreConfig, WalWrite};
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)] struct Row { v: u64 }

fn store(dir: &std::path::Path) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced).paged(PagedOptions::builder().build());
    let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s.register_table::<Row>("rows").unwrap();
    s
}

#[test]
fn paged_checkpoint_writes_root_and_pages_only_for_dirty_nodes() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path());
    { let mut w = s.begin_write(None).unwrap(); let mut t = w.open_table::<Row>("rows").unwrap(); for i in 0..20_000 { t.insert(Row { v: i }).unwrap(); } w.commit().unwrap(); }
    let v1 = s.checkpoint().unwrap();
    assert!(d.path().join(format!("checkpoint_{v1}.root")).exists());
    assert!(d.path().join("pages.bin").exists());
    let pages_after_first = s.paged_stats().unwrap().pages_written;
    assert!(pages_after_first > 300);
    // no-op checkpoint writes nothing
    s.checkpoint().unwrap();
    assert_eq!(s.paged_stats().unwrap().pages_written, pages_after_first);
    // one update → one path
    { let mut w = s.begin_write(None).unwrap(); let mut t = w.open_table::<Row>("rows").unwrap(); t.update(&5, Row { v: 99 }).unwrap(); w.commit().unwrap(); }
    s.checkpoint().unwrap();
    let delta = s.paged_stats().unwrap().pages_written - pages_after_first;
    assert!(delta >= 3 && delta <= 5, "leaf + inner path + root, got {delta}");
}

#[test]
fn paged_requires_disk_persistence() {
    let p = Persistence::None.paged(PagedOptions::builder().build());
    assert!(Store::new(StoreConfig::builder().persistence(p).build()).is_err());
}
```

`Store::paged_stats() -> Option<PagedStatsSnapshot>` (plain struct of `u64`s) is a public read-only accessor added here; it becomes the metrics source in Task 14.

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement** as specified in Interfaces. Keep `Persistence::paged` a consuming builder method on the enum (`match self { Standalone{..} => Standalone{ paged: Some(opts), .. }, Smr{..} => .., None => None /* rejected at Store::new */ }`).
- [ ] **Step 4: Run** — `cargo test --features persistence --test paged_checkpoint && cargo test --features persistence && cargo clippy --features persistence -- -D warnings`. Re-anchor cites shifted in `store.rs`/`persistence.rs`/`wal.rs`: run `formal/scripts/check-cites.py` (the CI cite guard, `.github/workflows/formal.yml:93`) and fix each reported cite by reading the new line.
- [ ] **Step 5: Commit** — `git commit -am "feat(store): PagedOptions/Persistence::paged and the paged checkpoint path (dirty walk + root record)"`

---

### Task 9: Recovery from a paged root; legacy upgrade; config mismatch refusal

**Files:**
- Modify: `src/store.rs` (`recover` `:1231`, `Store::new` guard), `src/registry.rs` (`attach_paged` closure body), `src/error.rs`, `src/table.rs` (`Table::from_paged_entry`)
- Test: `tests/paged_recovery.rs`, extend `tests/checkpoint_chain_equivalence.rs`

**Interfaces:**
- Produces:
  ```rust
  // registry.rs TableTypeInfo
  pub attach_paged: Box<dyn Fn(&PagedTableEntry, Arc<PageFile>, Arc<PagedStats>) -> Result<Box<dyn MergeableTable>> + Send + Sync>;
  // table.rs
  impl<R: Record, K: PrimaryKey> Table<R, K> { pub(crate) fn from_paged_entry(e: &PagedTableEntry, file: Arc<PageFile>, stats: Arc<PagedStats>) -> Result<Self>; }
  //  = key type guard (e.key_type_id == K::KEY_TYPE_ID else Error::Persistence(key_type_mismatch_msg)), BTree::from_root_page + load_inner_levels (or BTree::new when root_page is None), next_id = e.next_id.map(K::decode), pending_indexes = e.indexes.clone()
  // error.rs
  Error::PagedFormatRequired { dir: PathBuf }   // "newest checkpoint in {dir} is paged; configure Persistence::..paged(..)"
  ```
  `recover()` branch on `find_latest_checkpoint_any`: `Rows(p)` → existing chain load; `Paged(p)` → `read_paged_root`, per table `(info.attach_paged)(entry, file, stats)`, install snapshot `{version, tables}`, `file.set_cursor(root.file_end)`, `paged.last_root = Some(..)`; then WAL replay as today (it already clones tables via serialize/deserialize — **change that**: for paged tables replay must not round-trip through row serialization; use `boxed_clone()` (O(1)) instead of `serialize_table`/`deserialize_table` for every table — that block's comment says the re-wrap exists so `Arc::get_mut` succeeds, which `boxed_clone` into a fresh `Arc` also gives).
  `Store::new` guard: if `paged_opts().is_none()` and `find_latest_checkpoint_any(dir)` is `Some(Paged(_))` → `Err(PagedFormatRequired)`.

- [ ] **Step 1: Write the failing tests**

```rust
// tests/paged_recovery.rs
#[test]
fn recover_from_paged_root_loads_inner_levels_only_then_faults_on_read() {
    let d = tempfile::tempdir().unwrap();
    { let s = store(d.path()); write_rows(&s, 20_000); s.checkpoint().unwrap(); }
    let s = store(d.path());
    s.recover().unwrap();
    let st = s.paged_stats().unwrap();
    assert!(st.page_faults < 40, "inner levels + one probe leaf, got {}", st.page_faults);
    let r = s.begin_read(None).unwrap(); let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get(&1234).map(|r| r.v), Some(1233));
    assert_eq!(s.paged_stats().unwrap().page_faults, st.page_faults + 1);
}
#[test]
fn wal_replay_after_paged_root_faults_only_touched_leaves() {
    let d = tempfile::tempdir().unwrap();
    { let s = store(d.path()); write_rows(&s, 20_000); s.checkpoint().unwrap();
      let mut w = s.begin_write(None).unwrap(); let mut t = w.open_table::<Row>("rows").unwrap(); t.update(&3, Row { v: 1 }).unwrap(); t.update(&19_000, Row { v: 2 }).unwrap(); w.commit().unwrap(); }
    let s = store(d.path()); s.recover().unwrap();
    let f0 = s.paged_stats().unwrap().page_faults;
    assert!(f0 < 40 + 2);
    let r = s.begin_read(None).unwrap(); let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get(&3).unwrap().v, 1); assert_eq!(t.get(&19_000).unwrap().v, 2);
}
#[test]
fn legacy_directory_upgrades_on_first_paged_checkpoint() {
    let d = tempfile::tempdir().unwrap();
    { let p = Persistence::standalone(d.path(), Durability::Eventual, WalWrite::Coalesced);
      let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap(); s.register_table::<Row>("rows").unwrap(); write_rows(&s, 5_000); s.checkpoint().unwrap(); }
    let s = store(d.path()); s.recover().unwrap();
    assert_eq!(s.paged_stats().unwrap().page_faults, 0, "legacy load is fully resident");
    s.checkpoint().unwrap();
    assert!(s.paged_stats().unwrap().pages_written > 70, "first paged checkpoint writes every node");
    let s2 = store(d.path()); s2.recover().unwrap();
    assert_eq!(s2.begin_read(None).unwrap().open_table::<Row>("rows").unwrap().get(&4_000).unwrap().v, 3_999);
}
#[test]
fn row_config_refuses_paged_directory() {
    let d = tempfile::tempdir().unwrap();
    { let s = store(d.path()); write_rows(&s, 10); s.checkpoint().unwrap(); }
    let p = Persistence::standalone(d.path(), Durability::Eventual, WalWrite::Coalesced);
    let e = Store::new(StoreConfig::builder().persistence(p).build()).unwrap_err();
    assert!(matches!(e, ultima_db::Error::PagedFormatRequired { .. }), "{e:?}");
}
#[test]
fn garbage_past_file_end_is_overwritten() {
    let d = tempfile::tempdir().unwrap();
    let end = { let s = store(d.path()); write_rows(&s, 1_000); s.checkpoint().unwrap(); std::fs::metadata(d.path().join("pages.bin")).unwrap().len() };
    // append torn bytes at the end (inside the zero-filled region)
    { use std::os::unix::fs::FileExt; let f = std::fs::OpenOptions::new().write(true).open(d.path().join("pages.bin")).unwrap(); f.write_at(&[0xEE; 500], end - 1024).unwrap(); }
    let s = store(d.path()); s.recover().unwrap();
    write_one_update(&s); s.checkpoint().unwrap();
    let s2 = store(d.path()); s2.recover().unwrap();
    assert_eq!(s2.begin_read(None).unwrap().open_table::<Row>("rows").unwrap().len(), 1_000);
}
```

(`store`, `write_rows`, `write_one_update` are file-local helpers like Task 8's; `TableReader::len()` exists at `src/store.rs:3025`.) Note the `garbage` test's `end - 1024` must land past the last committed `file_end` — read `file_end` from the root via a `pub(crate)` test hook or compute from `pages_written`; simplest: expose `Store::paged_file_end()` under `#[doc(hidden)]`.

`tests/checkpoint_chain_equivalence.rs`: add a third store variant `Paged` to its matrix (same op script through rows-only, chained, and paged; the existing comparison closure applies unchanged). Read that file's `run_workload`/`store_for` helpers first; the change is a new arm, not new logic.

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement** per Interfaces.
- [ ] **Step 4: Run** — `cargo test --features persistence --test paged_recovery --test checkpoint_chain_equivalence --test persistence_integration --test format_compat`.
- [ ] **Step 5: Commit** — `git commit -am "feat(store): recover from paged roots (inner levels only), legacy upgrade path, PagedFormatRequired guard"`

---

### Task 10: Demotion install (phase 3) and the memory estimate

**Files:**
- Modify: `src/store.rs` (`checkpoint_impl` after the root write; new `fn demote_pass(&self) -> Result<usize>`; `Store::set_residency`)
- Test: `tests/paged_demotion.rs`

**Interfaces:**
- Produces:
  ```rust
  impl Store {
      /// Phase 3. Walks every Lazy table in batches of `opts.demote_batch` parents; each batch
      /// re-publishes `snapshots[latest]` with the demoted table under the SAME version number.
      /// Returns leaves demoted.
      pub(crate) fn demote_pass(&self) -> Result<usize>;
      pub fn set_residency(&self, table: &str, r: Residency) -> Result<()>;   // re-publishes latest with the flag set
  }
  ```
  Batch loop, per table: `cursor = None; loop { let mut inner = self.inner.write(); let latest = inner.latest_version; let snap = inner.snapshots[&latest].clone(); let Some(tbl) = snap.tables.get(name) else break; if tbl.residency() == Resident { break } let (new_tbl, n, next) = tbl.paged_demote(cursor.as_deref(), opts.demote_batch); let mut tables = snap.tables.clone(); tables.insert(name.clone(), Arc::from(new_tbl)); inner.snapshots.insert(latest, Arc::new(Snapshot { version: latest, tables })); drop(inner); demoted += n; match next { Some(c) => cursor = Some(c), None => break } }`. The `inner.write()` re-read of `latest_version` per batch is the "retry if a commit interleaved" rule: a commit between batches simply means the next batch starts from the newer latest. `checkpoint_impl` calls `demote_pass` after writing the root when `opts.memory_budget_bytes` is `Some` or when invoked by the resident-bytes trigger (Task 12); a plain explicit `checkpoint()` with no budget does not demote (spec §8: `None` = never demote on memory).

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn demotion_keeps_version_and_frees_after_gc() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().memory_budget_bytes(Some(1)).build());
    write_rows(&s, 20_000);
    let v = s.latest_version();
    s.checkpoint().unwrap();
    assert_eq!(s.latest_version(), v, "demotion re-publishes, never bumps");
    assert!(s.paged_stats().unwrap().leaves_demoted > 300);
    // fault count proves leaves are gone from the latest version
    let f0 = s.paged_stats().unwrap().page_faults;
    let r = s.begin_read(None).unwrap(); let t = r.open_table::<Row>("rows").unwrap();
    t.get(&1); t.get(&2); t.get(&15_000);
    assert_eq!(s.paged_stats().unwrap().page_faults, f0 + 2);
}
#[test]
fn accessed_leaf_survives_one_pass() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().memory_budget_bytes(Some(1)).build());
    write_rows(&s, 20_000); s.checkpoint().unwrap();
    { let r = s.begin_read(None).unwrap(); r.open_table::<Row>("rows").unwrap().get(&5); } // faults + marks
    let f = s.paged_stats().unwrap().page_faults;
    s.checkpoint().unwrap(); // second chance
    { let r = s.begin_read(None).unwrap(); r.open_table::<Row>("rows").unwrap().get(&5); }
    assert_eq!(s.paged_stats().unwrap().page_faults, f, "still resident");
    s.checkpoint().unwrap();
    { let r = s.begin_read(None).unwrap(); r.open_table::<Row>("rows").unwrap().get(&5); }
    assert_eq!(s.paged_stats().unwrap().page_faults, f + 1, "gone on the third pass");
}
#[test]
fn old_reader_keeps_its_leaves_across_demotion() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().memory_budget_bytes(Some(1)).build());
    write_rows(&s, 20_000); s.checkpoint().unwrap();
    let r = s.begin_read(None).unwrap();
    let f = s.paged_stats().unwrap().page_faults;
    s.checkpoint().unwrap(); // demotes latest again (nothing accessed) — r holds the old Arc
    let t = r.open_table::<Row>("rows").unwrap();
    for k in 1..=200 { t.get(&k); }
    // r's leaves were resident before demotion? No: after the FIRST checkpoint they were demoted.
    // So take the reader BEFORE any checkpoint instead:
    let _ = (f, t);
}
#[test]
fn commit_interleaved_with_demotion_loses_nothing() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().memory_budget_bytes(Some(1)).demote_batch(1).build());
    write_rows(&s, 20_000);
    let s2 = s.clone();
    let writer = std::thread::spawn(move || { for i in 0..200u64 { let mut w = s2.begin_write(None).unwrap(); let mut t = w.open_table::<Row>("rows").unwrap(); t.update(&(i * 97 + 1), Row { v: 1 }).unwrap(); w.commit().unwrap(); } });
    for _ in 0..3 { s.checkpoint().unwrap(); }
    writer.join().unwrap();
    let r = s.begin_read(None).unwrap(); let t = r.open_table::<Row>("rows").unwrap();
    for i in 0..200u64 { assert_eq!(t.get(&(i * 97 + 1)).unwrap().v, 1); }
    assert_eq!(t.len(), 20_000);
}
#[test]
fn resident_table_is_never_demoted() { /* set_residency("rows", Resident); checkpoint; leaves_demoted == 0 */ }
```

Fix the third test's setup as its comment says (take `r` before the first checkpoint, assert zero faults reading through it after two checkpoints, then drop it and `gc()`; `resident_leaf_bytes` estimate must not go negative).

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement** — `demote_pass` as above; maintain `stats.resident_leaf_bytes` (`+= NODE_BYTES` in `PagedSource::read_node` for leaf kinds, `-= NODE_BYTES × demoted` in `paged_demote`; clamp at 0 on read).
- [ ] **Step 4: Run** — `cargo test --features persistence --test paged_demotion`.
- [ ] **Step 5: Commit** — `git commit -am "feat(store): demotion install as a same-version re-publish, batched under the store lock"`

---

### Task 11: Dead-list and hole-punch reclaim (phase 4) with crash points

**Files:**
- Modify: `src/store.rs` (`checkpoint_impl`: compute `dead_pages`, punch after `cleanup_old_roots`), `src/mutation.rs` (two new `Mutation` variants), `src/checkpoint.rs` (`cleanup_old_roots` returns deleted versions)
- Test: `tests/paged_reclaim.rs`, `tests/paged_fault_crash.rs` (feature `mutation-testing`, pattern from `tests/wal_fault_torn_tail.rs`)

**Interfaces:**
- `checkpoint_impl`: `dead = match &paged.last_root { Some((prev, _)) => tables_changed_pages(&snap, prev) /* per table paged_changed_pages; tables gone from snap contribute all ids via for_each_page_id */, None => vec![] }` → `PagedRoot.dead_pages` as `(offset, len)` — length needs the page's `payload_len`: read the 12-byte header at each dead id (`PageFile::read_len(id)`, add it). After `write_paged_root`: `for v in cleanup_old_roots(dir, keep) { if let Ok(r) = read_paged_root(&root_path(dir, v_next(v))) { file.punch(&r.dead_pages) } }` — precisely: when root *v−1* is deleted, punch root *v*'s dead list (they were dead relative to *v−1*). Keep an in-memory map `punch_after: BTreeMap<u64 /*root version*/, Vec<(u64,u64)>>` so no re-read is needed in the common path; the on-disk copy exists for the crash case.
- Mutations: `Mutation::CrashAfterPageSync` (return `Err` after `file.sync()` and before `write_paged_root`), `Mutation::CrashBeforePunch` (return `Err` after root rename and before punch).

- [ ] **Step 1: Write the failing tests**

```rust
// tests/paged_reclaim.rs
#[test]
fn dead_list_equals_replaced_path_and_is_punched_after_retention() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().retained_checkpoints(1).build());
    write_rows(&s, 20_000); let v1 = s.checkpoint().unwrap();
    write_one_update(&s); let v2 = s.checkpoint().unwrap();
    let root2 = read_root(d.path(), v2);   // test helper via a #[doc(hidden)] pub fn Store::debug_read_root
    assert!(root2.dead_pages.len() >= 3 && root2.dead_pages.len() <= 5, "{:?}", root2.dead_pages);
    assert!(!d.path().join(format!("checkpoint_{v1}.root")).exists(), "retained_checkpoints = 1");
    assert_eq!(s.paged_stats().unwrap().dead_pages_punched, root2.dead_pages.len() as u64);
}
#[test]
fn dropped_table_pages_are_dead() { /* register two tables, checkpoint, drop one via the existing DDL, checkpoint; dead_pages count == that table's page count */ }
// tests/paged_fault_crash.rs  (#![cfg(all(feature = "persistence", feature = "mutation-testing"))])
#[test]
fn crash_between_page_sync_and_root_rename_recovers_previous_root() {
    let d = tempfile::tempdir().unwrap();
    { let s = store(d.path()); write_rows(&s, 5_000); s.checkpoint().unwrap();
      write_one_update(&s);
      ultima_db::mutation::set(Mutation::CrashAfterPageSync);
      assert!(s.checkpoint().is_err()); ultima_db::mutation::clear(); }
    let s = store(d.path()); s.recover().unwrap();   // root v1 + WAL replay of the update
    assert_eq!(s.begin_read(None).unwrap().open_table::<Row>("rows").unwrap().get(&updated_key()).unwrap().v, updated_value());
    s.checkpoint().unwrap(); // the orphaned pages past file_end are overwritten, not referenced
}
#[test]
fn crash_before_punch_leaks_space_not_data() { /* CrashBeforePunch; recover; every row readable; a following checkpoint punches the pending list (stats.dead_pages_punched grows) */ }
```

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — `cargo test --features persistence --test paged_reclaim && cargo test --features persistence,mutation-testing --test paged_fault_crash`. Re-anchor cites.
- [ ] **Step 5: Commit** — `git commit -am "feat(store): dead-page lists in root records, hole-punch after retention, crash points"`

---

### Task 12: The checkpointer thread

**Files:**
- Modify: `src/store.rs` (`PagedState.checkpointer: Option<Checkpointer>`; start in `Store::new`, stop in `Drop for StoreInner`/`WalHandle`'s pattern), `src/child.rs`/`src/pagecodec.rs` (dirty-bytes already flow through `note_dirty`)
- Test: `tests/paged_checkpointer.rs`

**Interfaces:**
```rust
struct Checkpointer { stop: Arc<AtomicBool>, wake: Arc<(parking_lot::Mutex<bool>, parking_lot::Condvar)>, handle: Option<JoinHandle<()>> }
// loop: wait on condvar with timeout = opts.checkpoint_interval.unwrap_or(1s); on wake or timeout:
//   let due_dirty = stats.dirty_bytes >= opts.checkpoint_dirty_bytes;
//   let due_mem   = opts.memory_budget_bytes.is_some_and(|b| stats.resident_leaf_bytes >= b);
//   let due_time  = opts.checkpoint_interval.is_some_and(|i| last_checkpoint_at.elapsed() >= i) && stats.dirty_bytes > 0;
//   if due_dirty || due_mem || due_time { let _ = store.checkpoint_impl(false); /* errors latch wal_poison-style: log once, keep going */ }
// commit path: after install, `if paged { stats.dirty_bytes >= threshold → notify_one() }`; `PagedSource::read_node` for leaves: resident_leaf_bytes >= budget → notify_one().
```
`Store` holds a `Weak<RwLock<StoreInner>>` in the thread so a dropped store stops it; `Drop` sets `stop` and joins. `checkpoint_impl` resets `stats.dirty_bytes = 0` after phase 2 (dirty nodes written) — do it by subtracting the bytes written, not zeroing, so nodes dirtied during the write are not lost.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn dirty_bytes_trigger_checkpoints_without_app_call() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(d.path(), PagedOptions::builder().checkpoint_dirty_bytes(1 << 20).build());
    write_rows(&s, 50_000);   // ~1.5 KB × 800 leaves ≫ 1 MiB
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while s.paged_stats().unwrap().checkpointer_runs == 0 && std::time::Instant::now() < deadline { std::thread::sleep(std::time::Duration::from_millis(20)); }
    assert!(s.paged_stats().unwrap().checkpointer_runs >= 1);
    assert!(std::fs::read_dir(d.path()).unwrap().any(|e| e.unwrap().file_name().to_string_lossy().ends_with(".root")));
}
#[test]
fn memory_budget_trigger_demotes_read_only_store() {
    let d = tempfile::tempdir().unwrap();
    { let s = store(d.path()); write_rows(&s, 50_000); s.checkpoint().unwrap(); }
    let s = store_with(d.path(), PagedOptions::builder().memory_budget_bytes(Some(64 * 1024)).build());
    s.recover().unwrap();
    { let r = s.begin_read(None).unwrap(); let t = r.open_table::<Row>("rows").unwrap(); for k in 1..=50_000u64 { t.get(&k); } } // reads only; faults everything in
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while s.paged_stats().unwrap().leaves_demoted == 0 && std::time::Instant::now() < deadline { std::thread::sleep(std::time::Duration::from_millis(20)); }
    assert!(s.paged_stats().unwrap().leaves_demoted > 0, "a read-only store must still demote on the memory trigger");
}
#[test]
fn drop_joins_the_thread() { let d = tempfile::tempdir().unwrap(); let s = store(d.path()); drop(s); /* must return promptly: wrap in a 5 s watchdog thread */ }
```

- [ ] **Step 2: Run to verify failure.**  **Step 3: Implement.**  **Step 4: Run** all persistence tests + `--test send_bounds`.  **Step 5: Commit** — `git commit -am "feat(store): background checkpointer with dirty-bytes, memory-budget and interval triggers"`

---

## Phase D — indexes, config surface, docs, acceptance

### Task 13: Index attach after recovery (`define_persisted_index` semantics)

**Files:**
- Modify: `src/table.rs` (`define_persisted_index` attach branch using `pending_indexes`), `src/index.rs` (`UniqueStorage::from_root_page`, `NonUniqueStorage::from_root_page`, eager `load_all`), `src/error.rs` (`IndexDefinitionMismatch { table, index, reason }`), `src/store.rs` (recover logs unpersisted indexes)
- Test: `tests/paged_index.rs`

**Behaviour table (spec §6):** name absent → rebuild by scan; `ik_type_id`+`kind`+`generation` match → attach (load index tree resident via `from_root_page` + full walk, bind extractor, **zero data-page reads**); `ik_type_id` or `kind` differ → `Err(IndexDefinitionMismatch)`; `generation` differs → rebuild by scan, storage keeps the new generation. Plain `define_index` on a name with pending persisted contents → rebuild by scan, pending entry dropped (its pages become dead next checkpoint, which `paged_changed_pages` already yields because the prev snapshot's index has ids the new one lacks).

- [ ] **Step 1: Tests**

```rust
#[test]
fn attach_reads_no_data_pages() {
    let d = tempfile::tempdir().unwrap();
    { let s = store(d.path()); { let mut w = s.begin_write(None).unwrap(); let mut t = w.open_table::<Row>("rows").unwrap(); t.define_persisted_index::<u64>("by_v", IndexKind::NonUnique, IndexDef { generation: 1 }, |r: &Row| r.v % 100).unwrap(); w.commit().unwrap(); } write_rows(&s, 20_000); s.checkpoint().unwrap(); }
    let s = store(d.path()); s.recover().unwrap();
    let f0 = s.paged_stats().unwrap().page_faults;
    { let mut w = s.begin_write(None).unwrap(); let mut t = w.open_table::<Row>("rows").unwrap(); t.define_persisted_index::<u64>("by_v", IndexKind::NonUnique, IndexDef { generation: 1 }, |r: &Row| r.v % 100).unwrap(); w.commit().unwrap(); }
    let f1 = s.paged_stats().unwrap().page_faults;
    assert!(f1 - f0 < 40, "index pages only (~320 index leaves are small: count them via stats.index_page_faults if you split the counter), no data leaves");
    let r = s.begin_read(None).unwrap(); let t = r.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get_by_index::<u64>("by_v", &7).unwrap().len(), 200);
}
#[test]
fn kind_or_type_mismatch_errors_and_generation_rebuilds() { /* define as Unique after persisting NonUnique → IndexDefinitionMismatch; define with generation 2 → Ok, faults every data leaf (page_faults jumps by ~318), stats show rebuild */ }
#[test]
fn plain_define_index_over_persisted_contents_rebuilds_and_orphans_pages() { /* define_index (non-persisted) same name; next checkpoint's dead_pages ≥ index page count */ }
#[test]
fn custom_index_on_paged_store_rebuilds_by_scan_and_logs() { /* tests/custom_index_api.rs has a CustomIndex fixture; recover; define it; page_faults ≈ every leaf */ }
```

Split `PagedStats.page_faults` into `data_page_faults` and `index_page_faults` (the codec knows the kind) so the first assertion is exact: data faults == 0.

- [ ] **Steps 2–5:** fail → implement → `cargo test --features persistence --test paged_index --test custom_index_api --test fulltext_integration` → `git commit -am "feat(index): attach persisted index contents on define_persisted_index with type/kind/generation guards"`

---

### Task 14: Config completeness, metrics, docs

**Files:**
- Modify: `src/store.rs` (`Store::set_residency` public docs; `PagedStatsSnapshot` fields: `page_faults`, `data_page_faults`, `index_page_faults`, `pages_written`, `leaves_demoted`, `dirty_bytes`, `resident_leaf_bytes_est`, `checkpointer_runs`, `dead_pages_punched`), `src/metrics.rs` (gauges/counters under the `metrics` feature mirroring those names), `src/lib.rs` (re-export `PagedOptions`, `PagedOptionsBuilder`, `Residency`, `IndexDef`; docs.rs landing page mention), `CLAUDE.md` (Persistence bullet: paged mode, `checkpoint_chain_max` inert, checkpointer thread), `docs/tasks/task63_paged_btree.md` (new: consolidates spec decisions + what shipped + measured numbers placeholder "unmeasured until Task 16"), `README.md` (one bullet)
- Test: `tests/paged_config.rs` — `checkpoint_chain_max` set with paged → `Store::new` ok and every checkpoint is a root (no `.bin`); `Persistence::standalone_fast(dir).paged(..)` works; MultiWriter + paged: 4 writers on disjoint keys + checkpoints → all rows present; `bulk_load` on a paged store then checkpoint then recover → rows present.

- [ ] **Steps:** tests → implement → `cargo test --features persistence,metrics` → `cargo doc --features persistence --no-deps 2>&1 | grep -c warning` (diff against baseline count, see memory note: ~10 pre-existing) → `git commit -am "docs+config: paged mode surface, metrics, task63 doc, CLAUDE.md"`

---

### Task 15: Regression gates on the in-memory path and formal cites

- [ ] `make perf/check` — autobench Gate-A against committed baselines. A failure here is **reported**, not silenced: record the delta in `docs/tasks/task63_paged_btree.md` ("slot size regression, local, ±2×") and re-record baselines only on the bench host (memory note: baselines are per-machine).
- [ ] `cargo bench --bench btree_get_bench` and `--bench btree_insert_mut_bench` before/after Task 2 commit (`git stash`-free: check out `21f9757` into a worktree, run there, compare shapes only).
- [ ] `formal/scripts/check-cites.py` (the CI cite guard) — must pass; re-anchor by reading the cited lines, never by uniform offset.
- [ ] `make consistency/elle` once (needs java; skip with a note if unavailable).
- [ ] Full: `cargo test && cargo test --features persistence && cargo test -p ultima-vector && cargo clippy --all-features -- -D warnings && cargo bench --no-run -p compare-benches`.
- [ ] Commit any baseline/cite re-anchoring: `git commit -am "chore: re-anchor formal cites and record perf-gate result after paged-btree"`

---

### Task 16: Acceptance — bigger than RAM, built by writes (`make paging/check`)

**Files:**
- Modify: `compare_benches/src/bin/paging_matrix.rs` (new engine `ultima-paged`: `Persistence::standalone(dir, Eventual, Coalesced).paged(PagedOptions::builder().memory_budget_bytes(Some(budget)).build())`, `--load=insert` default for it, `--restart` flag: after the run, drop the store, recover, run again and report both), `compare_benches/scripts/paging_matrix_run.sh` (engine), `Makefile` (`paging/check` target)
- Test: the target itself.

`make paging/check` runs, inside `scripts/paging_matrix.sh LIMIT=<footprint/4>`: `--engine=ultima-paged --rows=5000000 --load=insert --workload=C --dist=uniform` and `--workload=A --dist=zipf` and asserts from the JSON: `cg_events_oom == 0`; `majflt_per_op` (now counting *page-file reads* via `data_page_faults / ops` — the harness reads `paged_stats` for this engine and reports `pf_per_op`) `≤ 1.5` for C and `≤ 3.0` for A; second-run `recover_secs` ≤ 5× the first checkpoint's inner-level count × 0.1 ms. Thresholds are shape gates for the sandbox; the NVMe rig run (follow-on 7 in the spec) sets the published numbers.

- [ ] Implement engine + target; run `make paging/check`; paste the two summary lines into `docs/tasks/task63_paged_btree.md` marked **local, shape only**.
- [ ] Commit — `git commit -am "bench(paging): ultima-paged engine and make paging/check acceptance gate"`

---

## Self-review (done while writing)

**Spec coverage:** §3 page format → Tasks 4–6 (payload corrected to carry inner values). §4 slot → Tasks 1–2. §5 checkpointer phases 1–4 → Tasks 8, 10, 11, 12; batched install + same-version re-publish → Task 10; `checkpoint_chain_max` inert → Task 14 test; `bulk_load` limitation → Task 14 test documents it. §6 indexes → Tasks 7, 13 (custom/full-text rebuild-by-scan → Task 13 test). §7 recovery/compat → Task 9 (legacy upgrade, refusal, `file_end`, WAL replay faults only touched leaves); old-binary kind rejection → Task 6 test. §8 config → Tasks 8, 14; residency override → Tasks 7, 10. §9 tests → each phase's tests; property twin → Task 3; crash points → Task 11; acceptance → Task 16; perf/cites → Task 15. Follow-ons untouched.

**Type consistency:** `Child::{resident, on_disk, page_id, is_loaded, set_page_id, mark_accessed, take_accessed, load, load_arc, make_mut, same_node, strong_count}` used identically in Tasks 1–3, 5, 7. `PagedStats` fields: `page_faults` (Task 5) split into `data_page_faults`/`index_page_faults` in Task 13 — Task 14 lists both plus the original total. `PagedTableEntry`/`PagedIndexEntry`/`PagedRoot` (Task 6) consumed unchanged by Tasks 7–11. `paged_demote` returns `(Box<dyn MergeableTable>, usize, Option<Box<dyn Any + Send>>)` in Task 7 and is consumed with that shape in Task 10.

**Resolved lookups (verified 2026-08-30):** crc helper is `crate::wal::crc32` (`wal.rs:581`); `TableReader::len` exists (`store.rs:3025`); the cite guard is `formal/scripts/check-cites.py`; `preallocate_to` ends in `sync_all` (`wal.rs:669`); `libc` is already transitive in `Cargo.lock` (so is `rustix`) — the plan uses `libc`.
