# Paged Leaf Value Blocks + Memory-Honest Budget — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Paged data leaves own their values in one `Box<[V]>` block (killing heap-fragmentation F6), the resident/pinned accounting becomes exact (including retained-snapshot pins, the NVMe 56× lever), and the memory budget becomes a hard cap (clock eviction + adaptive retention shrink).

**Architecture:** `Value<V>` newtype (niche over `Option<Arc<V>>`; `None` = in-block at the entry's own position) + `block: Option<Box<[V]>>` on `BTreeNode`. Cloning a shared block leaf is impossible through plain `Clone` (no `V: Clone` on the tree), so `NodeSource` gains `clone_value` (a fn pointer captured at `register_table_paged::<R: Record + Clone>`) and `Child::make_mut_after_load` stops using `Arc::make_mut` in favor of an explicit clone-via-source. Accounting reconciles against a pointer-dedup walk over ALL retained snapshots; the demote pass cycles until under budget; the checkpointer can direct gc below configured retention (floor: latest + explicit `VersionPin`s).

**Tech Stack:** Rust stable; no new dependencies; no new `unsafe` beyond the already-audited `child.rs` block being edited (Miri SB+TB re-run mandatory).

**Spec:** `docs/superpowers/specs/2026-08-31-paged-leaf-arena-memory-honesty-design.md` — the binding authority; conflicts in this plan resolve against it. One deliberate refinement vs the spec's wording: `clone_fn` lives on `NodeSource` (threaded through every mutation already), not on `BTree` — same erased-fn mechanism, better plumbing.

## Global Constraints

- Never run `cargo fmt` (repo-wide rustfmt drift; match surrounding style by hand).
- `cargo clippy --all-targets --features persistence -- -D warnings` must pass at every commit.
- Full gates before the final task completes: `cargo test --features persistence`, Miri on `btree`+`child` unit tests under BOTH aliasing models (`cargo +nightly miri test -p ultima-db --lib btree:: child::` and the same with `MIRIFLAGS=-Zmiri-tree-borrows`; judge by test lists + exit codes, never by the deterministic "finished in" timings), `make formal/cite-check` (any `src/store.rs` line shift breaks it — re-anchor by READING, never by offset; see `formal/tla/wal/README.md`).
- Wire format MUST NOT change: `pages.bin` bytes and root records are identical before/after (Task 4 pins this with fixtures + golden bytes).
- Invariant I-A: a node's entries are all-Arc or all-in-block, never mixed. Invariant I-B: block leaves exist only in trees whose source has `clone_value` (i.e. paged data trees).
- All-new public API is additive; `PagedStatsSnapshot` becomes `#[non_exhaustive]` (spec §7).
- Bench work: local numbers are sanity-only; the NVMe validation run (final task's step) happens ONLY with Peter's per-run approval + verified teardown (CLAUDE.md fleet policy).

## File Map

- `src/btree.rs` — `Value<V>`, `block` field, block-leaf read/mutation paths, `resident_leaf_estimate` upgrade, dedup walk helper.
- `src/child.rs` — `NodeSource::clone_value`, `make_mut_after_load` rewrite, `Child::same_node` reuse.
- `src/pagecodec.rs` — decode-to-block, encode accessor, `PagedStats` new fields, real-bytes fault-in credit.
- `src/table.rs` — attach plumbs clone fn; boundary conversions (`delete`, `upsert_arc`, overlay flush, `BulkBuilder`); `paged_resident_leaf_bytes` real bytes.
- `src/store.rs` — `register_table_paged`, pin-aware reconciliation, hard-cap demote loop, adaptive gc floor, snapshot stats.
- `src/registry.rs` — registry entry carries the erased clone capability.
- `src/error.rs` — `Error::PagedNeedsClone`.
- `src/persistence.rs` — `PagedOptions::shrink_retention_under_pressure` (default true) + builder setter.
- Tests: `tests/paged_block_leaves.rs` (new: equivalence oracle, invariants, boundaries), `tests/paged_accounting.rs` (new: counter==walk proptest, pins, hard cap, retention shrink), `tests/paged_format_compat.rs` (new: fixture + golden bytes), existing `tests/paged_demotion.rs`, `tests/mw_race_matrix` re-runs.

---

### Task 1: `Value<V>` newtype + `block` field — mechanical sweep, behavior-preserving

**Files:**
- Modify: `src/btree.rs` (type at :384, `BTreeNode` at :397, every `entries[..].1` access)
- Modify: `src/pagecodec.rs`, `src/table.rs` (entry constructions/accesses the compiler flags)
- Test: `src/btree.rs` unit tests (size asserts)

**Interfaces:**
- Produces: `pub(crate) struct Value<V>(Option<Arc<V>>)` with `fn arc(a: Arc<V>) -> Self`, `fn in_block() -> Self`, `fn as_arc(&self) -> Option<&Arc<V>>`, `fn is_in_block(&self) -> bool`; `BTreeNode { entries, children, block: Option<Box<[V]>> }`; `impl BTreeNode { fn value_at(&self, i: usize) -> &V }` (Arc deref or `&block[i]`).
- Consumes: nothing new. After this task every node is still all-Arc (`block: None` everywhere); zero behavior change.

- [ ] **Step 1: Introduce the types** in `src/btree.rs` next to `Entries` (:384):

```rust
/// A leaf entry's value slot: `Arc` = shared heap value (inner nodes,
/// non-paged trees — byte-identical to the old `Arc<V>` via the NonNull
/// niche); `in_block` = the value lives at this entry's own position in
/// the node's `block`. Spec §3 (I-A/I-B).
pub(crate) struct Value<V>(Option<Arc<V>>);

impl<V> Value<V> {
    pub(crate) fn arc(a: Arc<V>) -> Self { Value(Some(a)) }
    pub(crate) fn in_block() -> Self { Value(None) }
    pub(crate) fn as_arc(&self) -> Option<&Arc<V>> { self.0.as_ref() }
    pub(crate) fn is_in_block(&self) -> bool { self.0.is_none() }
}
impl<V> Clone for Value<V> {
    fn clone(&self) -> Self { Value(self.0.clone()) }
}
```

Change `Entries` to `FixedVec<(K, Value<V>), { MAX_KEYS + 1 }>` and add `block: Option<Box<[V]>>` to `BTreeNode`. `BTreeNode`'s manual `Clone` (`K: Clone` only, ~:404) clones `entries`/`children` as today and sets `block` via `debug_assert!(self.block.is_none(), "plain Clone must never see a block leaf (I-B)"); block: None` — Task 2 introduces the only legal shared-block clone path.

- [ ] **Step 2: Add `value_at`** on `BTreeNode`:

```rust
impl<K, V> BTreeNode<K, V> {
    /// The value of entry `i`, from either representation. Panics on an
    /// in-block entry with no block — impossible under I-A.
    pub(crate) fn value_at(&self, i: usize) -> &V {
        match self.entries[i].1.as_arc() {
            Some(a) => a,
            None => &self.block.as_ref().expect("I-A: in-block entry requires a block")[i],
        }
    }
}
```

- [ ] **Step 3: Mechanical sweep, compiler-driven.** `cargo check --features persistence` and fix every error: entry constructions become `(key, Value::arc(v))`; reads through `.1` become `value_at(i)` where a `&V` is wanted, or `.1.as_arc()` where the `Arc` itself is needed (e.g. `get_arc_in_node` at :1642 → `entry.1.as_arc().cloned()` for Arc-entries; leave a `// Task 5 wires the block branch` marker returning the arc path only — behavior unchanged because no block exists yet). All construction sites (`BTreeNode { entries, children }`) gain `block: None`. `pagecodec.rs` encode (:181 `let (k, v) = &node.entries[i]`) uses `node.value_at(i)`; decode (:263) pushes `(key, Value::arc(Arc::new(val)))`. Do NOT change semantics anywhere — this task is a representation rename.

- [ ] **Step 4: Size regression tests** (btree unit tests):

```rust
#[test]
fn value_slot_is_pointer_sized_and_entry_layout_unchanged() {
    use std::mem::size_of;
    assert_eq!(size_of::<Value<u64>>(), size_of::<Arc<u64>>(), "niche lost");
    assert_eq!(size_of::<(u64, Value<u64>)>(), size_of::<(u64, Arc<u64>)>());
}
```

- [ ] **Step 5: Full suite green** — `cargo test --features persistence` (this is the whole verification: representation-only change) and `cargo clippy --all-targets --features persistence -- -D warnings`.
- [ ] **Step 6: Commit** — `git commit -m "refactor(btree): Value<V> slot + block field (all-Arc, behavior-preserving)"`.

### Task 2: `NodeSource::clone_value` + the `make_mut` rewrite (the landmine)

**Files:**
- Modify: `src/child.rs` (`NodeSource` trait :35, `make_mut_after_load` :332)
- Test: `src/child.rs` unit tests + Miri both models

**Interfaces:**
- Produces: `NodeSource::clone_value(&self, v: &V) -> Option<V>` (default `None`); `BTreeNode::clone_with(&self, src) -> BTreeNode<K, V>` (clones a block via `clone_value`; plain clone otherwise).
- Consumes: Task 1's types.

**Why:** `make_mut_after_load` uses `Arc::make_mut` (:337), which needs `BTreeNode: Clone` — and plain `Clone` cannot clone a block (no `V: Clone`, and Task 1 made it assert). Every in-place mutation path (`insert_mut`, `remove_mut`, `rotate_right`/`rotate_left` :2208/:2237, the merges :2268/:2284) reaches leaves through `make_mut`, so shared block leaves MUST clone through the source.

- [ ] **Step 1: Trait method** on `NodeSource` (child.rs:35):

```rust
    /// Clone one value for a block-leaf CoW. `None` (the default) means
    /// this source cannot clone values — trees on such sources must never
    /// hold block leaves (I-B). `PagedSource` returns `Some` via the fn
    /// pointer captured at `register_table_paged` (Task 3).
    fn clone_value(&self, _v: &V) -> Option<V> {
        None
    }
```

- [ ] **Step 2: `clone_with`** on `BTreeNode` (in btree.rs, but landing here because make_mut needs it):

```rust
    /// The one legal clone of a block leaf: values duplicated through the
    /// source's `clone_value`. Non-block nodes take the plain-Clone path.
    pub(crate) fn clone_with(&self, src: Option<&dyn NodeSource<K, V>>) -> BTreeNode<K, V>
    where K: Clone {
        match &self.block {
            None => self.clone(),
            Some(b) => {
                let src = src.expect("I-B: block leaf on a sourceless tree");
                let block: Box<[V]> = b.iter()
                    .map(|v| src.clone_value(v).expect("I-B: block leaf on a non-cloning source"))
                    .collect();
                BTreeNode { entries: self.entries.clone(), children: self.children.clone(), block: Some(block) }
            }
        }
    }
```

- [ ] **Step 3: Rewrite `make_mut_after_load`** (child.rs:332) to clone explicitly instead of `Arc::make_mut`:

```rust
    fn make_mut_after_load(&mut self, src: Option<&dyn NodeSource<K, V>>) -> &mut BTreeNode<K, V> {
        let was_clean = self.page_id().is_some();
        let p = *self.node.get_mut();
        // SAFETY: caller already ensured residency (via `load`/`load_quiet`), so p is non-null.
        let mut arc = unsafe { Arc::from_raw(p) };
        // `Arc::make_mut` would clone through plain `Clone`, which cannot
        // duplicate a value block (no `V: Clone` bound on the tree) — go
        // through `clone_with`, which routes block values via the source.
        if Arc::get_mut(&mut arc).is_none() {
            arc = Arc::new(arc.clone_with(src));
        }
        let raw = Arc::into_raw(arc) as *mut BTreeNode<K, V>;
        *self.node.get_mut() = raw;
        // ... keep the existing tail (was_clean dirty-marking) byte-for-byte ...
```

Keep everything after the pointer swap exactly as it is today (the `was_clean` → `note_dirty` tail). The unique-owner fast path (`get_mut` succeeds) mutates in place exactly as `Arc::make_mut` did.

- [ ] **Step 4: Unit tests** (child.rs test module, MockDisk already exists there):

```rust
#[test]
fn make_mut_unique_block_leaf_is_in_place() { /* build block leaf via test helper, one Arc, make_mut, assert same ptr */ }
#[test]
fn make_mut_shared_block_leaf_clones_via_source() { /* hold a second Arc, make_mut, assert new ptr and MockDisk clone_value called n times */ }
```

Give `MockDisk` a `clone_value` impl counting calls (`cloned: AtomicU64`) and returning `Some(*v)` for `u64` values. Write the tests to FAIL first by asserting the clone-count before implementing Step 1-3 wiring in the mock (red), then green.

- [ ] **Step 5: Miri, both models, this module** — `cargo +nightly miri test -p ultima-db --lib child::` and with `MIRIFLAGS=-Zmiri-tree-borrows`. The rewritten function touches the raw-pointer choreography; Miri is the arbiter (judge by test list + exit code).
- [ ] **Step 6: Suite + clippy green; commit** — `git commit -m "feat(child): clone_value on NodeSource; make_mut clones block leaves via source"`.

### Task 3: `register_table_paged` + registry clone capability + `Error::PagedNeedsClone`

**Files:**
- Modify: `src/registry.rs` (entry gains the erased clone capability), `src/store.rs` (:1219 `register_table` area — new sibling fn), `src/error.rs`, `src/table.rs` (`attach_paged_source` :1607 takes/forwards the clone fn), `src/pagecodec.rs` (`PagedSource` stores `clone: Option<fn(&V) -> V>`; `NodeSource::clone_value` impl returns it)
- Test: `tests/paged_config.rs`

**Interfaces:**
- Produces: `Store::register_table_paged::<R: Record + Clone>(name) -> Result<()>` and `register_table_paged_keyed::<R, K>`; `Error::PagedNeedsClone { table: String }`; `PagedSource { clone: Option<fn(&V) -> V>, .. }`.
- Consumes: Task 2's `clone_value` hook.

- [ ] **Step 1: Failing tests** in `tests/paged_config.rs`:

```rust
#[test]
fn plain_register_on_paged_store_errors_needs_clone() {
    let dir = tempfile::tempdir().unwrap();
    let s = paged_store(dir.path()); // existing helper in this file
    let e = s.register_table::<Row>("rows").unwrap_err();
    assert!(matches!(e, ultima_db::Error::PagedNeedsClone { .. }), "{e:?}");
}
#[test]
fn register_table_paged_works_on_paged_and_plain_stores() {
    let dir = tempfile::tempdir().unwrap();
    paged_store(dir.path()).register_table_paged::<Row>("rows").unwrap();
    let plain = ultima_db::Store::default();
    // additive: harmless on a non-paged store too
    // (compiles+succeeds; behaves as plain registration)
}
```

Run: `cargo test --features persistence --test paged_config needs_clone` → FAIL (method/variant missing).

- [ ] **Step 2: Implement.** Registry entry gains `paged_clone: bool` plus the monomorphized attach closure capturing `Some(<R as Clone>::clone as fn(&R) -> R)` (registered by `register_table_paged`) or `None` (plain `register_table`). `Store::register_table` on a store whose persistence is paged returns `Err(Error::PagedNeedsClone { table })` immediately (the store knows `inner.paged.is_some()` at registration time). `attach_paged_source(file, stats, name, clone: Option<fn(&R) -> R>)` stores it on the built `PagedSource`; `impl NodeSource for PagedSource` overrides `clone_value` to `self.clone.map(|f| f(v))`. `register_table_paged` delegates to a `register_table_impl` shared with the keyed variants (mirror the `_keyed` pattern at store.rs:1219-1221).
- [ ] **Step 3: Tests green; suite; clippy. Commit** — `git commit -m "feat(store): register_table_paged (R: Clone) + PagedNeedsClone; clone fn rides PagedSource"`.

**Migration note for the executor:** every existing paged test/bench registers via `register_table::<R>` — they now err. Sweep `tests/paged_*.rs`, `tests/checkpoint_chain_equivalence.rs`, `compare_benches/src/bin/paging_matrix.rs` to `register_table_paged` in THIS task (their `Row` types already derive Clone or gain it here). This is the spec's "free break" — paged mode is unreleased.

### Task 4: Decode-to-block, encode accessor, format-compat fixture, real-bytes fault-in credit

**Files:**
- Modify: `src/pagecodec.rs` (decode :228-280, encode :169-196, `read_node` credit :449-459)
- Create: `tests/paged_format_compat.rs` + fixture dir `tests/fixtures/paged_prechange/` (committed)
- Test: `tests/paged_format_compat.rs`

**Interfaces:**
- Produces: `DataLeaf` decode through a `PagedSource` yields a block leaf (`entries: (K, Value::in_block())`, `block: Some`); fault-in credit becomes `NODE_BYTES + n * size_of::<V>()`; `BTreeNode::leaf_bytes(&self) -> usize` (that same formula — Tasks 8/9 consume it).
- Consumes: Task 1's types.

- [ ] **Step 1: Fixture FIRST, from pre-change code.** Before touching decode, generate the fixture with the CURRENT build: a small program (or `#[ignore]`d test run once) creates a paged store at `tests/fixtures/paged_prechange/`, writes 200 rows across several leaves via `register_table_paged` (Task 3 exists now) + commits + `checkpoint()`, closes cleanly. Commit the directory (pages.bin + root + wal). This is the task62 discipline: the fixture's bytes were written by code that predates block decoding.
- [ ] **Step 2: Failing test** in `tests/paged_format_compat.rs`:

```rust
#[test]
fn prechange_directory_recovers_and_reads() {
    let dir = copy_fixture_to_temp("paged_prechange"); // helper: recursive copy
    let s = open_paged(dir.path());
    s.register_table_paged::<Row>("rows").unwrap();
    s.recover().unwrap();
    let r = s.begin_read(None).unwrap();
    let t = r.open_table::<Row>("rows").unwrap();
    for k in 1..=200u64 { assert_eq!(t.get(k).unwrap().v, k, "row {k}"); }
}
#[test]
fn encode_bytes_identical_across_representations() {
    // Build the same logical leaf twice: all-Arc (test constructor) and
    // block-backed (via decode of the first's encoding); encode both and
    // assert byte equality — the wire format must not know which
    // representation produced it.
}
```

- [ ] **Step 3: Implement decode-to-block.** In `decode` (pagecodec.rs), for `kind == PageKind::DataLeaf` (index codecs and inner nodes unchanged): collect values into `Vec<V>`, push `(key, Value::in_block())`, finish with `block: Some(values.into_boxed_slice())`. `DataInner` keeps `Value::arc(Arc::new(val))` (inner nodes carry values, always resident, never blocks). Encode already reads via `value_at` (Task 1) — verify no path assumes `as_arc().is_some()`.
- [ ] **Step 4: Real-bytes credit.** In `read_node` (:449-459) replace the flat `Child::<K, V>::NODE_BYTES` credit with `NODE_BYTES + n * size_of::<V>()` via the new `BTreeNode::leaf_bytes`; the budget-wake comparison uses the same value. (Demote's debit is Task 8 — until then the counter is transiently asymmetric; the F1 reconciliation at checkpoint end corrects it every checkpoint, and Task 8's tests pin exact symmetry.)
- [ ] **Step 5: Tests green (fixture + golden bytes + existing suite).** Also re-run `tests/paged_recovery.rs` and `tests/checkpoint_chain_equivalence.rs` explicitly.
- [ ] **Step 6: Commit** — `git commit -m "feat(pagecodec): DataLeaf decodes to value blocks; real-bytes fault-in credit; format-compat fixture"`.

### Task 5: Block-leaf mutations, immutable path (+ the equivalence oracle)

**Files:**
- Modify: `src/btree.rs` (`insert_into_node` :1654, `maybe_split` :1712, `delete_from_node` :2074, `merge_with_left`/`merge_with_right` :2268/:2284)
- Create: `tests/paged_block_leaves.rs`
- Test: same

**Interfaces:**
- Produces: `fn rebuild_block_leaf<K: Clone, V>(node: &BTreeNode<K,V>, edit: LeafEdit<K,V>, src: Option<&dyn NodeSource<K,V>>) -> BTreeNode<K,V>` where `enum LeafEdit<K,V> { Replace(usize, V), Insert(usize, K, V), Remove(usize) }` — the ONE-PASS builder every immutable block mutation goes through (spec §4: never clone-then-mutate); `fn split_block(node) -> (left, median_kv, right)` building one fresh block per half.
- Consumes: Tasks 1-4.

- [ ] **Step 1: The oracle test harness** (write it first — it drives every remaining btree task):

```rust
// tests/paged_block_leaves.rs — proptest oracle: identical op sequences
// against a paged (block-leaf) table and a plain in-memory table must
// agree after every batch and across checkpoint->recover.
proptest! {
  #![proptest_config(ProptestConfig { cases: if cfg!(miri) { 4 } else { 64 }, ..Default::default() })]
  #[test]
  fn paged_blocks_equal_in_memory_oracle(ops in prop::collection::vec(op_strategy(), 1..120)) {
      let dir = tempfile::tempdir().unwrap();
      let paged = paged_store(dir.path()); paged.register_table_paged::<Row>("t").unwrap();
      let plain = ultima_db::Store::default();
      apply_ops(&paged, &ops); apply_ops(&plain, &ops);
      assert_tables_equal(&paged, &plain);
      paged.checkpoint().unwrap();
      // force cold reads through blocks:
      let reopened = reopen_and_recover(dir.path());
      assert_tables_equal(&reopened, &plain);
  }
}
// op_strategy(): Put(k in 0..500u64, v), Update-if-present, Delete-if-present,
// InsertBatch(1..40 rows) — batches exercise BulkBuilder+splits.
```

Run: FAILS today at the first mutation on a recovered (block-leaf) tree — `insert_into_node` clones block-leaf entries and builds `block: None` nodes with `Value::in_block()` slots dangling → the Step-3 `value_at` assert trips. That failure IS the red step.

- [ ] **Step 2: `rebuild_block_leaf` + `split_block`.** In the immutable insert path (`insert_into_node`), branch on `node.block.is_some()` at the leaf arms:
  - Replace (`Ok(pos)`): `rebuild_block_leaf(node, LeafEdit::Replace(pos, take_value(val, src)), src)`.
  - Insert (`Err(pos)`, leaf): `LeafEdit::Insert(pos, key, take_value(val, src))`; if the result exceeds `MAX_KEYS`, `split_block` splits entries AND block into two fresh boxed slices (median value moves into the parent as `Value::arc(Arc::new(v))` — separators live in inner nodes, which are Arc-backed).
  - `take_value(val: Arc<V>, src) -> V`: `Arc::try_unwrap(val).unwrap_or_else(|a| src.unwrap().clone_value(&a).expect("I-B"))` — zero-copy for the common freshly-allocated insert.
  The builder clones untouched values via `src.clone_value` while streaming the edit in — one allocation, one pass.
- [ ] **Step 3: Delete path.** `delete_from_node` leaf arm: `LeafEdit::Remove(pos)`. The merges (`merge_with_left`/`merge_with_right`) concatenate two leaves: for block leaves build ONE fresh block from both sides' values (clone via `clone_value`; the descending separator value converts Arc→owned via `take_value`). Underflow rotations at leaf depth move one value across: rebuild BOTH siblings' blocks (rotations are the immutable path's counterparts — check whether `delete_from_node` uses the shared `rotate_*`; if the immutable path has its own copies, patch those).
- [ ] **Step 4: Invariant walk test** (same file): after a mixed workload + checkpoint + partial reads, walk every reachable node and assert I-A (all entries agree with `block.is_some()`) and blocks-only-at-leaf-depth-of-data-trees.
- [ ] **Step 5: Oracle green (non-Miri), suite, clippy. Commit** — `git commit -m "feat(btree): block-leaf mutations, immutable path (one-pass rebuilds + splits/merges)"`.

### Task 6: Block-leaf mutations, in-place path (`_mut` family + rebalance)

**Files:**
- Modify: `src/btree.rs` (`insert_into_node_mut` :1997-2049, `maybe_split_mut` :2049, `delete_from_node_mut` :2323, `rotate_right` :2208, `rotate_left` :2237, in-place merges)
- Test: `tests/paged_block_leaves.rs` (oracle already covers; add targeted rotation/merge cases)

**Interfaces:**
- Consumes: Task 2's `make_mut` (shared block leaves clone correctly via source) and Task 5's `rebuild_block_leaf`/`split_block`.
- Produces: nothing new — the `_mut` family becomes block-correct.

- [ ] **Step 1: Targeted failing tests.** Deterministic sequences forcing (a) an in-place update on a uniquely-owned block leaf (must mutate `block[i]` directly — no rebuild: assert via `paged_stats` no extra allocation-visible behavior, and value correctness), (b) leaf split through `maybe_split_mut`, (c) a delete triggering `rotate_right` between two block leaves, (d) a delete triggering merge. Drive via a small-fanout-friendly key pattern (sequential keys at T=32: 64+ inserts → split; then deletes clustered on one leaf → underflow).
- [ ] **Step 2: Implement.**
  - Update-in-place (unique owner, key exists): after `make_mut`, `if let Some(b) = &mut n.block { b[pos] = take_value(val, src); }` — the O(1) hot path.
  - Insert/split via `maybe_split_mut`: when the node has a block, rebuild through Task 5's builders (an insert changes block length — `Box<[V]>` cannot grow in place).
  - `rotate_right`/`rotate_left`: after the two `make_mut` calls, if leaves are block-backed, move the migrating value across by rebuilding both blocks (the stolen entry's value comes out of one block and the separator's value goes into the other; separators in `entries` of the parent stay Arc-backed — convert with `take_value`/`Value::arc(Arc::new(..))` at the boundary).
  - In-place merges mirror Task 5's merge: one fresh combined block.
- [ ] **Step 3: Oracle re-run with an in-place-biased op mix** (the oracle's `apply_ops` uses `Table` which drives the `_mut` paths through `upsert_arc`/overlay — confirm by asserting `insert_mut` is on the call path with a temporary `debug_assert`/trace during development, then remove).
- [ ] **Step 4: Miri on the btree tests** (`cargo +nightly miri test -p ultima-db --lib btree::` both models — the `_mut` family plays with `split_at_mut` + raw Child internals).
- [ ] **Step 5: Suite, clippy. Commit** — `git commit -m "feat(btree): block-leaf mutations, in-place path (make_mut fast path, rotations, merges)"`.

### Task 7: Boundaries — delete clone-out, upsert/overlay clone-in, bulk move, MultiWriter matrix

**Files:**
- Modify: `src/table.rs` (`delete` :1022, `upsert_arc` :963, overlay flush, `BulkBuilder`/`extend_from_sorted` (task51 fast path), `from_sorted`)
- Test: `tests/paged_block_leaves.rs` + re-run `tests/` MultiWriter race matrix (task59 suite)

**Interfaces:**
- Consumes: Tasks 5-6.
- Produces: `Table::delete` on a block leaf returns `Arc::new(cloned_value)`; bulk paths build blocks by MOVE (zero clones).

- [ ] **Step 1: Failing tests:** (a) `delete` on a paged (recovered, block-backed) table returns the correct value and the table equals the oracle after; (b) a bulk `insert_batch` on a fresh paged table followed by checkpoint+recover reads back correctly AND `clone_value` was never called during the build (count via a test `PagedSource` wrapper — or simpler: a `Row` type whose `Clone` impl bumps a static counter, asserted zero across the bulk path); (c) MultiWriter: two writers, same table, disjoint keys, on a paged store — both commit (drives `upsert_arc` merge onto block leaves).
- [ ] **Step 2: Implement.** `BTree::remove` (:708) currently loses the removed value? — check: `Table::delete` fetches `get_arc` first then removes (verify at :1022). For a block leaf, `get_arc`-equivalent materializes `Arc::new(src.clone_value(v))` — route through a `BTree::get_owned(key) -> Option<Arc<V>>` helper that clones out of blocks and Arc-clones otherwise. Bulk builders (`from_sorted`, `BulkBuilder`) own their `V`s: build blocks directly by move for paged targets (`bulk_load` builds in memory THEN attaches — confirm ordering; if the tree is built pre-attach as all-Arc and only later paged-attached, that is CORRECT and cheap (I-B: no source yet, no blocks) — blocks then appear progressively at fault-in after the first checkpoint's demotion. State this explicitly in the code comment: bulk-built trees are all-Arc until their first demote/fault cycle; the zero-clone claim holds trivially).
- [ ] **Step 3: Run the task59 race matrix** (`cargo test --features persistence --test` the mw race-matrix test file(s) — locate via `ls tests/ | grep -i race`; run whole files). All green.
- [ ] **Step 4: Suite, clippy. Commit** — `git commit -m "feat(table): block-leaf boundaries (delete clone-out, merge clone-in, bulk move path)"`.

### Task 8: Demote debit symmetry + `resident_leaf_estimate` real bytes

**Files:**
- Modify: `src/btree.rs` (`demote_leaves` :921 debit computation, `resident_leaf_estimate` :1218), `src/table.rs` (`paged_node_bytes` / `paged_demote` byte reporting — demote's caller at store.rs:2110-2128 multiplies `demoted * node_bytes`; replace with exact bytes returned by the pass), `src/store.rs` (:2128 debit)
- Test: `tests/paged_accounting.rs` (new)

**Interfaces:**
- Produces: `demote_leaves` returns `(tree, demoted_count, demoted_bytes, cursor)` (bytes = Σ `leaf_bytes()` of demoted leaves); `resident_leaf_estimate` sums `leaf_bytes()` over loaded leaves; `MergeableTable::paged_demote` forwards the bytes; store debits exactly `demoted_bytes`.
- Consumes: Task 4's `leaf_bytes`.

- [ ] **Step 1: Failing test** (`tests/paged_accounting.rs`): build a paged store, load rows, checkpoint (demote-all), fault N distinct leaves back in via reads, then assert `paged_stats().resident_leaf_bytes_est == sum-of-walk` where the walk is an exact reference recomputation via a new test-only `Store` helper OR (better, no new API) assert credit/debit symmetry: `est after (checkpoint→fault k leaves→checkpoint)` returns to the same value across 3 cycles (drift == 0, which the old NODE_BYTES flat debit fails once blocks exist because credit ≠ debit).
- [ ] **Step 2: Implement** the exact-bytes plumbing end to end; update the F1 reconciliation's walk (`paged_resident_leaf_bytes` → `resident_leaf_estimate`) to the same `leaf_bytes` formula — credit, debit, and reconcile all speak one unit.
- [ ] **Step 3: Existing `tests/paged_demotion.rs` green** (its assertions are count-based; where any assumed NODE_BYTES flat math, update to the new unit).
- [ ] **Step 4: Suite, clippy. Commit** — `git commit -m "feat(paged): exact-bytes demote debit + resident estimate (one unit everywhere)"`.

### Task 9: Pin-aware reconciliation + `pinned_leaf_bytes` + snapshot stats surface

**Files:**
- Modify: `src/store.rs` (the F1 reconciliation block inside `checkpoint_impl_paged` — find via `grep -n "F1 (spike/paged-write-path" src/store.rs`; `PagedStatsSnapshot` :640), `src/btree.rs` (dedup walk helper), `src/pagecodec.rs` (`PagedStats` field)
- Test: `tests/paged_accounting.rs`

**Interfaces:**
- Produces: `BTree::resident_leaf_bytes_dedup(&self, seen: &mut HashSet<*const ()>) -> usize` (skips leaves whose node ptr is already in `seen` — the `Child::same_node`/`BTree::diff` ptr-identity trick; ptr obtained WITHOUT touching accessed bits via `load_quiet`-style peeks of already-resident slots only — an unloaded slot contributes 0 and is never faulted); `MergeableTable::paged_resident_leaf_bytes_dedup(&self, seen: &mut dyn Any) -> usize` (erased mirror, same downcast pattern as `merge_keys_from`); `PagedStatsSnapshot { pinned_leaf_bytes: u64, .. }` + `#[non_exhaustive]` on the snapshot; `PagedStats::pinned_leaf_bytes: AtomicU64`.
- Consumes: Task 8's `leaf_bytes` unit.

- [ ] **Step 1: Failing test:** paged store with `num_snapshots_retained(4)`; insert-load in several commits; checkpoint (demotes latest); assert `resident_leaf_bytes_est` ≈ small BUT `pinned_leaf_bytes` > 0 and ≈ (bytes of leaves reachable only from the 3 older snapshots). Then `gc()` after dropping retention (set 1 via a fresh store? — retention is config-fixed; instead drop the older snapshots by committing 4 more times so they age out) → next checkpoint reconcile → `pinned_leaf_bytes == 0`.
- [ ] **Step 2: Implement.** In the reconciliation block: walk ALL retained snapshots' registered tables newest-first with one shared `seen` set; `resident = walk(latest)`; `pinned = (total over all snapshots) - resident`. Store both (`resident_leaf_bytes.store(resident)`, `pinned_leaf_bytes.store(pinned)`). Snapshot iteration: `inner.snapshots.values()` under the same write lock the block already holds. Add the field to `PagedStatsSnapshot::from_stats`, mark the struct `#[non_exhaustive]`, and extend the proptest from Task 8's file: after arbitrary op/checkpoint/gc sequences, `resident + pinned == full dedup walk over all snapshots` (reference implementation recomputed in the test).
- [ ] **Step 3: Suite (note: `#[non_exhaustive]` may break test-side struct literals — fix with `..Default::default()` patterns), clippy. Commit** — `git commit -m "feat(paged): pin-aware reconciliation + pinned_leaf_bytes (retained snapshots join the budget)"`.

### Task 10: Hard-cap clock eviction (cycling demote pass)

**Files:**
- Modify: `src/store.rs` (`demote_pass_inner` :2056 — wrap the per-table loop in a budget-cycled outer loop), `src/btree.rs` (`demote_leaves` :921 — second-chance stays; no change unless the cursor needs a wrapped-restart signal), `src/pagecodec.rs` (`PagedStats { clock_cycles: AtomicU64, forced_evictions: AtomicU64 }` + snapshot fields)
- Test: `tests/paged_accounting.rs`

**Interfaces:**
- Produces: `demote_pass` semantics change: when `memory_budget_bytes` is `Some` and the post-pass reconciled resident is still over budget, restart the cursor and sweep again — leaves cleared last cycle now evict (their bit was cleared and, unless re-touched mid-pass, stays clear). Stop when `resident <= budget` or a full cycle evicts 0 bytes (everything remaining is pinned/Resident/re-touched-every-cycle — the un-evictable floor). `forced_evictions` counts leaves evicted on cycle >= 2 (they were resident-touched when the pass began). `clock_cycles` counts cycles per pass (cumulative).
- Consumes: Tasks 8-9 (exact bytes; the pass reads the reconciled counter).

- [ ] **Step 1: Failing test (deterministic):** budget tiny (64 KiB), 5k rows, TOUCH EVERY LEAF (read scan) right before `checkpoint()` — the old single-sweep demotes ~0 (second-chance; this is `accessed_leaf_survives_one_pass`'s premise at scale) leaving resident >> budget; assert post-checkpoint `resident_leaf_bytes_est <= budget` AND `forced_evictions > 0` AND `clock_cycles >= 2`. A second test: everything pinned (hold a `pin_version` on a pre-demotion version... simpler: set table `Residency::Resident`) → pass terminates with resident over budget and does NOT loop forever (bounded by the evicted-0-bytes stop; assert the call returns).
- [ ] **Step 2: Implement** the outer loop in `demote_pass_inner` (per-table cursors reset per cycle; recompute resident between cycles from the stats counter which Task 8 keeps exact through demote debits). Termination clause exactly as the Interfaces block states — cite spec §6 in the comment.
- [ ] **Step 3: Suite (watch `tests/paged_demotion.rs::accessed_leaf_survives_one_pass` — it asserts the OLD semantics: with no budget pressure it must still pass, because cycling only engages while over budget; if its store sets a tiny budget, adjust that test's budget so it exercises second-chance under no pressure), clippy. Commit** — `git commit -m "feat(paged): hard-cap clock eviction — demote cycles until under budget"`.

### Task 11: Adaptive retention shrink under pin pressure

**Files:**
- Modify: `src/store.rs` (checkpointer tick after demote — the "order of weapons"; `gc_inner` :4080 gains a floor override), `src/persistence.rs` (`PagedOptions { shrink_retention_under_pressure: bool }` default `true` + builder setter + doc callout)
- Test: `tests/paged_accounting.rs`

**Interfaces:**
- Produces: `fn gc_inner_with_retain(inner: &mut StoreInner, retain: usize)` (existing `gc_inner` delegates with the configured count); after a hard-cap pass that ends over budget with `pinned_leaf_bytes >= (resident_total - budget)`, the checkpointer (when the knob is true) calls `gc_inner_with_retain(inner, 1)` — the existing `Arc::strong_count == 1` filter already spares every `ReadTx`/`VersionPin` holder, which IS the spec's floor; then re-reconciles.
- Consumes: Tasks 9-10.

- [ ] **Step 1: Failing tests:** (a) retention 8, insert-load across many commits, budget tiny → after ONE background-triggered (or direct `checkpoint()`) cycle, older unpinned snapshots are gone (`Store` observable: `begin_read(Some(old_v))` errs) and resident+pinned fits budget; (b) same but a `VersionPin` holds an old version → that version SURVIVES (assert `begin_read(Some(pinned_v))` still works) even though retention shrank around it; (c) knob false → nothing gc'd beyond configured retention, store stays over budget, `pinned_leaf_bytes` reports the excess.
- [ ] **Step 2: Implement** (weapons order inside the same checkpoint tick: cycles → shrink → gc → re-reconcile; one-line INFO-level `eprintln`-free — use the existing transition-logged pattern from `checkpointer_loop`'s `last_err` if any logging at all). Prominent doc on the knob + `PagedOptions` docs: this is the one deliberate behavior default (spec §5, Peter's ruling).
- [ ] **Step 3: Suite, clippy. Commit** — `git commit -m "feat(paged): adaptive retention shrink under pin pressure (on by default; VersionPins honored)"`.

### Task 12: Docs, gates, and the validation story

**Files:**
- Create: `docs/tasks/task64_paged_leaf_value_blocks.md` (canonical record: representation, invariants, accounting unit, hard cap, shrink, decision log pointer to the spec)
- Modify: `CLAUDE.md` (paged bullet: block leaves + hard-cap budget + `register_table_paged` + retention-shrink default), `docs/tasks/task63_paged_btree.md` (§7 pointer to task64)
- Test: whole-repo gates

- [ ] **Step 1: Write `task64` doc** (consolidate from the spec + what shipped; per the Feature Development Workflow both spec and this plan stay committed alongside).
- [ ] **Step 2: Full gates, in order, each green before the next:** `cargo test --features persistence`; `cargo clippy --all-targets --features persistence -- -D warnings`; Miri both models on `btree::`+`child::`; `make formal/cite-check` (re-anchor by reading if store.rs shifted); `make paging/check` (if a shape gate moved because pf/op changed with block decode, re-record thresholds in the Makefile with a comment citing this plan).
- [ ] **Step 3: Oracle long run:** `PROPTEST_CASES=512 cargo test --features persistence --test paged_block_leaves oracle -- --nocapture` once, as the pre-merge soak.
- [ ] **Step 4: Local sanity lever cell** (NOT publishable): `compare_benches/scripts/fs_paged_levers.sh` with default LIMIT — expect the glibc-T=32 cell's majflt/op to drop vs the 2026-08-31 baseline (fragmentation gone structurally). Record the number in task64 with the "local, sanity only" label.
- [ ] **Step 5: Commit docs.** The NVMe validation run (spec §8.7) is a SEPARATE, explicitly-approved follow-up under the fleet policy — record it in task64 as the open pre-publish obligation; do NOT provision from this plan.

## Plan Self-Review Notes (written at plan time)

- Spec coverage: §3→T1/T4/T5, §4→T2/T3/T5/T6/T7, §5→T4/T8/T9/T11, §6→T10, §7→T3/T7/T9, §8→T5 oracle/T4 fixture/T9 proptest/T10-T11 tests/T12 gates. No uncovered spec requirement found.
- Known verify-at-implementation anchors: `Table::delete` internals (:1022) and whether the immutable delete path shares `rotate_*` — T5/T7 say "check"; the executor verifies before patching (the controller rules if the plan text mismatches reality).
- Line numbers cite tree `255459d` and drift as tasks land — anchor by symbol name first, number second.
