# Paged CoW B-tree, stages 1+2 (larger-than-memory) — design

**Date:** 2026-08-30
**Status:** approved design, pre-implementation (stage 3 — in-place eviction,
pinning, `Result`-based reads — is a separate, later spec)
**Prior art / motivation:** `docs/benchmarks/paging-baseline-local-2026-08-29.md`
(OS swap as a memory tier: reads at ReDB parity, cold-key writes 100–300× slower
because `Arc` CoW touches the whole sibling fan-out); task37 (WAL preallocation
primitive); task56 (`PrimaryKey` order-preserving encoding); task61 (pointer-
identity diff); task62 (format back-compat rules); task58 (write overlay).

## 1. Goal, target, non-goals

**Goal.** A store whose data does not fit in RAM: many small rows (10⁸–10⁹),
secondary indexes and the data tree's inner levels resident, data leaves on
disk and loaded on touch, memory bounded by an operator-set budget, no change
to the read API.

**Success criteria** (gated in §9, measured by the harness from the spike):

- A 5M-row store can be **built by inserts** inside a cgroup at ¼ of its
  in-memory footprint with `memory_budget_bytes` set, without an OOM kill.
- Cold point read ≈ **1** page fault; cold update ≈ **2** (leaf in, leaf out).
  The in-memory tree on swap measured ~1.4 and ~30–40.
- `recover()` cost ∝ inner levels + indexes, not rows.
- In-memory (non-paged) stores: byte-for-byte unchanged behaviour; `make
  perf/check` and the fanout microbench within noise.

**Non-goals for this spec** (stage 3, or follow-ons listed in §10): eviction
between checkpoints or under memory pressure, a pinning/guard API, fallible
(`Result`) reads, persisting `CustomIndex`/`FullTextIndex`, bulk-loading
directly to pages, page compression, the leaf/inner node split.

## 2. Decisions taken during brainstorming

| # | decision | why |
|---|---|---|
| Q1 | The page file is the **checkpoint format**; the WAL is untouched. | Keeps task15/37/38 durability semantics, `Durability`/`WalWrite`, SMR mode. Committed nodes are immutable, so the page file is append-only with no write-back and no dirty-page tracking. The LMDB-style "every commit appends nodes" model was rejected as a separate project. |
| Q2 | A lazy read that hits an I/O or CRC error **panics**; read signatures are unchanged. | `None` would be a silent wrong answer; `Result` reads break every caller and are exactly the API rework stage 3 must do anyway (evicted memory cannot be handed out as `&R`). One breaking change later, not two. Equivalent to mmap engines' SIGBUS. |
| Q3 | **Demotion at checkpoint, as a re-publish of the latest version**, is in scope. | Without it every touched leaf stays resident forever, so a larger-than-memory store cannot be *built* or written to. Doing it as a version keeps old readers' views intact with no pins and no API change. Eviction *in place* stays stage 3. |
| — | A **background checkpointer thread** is added. | With demotion, checkpoint cadence bounds memory; it cannot stay app-driven only (today the only in-crate `checkpoint()` caller is a test). |
| — | Page file I/O reuses the **task37 prealloc primitive**. | Physical zero-fill (not `fallocate`), chunked grow-ahead, `sync_data`: sync p50 123 → 36 µs in hi-perf-cmp's ladder; a checkpoint is thousands of pages per sync, so the "batch" rung is free. |
| — | `posix_fadvise(RANDOM)` on the page file. | Swap readahead pulled 2–3 pages per useful one in the spike; RocksDB's `advise_random_on_open` avoids it. |
| — | Scope: **stages 1+2 in this spec; stage 3 separate.** | |

## 3. Page file format and `PageId` (§1)

**One file per store, `pages.bin`**, holding every table's data nodes and every
persisted index's nodes. One prealloc cursor, one grow-ahead, one fadvise, one
`sync_data` per checkpoint, one root record naming every table's root
atomically.

**`PageId = u64` byte offset into `pages.bin`, stable for the page's life.**
Pages are never rewritten (append-only). Space is reclaimed by
**hole-punching dead ranges** (`fallocate(FALLOC_FL_PUNCH_HOLE)`), never by
moving pages: ids never change, parents are never rewritten because a child
moved, and no compaction is required for correctness. Punching never
rewrites a region, so the "written extents only" property of the prealloc
primitive holds. A rewrite-into-a-fresh-file compaction exists only as an
explicit maintenance operation for a badly fragmented file.

*Rejected:* `(generation, offset)` ids with compaction into new files —
every compaction rewrites the live inner tree and changes every id.

**Page = one node, variable length.**

```
page header (12 B): kind u8 | fmt u8 | flags u8 (bit0 = compressed, reserved) | pad u8
                    | payload_len u32 | crc32 u32 (crc32fast, over header + payload)
kind ∈ { DataLeaf, DataInner, IndexLeaf, IndexInner }

inner payload:  n u16 | (key, value)[n]     (values are stored in every node, not just leaves —
                                            this is a B-tree, not a B+-tree, so an inner node's
                                            own keys carry their values directly)
                       | child_ids[n+1] u64
leaf payload:   n u16 | (key, value)[n]     (key as above; value = bincode via the Record bounds;
                                            index non-unique value `()` = zero bytes)
```

Fetch: one `pread` of `page_prefetch_bytes` (4 KiB default) at the id, then a
second read for the remainder when `payload_len` exceeds it. Keeping the
length out of the slot keeps the slot 16 bytes (§4), which matters because
inner nodes are resident.

**Root record: `checkpoint-<version>.root`**, written tmp+rename, using the
existing container framing (magic, `FORMAT_VERSION`, new
`CheckpointKind::Paged`) so older readers fail cleanly and the new reader
reads all older kinds. Contents:

```
version u64 | file_end u64 (write cursor after this checkpoint)
| tables: [{ name, key_type_id u32, root_page, height u32, len, next_id,
             indexes: [{ name, ik_type_id u32, kind, generation u32, root_page, height u32, len }] }]
| dead_pages: [(offset, len)]   — pages the previous root referenced and this one does not
```

Each tree's `height` rides along with its `root_page` because `BTree` caches its
height and `BTree::from_root_page(id, len, height, source)` needs it up front —
without it, attaching a paged tree would require an extra read (or a
height-discovery walk) before the first operation.

One root file per checkpoint keeps `cleanup_old_checkpoints`, chain
bookkeeping and the WAL-prune trigger on their existing file-per-checkpoint
model.

**Compression:** header flag reserved; off in this cut.

## 4. The child slot and fault-in (§2)

Today `BTreeNode.children` is `FixedVec<Arc<BTreeNode<K,V>>, MAX_KEYS+2>`
(`src/btree.rs:232`). The slot replaces the `Arc`:

```rust
struct Child<K, V> {
    /// bit 63: accessed (second-chance bit); bits 0..63: PageId or NO_PAGE.
    meta: AtomicU64,
    /// Arc::into_raw(node) or null = not loaded. Set once per slot instance
    /// (CAS); never cleared — a slot only moves toward "loaded".
    node: AtomicPtr<BTreeNode<K, V>>,
}
```

Legal states: **resident-dirty** (`node` set, `NO_PAGE`: created by CoW since
the last checkpoint), **resident-clean** (`node` set, page id), **on-disk**
(`node` null, page id). `null + NO_PAGE` is a bug (`debug_assert`).

| operation | behaviour | what it touches |
|---|---|---|
| `load(&self, src) -> &BTreeNode` | non-null → return it, setting `accessed` only if clear. Null → `src.read_node(id)` (one `pread` + CRC + decode into a fresh `Arc`), CAS into `node`; a concurrent loser drops its copy. I/O or CRC failure → panic naming table, page id, cause (Q2). | one page on miss |
| `clone()` (CoW of the *parent*) | copy `meta`; if `node` non-null, `Arc::increment_strong_count`; **if null, nothing**. | resident children only — this line is the sibling fix |
| `make_mut(&mut self, src) -> &mut BTreeNode` | `load`; strong count 1 → in place; else clone the node into a new `Arc`, store it (we hold `&mut`), set `meta = NO_PAGE`. The displaced node keeps its page id in older versions; the checkpoint's dead-list finds it. | the target node |
| `drop` | non-null → `Arc::from_raw` drop. | |

Returning `&BTreeNode` tied to `&self` is sound because the pointer is never
cleared within a slot's life; this is what keeps `Table::get -> Option<&R>`
and every read method unchanged.

**Loader.** `BTree<K,V>` gains `source: Option<Arc<dyn NodeSource<K,V>>>`
(read + decode a page into a node), implemented in `checkpoint.rs` under the
`persistence` feature for `K: PrimaryKey, V: Record`, set at `recover()`/
attach, cloned O(1) with the tree. `source == None` (in-memory stores, every
existing test) can never hold an on-disk slot: behaviour unchanged, only the
slot size is paid. The tree also carries `root_page: Option<PageId>` with the
same dirty/clean meaning as a slot.

**Identity for the checkpoint walk / dead-list:** two slots are the same node
iff they carry the same page id, or point at the same `Arc`. A resident-dirty
slot equals nothing.

**Costs, stated:** slot 16 B vs 8 B → inner nodes' child storage 520 → 1040 B
(~+130 MB resident at 10⁹ rows). Leaves carry the inline children `FixedVec`
too, so pre-existing dead space per leaf doubles (520 → 1040 B); the fix is
the leaf/inner node split (§10). Values stay `(K, Arc<V>)` inside a loaded
leaf; the leaf is the unit of residency.

Index trees use the same `BTree`, so they get slots; their nodes are loaded
eagerly and never demoted (§8), so for them the slot is where the checkpoint
finds the page id.

## 5. The checkpointer (§3)

Reuses `checkpoint_impl` (`src/store.rs`): serialize under `checkpoint_lock`,
snapshot under a brief `inner.read()`, **no store lock during I/O**, WAL
prune handed to the WAL thread.

**Thread.** A `checkpointer` thread owned by the `Store` (started in
`Store::new` when persistence is paged, joined on drop — the WAL thread's
pattern). `Store::checkpoint()` stays and runs the same routine on the
caller's thread under the same lock. Triggers: `dirty_bytes ≥
checkpoint_dirty_bytes` (each CoW clone in `make_mut` adds its node size to
a per-store counter), `resident_leaf_bytes_est ≥ memory_budget_bytes`,
`checkpoint_interval` elapsed with any dirt, or an explicit call. All run
the same four phases.

**Phase 1 — dirty walk.** A node is on disk iff its slot carries a page id,
so the walk descends from each root only through `NO_PAGE` slots. O(dirty);
needs no retained base snapshot (unlike task61's diff). Index trees walk the
same way.

**Phase 2 — write.** Grow-ahead zero-fill if within a chunk of capacity.
Write dirty nodes **bottom-up** (a parent's payload needs its children's
ids), `pwrite` at the cursor, store the id into the node's slot `meta` — a
monotonic `NO_PAGE → id` transition on an atomic, safe on a snapshot readers
share; every newer version sharing the node now sees it as clean.
`sync_data`; write `checkpoint-<v>.root` tmp+rename; `sync_data`. That is
the commit point.

**Phase 3 — demotion install.** Under `inner.write()`, take the *current*
latest, walk resident inner levels, and for each data-leaf slot with a page
id: `accessed` clear → rebuild the parent with `{id, null}`; `accessed` set
→ clear the bit, keep. Batched at ≤ `demote_batch` parents per lock hold
(~1 ms), lock released between batches; each batch re-reads latest and
retries if a commit interleaved. Tables with `Residency::Resident`, index
trees and inner levels are skipped.

Rules:
- **Demotion re-publishes the latest version; it creates none.** Rows are
  unchanged, so `snapshots[latest]` is swapped for a new `Arc<Snapshot>` with
  the same version number. No churn for `gc()`/retention/`VersionPin`; no
  version outside the consensus log in SMR mode. Readers holding the old
  `Arc` keep it; it frees when they drop it.
- **A concurrent commit may undo a demotion, never corrupt one.** The
  MultiWriter fast path installs a writer's table wholesale when no committed
  writer touched it; racing a demotion, the un-demoted lineage wins — same
  rows, memory freed one checkpoint later.

**Phase 4 — reclaim.** Dead pages = nodes the previous root referenced that
this root does not, computed by walking previous vs new root by page id
(inner levels resident, leaves compared by id without loading), O(changed),
written into the new root's `dead_pages`. When `cleanup_old_checkpoints`
deletes root *v−1*, *v*'s dead-list is hole-punched. Punch is idempotent: a
crash between delete and punch leaks space (reclaimable by a maintenance
scan), never data. Then the existing WAL prune.

**`checkpoint_chain_max`** is inert in paged mode: every root is
self-contained, there is no chain; cleanup is "keep `retained_checkpoints`
roots". Documented, not repurposed.

**`bulk_load`** still builds in memory via `from_sorted`; a larger-than-
memory bulk load does not fit. Same limitation as today, now visible; direct
page writing from the sorted iterator is a follow-on (§10).

## 6. Index persistence and attach (§4)

`IK` today is `Ord + Clone + Send + Sync + 'static` (`src/index.rs:94,125`) —
no encoding. `PrimaryKey` is the bound persistence needs (order-preserving
`encode`/`decode`, stable type id; integers, `String`, `Vec<u8>`, 2-/3-tuples,
so the `(IK, K)` composite key is covered).

**Persisted:** managed `Unique`/`NonUnique` indexes with `IK: PrimaryKey`.
Storage is already `BTree<IK, K>` / `BTree<(IK, K), ()>`; §3–§4 apply
unchanged. Loaded eagerly at attach, never demoted.

**Not persisted in this cut:** `CustomIndex`, `FullTextIndex` — rebuilt by
scan at recover as today; on a paged store that scan faults every leaf.
Logged at recover and documented.

**API.** No specialization in Rust, so `define_index` cannot become
persistent when `IK: PrimaryKey`. Following task56's additive precedent:

```rust
table.define_persisted_index::<IK: PrimaryKey>(name, kind, IndexDef { generation: u32 }, extractor)
```

`define_index` keeps its signature and rebuild-on-recover behaviour.
*Accepted alternative:* tighten `define_index` to `IK: PrimaryKey` in a
breaking release (one method; breaks callers indexing by a custom `Ord`
type). **Open for the owner's call at plan time; the design is identical
either way.**

**Attach**, at the app's call after `recover()`. `recover()` keeps each root
record index entry `{ name, ik_type_id, kind, generation, root_page }` as an
opaque pending entry:

| root record vs. call | action |
|---|---|
| name absent | new index → rebuild by scan (faults every leaf; logged) |
| type id, kind, generation all match | **attach**: load the index tree resident, bind the extractor; no data touched |
| type id or kind differ | `Error::IndexDefinitionMismatch`; never silently rebuild |
| generation differs | rebuild by scan, persist under the new generation |

**`generation` — the trap it closes.** The extractor is a closure; nothing on
disk can detect that the app changed which field it indexes. Attaching stale
contents would return wrong query results silently. `generation` is the
app's declaration that the definition changed; forgetting to bump it is a
silent-staleness bug, said in bold in the docs. Default `0`.

**Lifecycle.** Plain `define_index` or `drop_index` on a table whose root
record names persisted contents → those pages appear in the next dead-list.
Index DDL inside MultiWriter transactions keeps the task41
`IndexDdlConflict` rule. `ik_type_id` reuses the task62 key-type-id guard.

## 7. Recovery and format compatibility (§5)

Rules kept from task62: newer releases read every older format; two
independent version axes; the key-type-id guard; pre-0.3.0 WALs unreadable.

**Directory in paged mode:** `pages.bin`; `checkpoint-<v>.root` × N;
`wal.bin` (Standalone); legacy `checkpoint-<v>.bin` may coexist during an
upgrade.

**`recover()`:**
1. Find the newest checkpoint by version across both kinds.
2. Newest is legacy → load through the existing row path; tables come up
   fully resident. The first checkpoint afterwards finds every slot
   `NO_PAGE` and writes the whole tree (one O(N) checkpoint). **That is the
   upgrade path; no conversion tool.**
3. Newest is paged → read the root; per registered table check the key-type
   id (records are guarded as today: by table name and the registry's
   typed downcast); load the **inner levels** by descending from the root
   page and stopping at leaf-kind slots (sequential reads, resident); leaves
   stay on disk; index entries held pending (§6); write cursor =
   `file_end`.
4. Replay the WAL as today; row ops fault in exactly the leaves they touch.
   `BulkLoadNotCheckpointed` semantics unchanged.

Cost O(inner levels + attached indexes + WAL-touched leaves). Failures in
1–3 are ordinary `Err` from `recover()` (`CheckpointCorrupted`/
`Persistence`); only lazy faults after recovery panic (Q2).

**Crash consistency.** The root record is the commit point. Pages a crashed
checkpoint wrote but never named lie past the last committed `file_end`;
recovery resets the cursor there and the next checkpoint overwrites them
(the region was zero-filled once, so the written-extents property holds).
Per-page CRC means a torn page is never *read* as valid, only unreferenced.

**Mismatches:** an older binary opening a paged directory hits the unknown
`CheckpointKind::Paged` byte → clean "unsupported checkpoint kind". A paged-
capable binary configured for row checkpoints opening a directory whose
newest checkpoint is paged → refused at `Store::new` naming the directory
and the fix (the ≤0.2.x precedent; silently ignoring a newer root would
recover stale data). Paged config on a legacy directory → allowed (step 2).

**Snapshot streaming** (SMR handoff) is unchanged: it iterates rows through
the read path and faults leaves as it goes; O(rows) I/O is inherent.

**Version axes:** the root carries the container `FORMAT_VERSION`; each page
carries its own `fmt` byte.

## 8. Config surface and residency (§6)

All additive behind existing `#[non_exhaustive]` types; the default store is
byte-for-byte unchanged.

```rust
Persistence::standalone(dir, durability, wal_write).paged(PagedOptions::default())
Persistence::smr(dir).paged(..)
Persistence::standalone_fast(dir).paged(..)     // durability knobs are orthogonal
```

`PagedOptions` (builder, `#[non_exhaustive]`):

| knob | default | bounds |
|---|---|---|
| `memory_budget_bytes` | `None` | resident data-leaf bytes (estimate); exceeding it runs a demotion pass. `None` = never demote on memory. The operator's one important knob. |
| `checkpoint_dirty_bytes` | 256 MiB | dirty node bytes before a checkpoint; bounds WAL replay length and un-demotable memory |
| `checkpoint_interval` | `None` | time-based checkpoint for quiet stores |
| `demote_batch` | 1024 parents | lock hold per demotion batch |
| `page_prefetch_bytes` | 4 KiB | first `pread` on fault-in |
| `prealloc_chunk_bytes` | 16 MiB | zero-fill grow-ahead quantum |
| `retained_checkpoints` | 2 | roots kept; a root's dead-list is punched when it is deleted |

**Residency:** `Resident` (eager, never demoted) or `Lazy` (leaves on disk,
demotable). Fixed by rule: index trees and every data tree's inner levels
are `Resident`. Data leaves default `Lazy`; one override,
`Store::set_residency("table", Residency::Resident)`, for small hot tables.

`memory_budget_bytes` and `checkpoint_dirty_bytes` are separate because
reads don't dirty: a read-heavy store never hits the dirty trigger yet its
touched leaves accumulate. `resident_leaf_bytes_est` = faulted-in + created
− demoted, maintained by `load()`/`make_mut()`/the demotion pass; it does not
see frees (which lag `gc()` anyway). A trigger, not a guarantee; documented.

**Metrics** (existing `metrics` feature): `page_faults`, `pages_written`,
`leaves_demoted`, `dirty_bytes`, `resident_leaf_bytes_est`,
`checkpointer_runs`, `dead_pages_punched`.

**Interactions (documented):** `checkpoint_chain_max` inert in paged mode;
`num_snapshots_retained`, `VersionPin`, long `ReadTx` now also delay memory
release; MultiWriter allowed (demotion may be undone, never broken);
`bulk_load` allowed, builds in memory; `fanout-t8`, the write overlay,
`ConsistentInline`, `CoalescedPrealloc` all compose; requires the
`persistence` feature.

## 9. Testing (§7)

- **Slot — unit (`btree.rs`).** Counting `NodeSource`. Cloning a parent with
  *n* on-disk children performs zero reads and zero refcount changes (named
  test: the sibling fix). Two-thread fault-in race leaves one `Arc`.
  `make_mut` on a shared node → `NO_PAGE`. Three legal states; panic on I/O
  and CRC error.
- **Page codec — unit (`checkpoint.rs`).** Round-trip every `PrimaryKey`
  impl and tuples through all four kinds; flipped byte fails CRC; payloads at
  `prefetch − 1`, `prefetch`, `prefetch + 1`; unknown `kind`/`fmt` rejected.
- **Tree with a source — property test.** Random op sequences against an
  all-resident tree and an all-on-disk twin behind a counting source: every
  read/iterate/mutate op agrees, and the twin's read count equals distinct
  leaves touched (one fault per cold read as an invariant).
- **Checkpointer — integration.** Pages written == nodes CoW'd; bottom-up ids
  resolve; a no-commit checkpoint writes nothing. Demotion: accessed-clear →
  null, accessed-set survives exactly one pass, `latest_version` unchanged
  after re-publish, `demote_batch = 1` interleaved with commits loses no
  rows. Dead-list == replaced nodes; punch only after the older root is
  deleted. Crash points via the task60 fault-injection pattern: between page
  `sync_data` and root rename → previous root recovers; between rename and
  punch → clean recovery, nothing leaked that a rescan can't reclaim.
- **Recovery / compat.** Legacy dir upgrades and the first paged checkpoint
  writes every node; paged dir + row config refused at `Store::new`; future
  `kind` byte fails cleanly; key-type guard fires for index keys; WAL replay
  read count == leaves touched; garbage past `file_end` ignored and
  overwritten. Extend `tests/checkpoint_chain_equivalence.rs`: same op
  script through rows-format and paged stores reads identically after
  recovery.
- **Indexes.** Persist → attach with no data reads; `IndexDefinitionMismatch`;
  `generation` bump rebuilds; `CustomIndex` rebuilds by scan and logs;
  orphaned persisted pages appear in the next dead-list.
- **Acceptance — bigger than RAM, built by writes.** `paging_matrix` gains an
  `ultima-paged` engine. In a cgroup at ¼ footprint with
  `memory_budget_bytes`: build 5M rows by inserts, run YCSB-C/A, restart,
  run again. Assert `memory.events oom == 0`; faults/op ≈ 1 read, ≈ 2
  update; recovery time ∝ inner levels. Exposed as `make paging/check`
  (needs cgroup delegation); shape-only in the sandbox, numbers on the NVMe
  rig.
- **In-memory regression.** `make perf/check` (autobench Gate-A) and the
  fanout microbench; a regression from the 16-byte slot is a finding to
  report. The formal cite guard (`check-cites.py`) breaks on any line shift
  in `store.rs`/`persistence.rs`: re-anchor by reading.
- **Existing suites.** Full `cargo test`, `-p ultima-vector`, clippy
  `-D warnings`, one Elle run. Mutation testing, if any, in a worktree.

## 10. Follow-ons (explicitly out of scope)

1. **Stage 3:** in-place eviction between checkpoints, pinning/guards,
   `Result`-based reads, memory-pressure-driven demotion.
2. **Leaf/inner node split** of `BTreeNode` (removes the inline children
   capacity from leaves; halves resident leaf overhead).
3. **Bulk load directly to pages** from the sorted iterator.
4. **Page compression** (header flag reserved): the OS page cache would hold
   ~1.5–2× more rows, and a demoted-then-refaulted leaf hits page cache.
5. **Persisting `CustomIndex`/`FullTextIndex`** via a trait exposing their
   backing trees.
6. **Sorted overlay flush** so a flush's leaf faults are sequential-ish.
7. **`bench/paging` target** on `bench-infra` (swap on NVMe, swappiness
   override, non-compressible payload, 8× column) for the published baseline.

## 11. Open items for the plan

- `define_persisted_index` (additive) vs. tightening `define_index` to
  `IK: PrimaryKey` (breaking) — owner's call (§6).

Resolved during self-review: the root record carries `key_type_id` and
`ik_type_id` as `PrimaryKey::KEY_TYPE_ID` (`u32`, `src/primary_key.rs:42`,
tuple ids composed by the existing `const fn`). There is no record type id
in the codebase and the root record does not introduce one; records stay
guarded by table name plus the registry's typed downcast, exactly as row
checkpoints are today.
