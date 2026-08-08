# Incremental Checkpoints via CoW Structural Diff

**Status:** design
**Date:** 2026-08-08
**Task doc (on completion):** `docs/tasks/task61_incremental_checkpoints.md`

## Problem

`Store::checkpoint()` writes a *full* serialized snapshot of every registered
table every time (`src/checkpoint.rs:6`). `serialize_table` walks the whole
B-tree with `table.iter()` and bincode-encodes every row
(`src/registry.rs:414`). A store that commits ten rows between checkpoints
re-serializes all N rows to move a ten-row watermark.

Checkpoint cost therefore scales with dataset size rather than with change
volume, and it is on the critical path of two things operators care about:
WAL prune cadence (the WAL cannot be trimmed past the last checkpoint) and
recovery time. The workaround today is to checkpoint less often, which trades
one problem for the other.

## Key insight

UltimaDB is copy-on-write. When a commit forks a new snapshot, every subtree it
did not touch is *literally the same `Arc<BTreeNode>`* as in the previous
snapshot. Two snapshots can be diffed by pointer identity, in time proportional
to what changed, with no dirty-key bookkeeping on the commit path and nothing to
keep in sync.

This is the direct payoff of the CoW design, and it is strictly better than the
LSM-world answer to the same problem (WiscKey-style key/value separation), which
exists to avoid rewriting values during compaction — a cost UltimaDB does not
have.

## Scope

In scope:

- `BTree::diff` — an ordered diff of two versions of the same tree.
- A delta checkpoint format carrying per-key changes, including deletions.
- Chain-aware recovery, cleanup, and WAL prune.
- A `checkpoint_chain_max` config knob bounding chain length.

Explicitly out of scope:

- **Value logs / KV separation.** Once deltas exist, unchanged values are
  already not rewritten. A value log would only pay for very large values and
  costs an out-of-line format plus its own GC story. Cut.
- **Background chain compaction.** Bounded chains (below) make it unnecessary,
  and a background rewriter racing `checkpoint()` and the WAL prune is the
  riskiest part of the design space.
- **Delta-encoding of secondary indexes.** Checkpoints store data + `next_id`
  only; indexes are rebuilt on load via `rebuild_from_sorted_data`. Unchanged.

## Design

### 1. `BTree::diff`

```rust
pub enum Change<'a, K, V> {
    Added(&'a K, &'a Arc<V>),
    Updated(&'a K, &'a Arc<V>),
    Removed(&'a K),
}

impl<K: Ord + Clone, V> BTree<K, V> {
    /// Changes that turn `base` into `self`, in ascending key order.
    pub fn diff<'a>(&'a self, base: &'a BTree<K, V>) -> impl Iterator<Item = Change<'a, K, V>>;
}
```

**Algorithm — dual-cursor ordered merge with subtree skipping.** Both trees are
walked in key order by cursors that expose the current *subtree root*, not just
the current entry. At each step:

- If the two cursors' subtree roots are `Arc::ptr_eq`, both skip that subtree
  wholesale — every key inside is unchanged, by CoW construction.
- Otherwise compare current keys: base key less → `Removed`; new key less →
  `Added`; equal → `Updated` unless the value `Arc`s are `ptr_eq`.

Complexity is O(changed + height), and removals fall out of the same pass — no
auxiliary pointer set, no second traversal. This requires a cursor that can skip
a subtree; the existing iterator machinery (`src/btree.rs:737` onward) descends
but does not expose skip, so this is the one new B-tree primitive.

**Correctness bar.** A diff that drops a key silently loses a row on recovery.
This is verified against a full-scan oracle by proptest, not by example tests
(see Testing).

### 2. Type erasure

Snapshot tables are `Arc<dyn MergeableTable>` and `K` must not appear in the
trait's signature. The diff therefore goes through the registry's function
pointers, exactly as `serialize_table` does today: a new
`diff_table: fn(new: &dyn Any, base: &dyn Any) -> Result<Vec<u8>>` in
`TableInfo`, downcasting both sides to `Table<R, K>` and erroring if the base
side is a different concrete type (a table dropped and recreated with a
different `R`/`K` — see Table lifecycle below).

### 3. File format

Deltas share the `checkpoint_{version}.bin` namespace and are distinguished by a
format-version byte in the header. This is deliberate: an older binary reading a
directory whose head is a delta picks that file as latest and fails its format
check *loudly*, rather than silently loading an older full checkpoint whose WAL
tail has already been pruned. Downgrade must not lose committed data quietly.

```text
[magic "ULDB"][format_version: u32 = 2][kind: u8 = FULL | DELTA]
[snapshot_version: u64]
[base_version: u64]          -- DELTA only
[num_table_entries: u32]
for each table:
    [name_len: u32][name]
    [entry_kind: u8 = Unchanged | Delta | Full | Dropped]
    Unchanged -> (no payload)
    Full      -> [data_len: u64][serialize_table bytes]      -- new tables
    Dropped   -> (no payload)
    Delta     -> [next_id: opt][num_changes: u64]
                 [ [klen: u32][key][op: u8 = Put | Del]
                   Put -> [rlen: u32][record] ]*
[crc32: u32]
```

`FULL` is byte-identical to today's format apart from the version/kind bytes, so
a full checkpoint remains a self-contained chain of length one.

Per-key length caps stay as they are: `check_encoded_key_len` must be applied on
the delta path too, or a key the delta accepts and the WAL refuses becomes a row
that survives one durability path and destroys the other (the task56 hazard).

### 4. Base snapshot retention

Diffing needs the last-checkpointed snapshot still in memory. `gc()` can evict
it, so `checkpoint()` takes a `VersionPin` (task53) on the version it wrote and
holds it until the next checkpoint replaces it.

This is the design's real cost: a pinned base snapshot keeps its unshared nodes
alive. Incremental checkpointing trades **memory for I/O**, and the trade gets
worse the longer the gap between checkpoints. The `checkpoint_chain_max` knob
(below) bounds it, and setting the knob to force full checkpoints drops the pin
entirely.

Two cases force a full checkpoint regardless of chain position:

- **After recovery**, there is no in-memory base.
- **After `bulk_load`**, which installs a wholly new tree; the diff would
  degenerate to a full rewrite anyway, and going straight to full is cheaper and
  simpler. `checkpoint_and_prune_after_bulk` (`src/store.rs:1607`) forces it.

### 5. Chain policy and cleanup

`StoreConfig::checkpoint_chain_max: usize` (default `1` — always full, i.e.
today's behaviour exactly). With `chain_max = N`, at most `N - 1` deltas follow
a full before the next `checkpoint()` writes a full.

Recovery cost and file count are both bounded by construction. Cleanup
(`src/checkpoint.rs:249`) gains a second invariant rather than trading its
existing one away. It keeps **never delete newer than `keep_version`** — a
stale caller must not remove a file it knows nothing about — and adds **never
delete an ancestor of the head chain**, since that is what would make the head
unrecoverable. In practice this means cleanup deletes only files older than the
newest full that the head descends from.

Config plumbing follows task44: `StoreConfig` is `#[non_exhaustive]`, so the
knob is added through the builder.

### 6. Recovery

`find_latest_checkpoint` becomes `find_head_chain`, returning the head file plus
its ancestors resolved by `base_version` back-pointers. `load_checkpoint`
applies the base full and then each delta in version order, applying `Put`/`Del`
per key and honouring `Dropped`/`Full` table entries.

A missing or corrupt mid-chain file is a hard failure — a new
`Error::CheckpointChainBroken`. It cannot be handled by falling back to an older
full, because the WAL has already been pruned to the head version, so the older
full plus the surviving WAL does not reconstruct the committed state. Note that
tmp+rename means a delta is atomically present or absent; an absent delta simply
means the head is the previous file, which is a complete chain and recovers
normally. A broken chain therefore means genuine corruption.

**Ordering, and why it is crash-safe:** write delta → fsync → verify ancestors
present → prune WAL to the delta's version → cleanup. A crash anywhere before
the prune leaves a durable delta and an un-pruned WAL, which over-recovers
harmlessly (replay is idempotent against the checkpointed version).

### 7. Table lifecycle

Between two checkpoints a table can be created (→ `Full` entry), dropped (→
`Dropped`), or dropped and recreated with a different concrete type (→ `Full`,
detected by the `diff_table` downcast failing on the base side). The registry
already exposes `key_type_id`/`key_type_name` for exactly this kind of guard;
the delta path reuses them rather than inventing a second identity check. The
task59 race matrix is the reference for which of these interleavings are
reachable.

## Testing

The correctness risk is concentrated in `diff` and in chain recovery, so both
get oracle-based property tests rather than examples.

1. **Diff oracle (proptest).** Random op sequences produce two snapshots;
   `diff` output must equal the brute-force difference of the two full key/value
   scans. Shrinks to minimal counterexamples.
2. **Chain equivalence (proptest).** Random op sequences checkpointed under a
   random chain policy; the recovered store must be byte-identical to one
   recovered from a full checkpoint at the same version. This is the test that
   would catch a dropped key.
3. **Subtree-skip effectiveness.** Assert `diff` visits O(changed) nodes, not
   O(N) — a diff that is *correct* but silently walks the whole tree passes
   every other test while delivering none of the benefit.
4. **Chain break.** Delete or corrupt a mid-chain delta; recovery must fail with
   `CheckpointChainBroken`, never succeed with partial data.
5. **Lifecycle.** Create / drop / drop-and-recreate-with-different-type across a
   delta boundary.
6. **Forced-full paths.** Post-recovery and post-`bulk_load` checkpoints are
   full.
7. **Downgrade.** A delta-headed directory read by the v1 format check fails
   loudly.

## Benchmarking

Checkpoint cost is serialize + I/O, both of which the sandbox measures at ±2x —
**no perf conclusion from a local run** (CLAUDE.md). The win must be shown on
the `bench-infra` NVMe host: checkpoint wall time and bytes written vs. change
ratio (1%, 10%, 100% of rows dirtied between checkpoints), at a fixed dataset
size, both durability tiers. The expected shape is cost tracking change ratio
instead of dataset size; the interesting question is where the crossover sits,
since a 100%-dirty delta is strictly worse than a full (it pays diff plus
per-key framing).

Recovery time as a function of chain length is the second measurement, and it is
what should set the recommended `checkpoint_chain_max` default in the task doc.

## Risks

| Risk | Mitigation |
|---|---|
| Diff drops a key → silent data loss on recovery | Proptest against full-scan oracle; chain-equivalence proptest |
| Cleanup deletes a chain ancestor → unrecoverable head | Invariant rewritten to "never delete an ancestor of the head"; dedicated test |
| Pinned base snapshot grows memory | Bounded by `checkpoint_chain_max`; default `1` retains today's behaviour and holds no pin |
| Old binary silently loads stale full checkpoint | Deltas share the filename namespace so the format check fails loudly |
| Delta larger than a full at high change ratios | Measured crossover; policy can force full above a threshold if the bench justifies it |

## Estimate

~1 week, roughly half of it tests. `BTree::diff` plus the cursor skip is ~150
LOC; format, writer, and chain recovery ~400 LOC; cleanup and config the
remainder.

## Origin

Prompted by <https://jidin.org/lsm/> (LSM survey). The transferable idea was
WiscKey's "stop rewriting what didn't change"; the CoW structural diff is the
UltimaDB-native form of it, and it subsumes the value-log mechanism the post
describes.
