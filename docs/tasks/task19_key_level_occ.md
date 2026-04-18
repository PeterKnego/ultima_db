# Task: Key-level MultiWriter OCC with per-key merge

## Motivation

Task 18 fixed a commit-time data-loss bug by switching MultiWriter OCC from
key-level to table-level. That was correct but pessimistic: any two writers
touching the same table always conflicted, even if they edited different
rows. Under the smallbank contention workload, this showed up as ~2.5× lower
throughput than fjall (which uses per-key SSI) — almost all of the gap was
pointless retries serialized through the commit lock.

This task restores key-level OCC, but with a commit path that actually
preserves concurrent writers' edits instead of silently overwriting them.
The task-18-era data-loss bug is fixed properly at key granularity via a
per-key merge.

---

## Goals

- Two writers on the same table but disjoint rows both commit on first try,
  and the final snapshot contains both edits.
- Two writers on overlapping rows still conflict (loser retries).
- Single-writer and no-concurrent-contention commits pay no more than the
  previous table-level design (fast path: wholesale install when no
  concurrent writer touched the table).
- The cross-DB correctness gate still passes — every `cargo bench`
  compares the engine's final state against a reference implementation.
  `StateHash` stays bit-identical across Ultima, RocksDB, and Fjall.

## Non-goals

- SSI (read-set tracking). Still SI — write skew possible across unrelated
  rows. Documented as a separate follow-up.
- Lock-free commit path. Still a single RwLock around the commit critical
  section.

---

## Architecture

### `MergeableTable` — new trait object in Snapshot.tables / WriteTx.dirty

Replaces `Arc<dyn Any + Send + Sync>` with `Arc<dyn MergeableTable>`.
`MergeableTable: Any + Send + Sync` so every existing downcast still works
via `.as_any().downcast_ref::<Table<R>>()`.

```rust
pub(crate) trait MergeableTable: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn boxed_clone(&self) -> Box<dyn MergeableTable>;
    fn merge_keys_from(
        &mut self,
        source: &dyn MergeableTable,
        keys: &BTreeSet<u64>,
    ) -> Result<()>;
}
```

`Table<R>: MergeableTable`. `boxed_clone` is O(1) CoW (`Clone` on `Table<R>`
bumps the B-tree root Arc and clone-boxes each index maintainer). `merge_keys_from`
walks the writer's write-set and upserts each modified record from `source`
into `self`, or deletes if the writer removed it.

### `Table::upsert_arc` — index-maintaining insert-or-replace

New `pub(crate)` method that takes a prebuilt `Arc<R>` and installs it at
an explicit id, dispatching through the existing `IndexMaintainer` hooks:

```rust
pub(crate) fn upsert_arc(&mut self, id: u64, arc: Arc<R>) -> Result<()> {
    let prior = self.data.get_arc(&id);
    // ... call on_update / on_insert on each index, rollback on failure ...
    self.data = self.data.insert_arc(id, arc);  // no R: Clone
    Ok(())
}
```

Key point: **no `R: Clone` bound.** `BTree<K, V>` already stores `Arc<V>`
internally, so we expose an Arc-passing insert (`BTree::insert_arc`) and
thread it through all the merge-path calls.

### Commit path

```rust
for (name, my_dirty) in dirty {
    // Did any concurrent committed writer touch this table since my base?
    let has_concurrent_write = inner
        .committed_write_sets
        .iter()
        .any(|cws| cws.version > self.base.version && cws.tables.contains_key(&name));

    if !has_concurrent_write {
        // FAST PATH: no concurrent write → dirty is already correct → wholesale swap.
        new_tables.insert(name, Arc::from(my_dirty));
        continue;
    }

    // SLOW PATH: merge my keys into latest's table.
    let keys = self.write_set.get(&name);
    match (new_tables.get(&name), keys) {
        (Some(latest_arc), Some(keys)) if !keys.is_empty() => {
            let mut merged = latest_arc.boxed_clone();
            merged.merge_keys_from(&*my_dirty, keys)?;
            new_tables.insert(name, Arc::from(merged));
        }
        // open-but-unwritten table while someone else wrote it:
        // keep latest's version.
        (Some(_), _) => {} // skip
        (None, _) => { new_tables.insert(name, Arc::from(my_dirty)); }
    }
}
```

### OCC check reverts to key-level

```rust
for (table_name, my_keys) in &self.write_set {
    if my_keys.is_empty() { continue; }
    if let Some(their_keys) = cws.tables.get(table_name)
        && !my_keys.is_disjoint(their_keys)
    {
        return Some(Error::WriteConflict { ... });
    }
}
```

Disjoint rows in the same table → no conflict. The per-key merge at commit
preserves each writer's edits.

### Commit-version bump for auto-assigned versions

Found during integration testing: pre-assigned versions from `begin_write(None)`
can land in a different order than commits acquire the lock. A writer with
pre-assigned `version=3` that commits AFTER one with `version=7` would
install its snapshot at v3, leaving `latest_version=7` pointing at a
chronologically earlier snapshot. The next commit then rebases onto v7,
missing v3's edits.

Fix: under the commit lock, auto-assigned versions bump to
`max(self.version, latest_version + 1)`. Explicit-version writers (SMR
mode) are left alone — the caller controls the version.

```rust
if matches!(self.writer_mode, WriterMode::MultiWriter)
    && !self.explicit_version
    && self.version <= inner.latest_version
{
    self.version = inner.latest_version + 1;
    if self.version >= inner.next_version {
        inner.next_version = self.version + 1;
    }
}
```

A new `explicit_version: bool` field on `WriteTx` tracks whether the caller
passed `Some(v)` to `begin_write`.

---

## Files touched

- **`src/btree.rs`** — added `pub fn insert_arc(&self, K, Arc<V>) -> BTree<K, V>`.
  `insert` now delegates to `insert_arc(key, Arc::new(val))`.
- **`src/table.rs`** — added `MergeableTable` trait and its impl for `Table<R>`
  (`boxed_clone`, `merge_keys_from`, `as_any`, `as_any_mut`). Added
  `Table::upsert_arc` for index-preserving arc installs.
- **`src/store.rs`** — `Snapshot.tables` and `WriteTx.dirty` element types
  changed to `MergeableTable`. OCC reverted to key-level. Commit path
  rewritten with fast-path + per-key merge. Added `explicit_version` field
  and commit-time version bump. Every `.downcast_ref::<Table<R>>()` /
  `.downcast_mut::<Table<R>>()` call site routed through `.as_any()` /
  `.as_any_mut()`.
- **`src/registry.rs`** — type aliases: `DeserializeTableFn` and
  `NewEmptyTableFn` return `Box<dyn MergeableTable>`; replay closures keep
  `&mut dyn Any` (callers upcast via `as_any_mut`). In-file tests updated.
- **`src/checkpoint.rs`** — cast sites migrated to `Arc<dyn MergeableTable>`;
  serialize closure receives `&dyn Any` via `as_any()`. In-file tests updated.
- **`tests/store_integration.rs`** — renamed
  `multi_writer_same_table_conflicts_and_retry_succeeds` back to
  `multi_writer_disjoint_keys_same_table_both_commit`, now asserting that
  both commits succeed and the final snapshot has both edits.
- **`CLAUDE.md`**, **`docs/ARCHITECTURE.md`** — updated OCC description and
  decision table.

---

## Verification

- `cargo test` (203 tests) and `cargo test --features persistence` (271 tests) green.
- `cargo clippy --tests -- -D warnings` clean for both feature combos.
- Concurrent correctness gate (16 bursts × 4 writers × 50 commutative ops)
  passes on all three engines with bit-identical StateHash. Abort counts:
  - ultima: ~81 (down from task18's ~95 — fewer retries thanks to key-level)
  - rocksdb: ~96
  - fjall: ~96
- Real-thread integration tests in `tests/store_integration.rs`
  (`concurrent_*`) pass across 5 consecutive runs — no flakes.

## Benchmark results (4 threads, Kelem/s median)

| Workload | task18 (table-level) | task19 (key-level + fast path) | fjall (SSI, reference) |
|---|---:|---:|---:|
| smallbank_contention_low | 320 | **370** | 418 |
| smallbank_contention_high | 275 | **292** | 747 |
| smallbank_mixed (1 writer) | 515 | 532 | 183 |
| smallbank_read_heavy | 1230 | 1230 | 427 |

Contention_low gains +16%. Contention_high gains +6%. Single-writer
workloads are unchanged (fast-path hits every commit). The remaining
gap to fjall on contention_high comes from fjall's LSM-memtable-only
commit path (no table-level CoW) and its SSI semantics, which are a
different architectural direction.

---

## Downsides still present

1. **Commit critical section is longer under contention.** The per-key
   merge takes the commit lock for O(keys × indexes) work. For writers
   with many keys in contended tables, the lock hold time can dominate.
   The fast path mitigates this when contention is zero; under real
   contention, commits serialize through the merge cost.
2. **Unique-index violations across writers surface at commit time.** Two
   writers setting different rows to the same indexed unique value both
   pass key-level OCC (disjoint rows) but the second's merge's
   `on_update` hits a `DuplicateKey` and the commit returns an error.
   Retry-friendly but a new failure mode.
3. **SI, still not SSI.** Write skew across unrelated rows is still
   possible. Needs read-set tracking in `ReadTx` / `TableWriter.get*`,
   analogous to fjall's `mark_read`. Tracked separately.
