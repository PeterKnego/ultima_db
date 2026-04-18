# Task: Full Multithreaded Concurrent Writes

## Motivation

`WriterMode::MultiWriter` already existed with OCC, write-set tracking, three-phase commit for WAL, and pruning — but `Store` could not actually cross thread boundaries. Tests that drove real threads wrapped `Store` in an `unsafe impl Send/Sync for SendStore(Store)` shim because `Snapshot.tables: BTreeMap<String, Arc<dyn Any>>` and `WriteTx.dirty: BTreeMap<String, Box<dyn Any>>` lacked `Send + Sync` on their trait objects. "Concurrent writes" was only exercised in single-threaded simulation or via the unsafe wrapper.

The user requirement: `Store` must be usable across threads so that (a) a new `WriteTx` can be opened on any thread, and (b) multiple `WriteTx` instances can run simultaneously on separate threads. A `WriteTx` itself is **not** intended to move between threads — it stays on its creating thread.

A second, larger issue surfaced during test development: the existing MultiWriter commit path built the new snapshot from `self.base.tables` (the writer's original base). Two concurrent writers with non-conflicting writes (different tables, or different keys in the same table) would both commit, but the later commit's snapshot *did not include the earlier commit's changes*. This was silent data loss — masked because existing tests only asserted that `commit()` returned `Ok`, never that both commits were visible in the final state. Making this correct is a prerequisite for the Send+Sync change to be meaningful.

---

## Goals

- `Store`, `ReadTx`, `TableReader`, `Snapshot` are `Send + Sync`. Clones can be moved into `thread::spawn` closures and used there.
- `WriteTx` and `TableWriter` are `!Send` — a transaction must be opened and committed on the same thread. Enforced at compile time via `PhantomData<*const ()>`.
- MultiWriter OCC catches all cases where two concurrent commits could cause one to shadow the other.
- Concurrent non-conflicting commits (different tables) merge into the same final snapshot — neither is lost.
- The `SendStore` unsafe wrapper is removed from tests and benches.

## Non-goals

- Key-level OCC. Two writers on the same table always conflict under this design, even on disjoint keys. The loser retries; the retry rebases onto the latest snapshot and succeeds.
- Lock-free commit path. Phases 1 and 3 of commit still take the store's `RwLock` write lock. Under heavy same-table contention, writers serialize on that lock plus OCC retries. Multi-core write scaling is a separate effort.
- `auto_snapshot_gc` default change. GC-during-commit amplifies contention but is out of scope.
- Async API. `Store` remains blocking.

---

## Architecture

### Bounds change

Trait objects that flow into snapshots picked up `+ Send + Sync`:

| Location | Before | After |
|----------|--------|-------|
| `Snapshot.tables` | `BTreeMap<String, Arc<dyn Any>>` | `BTreeMap<String, Arc<dyn Any + Send + Sync>>` |
| `WriteTx.dirty` | `BTreeMap<String, Box<dyn Any>>` | `BTreeMap<String, Box<dyn Any + Send + Sync>>` |
| `WriteTx::commit` local `new_tables` | same as Snapshot.tables | same as Snapshot.tables |
| `checkpoint.rs` cast sites (5) | `Arc<dyn Any>` | `Arc<dyn Any + Send + Sync>` |
| `registry.rs` type aliases | `Box<dyn Any>` in returns/params | `Box<dyn Any + Send + Sync>` / `&mut (dyn Any + Send + Sync)` |

All other supporting traits (`Record`, `IndexMaintainer`, `KeyExtractor`, `CustomIndex`, `FullTextIndex` extractor, registry closures) already required `Send + Sync`. The `R: Send + Sync + 'static` bound on `Table<R>` was also already in place.

The module-level `#![allow(clippy::arc_with_non_send_sync)]` in `src/store.rs` is gone. A compile-time `const fn _assert_store_is_thread_safe()` enforces `Store: Send + Sync`.

### `WriteTx` / `ReadTx` are `!Send`

Both structs gained a `_not_send: PhantomData<*const ()>` field. A raw pointer is `!Send + !Sync`, so the enclosing struct inherits that. Users who try to `thread::spawn(move || wtx.commit())` across thread boundaries get a compile error.

The correct pattern:

```rust
let store = store.clone();      // Store: Send + Sync
thread::spawn(move || {
    let mut wtx = store.begin_write(None)?;  // WriteTx is created on this thread
    // ... use wtx locally ...
    wtx.commit()
});
```

`ReadTx` is also `!Send`. A symmetric choice — a read transaction is always cheaply recreatable via `store.begin_read(Some(version))`, so pinning it to the thread is safer with no ergonomic cost.

### Table-level OCC + rebase-to-latest commit

The heart of the MultiWriter correctness fix:

**Old OCC (key-level):** `WriteTx::commit` scanned `committed_write_sets` for entries with `version > self.base.version` and flagged conflict only if the intersection of my modified keys and theirs (per table) was non-empty.

**New OCC (table-level):** The same scan, but *any* shared table name with a non-empty write-set on my side is a conflict. Empty write-set entries (from bare `open_table` calls that never wrote) are skipped.

**Old commit:** built `new_tables` from `self.base.tables` + `self.dirty`. Writer B committing after writer A would not include A's new table, because B's base was from before A's commit.

**New commit:** builds `new_tables` from `inner.snapshots[&inner.latest_version].tables` + `self.dirty`. Table-level OCC has already guaranteed no concurrent commit wrote to any table in my `dirty`, so unconditionally overwriting `new_tables[name]` with my dirty copy is safe. Concurrent commits to *other* tables survive into my final snapshot.

### Why table-level rather than key-level

Key-level OCC (the old design) allowed two writers to update disjoint keys in the same table without conflict. Making that work under rebase-to-latest requires replaying each writer's per-key modifications onto the latest snapshot's table. But `Snapshot.tables` holds `Arc<dyn Any>` — the concrete `Table<R>` type is erased. A per-table replay would need a new trait (`MergeableTable: Any + Send + Sync`) exposing "copy these keys from another table of the same type into me", which in turn needs either `R: Clone` (breaking API change to `Record`) or new `Arc<R>`-level accessors on `Table<R>` (`get_arc`, `reinsert_arc`) plus corresponding support in indexes and custom indexes. That's a large design effort.

Table-level OCC sidesteps all of that. The cost: any two writers sharing a table serialize through retry. In practice, concurrent writers typically partition work by table (users table vs. orders table vs. audit log); the same-table-disjoint-keys case is easy to work around by retry.

Four existing tests encoded the key-level semantic and were updated:

- `tests/store_integration.rs`: `two_overlapping_write_txs_to_different_tables` now asserts that v2 contains *both* `table_a` (from v1) and `table_b` (from v2's rebase).
- `tests/store_integration.rs`: `multi_writer_disjoint_tables_both_commit` now asserts both tables present in the final snapshot.
- `tests/store_integration.rs`: `multi_writer_disjoint_keys_same_table_both_commit` renamed to `multi_writer_same_table_conflicts_and_retry_succeeds` — writer B gets `WriteConflict` and demonstrates the retry pattern that lands both changes correctly.
- `tests/store_integration.rs`: `multi_writer_update_batch_failure_does_not_poison_write_set` restructured — writer A now writes to a different table so poisoning detection still works under table-level OCC (an empty write_set entry for "t" passes OCC; a poisoned non-empty one would conflict with B's commit to "t").

### `open_table` empty-write-set quirk

`WriteTx::open_table` in MultiWriter mode eagerly inserts an empty `BTreeSet<u64>` into `self.write_set[name]` (`entry().or_default()`). A reader-only `WriteTx` (one that opens a table and calls only read methods) has an empty entry for that table name. The new OCC skips entries where `my_keys.is_empty()`, so read-only writers still don't conflict. Batch operation failures rely on this too: a failed batch that properly rolls back its write-set additions leaves the entry empty, which is indistinguishable from "never wrote to this table" for OCC purposes.

### Three-phase commit unchanged

The WAL three-phase commit protocol (phase 1 under lock, phase 2 WAL fsync released, phase 3 under lock for promote) was already correct for multi-thread use. Each thread independently walks phases 1–3 for its own commit; the write lock provides mutual exclusion for OCC validation and snapshot promotion. The WAL writer thread is shared. No changes here.

### `SendStore` removed

Every `SendStore(store.clone())` in `src/store.rs` tests and `benches/multiwriter_persistence_bench.rs` became `store.clone()` directly. The `unsafe impl Send/Sync` shim and its 11 call sites are gone.

---

## Files touched

- **Modified:** `src/store.rs` — bounds change, `PhantomData` markers, OCC rewrite, commit rebase, `#![allow(clippy::arc_with_non_send_sync)]` removed, `SendStore` removed, compile-time assertion added.
- **Modified:** `src/checkpoint.rs` — cast sites updated for `+ Send + Sync`.
- **Modified:** `src/registry.rs` — type aliases updated.
- **Modified:** `tests/store_integration.rs` — 4 existing tests updated for new semantics, 6 new real-thread tests added.
- **Modified:** `benches/multiwriter_persistence_bench.rs` — `SendStore` wrapper removed.
- **Created:** `examples/concurrent_writes.rs` — 8-thread retry-loop demonstration.
- **Modified:** `CLAUDE.md`, `docs/ARCHITECTURE.md` — limitations and decision table updated.

---

## Verification

- `cargo test` and `cargo test --features persistence` — 66 and 67 tests green respectively.
- `cargo clippy --tests -- -D warnings` and `cargo clippy --tests --features persistence -- -D warnings` — zero warnings.
- New `concurrent_*` tests in `tests/store_integration.rs` (6 tests: send+sync compile check, disjoint tables both visible, same-table disjoint keys via retry, same-table overlapping keys with conflict+retry, readers unaffected by writers, stress test with 200 parallel inserts) — all green, re-run 5× without flakiness.
- `cargo run --example concurrent_writes` — 8 threads × 50 writes = 400 commits, ~350 retries on the shared-table hot path, all counters land at 50.

---

## Downsides still present

1. **No multi-core scaling on the commit path.** Phases 1 and 3 take `inner.write()`. Contention serializes writers.
2. **Table-level OCC is pessimistic.** Any two writers on a shared table retry through each other. For workloads that want key-level granularity, the retry cost may dominate.
3. **`auto_snapshot_gc = true` default amplifies contention** under multiple writers. GC runs inside the commit critical section.
4. **WAL still funnels through one writer thread.** Throughput bounded by fsync rate when persistence is on.
5. **Retry-loop is now the normal pattern.** `WriteConflict { table, keys, version }` is retry-friendly, but users need to wrap commits in a loop.
