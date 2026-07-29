# Multi-table writer (`open_tables2`/`open_tables3`) — design

Date: 2026-07-29
Status: approved (API shape chosen: tuple opener)

## Motivation

Issue #20. `WriteTx::open_table` takes `&mut self` and returns a `TableWriter`
borrowing the transaction, so only one table can be open for writing at a time.
A transaction that touches several tables per operation (an SMR applier: the LOB
cell opens `levels`, `orders`, `meta` per command) must re-open on every table
switch. #19 made re-opening cheap; #20 removes the re-open entirely.

Measured headroom (post-#19, table-major-reorder probe): keeping command-major
access but opening each table once per batch is worth **~35–40% per-op** on the
multi-table batched cells — the largest remaining lever after fanout.

## API

```rust
impl WriteTx {
    pub fn open_tables2<A: Record, B: Record>(
        &mut self,
        a: impl TableOpener<A>,
        b: impl TableOpener<B>,
    ) -> Result<(TableWriter<'_, A>, TableWriter<'_, B>)>;

    pub fn open_tables3<A: Record, B: Record, C: Record>(
        &mut self,
        a: impl TableOpener<A>,
        b: impl TableOpener<B>,
        c: impl TableOpener<C>,
    ) -> Result<(TableWriter<'_, A>, TableWriter<'_, B>, TableWriter<'_, C>)>;
}
```

Heterogeneous tuple (each table its own `R`). Arities 2 and 3 cover the known
callers (LOB = 3); more can be added if a real need appears (YAGNI). Chosen over
the closure form (`with_tables`) for ergonomics — the SMR pattern holds the
writers across a whole batch loop, which reads naturally as a tuple binding —
and over the `RefCell`-per-entry form, which would add a runtime borrow check to
*every* `open_table` including the single-table common case (eroding #19) and
turn same-table double-open into a runtime error instead of a compile error.

**Duplicate names.** Opening the same table twice in one call is the aliasing
hazard the tuple must reject: returns a new `Error::DuplicateTableOpen(String)`
if any two names are equal. (Different tables only; same table → use one
writer.)

## Internals

Two phases, because phase 1 mutates the `dirty` map (insert-on-first-open) and
phase 2 hands out borrows into it:

1. **Ensure entries (sequential, `&mut self`).** Refactor the first half of
   `open_table` into `fn ensure_dirty_entry<R: Record>(&mut self, name: &str) ->
   Result<()>`: create the table from the base/deleted-set, `register_table`,
   insert the `DirtyEntry`, type-check. Call it for each name. This also
   type-checks each table up front.
2. **Hand out disjoint borrows (`unsafe`, keys proven distinct).** With all
   entries present and names checked pairwise-distinct, take raw `*mut` to each
   `DirtyEntry` and to each `write_set` entry and reconstitute `&mut`. Distinct
   `BTreeMap` keys ⇒ distinct, non-overlapping values ⇒ sound. This is the
   pattern already used in `src/table.rs` (index mutation, "raw pointers to
   avoid borrowing … while iterating"). `dirty` must stay a `BTreeMap` (commit
   acquires table locks in canonical sorted order for deadlock-freedom), and
   `BTreeMap` has no `get_disjoint_mut`, so the raw-pointer route is required.

**WAL sharing — the load-bearing change.** `TableWriter` currently holds
`wal_ops: Option<WalOpsWriter { ops: &'tx mut Vec<WalOp> }>` — a `&mut` to the
transaction's single WAL-op `Vec`, pushed on every insert/update. Two live
writers cannot both hold `&mut` to that `Vec`. Fix: route it through a shared
reference, exactly as `read_set` and `ddl_tables` already do (both are
`&RefCell<…>` "recorded through a shared reference held by `TableWriter`"):

- `WriteTx.wal_ops: Vec<WalOp>` → `RefCell<Vec<WalOp>>`.
- `WalOpsWriter.ops: &'tx mut Vec<WalOp>` → `&'tx RefCell<Vec<WalOp>>`.
- Each push site (`insert`/`update`/`delete`, ~6 sites) becomes
  `w.ops.borrow_mut().push(...)`.

The borrow is a short-lived single-threaded `borrow_mut` (`WriteTx` is
`!Send + !Sync`), so no contention. WAL-op ordering within a transaction is
preserved: pushes happen in call order regardless of which writer issues them,
and commit drains the `Vec` once (`std::mem::take` / `into_inner`). This is
`#[cfg(feature = "persistence")]`; without the feature there is no `wal_ops`
field and nothing changes.

`metrics`, `table_metrics` (`Arc` clones), `intent_ctx` (shared `&intents` +
`&waiter`), `read_set`, `ddl_tables` are already shareable across writers —
`Arc` clones or shared `&`/`&RefCell`. Only `table`, `write_set`, and `wal_ops`
needed attention; the first two via disjoint pointers, the third via `RefCell`.

## OCC / MultiWriter semantics

Unchanged. Opening N tables at once registers N `write_set` entries and N intent
contexts — identical to opening them sequentially, just without dropping the
first writer. Conflict detection, per-table locks (still acquired in canonical
order at commit from the sorted `dirty` map), and the task41 index-DDL guard are
untouched. `Serializable` read-set tracking is per-table and unaffected.

## Error handling

- Any two names equal → `Error::DuplicateTableOpen(name)`, before any borrow.
- Per-table type mismatch → `Error::TypeMismatch(name)` from phase 1, as
  `open_table` does today.

## Testing (TDD)

1. Hold two writers to different tables, interleave writes, commit; result
   byte-identical to the same writes done via sequential single `open_table`s.
2. Same for three tables, across a loop (the LOB-batch shape).
3. Duplicate name in `open_tables2`/`3` → `DuplicateTableOpen`.
4. Per-element type mismatch → `TypeMismatch`.
5. First-open-of-a-new-table via the tuple creates it correctly (matches
   single-open create).
6. MultiWriter: two concurrent transactions, each using `open_tables2` on
   disjoint keys, both commit; overlapping keys → `WriteConflict`. Confirms the
   write-set entries are tracked per table through the tuple path.
7. Persistence: a transaction using `open_tables3` records all WAL ops (presence
   + order) and recovers to the same state as the sequential path — exercises
   the `RefCell` WAL route. (Run under `--features persistence`.)
8. Serializable: a read through one tuple writer participates in SSI tracking
   (read-set recorded), matching single-open behavior.

Gates: `cargo test`, `cargo test --features persistence`, `cargo clippy
--all-targets -- -D warnings`, and the Elle consistency harness (commit-path
adjacent). The `unsafe` block gets an explicit review pass.

## Out of scope

- Arities beyond 3 (add when needed).
- `ReadTx` multi-open (readers are cheap to hold via `Arc`; no aliasing).
- A closure form (`with_tables`) — the tuple covers the need.
- Changing `open_table` itself — unchanged, still the single-table path.

## Docs

`docs/tasks/task54_multi_table_writer.md` consolidates on completion; this spec
retained as design history.
