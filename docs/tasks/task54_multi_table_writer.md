# Task 54: multi-table writer (`open_tables2` / `open_tables3`)

## Motivation

Issue #20. `WriteTx::open_table` takes `&mut self` and returns a `TableWriter`
borrowing the transaction, so only one table can be open for writing at a time.
A transaction that touches several tables per operation — an SMR applier: the
`hi-perf-cmp` LOB cell opens `levels`, `orders`, `meta` per command — must
close and re-open on every table switch. Task 53 (#19) made re-opening cheap;
this removes the re-open.

Measured headroom (post-#19, table-major-reorder probe on the batched cells):
opening each table once per batch instead of per command is worth **~35–40%
per applied op**, the largest lever after B-tree fanout.

## API

```rust
impl WriteTx {
    pub fn open_tables2<A: Record, B: Record>(
        &mut self, a: impl TableOpener<A>, b: impl TableOpener<B>,
    ) -> Result<(TableWriter<'_, A>, TableWriter<'_, B>)>;

    pub fn open_tables3<A: Record, B: Record, C: Record>(
        &mut self, a: impl TableOpener<A>, b: impl TableOpener<B>, c: impl TableOpener<C>,
    ) -> Result<(TableWriter<'_, A>, TableWriter<'_, B>, TableWriter<'_, C>)>;
}
```

Heterogeneous tuple (each table its own record type). Arities 2 and 3 cover the
known callers (LOB = 3); more can be added if needed. Chosen over a
closure form (ergonomics: the SMR loop holds the writers across a whole batch,
which reads naturally as a tuple binding) and over `RefCell`-per-entry (which
would add a runtime borrow check to *every* `open_table`, eroding #19, and turn
same-table double-open into a runtime error instead of a compile error).

Opening the same name twice in one call returns `Error::DuplicateTableOpen` —
two writers to one table would alias; use a single writer for that table.

## Implementation

- **Disjoint borrows without `unsafe`.** The several dirty/write-set entries are
  obtained with `BTreeMap::iter_mut`, which yields disjoint `&mut` to distinct
  keys in a single borrow. `BTreeMap` has no `get_disjoint_mut`, and it must
  stay a `BTreeMap` (commit locks tables in canonical sorted order for
  deadlock-freedom), so `iter_mut` — O(#dirty tables), tiny — is the route. An
  earlier raw-pointer draft (two `get_mut` through one `*mut` map) tripped
  Stacked Borrows; **Miri caught it**, and `iter_mut` replaced it. The final
  implementation contains no `unsafe`.
- **WAL sharing (the load-bearing change).** `TableWriter` held `&mut` to the
  transaction's single `Vec<WalOp>`, pushed on every insert/update — several
  live writers cannot share a `&mut Vec`. `WriteTx.wal_ops` became
  `RefCell<Vec<WalOp>>` and `WalOpsWriter.ops` a `&RefCell<…>`, the same
  shared-reference pattern `read_set`/`ddl_tables` already use. Each push is a
  short `borrow_mut`; `WriteTx` is `!Send + !Sync`, so it never contends.
  `#[cfg(feature = "persistence")]` only.
- **Shared helpers.** `open_table` was refactored through `ensure_dirty_entry`
  (phase-1: create-from-base + `register_table` + type-check, once per table)
  and the free functions `entry_writer_parts` / `assemble_writer`. The
  single-table path stays `unsafe`-free with one `get_mut` per map; the tuple
  methods reuse the same assembly. Every `TableWriter` field except `table` and
  `write_set` is an `Arc` clone or a shared `&`/`&RefCell`, so several writers
  share them freely; only `table` and `write_set` needed the disjoint-entry
  treatment.

## OCC / MultiWriter

Unchanged. Opening N tables registers N write-set entries and N intent contexts
— identical to sequential opens, without dropping the first writer. Per-table
locks are still acquired in canonical sorted order at commit; the task41
index-DDL guard and `Serializable` read-set tracking are per-table and
unaffected.

## Testing

`store::tests::open_tables*`: writes land in every table; a tuple-opened
transaction is byte-identical to the same writes via sequential single opens;
3-table interleave; `DuplicateTableOpen` for repeated names (both `2` and `3`,
all colliding pairs); per-element `TypeMismatch`; MultiWriter
disjoint-keys-both-commit vs same-key-conflict (proving write-set + intents are
tracked through the tuple path). Gates: `cargo test`,
`cargo test --features persistence` (418), `cargo clippy --all-targets
--features persistence` (clean); the tuple tests pass under Miri.

## Design history

`docs/superpowers/specs/2026-07-29-multi-table-writer-design.md`.
