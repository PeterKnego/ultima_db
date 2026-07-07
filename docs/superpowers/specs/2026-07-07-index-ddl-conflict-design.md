# MultiWriter index-DDL hard error — design

**Date:** 2026-07-07
**Status:** approved (brainstormed with Peter)
**Origin:** 2026-06 deep-review deferred backlog / task21 "v1 limitations" —
"Index DDL is invisible to SSI and OCC … on the MultiWriter merge slow path
the new index definition is silently dropped."

## Problem

`define_index` / `define_custom_index` inside a MultiWriter transaction can be
silently lost at commit. The merge slow path (a concurrent commit touched the
same table since my base) clones the *latest* table — which lacks my new
index — and replays only my write-set keys onto it. Worse than documented:
the slow path's no-row-writes sub-branch ("read-only open or failed batch")
discards the dirty table wholesale, so even the *recommended* pattern — DDL in
its own transaction — silently loses the index if a concurrent writer sneaks
in on the same table.

There is no `drop_index` API; the two `TableWriter` DDL methods are the whole
surface.

## Decision (with Peter, 2026-07-07)

Turn the silent drop into a hard commit-time error — a **new distinct variant
`Error::IndexDdlConflict { table }`** (not a reused `WriteConflict`): loud and
self-explanatory is the point of the fix. Existing `WriteConflict` retry loops
won't auto-retry it; docs tell users to retry the DDL transaction when the
table is quiet. Full DDL conflict-checking / carrying the index through the
merge is explicitly out of scope (v2 if ever needed).

## Design

### Error variant (`src/error.rs`, next to `WriteConflict`)

```rust
#[error("index DDL on table '{table}' conflicts with a concurrent commit; \
         retry the DDL in its own transaction when the table is quiet")]
IndexDdlConflict { table: String },
```

### Tracking

`WriteTx` gains `ddl_tables: RefCell<BTreeSet<String>>` — same
interior-mutability pattern as the SSI `read_set` (`WriteTx` is
`!Send + !Sync`). `TableWriter` gets a `&'tx RefCell<BTreeSet<String>>`
reference plus its existing `table_name`; `define_index` and
`define_custom_index` insert the table name **after** the underlying call
succeeds, so a failed DDL (kind mismatch, name collision) doesn't taint the
commit.

### Commit check

In commit phase 1, inside the existing read-lock scope that computes the
per-table fast/slow flags from `committed_write_sets` (store.rs ~2627): if a
dirty table is in `ddl_tables` **and** any committed write set with
`version > base` contains that table → return
`Err(IndexDdlConflict { table })` before any merge or WAL submission.

This covers all three slow-path sub-branches:
1. keys replayed onto latest (index would be dropped),
2. no-row-writes wholesale discard (DDL-only transaction — the sneaky one),
3. concurrent table delete + my wholesale reinstall — erroring here is a
   deliberate conservative false positive; delete+recreate semantics combined
   with DDL are murky.

### Unchanged

- Fast path: DDL with no concurrent commit on that table — including **all**
  of SingleWriter mode — works exactly as today (dirty table installs
  wholesale, index included).
- DDL still generates no conflicts *for others*, and the full-table backfill
  read remains invisible to SSI (documented task21 limitation). This fix
  removes only the silent loss.
- No `drop_index`; no new public API beyond the error variant.

## Testing

1. DDL tx + concurrent row commit on the same table → `IndexDdlConflict`;
   afterwards the latest snapshot's table has no half-installed index.
2. DDL-only tx (no row writes) + concurrent commit on same table → same error
   (the wholesale-discard branch).
3. DDL tx + concurrent commit on a *different* table → commits fine; index
   present and queryable.
4. DDL + row writes with no concurrency (MultiWriter fast path) → commit
   succeeds; index queryable.
5. SingleWriter DDL → unaffected.
6. Failed `define_index` (kind mismatch) does not mark the table → commit
   succeeds despite a concurrent commit on it.

## Docs

- `docs/tasks/task41_index_ddl_conflict.md` (canonical feature doc).
- Update task21 "v1 limitations" bullet (silent drop → hard error).
- Update the CLAUDE.md conventions line that currently says index DDL "is
  silently dropped if the commit takes the merge slow path".
