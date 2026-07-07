# Task 41: MultiWriter Index-DDL Hard Error

**Origin:** 2026-06 deep-review deferred backlog / task21 "v1 limitations".
`define_index` / `define_custom_index` inside a MultiWriter transaction was
silently dropped when the commit took the merge slow path: the merge clones
the *latest* table (which lacks the new index) and replays only write-set
keys. Worse, the slow path's no-row-writes branch discarded the dirty table
wholesale — so even the recommended "DDL in its own transaction" pattern
silently lost the index when a concurrent writer snuck in.

## What changed

- New `Error::IndexDdlConflict { table }`.
- `WriteTx` tracks DDL'd tables (`ddl_tables: RefCell<BTreeSet<String>>`,
  the SSI read-set pattern); `TableWriter::define_index` /
  `define_custom_index` record the table name **after** the underlying call
  succeeds (a rejected DDL doesn't taint the commit). SingleWriter
  transactions skip tracking entirely (`TableWriter.ddl_tables` is `None`).
- Commit phase 1 (under the same read lock that computes the per-table
  fast/slow flags): any DDL'd table with a concurrent committed write since
  the tx base aborts the commit with `IndexDdlConflict` — before any merge
  or WAL submission, so nothing is half-installed.

## Semantics

- Fast path unchanged: DDL commits fine when no concurrent commit touched
  that table — including all of SingleWriter mode.
- Plain key-level OCC still runs first: if the DDL transaction's row writes
  *also* genuinely overlap a concurrent commit's keys (e.g. two inserts from
  the same base colliding on an auto-assigned id), `WriteConflict` fires
  before the DDL check. Both are correct aborts; the ordering is deliberate.
- The check covers all slow-path branches, including the concurrent
  table-delete branch: erroring there is a deliberate conservative false
  positive (delete+recreate semantics combined with DDL are murky).
- Re-defining an existing index (idempotent same-kind call) also marks the
  table — conservative, rare, and harmless to retry.
- `IndexDdlConflict` is deliberately distinct from `WriteConflict`:
  existing rebase-retry loops won't auto-retry it. Retry the DDL in its own
  transaction when the table has no concurrent writers.
- Still out of scope (task21 limitations): the DDL backfill read remains
  invisible to SSI, and DDL generates no conflicts for other writers. This
  fix removes only the silent loss.

Design history: `docs/superpowers/specs/2026-07-07-index-ddl-conflict-design.md`,
`docs/superpowers/plans/2026-07-07-index-ddl-conflict.md`.
