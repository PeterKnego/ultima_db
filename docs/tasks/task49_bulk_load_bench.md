# task49: Bulk-load benchmark vs competitors

**Status:** Implemented. Timed bulk-ingest comparison (build empty db of N
records) across five arms — UltimaDB `insert_batch`, UltimaDB `Store::bulk_load`,
RocksDB, Fjall, ReDB — in both durability tiers. Run:
`make bench/bulk-load/compare ULTIMA_BENCH_DIR=$HOME/bench-disk`.
**Related:** `docs/tasks/task48_btree_insert_mut.md` (the `insert_mut` win this
exercises), `docs/superpowers/specs/2026-07-08-bulk-load-benchmark-design.md`.

## Why

The YCSB comparison (`ycsb_bench`) times only single-op-per-transaction writes,
where `insert_mut` cannot help (a lone insert CoW-clones the whole path), and it
loads its data as untimed setup. This benchmark times the multi-insert-per-
transaction ingest path — where `insert_mut` actually wins — against the same
competitors.

## What is measured

Per engine/arm and size N ∈ {10 000, 100 000, 1 000 000}: wall-clock to ingest
records `1..=N` into an empty database via that engine's idiomatic single-batch
path, plus one end-of-load durability sync (Strict tier) or none (NonDurable).
Db-open and teardown are untimed criterion setup.

Fairness invariant: idiomatic single-batch path per engine; exactly one fsync at
the end in Strict, none in NonDurable. No per-record fsync.

## Arms

| arm            | load path                                                        |
|----------------|------------------------------------------------------------------|
| ub_batch       | `Table::insert_batch` in one `WriteTx` → commit (`insert_mut`)    |
| ub_sorted      | `Store::bulk_load(Replace(auto_id_vec), checkpoint_after)`        |
| ub_rocksdb     | one `WriteBatch` of N puts → `write_opt(sync)`                    |
| ub_fjall       | insert N → `persist(SyncAll)` once (Strict)                       |
| ub_redb        | one write-txn, N inserts, `set_durability`, commit               |

## Results

_(fill in the two `critcmp` tables — non-durable and strict — from a run on a
quiet real-disk host.)_

## Notes

- Absolute numbers are host-relative; the deliverable is the cross-engine A/B on
  a quiet real-disk `ULTIMA_BENCH_DIR`. Opt-in tier, not wired into `make bench`
  or CI (like the rest of `compare-benches`).
- `ub_sorted` (from_sorted, O(N)) is the ceiling and should be ≤ `ub_batch`.
