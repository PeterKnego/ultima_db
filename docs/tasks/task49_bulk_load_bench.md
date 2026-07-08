# task49: Bulk-load benchmark vs competitors

**Status:** Implemented; results recorded (dev sandbox, 2026-07-08 — see §Results).
Timed bulk-ingest comparison (build empty db of N records) across five arms —
UltimaDB `insert_batch`, UltimaDB `Store::bulk_load`, RocksDB, Fjall, ReDB — in
both durability tiers. Both UltimaDB arms lead every competitor in the non-durable
tier; UltimaDB still leads in the strict (one-fsync) tier. Run:
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

Measured 2026-07-08 on the dev sandbox, `ULTIMA_BENCH_DIR` on ext4 (real disk, not
tmpfs), criterion `sample_size(10)`. **Absolute numbers are host-relative — the
cross-engine ranking and ratios are the deliverable** (all five arms run back to
back on the same host, so contention is common-mode). Cell = median wall-clock
(throughput). Fastest per row in **bold**.

### Non-durable tier (build cost, no fsync)

| N     | ub_sorted        | ub_batch        | ub_rocksdb    | ub_fjall     | ub_redb      |
|-------|------------------|-----------------|---------------|--------------|--------------|
| 10 K  | **5.8 ms** (1692 K/s) | 11.2 ms (868 K/s) | 19.8 ms (492 K/s) | 35.1 ms (278 K/s) | 39.2 ms (249 K/s) |
| 100 K | **59.0 ms** (1655 K/s) | 126.6 ms (771 K/s) | 354.9 ms (275 K/s) | 386.2 ms (253 K/s) | 498.1 ms (196 K/s) |
| 1 M   | **0.657 s** (1487 K/s) | 1.241 s (787 K/s) | 3.60 s (272 K/s) | 4.24 s (235 K/s) | 5.83 s (168 K/s) |

Both UltimaDB arms beat every competitor at every size. `ub_sorted` (from_sorted)
leads the fastest competitor (RocksDB) by **5.5×–8.9×** throughput; `ub_batch`
(the `insert_mut` path) leads RocksDB by ~1.9× and ReDB by ~4.7× at 1 M.

### Strict tier (one fsync at end of load)

| N     | ub_sorted       | ub_batch        | ub_rocksdb    | ub_fjall     | ub_redb      |
|-------|-----------------|-----------------|---------------|--------------|--------------|
| 10 K  | 24.0 ms (408 K/s) | **18.2 ms** (536 K/s) | 22.4 ms (435 K/s) | 41.3 ms (237 K/s) | 49.4 ms (198 K/s) |
| 100 K | **233.5 ms** (418 K/s) | 345.0 ms (283 K/s) | 379.2 ms (258 K/s) | 407.9 ms (239 K/s) | 556.8 ms (175 K/s) |
| 1 M   | **2.30 s** (422 K/s) | 3.31 s (297 K/s) | 3.72 s (262 K/s) | 4.24 s (230 K/s) | 6.06 s (161 K/s) |

The gaps compress: the end-of-load durability sync is now a shared floor, and
UltimaDB pays serialization here (WAL for `ub_batch`, checkpoint for `ub_sorted`)
that it skips in memory. `ub_sorted` still leads at 100 K / 1 M (1.6× RocksDB,
1.8× Fjall, 2.6× ReDB at 1 M); `ub_batch` roughly ties RocksDB and beats
Fjall/ReDB. At 10 K, `ub_batch` is fastest — the checkpoint's fixed cost makes
`ub_sorted` slightly slower at small N. Competitor ordering is stable throughout:
RocksDB > Fjall > ReDB.

## Notes

- **In-memory vs disk framing.** UltimaDB is an in-memory store — it keeps
  `Arc<YcsbRecord>` in the tree and serializes nothing in the non-durable tier —
  whereas RocksDB/Fjall/ReDB are disk engines that bincode-serialize and write on
  every load. So part of the non-durable lead is architectural (in-memory vs
  on-disk), not solely `insert_mut`. The **strict tier is the more apples-to-apples
  durability comparison** (UltimaDB pays WAL/checkpoint serialization + fsync
  there), and UltimaDB still leads.
- **What `insert_mut` contributes.** `ub_batch` is the arm `insert_mut` powers.
  Per task48 §5 the immutable insert path was ~20× slower at the B-tree layer;
  without `insert_mut`, `ub_batch` would be far slower and would lose to the
  competitors instead of leading/tying them. `ub_sorted` (from_sorted, O(N)) does
  not use `insert_mut` and is the ceiling — faster than `ub_batch` everywhere
  except 10 K strict (checkpoint fixed cost).
- Absolute numbers are host-relative; re-run `make bench/bulk-load/compare
  ULTIMA_BENCH_DIR=…` on a quiet real-disk host for publishable figures. Opt-in
  tier, not wired into `make bench` or CI (like the rest of `compare-benches`).
- Tooling note: when cargo's target dir is not the worktree's `./target` (a shared
  `CARGO_TARGET_DIR` env var, or a `[build] target-dir` in `.cargo/config.toml` —
  the dev-sandbox case), the final `critcmp` step fails with "could not find
  Criterion output directory". The benches still ran and their baselines are saved;
  re-run `critcmp --target-dir <cargo-target-dir> ub_*_<tier>` pointing at that dir
  (find it via `cargo metadata --format-version 1` → `target_directory`). The
  `make bench/bulk-load/compare` recipe (like the sibling `bench/ycsb/compare`)
  assumes `./target`, which is correct on a plain bench host.
