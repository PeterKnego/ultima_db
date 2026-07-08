# Bulk-load benchmark vs competitors — design spec

**Date:** 2026-07-08
**Status:** Approved (design), ready for implementation plan
**Related:** `docs/tasks/task48_btree_insert_mut.md` (the `insert_mut` optimization this
benchmark exercises), `benches/ycsb_bench.rs` + `bench_workloads/src/ycsb.rs` (the existing
comparison harness this mirrors).

---

## 1. Motivation

The `insert_mut` optimization (task48) only pays off when **many inserts share the same
transaction**, so successive inserts reuse the path nodes the transaction already
privatized. The existing YCSB comparison (`ycsb_bench` vs RocksDB / Fjall / ReDB) does
**one write op per transaction** in every *timed* workload, so it cannot show the win — and
its only multi-insert code (`preload()`) runs as *untimed* setup.

This benchmark closes that gap: it **times the bulk-load / ingest path** — building a fresh,
empty database of N sequential records — across the same four engines, in both durability
tiers. It answers "did `insert_mut` make UltimaDB's batch-ingest competitive?" and shows
where `Store::bulk_load` (the `from_sorted` specialized path) sits as the ceiling.

## 2. What is measured

For each engine and each size N ∈ {10 000, 100 000, 1 000 000}: the wall-clock to ingest
records `1..=N` into an **empty** database via that engine's idiomatic single-batch path,
then make the dataset durable **once** (strict tier) or not at all (non-durable tier).

**Timing boundary.** Database open (fresh tempdir + open handle) is *untimed* setup;
teardown (flush + drop + remove tempdir) is *untimed*. Only the record ingestion plus the
single end-of-load durability sync is timed. Implemented with criterion
`iter_batched(setup = make_empty(N), routine = load(&mut e, N), BatchSize::PerIteration)`.

**Fairness rule (INVARIANT).** Every engine loads N sequential keys into an empty db via
its idiomatic single-batch path, with exactly **one** durability sync at the *end* in the
strict tier and **none** in the non-durable tier. No per-record fsync — this is batch
ingest, deliberately distinct from the per-op commit granularity of the YCSB suite. Each
bench file documents its engine's specific choice in a header comment (matching the style
of the existing `ycsb_*_bench.rs` files).

## 3. Arms

Six bars, benchmark IDs `bulk_load/{arm}` so `critcmp` groups them:

| arm                | load path                                                                 | non-durable            | strict (one sync)                       |
|--------------------|---------------------------------------------------------------------------|------------------------|-----------------------------------------|
| `ultima_batch`     | one `WriteTx` → `Table::insert_batch(Vec<YcsbRecord>)` → `commit()`        | `Durability::Eventual` | `Durability::Consistent` (WAL coalesced + 1 fsync) |
| `ultima_bulk_load` | `Store::bulk_load("ycsb", Replace(auto_id_vec(vec)), BulkLoadOptions{..})` | `checkpoint_after:false`| `checkpoint_after:true` (durable checkpoint) |
| `rocksdb`          | one `WriteBatch` of N puts → `db.write_opt(batch, write_opts)`             | `set_sync(false)`, WAL on | `set_sync(true)`                       |
| `fjall`            | insert N records → `db.persist(SyncAll)` once (strict only)               | none (inserts buffer, no fsync) | `PersistMode::SyncAll` once    |
| `redb`             | one write-txn, N inserts, `set_durability(_)`, `commit()`                  | `Durability::None`     | `Durability::Immediate`                 |

`ultima_batch` is the arm powered by `insert_mut`; `ultima_bulk_load` is the `from_sorted`
ceiling. All engines use identical WAL/storage tuning to the existing YCSB benches where
applicable (e.g. RocksDB: large write buffer, `disable_auto_compactions(true)` so the
window measures the write path, not background compaction).

## 4. Components & file layout

- **`bench_workloads/src/bulk_load.rs`** (new module, re-exported from `lib.rs`): the shared
  harness. Contains:
  - `pub const BULK_SIZES: &[u64] = &[10_000, 100_000, 1_000_000];`
  - `pub fn bench_bulk_load<E>(c, name, make_empty, load)` where
    `make_empty: impl FnMut(u64) -> E` (untimed) and `load: impl FnMut(&mut E, u64)`
    (timed). Registers one criterion bench per size under group `bulk_load/{name}` with
    `throughput = Throughput::Elements(N)`.
  - Reuses `bench_disk_dir()` and `bench_durability()` / `BenchDurability` from the existing
    `ycsb` module (no duplication).
- **`benches/ycsb_bulk_load_bench.rs`** (new, root crate): registers `ultima_batch` and
  `ultima_bulk_load` via `bench_bulk_load`. `[[bench]]` entry in root `Cargo.toml`
  (`harness = false`).
- **`compare_benches/benches/ycsb_bulk_load_rocksdb_bench.rs`**,
  **`…_fjall_bench.rs`**, **`…_redb_bench.rs`** (new): one arm each. `[[bench]]` entries in
  `compare_benches/Cargo.toml`.
- **Makefile**: new target `bench/bulk-load/compare`, mirroring `bench/ycsb/compare` —
  same `ULTIMA_BENCH_DIR` real-disk guard (refuse if unset), loop over `nondurable`/`strict`
  tiers, `--save-baseline {arm}_{tier}` per engine, `critcmp` side-by-side per tier. Add the
  new `.PHONY` entry.
- **`docs/tasks/task49_bulk_load_bench.md`** (new): canonical feature doc; record the A/B
  once run. Cross-link from `task48` §5.

## 5. Data flow

```
criterion iter_batched:
  setup  (untimed): make_empty(N) -> fresh tempdir under bench_disk_dir(), open empty db/store
  routine (timed):  load(&mut engine, N):
                      build records 1..=N  (YcsbRecord::new(i))
                      ingest via engine's single-batch path
                      one durability sync iff strict tier
  drop   (untimed): engine + tempdir dropped
```

`bench_durability()` (reads `ULTIMA_BENCH_DURABILITY`) selects the tier inside each `load`
closure and each `make_empty` (for engines that fix durability at open time). Record
payload is the existing `YcsbRecord` (already `Serialize + Deserialize + Clone`); competitors
bincode-encode it exactly as in the YCSB benches, with big-endian `u64` keys.

## 6. Error handling

Benchmark code: `.expect(...)` on setup/IO failures (fatal, aborts the bench) — consistent
with every existing `*_bench.rs`. The `load` closures `.unwrap()` on `insert_batch` /
`bulk_load` / commit results (a failure is a bench bug, not a measured condition). No
in-band error paths are exercised.

## 7. Testing / validation

- `cargo build --benches` (root) and `cargo build -p compare-benches --benches` compile.
- Smoke each engine: `cargo bench --bench ycsb_bulk_load_bench -- --quick` and the three
  competitor benches likewise, confirming all six arms produce results at all three sizes.
- Run the full `bench/bulk-load/compare` on a real-disk `ULTIMA_BENCH_DIR`, both tiers, and
  record the critcmp table into `task49`.
- Sanity: `ultima_bulk_load` ≤ `ultima_batch` (from_sorted is the ceiling); both UltimaDB
  arms should be dramatically faster than the pre-`insert_mut` batch cost (documented in
  task48 as the ~20× regression the immutable path incurred).

## 8. Scope / non-goals (YAGNI)

- No secondary indexes maintained during load (plain table — matches YCSB).
- No multi-threaded / concurrent bulk load.
- No new record type; reuse `YcsbRecord`.
- Absolute numbers are host-relative; the deliverable is the cross-engine A/B on a quiet
  real-disk host (`make bench/bulk-load/compare ULTIMA_BENCH_DIR=…`). Not wired into
  `make bench` or CI (opt-in tier, like the rest of `compare-benches`).
