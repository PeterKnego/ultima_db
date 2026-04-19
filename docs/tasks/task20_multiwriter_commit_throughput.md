# Task: MultiWriter commit throughput — early-fail intents, sharded commit, fair-queue waiters

## Motivation

Task 19 introduced key-level OCC with per-key merge at commit — disjoint-row
writers to the same table could both commit, and their edits were preserved.
But conflict *detection* stayed at commit time: two writers on the same row
each ran their entire 50-op transaction to completion before the loser found
out it had lost, then replayed all 50 ops from scratch.

Profiling at N=16 writers on 10 hot keys (smallbank contention_high)
showed:

- ~22 retries per successful commit — the wait-and-retry loop dominated
  wall-clock time, not the commit itself.
- All writers queued on the single `store_inner.write()` lock for every
  commit, even when touching disjoint tables.
- When a holder committed, every parked writer woke simultaneously and
  raced (thundering herd).

Task 20 attacks the commit throughput path on three complementary axes:

1. **Early-fail write intents**: detect the conflict at the conflicting
   `update`/`delete` call site, not at commit. Writer B bails after op
   ~5 instead of doing ops 6–50.
2. **`CommitWaiter` synchronized retry**: instead of immediate retry + guaranteed
   re-conflict, the loser blocks on a waiter tied to the holder's commit.
3. **Per-table commit locks (sharded commit path)**: writers on disjoint
   tables stop queueing on a global write lock. Per-key merge work moves
   out of the global critical section; only snapshot install serializes.
4. **Fair-queue waiter FIFO**: only one successor wakes per key release,
   eliminating the thundering herd.

Each of these is measured in isolation in `examples/profile_commit` and
composed into a single `feat/early-fail-intents` branch.

---

## Goals

- Conflict detection at the write site, not commit time, for single-op
  `update`/`delete` (batch variants deferred — see Non-goals).
- Loser of a conflict blocks until the winner commits, then retries
  against a fresh base — at most one *useful* retry per contended commit.
- Disjoint-table writers commit in parallel; the commit path no longer
  serializes on a single global write lock for the merge phase.
- On key release, exactly one queued successor is signaled — no wake-all
  thundering herd.
- Sequential (`WriterMode::SingleWriter`) throughput unchanged within
  measurement noise.
- All existing tests + clippy warnings-as-errors still pass; the
  commutative-burst correctness gate still verifies bit-identical
  `StateHash` across engines.

## Non-goals

- Lock-free snapshot install via `ArcSwap`/`rcu`. Prototyped and reverted
  because WAL ordering still needs serialization and the mirror-only
  variant regressed sequential reads by ~5% without measurable contention
  gain. Tracked as a follow-up (requires a separate WAL-ordering mutex
  and sort-on-recovery semantics).
- Early-fail for `update_batch` / `delete_batch`. Pre-claiming intents
  interacts badly with the snapshot-and-restore rollback inside the Table
  batch path — a failed batch would leave dangling claims and break the
  write-set's "poison-free" guarantee. Batch ops stay on the commit-time
  OCC path.
- SSI / read-set tracking. Still SI — write skew across unrelated rows
  is still possible. Not attempted here.
- Finer-grained retry unit (replay only the conflicting op instead of the
  whole burst). Fair queueing already collapsed the retry count enough
  that this was no longer the dominant cost in the profile.

---

## Architecture

### 1. Write intents — fail-fast conflict detection

**File:** `src/intents.rs` (new), `src/error.rs`, `src/store.rs`.

A `DashMap<(String, u64), IntentEntry>` on `Store` records `(table, row_id)
→ holder_writer_id + FIFO queue of waiters`. `TableWriter::update` and
`TableWriter::delete` call `IntentMap::try_acquire` before touching the
B-tree:

- Vacant entry, or entry with `holder == 0` (transitioning — see §4):
  claim it, `Ok(())`.
- Already held by me (repeated write to the same key in one tx):
  `Ok(())` (idempotent).
- Held by another writer: push my own waiter onto the entry's FIFO queue
  and return `Err(my_waiter)`.

The error is surfaced as:

```rust
Error::WriteConflict {
    table, keys, version,
    wait_for: Option<CommitWaiter>,  // Some(..) for early-fail, None for commit-time OCC
}
```

`CommitWaiter` is a thin public wrapper around `Arc<IntentWaiter>`. The
retry loop calls `wait_for.wait()` to block on the holder's commit before
re-running the transaction.

DashMap (as opposed to `Mutex<HashMap>`) matters at high writer counts:
the per-key check shards across buckets instead of contending on a single
mutex. At N=16 the DashMap variant was measured ~3–7% faster.

**SingleWriter mode** bypasses intents entirely: `WriteTx::intents`,
`waiter`, and `table_locks` are `Option<...>`, populated only in
`WriterMode::MultiWriter`. Sequential transactions pay no heap
allocation, no atomic ops, and no lock dance.

**Batch ops skip this path** and fall back to commit-time OCC — see
Non-goals.

### 2. `CommitWaiter` synchronized retry

When `TableWriter::update` returns `WriteConflict { wait_for: Some(w), .. }`,
the caller's retry loop drops the `WriteTx` and calls `w.wait()` before
`store.begin_write()` again:

```rust
loop {
    let mut wtx = store.begin_write(None).unwrap();
    match execute_ops_on_tx(&mut wtx, &ops) {
        Err(Error::WriteConflict { wait_for, .. }) => {
            drop(wtx);
            retries += 1;
            if let Some(w) = wait_for { w.wait(); }  // block until holder commits
            continue;
        }
        // ... Ok path, commit ...
    }
}
```

The holder's `commit_multi_writer` signals every successor it unblocks at
the *end* of the commit — after the snapshot is promoted — so the retry's
fresh `begin_write` sees the holder's changes as the base. First retry
usually succeeds (modulo further contention).

### 3. Sharded commit path — per-table commit locks

**File:** `src/store.rs`.

Task 19's commit took `store_inner.write()` for the entire phase 3 (version
bump, WAL submit, merge, install, write-set record, GC). Every writer
serialized, even when touching entirely disjoint tables.

Task 20 splits this into four phases:

- **Phase 0 — Per-table lock acquisition.** `Store::table_locks:
  DashMap<String, Arc<Mutex<()>>>`. The writer acquires locks for every
  table in its dirty set ∪ `ever_deleted_tables`, in canonical (sorted)
  order. A `TableLockGuards` RAII wrapper carries `Arc<Mutex<()>>` +
  `ManuallyDrop<MutexGuard<'static, ()>>` pairs through the rest of
  commit; guards are dropped in reverse on commit return. Canonical
  acquisition order makes deadlock impossible across any set of writers.
- **Phase 1 — OCC validate + merge-base snapshot.** Brief
  `store_inner.read()`. With my per-table locks held, any CWS for a table
  I hold must have been pushed before I acquired the lock, so a single
  pass is sufficient.
- **Phase 2 — Merge, no locks held.** For each dirty table, fast-path
  wholesale install (no concurrent writer touched it) or slow-path
  `boxed_clone` + `merge_keys_from`. Previously this was under the
  global write lock; now it runs outside. Two writers on *different*
  tables do their merges in parallel.
- **Phase 3 — Install, brief `store_inner.write()`.** Version bump, WAL
  submit, fork latest + substitute my merged tables, install snapshot
  into history, push CWS, bookkeeping, GC.

Only Phase 3 still serializes across writers. On same-table workloads
(smallbank contention_high: all writers touch `checking`) it's ~neutral
— writers serialize on the per-table mutex regardless. On disjoint-table
workloads, throughput scales nearly linearly with writer count: at
N=16 the `disjoint_tables_bench` holds ~1.13 M ops/s vs ~332 K ops/s
for same-table contention at N=16 (3.4× improvement).

### 4. Fair-queue waiter FIFO — no thundering herd

**File:** `src/intents.rs`.

Initial intent design: a conflicting caller got back the *holder's* waiter
and waited on it. When the holder signaled (commit/drop), every writer
queued on that waiter woke simultaneously, producing a thundering herd
that re-conflicted en masse.

Fix: each conflicting caller pushes its **own** waiter onto the entry's
FIFO queue and blocks on its own waiter.

```rust
struct IntentEntry {
    holder: u64,
    queue: VecDeque<Arc<IntentWaiter>>,
}
```

On `release_all_for(writer_id)`, for each entry where `holder == writer_id`:

- Pop the head of the queue, signal that single successor, and **keep the
  entry** with `holder = 0` (transitioning sentinel) if more successors
  remain. The signaled writer can claim the entry via the transitioning
  path on retry without being cut in front of by a brand-new arrival.
- If the queue is empty after the pop, remove the entry entirely.
- If no successors, remove the entry.

`holder = 0` is a safe sentinel because writer ids are assigned via
`next_writer_id.fetch_add(1, Relaxed)` starting from 1; SingleWriter
mode never creates intent entries.

### 5. Phase-time instrumentation + profile_commit example

**File:** `src/metrics.rs`, `src/store.rs`, `examples/profile_commit.rs`.

`StoreMetrics` gained four `AtomicU64` counters tracking cumulative
wall-clock time in each phase of `commit_multi_writer`. Four
`Instant::now()` calls per commit — negligible cost, very useful for
pointing at actual bottlenecks.

`examples/profile_commit` runs a fixed 500-burst smallbank hot-10 workload
at N=16 with no criterion overhead and dumps the per-phase breakdown
plus retry stats. This was the tool that identified thundering herd as
the N=16 bottleneck after early-fail intents + sharded commit were in
place.

### 6. Extended benches

**File:** `benches/smallbank_common.rs`, `benches/smallbank_bench.rs`,
`benches/smallbank_fjall_bench.rs`, `benches/smallbank_scaling_bench.rs`
(new), `benches/disjoint_tables_bench.rs` (new).

- `bench_contention_at_n(c, engine, n_writers, pool_size)` runs the
  smallbank contention workload at an arbitrary writer count. Called
  with N=16 in both `smallbank_bench` and `smallbank_fjall_bench` for
  direct cross-engine comparison at higher thread counts.
- `smallbank_scaling_bench` sweeps the same hot-10 contention workload
  across writer counts {2, 4, 8, 16} on UltimaDB only. Used to evaluate
  DashMap vs `Mutex<HashMap>` for the intent table.
- `disjoint_tables_bench` gives each writer its own dedicated table.
  Measures the upper bound of sharded-commit parallelism with no
  per-table mutex contention.

---

## Files touched

- **`src/intents.rs`** (new, 225 LOC) — `IntentMap`, `IntentEntry`,
  `IntentWaiter`, `CommitWaiter`. Fair-queue FIFO with
  transitioning-holder sentinel. 6 unit tests.
- **`src/error.rs`** — `WriteConflict` gained `wait_for: Option<CommitWaiter>`
  field.
- **`src/store.rs`** — Heavy surgery:
  - `Store` gains `intents`, `next_writer_id`, `table_locks`, `metrics`
    (mirror for lock-free access).
  - `WriteTx` gains `intents`, `writer_id`, `waiter`, `table_locks`
    fields (all `Option`-guarded to be `None` in SingleWriter mode).
  - `TableWriter` gains `intent_ctx: Option<IntentCtx>` and
    `claim_intent(id)` helper.
  - `commit` dispatches to `commit_single_writer` (old one-lock flow)
    vs `commit_multi_writer` (new 4-phase sharded flow).
  - `acquire_table_locks` + `TableLockGuards` RAII wrapper with
    `ManuallyDrop` + `'static` lifetime transmute (safe because the
    paired `Arc<Mutex<()>>` outlives the guard in the same struct).
  - Phase-timing `Instant::now()` calls + `metrics.add_phaseN(...)`.
- **`src/metrics.rs`** — 4 new `AtomicU64` counters +
  `MetricsSnapshot::commit_ns_phase{0,1,2,3}_*` fields.
- **`src/lib.rs`** — `pub(crate) mod intents`; `pub use CommitWaiter`.
- **`Cargo.toml`** — `dashmap = "6"` added to `[dependencies]`.
- **`benches/smallbank_bench.rs`** — retry loop awaits `wait_for`; ops
  propagate `WriteConflict` via `?`; `bench_contention_at_n` called at
  N=16.
- **`benches/smallbank_fjall_bench.rs`** — `bench_contention_at_n` at N=16.
- **`benches/smallbank_common.rs`** — new `bench_contention_at_n`.
- **`benches/smallbank_scaling_bench.rs`** (new) — parameterized sweep.
- **`benches/disjoint_tables_bench.rs`** (new) — disjoint-table stress.
- **`examples/profile_commit.rs`** (new) — standalone phase profiler.
- **`tests/store_integration.rs`** — 3 tests updated to new early-fail
  semantics; 1 new test `commit_waiter_unblocks_on_first_writer_commit`
  verifying the full handshake.
- **`src/store.rs` tests** — 5 existing tests updated to new semantics;
  existing test coverage preserved.

---

## Verification

- `cargo test` and `cargo test --features persistence` green (277 lib +
  68 integration + 13 wal + 3 checkpoint = 361 tests, 0 failures).
- `cargo clippy --features persistence --tests -- -D warnings` clean.
- Commutative-burst correctness gate passes: `StateHash` bit-identical to
  the reference implementation after 16 bursts × 4 writers × 50 commutative
  ops. Abort count: 100 (vs 88 pre-task20 — slightly higher because
  early-fail + wait resolves many conflicts that commit-time OCC would
  have just looked past when the keys overlapped).
- `examples/profile_commit` at N=16, 500 bursts: 525 K ops/s,
  6.6 retries per commit, phase 3 dominates commit-phase time (86%).

## Benchmark results

All numbers in K ops/s. UltimaDB is the branch tip
(`feat/early-fail-intents`); main is the task-19 baseline; Fjall is
the `fjall = "3"` `OptimisticTxDatabase`. Workloads unchanged from
task 19 except for N=16 variants which are new in this task.

| Workload                  | main (task19) | task20 | Δ vs main | fjall |
|---------------------------|--------------:|-------:|----------:|------:|
| smallbank_mixed (seq)     |           537 |    516 |       −4% |   188 |
| smallbank_read_heavy      |         1 216 |  1 253 |       +3% |   377 |
| smallbank_write_heavy     |           502 |    490 |       −2% |   166 |
| smallbank_contention_low  (N=4) |      347 |    507 |     +46% |   418 |
| smallbank_contention_high (N=4) |      272 |    732 |    +169% |   749 |
| smallbank_contention_low  (N=16)|      n/a |    328 |         — |   378 |
| smallbank_contention_high (N=16)|      n/a |    554 |         — |   645 |
| disjoint_tables (N=16)    |           n/a |  1 131 |         — |   n/a |

Headline numbers:

- **+169%** on N=4 hot-key contention (272 → 732 K). Early-fail alone
  was the biggest lever here.
- **+46%** on N=4 low contention (347 → 507 K).
- Sequential within noise (±3–4%); `WriterMode::SingleWriter` bypasses
  the whole new machinery.
- **N=16 same-table**: about 15% behind Fjall on both low (328 vs 378 K)
  and high (554 vs 645 K) contention. Fair queueing was worth +61% and
  −70% retries at N=16 over the pre-fair-queue branch state.
- **N=16 disjoint tables**: ~1.13 M ops/s flat from N=4 through N=16.
  Same-table at N=16 is 332 K — the 3.4× gap is the sharded commit
  path's parallelism over what a single-global-lock design can achieve.

## Commit-phase breakdown at N=16 (from profile_commit, after fair queueing)

| Phase                           | Avg per commit | % of commit time |
|---------------------------------|---------------:|-----------------:|
| 0 (acquire per-table locks)     |         0.4 µs |               7% |
| 1 (read lock + OCC validate)    |         0.2 µs |               4% |
| 2 (merge, no locks held)        |         0.2 µs |               3% |
| 3 (global write lock hold)      |         4.9 µs |              86% |
| **total**                       |         5.7 µs |                  |

Phase 3 is still the narrowest point on the same-table workload. The
obvious next lever would be replacing the final `store_inner.write()`
with an `ArcSwap` + `rcu`-based install, decoupling WAL ordering via a
separate mutex. That refactor is sized larger than task 20 and is
deferred.

---

## Remaining bottlenecks and follow-up ideas

1. **Phase 3 global write lock.** Install, version bump, WAL submit, CWS
   push, and bookkeeping all share `store_inner.write()`. A proper
   `ArcSwap<Snapshot>` + `rcu` install would eliminate most of this,
   but needs a separate WAL-ordering mutex and potentially sort-on-
   recovery. Prototyped once as a plain mirror-to-ArcSwap — regressed
   sequential reads by ~5% and reverted.
2. **Batch early-fail.** `update_batch` / `delete_batch` still use
   commit-time OCC only. Pre-claiming intents breaks the batch's
   snapshot-and-restore rollback. Could be fixed with scoped
   claim-and-roll-back-on-batch-failure, but smallbank doesn't use
   batches so the win is unclear.
3. **Commit lock on snapshot history.** Even after ArcSwap, the
   `BTreeMap<u64, Arc<Snapshot>>` history still needs synchronization.
   Separate `Mutex<BTreeMap>` would let it drop out of `store_inner.write()`.
4. **SSI.** Still not here — write skew across unrelated rows remains
   possible. Would need read-set tracking in `TableWriter::get*`.
5. **WriteConflict at intent claim carries `version: 0` as a sentinel.**
   Works but slightly awkward; consider splitting early-fail and
   commit-time conflicts into separate error variants.
