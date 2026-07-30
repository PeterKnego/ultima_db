# Task 55: `Send` audit for `WriteTx` / `ReadTx`

## Motivation

Since [task18](task18_concurrent_writes.md) both transaction types carried a
`_not_send: PhantomData<*const ()>` field, making them `!Send + !Sync`. The
stated reason (task18, "`WriteTx` / `ReadTx` are `!Send`"; `ARCHITECTURE.md`
trade-off table) was that "a transaction is not designed to split work across
threads; pinning to the creating thread prevents a footgun" — a design
preference, not a documented correctness requirement.

That preference has a real cost. A large share of the crate's target audience
is on tokio, and a transaction that cannot be held across an `.await` is a
hard barrier: the only shape available was "open, use, and commit entirely
inside one `spawn_blocking` closure". [task53](task53_version_pin_gc.md)
already had to add `VersionPin` specifically because `ReadTx` was `!Send` and
so could not carry a snapshot to a serializer thread.

Compounding it, the marker was **unverified and mis-documented**. The comment
at the bottom of `src/store.rs` claimed:

```rust
// ... that's verified by a trybuild-style negative test in
// `tests/store_integration.rs`.
```

No such test existed. There is no `trybuild` dev-dependency, no `.stderr`
fixture, and no `compile_fail` anywhere in the repo — a grep for `not_send`,
`compile_fail`, and `trybuild` across `src/`, `tests/`, and `examples/`
returned only that comment. The crate's most load-bearing-looking thread-safety
claim was a comment nobody had ever checked.

This task audits whether the marker is load-bearing.

## What was audited

The four questions, with evidence.

### 1. What non-`Send` data does each struct actually hold?

**None.**

`ReadTx` (`src/store.rs:1845-1848`) holds exactly two fields:
`snapshot: Arc<Snapshot>` and `metrics: Arc<StoreMetrics>`. Both are already
required to be `Send + Sync` — `StoreInner.snapshots` is a
`BTreeMap<u64, Arc<Snapshot>>` (`src/store.rs:340`) reachable from
`Store: Send + Sync`, which the crate asserts at compile time.

`WriteTx` (`src/store.rs:1960-2028`) holds `Arc<Snapshot>`,
`BTreeMap<String, DirtyEntry>` (whose `table` is `Box<dyn MergeableTable>`,
and `MergeableTable: Any + Send + Sync`), `Arc<RwLock<StoreInner>>`,
`Arc<StoreMetrics>`, `Option<Arc<IntentMap>>`, `Option<Arc<IntentWaiter>>`,
`Option<Arc<TableLockTable>>`, plain `BTreeMap`/`BTreeSet`/`u64`/`bool`
fields, and three `RefCell`s:

- `ddl_tables: RefCell<BTreeSet<String>>` (`src/store.rs:1986`)
- `read_set: Option<RefCell<BTreeMap<String, ReadSetEntry>>>` (`src/store.rs:2025`)
- `wal_ops: RefCell<Vec<WalOp>>` (`src/store.rs:2016`, persistence only)

`RefCell<T>` is `Send` when `T: Send`; it is only `!Sync`. So the `RefCell`s
force `!Sync` — which is exactly what the borrow-flag reasoning in task21 and
task54 relies on — and contribute nothing to `!Send`.

Verified empirically rather than by inspection: with the two `PhantomData`
fields deleted, `assert_send::<WriteTx>()` and `assert_send::<ReadTx>()`
compile under both the default and the `persistence` feature. A probe
asserting `Sync` fails for `WriteTx` on all three `RefCell` fields and
succeeds for `ReadTx`, confirming the split is `Send + !Sync` / `Send + Sync`.

Notably, no lock guard is ever stored in a transaction. `parking_lot` is built
without the `send_guard` feature (`Cargo.toml:59` enables only `arc_lock`), so
its guards are `!Send` and would have shown up here. `TableLockGuards`
(`src/store.rs:1705-1709`) holds `Vec<ArcMutexGuard<RawMutex, ()>>`, but it is
a local inside `commit_multi_writer` (`src/store.rs:3288`:
`let _table_guards = self.acquire_table_locks();`), acquired and dropped
inside one synchronous call — never a `WriteTx` field.

### 2. Does the commit path assume the committing thread is the thread that called `begin_write`?

**No.** Every piece of writer identity is a value, not a thread.

- The writer's identity in the intent table is `writer_id`, drawn from
  `self.next_writer_id.fetch_add(1, Ordering::Relaxed)` at
  `src/store.rs:694` — a store-wide atomic counter.
  `IntentMap::try_acquire` compares `entry.holder == writer_id`
  (`src/intents.rs:186`) and `release_all_for` is keyed the same way
  (`src/intents.rs:221-235`). No thread identity is recorded or compared.
- Blocking on a conflicting writer goes through `IntentWaiter`
  (`src/intents.rs:59-104`): a `Mutex<bool>` plus a `Condvar`. Any thread may
  signal it and any thread may wait on it.
- `PromoteGate` (`src/store.rs:294-332`) is a `Mutex<u64>` turn counter plus a
  `Condvar`. A committing writer's ticket is a `u64` taken at WAL-submission
  time and consumed by `wait_turn` / `advance` — both take the ticket as an
  argument. The ticket lives in a local for the duration of one `commit()`
  call; it is not stored on `WriteTx` and is never associated with a thread.
- The store bookkeeping a commit mutates — `active_writer_count`,
  `active_writer_base_versions`, `committed_write_sets`,
  `last_submitted_version` (`src/store.rs:340-358`) — is all counters and
  version numbers behind `RwLock<StoreInner>`.
- `begin_write` (`src/store.rs:641-729`) records nothing thread-specific:
  it bumps `active_writer_count`, pushes `base_version`, and hands the
  transaction its `writer_id` and `waiter`.
- `Drop for WriteTx` (`src/store.rs:3820-3837`) takes `store_inner.write()`
  and calls `intents.release_all_for(self.writer_id, waiter)` — correct from
  any thread.

The only genuine affinity is *inside* `commit()`: it acquires per-table
guards, parks on the promote gate, and (under `Durability::Consistent`)
parks on the WAL fsync (`src/store.rs:3214-3236`) — all within one
synchronous call on one thread, which is automatic for a `fn` that takes
`self`.

### 3. Does any thread-local or thread-id-keyed state exist on the write path?

**No.** `grep -rn "thread_local!\|thread::current\|ThreadId" src/` returns
zero hits across the entire crate.

The `unsafe` blocks in `src/table.rs` (275, 279, 306, 310, 343, 349, 358, 361,
474, 529, 610, 613) are the documented raw-pointer trick for mutating index
entries while iterating `self.indexes`; each lives inside a single `&mut self`
method call and stores nothing. The `unsafe` in `src/wal.rs` (861, 879, 984)
is `memmap2` mapping, owned by the WAL, not by a transaction.

### 4. Would `WriteTx: Send` allow a program that is currently impossible and unsound?

**No.** `Send` means "safe to transfer ownership to another thread". Every
field is `Send` (Q1), no invariant is keyed to a thread (Q2, Q3), and there is
no `unsafe` relying on thread affinity. `WriteTx` remains `!Sync`, so the
compiler still forbids *sharing* it — two threads can never hold `&WriteTx`
concurrently, which is what the `RefCell` borrow flags need. Moving requires
ownership, so no `TableWriter`/`TableReader` borrow can be outstanding across
the move.

What `Send` does enable is a class of programs that are **safe but unwise**,
and those are documented rather than prevented — see Consequences.

## Verdict: A — removable

The `_not_send: PhantomData<*const ()>` markers are removed from both
`ReadTx` and `WriteTx`. The resulting bounds are:

| Type | Before | After |
|---|---|---|
| `Store` | `Send + Sync` | `Send + Sync` (unchanged) |
| `VersionPin` | `Send + Sync` | `Send + Sync` (unchanged) |
| `ReadTx` | `!Send + !Sync` | `Send + Sync` |
| `WriteTx` | `!Send + !Sync` | `Send`, still `!Sync` |

This is an additive change: gaining an auto-trait impl is semver-compatible,
so it ships in a minor release. The converse is not true — *removing* `Send`
later would be breaking. That is the real cost of this decision, and it is why
`tests/send_bounds.rs` exists and carries a comment telling the next person
not to "fix" a failure by deleting the assertion.

Verdict C ("load-bearing") was the expected outcome going in and was not
supportable: the only mechanism the marker protected was the programmer, not
an invariant. Verdict B ("removable under a restriction") was considered and
rejected because the hazards below are not restrictions that `Send` creates —
they apply equally to a transaction that never leaves its thread. Blocking a
tokio worker in `commit()` is already possible today without moving anything.

## Consequences

**What is now legal.** A `WriteTx` can be opened on one thread and committed
on another, and can be held across an `.await` in an async task. A `ReadTx`
can be moved to another thread, and — being `Sync` — shared by reference for
parallel scans of one snapshot.

**Two hazards survive the type system.** They are documented on `WriteTx`
itself (`src/store.rs`, the struct doc comment) because the compiler no longer
enforces them:

1. **An open transaction holds resources.** In `WriterMode::SingleWriter` an
   open `WriteTx` holds the only writer slot: every other `begin_write`
   returns `Error::WriterBusy` (`src/store.rs:651-658`). In
   `WriterMode::MultiWriter` it holds its intents, so conflicting writers park
   on its `IntentWaiter`, and its `base_version` stays in
   `active_writer_base_versions`, which blocks `prune_write_sets` from
   trimming the committed-write-set log (`src/store.rs:1743`). Parking a
   transaction on a long `.await` therefore stalls other writers and grows
   memory. `intents.rs`'s "drop your `WriteTx` before waiting" convention is
   the same warning in its synchronous form.
2. **`commit()` blocks.** It takes `RwLock<StoreInner>`, may park on the
   `PromoteGate`, and under `Durability::Consistent` parks until the WAL
   background thread fsyncs (`src/store.rs:3214-3236`); under
   `ConsistentInline` the calling thread performs the fsync itself. None of
   that is async-aware. **On an async runtime, `commit()` belongs in
   `spawn_blocking`.** So does `begin_write` on a contended store — it takes
   the store write lock.

The recommended pattern is unchanged: clone the `Store` into each thread and
open transactions locally. `Send` removes a compile-time barrier; it is not
an invitation to move transactions around.

**`VersionPin` is not obsoleted.** With `ReadTx: Send`, the
[task53](task53_version_pin_gc.md) SMR handoff could send a `ReadTx` directly.
`VersionPin` remains preferable there: it is a bare `Arc<Snapshot>` handle
with no table-map access or metrics registration, it is `Clone`, and it makes
"I am holding this version alive" explicit at the call site rather than a side
effect of holding a read view.

## Testing

`tests/send_bounds.rs` (new) is the contract:

- `public_types_have_expected_thread_bounds` — `Store` and `VersionPin` are
  `Send + Sync`. This is the pre-existing baseline the audit must not
  regress; it passed before any change was made.
- `transactions_are_send` — `WriteTx: Send`, `ReadTx: Send + Sync`.
- `write_tx_commits_on_a_different_thread` — a runtime proof, not just a
  bound: a transaction is opened and written on the main thread, moved into a
  `std::thread::spawn`, committed there, and the resulting version is then
  read back through a `ReadTx` moved to a *third* thread.

`src/store.rs`'s in-crate `#[cfg(test)] const fn _assert_thread_bounds()`
(formerly `_assert_store_is_thread_safe`, whose comment carried the false
trybuild claim) now asserts all three types, so a thread-affine field added to
a transaction fails the unit-test build as well as the integration test.

Verification run: `cargo test`, `cargo test --features persistence`,
`cargo test -p ultima-vector`, and
`cargo clippy --all-targets --features persistence -- -D warnings`.

## Notes

Docs updated for the reversal: `docs/ARCHITECTURE.md` (the `ReadTx`/`WriteTx`
struct listings, the writer-modes thread-safety paragraph, and the trade-off
table row), `docs/isolation-levels.md`, `CLAUDE.md`, and a "superseded" banner
on [task18](task18_concurrent_writes.md). Task docs 21, 23, and 24 still say
"`!Send + !Sync`" in passing; they are historical records, and in every case
the reasoning depends only on the `!Sync` half, which is unchanged.
