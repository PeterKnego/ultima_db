# Task: Serializable isolation as store-level config

## Motivation

UltimaDB through Task 20 provided Snapshot Isolation. SI prevents dirty
reads, nonrepeatable reads, and phantom reads, but allows write skew —
two concurrent transactions can each read an overlapping set of rows,
each write to a disjoint subset, and produce an invariant-violating
combined result that no serial execution could.

For workloads that need true serializability (e.g. classic
"both-doctors-go-off-call", balance-constraint enforcement, conditional
inserts), SI is not enough. Task 21 adds opt-in
`IsolationLevel::Serializable` on `StoreConfig`, with `SnapshotIsolation`
remaining the default so existing callers see no overhead.

The implementation reuses the `committed_write_sets` data structure
that key-level OCC (task 19) and the sharded commit path (task 20)
already maintain — no parallel data structure, no separate validator
thread, no per-key locks beyond what OCC already takes. Read-set
validation runs as one extra pass inside `commit_multi_writer`'s
existing Phase 1.

---

## Goals

- Opt-in serializable isolation that prevents write skew under
  `WriterMode::MultiWriter`.
- Zero overhead for existing callers — `IsolationLevel::SnapshotIsolation`
  is the default and the read-set machinery is allocated only when
  Serializable is selected.
- No overhead for read-only transactions in any mode.
- All existing tests + clippy warnings-as-errors still pass.

## Non-goals

- Range-precise read tracking. v1 conservatively flips a coarse
  `table_scan` flag for any `iter` / `range` / `index_range` / `len` /
  `is_empty` / `first` / `last` / `get_unique` / `get_by_index` /
  `get_by_key` / `custom_index` call; range-key tracking is a v2
  follow-up.
- SSI for `update_batch` / `delete_batch` early-fail. Same as task 20's
  batch limitation — those still rely on commit-time OCC.
- Read-set tracking for `ReadTx`. Read-only transactions cannot
  write-skew, so the cost is unjustified.
- SSI under SMR (`require_explicit_version`). Validation still runs,
  but `committed_write_sets` ordering in SMR depends on the caller's
  version assignment; first-match-by-vector-position semantics are
  best-effort there.

---

## Design summary

- `StoreConfig::isolation_level: IsolationLevel { SnapshotIsolation, Serializable }`. Default `SnapshotIsolation`.
- `WriteTx::read_set: Option<RefCell<BTreeMap<String, ReadSetEntry>>>` — `None` in SI, `Some(_)` in Serializable. Tracks the keys/scans this writer has performed.
- `TableWriter` read methods (`get`, `iter`, `range`, `len`, `index_range`, `get_unique`, …) call into two private helpers, `record_point_read` and `record_table_scan`, that populate the parent WriteTx's read_set when present.
- `validate_read_set` runs in `commit_multi_writer`'s Phase 1, after `validate_write_set`. Walks the same `committed_write_sets` data structure that OCC already maintains, applying three conflict criteria.
- `Error::SerializationFailure { table, version }` is the new failure mode. Distinct from `WriteConflict` because there is no concurrent writer left to wait on — the conflicting committer has already finished, and the retry must rebase against a fresh base.
- `commit_single_writer` does not call `validate_read_set` — SingleWriter has no concurrent writers, so SSI is a no-op there.

---

## Architecture

### 1. The `IsolationLevel` enum and `StoreConfig` knob

**File:** `src/store.rs`.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    SnapshotIsolation,
    Serializable,
}
```

`StoreConfig` gains `pub isolation_level: IsolationLevel` defaulting to
`SnapshotIsolation`. `Default for StoreConfig` is the only change to
existing-code surface area; all existing callers keep their behavior
without modification.

The enum lives at the top of `src/store.rs` (next to `WriterMode`) and
is re-exported from `lib.rs` alongside `Store`, `StoreConfig`, and
`WriterMode`.

### 2. Read-set storage on `WriteTx`

```rust
struct ReadSetEntry {
    keys: BTreeSet<u64>,
    table_scan: bool,
}

pub struct WriteTx {
    // ... existing fields ...
    read_set: Option<RefCell<BTreeMap<String, ReadSetEntry>>>,
    isolation_level: IsolationLevel,
}
```

`WriteTx::new` allocates the `Some(RefCell::new(BTreeMap::new()))` only
when `inner.config.isolation_level == Serializable`; otherwise the
field is `None` and every later `record_*` call short-circuits at the
first `Option::as_ref()?`.

The interior-mutability choice (`RefCell`) is forced by the API:
`TableWriter`'s read methods take `&self`, and `WriteTx` is `!Send +
!Sync`, so a `Mutex` would be wasteful. A `RefCell` is borrow-checked
at runtime; the borrow scopes inside `record_point_read` and
`record_table_scan` are short and non-reentrant, so a panic is
impossible in normal use.

### 3. Recording reads from `TableWriter`

**File:** `src/store.rs` (lines ~1060–1240).

`TableWriter` exposes the same read API as `TableReader` (`get`,
`contains`, `iter`, `range`, `len`, `is_empty`, `first`, `last`,
`get_many`, `resolve`, `get_unique`, `get_by_index`, `get_by_key`,
`index_range`, `custom_index`). Each method now begins with a single
call to one of:

```rust
fn record_point_read(
    rs: Option<&RefCell<BTreeMap<String, ReadSetEntry>>>,
    table: &str,
    id: u64,
);

fn record_table_scan(
    rs: Option<&RefCell<BTreeMap<String, ReadSetEntry>>>,
    table: &str,
);
```

Both are no-ops if `rs.is_none()`. In Serializable mode they
`borrow_mut()` the read_set, look up (or insert default) the entry for
`table`, and either insert into `keys` (point read) or set `table_scan
= true` (scan).

`TableWriter` carries the read_set as `Option<&'tx RefCell<...>>` so it
borrows from the parent `WriteTx` rather than owning a clone. The
plumbing is one extra struct field passed through
`WriteTx::open_table`.

`get_many` and `resolve` allocate `String::from(table)` once per
recorded id — N allocations for an N-id batch. A batch helper that
records one entry-with-N-keys would collapse this; deferred until a
profile shows it (smallbank doesn't use `get_many`).

### 4. Validation at commit

**File:** `src/store.rs`, `validate_read_set` ~ line 1899.

`commit_multi_writer`'s Phase 1 already takes a brief `inner.read()`
and runs `validate_write_set`. SSI adds a second validator immediately
after, gated on `matches!(self.isolation_level, IsolationLevel::Serializable)`:

```rust
if matches!(self.isolation_level, IsolationLevel::Serializable)
    && let Some(conflict) = self.validate_read_set(&inner)
{
    self.metrics.inc_serialization_failure();
    drop(inner);
    return Err(conflict);
}
```

`validate_read_set` walks the same `committed_write_sets` vector that
`validate_write_set` walks, skipping `cws.version <= base_version`. For
each later-than-base CWS, three conflict criteria apply per `(table,
entry)` in the read set:

1. `cws.deleted_tables.contains(table)` → conflict.
2. `entry.table_scan && cws.tables.contains_key(table)` → conflict.
3. `!entry.table_scan` and `entry.keys ∩ cws.tables[table] != ∅` →
   conflict.

First-match wins. The let-chain syntax matches `validate_write_set`'s
style for consistency.

`commit_single_writer` does not call `validate_read_set`. SingleWriter
has no concurrent writers, no `committed_write_sets` to walk against,
and the read-set field exists only because `WriteTx::new` allocates it
based on `isolation_level` rather than `writer_mode`.

### 5. The `SerializationFailure` error variant

**File:** `src/error.rs`.

```rust
#[error("serialization failure on table '{table}' (conflicting version {version})")]
SerializationFailure {
    table: String,
    version: u64,
},
```

Distinct from `WriteConflict` because there is no `wait_for`. The
conflicting committer has already finished — there is nothing to wait
on. Retry must rebase: drop the `WriteTx`, call `begin_write` again,
and replay against the fresh base.

A `serialization_failures` counter was added to `StoreMetrics` for
observability; integration tests assert it increments only in SSI
mode.

---

## Implementation notes

The `Option<RefCell<BTreeMap<...>>>` shape was chosen over a sentinel
empty map because the conditional allocation is the cost-of-feature
boundary: SI callers must pay nothing, so the read_set must be
*absent*, not just empty. Making it `Option` also makes the SI fast
path zero-cost statically — `record_point_read` and
`record_table_scan` short-circuit on the first `as_ref()?` and the
optimizer elides the entire body. `RefCell` (rather than `Mutex`) is
correct because `WriteTx` is `!Send + !Sync` (`PhantomData<*const ()>`
marker); the borrow-checker enforcement is dynamic but the borrow
windows are short and non-reentrant, so panics from re-borrow are not
a real concern.

Range/scan reads are tracked at coarse table-scan granularity rather
than range-precise. The reason is twofold: first, the storage and
validation cost of a precise per-range structure (interval tree,
sorted intervals, etc.) is meaningful, and v1 wanted to ship a
correctness-complete feature with measurable overhead. Second, the
false-positive failure mode of coarse tracking is *retry*, which is
exactly the failure mode for OCC write conflicts — callers that
already handle `WriteConflict` retry will handle
`SerializationFailure` retry the same way. False negatives (missed
conflicts) would be a correctness bug; the coarse flag is
conservatively correct (any write to the read table is treated as
overlapping any range on it).

Reusing `committed_write_sets` rather than introducing a parallel
"read-set registry of in-flight writers" keeps the design dramatically
simpler. The CWS already records, for every committed transaction,
which keys it modified — exactly the information a read-set validator
needs. The price is that validation only catches conflicts after the
other writer has *committed*, not while both are in-flight; for SSI
this is fine because the abort decision is local to the committing
writer's read set, not a graph traversal across in-flight transactions
(which is what the canonical SSI paper algorithm does for cycle
detection). UltimaDB's variant is closer to "first-committer wins on
read-write overlap" than to cycle-aware SSI, but the practical
guarantee (no write skew) is the same for the workloads we care about.

The let-chain syntax in `validate_read_set`
(`if matches!(...) && let Some(conflict) = ...`) matches
`validate_write_set`'s style. Rust 2024 edition is required (already
the project's edition).

---

## v1 limitations / future work

- **Range-precise read tracking.** Any `iter` / `range` / `index_range`
  / `len` / `is_empty` / `first` / `last` / `get_unique` /
  `get_by_index` / `get_by_key` / `custom_index` flips a coarse
  `table_scan` flag. Any concurrent commit on the read table is
  treated as a conflict — false positives possible on read-heavy scan
  workloads, but no false negatives. Range-key tracking (interval
  storage + interval-overlap test) is a v2 follow-up.
- **`get_many` / `resolve` allocate per id.** `record_point_read`
  takes `&str` and constructs the entry key via `String::from(table)`
  per call; for batch reads that's N allocations. A batch helper
  recording one entry with N keys would collapse to one. Defer until a
  profile shows it.
- **First-match validation by `committed_write_sets` vector position,
  not by smallest invalidating version.** In MultiWriter the vector is
  naturally version-ordered (commit-version bump under the per-table
  lock), so first-match equals earliest version. SMR mode (explicit
  versions) breaks this; not a real concern because SMR + Serializable
  is unusual.
- **No SSI for `update_batch` / `delete_batch` early-fail.** Those
  still rely on commit-time OCC. Same as task 20's batch limitation.
- **No support for serializable read-only transactions.** `ReadTx`
  doesn't track reads at all. Read-only transactions can't write-skew,
  so this is fine for the SSI guarantee, but a "snapshot isolation
  read with a stable view across a longer multi-step computation"
  workflow that wants a serializable-equivalent guarantee against
  in-flight committers would need a different mechanism. User-facing
  guidance on the read-on-WriteTx vs read-on-ReadTx pattern (and the
  foot-gun of doing the conditional read on a separate `ReadTx` before
  the `WriteTx` that acts on it) lives in
  `docs/isolation-levels.md` § "Using SSI correctly".

---

## Tests

- **6 unit tests in `src/store.rs`** covering read-set recording:
  - `ssi_read_set_records_point_reads` — `get` populates `keys`.
  - `ssi_read_set_records_iter_as_table_scan` — `iter` flips
    `table_scan`.
  - `ssi_read_set_empty_in_si_mode` — `read_set` is `None` in SI.
  - `ssi_read_set_records_get_many_per_id` — batch read pins each id.
  - `ssi_read_set_records_missing_key_get` — missing-key `get` still
    records the read attempt.
  - `ssi_read_set_mixed_point_and_scan` — both granularities coexist
    on one table.
- **5 integration tests in `tests/store_integration.rs`:**
  - `ssi_prevents_write_skew_via_table_scan` — table_scan
    invalidation under SSI.
  - `si_allows_write_skew_table_scan` — SI permits write skew
    (control case).
  - `ssi_read_then_write_conflicts_on_concurrent_modify` — point-read
    invalidation under SSI.
  - `ssi_disjoint_point_reads_dont_conflict` — disjoint point
    read+write commit cleanly.
  - `ssi_in_single_writer_mode_is_noop` — SingleWriter+SSI no-op
    contract; metric counter stays 0.

The existing test suite continues to pass with no behavioral change.

---

## Cost

- **`IsolationLevel::SnapshotIsolation` (default):** zero overhead.
  `Option` is `None`, every `record_*` call short-circuits at
  `as_ref()?`, the `validate_read_set` branch is elided.
- **`IsolationLevel::Serializable` + `WriterMode::SingleWriter`:** zero
  overhead. Both validation AND tracking are skipped — `begin_write`
  gates the `read_set` allocation on `(Serializable, MultiWriter)`, so
  SingleWriter+SSI yields `read_set: None` and every `record_*` call
  short-circuits at `as_ref()?` exactly like SI. `commit_single_writer`
  also skips `validate_read_set`. Net cost: nothing.
- **`IsolationLevel::Serializable` + `WriterMode::MultiWriter`:** one
  `BTreeSet::insert` per point read, one `bool` set per scan, plus
  one `committed_write_sets` walk at commit. Measured on smallbank
  N=16, hot-keys=10, 500 bursts × 50 ops/writer (`examples/ssi_cost.rs`):
  SI ≈10.4–10.8k commits/s, SSI ≈10.3–10.5k commits/s, slowdown
  fluctuates run-to-run between roughly **−0.4% and +4.9%, mean
  ~1–2%** — i.e. within noise of the SI baseline. Retry ratios are
  also indistinguishable (SI 6.5–6.9, SSI 6.5–6.8), so SSI is *not*
  forcing additional aborts on this workload; the cost is just the
  per-read `BTreeSet::insert` and one `committed_write_sets` walk
  per commit. Read-heavy YCSB-B has no measurement yet — likely
  higher relative overhead because the per-point-read tracking cost
  is amortized over fewer mutations; left as TBD.
- **`ReadTx` (always):** zero overhead. Read-only transactions cannot
  write-skew, so SSI tracking is not applied.

---

## Files touched

- `src/store.rs` — `IsolationLevel` enum, `StoreConfig::isolation_level`,
  `ReadSetEntry`, `WriteTx::read_set`, `WriteTx::isolation_level`,
  `TableWriter` read instrumentation, `record_point_read` /
  `record_table_scan` helpers, `validate_read_set`, Phase 1
  validator branch, 6 unit tests.
- `src/error.rs` — `Error::SerializationFailure` variant + unit test
  for its `Display` impl.
- `src/lib.rs` — `pub use IsolationLevel`.
- `src/metrics.rs` — `serialization_failures` counter +
  `MetricsSnapshot::serialization_failures`.
- `tests/store_integration.rs` — 5 new SSI integration tests.
