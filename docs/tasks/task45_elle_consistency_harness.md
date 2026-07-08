# Task 45: Elle Consistency Harness

> Renumbered from task41 (numbering collided with the index-DDL conflict fix,
> which is the canonical task41); the Elle harness is task45.


Black-box verification of UltimaDB's transactional isolation claims using
[Elle](https://github.com/jepsen-io/elle) (Jepsen's transactional-safety
checker) on list-append histories, wired up as an opt-in conformance tier:

```bash
make consistency/elle          # needs java (repo toolchain: Temurin 21)
```

> **Extended in task47** (`docs/tasks/task47_elle_anomaly_and_mutation.md`): the
> checks now assert the exact anomaly set (SI ⊆ `{G2-item}`, SSI = ∅), and
> `make consistency/elle-mutation` injects commit-path bugs to prove the harness
> catches them.

Design history: `docs/superpowers/specs/2026-07-07-elle-consistency-harness-design.md`,
`docs/superpowers/plans/2026-07-07-elle-consistency-harness.md`.

## What is checked

`make consistency/elle` runs **two passes**, each generating one history per
isolation level (MultiWriter, `Persistence::None`, default workload: 8 threads
× 1500 txns × 4 ops on 16 keys):

- **Point pass** (`--scan-ratio 0`): every read is a point `get`, so SSI
  validates the *per-key* read set.
- **Scan pass** (`--scan-ratio 0.5`, `ELLE_SCAN_RATIO`): half the transactions
  read the whole table via `range(..)`, which registers SSI's *coarse
  `table_scan`* read set (a scan conflicts with any concurrent write to the
  table). This exercises the `table_scan` branch of `validate_read_set` that
  the point pass never touches.

Each pass runs the same three assertions plus a self-test:

| # | History | Elle model | Expectation |
|---|---------|-----------|-------------|
| 0 | `tools/elle-cli/fixtures/known_bad.edn` | serializable / snapshot-isolation | rejected / accepted (checker self-test) |
| 1 | `IsolationLevel::SnapshotIsolation` | `snapshot-isolation` | **valid** — hard fail otherwise (SI claim broken) |
| 2 | `IsolationLevel::SnapshotIsolation` | `serializable` | **invalid** — write skew (G2-item) is legal under SI; a `true` verdict prints a WARN (workload not contended enough), never a failure, because anomaly occurrence is probabilistic |
| 3 | `IsolationLevel::Serializable` | `serializable` | **valid** — hard fail otherwise (SSI claim broken) |

Check #2 doubles as an end-to-end detector test on real data: it proves the
harness generates genuine rw-antidependency cycles and that the checker finds
them. On this workload it fires reliably (thousands of overlapping commits).

The fixture in check #0 is a hand-written write-skew history (two transactions
each read a key the other appends to, plus a final read observing both
commits). It is invalid under `serializable` but valid under
`snapshot-isolation`, so it exercises exactly the model distinction the real
verdicts rely on. It guards against the false-confidence failure mode where a
misconfigured checker silently passes everything.

## How the workload maps onto UltimaDB

- **Pre-seeded keyspace.** `TableWriter` has no public insert-at-specific-key
  and `update()` requires the key to exist, so a setup transaction
  `insert_batch`es `--keys` empty rows and the returned auto-ids become the
  Elle key universe. The workload itself is pure `get` + `update`.
- **Append = read-modify-write.** `[:append k v]` is `get(k)` → clone `Vec` →
  push → `update(k, ..)`. The `get` is also what registers the SSI point read
  (`update` alone records no read), so appends participate in read-set
  validation exactly like explicit reads.
- **Scan mode** (`--scan-ratio`): a scan transaction reads the whole table once
  via `range(..)` (registering the coarse `table_scan` read), then serves every
  read *and* append from a per-transaction working copy. Serving from that copy
  is essential: a read after an append in the same transaction must observe its
  own write, or the history would show a false anomaly. The unit test
  `scan_txn_reads_its_own_appends` pins this invariant. Because coarse tracking
  taints the whole table regardless of the range bounds, scan transactions
  abort far more often under SSI (that is the path being stressed), but the
  committed history must still be serializable.
- **Globally unique append values** from one `AtomicU64` — Elle's list-append
  inference requires each value be appended at most once per key; uniqueness
  across the whole run satisfies it trivially, including across retries.
- **`:fail` is used only for `Error::WriteConflict` and
  `Error::SerializationFailure`** — both guarantee the transaction did not
  commit, which is what `:fail` ("definitely did not happen") means to Elle.
  A failed attempt is *not* retried as the same operation: the thread simply
  generates a fresh transaction with fresh values under its own `:invoke`.
  On `WriteConflict { wait_for: Some(w) }` the thread parks on the
  `CommitWaiter` before its next attempt (livelock avoidance, same pattern as
  `autobench/tests/mw_commit_torture.rs`).
- **Any other error becomes `:info` and retires the process** (a Jepsen
  process may not issue ops after an indeterminate outcome); the run is then
  reported degraded and exits nonzero. With `Persistence::None` this should
  never happen.
- **History recording** is a single `Mutex<Vec<Event>>` appended at invoke and
  completion, so event order is real-time order by construction; `:index` is
  the vector position. Events are written as EDN, one map per line:
  `{:index N, :type :invoke|:ok|:fail|:info, :f :txn, :process P, :time NS,
  :value [[:append K V] [:r K nil-or-[V ...]]]}`.

## Predicate reads (index pass)

A third read shape exercises UltimaDB's secondary-index read path. Each row
carries a static `bucket` field (assigned `id_index % --buckets` at seed, never
mutated) with a non-unique index. A predicate transaction (probability
`--predicate-ratio`) selects a key group via the index — `get_by_index(bucket)`
for equality or `index_range(lo..=hi)` for a range — then reads/appends within
that group, serving read-your-writes from a local copy exactly like the scan
pass. The index read is invisible to the emitted history (like the scan), so the
EDN stays valid `list-append`.

Two properties are checked:

1. **SSI conflict tracking.** Every index read registers the coarse `table_scan`
   read (`src/store.rs`), so a predicate reader is serialization-failed by any
   concurrent write to the table — phantoms are prevented. The predicate pass is
   therefore clean under SSI, exactly like the scan pass.
2. **Index integrity under concurrency.** Each predicate transaction asserts the
   index returned exactly the statically-known bucket membership; a mismatch
   (an index-maintenance bug under concurrent `update`s) retires the process and
   fails the run.

Because index reads degrade to `table_scan`, a predicate read has the **same
conflict profile as a full scan**: SSI clean, SI shows `{G2-item}` write skew and
nothing worse. So the predicate pass reuses the existing whitelist — **no new
anomaly type is needed.** (This resolves the task45/47 note that a predicate
workload "could need its own whitelist": for the index-read shape it does not.)

`make consistency/elle` runs a predicate pass after the point and scan passes,
tuned by `ELLE_PREDICATE_RATIO` (default 0.5) and `ELLE_BUCKETS` (default 4).

## Components

- `autobench/src/bin/elle-history.rs` — workload driver
  (`cargo run --release -p ultima-autobench --bin elle-history -- --help`).
  No new dependencies: inline SplitMix64 PRNG (seeded `--seed` + thread index,
  fully deterministic op generation), hand-formatted EDN. Unit tests cover the
  EDN encoding, PRNG determinism, and generator bounds/uniqueness.
- `tools/elle-cli/` — vendored [elle-cli](https://github.com/ligurio/elle-cli)
  0.1.9 standalone jar (EPL-2.0, sha256 in its README) + the known-bad fixture.
- `scripts/elle_check.sh` — runs the four checks above. elle-cli exits nonzero
  on a `false` verdict, so the script parses the stdout verdict
  (`<file>\t<true|false|unknown>`) instead of trusting exit codes; `unknown`
  (cycle-search timeout / OOM) is always a hard failure.
- Makefile `consistency/elle` — builds, generates the four histories (point +
  scan × SI + SSI) into `$(ELLE_DIR)` (default `/tmp/ultima-elle`; cargo's
  target dir may be redirected machine-wide, so histories don't go under
  `target/`), and runs `elle_check.sh` once per pass. The scan fraction is
  `ELLE_SCAN_RATIO` (default 0.5); extra driver flags via `ELLE_ARGS` (e.g.
  `make consistency/elle ELLE_ARGS="--threads 16 --txns-per-thread 5000"`).
- `.github/workflows/consistency.yml` — CI. The `elle` job **gates every PR**
  with a bounded run (`ELLE_ARGS="--threads 8 --keys 8 --txns-per-thread 800"`,
  small enough that elle-cli's cycle-search stays clear of an `unknown` verdict,
  contended enough that SI still shows write skew). A weekly `elle-deep` job
  re-runs the canonical sizing plus `make consistency/elle-mutation` (task47);
  `workflow_dispatch` runs both. java is provisioned per-job (Temurin 21); jq
  ships on the runner; the elle-cli jar is vendored so nothing is downloaded.

## Reading a failure

A `FAIL` on check #1 or #3 means Elle found a dependency cycle (or aborted
read) that the claimed model forbids — a real isolation bug, reproducible via
the printed seed (default 42). Re-run elle-cli by hand with `--directory out/`
to get per-anomaly explanations and SVG cycle plots:

```bash
java -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar --model list-append \
    --consistency-models serializable --directory out/ /tmp/ultima-elle/point-ser/history.edn
```

Histories live under `$(ELLE_DIR)` (default `/tmp/ultima-elle`) in
`point-si/`, `point-ser/`, `scan-si/`, `scan-ser/`.

## Results (2026-07-07, defaults)

All checks pass on 24,000-event histories per level. Both the SI histories are
confirmed non-serializable (write skew present) and both SSI histories are
serializable.

- **Point pass** — SI: ~6386 ok / ~5614 write-conflicts / 0 serialization
  failures; SSI: ~6026 ok / ~4901 write-conflicts / ~1073 serialization
  failures.
- **Scan pass** (`--scan-ratio 0.5`, ~5928 scan txns/level) — SI: ~6358 ok /
  ~5642 write-conflicts; SSI: ~4727 ok / ~5015 write-conflicts / ~2258
  serialization failures. The coarse `table_scan` read set roughly doubles the
  SSI abort count versus the point pass, confirming that branch is exercised;
  the surviving history is still serializable.

## Known limitations (v1)

- **In-memory only** (`Persistence::None`): exercises the OCC/SSI commit path,
  not recovery. Crash-durability checking (LazyFS-style fault injection
  against the WAL) is a separate future task.
- **Coarse read-set tracking**: the scan pass registers `table_scan` via
  `range(..)`, and secondary-index reads ARE now exercised by the predicate pass
  (see "Predicate reads" above) — but both taint the whole table under v1 coarse
  tracking, so *fine-grained* sub-range / per-predicate read-set tracking is not
  exercised (there is none to exercise: index and sub-range reads all degrade to
  `table_scan`).
- **Histories, not final state**: Elle checks the recorded history; the frozen
  `autobench/tests/mw_commit_torture.rs` floor already covers final-state
  lost-update checking.
- **Write-skew occurrence under SI is probabilistic**, hence WARN rather than
  FAIL when absent (with default parameters it is reliably present).
- **Predicate reads via `table_scan` only**: predicate reads are covered by the
  index pass (see "Predicate reads" above), but only via UltimaDB's coarse
  `table_scan` degradation — the harness does not yet test *fine-grained*
  predicate read-set tracking (there is none to test) or predicate write skew via
  a domain invariant (a possible future SmallBank-style workload, deliberately
  out of scope here).
