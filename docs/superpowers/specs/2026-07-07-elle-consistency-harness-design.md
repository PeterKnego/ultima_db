# Elle Consistency Harness (task41) — Design

**Date:** 2026-07-07
**Status:** Approved (user delegated recommended options; autonomous session)

## Goal

Black-box verification of UltimaDB's transactional isolation claims using Elle
(Jepsen's transactional safety checker) on list-append histories:

- **MultiWriter + SnapshotIsolation** must satisfy Elle's `snapshot-isolation`
  model (no G0, G1a/b/c, G-single, lost updates).
- **MultiWriter + Serializable (SSI)** must satisfy Elle's `serializable` model
  (no dependency cycles at all).
- As a harness-sanity signal, the SI history checked against `serializable` is
  *expected* to be invalid (write skew / G2-item is legal under SI). If it comes
  back valid, the workload isn't generating enough contention — warn, don't fail.

This is an opt-in conformance tier (like `make bench/compare-engines`), not part
of `cargo test`.

## Why list-append

Elle's strongest workload: every write is "append value V to the list at key K",
every read returns the full list. Because lists record their entire version
history, Elle can recover write-write, write-read, and read-write dependencies
from the history alone and search for cycles that falsify an isolation model.

## Components

### 1. Workload driver — `autobench/src/bin/elle-history.rs`

New binary `elle-history` in the existing `ultima-autobench` crate (already has
`clap`, `serde`, `ultima-db` with `persistence`). No new dependencies: op
generation uses an inline SplitMix64 PRNG; EDN output is hand-formatted (the
event shape is tiny and fixed).

**CLI (clap):**

| Flag | Default | Meaning |
|------|---------|---------|
| `--isolation si\|serializable` | required | store isolation level (MultiWriter always) |
| `--threads` | 8 | writer threads = Elle processes |
| `--keys` | 16 | keyspace size (small ⇒ contention) |
| `--txns-per-thread` | 1500 | attempted transactions per thread |
| `--ops-per-txn` | 4 | ops per transaction |
| `--read-ratio` | 0.4 | probability an op is a read (rest are appends) |
| `--seed` | 42 | PRNG seed (per-thread: seed + thread index) |
| `--out` | required | path for `history.edn` |

**Data model:** `ElleRow { list: Vec<u64> }` (serde derive — required because
autobench builds ultima-db with the `persistence` feature) in table `"elle"`.
Store: `Store::new(StoreConfig { writer_mode: MultiWriter, isolation_level: <flag>,
persistence: Persistence::None, .. })`.

**Setup:** one transaction inserts `--keys` rows with empty lists via
`TableWriter::insert` (auto-id) and records the returned ids as the key
universe. Rationale: `TableWriter` has no public insert-at-key, and `update`
requires the key to exist — so the whole keyspace is pre-seeded and the workload
is pure `get` + `update`. This runs before any recorded event, so Elle sees
reads of `[]` as consistent with "nothing appended yet".

**Transaction execution (per attempt):**

1. Generate ops: each op is `[:append k v]` (v from a global `AtomicU64`, so
   every append value is globally unique — Elle's per-key uniqueness requirement
   holds trivially, even across retried keys) or `[:r k nil]`.
2. Record `:invoke` event.
3. `store.begin_write(None)`; for each op in order:
   - `:r k` → `get(k)`, copy the list out (this is also what registers the SSI
     point read).
   - `:append k v` → `get(k)`, clone `Vec`, push `v`, `update(k, row)`.
4. `commit()`:
   - `Ok(_)` → `:ok` event with read values filled in.
   - `Err(WriteConflict { wait_for, .. })` → `:fail` event; if a `CommitWaiter`
     is present, wait on it before the next attempt (livelock avoidance, same
     pattern as `autobench/tests/mw_commit_torture.rs`).
   - `Err(SerializationFailure { .. })` → `:fail` event.
   - Any other error → `:info` event + stderr note (must not happen with
     `Persistence::None`; an `:info` also means the process id must be retired,
     so the thread stops issuing ops and the run is reported degraded).
5. Failed attempts are **not** retried as the same op: the next attempt is a
   fresh transaction with fresh values and its own `:invoke`. `:fail` means
   "definitely did not commit", which is exactly what both error paths guarantee.

**History recording:** a global `Mutex<Vec<Event>>`; events are pushed at invoke
time and completion time, so vector order is real-time order. `:index` is the
vector position; `:time` is nanos from a shared `Instant`. Elle requires that
completion order reflect real time — a single append-only vector under a mutex
gives that by construction. (The mutex adds a sync point between transactions,
not inside the commit path; measured contention at these scales is irrelevant
and it does not linearize the transactions themselves.)

**Output:** EDN, one event map per line:

```edn
{:index 0, :type :invoke, :process 3, :time 1200, :f :txn, :value [[:append 7 42] [:r 3 nil]]}
{:index 1, :type :ok,     :process 3, :time 5800, :f :txn, :value [[:append 7 42] [:r 3 [12 42]]]}
```

Plus a stderr summary: committed / write-conflict / serialization-failure
counts and events written.

### 2. Checker wiring — vendored elle-cli + `scripts/elle_check.sh`

**Vendored jar:** `tools/elle-cli/elle-cli-0.1.9-standalone.jar` (user chose
vendoring; ~30 MB, from github.com/ligurio/elle-cli release 0.1.9, Java 21 works).
`tools/elle-cli/README.md` records provenance: upstream URL, version, sha256,
license (elle-cli is MIT... verify at vendor time and include upstream LICENSE).

**Known-bad fixture:** `tools/elle-cli/fixtures/known_bad.edn` — a tiny
hand-written list-append history containing a lost update (two committed
transactions that both read `[]` at key 1 and both successfully append, but a
later read sees only one value). Checked into the repo. The script runs elle-cli
on it first and requires the verdict `false`. This guards the whole pipeline
against the classic false-confidence failure where the checker is silently
misconfigured and passes everything.

**`scripts/elle_check.sh` flow** (bash, `set -euo pipefail`):

1. Locate `java` (env `JAVA` override) and the jar; fail with instructions if missing.
2. Fixture smoke test: `elle-cli --model list-append known_bad.edn` → expect `false`.
3. `elle-cli --model list-append --consistency-models snapshot-isolation si/history.edn`
   → require `true`, else **FAIL** ("SI violated").
4. `elle-cli --model list-append --consistency-models serializable si/history.edn`
   → expect `false`; if `true`, print **WARN** ("no write skew observed — raise
   contention (--keys down / --threads up)"), exit 0.
5. `elle-cli --model list-append --consistency-models serializable ser/history.edn`
   → require `true`, else **FAIL** ("SSI violated").
6. Verdict parsing: elle-cli prints `<file>\t<true|false|unknown>` on stdout.
   `unknown` (cycle-search timeout / memory) → hard fail with a hint to bump
   `--cycle-search-timeout` or shrink the history.

### 3. Make target

```make
consistency/elle:            # opt-in, needs java 21+
	cargo build --release -p ultima-autobench --bin elle-history
	target/release/elle-history --isolation si --out target/elle/si/history.edn
	target/release/elle-history --isolation serializable --out target/elle/ser/history.edn
	scripts/elle_check.sh target/elle/si/history.edn target/elle/ser/history.edn
```

(Actual target passes flags through `ELLE_ARGS` for tuning.) Runtime budget with
defaults: ≲2 min generation + ≲1 min checking.

### 4. Docs

`docs/tasks/task41_elle_consistency_harness.md`: what Elle checks, how the
mapping works (pre-seeded keys, get+update appends, fresh-values-on-retry,
:fail semantics), how to run, how to read a failure (elle-cli `--directory` dumps
anomaly explanations + SVG cycle plots), and known limitations (below).

## Expected anomaly landscape (what the checks mean)

- UltimaDB SI = consistent snapshot + first-committer-wins on key overlap ⇒
  no dirty writes/reads, no lost updates ⇒ `snapshot-isolation` model must hold.
- Write skew (G2-item): T1 reads y appends x, T2 reads x appends y, disjoint
  write sets ⇒ both commit under SI ⇒ `serializable` check on the SI history
  should find rw-antidependency cycles. This is why `--read-ratio > 0` matters.
- SSI adds read-set validation at commit (`validate_read_set`,
  `Error::SerializationFailure`) ⇒ those cycles must be absent.

## Error handling

- Driver: thread panic ⇒ join fails ⇒ nonzero exit (history file not emitted).
  Unexpected commit error ⇒ `:info` + degraded-run notice on stderr + nonzero
  exit at the end (history still written; an `:info`-bearing history is still
  Elle-valid input).
- Script: every elle-cli invocation's verdict is parsed explicitly; `unknown`
  and missing-output are hard failures, never treated as pass.

## Testing

- Unit tests in the bin module: EDN event formatting (exact string match),
  SplitMix64 determinism, op-generation respects read-ratio/keys bounds.
- Pipeline self-checks: known-bad fixture must fail; SI-vs-serializable
  expected-false doubles as an end-to-end "the harness generates real
  contention and the checker fires" test.
- `cargo test -p ultima-autobench` stays green; full `make consistency/elle`
  run is the acceptance test.

## Known limitations (v1)

- In-memory only (`Persistence::None`): checks the OCC/SSI commit path, not
  recovery. Crash-durability checking (LazyFS-style) is a separate future task.
- SSI's coarse read tracking (index/range reads taint the whole table) is not
  exercised — the workload uses point `get` only. A range-read workload variant
  is a natural v2.
- Elle checks the history, not the store's final state; the existing
  `mw_commit_torture` floor already covers final-state lost-update checking.
- Write-skew *occurrence* under SI is probabilistic ⇒ warning, not assertion.
