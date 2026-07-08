# Transactional Consistency Verification with Elle — 2026-07-07 (updated 2026-07-08)

**TL;DR:** UltimaDB's two isolation claims — Snapshot Isolation by default,
Serializable (SSI) as opt-in — are verified by
[Elle](https://github.com/jepsen-io/elle), the transactional-safety checker
behind the Jepsen analyses, running against real concurrent MultiWriter
histories. Both claims hold. Two opt-in tiers:

- `make consistency/elle` — generates histories (point-read **and** table-scan
  passes, both isolation levels) and asserts the *exact anomaly set*: SI must
  show only `G2-item` (write skew), SSI none.
- `make consistency/elle-mutation` — injects known bugs into the commit path
  and proves Elle *catches* them (the harness's teeth-test).

Both need Java + `jq`. Technical detail in
`docs/tasks/task45_elle_consistency_harness.md` (harness) and
`docs/tasks/task47_elle_anomaly_and_mutation.md` (classification + mutation
testing).

## What was built (in order)

**1. The harness (task45).** `elle-history`
(`autobench/src/bin/elle-history.rs`) runs N concurrent writer threads doing
Elle's *list-append* workload — read-modify-write appends and reads over a small
hot keyspace — through real `begin_write`/`commit` transactions in
`WriterMode::MultiWriter`, under either isolation level. Every attempt is an Elle
`:invoke`/`:ok`/`:fail` event with globally unique append values;
`WriteConflict`/`SerializationFailure` map to `:fail` (both guarantee no commit).
Deterministic (seeded PRNG), zero new dependencies. The checker is a **vendored**
`elle-cli` 0.1.9 jar (`tools/elle-cli/`, EPL-2.0, sha256 pinned) so no Clojure
toolchain is needed — just `java -jar`. `scripts/elle_check.sh` runs the checks;
a committed write-skew fixture self-tests the checker before any real verdict is
trusted.

**2. The scan-read pass (task45 extension).** A `--scan-ratio` fraction of
transactions read the whole table via `range(..)` instead of point `get`s,
exercising SSI's **coarse `table_scan` read-set validation** — the code path the
point-read workload never touches. Scan transactions serve reads and appends
from a per-transaction working copy so read-your-writes holds (otherwise the
history would show a false anomaly). `make consistency/elle` now runs both a
point pass and a scan pass; the coarse read set roughly doubles the SSI abort
count, and the surviving history is still serializable.

**3. Anomaly classification (task47).** The check no longer asserts a bare
pass/fail. It parses elle-cli `--verbose` JSON and asserts the **exact anomaly
set**: SI histories must classify as anomalies ⊆ `{G2-item}` (write skew and
nothing worse — a `G-single`, `lost-update`, or `G1c` fails), SSI histories as
`∅`. The SI whitelist is deliberately narrow (widening requires human review).
This gives two independent detectors for a lost-update regression: the
SI-validity check flips *and* the classification whitelist rejects.

**4. Mutation testing (task47).** A `mutation-testing` cargo feature — off in
every normal build, and inert even when compiled in unless `ULTIMA_MUTATION` is
set — injects known bugs into the commit path: `skip-readset-validation`
(disables SSI read-set validation → write skew reappears) and
`skip-writeset-validation` (disables OCC write-conflict detection → lost update).
`make consistency/elle-mutation` runs a control (feature on, no mutation → clean
checks still pass, proving inertness), then confirms elle-cli **catches** each
injected bug. The assertion is inverted: a clean check must *fail* when mutated.

### Results (2026-07-08, defaults)

| Check | Expectation | Result |
|---|---|---|
| known-bad fixture (write skew) vs `serializable` / `snapshot-isolation` | rejected / accepted (self-test) | ✅ / ✅ |
| SI history vs `snapshot-isolation` | valid | ✅ |
| SI history vs `serializable`, classified | invalid, anomalies = `{G2-item}` | ✅ write skew only |
| SSI history vs `serializable`, classified | valid, anomalies = `∅` | ✅ |
| (mutation) `skip-readset-validation` | SSI now non-serializable | ✅ CAUGHT |
| (mutation) `skip-writeset-validation` | SI now violates snapshot-isolation | ✅ CAUGHT |
| (mutation control) feature on, env unset | clean checks still pass | ✅ inert |

Point pass, 24k events/level: SI ~6.4k committed / ~5.6k write-conflicts / 0
serialization failures; SSI ~5.5k committed / ~5.3k write-conflicts / ~1.1k
serialization failures. Scan pass (`--scan-ratio 0.5`): SSI serialization
failures roughly double (~2.2k), history still serializable. Feature-off root
`cargo test` unchanged (377); the injected code compiles out entirely.

## Why this is important

**Isolation guarantees are the hardest claims in a database to test.** Unit and
integration tests check interleavings someone thought of; a subtle bug in the
OCC merge path (`WriteTx::commit` rebase, per-key `merge_keys_from`, promotion
ordering, read-set validation) would produce anomalies only under interleavings
nobody scripted. Elle treats the store as a black box, records what thousands of
overlapping transactions actually observed, and *mathematically searches for
dependency cycles* that falsify the claimed model. A violation comes back as a
concrete counterexample with an SVG of the offending cycle.

**It checks the distinction, not just the happy path.** The SI history being
provably *non*-serializable (exactly `{G2-item}`) while the SSI history is clean
demonstrates, on real data, that `IsolationLevel::Serializable` does exactly the
work it claims. The scan pass proves the *coarse* read-set path is exercised, not
just point reads. The classification whitelist means a lost-update regression can
no longer hide behind "SI is not serializable, for some reason."

**The mutation tests prove the harness has teeth.** A checker that never fails is
worthless. By deliberately breaking read-set and write-set validation and
confirming Elle catches each, we know the green checks mean something. We got a
preview of this value early: the first hand-written "known-bad" fixture turned
out to be *legally serializable* under reordering, and the self-test caught it
before the harness shipped.

**It is a regression net for the riskiest code in the store,** the same way
`make perf/check` gates performance — and the fault injection is provably absent
from production builds (feature off = zero code; feature on + env unset =
byte-for-byte normal behavior, verified by the control run and the unchanged
test suite).

**It is credibility.** This is the same methodology (list-append + Elle) used in
the published Jepsen analyses of PostgreSQL, MySQL, and CockroachDB, and applied
to embedded engines like DuckDB. "Isolation levels verified with Elle, and the
verification proven to catch real bugs" is a checkable statement, not marketing.

## Bug found and fixed via this harness

The scan pass surfaced a real concurrency bug — exactly the kind isolation
testing is meant to flush out. While generating scan-pass histories,
`elle-history` **intermittently stalled for many minutes** in the SSI +
scan-read path (all threads parked in `futex_do_wait`, zero CPU). Root cause: a
**quiescence deadlock in the write-intent table** (`src/intents.rs`). On
releasing a contended key, `release_all_for` woke only the head waiter and left
the rest queued behind a `holder = 0` "transitioning" entry, relying on a
*future* acquirer to drain them. But queued waiters belong to already-dropped
transactions (drop-before-wait), so no waiter can be promoted to holder; under
SSI + scan's high abort churn over a small keyspace, threads pile up as such
waiters until all are parked, and with no thread left to re-touch the keys the
queues never drain. Fixed by waking **all** queued waiters on release (they
re-race on retry) — FIFO single-successor handoff is unachievable here.
Confirmed with a deterministic `IntentMap`-level regression test and a
150-iteration SSI+scan stress loop (0 hangs, was ~1-in-4 before). This is the
harness paying for itself: a rare deadlock in the OCC/SSI intent path, found by
the scan workload and reproduced deterministically.

## Next steps

1. **Crash-durability harness — the other half of the Jepsen methodology.**
   Elle checks isolation, not durability. Add a kill-9 / LazyFS-style torture
   that crashes mid-commit and mid-checkpoint across the `Durability` ×
   `WalWrite` matrix (especially `ConsistentInline` + `CoalescedPrealloc`, the
   `standalone_fast` preset) and verifies every acknowledged commit survives
   `Store::recover()`.
2. **Persistence-enabled Elle run.** Repeat the checks with
   `Persistence::Standalone` (Eventual durability) to cover the WAL-batched
   commit path's visibility ordering, not just the in-memory OCC path.
3. **A third mutation** — dropping a key in `Table::merge_keys_from` — to cover
   the per-key merge path alongside the read-set/write-set validation already
   injected.
4. **CI integration.** A nightly GitHub Actions job (`setup-java`, `jq`,
   `make consistency/elle` + `consistency/elle-mutation`). Everything needed is
   in-repo (the SSI+scan deadlock that would have hung such a job is now fixed).
   *Note: introducing CI to this repo is a maintainer decision — it currently
   has none.*
5. **Full Jepsen for ultima_cluster** (separate repo). The Raft/SMR deployment
   is a genuine distributed system — partitions, leader elections, clock skew —
   which is what the full Jepsen harness was built for; the etcd and redis-raft
   suites are the templates.
6. **Gate risky changes on it.** Adopt the convention that any PR touching
   `src/store.rs` commit/merge logic runs `make consistency/elle` before merge,
   alongside `make perf/check`.
