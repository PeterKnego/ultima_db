# Transactional Consistency Verification with Elle — 2026-07-07 (updated 2026-07-08)

**TL;DR:** UltimaDB's two isolation claims — Snapshot Isolation by default,
Serializable (SSI) as opt-in — are verified by
[Elle](https://github.com/jepsen-io/elle), the transactional-safety checker
behind the Jepsen analyses, running against real concurrent MultiWriter
histories. Both claims hold, and the checks now gate every pull request. Two
opt-in tiers:

- `make consistency/elle` — generates histories (point-read, table-scan, **and
  predicate/index** passes, both isolation levels) and asserts the *exact
  anomaly set*: SI must show only `G2-item` (write skew), SSI none.
- `make consistency/elle-mutation` — injects **three** known commit-path bugs
  and proves Elle *catches* each (the harness's teeth-test).

Both need Java + `jq`. `.github/workflows/consistency.yml` runs the bounded
`elle` check on every PR and the full-sizing check plus the mutation suite
weekly. Technical detail in `docs/tasks/task45_elle_consistency_harness.md`
(harness + predicate reads) and `docs/tasks/task47_elle_anomaly_and_mutation.md`
(classification + mutation testing).

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

**3. The predicate-read (index) pass (task45 extension).** A `--predicate-ratio`
fraction of transactions select a row group through a **secondary index** —
`get_by_index` (equality) or `index_range` (range) on a static `bucket` column —
instead of a point `get` or a full scan, exercising the index read path the
other passes never touch. Because every secondary-index read registers the same
coarse `table_scan` read-set entry (`src/store.rs`), a predicate read has the
*same* conflict profile as a full scan: SSI stays clean (phantoms prevented), SI
shows only `{G2-item}` — so it needs **no new anomaly whitelist**. Each predicate
transaction also asserts the index returned exactly the statically-known bucket
membership, an index-integrity check under concurrent updates. `make
consistency/elle` now runs three passes: point, scan, predicate.

**4. Anomaly classification (task47).** The check no longer asserts a bare
pass/fail. It parses elle-cli `--verbose` JSON and asserts the **exact anomaly
set**: SI histories must classify as anomalies ⊆ `{G2-item}` (write skew and
nothing worse — a `G-single`, `lost-update`, or `G1c` fails), SSI histories as
`∅`. The SI whitelist is deliberately narrow (widening requires human review).
This gives two independent detectors for a lost-update regression: the
SI-validity check flips *and* the classification whitelist rejects.

**5. Mutation testing (task47).** A `mutation-testing` cargo feature — off in
every normal build, and inert even when compiled in unless `ULTIMA_MUTATION` is
set — injects known bugs into the commit path: `skip-readset-validation`
(disables SSI read-set validation → write skew reappears),
`skip-writeset-validation` (disables OCC write-conflict detection → lost update),
and `drop-merge-key` (silently drops a writer's edited key in the commit
slow-path merge `Table::merge_keys_from` → a lost update *below* the isolation
layer, where OCC/SSI validation has already passed). The three cover both
isolation-enforcement paths (read-set and write-set validation) *and* the per-key
merge that reconciles disjoint concurrent writers. `make consistency/elle-mutation`
runs a control (feature on, no mutation → clean checks still pass, proving
inertness), then confirms elle-cli **catches** each injected bug. The assertion
is inverted: a clean check must *fail* when mutated.

**6. CI gating (task45).** `.github/workflows/consistency.yml` (mirroring the
existing `formal.yml` split) runs a bounded `elle` job on every pull request —
the three passes at reduced contention (`--threads 8 --keys 8
--txns-per-thread 800`), sized so elle-cli's cycle search stays clear of an
`unknown` verdict — blocking a consistency regression at merge time. A weekly
`elle-deep` job re-runs the canonical sizing plus `make consistency/elle-mutation`.
java is provisioned per-job (Temurin 21); `jq` ships on the runner; the elle-cli
jar is vendored, so nothing is downloaded. Self-tested live on its own
introducing PR.

### Results (2026-07-08, defaults)

| Check | Expectation | Result |
|---|---|---|
| known-bad fixture (write skew) vs `serializable` / `snapshot-isolation` | rejected / accepted (self-test) | ✅ / ✅ |
| SI history vs `snapshot-isolation` | valid | ✅ |
| SI history vs `serializable`, classified | invalid, anomalies = `{G2-item}` | ✅ write skew only |
| SSI history vs `serializable`, classified | valid, anomalies = `∅` | ✅ |
| predicate/index pass, SI vs `serializable` / SSI vs `serializable` | `{G2-item}` / `∅` (same as scan) | ✅ / ✅ |
| (mutation) `skip-readset-validation` | SSI now non-serializable | ✅ CAUGHT |
| (mutation) `skip-writeset-validation` | SI now violates snapshot-isolation | ✅ CAUGHT |
| (mutation) `drop-merge-key` | SI now violates snapshot-isolation | ✅ CAUGHT |
| (mutation control) feature on, env unset | clean checks still pass | ✅ inert |

Point pass, 24k events/level: SI ~6.4k committed / ~5.6k write-conflicts / 0
serialization failures; SSI ~5.5k committed / ~5.3k write-conflicts / ~1.1k
serialization failures. Scan pass (`--scan-ratio 0.5`): SSI serialization
failures roughly double (~2.2k), history still serializable. Predicate pass
(`--predicate-ratio 0.5 --buckets 4`): same anomaly profile as the scan pass
(index reads degrade to `table_scan`), SI shows `{G2-item}`, SSI clean.
Feature-off root `cargo test` unchanged; the injected code compiles out entirely.

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
work it claims. The scan and predicate passes prove the *coarse* read-set path is
exercised — through both full-table scans and secondary-index (`get_by_index` /
`index_range`) reads — not just point reads. The classification whitelist means a
lost-update regression can no longer hide behind "SI is not serializable, for
some reason."

**The mutation tests prove the harness has teeth.** A checker that never fails is
worthless. By deliberately breaking read-set and write-set validation and
confirming Elle catches each, we know the green checks mean something. We got a
preview of this value early: the first hand-written "known-bad" fixture turned
out to be *legally serializable* under reordering, and the self-test caught it
before the harness shipped.

**It is a regression net for the riskiest code in the store,** and it now runs
automatically: the `elle` job gates every pull request the way `make perf/check`
gates performance, and the fault injection is provably absent from production
builds (feature off = zero code; feature on + env unset = byte-for-byte normal
behavior, verified by the control run and the unchanged test suite).

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

## Completed since 2026-07-07

- **Third mutation (`drop-merge-key`).** Covers the per-key commit merge
  (`Table::merge_keys_from`) alongside the read-set/write-set validation — the
  one injection *below* the isolation layer. Caught on the first attempt.
- **Predicate-read (index) pass.** Widened the workload to secondary-index reads
  (`get_by_index` / `index_range`); confirmed they share the scan pass's
  `{G2-item}` profile (index reads degrade to `table_scan`), so no new whitelist.
- **CI gating.** `.github/workflows/consistency.yml` — `elle` on every PR,
  `elle-deep` (canonical sizing + mutation suite) weekly. (The repo does have CI
  now — `formal.yml` — so this was no longer a maintainer-decision blocker; the
  SSI+scan deadlock that would have hung such a job was already fixed.)

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
3. **Predicate write skew via a domain invariant.** The index pass exercises the
   read *path* but, because index reads degrade to coarse `table_scan`, it shares
   the scan anomaly profile rather than probing *phantoms* directly — elle-cli's
   list-append model can't express a predicate. A SmallBank-style workload with a
   checked invariant (e.g. "sum over a group ≥ 0") would demonstrate SSI prevents
   (and SI allows) predicate write skew, and would motivate any future
   fine-grained predicate read-set tracking (there is none today).
4. **Full Jepsen for ultima_cluster** (separate repo). The Raft/SMR deployment
   is a genuine distributed system — partitions, leader elections, clock skew —
   which is what the full Jepsen harness was built for; the etcd and redis-raft
   suites are the templates.
