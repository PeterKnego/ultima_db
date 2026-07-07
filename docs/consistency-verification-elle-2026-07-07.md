# Transactional Consistency Verification with Elle — 2026-07-07

**TL;DR:** UltimaDB's two isolation claims — Snapshot Isolation by default,
Serializable (SSI) as opt-in — are now verified by
[Elle](https://github.com/jepsen-io/elle), the transactional-safety checker
behind the Jepsen analyses, running against real concurrent MultiWriter
histories. Both claims hold. The check is a repeatable, opt-in tier:
`make consistency/elle` (~2–3 min, needs Java). Landed on `main` @ `cfd3153`;
technical details in `docs/tasks/task45_elle_consistency_harness.md`.

## What was done

- **`elle-history`** (`autobench/src/bin/elle-history.rs`): a workload driver
  that runs 8 concurrent writer threads doing Elle's *list-append* workload
  (read-modify-write appends and reads over 16 hot rows) through real
  `begin_write`/`commit` transactions in `WriterMode::MultiWriter`, under
  either isolation level. Every attempt is recorded as an Elle
  `:invoke`/`:ok`/`:fail` event with globally unique append values;
  `WriteConflict` and `SerializationFailure` map to `:fail` (both guarantee
  no commit). Fully deterministic (seeded PRNG), zero new dependencies.
- **Vendored checker** (`tools/elle-cli/`): elle-cli 0.1.9 standalone jar
  (EPL-2.0, sha256 pinned) so the check needs no Clojure toolchain — just
  `java -jar`.
- **Verdict pipeline** (`scripts/elle_check.sh` + `make consistency/elle`):
  generates one history per isolation level and asserts four things:

  | Check | Expectation | Result 2026-07-07 |
  |---|---|---|
  | known-bad fixture vs `serializable` / `snapshot-isolation` | rejected / accepted (checker self-test) | ✅ / ✅ |
  | SI history vs `snapshot-isolation` | valid — hard fail otherwise | ✅ valid |
  | SI history vs `serializable` | *invalid* (write skew is legal under SI) | ✅ invalid, as expected |
  | SSI history vs `serializable` | valid — hard fail otherwise | ✅ valid |

  Run stats (24,000 events per level): SI — 8,844 committed, 3,156
  write-conflicts, 0 serialization failures; SSI — 6,751 committed, 4,308
  write-conflicts, 941 serialization failures (the read-set validation
  visibly aborting the would-be write-skew transactions).

## Why this is important

**Isolation guarantees are the hardest claims in a database to test.** Unit
and integration tests check interleavings someone thought of;
`isolation-levels.md` documents what we promise; but a subtle bug in the OCC
merge path (`WriteTx::commit` rebase, per-key `merge_keys_from`, promotion
ordering) would produce anomalies only under interleavings nobody scripted.
Elle closes that gap: it treats the store as a black box, records what
thousands of overlapping transactions actually observed, and *mathematically
searches for dependency cycles* that falsify the claimed model. If any
committed transaction ever saw a state inconsistent with SI (or SSI), the
check fails with a concrete counterexample and an SVG of the offending cycle.

**It checks the distinction, not just the happy path.** The SI history being
provably *non*-serializable (write skew present) while the SSI history is
clean demonstrates, on real data, that `IsolationLevel::Serializable` is
doing exactly the work it claims — and that the checker actually fires. The
committed write-skew fixture guards the pipeline against the classic
false-confidence failure where a misconfigured checker silently passes
everything. (The first fixture draft — an "obvious" lost update — turned out
to be serializable under reordering; the self-test caught it immediately.
That is the guard earning its keep before the harness even shipped.)

**It is a regression net for the riskiest code in the store.** Planned work
touches the MultiWriter commit path (table-lock/parking-lot backlog, future
merge optimizations). Each such change can now be gated on a deterministic,
minutes-long consistency check with real teeth, the same way `make
perf/check` gates performance.

**It is credibility.** This is the same methodology (list-append + Elle) used
in the published Jepsen analyses of PostgreSQL, MySQL, and CockroachDB, and
applied to embedded engines like DuckDB. "Isolation levels verified with
Elle" is a checkable statement, not marketing.

## Next steps

1. **CI integration.** A nightly (or pre-release) GitHub Actions job:
   checkout, `setup-java`, `make consistency/elle`. Everything needed is in
   the repo; the job is ~5 min. Optionally vary `--seed` per run for
   interleaving diversity while keeping failures reproducible from the log.
2. **Range-read workload variant.** v1 uses point `get` only, so SSI's coarse
   read tracking (index/range reads taint the whole table) is unexercised.
   Add a mode where some transactions do `range`/`get_by_index` reads and
   verify the coarse conflicts still yield serializable histories.
3. **Crash-durability harness — the other half of the Jepsen methodology.**
   Elle checks isolation, not durability. Add a kill-9 / LazyFS-style torture
   that crashes the process mid-commit and mid-checkpoint across the
   `Durability` × `WalWrite` matrix (especially `ConsistentInline` +
   `CoalescedPrealloc`, the `standalone_fast` preset) and verifies every
   acknowledged commit survives `Store::recover()`.
4. **Persistence-enabled Elle run.** Repeat the existing checks with
   `Persistence::Standalone` (Eventual durability) to cover the WAL-batched
   commit path's visibility ordering, not just the in-memory OCC path.
5. **Full Jepsen for ultima_cluster** (separate repo). The Raft/SMR
   deployment is a genuine distributed system — network partitions, leader
   elections, clock skew — which is what the full Jepsen harness (nemesis +
   cluster) was built for; the etcd and redis-raft suites are the templates.
6. **Gate risky changes on it.** Adopt the convention that any PR touching
   `src/store.rs` commit/merge logic runs `make consistency/elle` before
   merge, alongside `make perf/check`.
