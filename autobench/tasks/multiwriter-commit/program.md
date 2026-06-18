# task: multiwriter-commit

Objective: raise sustained MultiWriter OCC commit throughput and cut commit
latency under key-level contention — the concurrent-writer path where disjoint
commits take the fast path (single Arc swap) and overlapping commits take the
`commit_multi_writer` merge slow path (`merge_keys_from`/`upsert_arc` + index
maintenance).

- **Primary metric:** `mw_commit_throughput` (maximize) — initial campaign.
  Sustained successful commits/sec across `writers` concurrent writers under a
  mixed hot/cold key schedule.
- **Secondary:**
  - `mw_commit_p99_ns` (minimize) — p99 per-commit latency under contention.
  - `read_p99_under_load_ns` (minimize) — p99 read latency of a concurrent
    reader during the contended write phase.
  - `mw_disjoint_throughput` (maximize) — commits/sec when writers own disjoint
    id ranges (pure fast-path) at the standard 4-writer width.
  - `mw_scaling_8x` (**gated**, maximize) — aggregate commits/sec at 8 disjoint
    writers (`SCALING_WRITERS` in `mw_commit_bench`). Disjoint ids → zero logical
    conflict, so any ceiling here is the serialized commit-install critical
    section (`inner.write()` hold time in `commit_multi_writer` Phase 3), not OCC.
    This is the *scaling* target: a campaign that shortens the serialized install
    should raise this faster than the 4-writer `mw_disjoint_throughput`. Gated via
    the baseline (`make perf/check`); on the sandbox it currently sits ~21K/s.
  - `mw_scaling_efficiency` (**informational, recorded NOT gated**) — ratio of
    the 8-writer aggregate to the 1-writer aggregate disjoint throughput. 8.0 =
    perfect linear scaling; <1.0 = adding writers *loses* aggregate throughput.
    Currently ~0.45 on the sandbox (negative scaling — the known lock-hold-time
    bottleneck). Goodhart guard: a candidate that raises `mw_scaling_8x` purely
    by speeding up every commit (not by shortening the serialized section) leaves
    efficiency flat — inspect before crediting it as a *scaling* win. A wide
    tolerance keeps it off the gate; it is a diagnostic, not an objective.
  - `mw_conflict_rate` (**informational, recorded NOT gated**) — observed
    conflict ratio under the fixed deterministic key schedule. Goodhart guard:
    the schedule is deterministic, so this should stay roughly constant across
    candidates. A candidate that moves `mw_conflict_rate` more than ~10% has
    changed the conflict-detection behaviour, not just the commit cost — treat
    its throughput delta as suspect and inspect before keeping.
- **Mutable paths:** `src/**` (root crate: store/OCC commit, table merge,
  index maintenance, btree).
- **Frozen:** the store public API surface, on-disk format compatibility
  (checkpoint/snapshot wire formats — recovery tests must keep passing),
  `autobench/**`, `ultima_journal/**`, all tests and benches.
- **Floor:** `cargo test -p ultima-autobench --test mw_commit_torture` green.
- **Gates:**
  - Gate A: `cargo test --features persistence` + `cargo test -p ultima-journal`
  - Gate B: cluster `shmem-e2e` **p50 median-of-N** within the noise-aware band
    (≤50% regression vs champion p50) — p99 recorded, not gated. The tail
    captures host scheduling jitter, not code, on a shared/virtualized box; the
    median of N samples (default 5, `AUTOBENCH_E2E_SAMPLES` to override) is the
    gated metric. On a quiet dedicated bench machine, tighten the band
    (`GATE_REGRESS_PCT_MAX` in `run-iter.rs`) and you may gate on p99 again.
- **Implementation-agent model:** opus by default — the OCC commit path is
  concurrency-critical (per-table lock ordering, the unsafe lock-guard
  transmute in commit, PromoteGate submission ordering, and the per-key merge),
  where correctness bugs are subtle and durability/isolation-relevant.
- **Noise:** commit-path metrics vary ±2.5–9% on the bench host, and up to ±2×
  on a shared/virtualized host; use MEDIAN-of-5 for the microbench. Never gate
  on the tail (p99) on a shared box.

## results.tsv schema

```
commit	mw_commit_throughput	mw_commit_p99_ns	e2e_p50_ns	e2e_p99_ns	memory_kb	status	description
```

Column notes:
- `commit`: git commit SHA (short) at the time of the iteration.
- `mw_commit_throughput`: primary metric — successful commits/sec under mixed
  contention, median-of-5.
- `mw_commit_p99_ns`: p99 per-commit latency under contention, ns, median-of-5.
- `e2e_p50_ns`: **gated** metric — median of `submit_to_resp_p50_ns` across the
  Gate B samples; 0 if Gate B did not run.
- `e2e_p99_ns`: median `submit_to_resp_p99_ns` across the Gate B samples —
  recorded for observability only, NOT gated; 0 if Gate B did not run.
- `memory_kb`: reserved — write `0` until a memory metric exists.
- `status`: `keep` | `discard` | `crash`.
- `description`: one-line hypothesis + measured deltas + gate/torture outcome.
  Note any `mw_conflict_rate` move >~10% (Goodhart guard) here.
