# task: journal-commit

Objective: raise sustained journal commit throughput and cut commit latency —
the cluster's binding constraint (group-commit fsync ceiling).

- **Primary metric:** `group_commit_throughput` (maximize) — initial campaign.
- **Secondary:** `append_consistent_p99_ns`, `append_eventual_ack_p99_ns`
  (minimize), `replay_throughput_entries_s` (maximize).
- **Mutable paths:** `ultima_journal/src/**`
- **Frozen:** the journal public API surface (signatures in
  `ultima_journal/src/journal/mod.rs`, `lib.rs` re-exports), `autobench/**`,
  all tests and benches.
- **Floor:** `cargo test -p ultima-autobench --test journal_torture` green.
- **Gates:**
  - Gate A: `cargo test --features persistence` + `cargo test -p ultima-journal`
  - Gate B: cluster `shmem-e2e` **p50 median-of-N** within the noise-aware band
    (≤50% regression vs champion p50) — p99 is recorded but NOT gated. The tail
    captures host scheduling jitter, not code, on a shared/virtualized box; the
    median of N samples (default 5, `AUTOBENCH_E2E_SAMPLES` to override) is the
    gated metric. On a quiet dedicated bench machine, tighten the band
    (`GATE_REGRESS_PCT_MAX` in `run-iter.rs`) and you may gate on p99 again.
- **Implementation-agent model:** opus by default — most wins here are in
  fsync ordering and group-commit batching, which is durability-critical.
- **Noise:** fsync metrics vary ±5–9% between runs on the bench host, and up to
  ±2× on a shared/virtualized host; use MEDIAN-of-5 for the microbench and the
  e2e gate alike. Never gate on the tail (p99) on a shared box.

## results.tsv schema

```
commit	group_commit_throughput	append_consistent_p99_ns	e2e_p50_ns	e2e_p99_ns	memory_kb	status	description
```

Column notes:
- `commit`: git commit SHA (short) at the time of the iteration.
- `group_commit_throughput`: primary metric — entries/sec, median-of-5.
- `append_consistent_p99_ns`: p99 latency of a consistent append, ns, median-of-5.
- `e2e_p50_ns`: **gated** metric — median of `submit_to_resp_p50_ns` across the
  Gate B samples; 0 if Gate B did not run (no cluster dir or gate skipped as not
  plausible). This is what the Gate B regression band is measured on.
- `e2e_p99_ns`: cluster `submit_to_resp_p99_ns` (median across samples) —
  recorded for observability only, NOT gated; 0 if Gate B did not run. Rows
  before the median-of-N gate rework carry a single-sample p99 here and `0` for
  `e2e_p50_ns`.
- `memory_kb`: reserved — write `0` until a memory metric exists.
- `status`: `keep` | `discard` | `crash`.
- `description`: one-line hypothesis + measured deltas + gate/torture outcome.
