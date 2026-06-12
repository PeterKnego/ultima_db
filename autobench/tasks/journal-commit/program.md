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
  - Gate B: cluster `shmem-e2e` p99 within 5% of baseline
- **Implementation-agent model:** opus by default — most wins here are in
  fsync ordering and group-commit batching, which is durability-critical.
- **Noise:** fsync metrics vary ±5–9% between runs on the bench host (far more
  on virtualized hosts); use MEDIAN-of-5.

## results.tsv schema

```
commit	group_commit_throughput	append_consistent_p99_ns	e2e_p99_ns	memory_kb	status	description
```

Column notes:
- `commit`: git commit SHA (short) at the time of the iteration.
- `group_commit_throughput`: primary metric — entries/sec, median-of-5.
- `append_consistent_p99_ns`: p99 latency of a consistent append, ns, median-of-5.
- `e2e_p99_ns`: cluster `submit_to_resp_p99_ns` from Gate B; 0 if Gate B did
  not run (no cluster dir or gate skipped as not plausible).
- `memory_kb`: reserved — write `0` until a memory metric exists.
- `status`: `keep` | `discard` | `crash`.
- `description`: one-line hypothesis + measured deltas + gate/torture outcome.
