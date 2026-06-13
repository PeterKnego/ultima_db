# task: smr-apply

Objective: cut per-entry apply latency and raise sustained apply throughput
for the SMR state-store path (one pinned-version WriteTx per Raft entry).

- **Primary metric:** `apply_p99_ns` (minimize) — initial campaign.
- **Secondary:** `apply_throughput`, `apply_mw_throughput` (maximize),
  `read_p99_under_load_ns` (minimize).
- **Mutable paths:** `src/**` (root crate: btree, table, store, persistence,
  checkpoint)
- **Frozen:** the store public API surface, on-disk format compatibility
  (checkpoint/snapshot wire formats — recovery tests must keep passing),
  `autobench/**`, `ultima_journal/**`, all tests and benches.
- **Floor:** `cargo test -p ultima-autobench --test smr_apply_torture` green.
- **Gates:**
  - Gate A: `cargo test --features persistence` + `cargo test -p ultima-journal`
  - Gate B: cluster `shmem-e2e` **p50 median-of-N** within the noise-aware band
    (≤50% regression vs champion p50) — p99 recorded, not gated. See the
    journal-commit overlay / `run-iter.rs` for the shared rationale.
- **Implementation-agent model:** sonnet (escalate per generic rules — opus
  when touching lock-free code, unsafe, or fsync ordering, or after two failed
  sonnet build attempts).
- **Noise:** commit-path metrics vary ±2.5–5% on the bench host; use
  MEDIAN-of-5.

## results.tsv schema

```
commit	apply_p99_ns	apply_throughput	e2e_p50_ns	e2e_p99_ns	memory_kb	status	description
```

Column notes:
- `commit`: git commit SHA (short) at the time of the iteration.
- `apply_p99_ns`: primary metric — p99 apply latency, ns, median-of-5.
- `apply_throughput`: sustained apply entries/sec (single-writer), median-of-5.
- `e2e_p50_ns`: **gated** metric — median of `submit_to_resp_p50_ns` across the
  Gate B samples; 0 if Gate B did not run.
- `e2e_p99_ns`: median `submit_to_resp_p99_ns` across the Gate B samples —
  recorded for observability only, NOT gated; 0 if Gate B did not run.
- `memory_kb`: reserved — write `0` until a memory metric exists.
- `status`: `keep` | `discard` | `crash`.
- `description`: one-line hypothesis + measured deltas + gate/torture outcome.
