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
commit	group_commit_throughput	append_consistent_p99_ns	e2e_p50_ns	e2e_p99_ns	memory_kb	group_commit_throughput_prealloc	fsync_only_p50_ns	fsync_prealloc_p50_ns	write_only_p50_ns	status	description
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
- `group_commit_throughput_prealloc`: same burst loop with
  `preallocate_segments: true` — entries/sec; the end-to-end preallocation win.
  `0` for rows recorded before this metric existed.
- `fsync_only_p50_ns`: p50 of an isolated `sync_data` barrier after a
  size-extending append (the per-commit fsync cost). `0` for older rows.
- `fsync_prealloc_p50_ns`: same, but over a preallocated segment (no `i_size`
  metadata commit). `fsync_only − fsync_prealloc` is the metadata-commit cost.
  `0` for older rows.
- `write_only_p50_ns`: p50 of the page-cache append alone (encode + `write_all`,
  no fsync). `0` for older rows.
  (The four columns above are observability only — NOT gated; the gated baseline
  for all isolated/prealloc metrics lives in `autobench/baselines/journal-commit.json`,
  which also carries the p99 variants. See `journal_bench.rs`.)
- `status`: `keep` | `discard` | `crash`.
- `description`: one-line hypothesis + measured deltas + gate/torture outcome.
