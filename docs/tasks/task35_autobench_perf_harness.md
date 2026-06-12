# Task 35: Autobench Perf Harness + Bench Suite Restructure

Date: 2026-06-12
Status: complete

## 1. Motivation

A full audit of the existing performance tests (21 bench files across
`ultima-db`, `ultima-journal`, `ultima-vector`; ~45–50 min per full run) found
several structural problems:

- **7 of 21 files are competitor baselines** (RocksDB/Fjall/ReDB) that dominate
  runtime (~20+ min) and contribute nothing to optimizing or regression-guarding
  our own code.
- **Three benches answer one question** ("how does multiwriter commit scale?"):
  `ycsb_multiwriter_bench` (scaling group), `smallbank_scaling_bench`, and
  `disjoint_tables_bench`.
- **The prime use case is barely benchmarked.** `ultima_cluster` drives these
  crates as: (a) an SMR apply loop — single writer, one `WriteTx` per Raft entry
  pinned via `begin_write(Some(log_index))`, immediate commit, snapshot
  freeze→stream→install→`checkpoint()` every ~5000 entries; (b) the Raft log —
  unbatched `Journal::append` with notifier-completion as the durability
  boundary, `iter_range` replay, `truncate_after`/`purge_before`. The
  `Persistence::Smr` variant appeared in **zero** benchmarks; journal
  truncate/purge, notifier ack latency, and recovery replay were unmeasured. The
  cluster's own analysis pins its ~80 commits/s ceiling on journal commit
  throughput — exactly what the suite didn't measure.
- **No regression prevention.** critcmp targets and `bench_history.sh` were
  manual, YCSB-only, throughput-only. No percentile output anywhere; the
  cluster's SLAs are p99.

This task adds an autoresearch-style optimization harness (modeled on
`ultima_cluster/uc_autobench`) whose fitness binaries double as a standalone
perf-regression gate, and restructures the existing suite around first-party vs
comparison tiers.

## 2. Suite Restructure

### New workspace members

**`bench_workloads/`** — lib crate holding the YCSB/SmallBank generators
previously `#[path]`-included into each bench binary. Exposes
`ultima_bench_workloads::ycsb` and `::smallbank` to root benches,
`compare_benches`, and the autobench harness.

**`compare_benches/`** — bench-only crate holding the 7 RocksDB/Fjall/ReDB
baseline benches and their dev-deps. After the move, plain `cargo bench`/
`cargo test` at the workspace root no longer compile RocksDB at all.
`make bench/compare-engines` is the entry point for the opt-in tier.

### Consolidated `multiwriter_scaling_bench`

`benches/multiwriter_scaling_bench.rs` replaces the former
`disjoint_tables_bench.rs`, `smallbank_scaling_bench.rs`, and the scaling group
inside the shared YCSB suite. It sweeps writer counts over four contention
shapes:
- `mw_scaling_ycsb_low` — shared table, Zipfian over 10K keys, in-memory
- `mw_scaling_ycsb_high` — shared table, hot keys 1..=10, in-memory
- `mw_scaling_disjoint` — one table per writer, Eventual persistence
- `mw_scaling_smallbank_high` — 3-table SmallBank txns on hot accounts, Eventual

### Retired benches

| File | Superseded by |
|------|---------------|
| `benches/snapshot_throughput.rs` | `smr-apply-microbench`: freeze/stream/checkpoint/install all measured under apply-loop load with realistic rows and cadence |
| `ultima_journal/benches/append_throughput.rs` | `journal-microbench`: covers Consistent/Eventual append throughput and latency, replay, truncate/purge, and reopen — cases it never had |

The `criterion` dev-dep in `ultima_journal/Cargo.toml` was removed along with
the bench; `tempfile` was retained (still used by integration tests).

### Default `make bench`

```bash
make bench              # first-party tier only (saves ~25 min)
make bench/compare-engines  # competitor baselines, opt-in
```

## 3. Autobench Harness

### Layout

```
autobench/
├── program.md             # generic loop spec: autonomy, subagent dispatch, per-iter sequence
├── CLAUDE.md              # operator manual (how to start a run, read results, add a task)
├── baselines/
│   ├── journal-commit.json
│   └── smr-apply.json
├── tasks/
│   ├── TEMPLATE.md
│   ├── journal-commit/    # program.md + results.tsv
│   └── smr-apply/         # program.md + results.tsv
├── src/
│   ├── lib.rs             # pub mod sampling, baseline, task_spec, journal_bench, smr_bench, diskcheck
│   ├── sampling.rs        # batched_samples_ns, percentile
│   ├── baseline.rs        # Direction, MetricBaseline, Baselines::check
│   ├── task_spec.rs       # TaskSpec registry (task_spec("journal-commit"), task_spec("smr-apply"))
│   ├── diskcheck.rs       # backing_fs + assert_real_disk (tmpfs guard)
│   ├── journal_bench.rs   # fitness function: METRIC_KEYS, Config, run()
│   ├── smr_bench.rs       # fitness function: METRIC_KEYS, Config, run()
│   └── bin/
│       ├── run-iter.rs
│       ├── journal-microbench.rs
│       └── smr-apply-microbench.rs
└── tests/
    ├── journal_torture.rs      # FROZEN conformance floor
    ├── smr_apply_torture.rs    # FROZEN conformance floor
    └── microbench_smoke.rs     # quick-mode: every metric present and positive
```

### run-iter stage model

`run-iter --task <id> --json` runs five sequential stages:

1. **Build** — `cargo build --release -p ultima-autobench` (+ deps).
2. **Torture** — `cargo test -p ultima-autobench --test <torture_test>`: the
   task's frozen conformance suite (must pass; immutable by design).
3. **Microbench** — spawn the task's fitness binary; parse JSON stdout; extract
   the primary metric.
4. **Gate A** (runs only when the primary metric is a plausible winner — within
   5% of the champion in the metric's direction): `cargo test --features
   persistence` + `cargo test -p ultima-journal`.
5. **Gate B** (conditional, cross-repo): in `../ultima_cluster`, run
   `uc-autobench shmem-e2e --json`; fail on >5% `submit_to_resp_p99_ns`
   regression vs the recorded baseline. Skipped with an explicit reason if the
   cluster checkout is absent or Gate A failed.

**Direction-aware gate decision:** the journal primary metric is
`group_commit_throughput` (maximize); the SMR primary is `apply_p99_ns`
(minimize). The plausibility window is direction-aware: for maximize the variant
must be ≥ baseline × 0.95; for minimize it must be ≤ baseline × 1.05.

**JSON output schema:**

```json
{
  "status":     "pass | build_failed | torture_failed | microbench_failed | tests_failed | e2e_failed | timeout | unknown_task",
  "stage":      "last stage that ran",
  "duration_s": { "build": 0.0, "torture": 0.0, "microbench": 0.0, "tests": 0.0, "e2e": 0.0 },
  "metrics":    { "<key>": 0.0 },
  "gate": {
    "ran": true,
    "passed": true,
    "e2e_p99_ns": 12345678,
    "baseline": 12000000,
    "regress_pct": 2.8,
    "reason": null
  },
  "stderr_tail": "..."
}
```

## 4. Task 1: `journal-commit`

Fitness binary: `journal-microbench`. Mirrors the Raft-log usage in
`ultima_cluster`: unbatched appends, notifier acks, `iter_range` replay,
`truncate_after`/`purge_before` with metadata fsync. Bincode payloads at 64 B /
1 KiB / 4 KiB.

### Metric table

| Metric | Direction | Notes |
|--------|-----------|-------|
| `group_commit_throughput` | maximize, **primary** | 256-entry burst, Consistent durability — last notifier resolves after the batch fsync; times the fsync coalesce pipeline |
| `append_consistent_p99_ns` | minimize | 1 KiB payload, batch=1 (each op includes its own fsync) |
| `append_consistent_p99_ns_64b` | minimize | 64 B payload, same method |
| `append_consistent_p99_ns_4k` | minimize | 4 KiB payload, same method |
| `append_eventual_ack_p99_ns` | minimize | Eventual durability; notifier resolves at buffered-write (fsync off-clock) |
| `append_eventual_throughput` | maximize | entries/s, fire-and-forget fill of `fill_entries` then `wait_durable` |
| `replay_throughput_entries_s` | maximize | `iter_range` over 100k entries (warm page cache; tracks open-API replay cost) |
| `truncate_purge_p99_ns` | minimize | `truncate_after` + `purge_before` incl. metadata fsync; `purge_before` is segment-granular (see §6) |
| `reopen_ms` | minimize | p50 of 9 reopens after 100k-entry fill (warm cache; tracks open-path CPU cost) |

**Implementation deviation from spec:** `group_commit_throughput` runs under
`Durability::Consistent`, not `Eventual` as implied by the spec's "drain"
phrasing. The rationale: the cluster's ceiling is the fsync pipeline, so the
metric must include the fsync. The coalescing writer batches the queued 256-entry
burst into group-commit batches automatically; waiting only the last notifier
implies the whole burst is durable (FIFO channel + fsync-as-barrier). This makes
the metric a cleaner measure of fsync group-commit throughput.

**Implementation deviation:** `append_eventual_ack_p99_ns` uses `batch=1` (not
the spec's `batch=16`), so each sample is a single append+wait round-trip. This
gives honest per-op tail latency rather than a batch mean.

## 5. Task 2: `smr-apply`

Fitness binary: `smr-apply-microbench`. Reproduces the cluster apply loop:
`Persistence::Smr`, `WriteTx` pinned to the log index via `begin_write(Some(log_index))`,
a reader thread issuing `begin_read(None)` throughout, and a
freeze→stream→checkpoint cadence every `snapshot_every` iterations (2000 in
standard config; spec says 5000 but 2000 yields ~10 events per 20k-iter run for
stable percentiles — cadence changes sample count, not per-event cost).

### Metric table

| Metric | Direction | Notes |
|--------|-----------|-------|
| `apply_p99_ns` | minimize, **primary** | T=1 SingleWriter pinned apply; `APPLY_BATCH=1` so each sample is one commit, not a batch mean |
| `apply_throughput` | maximize | iterations/s over the full 20k-iter loop incl. snapshot cadence |
| `apply_sw_batch_throughput` | maximize | txns/s, T=8 sequential per iteration on the main store |
| `apply_mw_throughput` | maximize | txns/s, T=8 across 4 parallel MultiWriter OCC writers (disjoint keys) |
| `apply_mw_p99_ns` | minimize | per-txn commit p99 under the 4-writer parallel workload |
| `read_p99_under_load_ns` | minimize | concurrent `begin_read(None)` + point query p99 during the apply loop |
| `snapshot_freeze_p99_us` | minimize | MVCC pin time (`snapshot_stream(None)`) under load |
| `snapshot_stream_ms` | minimize | p50 of serialize-to-bytes for ~100k-row state |
| `checkpoint_ms` | minimize | p50 of `checkpoint()` |
| `restore_install_ms` | minimize | `install_snapshot_stream` into a fresh store; install lands at `base_version + 1` per v1 install semantics |

**Implementation deviation from spec:** `apply_p99_ns` uses `APPLY_BATCH=1` —
each apply-loop sample is one `begin_write`→`commit` round-trip (not a batch
mean), so the p99 sees individual slow commits. The spec sketch showed
`APPLY_BATCH=32`; the comment in `smr_bench.rs` explains the change.

## 6. Subagent Orchestration

Prescribed in `autobench/program.md`. Model assignments:

| Step | Model | Rationale |
|------|-------|-----------|
| Orchestration (read TSV, keep/discard, commit/revert) | session model | Tiny context; never reads source or raw logs |
| Hypothesis generation | opus | Highest-leverage reasoning; drives the loop |
| Implementation | sonnet; escalate to opus for lock-free/unsafe/fsync-ordering changes | Scoped, well-specified edits |
| Failure triage | haiku → sonnet | High-volume, low-reasoning; escalate on complexity (max 2 attempts) |
| Profiling (every ~10 iters or on plateau): perf/flamegraph → hotspot summary | sonnet | Absorbs large output, returns short summary |

Context hygiene: subagents absorb file reads and log dumps; only summaries
return to the orchestrator. This is what lets a run go for hours without
exhausting the main-loop context.

## 7. Regression Gate

Each fitness binary accepts:
- `--check --baseline autobench/baselines/<task>.json` — exit 2 on breach
- `--write-baseline autobench/baselines/<task>.json` — record baselines from
  the current run

```bash
make perf/check      # run both microbenches in --check mode (~3–6 min)
make perf/baseline   # re-record baselines (only after a deliberate perf change)
```

Tolerances are per-metric, derived from measured noise floors (±2.5% for
commit-path metrics, ±5–9% for alloc/fsync-heavy metrics). The committed
baseline JSONs were recorded on a noisy virtualized sandbox host — tolerances
were widened substantially. **Re-record with `make perf/baseline` on the real
bench machine** before relying on `make perf/check` as a regression gate. Each
baseline JSON carries a `"note"` field that says the same.

Baselines are machine-specific. Never silently update them; re-record explicitly
when a deliberate perf change lands.

## 8. Key Implementation Findings

**`purge_before` is segment-granular.** The journal drops only whole non-active
segments whose last seq ≤ the argument. With 1 KiB payloads and the default
64 MiB segments, the `truncate_purge` metric's `purge_before` calls are
effectively no-ops (the data never fills a segment in the test window). The
metric still measures the real cluster cost: `truncate_after` + metadata fsync
+ the `purge_before` call overhead, which is what the cluster pays on each
log-compaction step.

**Gate B path-dep / worktree trap.** `ultima_cluster` Cargo path-deps resolve
against `../ultima_db` at the directory where `cargo build` runs. Inside a git
worktree (`.claude/worktrees/…`), `../ultima_db` still resolves to the
**primary checkout**, not the worktree. Gate B would measure the wrong code.
The mitigation is documented in both `program.md` and `autobench/CLAUDE.md`:
always start the optimization loop from the primary checkout on a branch.

**Eventual notifier resolves at buffered-write; Consistent resolves at fsync.**
`append_eventual_ack_p99_ns` measures the write-path latency up to buffer
flush only — fsync is off-clock. `group_commit_throughput` (Consistent) times
the full fsync pipeline. The two metrics bound different layers; both are needed
because the cluster cares about both the apply-path ceiling and the tail latency
of individual commits.

**`snapshot_stream(None)` freezes a new MVCC pin immediately** — the "freeze"
latency is the pin acquisition, not the full stream. Stream time is measured
separately. `snapshot_every=2000` in standard config yields ~10 snapshot events
over a 20k-iteration run, giving stable p50/p99 percentiles for both.

## 9. Deferred

- **`memory_kb` metric** — column reserved in TSV schema; `smrss`/`mallinfo2`
  collection scaffolding not wired up in v1.
- **`gc-soak` task** — candidate third autobench task for sustained multi-hour
  GC-pause measurement.
- **Mechanical freeze enforcement** for torture files — currently a convention;
  a pre-commit hook that rejects edits to `autobench/tests/*.rs` would enforce
  it mechanically.
- **Parallel variant execution** — matches the `uc_autobench` deferral; serial
  iterations are the current design.

## 10. References

- Spec: `docs/superpowers/specs/2026-06-12-autobench-perf-harness-design.md`
- Bench restructure plan: `docs/superpowers/plans/2026-06-12-bench-suite-restructure.md`
- Harness implementation plan: `docs/superpowers/plans/2026-06-12-autobench-harness.md`
- Operator manual: `autobench/CLAUDE.md`
- Loop spec: `autobench/program.md`
