# Autobench: SMR-Focused Perf Harness + Benchmark Suite Restructure

Date: 2026-06-12
Status: approved (approach A + full cleanup + subagent orchestration)

## 1. Motivation

A full audit of the existing performance tests (21 bench files across ultima-db,
ultima-journal, ultima-vector; ~45–50 min per full run) found:

- **7 of 21 files are competitor baselines** (RocksDB/Fjall/ReDB) that dominate
  runtime (~20+ min) and contribute nothing to optimizing or regression-guarding
  our own code.
- **Three benches answer one question** ("how does multiwriter commit scale?"):
  `ycsb_multiwriter_bench` (scaling group), `smallbank_scaling_bench`,
  `disjoint_tables_bench`.
- **The prime use case is barely benchmarked.** ultima_cluster drives these
  crates as: (a) SMR apply loop — single writer, one `WriteTx` per Raft entry
  pinned via `begin_write(Some(log_index))`, immediate commit, snapshot
  freeze→stream→install→`checkpoint()` every ~5000 entries
  (`uc_service/src/runtime/apply_loop.rs`,
  `uc_service/src/ultima_db/store_state_machine.rs`); (b) Raft log —
  unbatched `Journal::append` with notifier-completion as the durability
  boundary, `iter_range` replay, `truncate_after`/`purge_before`
  (`uc_node/src/raft/log_storage.rs`). `Persistence::Smr` appears in **zero**
  benchmarks; journal truncate/purge, notifier ack latency, and recovery replay
  are unmeasured. The cluster's own analysis pins its ~80 commits/s ceiling on
  journal commit throughput — exactly what the suite doesn't measure.
- **No regression prevention.** critcmp targets and `bench_history.sh` are
  manual, YCSB-only, throughput-only. No percentile output anywhere; the
  cluster's SLAs are p99.

This spec adds an autoresearch-style optimization harness (modeled on
`ultima_cluster/uc_autobench`, see its design spec
`2026-05-29-uc-autobench-cc-driven-design.md`) whose fitness binaries double as
a standalone perf-regression gate, and restructures the existing suite around
first-party vs comparison tiers.

## 2. Layout

Three new workspace members:

```
ultima_db/
├── autobench/                 # the harness (not published)
│   ├── program.md             # generic loop spec (ported from uc_autobench)
│   ├── CLAUDE.md              # operator manual
│   ├── baselines/             # committed perf baselines for --check mode
│   │   ├── journal-commit.json
│   │   └── smr-apply.json
│   ├── tasks/
│   │   ├── TEMPLATE.md
│   │   ├── journal-commit/    # program.md + results.tsv
│   │   └── smr-apply/         # program.md + results.tsv
│   ├── src/
│   │   ├── task_spec.rs       # task registry
│   │   └── bin/
│   │       ├── run-iter.rs
│   │       ├── journal-microbench.rs
│   │       └── smr-apply-microbench.rs
│   └── tests/
│       ├── journal_torture.rs
│       └── smr_apply_torture.rs
├── bench_workloads/           # lib crate: ycsb_common + smallbank_common move here
└── compare_benches/           # competitor baselines + their dev-deps move here
```

`compare_benches` carries the rocksdb/fjall/redb dev-dependencies with it, so
root-crate `cargo test`/`cargo bench` stop compiling RocksDB entirely.
`bench_workloads` exposes the YCSB/SmallBank generators (scrambled Zipfian,
deterministic op pools, reference impls) to root benches, compare_benches, and
autobench fitness binaries.

## 3. Task 1: `journal-commit` (first priority)

The cluster's binding constraint. Fitness binary `journal-microbench` emits one
JSON object on stdout:

| Metric | Direction | Notes |
|---|---|---|
| `group_commit_throughput` | maximize, **primary** (initial campaign) | 256-entry burst + drain; mirrors the fsync group-commit pipeline behind the cluster's ~80 commits/s ceiling |
| `append_consistent_p99_ns` | minimize | fsync-acked append; natural primary for a latency campaign |
| `append_eventual_ack_p99_ns` | minimize | notifier completion = real durability boundary |
| `append_eventual_throughput` | maximize | entries/s, fire-and-forget |
| `replay_throughput_entries_s` | maximize | `iter_range` over ~100k entries (recovery speed) |
| `truncate_purge_p99_ns` | minimize | `truncate_after` + `purge_before` incl. metadata fsync |
| `reopen_ms` | minimize | journal open after 100k entries |

The primary metric for a given optimization campaign is declared in
`tasks/journal-commit/program.md` (the table above is the initial campaign);
all metrics are always emitted so baselines and the `--check` gate cover the
full set regardless of which one the loop is chasing.

Entry shapes mirror the Raft log: bincode payloads, 64 B–4 KiB sweep on the
append metrics. Measurement: batched-sample timing (1000 ops per `Instant`
sample) for sub-tick latency resolution; comparisons use median-of-5 runs.
Must run on a real disk (tmpfs check, as in `wal_bench.rs`).

- **Mutable paths:** `ultima_journal/src/**`
- **Frozen:** journal public API surface, autobench itself, all tests, benches.
- **Torture floor** (`journal_torture.rs`, frozen, <60 s): CRC-checked
  append/read round-trips, truncate/purge/reopen invariants, power-loss clamp
  (`durable_seq` vs `last_seq`), seq monotonicity, torn-tail recovery.

## 4. Task 2: `smr-apply`

Reproduces the apply loop exactly: `Persistence::Smr`, `SingleWriter`, one
`WriteTx` per entry via `begin_write(Some(log_index))`, immediate commit;
a reader thread issuing `begin_read(None)` queries throughout; snapshot freeze
+ `stream_snapshot` + `checkpoint()` every 5000 entries.

| Metric | Direction | Notes |
|---|---|---|
| `apply_p99_ns` | minimize, **primary** | per-entry begin_write→commit |
| `apply_throughput` | maximize | entries/s sustained incl. snapshot cadence |
| `read_p99_under_load_ns` | minimize | concurrent `begin_read(None)` + point query |
| `snapshot_freeze_p99_us` | minimize | MVCC pin under load |
| `snapshot_stream_ms` | minimize | serialize ~100k-row state |
| `checkpoint_ms` | minimize | |
| `restore_install_ms` | minimize | install_snapshot_stream path (follower catch-up) |

- **Mutable paths:** `src/**` (btree, table, store, persistence, checkpoint).
- **Frozen:** public API, on-disk format compatibility tests, autobench, tests.
- **Torture floor** (`smr_apply_torture.rs`, frozen, <60 s): apply N entries
  with pinned versions → `latest_version == last index`; snapshot/restore
  round-trip equality; read-during-apply isolation; checkpoint+recover
  equality (reusing corruption-injection patterns from the persistence tests).

## 5. run-iter

Ported from uc_autobench (same stage model, JSON output schema, subprocess
timeouts, stderr-tail capture):

1. **Build** — `cargo build --release -p ultima-autobench` (+ deps).
2. **Torture** — the task's frozen torture suite.
3. **Fitness** — task microbench, parse JSON, extract primary metric.
4. **Gates** (conditional: only when primary metric ≤ baseline × 1.05):
   - **Gate A (always when reached):** full `cargo test --features persistence`
     + `cargo test -p ultima-journal` — the workspace verification rule.
   - **Gate B (journal-commit + smr-apply, cross-repo):** `cargo build` in
     `../ultima_cluster` (path dep picks up local edits) and run its
     `commit-path-load` with a small request count; fail on >5% e2e p99
     regression vs recorded baseline. Skipped with an explicit reason if
     `../ultima_cluster` is absent.

Output: single JSON object (`status`, `stage`, `duration_s`, `metrics`,
`gate`, `stderr_tail`).

Loop discipline identical to uc_autobench: `results.tsv` row committed every
iteration (keep/discard/crash + one-line hypothesis and measured deltas);
revert losers via `git checkout -- <mutable_paths>`; champion = best primary
metric among `keep` rows.

## 6. Agent orchestration & model assignment

Unlike uc_autobench's single-session loop, heavy AI steps run in **subagents
with per-step model tiers**, prescribed in `autobench/program.md`:

| Step | Executor | Model | Rationale |
|---|---|---|---|
| Orchestration (read TSV, keep/discard, commit/revert) | main loop | session model | tiny context; never reads source or raw logs itself |
| Hypothesis generation | subagent | opus (or session model if stronger) | highest-leverage reasoning; drives the loop |
| Implementation of the hypothesis | subagent | sonnet; escalate to opus for lock-free / unsafe / fsync-ordering changes | scoped, well-specified edits |
| Run + parse run-iter JSON | no AI | — | deterministic binary; jq in main loop |
| Failure triage (stderr summary, trivial fix, max 2 tries) | subagent | haiku → sonnet | high-volume, low-reasoning; escalate on need |
| Profiling (every N iterations or on plateau): perf/flamegraph → hotspot summary | subagent | sonnet | absorbs large output, returns short summary feeding the hypothesis agent |

Rules encoded in `program.md`:

- **Escalate-on-failure:** start at the cheapest adequate tier
  (haiku→sonnet→opus); never start at the top for mechanical work.
- **Context hygiene:** subagents absorb file reads and log dumps; only
  summaries return to the orchestrator. This is what lets a run go for hours
  without exhausting the main-loop context.
- Per-task `program.md` may raise the implement tier for domain-hard tasks
  (e.g., journal fsync ordering defaults to opus).

## 7. Regression prevention without the loop

Each fitness binary accepts `--check --baseline autobench/baselines/<task>.json`:
committed baselines with per-metric tolerances derived from measured noise
floors (±2.5% for commit-path metrics, ±5–9% for alloc/fsync-heavy metrics —
re-measure per metric when recording baselines). Exit non-zero on breach,
printing the offending metrics. `make perf/check` runs both microbenches in
check mode (~2–3 min) — usable in CI or pre-merge, independent of any agent
loop. Baselines are re-recorded explicitly (`make perf/baseline`) when a
deliberate perf change lands, never silently.

## 8. Existing suite restructure

- **Move to `compare_benches`:** `ycsb_{fjall,rocksdb,redb}_bench`,
  `ycsb_multiwriter_{rocksdb,fjall}_bench`, `smallbank_{rocksdb,fjall}_bench`
  (+ their dev-deps). Makefile: `bench/compare-engines` tier; existing critcmp
  flows retargeted with `-p`.
- **Consolidate:** `smallbank_scaling_bench` + `disjoint_tables_bench` + the
  scaling group of `ycsb_multiwriter_bench` → one `multiwriter_scaling_bench`
  (writers × contention × disjoint-vs-shared-table matrix).
- **Retire:** `snapshot_throughput.rs` (superseded by smr-apply fitness
  metrics); `ultima_journal/benches/append_throughput.rs` once the
  journal-microbench covers its cases (keep until then).
- **Keep as-is:** `wal_bench` (sink/size/mode matrix feeds journal work),
  first-party `ycsb_bench`/`ycsb_multiwriter_bench`/`smallbank_bench`
  (API-level coverage), `bulk_load_bench`, vector benches.
- Default `make bench` runs first-party only (saves ~25 min per run).

## 9. Non-goals / deferred

- No parallel variant execution in the loop (matches uc_autobench deferral).
- No sustained multi-hour soak / GC-pause benchmark in v1 (candidate third
  task: `gc-soak`).
- No memory-footprint metric in v1 (`memory_kb` column reserved in TSV schema).
- Competitor benches are kept, not deleted — positioning still matters.

## 10. Testing the harness itself

- `run-iter` stage short-circuiting and JSON schema covered by unit tests with
  stub binaries (as in uc_autobench).
- Fitness binaries smoke-tested in CI with tiny N (env-var override, e.g.
  `AUTOBENCH_QUICK=1`) so they can't silently rot.
- Torture suites run as part of normal `cargo test -p ultima-autobench`.
