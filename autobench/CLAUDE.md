# autobench

Claude-Code-driven autoresearch loop for ultima_db / ultima_journal performance.
Karpathy/autoresearch shape: Claude Code proposes code changes, the `run-iter`
harness measures them, and the loop commits wins / reverts losses indefinitely
until the human presses Ctrl-C.

## Starting a run

**Start from the PRIMARY repo root — not a git worktree.** Gate B builds
`ultima_cluster` via a `../ultima_db` path dep; inside a worktree that dep
silently resolves to the primary checkout, so the gate would measure the
wrong code.

Open Claude Code in the primary `ultima_db/` checkout and prompt:

```
Run the autobench loop for task journal-commit per autobench/program.md.
```

or for the SMR task:

```
Run the autobench loop for task smr-apply per autobench/program.md.
```

That's the entire invocation. The loop reads `program.md`, creates a branch,
and runs indefinitely. Interrupt with Ctrl-C when satisfied.

## Reading results

Results live in `autobench/tasks/<task>/results.tsv` — one row per iteration,
committed every iteration.

**Columns (journal-commit):**
`commit | group_commit_throughput | append_consistent_p99_ns | e2e_p50_ns | e2e_p99_ns | memory_kb | status | description`
(`e2e_p50_ns` is the gated metric — median-of-N; `e2e_p99_ns` is recorded only.)

**Columns (smr-apply):**
`commit | apply_p99_ns | apply_throughput | e2e_p99_ns | memory_kb | status | description`

**Champion:** the row with the best `primary` value among `status=keep` rows.
Direction is task-specific (maximize for throughput, minimize for latency).
The `commit` column is the git SHA — check it out to inspect the winning code.

**Why was variant X rejected?** Find its `status=discard` or `status=crash`
row and read `description`; cross-reference the matching commit in `git log`.

## run-iter quick reference

```
cargo run -p ultima-autobench --bin run-iter --release -- \
  --task journal-commit \
  --json \
  --baseline-primary <champion_primary> \
  --baseline-gate-ns <champion_gate_p50_ns>
```

Flags:
- `--task`: `journal-commit` or `smr-apply`
- `--json`: required; emits one JSON object on stdout (exit 0 even on failure)
- `--baseline-primary`: champion's primary-metric value; omit on first iter
- `--baseline-gate-ns`: champion's Gate B e2e **p50** median (`gate.e2e_p50_ns`
  from its run); omit on first iter. The gate compares the candidate's p50
  median against this within a wide noise band — p99 is recorded, not gated.
  (Legacy alias `--baseline-gate-p99-ns` still parses but now feeds the p50
  comparison; pass the champion's p50, not its p99.)
- `AUTOBENCH_E2E_SAMPLES` (env): e2e samples per gate for the median (default 5).
- `--cluster-dir`: path to `ultima_cluster` checkout (default: `../ultima_cluster`)
- `--skip-tests`: skip Gate A test suites (for extra microbench samples only;
  a KEEP still requires a fully-gated run)

**JSON output fields:**

| Field | Type | Meaning |
|-------|------|---------|
| `status` | string | Overall result (see statuses below) |
| `stage` | string | Last stage that ran |
| `duration_s` | object | Per-stage wall times: `build`, `torture`, `microbench`, `tests`, `e2e` |
| `metrics` | object | All keys from the microbench's JSON stdout |
| `gate.ran` | bool | Whether Gate B (cluster e2e) was attempted |
| `gate.passed` | bool? | `true` if Gate B p50 median regressed ≤`GATE_REGRESS_PCT_MAX` (50%) vs baseline |
| `gate.e2e_p50_ns` | u64? | **Gated** metric — median of `submit_to_resp_p50_ns` across the N samples |
| `gate.e2e_p99_ns` | u64? | Median `submit_to_resp_p99_ns` across the N samples — recorded, not gated |
| `gate.baseline` | u64? | Echo of `--baseline-gate-ns` (champion p50) used for the comparison |
| `gate.regress_pct` | f64? | `(p50_median - baseline) / baseline * 100` |
| `gate.reason` | string? | Why the gate was skipped (if `ran=false`) |
| `stderr_tail` | string? | Last ~50 lines of stderr+stdout on failure |

**Statuses:**

| Status | TSV status | Meaning |
|--------|-----------|---------|
| `pass` | keep/discard | All stages ran; check `gate` and `metrics` to decide |
| `build_failed` | crash | `cargo build` failed |
| `torture_failed` | crash | Frozen torture suite failed |
| `microbench_failed` | crash | Microbench spawn/exit/JSON-parse failed |
| `tests_failed` | crash | Gate A (`cargo test`) failed |
| `e2e_failed` | crash | Gate B cluster e2e failed |
| `timeout` | crash | A stage exceeded its hard wall-clock budget |
| `unknown_task` | crash | `--task` value not in `task_spec.rs` |

## Standalone perf gate

```bash
make perf/check      # run both microbench binaries in --check mode (~3-6 min)
make perf/baseline   # re-record baselines from the current build
```

**Important:** the committed baseline JSONs (`autobench/baselines/*.json`) were
recorded on a noisy virtualized sandbox host — tolerances were widened
substantially (some metrics ±60–275%). Re-record with `make perf/baseline` on
the real bench machine before relying on `make perf/check` as a regression
gate. Each baseline JSON carries a `"note"` field that says the same.

## Subagent/model dispatch

| Step | Model | Notes |
|------|-------|-------|
| Hypothesis generation | opus | Champion description + last ~10 TSV rows + hotspot summary → one-line hypothesis + file sketch |
| Implementation | sonnet | Escalate to opus for lock-free code, unsafe, or fsync ordering; also after two failed build attempts |
| Failure triage | haiku | Summarize `stderr_tail` to ≤5 lines; dispatch a sonnet fix for trivial errors (max 2 attempts) |
| Profiling (every ~10 iters or on plateau) | sonnet | Run `perf`/flamegraph, return top-10 hotspot summary |

## Adding a task

See `autobench/tasks/TEMPLATE.md` for the full 6-step guide. Summary:

1. Write `autobench/tasks/<id>/program.md` (objective, metrics, mutable/frozen
   paths, gates, agent model, noise, TSV schema).
2. Add a fitness bin under `autobench/src/bin/` with `--json`/`--check`/`--write-baseline`
   (name is set explicitly in `TaskSpec::microbench_bin`, not derived from the id).
3. Add a frozen torture suite under `autobench/tests/` (name set in `TaskSpec::torture_test`).
4. Add a `TaskSpec` row to `autobench/src/task_spec.rs`.
5. Add the microbench to `make perf/check` and `make perf/baseline` in the
   root `Makefile`; run `make perf/baseline` to record initial baselines.
6. Create `autobench/tasks/<id>/results.tsv` with the header row only.

## Per-iteration cost

~5–15 min per iteration: build (~1 min) + torture (~30 s) + microbench
(~2 min) + Gate A tests (~2-5 min) + Gate B cluster e2e (~60 s+ including
cluster rebuild, conditional on microbench being a plausible winner).
