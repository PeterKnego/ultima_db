# Task template

Every optimization task lives in `autobench/tasks/<task>/` and is registered
by adding one `TaskSpec` row in `autobench/src/task_spec.rs`.

## Files per task

- `autobench/tasks/<task>/program.md` — mutable paths, frozen paths, primary
  and secondary metrics, TSV schema, and task-specific constraints.
  Modeled on `autobench/tasks/journal-commit/program.md`.
- `autobench/tasks/<task>/results.tsv` — committed run log, tab-separated.
  First column `commit`, last two columns `status` (keep|discard|crash) and
  `description`. Metric columns in between. Numeric values are median-of-N
  (note N in the description column).
- `autobench/baselines/<task>.json` — machine-recorded baseline for
  `make perf/check` / `make perf/baseline`. Each metric entry has `value`,
  `tolerance_pct`, and `direction`. The JSON carries a `"note"` field that
  warns when baselines were recorded on a noisy host. Re-record on the real
  bench machine before trusting the gate.

## Conventions (all tasks)

- Integer ns values for latency; float entries/sec for throughput.
- Warmup + fixed iteration counts; never single-sample a noisy percentile.
- Median-of-5 for all reported values; re-run with `--skip-tests` when the
  delta is within noise (a KEEP still requires one fully-gated run).
- No wall-clock `Date` and no `rand` in bench logic; vary inputs by index.
- Frozen conformance semantics are never removed to win a number (Goodhart
  trap).
- A change is KEEP only if it beats the champion beyond run-to-run noise on
  the primary metric without regressing the secondary metrics or the Goodhart
  gate.

## Registering a new task (6 steps)

1. **Task overlay** — create `autobench/tasks/<id>/program.md` following the
   structure of `autobench/tasks/journal-commit/program.md`: objective,
   primary/secondary metrics (name + direction), mutable paths, frozen paths,
   floor, gates, implementation-agent model, noise level, TSV schema.

2. **Fitness binary** — add a bin under `autobench/src/bin/` (any name; it is
   wired up explicitly via `TaskSpec::microbench_bin`, not derived from the
   task id — e.g. journal-commit uses `journal-microbench`).  
   Required interface:
   - `METRIC_KEYS: &[&str]` — list of JSON keys emitted on stdout.
   - `Config` struct with `standard()`, `quick()`, and `from_env()` methods
     (`from_env` reads `AUTOBENCH_QUICK=1` to select quick config).
   - CLI flags: `--json` (emit one JSON object on stdout), `--check` (check
     against a baseline file, exit non-zero on regression), `--write-baseline`
     (record current measurements to a baseline file).
   - The primary metric key must match the `primary_metric` field in the
     `TaskSpec` row.

3. **Frozen torture suite** — add `autobench/tests/<name>_torture.rs` (wired via `TaskSpec::torture_test` — e.g. journal-commit uses `journal_torture`).  
   This is a behavioral correctness suite, not a perf suite. It must pass
   before any KEEP is recorded. Do not skip this step.

4. **TaskSpec row** — add to `autobench/src/task_spec.rs`:
   ```rust
   "<id>" => Some(TaskSpec {
       task: "<id>",
       microbench_bin: "<id>-microbench",
       primary_metric: "<metric_key>",
       direction: Direction::Maximize,   // or Minimize
       torture_test: "<id>_torture",
       cluster_gate: true,               // false only if genuinely ungateable
   }),
   ```
   The `task_spec()` function maps the `--task` CLI arg to this struct;
   `run-iter` uses it to dispatch without forking.

5. **Baseline file** — create `autobench/baselines/<id>.json` by running:
   ```
   make perf/baseline
   ```
   after adding the `--check`/`--write-baseline` logic to the microbench
   binary. Add it to `make perf/check` and `make perf/baseline` in the root
   Makefile, mirroring the existing entries.

6. **TSV header** — create `autobench/tasks/<id>/results.tsv` containing only
   the header row from the program.md TSV schema block (tab-separated).
