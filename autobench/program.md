# autobench — generic optimization loop

You (Claude Code) are the loop orchestrator. The human supplies a task name;
everything else runs without questions, pauses, or approval. The only stop
signal is Ctrl-C. If the setup itself is broken, fix it and continue.

## Autonomy — DO NOT ASK QUESTIONS, DO NOT PAUSE

**The human is not at the keyboard.** There is nobody to answer a clarifying
question, approve a step, or read a mid-run summary. Once a run is set up, you
execute the loop continuously and silently until the human presses Ctrl-C. This
is the single most-violated rule of this framework, so internalize it:

- **Never ask the user anything** — not "should I continue?", not "which
  variant next?", not "is this setup right?". Resolve every ambiguity yourself
  by picking the most promising untried hypothesis and proceeding.
- **Never stop to summarize or wait for approval** between iterations. After
  you KEEP or DISCARD an iteration and append the TSV row, immediately start
  the next one. Treat every natural stopping point as "begin iteration N+1".
- **Never wait for confirmation to commit a win or revert a loss.** The
  decision rule below is unambiguous — just execute it.
- **The only stop signal is Ctrl-C.** Until then, GOTO the next iteration.
- If the *harness/setup itself* is broken (harness won't build, TSV missing,
  wrong branch, bad CLI args), fix it and keep going — still without asking.

## Ground rules

- **Run from the PRIMARY checkout root on a dedicated branch — never from a
  git worktree.** Gate B builds `ultima_cluster` against its `../ultima_db`
  path dep, which resolves to the primary checkout; inside a worktree
  (`.claude/worktrees/…`) that dep points at the worktree, but the cluster
  build still resolves to the *primary*, so the gate silently measures the
  wrong code.
- **Never set `AUTOBENCH_QUICK` for real iterations.** The microbench inherits
  the environment. Quick configs produce low-fidelity metrics that corrupt
  committed baselines and comparisons. Use `AUTOBENCH_QUICK=1` for
  smoke tests and dry-runs only.
- **Never edit frozen paths** (see the task program.md); the torture suites
  and `autobench/` itself are always frozen.

## Setup (run once at the start of a new run)

1. Confirm task: read `autobench/tasks/<task>/program.md` for task-specific
   constraints (mutable paths, primary metric, baselines, special notes).
2. Confirm the working tree is clean (`git status`). If not, revert or stash
   strays yourself and continue — never stop to ask.
3. Propose a run tag based on today's date (e.g. `jun12`). The branch
   `autoresearch/<task>-<tag>` must not already exist.
4. Create the branch: `git checkout -b autoresearch/<task>-<tag>` from `main`.
5. If `autobench/tasks/<task>/results.tsv` does not exist, create it with the
   header row declared in the task overlay.
6. Begin the loop immediately. Do NOT wait for confirmation.

## State

- **Branch:** `autoresearch/<task>-<tag>` (created from `main` at setup).
- **`autobench/tasks/<task>/results.tsv`** — one row per iteration, committed
  every iteration. Champion = the row with the best `primary` value and
  `status=keep` (direction per the task program.md).
- Git is the only other state.

## Subagent dispatch (MANDATORY)

The orchestrator context must stay small: it never reads source files or raw
bench output itself. Dispatch heavy steps to subagents via the Agent tool and
keep only their summaries:

| Step | Agent model | Notes |
|------|-------------|-------|
| Hypothesis generation | opus | Input: champion description, last ~10 TSV rows, latest hotspot summary. Output: one-line hypothesis + file-level sketch. |
| Implementation | sonnet | Prompt includes the hypothesis, the task's mutable paths, and constraints. Escalate to opus when the change touches lock-free code, unsafe, or fsync ordering — or after a sonnet attempt fails to build twice. |
| Failure triage | haiku | Summarize `stderr_tail` to ≤5 lines. If the fix looks trivial (typo, import), dispatch a sonnet fix attempt; max 2 attempts, then log as crash. |
| Profiling (every ~10 iters or on plateau) | sonnet | Run `perf`/flamegraph on the microbench, return top-10 hotspot summary; feed it to the next hypothesis agent. |

Rules: escalate-on-failure (haiku→sonnet→opus; never start at the top for
mechanical work); subagents absorb file dumps, only summaries return to the
orchestrator. This is what lets a run go for hours without context exhaustion.
A task program.md may raise the implementation tier for domain-hard tasks.

## Per-iteration sequence

1. Read state: `tail -20 autobench/tasks/<task>/results.tsv`; find champion.
2. Hypothesis subagent → one-line hypothesis.
3. Implementation subagent edits ONLY the task's mutable paths.
4. Run:
   ```
   cargo run -p ultima-autobench --bin run-iter --release -- \
     --task <task> --json \
     --baseline-primary <champion_primary> \
     --baseline-gate-ns <champion_gate_p50> > /tmp/run-iter.json 2>&1
   ```
   (`<champion_gate_p50>` is the champion's `gate.e2e_p50_ns` — the gate now
   measures the e2e **p50 median-of-N**, not a single p99 sample, so it survives
   shared/virtualized-host tail noise.)
5. Parse: `jq '.status, .metrics, .gate, .stderr_tail' /tmp/run-iter.json`.
6. Decide (comparisons use MEDIAN-of-5 microbench runs when the delta is
   within noise — re-run with `--skip-tests` for the extra samples; a KEEP
   still requires one fully-gated run):
   - `pass` AND primary improved AND gate passed → **KEEP**: append TSV row,
     `git add -A && git commit -m "<task>: <description>"`.
   - `pass` but no improvement, or gate failed → **DISCARD**:
     `git checkout -- <mutable_paths>`, append TSV row with `status=discard`,
     `git add` the TSV and commit `discard: <description>`.
   - `*_failed` or `timeout` → triage subagent; ≤2 fix attempts, else
     **CRASH** row, revert mutable paths, commit the TSV.
7. GOTO 1.

## Rules

- NEVER stop on your own and NEVER ask the user a question (see "Autonomy"
  above). The human will interrupt when satisfied. If you're stuck, think
  harder: re-read in-scope files, try combinations of prior near-misses, try
  more radical structural changes.
- NEVER touch frozen files. NEVER add a dependency to `Cargo.toml`. NEVER
  modify `run-iter`, the microbench, torture, or test binaries.
- Simplicity wins: a small gain that adds ugly complexity is not worth it. A
  wash that deletes code is always a keep.
- Use `git checkout -- <paths>` for reverts, not `git reset --hard` (which
  would drop the TSV row you just added).
