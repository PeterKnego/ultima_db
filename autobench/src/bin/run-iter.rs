// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! run-iter — one-command per-iteration harness for ultima_db autobench.
//! Stages: build → torture → microbench → tests (Gate A) → cluster e2e
//! (Gate B, conditional). One JSON object on stdout; exit 0 even on stage
//! failure (the agent reads `status`, not the exit code). Ported from
//! ultima_cluster/uc_autobench's run-iter.
//!
//! ## CWD assumption
//!
//! The local `cargo build/test/run` subprocesses (build, torture, microbench,
//! Gate A) inherit run-iter's own current working directory — they do NOT set
//! `current_dir`. The optimization loop MUST invoke run-iter from the
//! workspace root so these cargo invocations resolve the workspace correctly.
//! Only the Gate B cluster command sets `current_dir` (to `--cluster-dir`).
//!
//! ## Gate B and worktrees
//!
//! Gate B builds `ultima_cluster`, whose Cargo path-deps point at
//! `../ultima_db` — a fixed relative path. If the loop runs inside a git
//! worktree (e.g. `.claude/worktrees/...`), the cluster build links the
//! PRIMARY checkout's code, not the edited worktree, and the gate measures
//! the wrong tree. Run the optimization loop from the primary checkout on a
//! branch, not from a worktree.
//!
//! ## AUTOBENCH_QUICK
//!
//! The microbench subprocess inherits the environment, including
//! `AUTOBENCH_QUICK`. The loop must NOT set `AUTOBENCH_QUICK` for real
//! iterations: quick configs produce low-fidelity metrics that would corrupt
//! committed baselines. `AUTOBENCH_QUICK=1` is for dry-runs/smoke tests only.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use clap::Parser;
use serde::Serialize;
use ultima_autobench::baseline::Direction;
use ultima_autobench::task_spec::task_spec;
use wait_timeout::ChildExt;

#[derive(Parser, Debug)]
#[command(name = "run-iter")]
struct Args {
    /// journal-commit | smr-apply
    #[arg(long)]
    task: String,
    #[arg(long)]
    json: bool,
    /// Current champion's primary-metric value (omit on first iteration).
    #[arg(long)]
    baseline_primary: Option<f64>,
    /// Current champion's cluster-gate p99 ns (omit on first iteration).
    #[arg(long)]
    baseline_gate_p99_ns: Option<u64>,
    /// Path to the ultima_cluster checkout for Gate B.
    #[arg(long, default_value = "../ultima_cluster")]
    cluster_dir: std::path::PathBuf,
    /// Skip Gate A (full test suites). For tight inner loops only; a KEEP
    /// must still come from a gated run.
    #[arg(long)]
    skip_tests: bool,
}

#[derive(Serialize, Debug, Default)]
struct Output {
    /// pass | build_failed | torture_failed | microbench_failed |
    /// tests_failed | e2e_failed | timeout | unknown_task
    status: String,
    stage: String,
    duration_s: Durations,
    metrics: Option<serde_json::Value>,
    gate: Gate,
    stderr_tail: Option<String>,
}

#[derive(Serialize, Debug, Default)]
struct Durations {
    build: f64,
    torture: f64,
    microbench: f64,
    tests: f64,
    e2e: f64,
}

#[derive(Serialize, Debug, Default)]
struct Gate {
    ran: bool,
    passed: Option<bool>,
    e2e_p99_ns: Option<u64>,
    baseline: Option<u64>,
    regress_pct: Option<f64>,
    reason: Option<String>,
}

#[derive(Debug, PartialEq)]
enum GateDecision {
    Run,
    Skip(String),
}

/// Run the expensive gates only when the variant is a plausible winner:
/// within 5% of the champion in the metric's good direction.
fn gate_decision(primary: f64, baseline: Option<f64>, direction: Direction) -> GateDecision {
    let Some(b) = baseline else {
        return GateDecision::Run;
    };
    let plausible = match direction {
        Direction::Minimize => primary <= b * 1.05,
        Direction::Maximize => primary >= b * 0.95,
    };
    if plausible {
        GateDecision::Run
    } else {
        GateDecision::Skip("skipped_microbench_not_plausible".into())
    }
}

/// Percent change of `value` relative to `baseline`. Positive = regression
/// (assuming minimize-direction metrics). Returns 0.0 when baseline is 0.
fn regress_pct(value: u64, baseline: u64) -> f64 {
    if baseline == 0 {
        return 0.0;
    }
    ((value as f64) - (baseline as f64)) / (baseline as f64) * 100.0
}

/// Return the last `n` lines of `s`. If `s` has fewer than `n` lines, the
/// whole input is returned. A trailing newline is preserved if present.
fn tail_lines(s: &str, n: usize) -> String {
    let mut lines: Vec<&str> = s.split_inclusive('\n').collect();
    if lines.len() > n {
        lines = lines.split_off(lines.len() - n);
    }
    lines.concat()
}

/// Result of running a subprocess.
struct StageRun {
    /// Process exit status, or None if killed by timeout.
    exit_ok: bool,
    /// Combined stderr (and stdout for non-JSON stages).
    stderr: String,
    /// Stdout, captured separately so JSON-emitting stages can parse it.
    stdout: String,
    /// Wall-clock duration.
    duration_s: f64,
    /// True if the process was killed by the watchdog.
    timed_out: bool,
}

/// Spawn a command, capture stdout+stderr, enforce a hard wall-clock timeout.
/// On timeout, kill the process tree and return `timed_out = true`.
///
/// stdout and stderr are drained concurrently in background threads so the
/// child can't block on a full pipe buffer. The e2e gate in particular emits
/// torrential WARN traces on stderr; without concurrent drainage the child
/// runs ~10× slower and overshoots the gate budget.
fn run_stage(mut cmd: Command, timeout: Duration) -> StageRun {
    use std::io::Read;
    let started = Instant::now();
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return StageRun {
                exit_ok: false,
                stderr: format!("failed to spawn: {e}"),
                stdout: String::new(),
                duration_s: 0.0,
                timed_out: false,
            };
        }
    };

    let mut stdout_pipe = child.stdout.take().expect("stdout piped");
    let mut stderr_pipe = child.stderr.take().expect("stderr piped");
    let stdout_handle = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = stdout_pipe.read_to_string(&mut s);
        s
    });
    let stderr_handle = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = stderr_pipe.read_to_string(&mut s);
        s
    });

    let status = match child.wait_timeout(timeout).expect("wait_timeout") {
        Some(s) => Some(s),
        None => {
            let _ = child.kill();
            let _ = child.wait();
            None
        }
    };

    let stdout = stdout_handle.join().unwrap_or_default();
    let stderr = stderr_handle.join().unwrap_or_default();

    StageRun {
        exit_ok: matches!(status, Some(s) if s.success()),
        stderr,
        stdout,
        duration_s: started.elapsed().as_secs_f64(),
        timed_out: status.is_none(),
    }
}

fn emit_and_exit(out: &Output) -> ! {
    println!("{}", serde_json::to_string(out).expect("serialize Output"));
    std::process::exit(0);
}

/// Extract a u64 from a JSON value. Accepts an integer (`u64`) or a float
/// that is rounded to the nearest integer (the microbench emits its
/// percentile timings as fractional nanoseconds).
fn extract_u64(v: &serde_json::Value) -> Option<u64> {
    if let Some(n) = v.as_u64() {
        return Some(n);
    }
    if let Some(f) = v.as_f64()
        && f.is_finite()
        && f >= 0.0
    {
        return Some(f.round() as u64);
    }
    None
}

fn fail(out: &mut Output, status: &str, stage: &str, run: &StageRun) -> ! {
    out.status = if run.timed_out {
        "timeout".into()
    } else {
        status.into()
    };
    out.stage = stage.into();
    out.stderr_tail = Some(tail_lines(
        &format!("{}\n--- stdout ---\n{}", run.stderr, run.stdout),
        50,
    ));
    emit_and_exit(out);
}

fn cargo(args: &[&str]) -> Command {
    let mut c = Command::new("cargo");
    c.args(args);
    c
}

fn main() {
    let args = Args::parse();
    let Some(spec) = task_spec(&args.task) else {
        emit_and_exit(&Output {
            status: "unknown_task".into(),
            stage: "setup".into(),
            stderr_tail: Some(format!(
                "unknown task {:?}; known: journal-commit, smr-apply",
                args.task
            )),
            ..Default::default()
        });
    };
    if !args.json {
        eprintln!("run-iter: only --json output mode is supported");
        std::process::exit(2);
    }
    let mut out = Output::default();

    // 1: build
    let r = run_stage(
        cargo(&["build", "--release", "-p", "ultima-autobench"]),
        Duration::from_secs(600),
    );
    out.duration_s.build = r.duration_s;
    if r.timed_out || !r.exit_ok {
        fail(&mut out, "build_failed", "build", &r);
    }

    // 2: frozen torture suite
    let r = run_stage(
        cargo(&[
            "test",
            "-p",
            "ultima-autobench",
            "--test",
            spec.torture_test,
            "--release",
        ]),
        Duration::from_secs(300),
    );
    out.duration_s.torture = r.duration_s;
    if r.timed_out || !r.exit_ok {
        fail(&mut out, "torture_failed", "torture", &r);
    }

    // 3: fitness microbench
    let r = run_stage(
        cargo(&[
            "run",
            "-p",
            "ultima-autobench",
            "--bin",
            spec.microbench_bin,
            "--release",
            "--quiet",
            "--",
            "--json",
        ]),
        Duration::from_secs(600),
    );
    out.duration_s.microbench = r.duration_s;
    if r.timed_out || !r.exit_ok {
        fail(&mut out, "microbench_failed", "microbench", &r);
    }
    let metrics: serde_json::Value = match serde_json::from_str(r.stdout.trim()) {
        Ok(v) => v,
        Err(e) => {
            out.status = "microbench_failed".into();
            out.stage = "microbench".into();
            out.stderr_tail = Some(format!(
                "microbench stdout was not valid JSON: {e}\n--- raw ---\n{}",
                tail_lines(&r.stdout, 30)
            ));
            emit_and_exit(&out);
        }
    };
    let primary = metrics
        .get(spec.primary_metric)
        .and_then(serde_json::Value::as_f64);
    out.metrics = Some(metrics);
    let Some(primary) = primary else {
        out.status = "microbench_failed".into();
        out.stage = "microbench".into();
        out.stderr_tail = Some(format!("missing primary metric `{}`", spec.primary_metric));
        emit_and_exit(&out);
    };

    // Gate decision (covers both Gate A cost and Gate B cost)
    if let GateDecision::Skip(reason) = gate_decision(primary, args.baseline_primary, spec.direction)
    {
        out.gate = Gate {
            ran: false,
            reason: Some(reason),
            ..Default::default()
        };
        out.status = "pass".into();
        out.stage = "microbench".into();
        emit_and_exit(&out);
    }

    // 4: Gate A — full test suites (workspace verification rule)
    if !args.skip_tests {
        for cmd_args in [
            vec!["test", "--features", "persistence"],
            vec!["test", "-p", "ultima-journal"],
        ] {
            let r = run_stage(cargo(&cmd_args), Duration::from_secs(900));
            out.duration_s.tests += r.duration_s;
            if r.timed_out || !r.exit_ok {
                fail(&mut out, "tests_failed", "tests", &r);
            }
        }
    }

    // 5: Gate B — cluster e2e (shmem-e2e drives journal + store via path deps)
    if !spec.cluster_gate || !args.cluster_dir.join("Cargo.toml").exists() {
        out.gate = Gate {
            ran: false,
            reason: Some(if spec.cluster_gate {
                "cluster_dir_not_found".into()
            } else {
                "no_cluster_gate_for_task".into()
            }),
            ..Default::default()
        };
        out.status = "pass".into();
        // Report the last stage that actually executed.
        out.stage = if args.skip_tests { "microbench" } else { "tests" }.into();
        emit_and_exit(&out);
    }
    let mut e2e = cargo(&[
        "run",
        "-p",
        "uc_autobench",
        "--bin",
        "shmem-e2e",
        "--release",
        "--quiet",
        "--",
        "--json",
    ]);
    e2e.current_dir(&args.cluster_dir);
    let r = run_stage(e2e, Duration::from_secs(900)); // includes cluster rebuild
    out.duration_s.e2e = r.duration_s;
    if r.timed_out || !r.exit_ok {
        fail(&mut out, "e2e_failed", "e2e", &r);
    }
    let e2e_json: serde_json::Value = match serde_json::from_str(r.stdout.trim()) {
        Ok(v) => v,
        Err(e) => {
            out.status = "e2e_failed".into();
            out.stage = "e2e".into();
            out.stderr_tail = Some(format!(
                "e2e stdout not JSON: {e}\n{}",
                tail_lines(&r.stdout, 30)
            ));
            emit_and_exit(&out);
        }
    };
    let value = e2e_json.get("submit_to_resp_p99_ns").and_then(extract_u64);
    let rp = match (value, args.baseline_gate_p99_ns) {
        (Some(v), Some(b)) => Some(regress_pct(v, b)),
        _ => None,
    };
    out.gate = Gate {
        ran: true,
        passed: Some(rp.map(|p| p <= 5.0).unwrap_or(true)),
        e2e_p99_ns: value,
        baseline: args.baseline_gate_p99_ns,
        regress_pct: rp,
        reason: None,
    };
    out.status = "pass".into();
    out.stage = "e2e".into();
    emit_and_exit(&out);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimize_gate_decisions() {
        assert_eq!(
            gate_decision(100.0, None, Direction::Minimize),
            GateDecision::Run
        );
        assert_eq!(
            gate_decision(104.0, Some(100.0), Direction::Minimize),
            GateDecision::Run
        );
        assert!(matches!(
            gate_decision(106.0, Some(100.0), Direction::Minimize),
            GateDecision::Skip(_)
        ));
    }

    #[test]
    fn maximize_gate_decisions() {
        assert_eq!(
            gate_decision(96.0, Some(100.0), Direction::Maximize),
            GateDecision::Run
        );
        assert!(matches!(
            gate_decision(94.0, Some(100.0), Direction::Maximize),
            GateDecision::Skip(_)
        ));
    }

    #[test]
    fn regress_pct_basic() {
        assert!((regress_pct(105, 100) - 5.0).abs() < 1e-9);
        assert_eq!(regress_pct(100, 0), 0.0);
    }

    #[test]
    fn tail_lines_short_input_returned_whole() {
        let s = "a\nb\nc\n";
        assert_eq!(tail_lines(s, 50), "a\nb\nc\n");
    }

    #[test]
    fn tail_lines_returns_last_n_lines() {
        let s = "a\nb\nc\nd\ne\nf\n";
        // Last 3 lines: d, e, f
        assert_eq!(tail_lines(s, 3), "d\ne\nf\n");
    }

    #[test]
    fn tail_lines_no_trailing_newline() {
        let s = "a\nb\nc";
        assert_eq!(tail_lines(s, 2), "b\nc");
    }
}
