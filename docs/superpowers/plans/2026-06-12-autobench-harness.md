# Autobench Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An autoresearch-style optimization harness (`autobench/` workspace member) with two tasks — `journal-commit` and `smr-apply` — whose fitness binaries double as a standalone perf-regression gate (`make perf/check`).

**Architecture:** Ported from `../ultima_cluster/uc_autobench` (Claude-Code-driven loop, `run-iter` consolidation binary, TSV results, git-as-state), extended with: per-step subagent/model orchestration in `program.md`, a `Direction`-aware gate decision (the journal primary metric is maximize), a `--check/--write-baseline` mode on the fitness binaries, and a cross-repo Goodhart gate (uc_autobench's `shmem-e2e --json`). Fitness logic lives in lib modules (`journal_bench.rs`, `smr_bench.rs`); binaries are thin wrappers so smoke tests can call the lib with a quick config.

**Tech Stack:** Rust (edition 2024), clap 4 (derive), serde_json, wait-timeout, crc32fast, ultima-db (`persistence`), ultima-journal. Spec: `docs/superpowers/specs/2026-06-12-autobench-perf-harness-design.md` §3–§7, §9–§10.

**Prerequisite:** The bench-suite-restructure plan (`2026-06-12-bench-suite-restructure.md`) — not a hard build dependency, but Task 9 here retires benches that plan reorganizes; execute that plan first.

**Conventions:** SPDX + copyright header on every new file. `cargo clippy -p ultima-autobench -- -D warnings` before every commit. All fitness code must honor `AUTOBENCH_QUICK=1` (small N, skip real-disk assert) so tests never take minutes.

---

### Task 1: Crate skeleton + pure-logic lib modules

**Files:**
- Create: `autobench/Cargo.toml`, `autobench/src/lib.rs`, `autobench/src/task_spec.rs`, `autobench/src/sampling.rs`, `autobench/src/baseline.rs`
- Modify: root `Cargo.toml` (workspace member)

- [ ] **Step 1: Create the crate**

`autobench/Cargo.toml`:

```toml
[package]
name = "ultima-autobench"
version = "0.1.0"
edition = "2024"
publish = false
description = "Autoresearch-style perf harness for ultima-db / ultima-journal"
license = "Apache-2.0"

[dependencies]
ultima-db = { path = "..", features = ["persistence"] }
ultima-journal = { path = "../ultima_journal" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
clap = { version = "4", features = ["derive"] }
wait-timeout = "0.2"
tempfile = "3"
crc32fast = "1"
```

(If `cargo build` reports a wait-timeout version mismatch, use the version from `../ultima_cluster/Cargo.lock` — uc_autobench already depends on it.)

`autobench/src/lib.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Library half of the autobench harness. Binaries (`run-iter`,
//! `journal-microbench`, `smr-apply-microbench`) are thin wrappers over
//! these modules so smoke tests can drive the same code with quick configs.

pub mod baseline;
pub mod journal_bench;
pub mod sampling;
pub mod smr_bench;
pub mod task_spec;
```

(Comment out the `journal_bench`/`smr_bench` lines until Tasks 3/5 create them.)

Root `Cargo.toml`: add `"autobench"` to `[workspace] members`.

- [ ] **Step 2: Write failing tests for `sampling`**

`autobench/src/sampling.rs` (tests first, at the bottom of the new file with stub fns above; or write the whole file and watch tests pass — these are pure functions, full TDD cycle is: write test, `todo!()` body, red, implement, green):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Batched-sample timing helpers (pattern from uc_autobench's
//! shmem-microbench): time a batch of B ops with one Instant read, record
//! elapsed/B as one fractional per-op sample, compute percentiles over the
//! samples. Amortizes the host clock tick below 1 ns of resolution.

use std::time::Instant;

/// p in [0, 100]. Sorts in place. Empty input returns 0.0.
pub fn percentile(samples: &mut [f64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((samples.len() - 1) as f64 * p / 100.0).round() as usize;
    samples[idx]
}

/// Run `samples` batches of `batch` calls to `op`; return per-op ns samples.
pub fn batched_samples_ns(samples: usize, batch: usize, mut op: impl FnMut()) -> Vec<f64> {
    let mut out = Vec::with_capacity(samples);
    for _ in 0..samples {
        let t = Instant::now();
        for _ in 0..batch {
            op();
        }
        out.push(t.elapsed().as_nanos() as f64 / batch as f64);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_picks_expected_rank() {
        let mut s: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        assert_eq!(percentile(&mut s, 50.0), 50.0);
        assert_eq!(percentile(&mut s, 99.0), 99.0);
        assert_eq!(percentile(&mut s, 100.0), 100.0);
    }

    #[test]
    fn percentile_empty_is_zero() {
        assert_eq!(percentile(&mut [], 99.0), 0.0);
    }

    #[test]
    fn batched_samples_counts_and_positive() {
        let s = batched_samples_ns(10, 100, || std::hint::black_box(1 + 1));
        assert_eq!(s.len(), 10);
        assert!(s.iter().all(|&v| v >= 0.0));
    }
}
```

- [ ] **Step 3: Write `baseline` module with tests**

`autobench/src/baseline.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Committed perf baselines + tolerance checking (`--check` mode).
//! File format: { "metrics": { name: { value, tolerance_pct, direction } } }

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Minimize,
    Maximize,
}

/// Infer direction from the metric name: throughput-style names are
/// maximize, everything else (latencies, durations) is minimize.
pub fn infer_direction(metric: &str) -> Direction {
    if metric.contains("throughput") {
        Direction::Maximize
    } else {
        Direction::Minimize
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetricBaseline {
    pub value: f64,
    pub tolerance_pct: f64,
    pub direction: Direction,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Baselines {
    pub metrics: BTreeMap<String, MetricBaseline>,
}

#[derive(Debug, PartialEq)]
pub struct Breach {
    pub metric: String,
    pub value: f64,
    pub baseline: f64,
    pub regress_pct: f64,
}

impl Baselines {
    pub fn from_metrics(metrics: &BTreeMap<String, f64>, tolerance_pct: f64) -> Self {
        let metrics = metrics
            .iter()
            .map(|(k, &v)| {
                (
                    k.clone(),
                    MetricBaseline {
                        value: v,
                        tolerance_pct,
                        direction: infer_direction(k),
                    },
                )
            })
            .collect();
        Self { metrics }
    }

    /// Every baseline metric must be present and within tolerance.
    /// Metrics in `current` but not in the baseline are ignored.
    pub fn check(&self, current: &BTreeMap<String, f64>) -> Vec<Breach> {
        let mut breaches = Vec::new();
        for (name, base) in &self.metrics {
            let Some(&value) = current.get(name) else {
                breaches.push(Breach {
                    metric: name.clone(),
                    value: f64::NAN,
                    baseline: base.value,
                    regress_pct: f64::NAN,
                });
                continue;
            };
            if base.value == 0.0 {
                continue; // unmeasured baseline slot: never breach
            }
            let regress_pct = match base.direction {
                Direction::Minimize => (value - base.value) / base.value * 100.0,
                Direction::Maximize => (base.value - value) / base.value * 100.0,
            };
            if regress_pct > base.tolerance_pct {
                breaches.push(Breach {
                    metric: name.clone(),
                    value,
                    baseline: base.value,
                    regress_pct,
                });
            }
        }
        breaches
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(value: f64, tol: f64, dir: Direction) -> Baselines {
        let mut b = Baselines::default();
        b.metrics.insert(
            "m".into(),
            MetricBaseline {
                value,
                tolerance_pct: tol,
                direction: dir,
            },
        );
        b
    }

    fn cur(v: f64) -> BTreeMap<String, f64> {
        BTreeMap::from([("m".to_string(), v)])
    }

    #[test]
    fn minimize_within_tolerance_passes() {
        assert!(base(100.0, 10.0, Direction::Minimize).check(&cur(109.0)).is_empty());
    }

    #[test]
    fn minimize_over_tolerance_breaches() {
        let b = base(100.0, 10.0, Direction::Minimize).check(&cur(111.0));
        assert_eq!(b.len(), 1);
        assert!((b[0].regress_pct - 11.0).abs() < 1e-9);
    }

    #[test]
    fn maximize_drop_breaches() {
        // throughput fell 11% below baseline
        let b = base(1000.0, 10.0, Direction::Maximize).check(&cur(890.0));
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn maximize_gain_passes() {
        assert!(base(1000.0, 10.0, Direction::Maximize).check(&cur(2000.0)).is_empty());
    }

    #[test]
    fn missing_metric_breaches() {
        let b = base(100.0, 10.0, Direction::Minimize).check(&BTreeMap::new());
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn json_round_trip() {
        let b = Baselines::from_metrics(&cur(42.0), 10.0);
        let s = serde_json::to_string_pretty(&b).unwrap();
        let back: Baselines = serde_json::from_str(&s).unwrap();
        assert_eq!(back.metrics["m"].value, 42.0);
        assert_eq!(back.metrics["m"].direction, Direction::Minimize);
    }

    #[test]
    fn infer_direction_by_name() {
        assert_eq!(infer_direction("group_commit_throughput"), Direction::Maximize);
        assert_eq!(infer_direction("apply_p99_ns"), Direction::Minimize);
    }
}
```

- [ ] **Step 4: Write `task_spec` (port of uc_autobench's registry, plus direction/gates)**

`autobench/src/task_spec.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Per-task descriptors for `run-iter`. Adding a task = adding a TaskSpec
//! row plus its binaries/tests, not forking run-iter. Pattern from
//! ultima_cluster/uc_autobench/src/task_spec.rs.

use crate::baseline::Direction;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSpec {
    /// Task identifier (matches `autobench/tasks/<task>/`).
    pub task: &'static str,
    /// Cargo `--bin` name of the fitness binary.
    pub microbench_bin: &'static str,
    /// JSON key in the microbench stdout used for the KEEP/DISCARD gate.
    pub primary_metric: &'static str,
    pub direction: Direction,
    /// `--test` name of the frozen conformance suite.
    pub torture_test: &'static str,
    /// Whether to run the cross-repo cluster e2e gate (shmem-e2e).
    pub cluster_gate: bool,
}

pub fn task_spec(task: &str) -> Option<TaskSpec> {
    match task {
        "journal-commit" => Some(TaskSpec {
            task: "journal-commit",
            microbench_bin: "journal-microbench",
            primary_metric: "group_commit_throughput",
            direction: Direction::Maximize,
            torture_test: "journal_torture",
            cluster_gate: true,
        }),
        "smr-apply" => Some(TaskSpec {
            task: "smr-apply",
            microbench_bin: "smr-apply-microbench",
            primary_metric: "apply_p99_ns",
            direction: Direction::Minimize,
            torture_test: "smr_apply_torture",
            cluster_gate: true,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_tasks_resolve() {
        let j = task_spec("journal-commit").unwrap();
        assert_eq!(j.primary_metric, "group_commit_throughput");
        assert_eq!(j.direction, Direction::Maximize);
        let s = task_spec("smr-apply").unwrap();
        assert_eq!(s.torture_test, "smr_apply_torture");
        assert_eq!(s.direction, Direction::Minimize);
    }

    #[test]
    fn unknown_task_is_none() {
        assert!(task_spec("nope").is_none());
    }
}
```

- [ ] **Step 5: Run tests, lint, commit**

```bash
cargo test -p ultima-autobench
cargo clippy -p ultima-autobench -- -D warnings
git add -A && git commit -m "feat(autobench): crate skeleton with sampling, baseline, task_spec modules"
```

---

### Task 2: `journal_torture` conformance floor (frozen)

**Files:**
- Create: `autobench/tests/journal_torture.rs`

These tests pin behavior the optimization loop must never break. They should PASS against the current journal — write them, run them, fix the *test* (not the journal) if an assumption about semantics is wrong.

- [ ] **Step 1: Check purge/truncate semantics before asserting them**

```bash
grep -rn "purge_before\|truncate_after" ultima_journal/tests/*.rs ultima_journal/src/journal/mod.rs | grep -i "doc\|///\|assert" | head -30
```

Confirm: `truncate_after(keep_seq)` retains seq ≤ keep_seq; `purge_before(seq)` drops seq ≤ seq (head removal). Adjust the assertions below if the docs say otherwise.

- [ ] **Step 2: Write the suite**

`autobench/tests/journal_torture.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! FROZEN conformance floor for the `journal-commit` autobench task.
//! The optimization loop must never edit this file. Each test is a
//! behavioral invariant; CRC trailers catch torn/corrupted payloads.

use ultima_journal::{Durability, Journal, JournalConfig};

/// payload = 56 bytes derived from seq + 4-byte crc32 trailer.
fn payload_for(seq: u64) -> Vec<u8> {
    let mut p = Vec::with_capacity(60);
    for i in 0..7u64 {
        p.extend_from_slice(&(seq.wrapping_mul(i + 1)).to_le_bytes());
    }
    let crc = crc32fast::hash(&p);
    p.extend_from_slice(&crc.to_le_bytes());
    p
}

fn verify_payload(seq: u64, payload: &[u8]) {
    let (body, trailer) = payload.split_at(payload.len() - 4);
    let crc = u32::from_le_bytes(trailer.try_into().unwrap());
    assert_eq!(crc, crc32fast::hash(body), "torn/corrupt payload at seq {seq}");
    assert_eq!(payload, payload_for(seq).as_slice(), "wrong payload at seq {seq}");
}

fn open(dir: &std::path::Path, durability: Durability) -> Journal {
    let mut cfg = JournalConfig::new(dir);
    cfg.durability = durability;
    Journal::open(cfg).unwrap()
}

#[test]
fn fifo_round_trip_no_loss_no_torn_reads() {
    let dir = tempfile::tempdir().unwrap();
    let j = open(dir.path(), Durability::Eventual);
    const N: u64 = 10_000;
    let mut last = None;
    for seq in 1..=N {
        last = Some(j.append(seq, seq * 2, &payload_for(seq)).unwrap());
    }
    last.unwrap().wait().unwrap();
    let recs = j.read_range(1..=N).unwrap();
    assert_eq!(recs.len(), N as usize, "lost records");
    for (i, (meta, payload)) in recs.iter().enumerate() {
        let seq = i as u64 + 1;
        assert_eq!(*meta, seq * 2, "meta mismatch at {seq}");
        verify_payload(seq, payload);
    }
}

#[test]
fn truncate_after_drops_tail_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let j = open(dir.path(), Durability::Consistent);
    for seq in 1..=100u64 {
        j.append(seq, 0, &payload_for(seq)).unwrap().wait().unwrap();
    }
    j.truncate_after(60).unwrap().wait().unwrap();
    assert_eq!(j.last_seq(), Some(60));
    assert!(j.read(61).unwrap().is_none(), "truncated record still readable");
    verify_payload(60, &j.read(60).unwrap().unwrap().1);
    j.close().unwrap();

    let j = open(dir.path(), Durability::Consistent);
    assert_eq!(j.last_seq(), Some(60), "truncate lost across reopen");
    verify_payload(60, &j.read(60).unwrap().unwrap().1);
    // appending after reopen continues cleanly
    j.append(61, 0, &payload_for(61)).unwrap().wait().unwrap();
    assert_eq!(j.last_seq(), Some(61));
}

#[test]
fn purge_before_drops_head_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let j = open(dir.path(), Durability::Consistent);
    for seq in 1..=100u64 {
        j.append(seq, 0, &payload_for(seq)).unwrap().wait().unwrap();
    }
    j.purge_before(40).unwrap();
    assert!(j.read(40).unwrap().is_none(), "purged record still readable");
    verify_payload(41, &j.read(41).unwrap().unwrap().1);
    assert_eq!(j.last_seq(), Some(100));
    j.close().unwrap();

    let j = open(dir.path(), Durability::Consistent);
    assert!(j.read(40).unwrap().is_none(), "purge lost across reopen");
    verify_payload(41, &j.read(41).unwrap().unwrap().1);
    assert_eq!(j.last_seq(), Some(100));
}

#[test]
fn eventual_durability_watermark_clamps_and_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let j = open(dir.path(), Durability::Eventual);
    for seq in 1..=1000u64 {
        j.append(seq, 0, &payload_for(seq)).unwrap();
    }
    assert!(j.durable_seq() <= 1000, "durable_seq ran ahead of appends");
    j.wait_durable(1000).unwrap();
    assert!(j.durable_seq() >= 1000, "wait_durable returned before durability");
}

#[test]
fn consistent_append_is_immediately_durable() {
    let dir = tempfile::tempdir().unwrap();
    let j = open(dir.path(), Durability::Consistent);
    for seq in 1..=50u64 {
        j.append(seq, 0, &payload_for(seq)).unwrap().wait().unwrap();
        assert!(j.durable_seq() >= seq, "consistent ack before fsync at {seq}");
    }
}

#[test]
fn seq_monotonicity_reads_match_window() {
    let dir = tempfile::tempdir().unwrap();
    let j = open(dir.path(), Durability::Eventual);
    for seq in 1..=500u64 {
        j.append(seq, 0, &payload_for(seq)).unwrap();
    }
    j.wait_durable(500).unwrap();
    assert_eq!(j.first_seq(), Some(1));
    assert_eq!(j.last_seq(), Some(500));
    let recs = j.read_range(250..=260).unwrap();
    assert_eq!(recs.len(), 11);
}
```

- [ ] **Step 3: Run, fix test assumptions if needed, commit**

```bash
cargo test -p ultima-autobench --test journal_torture
```

Expected: all pass in <60 s. If `purge_before`/`first_seq` semantics differ from the assertions, align the test with the journal's documented behavior (these are conformance tests of existing behavior, not new requirements).

```bash
git add -A && git commit -m "test(autobench): frozen journal_torture conformance floor"
```

---

### Task 3: `journal_bench` lib + `journal-microbench` binary

**Files:**
- Create: `autobench/src/journal_bench.rs`, `autobench/src/bin/journal-microbench.rs`, `autobench/src/diskcheck.rs`, `autobench/tests/microbench_smoke.rs`
- Modify: `autobench/src/lib.rs` (uncomment `pub mod journal_bench;`, add `pub mod diskcheck;`)

- [ ] **Step 1: Write the smoke test first**

`autobench/tests/microbench_smoke.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Quick-mode smoke tests: the fitness functions must emit every metric key
//! with a positive value. Keeps the microbenches from silently rotting.

use ultima_autobench::journal_bench;

#[test]
fn journal_metrics_all_present_and_positive() {
    let m = journal_bench::run(&journal_bench::Config::quick());
    for key in journal_bench::METRIC_KEYS {
        let v = m.get(*key).copied().unwrap_or(f64::NAN);
        assert!(v.is_finite() && v > 0.0, "metric {key} missing or non-positive: {v}");
    }
}
```

Run: `cargo test -p ultima-autobench --test microbench_smoke` — expected FAIL (module doesn't exist).

- [ ] **Step 2: Real-disk guard module**

`autobench/src/diskcheck.rs` — port `backing_fs()` + `bench_root()` from `benches/wal_bench.rs` (lines ~40–85; copy the two functions verbatim) with these changes: make both `pub`, parameterize the directory name (`pub fn bench_root(name: &str) -> PathBuf` creating `<CARGO_MANIFEST_DIR>/target/<name>`), and replace the hard `assert!` with:

```rust
pub fn assert_real_disk(root: &Path, allow_tmpfs: bool) {
    let (mount, fs) = backing_fs(root);
    eprintln!("[autobench] dir = {} | mount = {mount} | fs = {fs}", root.display());
    if allow_tmpfs {
        return; // quick/CI mode: results are not durability-meaningful anyway
    }
    assert!(
        fs != "tmpfs" && fs != "ramfs",
        "bench dir is on {fs}; fsync is a no-op there — point at a real disk"
    );
}
```

- [ ] **Step 3: Implement `journal_bench`**

`autobench/src/journal_bench.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Fitness function for the `journal-commit` task. Mirrors the Raft-log
//! usage in ultima_cluster (unbatched appends, notifier acks, iter_range
//! replay, truncate/purge with metadata fsync). Emits a flat metric map.

use std::collections::BTreeMap;
use std::time::Instant;

use ultima_journal::{Durability, Journal, JournalConfig};

use crate::diskcheck;
use crate::sampling::{batched_samples_ns, percentile};

/// Every key `run()` emits, in stable order. The smoke test and
/// `--write-baseline` iterate this list.
pub const METRIC_KEYS: &[&str] = &[
    "group_commit_throughput",
    "append_consistent_p99_ns",
    "append_consistent_p99_ns_64b",
    "append_consistent_p99_ns_4k",
    "append_eventual_ack_p99_ns",
    "append_eventual_throughput",
    "replay_throughput_entries_s",
    "truncate_purge_p99_ns",
    "reopen_ms",
];

pub struct Config {
    /// Rounds for the group-commit throughput metric.
    pub group_rounds: usize,
    /// Samples for consistent-append latency (batch = 1; fsync-bound).
    pub consistent_samples: usize,
    /// (samples, batch) for eventual ack latency.
    pub eventual_samples: usize,
    pub eventual_batch: usize,
    /// Entry count for fill / replay / reopen.
    pub fill_entries: u64,
    /// Samples for truncate+purge latency.
    pub trunc_samples: usize,
    /// Skip the real-disk assert (tests / CI).
    pub allow_tmpfs: bool,
}

impl Config {
    pub fn standard() -> Self {
        Self {
            group_rounds: 50,
            consistent_samples: 400,
            eventual_samples: 300,
            eventual_batch: 16,
            fill_entries: 100_000,
            trunc_samples: 100,
            allow_tmpfs: false,
        }
    }

    pub fn quick() -> Self {
        Self {
            group_rounds: 4,
            consistent_samples: 20,
            eventual_samples: 20,
            eventual_batch: 4,
            fill_entries: 2_000,
            trunc_samples: 8,
            allow_tmpfs: true,
        }
    }

    /// `standard()` unless AUTOBENCH_QUICK=1.
    pub fn from_env() -> Self {
        if std::env::var("AUTOBENCH_QUICK").as_deref() == Ok("1") {
            Self::quick()
        } else {
            Self::standard()
        }
    }
}

const BURST: u64 = 256;
const PAYLOAD: usize = 1024;

fn fresh(root: &std::path::Path, durability: Durability) -> (tempfile::TempDir, Journal) {
    let dir = tempfile::Builder::new().prefix("jb").tempdir_in(root).unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.durability = durability;
    let j = Journal::open(cfg).unwrap();
    (dir, j)
}

pub fn run(cfg: &Config) -> BTreeMap<String, f64> {
    let root = diskcheck::bench_root("autobench-journal");
    diskcheck::assert_real_disk(&root, cfg.allow_tmpfs);
    let mut m = BTreeMap::new();
    let payload = vec![0xABu8; PAYLOAD];

    // -- group_commit_throughput: 256-entry eventual bursts, wait the last
    {
        let (_d, j) = fresh(&root, Durability::Eventual);
        let mut seq = 0u64;
        let t = Instant::now();
        for _ in 0..cfg.group_rounds {
            let mut last = None;
            for _ in 0..BURST {
                seq += 1;
                last = Some(j.append(seq, 0, &payload).unwrap());
            }
            last.unwrap().wait().unwrap();
        }
        let total = (cfg.group_rounds as u64 * BURST) as f64;
        m.insert("group_commit_throughput".into(), total / t.elapsed().as_secs_f64());
    }

    // -- append_consistent_p99_ns at 64 B / 1 KiB / 4 KiB
    for (key, size) in [
        ("append_consistent_p99_ns_64b", 64usize),
        ("append_consistent_p99_ns", 1024),
        ("append_consistent_p99_ns_4k", 4096),
    ] {
        let (_d, j) = fresh(&root, Durability::Consistent);
        let p = vec![0xABu8; size];
        let mut seq = 0u64;
        let mut s = batched_samples_ns(cfg.consistent_samples, 1, || {
            seq += 1;
            j.append(seq, 0, &p).unwrap().wait().unwrap();
        });
        m.insert(key.into(), percentile(&mut s, 99.0));
    }

    // -- append_eventual_ack_p99_ns (notifier resolution on the write path)
    {
        let (_d, j) = fresh(&root, Durability::Eventual);
        let mut seq = 0u64;
        let mut s = batched_samples_ns(cfg.eventual_samples, cfg.eventual_batch, || {
            seq += 1;
            j.append(seq, 0, &payload).unwrap().wait().unwrap();
        });
        m.insert("append_eventual_ack_p99_ns".into(), percentile(&mut s, 99.0));
    }

    // -- fill once; measure eventual throughput, replay, reopen on it
    {
        let dir = tempfile::Builder::new().prefix("jb").tempdir_in(&root).unwrap();
        let mut jcfg = JournalConfig::new(dir.path());
        jcfg.durability = Durability::Eventual;
        let j = Journal::open(jcfg).unwrap();
        let n = cfg.fill_entries;
        let t = Instant::now();
        for seq in 1..=n {
            j.append(seq, 0, &payload).unwrap();
        }
        j.wait_durable(n).unwrap();
        m.insert("append_eventual_throughput".into(), n as f64 / t.elapsed().as_secs_f64());

        let t = Instant::now();
        let mut count = 0u64;
        for rec in j.iter_range(1..=n).unwrap() {
            let _ = rec.unwrap();
            count += 1;
        }
        assert_eq!(count, n, "replay lost records");
        m.insert("replay_throughput_entries_s".into(), n as f64 / t.elapsed().as_secs_f64());

        j.close().unwrap();
        let mut opens = Vec::new();
        for _ in 0..3 {
            let mut jcfg = JournalConfig::new(dir.path());
            jcfg.durability = Durability::Eventual;
            let t = Instant::now();
            let j = Journal::open(jcfg).unwrap();
            opens.push(t.elapsed().as_secs_f64() * 1e3);
            j.close().unwrap();
        }
        m.insert("reopen_ms".into(), percentile(&mut opens, 50.0));
    }

    // -- truncate_purge_p99_ns: rolling window, truncate tail + purge head
    {
        let (_d, j) = fresh(&root, Durability::Consistent);
        let mut hi = 0u64;
        for _ in 0..128 {
            hi += 1;
            j.append(hi, 0, &payload).unwrap().wait().unwrap();
        }
        let mut lo = 0u64; // purged through
        let mut s = Vec::with_capacity(cfg.trunc_samples);
        for _ in 0..cfg.trunc_samples {
            for _ in 0..64 {
                hi += 1;
                j.append(hi, 0, &payload).unwrap().wait().unwrap();
            }
            let t = Instant::now();
            j.truncate_after(hi - 16).unwrap().wait().unwrap();
            hi -= 16;
            lo += 32;
            j.purge_before(lo).unwrap();
            s.push(t.elapsed().as_nanos() as f64);
        }
        m.insert("truncate_purge_p99_ns".into(), percentile(&mut s, 99.0));
    }

    m
}
```

(`iter_range` returns an iterator of results; if its item type differs — e.g. `(u64, u64, Vec<u8>)` tuples or a struct — adapt the loop body; the metric only needs the count. Check `ultima_journal/src/journal/mod.rs:350` for the exact signature.)

- [ ] **Step 4: The binary**

`autobench/src/bin/journal-microbench.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Fitness binary for the `journal-commit` task. One JSON object on stdout.
//! `--check --baseline <file>` exits 2 on tolerance breach (regression gate).

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Parser;
use ultima_autobench::baseline::Baselines;
use ultima_autobench::journal_bench;

#[derive(Parser)]
struct Args {
    /// Emit machine-readable JSON on stdout (the only mode).
    #[arg(long)]
    json: bool,
    /// Compare against --baseline and exit 2 on breach.
    #[arg(long)]
    check: bool,
    #[arg(long)]
    baseline: Option<PathBuf>,
    /// Write a fresh baselines file from this run's metrics, then exit.
    #[arg(long)]
    write_baseline: Option<PathBuf>,
    /// Tolerance recorded into --write-baseline (fsync-heavy: 10%).
    #[arg(long, default_value_t = 10.0)]
    tolerance_pct: f64,
}

fn main() {
    let args = Args::parse();
    let metrics: BTreeMap<String, f64> = journal_bench::run(&journal_bench::Config::from_env());
    println!("{}", serde_json::to_string(&metrics).unwrap());

    if let Some(path) = args.write_baseline {
        let b = Baselines::from_metrics(&metrics, args.tolerance_pct);
        std::fs::write(&path, serde_json::to_string_pretty(&b).unwrap()).unwrap();
        eprintln!("baseline written to {}", path.display());
        return;
    }
    if args.check {
        let path = args.baseline.expect("--check requires --baseline <file>");
        let b: Baselines =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let breaches = b.check(&metrics);
        if !breaches.is_empty() {
            for br in &breaches {
                eprintln!(
                    "PERF REGRESSION {}: {:.1} vs baseline {:.1} ({:+.1}%)",
                    br.metric, br.value, br.baseline, br.regress_pct
                );
            }
            std::process::exit(2);
        }
        eprintln!("perf check OK ({} metrics within tolerance)", b.metrics.len());
    }
}
```

- [ ] **Step 5: Run smoke test, lint, full-speed sanity run, commit**

```bash
cargo test -p ultima-autobench --test microbench_smoke   # PASS now
cargo clippy -p ultima-autobench -- -D warnings
cargo run -p ultima-autobench --bin journal-microbench --release -- --json | python3 -m json.tool
```

Expected: smoke green; release run finishes in ~2–4 min printing all 9 metrics. Commit:

```bash
git add -A && git commit -m "feat(autobench): journal-commit fitness function + microbench binary"
```

---

### Task 4: `smr_apply_torture` conformance floor (frozen)

**Files:**
- Create: `autobench/tests/smr_apply_torture.rs`

- [ ] **Step 1: Write the suite**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! FROZEN conformance floor for the `smr-apply` autobench task.
//! Pins the SMR contract: version pinning, snapshot/checkpoint round-trips,
//! read isolation, and multi-writer commutative equivalence.

use std::io::Read;

use serde::{Deserialize, Serialize};
use ultima_db::{Persistence, Store, StoreConfig, WriterMode};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Counter {
    name: u64,
    value: i64,
}

fn smr_store(dir: &std::path::Path, mode: WriterMode) -> Store {
    let store = Store::new(StoreConfig {
        writer_mode: mode,
        persistence: Persistence::Smr { dir: dir.to_path_buf() },
        ..StoreConfig::default()
    })
    .unwrap();
    store.register_table::<Counter>("counters").unwrap();
    store.recover().unwrap();
    store
}

/// Apply one log entry at `log_index`: add `delta` to counter row `id`.
fn apply(store: &Store, log_index: u64, id: u64, delta: i64) {
    let mut tx = store.begin_write(Some(log_index)).unwrap();
    {
        let mut t = tx.open_table::<Counter>("counters").unwrap();
        let cur = t.get(id).cloned().expect("row preloaded");
        t.update(id, Counter { value: cur.value + delta, ..cur }).unwrap();
    }
    tx.commit().unwrap();
}

fn preload(store: &Store, rows: u64) {
    let mut tx = store.begin_write(Some(1)).unwrap();
    {
        let mut t = tx.open_table::<Counter>("counters").unwrap();
        for i in 1..=rows {
            t.insert(Counter { name: i, value: 0 }).unwrap();
        }
    }
    tx.commit().unwrap();
}

#[test]
fn pinned_versions_track_log_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = smr_store(dir.path(), WriterMode::SingleWriter);
    preload(&store, 16);
    for idx in 2..=500u64 {
        apply(&store, idx, idx % 16 + 1, 1);
        assert_eq!(store.latest_version(), idx, "version != log index after apply");
    }
}

#[test]
fn snapshot_stream_install_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = smr_store(dir.path(), WriterMode::SingleWriter);
    preload(&store, 100);
    for idx in 2..=200u64 {
        apply(&store, idx, idx % 100 + 1, idx as i64);
    }
    let mut buf = Vec::new();
    store.snapshot_stream(None).unwrap().read_to_end(&mut buf).unwrap();

    let dir2 = tempfile::tempdir().unwrap();
    let store2 = smr_store(dir2.path(), WriterMode::SingleWriter);
    store2
        .install_snapshot_stream(&buf[..], ultima_db::InstallOptions::default())
        .unwrap();
    assert_eq!(store2.latest_version(), store.latest_version());
    let r1 = store.begin_read(None).unwrap();
    let r2 = store2.begin_read(None).unwrap();
    let t1 = r1.open_table::<Counter>("counters").unwrap();
    let t2 = r2.open_table::<Counter>("counters").unwrap();
    for id in 1..=100u64 {
        assert_eq!(t1.get(id), t2.get(id), "row {id} differs after install");
    }
}

#[test]
fn checkpoint_recover_equality() {
    let dir = tempfile::tempdir().unwrap();
    let last;
    {
        let store = smr_store(dir.path(), WriterMode::SingleWriter);
        preload(&store, 50);
        for idx in 2..=300u64 {
            apply(&store, idx, idx % 50 + 1, 1);
        }
        last = store.latest_version();
        store.checkpoint().unwrap();
    }
    let store = smr_store(dir.path(), WriterMode::SingleWriter);
    assert_eq!(store.latest_version(), last, "recover lost the checkpointed version");
    let r = store.begin_read(None).unwrap();
    let t = r.open_table::<Counter>("counters").unwrap();
    let total: i64 = (1..=50u64).map(|id| t.get(id).unwrap().value).sum();
    assert_eq!(total, 299, "counter sum wrong after recover");
}

#[test]
fn reads_are_isolated_during_apply() {
    let dir = tempfile::tempdir().unwrap();
    let store = smr_store(dir.path(), WriterMode::SingleWriter);
    preload(&store, 2);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let store = store.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut idx = 2u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                // one "entry" = +1 to both rows; readers must never see them differ
                let mut tx = store.begin_write(Some(idx)).unwrap();
                {
                    let mut t = tx.open_table::<Counter>("counters").unwrap();
                    for id in 1..=2u64 {
                        let cur = t.get(id).cloned().unwrap();
                        t.update(id, Counter { value: cur.value + 1, ..cur }).unwrap();
                    }
                }
                tx.commit().unwrap();
                idx += 1;
            }
        })
    };
    let t0 = std::time::Instant::now();
    while t0.elapsed().as_millis() < 500 {
        let r = store.begin_read(None).unwrap();
        let t = r.open_table::<Counter>("counters").unwrap();
        let a = t.get(1).unwrap().value;
        let b = t.get(2).unwrap().value;
        assert_eq!(a, b, "snapshot read saw a half-applied entry");
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
}

#[test]
fn multiwriter_commutative_equivalence() {
    let dir = tempfile::tempdir().unwrap();
    let store = smr_store(dir.path(), WriterMode::MultiWriter);
    preload(&store, 8);
    const WRITERS: usize = 4;
    const TXNS_PER_WRITER: usize = 50;
    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let store = store.clone();
            std::thread::spawn(move || {
                for k in 0..TXNS_PER_WRITER {
                    let id = ((w * TXNS_PER_WRITER + k) % 8 + 1) as u64;
                    loop {
                        let mut tx = store.begin_write(None).unwrap();
                        {
                            let mut t = tx.open_table::<Counter>("counters").unwrap();
                            let cur = t.get(id).cloned().unwrap();
                            t.update(id, Counter { value: cur.value + 1, ..cur }).unwrap();
                        }
                        match tx.commit() {
                            Ok(_) => break,
                            Err(ultima_db::Error::WriteConflict { .. }) => continue,
                            Err(e) => panic!("unexpected: {e}"),
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let r = store.begin_read(None).unwrap();
    let t = r.open_table::<Counter>("counters").unwrap();
    let total: i64 = (1..=8u64).map(|id| t.get(id).unwrap().value).sum();
    assert_eq!(total, (WRITERS * TXNS_PER_WRITER) as i64, "parallel increments lost");
}
```

(API checks while writing: `InstallOptions` lives at `src/snapshot_stream/install.rs:55` — confirm its re-export path with `grep -n "InstallOptions" src/lib.rs`; `TableReader::get` returns `Option<&R>` so `.cloned()` needs `R: Clone` — `Counter` derives it. If `install_snapshot_stream` is not a `Store` method, check `src/snapshot_stream/install.rs:105` for the actual receiver and adapt.)

- [ ] **Step 2: Run, fix assumptions, commit**

```bash
cargo test -p ultima-autobench --test smr_apply_torture
git add -A && git commit -m "test(autobench): frozen smr_apply_torture conformance floor"
```

---

### Task 5: `smr_bench` lib + `smr-apply-microbench` binary

**Files:**
- Create: `autobench/src/smr_bench.rs`, `autobench/src/bin/smr-apply-microbench.rs`
- Modify: `autobench/src/lib.rs`, `autobench/tests/microbench_smoke.rs`

- [ ] **Step 1: Extend the smoke test (failing)**

Append to `autobench/tests/microbench_smoke.rs`:

```rust
use ultima_autobench::smr_bench;

#[test]
fn smr_metrics_all_present_and_positive() {
    let m = smr_bench::run(&smr_bench::Config::quick());
    for key in smr_bench::METRIC_KEYS {
        let v = m.get(*key).copied().unwrap_or(f64::NAN);
        assert!(v.is_finite() && v > 0.0, "metric {key} missing or non-positive: {v}");
    }
}
```

- [ ] **Step 2: Implement `smr_bench`**

`autobench/src/smr_bench.rs` — structure mirrors `journal_bench`; the workload mirrors the cluster apply loop (spec §4). Full skeleton with all metric sections:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Fitness function for the `smr-apply` task. Reproduces the cluster apply
//! loop: Persistence::Smr, per-entry WriteTx pinned to the log index,
//! reader thread on begin_read(None), snapshot+checkpoint cadence; plus
//! multi-txn iterations (T=8 sequential, and T=8 across 4 OCC writers).

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use ultima_db::{Persistence, Store, StoreConfig, WriterMode};

use crate::diskcheck;
use crate::sampling::percentile;

pub const METRIC_KEYS: &[&str] = &[
    "apply_p99_ns",
    "apply_throughput",
    "apply_sw_batch_throughput",
    "apply_mw_throughput",
    "apply_mw_p99_ns",
    "read_p99_under_load_ns",
    "snapshot_freeze_p99_us",
    "snapshot_stream_ms",
    "checkpoint_ms",
    "restore_install_ms",
];

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SmrRow {
    pub key: u64,
    pub val: u64,
    pub blob: Vec<u8>, // 56 bytes: keeps rows non-trivial without dwarfing them
}

impl SmrRow {
    fn new(key: u64, val: u64) -> Self {
        Self { key, val, blob: vec![0xCD; 56] }
    }
}

pub struct Config {
    pub preload_rows: u64,
    pub apply_iters: u64,
    /// Snapshot freeze+stream+checkpoint every this many iterations.
    pub snapshot_every: u64,
    pub batch_iters: usize,    // rounds for the T=8 sequential metric
    pub mw_iters: usize,       // rounds for the 4-writer OCC metric
    pub allow_tmpfs: bool,
}

impl Config {
    pub fn standard() -> Self {
        Self {
            preload_rows: 100_000,
            apply_iters: 20_000,
            // Spec §4 says "every 5000" (the cluster default); we use 2000 so a
            // 20k-iteration run yields ~10 snapshot events — enough samples for a
            // stable percentile. Cadence changes sample count, not per-event cost.
            snapshot_every: 2_000,
            batch_iters: 500,
            mw_iters: 300,
            allow_tmpfs: false,
        }
    }
    pub fn quick() -> Self {
        Self {
            preload_rows: 5_000,
            apply_iters: 1_000,
            snapshot_every: 250,
            batch_iters: 20,
            mw_iters: 10,
            allow_tmpfs: true,
        }
    }
    pub fn from_env() -> Self {
        if std::env::var("AUTOBENCH_QUICK").as_deref() == Ok("1") {
            Self::quick()
        } else {
            Self::standard()
        }
    }
}

fn smr_store(dir: &std::path::Path, mode: WriterMode) -> Store {
    let store = Store::new(StoreConfig {
        writer_mode: mode,
        persistence: Persistence::Smr { dir: dir.to_path_buf() },
        ..StoreConfig::default()
    })
    .unwrap();
    store.register_table::<SmrRow>("state").unwrap();
    store.recover().unwrap();
    store
}

pub fn run(cfg: &Config) -> BTreeMap<String, f64> {
    let root = diskcheck::bench_root("autobench-smr");
    diskcheck::assert_real_disk(&root, cfg.allow_tmpfs);
    let mut m = BTreeMap::new();

    // ---- main store: T=1 pinned apply loop + reader thread + snapshots ----
    let dir = tempfile::Builder::new().prefix("smr").tempdir_in(&root).unwrap();
    let store = smr_store(dir.path(), WriterMode::SingleWriter);
    {
        let mut tx = store.begin_write(Some(1)).unwrap();
        {
            let mut t = tx.open_table::<SmrRow>("state").unwrap();
            let batch: Vec<SmrRow> = (1..=cfg.preload_rows).map(|i| SmrRow::new(i, 0)).collect();
            t.insert_batch(batch).unwrap();
        }
        tx.commit().unwrap();
    }

    // reader thread: closed loop of begin_read(None) + point get
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let store = store.clone();
        let stop = stop.clone();
        let rows = cfg.preload_rows;
        std::thread::spawn(move || {
            let mut lat = Vec::with_capacity(1 << 16);
            let mut k = 0u64;
            while !stop.load(Ordering::Relaxed) {
                k = k.wrapping_add(2_654_435_761) % rows + 1;
                let t = Instant::now();
                let r = store.begin_read(None).unwrap();
                let tbl = r.open_table::<SmrRow>("state").unwrap();
                std::hint::black_box(tbl.get(k));
                lat.push(t.elapsed().as_nanos() as f64);
            }
            lat
        })
    };

    const APPLY_BATCH: u64 = 32; // one latency sample per 32 applies
    let mut apply_samples = Vec::new();
    let mut freeze_us = Vec::new();
    let mut stream_ms = Vec::new();
    let mut checkpoint_ms = Vec::new();
    let mut idx = 1u64; // version 1 = preload
    let loop_t = Instant::now();
    while idx - 1 < cfg.apply_iters {
        let bt = Instant::now();
        for _ in 0..APPLY_BATCH {
            idx += 1;
            let mut tx = store.begin_write(Some(idx)).unwrap();
            {
                let mut t = tx.open_table::<SmrRow>("state").unwrap();
                let id = idx % cfg.preload_rows + 1;
                t.update(id, SmrRow::new(id, idx)).unwrap();
            }
            tx.commit().unwrap();
        }
        apply_samples.push(bt.elapsed().as_nanos() as f64 / APPLY_BATCH as f64);

        if (idx - 1) % cfg.snapshot_every < APPLY_BATCH {
            let t = Instant::now();
            let mut rd = store.snapshot_stream(None).unwrap();
            freeze_us.push(t.elapsed().as_nanos() as f64 / 1e3);
            let t = Instant::now();
            let mut sink = Vec::new();
            rd.read_to_end(&mut sink).unwrap();
            stream_ms.push(t.elapsed().as_secs_f64() * 1e3);
            let t = Instant::now();
            store.checkpoint().unwrap();
            checkpoint_ms.push(t.elapsed().as_secs_f64() * 1e3);
        }
    }
    let loop_secs = loop_t.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    let mut read_lat = reader.join().unwrap();

    m.insert("apply_p99_ns".into(), percentile(&mut apply_samples, 99.0));
    m.insert("apply_throughput".into(), (idx - 1) as f64 / loop_secs);
    m.insert("read_p99_under_load_ns".into(), percentile(&mut read_lat, 99.0));
    m.insert("snapshot_freeze_p99_us".into(), percentile(&mut freeze_us, 99.0));
    m.insert("snapshot_stream_ms".into(), percentile(&mut stream_ms, 50.0));
    m.insert("checkpoint_ms".into(), percentile(&mut checkpoint_ms, 50.0));

    // ---- restore_install_ms: stream the final state into a fresh store ----
    {
        let mut buf = Vec::new();
        store.snapshot_stream(None).unwrap().read_to_end(&mut buf).unwrap();
        let dir2 = tempfile::Builder::new().prefix("smr2").tempdir_in(&root).unwrap();
        let store2 = smr_store(dir2.path(), WriterMode::SingleWriter);
        let t = Instant::now();
        store2
            .install_snapshot_stream(&buf[..], ultima_db::InstallOptions::default())
            .unwrap();
        m.insert("restore_install_ms".into(), t.elapsed().as_secs_f64() * 1e3);
        assert_eq!(store2.latest_version(), store.latest_version());
    }

    // ---- apply_sw_batch_throughput: T=8 sequential txns per iteration ----
    {
        const T: u64 = 8;
        let t = Instant::now();
        for i in 0..cfg.batch_iters as u64 {
            for k in 0..T {
                let mut tx = store.begin_write(None).unwrap();
                {
                    let mut tbl = tx.open_table::<SmrRow>("state").unwrap();
                    let id = (i * T + k) % cfg.preload_rows + 1;
                    tbl.update(id, SmrRow::new(id, k)).unwrap();
                }
                tx.commit().unwrap();
            }
        }
        let txns = cfg.batch_iters as f64 * T as f64;
        m.insert("apply_sw_batch_throughput".into(), txns / t.elapsed().as_secs_f64());
    }

    // ---- apply_mw_*: T=8 across 4 parallel OCC writers, disjoint keys ----
    {
        const WRITERS: usize = 4;
        const TXNS_PER_WRITER: u64 = 2;
        let dirm = tempfile::Builder::new().prefix("smrmw").tempdir_in(&root).unwrap();
        let mstore = smr_store(dirm.path(), WriterMode::MultiWriter);
        {
            let mut tx = mstore.begin_write(None).unwrap();
            {
                let mut t = tx.open_table::<SmrRow>("state").unwrap();
                let batch: Vec<SmrRow> = (1..=10_000u64).map(|i| SmrRow::new(i, 0)).collect();
                t.insert_batch(batch).unwrap();
            }
            tx.commit().unwrap();
        }
        let mut commit_lat = Vec::new();
        let mut conflicts = 0u64;
        let t = Instant::now();
        for round in 0..cfg.mw_iters as u64 {
            let barrier = Arc::new(std::sync::Barrier::new(WRITERS));
            let handles: Vec<_> = (0..WRITERS)
                .map(|w| {
                    let store = mstore.clone();
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        barrier.wait();
                        let mut lat = Vec::with_capacity(TXNS_PER_WRITER as usize);
                        let mut conf = 0u64;
                        for k in 0..TXNS_PER_WRITER {
                            // disjoint id ranges per writer: independent txns
                            let id = (w as u64 * 2_500) + (round * TXNS_PER_WRITER + k) % 2_500 + 1;
                            loop {
                                let ct = Instant::now();
                                let mut tx = store.begin_write(None).unwrap();
                                {
                                    let mut tbl = tx.open_table::<SmrRow>("state").unwrap();
                                    tbl.update(id, SmrRow::new(id, k)).unwrap();
                                }
                                match tx.commit() {
                                    Ok(_) => {
                                        lat.push(ct.elapsed().as_nanos() as f64);
                                        break;
                                    }
                                    Err(ultima_db::Error::WriteConflict { .. }) => {
                                        conf += 1;
                                        continue;
                                    }
                                    Err(e) => panic!("unexpected: {e}"),
                                }
                            }
                        }
                        (lat, conf)
                    })
                })
                .collect();
            for h in handles {
                let (lat, conf) = h.join().unwrap();
                commit_lat.extend(lat);
                conflicts += conf;
            }
        }
        let txns = (cfg.mw_iters * WRITERS * TXNS_PER_WRITER as usize) as f64;
        m.insert("apply_mw_throughput".into(), txns / t.elapsed().as_secs_f64());
        m.insert("apply_mw_p99_ns".into(), percentile(&mut commit_lat, 99.0));
        eprintln!("[smr_bench] mw conflicts: {conflicts} across {txns} txns");
    }

    m
}
```

- [ ] **Step 3: The binary**

`autobench/src/bin/smr-apply-microbench.rs`: identical to `journal-microbench.rs` (Task 3 Step 4) with three substitutions: `journal_bench` → `smr_bench`, struct doc says `smr-apply` task, and `--tolerance-pct` default is `5.0` (commit-path noise floor, vs 10% for fsync-heavy journal metrics).

- [ ] **Step 4: Test, lint, sanity run, commit**

```bash
cargo test -p ultima-autobench
cargo clippy -p ultima-autobench -- -D warnings
cargo run -p ultima-autobench --bin smr-apply-microbench --release -- --json | python3 -m json.tool
git add -A && git commit -m "feat(autobench): smr-apply fitness function + microbench binary"
```

---

### Task 6: `run-iter` orchestration binary

**Files:**
- Create: `autobench/src/bin/run-iter.rs`

- [ ] **Step 1: Port the infrastructure helpers**

Copy these items **verbatim** from `/home/claude/ultima/ultima_cluster/uc_autobench/src/bin/run-iter.rs` into the new file: `struct StageRun` (lines 121–133), `fn run_stage` (lines 135–191), `fn tail_lines` (lines 111–119), `fn extract_u64` (lines 198–212), `fn emit_and_exit` (lines 193–196), and the `tail_lines` unit tests (lines 497–513).

- [ ] **Step 2: Write the harness-specific parts**

The rest of `autobench/src/bin/run-iter.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! run-iter — one-command per-iteration harness for ultima_db autobench.
//! Stages: build → torture → microbench → tests (Gate A) → cluster e2e
//! (Gate B, conditional). One JSON object on stdout; exit 0 even on stage
//! failure (the agent reads `status`, not the exit code).

use std::path::PathBuf;
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
    cluster_dir: PathBuf,
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
    let Some(b) = baseline else { return GateDecision::Run };
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

fn regress_pct(value: u64, baseline: u64) -> f64 {
    if baseline == 0 {
        return 0.0;
    }
    ((value as f64) - (baseline as f64)) / (baseline as f64) * 100.0
}

// [ verbatim ports: StageRun, run_stage, tail_lines, extract_u64, emit_and_exit ]

fn fail(out: &mut Output, status: &str, stage: &str, run: &StageRun) -> ! {
    out.status = if run.timed_out { "timeout".into() } else { status.into() };
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
    assert!(args.json, "only --json mode is supported");
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
        cargo(&["test", "-p", "ultima-autobench", "--test", spec.torture_test, "--release"]),
        Duration::from_secs(300),
    );
    out.duration_s.torture = r.duration_s;
    if r.timed_out || !r.exit_ok {
        fail(&mut out, "torture_failed", "torture", &r);
    }

    // 3: fitness microbench
    let r = run_stage(
        cargo(&["run", "-p", "ultima-autobench", "--bin", spec.microbench_bin,
                "--release", "--quiet", "--", "--json"]),
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
            out.stderr_tail = Some(format!("stdout not JSON: {e}\n{}", tail_lines(&r.stdout, 30)));
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
        out.gate = Gate { ran: false, reason: Some(reason), ..Default::default() };
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
        out.stage = "tests".into();
        emit_and_exit(&out);
    }
    let mut e2e = cargo(&["run", "-p", "uc_autobench", "--bin", "shmem-e2e",
                          "--release", "--quiet", "--", "--json"]);
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
            out.stderr_tail = Some(format!("e2e stdout not JSON: {e}\n{}", tail_lines(&r.stdout, 30)));
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
        assert_eq!(gate_decision(100.0, None, Direction::Minimize), GateDecision::Run);
        assert_eq!(gate_decision(104.0, Some(100.0), Direction::Minimize), GateDecision::Run);
        assert!(matches!(
            gate_decision(106.0, Some(100.0), Direction::Minimize),
            GateDecision::Skip(_)
        ));
    }

    #[test]
    fn maximize_gate_decisions() {
        assert_eq!(gate_decision(96.0, Some(100.0), Direction::Maximize), GateDecision::Run);
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

    // [ verbatim port: tail_lines tests ]
}
```

- [ ] **Step 3: Test, lint, dry-run, commit**

```bash
cargo test -p ultima-autobench --bin run-iter
cargo clippy -p ultima-autobench -- -D warnings
AUTOBENCH_QUICK=1 cargo run -p ultima-autobench --bin run-iter --release -- \
  --task journal-commit --json --skip-tests | python3 -m json.tool
```

Expected dry-run: `status: "pass"`, all stage durations populated, gate ran (or `cluster_dir_not_found` if the sibling checkout is absent). Then:

```bash
git add -A && git commit -m "feat(autobench): run-iter orchestration binary with direction-aware gates"
```

---

### Task 7: Baselines + `make perf/check`

**Files:**
- Create: `autobench/baselines/journal-commit.json`, `autobench/baselines/smr-apply.json`
- Modify: `Makefile`

- [ ] **Step 1: Record baselines (full-speed, real disk, quiet machine)**

```bash
mkdir -p autobench/baselines
cargo run -p ultima-autobench --bin journal-microbench --release -- \
  --json --write-baseline autobench/baselines/journal-commit.json
cargo run -p ultima-autobench --bin smr-apply-microbench --release -- \
  --json --write-baseline autobench/baselines/smr-apply.json
```

Then sanity-check the recorded values against the bench A/B noise floors (±2.5% commit-path, ±5–9% alloc/fsync): re-run each microbench once more with `--check --baseline <file>` — both must pass on an idle machine. If a metric flaps, raise its `tolerance_pct` in the JSON by hand and note why in the commit message.

- [ ] **Step 2: Makefile targets**

```makefile
# Perf regression gate (fitness binaries in --check mode, ~3-6 min total)
perf/check:
	cargo run -p ultima-autobench --bin journal-microbench --release -- \
		--json --check --baseline autobench/baselines/journal-commit.json > /dev/null
	cargo run -p ultima-autobench --bin smr-apply-microbench --release -- \
		--json --check --baseline autobench/baselines/smr-apply.json > /dev/null

# Re-record perf baselines (run only after a deliberate perf change lands)
perf/baseline:
	cargo run -p ultima-autobench --bin journal-microbench --release -- \
		--json --write-baseline autobench/baselines/journal-commit.json > /dev/null
	cargo run -p ultima-autobench --bin smr-apply-microbench --release -- \
		--json --write-baseline autobench/baselines/smr-apply.json > /dev/null
```

Add `perf/check perf/baseline` to `.PHONY`.

- [ ] **Step 3: Verify and commit**

```bash
make perf/check    # must pass against the just-recorded baselines
git add -A && git commit -m "feat(autobench): committed perf baselines + make perf/check gate"
```

---

### Task 8: Loop documentation (program.md, tasks, operator manual)

**Files:**
- Create: `autobench/program.md`, `autobench/CLAUDE.md`, `autobench/tasks/TEMPLATE.md`, `autobench/tasks/journal-commit/program.md`, `autobench/tasks/journal-commit/results.tsv`, `autobench/tasks/smr-apply/program.md`, `autobench/tasks/smr-apply/results.tsv`

- [ ] **Step 1: Write `autobench/program.md`** (the generic loop; adapted from `../ultima_cluster/uc_autobench/program.md` with the subagent orchestration the spec §6 requires):

```markdown
# autobench — generic optimization loop

You (Claude Code) are the loop orchestrator. The human supplies a task name;
everything else runs without questions, pauses, or approval. The only stop
signal is Ctrl-C. If the setup itself is broken, fix it and continue.

## State

- Branch: work on `autoresearch/<task>-<tag>` (create from main at start).
- `autobench/tasks/<task>/results.tsv` — one row per iteration, committed
  every iteration. Champion = best `primary` among `status=keep` rows
  (direction per the task program.md).
- Git is the only other state.

## Subagent dispatch (MANDATORY)

The orchestrator context must stay small: it never reads source files or raw
bench output itself. Dispatch heavy steps to subagents via the Agent tool and
keep only their summaries:

| Step                          | Agent model | Notes |
|-------------------------------|-------------|-------|
| Hypothesis generation         | opus        | Input: champion description, last ~10 TSV rows, latest hotspot summary. Output: one-line hypothesis + file-level sketch. |
| Implementation                | sonnet      | Prompt includes the hypothesis, the task's mutable paths, and constraints. Escalate to opus when the change touches lock-free code, unsafe, or fsync ordering — or after a sonnet attempt fails to build twice. |
| Failure triage                | haiku       | Summarize `stderr_tail` to ≤5 lines. If the fix looks trivial (typo, import), dispatch a sonnet fix attempt; max 2 attempts, then log as crash. |
| Profiling (every ~10 iters or on plateau) | sonnet | Run `perf`/flamegraph on the microbench, return top-10 hotspot summary; feed it to the next hypothesis agent. |

Rules: escalate-on-failure (haiku→sonnet→opus; never start at the top for
mechanical work); subagents absorb file dumps, only summaries return.

## Per-iteration sequence

1. Read state: `tail -20 autobench/tasks/<task>/results.tsv`; find champion.
2. Hypothesis subagent → one-line hypothesis.
3. Implementation subagent edits ONLY the task's mutable paths.
4. Run:
   cargo run -p ultima-autobench --bin run-iter --release -- \
     --task <task> --json \
     --baseline-primary <champion_primary> \
     --baseline-gate-p99-ns <champion_gate> > /tmp/run-iter.json 2>&1
5. Read `jq '.status, .metrics, .gate, .stderr_tail' /tmp/run-iter.json`.
6. Decide (comparisons use MEDIAN-of-5 microbench runs when within noise —
   re-run `run-iter --skip-tests` for the extra samples):
   - pass AND primary improved AND gate passed → KEEP: append TSV row,
     `git add -A && git commit`.
   - pass but no improvement, or gate failed → DISCARD:
     `git checkout -- <mutable_paths>`, append TSV row, commit the TSV.
   - *_failed or timeout → triage subagent; ≤2 fix attempts, else CRASH row,
     revert, commit the TSV.
7. GOTO 1.
```

- [ ] **Step 2: Write the two task overlays**

`autobench/tasks/journal-commit/program.md`:

```markdown
# task: journal-commit

Objective: raise sustained journal commit throughput and cut commit latency —
the cluster's binding constraint (~80 commits/s group-commit ceiling).

- Primary metric: `group_commit_throughput` (maximize) — initial campaign.
- Secondary: `append_consistent_p99_ns`, `append_eventual_ack_p99_ns`
  (minimize), `replay_throughput_entries_s` (maximize).
- Mutable paths: `ultima_journal/src/**`
- Frozen: the journal public API surface (signatures in
  `ultima_journal/src/journal/mod.rs`, `lib.rs` re-exports), `autobench/**`,
  all tests and benches.
- Floor: `cargo test -p ultima-autobench --test journal_torture` green.
- Gates: full `cargo test --features persistence` + `cargo test -p
  ultima-journal` (A); cluster `shmem-e2e` p99 within 5% (B).
- Implementation-agent model: opus by default — most wins here are fsync
  ordering / group-commit batching, which is durability-critical.
- Noise: fsync metrics vary ±5–9% between runs; use MEDIAN-of-5.

## results.tsv schema

commit	group_commit_throughput	append_consistent_p99_ns	e2e_p99_ns	memory_kb	status	description

(`memory_kb` is reserved per spec §9 — write 0 until a memory metric exists.)
```

`autobench/tasks/smr-apply/program.md` — same shape with: primary
`apply_p99_ns` (minimize); secondary `apply_throughput`,
`apply_mw_throughput`, `read_p99_under_load_ns`; mutable paths `src/**`
(root crate: btree/table/store/persistence/checkpoint); frozen: public API,
on-disk format compatibility tests, `autobench/**`, `ultima_journal/**`;
floor `smr_apply_torture`; implementation-agent model sonnet (escalate per
generic rules); noise ±2.5–5%, MEDIAN-of-5; TSV schema
`commit	apply_p99_ns	apply_throughput	e2e_p99_ns	memory_kb	status	description`
(`memory_kb` reserved per spec §9, write 0).

- [ ] **Step 3: TSV headers, template, operator manual**

Create each `results.tsv` containing only its header row (copy the schema line from the task program.md, tab-separated).

`autobench/tasks/TEMPLATE.md`: copy `../ultima_cluster/uc_autobench/tasks/TEMPLATE.md` and adjust paths/binary-name placeholders to this repo (task registry: `autobench/src/task_spec.rs`; binaries under `autobench/src/bin/`).

`autobench/CLAUDE.md` (operator manual, ~40 lines): how to start a run
(`claude` in repo root, prompt: "Run the autobench loop for task
journal-commit per autobench/program.md"), how to read results.tsv, how to
add a task (program.md overlay + microbench bin + torture test + TaskSpec
row), per-iteration cost (~5–15 min: build + torture + microbench + gates),
and the subagent/model dispatch table (copy from program.md).

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "docs(autobench): loop spec with subagent orchestration, task overlays, operator manual"
```

---

### Task 9: Retire superseded benches + final verification + feature doc

**Files:**
- Delete: `benches/snapshot_throughput.rs`, `ultima_journal/benches/append_throughput.rs`
- Modify: root `Cargo.toml`, `ultima_journal/Cargo.toml`, `Makefile` (if needed), root `CLAUDE.md`
- Create: `docs/tasks/task35_autobench_perf_harness.md`

- [ ] **Step 1: Retire**

```bash
git rm benches/snapshot_throughput.rs ultima_journal/benches/append_throughput.rs
```

Remove the `[[bench]] name = "snapshot_throughput"` section from root `Cargo.toml` and the `[[bench]] name = "append_throughput"` section from `ultima_journal/Cargo.toml`. Check whether `ultima_journal`'s `criterion` dev-dep is now unused (`grep -rn criterion ultima_journal/`) — if so remove it. Grep the Makefile for references to either bench (expected: none).

Rationale (commit message material): `smr-apply-microbench` supersedes `snapshot_throughput` (freeze/stream/checkpoint/install under load, spec §8); `journal-microbench` covers all `append_throughput` cases (consistent/eventual/batched appends, point/range reads via replay, plus reopen and truncate/purge it never had).

- [ ] **Step 2: Feature doc + CLAUDE.md**

Write `docs/tasks/task35_autobench_perf_harness.md` consolidating, per the repo's feature workflow: motivation (audit findings), the harness architecture (run-iter stages, direction-aware gate decision, subagent orchestration table), both task definitions with metric tables, the `--check` regression gate and baseline policy, the suite restructure summary (bench_workloads / compare_benches / multiwriter_scaling_bench / retirements), and pointers to the spec and both plans. Source material: `docs/superpowers/specs/2026-06-12-autobench-perf-harness-design.md` (copy its tables, update anything that changed during implementation).

In root `CLAUDE.md` Build & Test Commands add:

```bash
make perf/check                          # perf regression gate (autobench baselines)
```

and in the architecture section a bullet:

```markdown
- **`autobench/`**: autoresearch-style perf harness (`run-iter`, `journal-microbench`, `smr-apply-microbench`) with committed baselines (`make perf/check`). See `docs/tasks/task35_autobench_perf_harness.md`.
```

- [ ] **Step 3: Full workspace verification (the memory-gate)**

```bash
make lint
cargo test --features persistence
cargo test -p ultima-journal
cargo test -p ultima-vector
cargo test -p ultima-autobench
cargo bench --no-run --features bench-internals
cargo bench -p compare-benches --no-run
(cd ../ultima_cluster && cargo build)
```

All must pass.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat+docs: retire superseded benches, autobench feature doc (task35)"
```
