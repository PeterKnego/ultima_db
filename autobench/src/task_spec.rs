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
        "multiwriter-commit" => Some(TaskSpec {
            task: "multiwriter-commit",
            microbench_bin: "mw-commit-microbench",
            primary_metric: "mw_commit_throughput",
            direction: Direction::Maximize,
            torture_test: "mw_commit_torture",
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
        assert_eq!(j.microbench_bin, "journal-microbench");
        assert_eq!(j.primary_metric, "group_commit_throughput");
        assert_eq!(j.direction, Direction::Maximize);
        let s = task_spec("smr-apply").unwrap();
        assert_eq!(s.torture_test, "smr_apply_torture");
        assert_eq!(s.direction, Direction::Minimize);
        let mw = task_spec("multiwriter-commit").unwrap();
        assert_eq!(mw.microbench_bin, "mw-commit-microbench");
        assert_eq!(mw.primary_metric, "mw_commit_throughput");
        assert_eq!(mw.direction, Direction::Maximize);
        assert_eq!(mw.torture_test, "mw_commit_torture");
        assert!(mw.cluster_gate);
    }

    #[test]
    fn unknown_task_is_none() {
        assert!(task_spec("nope").is_none());
    }
}
