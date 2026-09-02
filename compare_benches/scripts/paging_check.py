#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
#
# Task 16 acceptance checker for `make paging/check`: asserts the paging_matrix
# JSON reports for the C/uniform and A/zipf ultima-paged cells meet the
# shape-gate thresholds from docs/tasks/task63_paged_btree.md /
# .superpowers/sdd/2026-08-30-paged-btree-stage-1-2/task-16-brief.md.
#
# Usage: paging_check.py <C-cell.json> <A-cell.json>
#
# Thresholds (sandbox shape gates, NOT published perf numbers — the NVMe
# bench-infra rig run is what produces those):
#   cg_events_oom == 0        (both cells, only when a cgroup snapshot exists)
#   C-cell pf_per_op <= 1.5
#   A-cell pf_per_op <= 3.0
#   recover_secs    <= RECOVER_SECS_MAX (both cells, when --restart was passed;
#                       see the disclosure comment above that constant)
#   paged_run.checkpointer_runs >= 1, paged_run.leaves_demoted > 0 and
#   min over trajectory samples taken after the first checkpointer run of
#   resident_leaf_bytes <= RESIDENT_TROUGH_MAX x paged_budget_bytes
#                       (both cells, when the report carries a paged budget;
#                       see the comment above that constant — the series
#                       max/mean are printed as disclosure, not asserted)
#
# Prints one PASS/FAIL line per assertion and exits nonzero if any assertion
# failed. Per task rules, thresholds are never adjusted here to force a pass.

import json
import sys

# DISCLOSED SUBSTITUTION (fix round 1, controller-ruled): the task16 plan
# specified a size-scaled recover_secs bound — "5x the first checkpoint's
# inner-level count x 0.1 ms" — that is unimplementable as written: "inner-
# level count" is defined nowhere in the plan, and no such field exists in
# the paging_matrix report JSON to compute it from. The controller ruled
# that the flat bound below stands as a coarse LOCAL shape gate in place of
# the unimplementable formula; the NVMe-host rerun (spec follow-on 7) is
# what sets real, published bounds.
RECOVER_SECS_MAX = 5.0

# task64 §14.1 "why the gate is blind": until 2026-09-02 this checker never
# compared the paged store's resident data-leaf bytes against the budget it
# was given, so the budget miss diagnosed there (resident 2.6-3.3x budget in
# both cells, ZERO leaves demoted during the run) passed every assertion.
#
# What is gated, and why the TROUGH: the soft counter
# `PagedStatsSnapshot::resident_leaf_bytes_est` (re-based from an exact walk
# at every checkpoint, then fault-in credits minus demote debits) is sampled
# into the trajectory every 5 s. Its value at any one instant is a phase
# sample of a saw-tooth: a demote pass lands and pulls it to <= budget, then
# the workload faults leaves back in until the next pass lands. The END
# value alone was tried first and is useless as a gate — two runs of the
# same C/uniform cell on the same tree ended at 1.44x and 4.20x budget with
# every other metric (ops, pf/op, swap-ins, runs, leaves demoted) within
# noise of each other. The trough after the first checkpoint is the robust
# assertion: it witnesses that passes land AND get the tree under budget
# (post-fix cells trough at 0.8-1.0x; the pre-fix ones never left 2.6x+).
# The series max and mean are printed, not asserted: they measure the
# steady-state overshoot (how far the workload runs past the budget while
# a swap-bound tick is in flight — task64 §14 item 5), which is the open
# item, and hiding it behind a loose bound helps nobody. `leaves_demoted >
# 0` is the direct witness for the diagnosed failure (passes that never
# land); `checkpointer_runs >= 1` keeps the trough from passing vacuously
# (before the first checkpoint the counter has never been credited for a
# built leaf — task63 F1 gap — and reads 0 whatever is resident).
RESIDENT_TROUGH_MAX = 1.25


def load(path):
    with open(path) as f:
        return json.load(f)


def check(label, ok, detail):
    status = "PASS" if ok else "FAIL"
    print(f"[paging/check] {status}: {label} — {detail}")
    return ok


def main():
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} <C-cell.json> <A-cell.json>", file=sys.stderr)
        return 2

    c = load(sys.argv[1])
    a = load(sys.argv[2])

    print(
        f"[paging/check] C/uniform: engine={c['engine']} load={c['load']} "
        f"rows={c['rows']} ops={c['ops']} timed_out={c['timed_out']} "
        f"ops_per_sec={c['ops_per_sec']:.0f} pf_per_op={c['pf_per_op']} "
        f"majflt_per_op={c['majflt_per_op']:.3f} recover_secs={c['recover_secs']}"
    )
    print(
        f"[paging/check] A/zipf:    engine={a['engine']} load={a['load']} "
        f"rows={a['rows']} ops={a['ops']} timed_out={a['timed_out']} "
        f"ops_per_sec={a['ops_per_sec']:.0f} pf_per_op={a['pf_per_op']} "
        f"majflt_per_op={a['majflt_per_op']:.3f} recover_secs={a['recover_secs']}"
    )

    results = []

    for label, report in (("C/uniform", c), ("A/zipf", a)):
        oom = report.get("cg_events_oom")
        if oom is None:
            print(f"[paging/check] SKIP: {label} cg_events_oom — no cgroup snapshot in this report")
        else:
            results.append(check(f"{label} cg_events_oom == 0", oom == 0, f"cg_events_oom={oom}"))

    pf_c = c.get("pf_per_op")
    results.append(
        check(
            "C-cell pf_per_op <= 1.5",
            pf_c is not None and pf_c <= 1.5,
            f"pf_per_op={pf_c}",
        )
    )

    pf_a = a.get("pf_per_op")
    results.append(
        check(
            "A-cell pf_per_op <= 3.0",
            pf_a is not None and pf_a <= 3.0,
            f"pf_per_op={pf_a}",
        )
    )

    for label, report in (("C/uniform", c), ("A/zipf", a)):
        rs = report.get("recover_secs")
        results.append(
            check(
                f"{label} recover_secs <= {RECOVER_SECS_MAX}",
                rs is not None and rs <= RECOVER_SECS_MAX,
                f"recover_secs={rs}",
            )
        )

    for label, report in (("C/uniform", c), ("A/zipf", a)):
        pr = report.get("paged_run")
        budget = report.get("paged_budget_bytes")
        if pr is None or not budget:
            print(f"[paging/check] SKIP: {label} resident-vs-budget — no paged_run/paged_budget_bytes in this report")
            continue
        runs = pr.get("checkpointer_runs")
        demoted = pr.get("leaves_demoted")
        results.append(
            check(
                f"{label} checkpointer_runs >= 1",
                runs is not None and runs >= 1,
                f"checkpointer_runs={runs}",
            )
        )
        results.append(
            check(
                f"{label} leaves_demoted > 0",
                demoted is not None and demoted > 0,
                f"leaves_demoted={demoted}",
            )
        )
        # Samples after the first checkpointer run of the run phase: the
        # trajectory's `checkpointer_runs` is the store's cumulative count,
        # so "after the first run of THIS phase" is `> runs_at_start`, and
        # `runs_at_start = cumulative_end - paged_run.checkpointer_runs`.
        traj = report.get("trajectory") or []
        end_runs = next((t["checkpointer_runs"] for t in reversed(traj) if t.get("checkpointer_runs") is not None), None)
        if end_runs is None or runs is None:
            # Not a SKIP: a paged report whose trajectory carries no counter
            # samples cannot witness the budget at all, and this checker's
            # whole reason to exist is not passing vacuously.
            results.append(check(f"{label} resident trough <= {RESIDENT_TROUGH_MAX}x budget", False, "trajectory carries no paged samples (harness predates task64 §7c?)"))
            continue
        runs_at_start = end_runs - runs
        series = [
            t["resident_leaf_bytes"] / budget
            for t in traj
            if t.get("resident_leaf_bytes") is not None
            and t.get("checkpointer_runs") is not None
            and t["checkpointer_runs"] > runs_at_start
        ]
        if not series:
            results.append(check(f"{label} resident trough <= {RESIDENT_TROUGH_MAX}x budget", False, "no samples after the first checkpointer run"))
            continue
        trough = min(series)
        print(
            f"[paging/check] INFO: {label} resident/budget series after first checkpoint: "
            f"trough={trough:.2f} max={max(series):.2f} mean={sum(series) / len(series):.2f} "
            f"n={len(series)} end={series[-1]:.2f} (max/mean are the steady-state overshoot, task64 §14 item 5 — disclosed, not gated)"
        )
        results.append(
            check(
                f"{label} resident trough <= {RESIDENT_TROUGH_MAX}x budget",
                trough <= RESIDENT_TROUGH_MAX,
                f"trough={trough:.2f} paged_budget_bytes={budget} leaves_demoted={demoted} dirty_bytes_end={pr.get('dirty_bytes_end')}",
            )
        )

    if all(results):
        print("[paging/check] ALL ASSERTIONS PASSED")
        return 0
    print("[paging/check] ONE OR MORE ASSERTIONS FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
