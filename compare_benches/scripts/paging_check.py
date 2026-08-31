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

    if all(results):
        print("[paging/check] ALL ASSERTIONS PASSED")
        return 0
    print("[paging/check] ONE OR MORE ASSERTIONS FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
