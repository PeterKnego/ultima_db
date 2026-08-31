#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""Pivot fs-paged matrix JSONL (paging_matrix_run.sh output) into markdown.

Usage: fs_paged_report.py results.jsonl [more.jsonl ...]

One table per (ratio, dist, durability): rows = workloads, columns = engines,
cell = ops/s with p99 latency. Flags timeouts (`~` prefix: the op cap was not
reached, ops/s still valid but from fewer samples) and OOM kills. The
calibration cell is skipped.

Local runs are SANITY ONLY (sandbox noise floor is ~2x, see
docs/superpowers/specs/2026-07-08-bench-infra-carveout-design.md) — numbers
for docs/benchmarks/ must come from the bench-infra NVMe host.
"""
import json
import sys
from collections import defaultdict

WL_ORDER = {w: i for i, w in enumerate("ABCDEF")}


def fmt_ops(c):
    v = c["ops_per_sec"]
    s = f"{v / 1000:.1f}k" if v >= 1000 else f"{v:.0f}"
    if c.get("timed_out"):
        s = "~" + s
    if c.get("cg_events_oom"):
        s += " OOM!"
    return f"{s} ({c['p99_us']:.0f}us p99)"


def main(paths):
    cells = []
    for p in paths:
        with open(p) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                if d.get("cell") == "calibration":
                    continue
                cells.append(d)
    if not cells:
        print("no cells", file=sys.stderr)
        return 1

    groups = defaultdict(dict)  # (ratio, dist, dur) -> {(wl, engine): cell}
    engines, workloads = [], []
    for c in cells:
        dur = c.get("durability", "eventual")
        key = (c.get("ratio", "?"), c.get("dist", "?"), dur)
        groups[key][(c["workload"], c["engine"])] = c
        if c["engine"] not in engines:
            engines.append(c["engine"])
        if c["workload"] not in workloads:
            workloads.append(c["workload"])
    workloads.sort(key=lambda w: WL_ORDER.get(w, 99))

    first = cells[0]
    print(f"# fs-paged matrix — {first['rows']:,} rows, load={first.get('load', '?')}\n")
    print("`~` = hit the wall-clock timeout before the op cap; ops/s still valid.")
    print("Workload E reports scans/s (each scan visits ~50 rows, uniform 1..=100).\n")
    for (ratio, dist, dur), g in sorted(groups.items()):
        limit = next(iter(g.values())).get("limit_bytes", 0)
        lim = f", cgroup limit {limit >> 20} MiB" if limit else ""
        print(f"## ratio={ratio} dist={dist} durability={dur}{lim}\n")
        print("| workload | " + " | ".join(engines) + " |")
        print("|---" * (len(engines) + 1) + "|")
        for wl in workloads:
            row = [wl]
            for e in engines:
                c = g.get((wl, e))
                row.append(fmt_ops(c) if c else "—")
            print("| " + " | ".join(row) + " |")
        pf = [
            f"{w}: {g[(w, e)]['pf_per_op']:.3f}"
            for (w, e) in sorted(g)
            if g[(w, e)].get("pf_per_op") is not None
        ]
        if pf:
            print("\npage-file faults/op (ultima-paged): " + ", ".join(pf))
        print()
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1:]))
