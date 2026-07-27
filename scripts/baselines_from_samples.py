#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
"""Reduce N microbench --json samples into an autobench baseline file.

The microbench bins' own --write-baseline records a SINGLE run at a flat
tolerance; that is too fragile to gate on. This reducer takes a JSONL of N
runs (one JSON metric object per line, as produced by bench-infra's
`bench_target=autobench-median`) and emits the committed baseline format:

  value         = median across the N samples
  tolerance_pct = max(10, ceil(1.5 * worst |deviation from median|, in %))
  direction     = maximize iff the metric name contains "throughput"
                  (must match ultima_autobench::baseline::infer_direction)

Usage:
  scripts/baselines_from_samples.py samples.jsonl out.json --note "..."
"""

import argparse
import json
import math
import statistics
import sys


def infer_direction(metric: str) -> str:
    # Mirrors ultima_autobench::baseline::infer_direction.
    return "maximize" if "throughput" in metric else "minimize"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("samples", help="JSONL: one metric object per line")
    ap.add_argument("out", help="baseline JSON to write")
    ap.add_argument("--note", default="", help="note field recorded in the baseline")
    ap.add_argument("--min-tolerance", type=float, default=10.0)
    ap.add_argument("--spread-factor", type=float, default=1.5)
    args = ap.parse_args()

    with open(args.samples) as fh:
        runs = [json.loads(line) for line in fh if line.strip()]
    if not runs:
        print(f"error: no samples in {args.samples}", file=sys.stderr)
        return 1

    names = sorted({k for r in runs for k in r})
    missing = [n for n in names if any(n not in r for r in runs)]
    if missing:
        print(f"error: metrics missing from some samples: {missing}", file=sys.stderr)
        return 1

    metrics = {}
    for name in names:
        vals = [float(r[name]) for r in runs]
        median = statistics.median(vals)
        if median == 0.0:
            # Sentinel slot: baseline.rs never breaches on 0.0.
            metrics[name] = {
                "value": 0.0,
                "tolerance_pct": args.min_tolerance,
                "direction": infer_direction(name),
            }
            continue
        worst_dev_pct = max(abs(v - median) / median * 100.0 for v in vals)
        tol = max(args.min_tolerance, math.ceil(args.spread_factor * worst_dev_pct))
        metrics[name] = {
            "value": median,
            "tolerance_pct": tol,
            "direction": infer_direction(name),
        }

    out = {"note": args.note, "metrics": metrics} if args.note else {"metrics": metrics}
    with open(args.out, "w") as fh:
        json.dump(out, fh, indent=2)
        fh.write("\n")

    print(f"{args.out}: {len(metrics)} metrics from {len(runs)} samples", file=sys.stderr)
    for name, m in metrics.items():
        vals = [float(r[name]) for r in runs]
        print(
            f"  {name:32s} median={m['value']:>14.3f} "
            f"tol={m['tolerance_pct']:>3}%  min={min(vals):.3f} max={max(vals):.3f}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
