#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
#
# Fanout (`T`) A/B for the SMR-apply + read-under-load microbench.
#
# WHY: the btree fanout bump T=32->64 (task50 / optimization-candidates §2) was
# chosen on UNCONTENDED bulk-op benches (btree_{get,insert_mut,remove_mut}). The
# autobench perf gate (`make perf/check`) exercises a different regime — SMR apply
# of SmallBank batches plus point reads run CONCURRENTLY with writes — where
# `Arc::make_mut` frequently CoW-clones shared nodes, and a bigger T means a bigger
# node to clone. On the dev sandbox that showed T=64 REGRESSING apply throughput
# (~-22%) and read p99 (~+41%) vs T=32. This harness re-runs that A/B on a quiet
# bench host to confirm the effect on real hardware (the ratios are the deliverable;
# absolute numbers are host-relative). See `docs/tasks/task50` §fanout.
#
# `T` is a compile-time const, so each arm rebuilds. The `smr-apply-microbench`
# binary runs its workload once per invocation; we run it $RUNS times and take the
# median of both metrics.
#
# Usage:
#   scripts/smr_apply_ab.sh                 # sweep T in "32 64", 7 runs each
#   T_SWEEP="32 48 64" RUNS=9 scripts/smr_apply_ab.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BTREE="$REPO_ROOT/src/btree.rs"
T_SWEEP="${T_SWEEP:-8 16 32 64}"
RUNS="${RUNS:-7}"
NORM_T="${NORM_T:-32}"

set_t() { sed -i -E "s/^const T: usize = [0-9]+;/const T: usize = $1;/" "$BTREE"; }
# git-independent restore (bench-infra rsyncs the tree WITHOUT .git/).
ORIG_T="$(grep -oE '^const T: usize = [0-9]+;' "$BTREE" | grep -oE '[0-9]+')"
WORK="$(mktemp -d)"
trap 'set_t "$ORIG_T"; rm -rf "$WORK"' EXIT

# $1 = T. Rebuild, warm up once, run $RUNS times, echo "median_apply median_p99".
# $1 = T. Rebuild, warm up once (untimed), then save each run's raw JSON to
# $WORK/$T.jsonl so the aggregator can report the full spread, not just a median.
measure() {
  local T="$1"
  set_t "$T"
  ( cd "$REPO_ROOT" && cargo build -q -p ultima-autobench --release ) >/dev/null 2>&1
  ( cd "$REPO_ROOT" && cargo run -q -p ultima-autobench --bin smr-apply-microbench --release -- --json ) >/dev/null 2>&1 || true
  : > "$WORK/$T.jsonl"
  for _ in $(seq 1 "$RUNS"); do
    ( cd "$REPO_ROOT" && cargo run -q -p ultima-autobench --bin smr-apply-microbench --release -- --json 2>/dev/null ) >> "$WORK/$T.jsonl"
  done
}

for T in $T_SWEEP; do
  echo ">>> building + benching T=$T (MAX_KEYS=$((2*T-1))), $RUNS runs..." >&2
  measure "$T"
done

# Report median [min-max] per arm for BOTH metrics plus the raw sorted per-run
# values — so overlapping arms (noise) are distinguishable from real separation.
python3 - "$NORM_T" "$RUNS" "$WORK" $T_SWEEP <<'PY'
import sys, json, statistics
norm=int(sys.argv[1]); runs=sys.argv[2]; work=sys.argv[3]; sweep=[int(x) for x in sys.argv[4:]]
data={}
for T in sweep:
    ap=[]; rp=[]
    for line in open(f"{work}/{T}.jsonl"):
        line=line.strip()
        if not line.startswith("{"): continue
        d=json.loads(line); ap.append(d["apply_sw_batch_throughput"]); rp.append(d["read_p99_under_load_ns"])
    data[T]=(ap,rp)
st=lambda xs:(min(xs),statistics.median(xs),max(xs))
apN=st(data[norm][0])[1]; rpN=st(data[norm][1])[1]
print(f"\n# SMR-apply A/B — {runs} runs/arm, median [min-max], normalized to T={norm}\n")
print("| T | MAX_KEYS | apply_sw_batch_throughput (higher=better) | read_p99_under_load_ns (lower=better) |")
print("|--:|--:|--:|--:|")
for T in sweep:
    ap,rp=data[T]; a=st(ap); r=st(rp); mark="**" if T==norm else ""
    print(f"| {mark}{T}{mark} | {2*T-1} | {a[1]:.0f} [{a[0]:.0f}-{a[2]:.0f}]  {a[1]/apN:.2f}x | {r[1]:.0f} [{r[0]:.0f}-{r[2]:.0f}]  {r[1]/rpN:.2f}x |")
print("\nraw apply_sw_batch_throughput per run (sorted):")
for T in sweep: print(f"  T={T}: " + " ".join(f"{v:.0f}" for v in sorted(data[T][0])))
print("raw read_p99_under_load_ns per run (sorted):")
for T in sweep: print(f"  T={T}: " + " ".join(f"{v:.0f}" for v in sorted(data[T][1])))
PY
