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
T_SWEEP="${T_SWEEP:-32 64}"
RUNS="${RUNS:-7}"
NORM_T="${NORM_T:-32}"

set_t() { sed -i -E "s/^const T: usize = [0-9]+;/const T: usize = $1;/" "$BTREE"; }
# git-independent restore (bench-infra rsyncs the tree WITHOUT .git/).
ORIG_T="$(grep -oE '^const T: usize = [0-9]+;' "$BTREE" | grep -oE '[0-9]+')"
trap 'set_t "$ORIG_T"' EXIT

# $1 = T. Rebuild, warm up once, run $RUNS times, echo "median_apply median_p99".
measure() {
  local T="$1"
  set_t "$T"
  ( cd "$REPO_ROOT" && cargo build -q -p ultima-autobench --release ) >/dev/null 2>&1
  ( cd "$REPO_ROOT" && cargo run -q -p ultima-autobench --bin smr-apply-microbench --release -- --json ) >/dev/null 2>&1 || true
  local out=""
  for _ in $(seq 1 "$RUNS"); do
    out+="$(cd "$REPO_ROOT" && cargo run -q -p ultima-autobench --bin smr-apply-microbench --release -- --json 2>/dev/null)"$'\n'
  done
  printf '%s' "$out" | python3 -c '
import json,sys,statistics
ap=[];rp=[]
for line in sys.stdin:
    line=line.strip()
    if not line.startswith("{"): continue
    d=json.loads(line); ap.append(d["apply_sw_batch_throughput"]); rp.append(d["read_p99_under_load_ns"])
print(f"{statistics.median(ap):.1f} {statistics.median(rp):.1f}")'
}

declare -A AP RP
for T in $T_SWEEP; do
  echo ">>> building + benching T=$T (MAX_KEYS=$((2*T-1))), $RUNS runs..." >&2
  read -r ap rp <<<"$(measure "$T")"
  AP[$T]="$ap"; RP[$T]="$rp"
done

python3 - "$NORM_T" "$RUNS" <<PY
import sys
norm, runs = sys.argv[1], sys.argv[2]
ap = { $(for T in $T_SWEEP; do echo "$T: ${AP[$T]},"; done) }
rp = { $(for T in $T_SWEEP; do echo "$T: ${RP[$T]},"; done) }
apN, rpN = ap[int(norm)], rp[int(norm)]
print(f"\n# SMR-apply A/B — median of {runs} runs, normalized to T={norm}\n")
print("| T | MAX_KEYS | apply_sw_batch_throughput (higher=better) | read_p99_under_load_ns (lower=better) |")
print("|--:|--:|--:|--:|")
for T in sorted(ap):
    mark = "**" if str(T)==norm else ""
    # throughput ratio >1 = faster; p99 ratio <1 = better
    print(f"| {mark}{T}{mark} | {2*T-1} | {ap[T]:.0f}  ({ap[T]/apN:.2f}x) | {rp[T]:.0f}  ({rp[T]/rpN:.2f}x) |")
PY
