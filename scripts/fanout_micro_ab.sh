#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
#
# Fanout (`T`) sweep for the read-vs-write ASYMMETRY study
# (docs/benchmarks/btree-fanout-t-sweep-2026-07-09.md).
#
# WHY a dedicated harness (not scripts/fanout_ab.sh): the existing bulk benches
# measure writes in the COLD/uniquely-owned regime (insert_mut/remove_mut on a
# private tree — make_mut mutates in place, no clone), where bigger `T` HELPS
# writes. The asymmetry the doc describes — bigger `T` slows CoW writes ~ T/lnT —
# only appears in the WARM/shared regime (make_mut clones the path). This driver
# runs `examples/fanout_microbench`, which measures get + insert/update/remove in
# BOTH regimes, so the warm curves can be checked against the T/lnT formula and
# the cold curves against the height+width (U-shape) corollary. It also adds the
# missing `update` op and extends the sweep to T=256.
#
# `T` is a compile-time const, so each arm rebuilds. Each build is run $RUNS times
# for a spread. RUN ON A QUIET BENCH HOST — sandbox noise (~±2x) drowns the signal.
#
# Usage:
#   scripts/fanout_micro_ab.sh                          # T in 8..256, 7 runs each
#   T_SWEEP="32 64" RUNS=9 scripts/fanout_micro_ab.sh
#   FANOUT_QUICK=1 T_SWEEP="32 64" RUNS=2 scripts/fanout_micro_ab.sh   # local smoke
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BTREE="$REPO_ROOT/src/btree.rs"
T_SWEEP="${T_SWEEP:-8 16 32 64 128 256}"
RUNS="${RUNS:-7}"
NORM_T="${NORM_T:-32}"

set_t() { sed -i -E "s/^const T: usize = [0-9]+;/const T: usize = $1;/" "$BTREE"; }
# git-independent restore (bench-infra rsyncs the tree WITHOUT .git/).
ORIG_T="$(grep -oE '^const T: usize = [0-9]+;' "$BTREE" | grep -oE '[0-9]+')"
WORK="$(mktemp -d)"
trap 'set_t "$ORIG_T"; rm -rf "$WORK"' EXIT

for T in $T_SWEEP; do
  echo ">>> building + benching T=$T (MAX_KEYS=$((2*T-1))), $RUNS runs..." >&2
  set_t "$T"
  ( cd "$REPO_ROOT" && cargo build -q --release --example fanout_microbench ) >/dev/null 2>&1
  # Untimed warmup (page-in, cpu ramp), then $RUNS timed runs. FANOUT_T stamps the
  # emitted JSON so the log is self-describing; each raw line is echoed for capture.
  ( cd "$REPO_ROOT" && FANOUT_T="$T" cargo run -q --release --example fanout_microbench -- --json ) >/dev/null 2>&1 || true
  : > "$WORK/$T.jsonl"
  for _ in $(seq 1 "$RUNS"); do
    line="$( cd "$REPO_ROOT" && FANOUT_T="$T" cargo run -q --release --example fanout_microbench -- --json 2>/dev/null )"
    echo "$line" | tee -a "$WORK/$T.jsonl"
  done
done

# Aggregate: median [min-max] per (metric, T), normalized to NORM_T's median.
python3 - "$NORM_T" "$RUNS" "$WORK" $T_SWEEP <<'PY'
import sys, json, statistics
norm=int(sys.argv[1]); runs=sys.argv[2]; work=sys.argv[3]; sweep=[int(x) for x in sys.argv[4:]]
METRICS=["get_ns","insert_warm_ns","update_warm_ns","remove_warm_ns",
         "insert_cold_ns","update_cold_ns","remove_cold_ns"]
data={T:{k:[] for k in METRICS} for T in sweep}
for T in sweep:
    for line in open(f"{work}/{T}.jsonl"):
        line=line.strip()
        if not line.startswith("{"): continue
        d=json.loads(line)
        for k in METRICS: data[T][k].append(d[k])
med=lambda T,k: statistics.median(data[T][k])
hdr=["get","ins_warm","upd_warm","rem_warm","ins_cold","upd_cold","rem_cold"]
print(f"\n# fanout micro A/B — {runs} runs/arm, MEDIAN normalized to T={norm} (lower=faster)\n")
print("| T | MAX_KEYS | " + " | ".join(hdr) + " |")
print("|--:|" + "--:|"*(len(hdr)+1))
base={k:med(norm,k) for k in METRICS}
for T in sweep:
    mark="**" if T==norm else ""
    cells=[f"{med(T,k)/base[k]:.2f}" for k in METRICS]
    print(f"| {mark}{T}{mark} | {2*T-1} | " + " | ".join(cells) + " |")
print(f"\nabsolute medians @T={norm} (ns/op): " +
      ", ".join(f"{h}={base[k]:.0f}" for h,k in zip(hdr,METRICS)))
# Formula check: read ~ 1/lnT, warm write ~ T/lnT. Print the theoretical ratios
# (normalized to T=norm) alongside so `fit` is visible without the plot.
import math
print("\n# theory (normalized to T=%d): read=ln(norm)/ln(T),  warm-write=(T/lnT)/(norm/ln(norm))" % norm)
print("| T | read_theory | warmwrite_theory |")
print("|--:|--:|--:|")
rn=1/math.log(norm); wn=norm/math.log(norm)
for T in sweep:
    print(f"| {T} | {(1/math.log(T))/rn:.2f} | {(T/math.log(T))/wn:.2f} |")
print("\nraw per-run (sorted) ns/op:")
for k in METRICS:
    print(f"  {k}:")
    for T in sweep:
        print(f"    T={T}: " + " ".join(f"{v:.0f}" for v in sorted(data[T][k])))
PY
