#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
#
# Fanout (`T`) A/B sweep for the persistent B-tree.
#
# WHY: `T` (min-degree; MAX_KEYS = 2T-1) is a compile-time const in
# src/btree.rs, so a fanout sweep cannot be one binary — each `T` needs its own
# build. This driver rewrites the const, rebuilds, and times the three axes that
# trade off against each other:
#   * get      (read_random)   — favors larger T (shallower tree, fewer misses)
#   * insert   (insert_mut)    — favors larger T
#   * remove   (remove_mut)    — favored smaller T historically; the in-place
#                                rebalance (task50 §5.1) was the prerequisite for
#                                re-testing whether the optimum shifts upward.
# All three are measured at 1M random keys, the `in_place` arm only (the
# immutable arms are not what ships). Results are normalized to T=32 (the current
# default) and printed as a markdown table — the *ratios* are the deliverable;
# an A/B cancels common-mode host noise (see the bench A/B methodology note).
#
# RUN THIS ON A QUIET BENCH HOST, not the dev sandbox (per-run noise there is
# ~±2x and will drown the signal). Record the printed table in task50 and in
# docs/superpowers/specs/2026-07-08-btree-optimization-candidates.md §2.
#
# Usage:
#   scripts/fanout_ab.sh                 # sweep T in 8 16 32 64 128
#   T_SWEEP="32 64" scripts/fanout_ab.sh # custom sweep
#   N=1000000 scripts/fanout_ab.sh       # custom key count
#   QUICK=1 scripts/fanout_ab.sh         # fast smoke pass (criterion --quick; NOT for real numbers)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BTREE="$REPO_ROOT/src/btree.rs"
# Authoritative target dir — honors CARGO_TARGET_DIR *and* .cargo/config.toml's
# build.target-dir (which a plain env-var check would miss).
TARGET_DIR="$(cd "$REPO_ROOT" && cargo metadata --format-version 1 --no-deps \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
CRIT_DIR="$TARGET_DIR/criterion"

T_SWEEP="${T_SWEEP:-8 16 32 64 128}"
N="${N:-1000000}"
NORM_T="${NORM_T:-32}"          # baseline the table is normalized against
QUICK_ARG=""; [ -n "${QUICK:-}" ] && QUICK_ARG="--quick"

set_t() { sed -i -E "s/^const T: usize = [0-9]+;/const T: usize = $1;/" "$BTREE"; }

# We rewrite one line of a tracked file; guarantee we restore it no matter how we
# exit (Ctrl-C, error, or normal completion) so no run leaves `T` mutated. Restore
# by rewriting the saved value, NOT `git checkout` — bench-infra rsyncs the tree
# to the remote host *without* .git/, so a git-based restore would fail there.
ORIG_T="$(grep -oE '^const T: usize = [0-9]+;' "$BTREE" | grep -oE '[0-9]+')"
restore() { set_t "$ORIG_T"; }
trap restore EXIT

# Run one criterion benchmark id and echo its median in nanoseconds.
# $1 = bench target, $2 = benchmark id (also the criterion dir path).
run_one() {
  local bench="$1" id="$2"
  ( cd "$REPO_ROOT" && cargo bench --bench "$bench" -- $QUICK_ARG --exact "$id" ) >/dev/null 2>&1
  python3 - "$CRIT_DIR/$id/new/estimates.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    print(json.load(f)["median"]["point_estimate"])
PY
}

declare -A GET INS REM
for T in $T_SWEEP; do
  echo ">>> building + benching T=$T (MAX_KEYS=$((2*T-1)))..." >&2
  set_t "$T"
  GET[$T]="$(run_one btree_get_bench        "get_random/get/$N")"
  INS[$T]="$(run_one btree_insert_mut_bench "insert_random/in_place/$N")"
  REM[$T]="$(run_one btree_remove_mut_bench "delete_random/in_place/$N")"
done

# Emit a normalized markdown table (lower = faster; 1.00 = the NORM_T baseline).
python3 - "$NORM_T" "$N" <<PY
import sys
norm, n = sys.argv[1], sys.argv[2]
get = { $(for T in $T_SWEEP; do echo "$T: ${GET[$T]},"; done) }
ins = { $(for T in $T_SWEEP; do echo "$T: ${INS[$T]},"; done) }
rem = { $(for T in $T_SWEEP; do echo "$T: ${REM[$T]},"; done) }
gN, iN, rN = get[int(norm)], ins[int(norm)], rem[int(norm)]
print(f"\n# fanout A/B @ {int(n):,} random keys — normalized to T={norm} (lower=faster)\n")
print("| T | MAX_KEYS | get | insert | remove |")
print("|--:|--:|--:|--:|--:|")
for T in sorted(get):
    mark = "**" if str(T)==norm else ""
    print(f"| {mark}{T}{mark} | {2*T-1} | {get[T]/gN:.2f} | {ins[T]/iN:.2f} | {rem[T]/rN:.2f} |")
print(f"\nabsolute @T={norm}: get {gN/1e6:.1f} ms, insert {iN/1e6:.1f} ms, remove {rN/1e6:.1f} ms")
PY
