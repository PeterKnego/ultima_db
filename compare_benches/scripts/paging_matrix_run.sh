#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
#
# Run the paging baseline matrix. Calibrates the memory budget from UltimaDB's
# measured post-load footprint, then applies the SAME absolute budget to every
# engine at each D/M ratio. Appends one JSON line per cell to $OUT.
#
#   ROWS=5000000 RATIOS="2 4" ENGINES="ultima redb" DISTS="zipf uniform" \
#   WORKLOADS="C A" OPS=2000000 TIMEOUT=60 OUT=results.jsonl \
#   scripts/paging_matrix_run.sh
#
# ENGINES may include `ultima-mimalloc` (bench-mimalloc build, BIN_MIMALLOC)
# and `ultima-t8` (ultima-db/fanout-t8 build, BIN_T8). Build them first:
#   cargo build --release -p compare-benches --bin paging_matrix
#   CARGO_TARGET_DIR="$(cargo metadata --format-version 1 --no-deps | jq -r .target_directory)/mimalloc" \
#       cargo build --release -p compare-benches --bin paging_matrix --features bench-mimalloc
#   CARGO_TARGET_DIR="$(cargo metadata --format-version 1 --no-deps | jq -r .target_directory)/t8" \
#       cargo build --release -p compare-benches --bin paging_matrix --features ultima-db/fanout-t8
set -euo pipefail
cd "$(dirname "$0")/.."

ROWS="${ROWS:-5000000}"
RATIOS="${RATIOS:-0.5 2 4}"
ENGINES="${ENGINES:-ultima redb}"
DISTS="${DISTS:-zipf uniform}"
WORKLOADS="${WORKLOADS:-C A}"
OPS="${OPS:-2000000}"
TIMEOUT="${TIMEOUT:-60}"
LOAD="${LOAD:-bulk}"
OUT="${OUT:-paging-results.jsonl}"
# Resolve cargo's target dir (honours CARGO_TARGET_DIR / config overrides).
TARGET_DIR="$(cargo metadata --format-version 1 --no-deps 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
BIN="${BIN:-$TARGET_DIR/release/paging_matrix}"
BIN_MIMALLOC="${BIN_MIMALLOC:-$TARGET_DIR/mimalloc/release/paging_matrix}"
BIN_T8="${BIN_T8:-$TARGET_DIR/t8/release/paging_matrix}"
DRIVER="$(dirname "$0")/paging_matrix.sh"

bin_for() {
  case "$1" in
    ultima-mimalloc) echo "$BIN_MIMALLOC" ;;
    ultima-t8)       echo "$BIN_T8" ;;
    *)               echo "$BIN" ;;
  esac
}
engine_arg() { case "$1" in ultima-*) echo "ultima" ;; *) echo "$1" ;; esac; }

# --- calibrate: UltimaDB footprint at this ROWS (glibc build), no workload.
echo "[matrix] calibrating footprint: ultima rows=$ROWS load=$LOAD" >&2
cal=$("$DRIVER" unlimited -- "$BIN" --engine=ultima --rows="$ROWS" --load="$LOAD" \
        --workload=C --dist=zipf --ops=1000 --timeout-secs=5)
footprint=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['rss_after_load_bytes'])" "$cal")
echo "[matrix] footprint=$((footprint>>20)) MiB" >&2
echo "$cal" | python3 -c "import json,sys; d=json.load(sys.stdin); d['cell']='calibration'; print(json.dumps(d))" >> "$OUT"

for ratio in $RATIOS; do
  if [[ "$ratio" == "0" ]]; then
    mode="unlimited"
  else
    limit=$(python3 -c "print(int($footprint / float('$ratio')))")
    mode="LIMIT=$limit"
  fi
  for engine in $ENGINES; do
    for dist in $DISTS; do
      for wl in $WORKLOADS; do
        echo "[matrix] ratio=$ratio ($mode) engine=$engine dist=$dist workload=$wl" >&2
        line=$("$DRIVER" "$mode" -- "$(bin_for "$engine")" \
                 --engine="$(engine_arg "$engine")" --rows="$ROWS" --load="$LOAD" \
                 --dist="$dist" --workload="$wl" --ops="$OPS" --timeout-secs="$TIMEOUT" \
                 --ratio="$ratio")
        echo "$line" | python3 -c "import json,sys; d=json.load(sys.stdin); d['engine']='$engine'; d['limit_bytes']=int('${limit:-0}'); d['footprint_bytes']=$footprint; print(json.dumps(d))" >> "$OUT"
      done
    done
  done
done
echo "[matrix] done -> $OUT" >&2
