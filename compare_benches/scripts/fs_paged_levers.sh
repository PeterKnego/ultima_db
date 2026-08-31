#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
#
# fs-paged "lever" validation cells (spike/paged-write-path follow-up, see
# docs/benchmarks/paged-write-path-spike-local-2026-08-31.md): the ultima-paged
# write-path levers — fanout-t8, mimalloc, ingest-then-serve reopen — each
# measured as one --restart cell (run1 = fresh-from-ingest, run2 = after
# drop+recover, i.e. compact heap) under ONE fixed absolute cgroup limit.
#
#   LIMIT=207642624 OUT=levers.jsonl scripts/fs_paged_levers.sh
#
# LIMIT defaults to the 198 MiB used by the 2026-08-31 local spike cells so
# shapes are comparable across machines (same-host relative only, as ever).
# Requires: cgroup v2 + swap enabled (the F6 mechanism under test is
# swap-backed reclaim; on a swapless host cells OOM instead of degrading).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

LIMIT="${LIMIT:-207642624}"
OUT="${OUT:-fs-paged-levers.jsonl}"
ROWS="${ROWS:-5000000}"
OPS="${OPS:-500000}"
TIMEOUT="${TIMEOUT:-60}"
PAGED_BUDGET="${PAGED_BUDGET:-67108864}"
TARGET_DIR="$(cargo metadata --format-version 1 --no-deps 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
DRIVER="$SCRIPT_DIR/paging_matrix.sh"

# Build the four variants into separate target dirs (glibc build is the
# plain release bin the caller has usually already built).
cargo build --release -p compare-benches --bin paging_matrix >&2
CARGO_TARGET_DIR="$TARGET_DIR/t8"   cargo build --release -p compare-benches --bin paging_matrix --features ultima-db/fanout-t8 >&2
CARGO_TARGET_DIR="$TARGET_DIR/mi"   cargo build --release -p compare-benches --bin paging_matrix --features bench-mimalloc >&2
CARGO_TARGET_DIR="$TARGET_DIR/mit8" cargo build --release -p compare-benches --bin paging_matrix --features bench-mimalloc,ultima-db/fanout-t8 >&2

COMMON="--engine=ultima-paged --rows=$ROWS --load=insert --dist=zipf --ops=$OPS --timeout-secs=$TIMEOUT --paged-budget=$PAGED_BUDGET --snapshots-retained=1 --restart --ratio=abs"

run_cell() {
  local name="$1" bin="$2"
  echo "[levers] cell=$name" >&2
  local line
  line=$("$DRIVER" "LIMIT=$LIMIT" -- "$bin" $COMMON)
  echo "$line" | python3 -c "import json,sys; d=json.load(sys.stdin); d['cell']='$name'; d['limit_bytes']=$LIMIT; print(json.dumps(d))" >> "$OUT"
}

run_cell glibc-t32 "$TARGET_DIR/release/paging_matrix"
run_cell glibc-t8  "$TARGET_DIR/t8/release/paging_matrix"
run_cell mi-t32    "$TARGET_DIR/mi/release/paging_matrix"
run_cell mi-t8     "$TARGET_DIR/mit8/release/paging_matrix"
echo "[levers] done -> $OUT" >&2
