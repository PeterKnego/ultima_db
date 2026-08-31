#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Peter Knego
#
# Run ONE paging_matrix cell inside a cgroup v2 memory-limited scope.
#
#   paging_matrix.sh RATIO=<n>|LIMIT=<bytes>|unlimited -- <paging_matrix bin> [--engine=... --rows=... ...]
#
# Protocol: the harness loads unconstrained, writes its RSS to <barrier>.rss and
# blocks. We then lower memory.max on the live scope (the kernel reclaims cold
# pages to swap once), touch <barrier>.go, and the measured phase starts.
#
#   RATIO=n   memory.max = this process's post-load RSS / n   (self-relative;
#             only meaningful for the in-memory engine)
#   LIMIT=b   memory.max = b bytes                            (absolute; use the
#             same LIMIT for every engine so budgets are comparable)
#   unlimited leave memory.max=max (the ceiling)
#
# Requires: systemd --user with the memory controller delegated (check:
# `cat /sys/fs/cgroup/user.slice/user-$UID.slice/user@$UID.service/cgroup.subtree_control`
# lists `memory`), and swap enabled (`swapon --show`).
set -euo pipefail

mode="${1:?RATIO=n | LIMIT=bytes | unlimited}"; shift
[[ "${1:-}" == "--" ]] && shift
bin="${1:?path to paging_matrix binary}"; shift

work="$(mktemp -d "${TMPDIR:-/tmp}/paging_matrix.XXXXXX")"
barrier="$work/barrier"
trap 'rm -rf "$work"' EXIT

label="$mode"
case "$mode" in
  RATIO=*|LIMIT=*|unlimited) ;;
  *) echo "bad mode: $mode" >&2; exit 2 ;;
esac

# Launch in its own transient scope. MemorySwapMax=infinity: the budget is
# RAM only; swap is unbounded (that is the whole point). As root (e.g. the
# bench-infra NVMe host, where ansible runs become) there is no user manager
# session — use the system manager instead; the harness reads its own cgroup
# from /proc/self/cgroup either way, and root can write that cgroup's
# memory.max directly.
user_flag="--user"
[[ "$(id -u)" == "0" ]] && user_flag=""
systemd-run $user_flag --scope --quiet \
  -p MemorySwapMax=infinity \
  -- "$bin" --barrier="$barrier" --ratio="$label" "$@" \
  > "$work/out.json" 2> >(tee "$work/err.log" >&2) &
child=$!

# Wait for the harness to publish its RSS.
while [[ ! -f "$barrier.rss" ]]; do
  if ! kill -0 "$child" 2>/dev/null; then
    echo "harness exited before barrier" >&2; wait "$child" || true; exit 1
  fi
  sleep 0.1
done
rss=$(sed -n 's/^rss_bytes=//p' "$barrier.rss")
cg=$(sed -n 's/^cgroup=//p' "$barrier.rss")
cgdir="/sys/fs/cgroup$cg"

case "$mode" in
  RATIO=*)
    n="${mode#RATIO=}"
    limit=$(python3 -c "import sys; print(int($rss / float('$n')))")
    ;;
  LIMIT=*)
    limit="${mode#LIMIT=}"
    ;;
  unlimited)
    limit=""
    ;;
esac

if [[ -n "$limit" ]]; then
  echo "[driver] rss=$((rss>>20)) MiB -> memory.max=$((limit>>20)) MiB ($cgdir)" >&2
  # Lowering memory.max reclaims synchronously; can take a while on big sets.
  echo "$limit" > "$cgdir/memory.max"
else
  echo "[driver] rss=$((rss>>20)) MiB, memory.max left at $(cat "$cgdir/memory.max")" >&2
fi
touch "$barrier.go"

wait "$child"
cat "$work/out.json"
