#!/usr/bin/env bash
# Drift guard between the Rust B-tree and its Aeneas/Lean model.
#
# src/btree.rs is the source of truth; formal/kernel/src/lib.rs is a hand-synced
# port of its insert/get/find_pos/split path, and formal/proofs/ machine-checks
# that port. Nothing forces the two to stay in step — this guard does, cheaply:
# if src/btree.rs changed in a diff but nothing under formal/ did, it fails and
# tells you how to re-sync (or to acknowledge a change that doesn't touch the
# verified surface).
#
# It is a *prompt to re-verify*, not a proof of equivalence. It fires on any
# btree.rs edit; the author resolves it by either updating + re-checking formal/,
# or acknowledging that the edit is outside the modeled path (remove/rebalance —
# not yet modeled — comments, unrelated methods).
#
# Usage:
#   formal/scripts/check-drift.sh [BASE_REF]
#     BASE_REF defaults to $BASE, else origin/main, else HEAD~1.
# Acknowledge a change that does not affect the verified insert/get surface:
#   ACK_NO_FORMAL=1 formal/scripts/check-drift.sh [BASE_REF]
set -euo pipefail

WATCHED="src/btree.rs"
FORMAL_PREFIX="formal/"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

base="${1:-${BASE:-}}"
if [ -z "$base" ]; then
  if git rev-parse --verify --quiet origin/main >/dev/null; then
    base="origin/main"
  else
    base="HEAD~1"
  fi
fi

# Union of: committed changes since the merge-base with $base, plus any
# uncommitted (worktree + staged) changes — so the guard is useful both in CI
# (range diff) and locally before you commit.
changed="$(
  {
    git diff --name-only "$base"...HEAD 2>/dev/null || true
    git diff --name-only 2>/dev/null || true
    git diff --cached --name-only 2>/dev/null || true
  } | sort -u | sed '/^[[:space:]]*$/d'
)"

touched_watched=false
touched_formal=false
while IFS= read -r f; do
  [ -z "$f" ] && continue
  [ "$f" = "$WATCHED" ] && touched_watched=true
  case "$f" in "$FORMAL_PREFIX"*) touched_formal=true ;; esac
done <<EOF
$changed
EOF

if ! $touched_watched; then
  echo "formal drift-check: $WATCHED unchanged vs $base — ok."
  exit 0
fi

if $touched_formal; then
  echo "formal drift-check: $WATCHED and formal/ both changed — ok."
  echo "  Re-verify: make test/formal-kernel && (cd formal/proofs && lake build)"
  exit 0
fi

if [ "${ACK_NO_FORMAL:-}" = "1" ]; then
  echo "formal drift-check: $WATCHED changed without formal/ — acknowledged (ACK_NO_FORMAL=1)."
  exit 0
fi

cat >&2 <<MSG
formal drift-check FAILED

  $WATCHED changed, but nothing under $FORMAL_PREFIX did (vs $base).

  The insert / get / find_pos / split path in $WATCHED is mirrored by the
  Aeneas/Lean model in formal/ and machine-checked there. Divergence must be
  a deliberate decision, not an accident.

  If your change touches that path:
    1. mirror it in formal/kernel/src/lib.rs
    2. re-run  make test/formal-kernel
    3. re-run  (cd formal/proofs && lake build)  and confirm axioms are clean
       (see formal/README.md)

  If it does NOT (e.g. remove/rebalance — not yet modeled — comments, an
  unrelated method), acknowledge it:
    ACK_NO_FORMAL=1 $0
MSG
exit 1
