#!/usr/bin/env bash
# Drift guard between the Rust sources and their Aeneas/Lean models.
#
# Two watched files, two kernels: src/btree.rs is the source of truth for
# formal/kernel/src/lib.rs (a hand-synced port of its insert/get and
# remove/rebalance paths), and src/primary_key.rs is the source of truth for
# formal/key_kernel/src/lib.rs (a hand-synced port of the order-preserving key
# encoding). formal/proofs/ machine-checks both ports. Nothing forces the Rust
# and the kernels to stay in step — this does: if a watched file changed in a
# diff but nothing under formal/ did, it fails and tells you how to re-sync
# (or to acknowledge a change that doesn't touch the verified surface).
#
# It is a *prompt to re-verify*, not a proof of equivalence. It fires on any
# watched-file edit; the author resolves it by either updating + re-checking
# formal/, or acknowledging that the edit is outside the modeled path (range
# iterators, comments, unrelated methods).
#
# Usage:
#   formal/scripts/check-drift.sh [BASE_REF]
#     BASE_REF defaults to $BASE, else origin/main, else HEAD~1.
# Acknowledge a change that does not affect the verified insert/get/remove surface:
#   ACK_NO_FORMAL=1 formal/scripts/check-drift.sh [BASE_REF]
set -euo pipefail

WATCHED="src/btree.rs src/primary_key.rs"
FORMAL_PREFIX="formal/"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# Portability guard: lake-manifest.json records a path dependency's dir at
# generation time, and editing the lakefile's `require ... from` does not
# regenerate it. A manifest generated against an absolute local checkout
# resolves nowhere else and breaks the weekly lean job — which PRs never run,
# so catch it here.
MANIFEST="formal/proofs/lake-manifest.json"
if grep -nE '"dir":[[:space:]]*"/' "$MANIFEST" >&2; then
  echo "formal drift-check FAILED: $MANIFEST pins a path dependency to an absolute directory (above)." >&2
  echo "  Use a path relative to formal/proofs, e.g. ../.toolchain/backends/lean." >&2
  exit 1
fi

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
touched_watched_list=""
while IFS= read -r f; do
  [ -z "$f" ] && continue
  for w in $WATCHED; do
    if [ "$f" = "$w" ]; then
      touched_watched=true
      touched_watched_list="$touched_watched_list $w"
    fi
  done
  case "$f" in "$FORMAL_PREFIX"*) touched_formal=true ;; esac
done <<EOF
$changed
EOF
touched_watched_list="$(echo "$touched_watched_list" | sed 's/^ *//')"

if ! $touched_watched; then
  echo "formal drift-check: $WATCHED unchanged vs $base — ok."
  exit 0
fi

if $touched_formal; then
  echo "formal drift-check: $touched_watched_list and formal/ both changed — ok."
  echo "  Re-verify: make test/formal-kernel && make test/formal-key-kernel && (cd formal/proofs && lake build)"
  exit 0
fi

if [ "${ACK_NO_FORMAL:-}" = "1" ]; then
  echo "formal drift-check: $touched_watched_list changed without formal/ — acknowledged (ACK_NO_FORMAL=1)."
  exit 0
fi

cat >&2 <<MSG
formal drift-check FAILED

  $touched_watched_list changed, but nothing under $FORMAL_PREFIX did (vs $base).

  Each watched file is mirrored by an Aeneas/Lean model and machine-checked
  there. Divergence must be a deliberate decision, not an accident.

    src/btree.rs       -> formal/kernel/       (insert/get, remove/rebalance)
    src/primary_key.rs -> formal/key_kernel/    (order-preserving key encoding)

  If your change touches the modeled surface:
    1. mirror it in the corresponding formal/*_kernel/src/lib.rs
    2. re-run  make test/formal-kernel  (btree.rs)  and/or
               make test/formal-key-kernel  (primary_key.rs)
    3. re-run  (cd formal/proofs && lake build)  and confirm axioms are clean
       (see formal/README.md)

  If it does NOT (e.g. range iterators, comments, an unrelated method),
  acknowledge it:
    ACK_NO_FORMAL=1 $0
MSG
exit 1
