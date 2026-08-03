#!/usr/bin/env bash
# Drift guard between the Rust sources and their formal models.
#
# Two kinds of model are watched:
#
#   Aeneas/Lean kernels — src/btree.rs is the source of truth for
#     formal/kernel/src/lib.rs (a hand-synced port of its insert/get and
#     remove/rebalance paths), and src/primary_key.rs is the source of truth
#     for formal/key_kernel/src/lib.rs (a hand-synced port of the
#     order-preserving key encoding). formal/proofs/ machine-checks both ports.
#
#   TLA+ model — formal/tla/wal/ models the three-phase commit pipeline, the
#     WAL sinks and crash recovery. It is not generated from the Rust; its
#     fidelity rests on prose claims that carry explicit src/*.rs:LINE cites
#     into src/wal.rs, src/store.rs and src/persistence.rs. Those cites rot on
#     any edit that moves lines — including a pure perf refactor that changes
#     no semantics at all (task57 shifted every wal.rs cite by 88-92 lines).
#
# Nothing forces the Rust and the models to stay in step — this does: if a
# watched file changed in a diff but nothing under formal/ did, it fails and
# tells you how to re-sync (or to acknowledge a change that doesn't touch the
# modelled surface).
#
# It is a *prompt to re-verify*, not a proof of equivalence. It fires on any
# watched-file edit; the author resolves it by either updating + re-checking
# formal/, or acknowledging that the edit is outside the modelled path (range
# iterators, comments, unrelated methods — and, for the TLA+ files, that it
# moved no cited line).
#
# Usage:
#   formal/scripts/check-drift.sh [BASE_REF]
#     BASE_REF defaults to $BASE, else origin/main, else HEAD~1.
# Acknowledge a change that does not affect the modelled surface:
#   ACK_NO_FORMAL=1 formal/scripts/check-drift.sh [BASE_REF]
set -euo pipefail

WATCHED="src/btree.rs src/primary_key.rs src/wal.rs src/store.rs src/persistence.rs"
FORMAL_PREFIX="formal/"

# The model each watched file is mirrored by, and how to re-check that model.
model_for() {
  case "$1" in
    src/btree.rs)       echo "formal/kernel/      (insert/get, remove/rebalance)" ;;
    src/primary_key.rs) echo "formal/key_kernel/  (order-preserving key encoding)" ;;
    src/wal.rs|src/store.rs|src/persistence.rs)
                        echo "formal/tla/wal/     (TLA+ commit/WAL/crash model + its src/*.rs:LINE cites)" ;;
  esac
}
recheck_for() {
  case "$1" in
    src/btree.rs)       echo "make test/formal-kernel && (cd formal/proofs && lake build)" ;;
    src/primary_key.rs) echo "make test/formal-key-kernel && (cd formal/proofs && lake build)" ;;
    src/wal.rs|src/store.rs|src/persistence.rs)
                        echo "make formal/tla-model   (and re-anchor the src/*.rs:LINE cites in formal/tla/wal/)" ;;
  esac
}

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
  echo "  Re-verify:"
  for w in $touched_watched_list; do
    echo "    $w -> $(recheck_for "$w")"
  done | sort -u
  exit 0
fi

if [ "${ACK_NO_FORMAL:-}" = "1" ]; then
  echo "formal drift-check: $touched_watched_list changed without formal/ — acknowledged (ACK_NO_FORMAL=1)."
  exit 0
fi

mapping_block=""
recheck_block=""
for w in $touched_watched_list; do
  mapping_block="${mapping_block:+$mapping_block
}    $(printf '%-18s' "$w") -> $(model_for "$w")"
  recheck_block="${recheck_block:+$recheck_block
}       $(printf '%-18s' "$w") : $(recheck_for "$w")"
done

cat >&2 <<MSG
formal drift-check FAILED

  $touched_watched_list changed, but nothing under $FORMAL_PREFIX did (vs $base).

  Each watched file is mirrored by a formal model — an Aeneas/Lean kernel that
  is machine-checked, or a TLA+ model whose fidelity claims cite it by line.
  Divergence must be a deliberate decision, not an accident.

$mapping_block

  If your change touches the modelled surface:
    1. mirror it — in the corresponding formal/*_kernel/src/lib.rs for a Lean
       kernel, or in the affected formal/tla/wal/ claims and their line cites
       for the TLA+ model
    2. re-check the model(s):
$recheck_block
    3. for the Lean kernels, confirm axioms are clean (see formal/README.md)

  If it does NOT — e.g. range iterators, comments, an unrelated method, or a
  perf refactor that moves no cited line — acknowledge it:
    ACK_NO_FORMAL=1 $0

  NOTE for src/wal.rs / src/store.rs / src/persistence.rs: "no semantic change"
  is NOT enough on its own. formal/tla/wal/ carries raw src/*.rs:LINE cites, so
  an edit that only shifts lines still invalidates them. Check the cites before
  acknowledging.
MSG
exit 1
