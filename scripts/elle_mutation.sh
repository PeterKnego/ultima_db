#!/usr/bin/env bash
# Mutation-test the Elle harness (task47): inject known bugs into UltimaDB's
# commit path and confirm elle-cli CATCHES each one. A checker that never fails
# is worthless — this proves the checks have teeth. Opt-in; needs java + jq.
set -euo pipefail

JAVA="${JAVA:-java}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/tools/elle-cli/elle-cli-0.1.9-standalone.jar"
DIR="${ELLE_DIR:-/tmp/ultima-elle-mutation}"
# Contention tuned so injected lost updates are reliably observable.
GEN_ARGS="${ELLE_MUTATION_ARGS:---threads 8 --keys 8 --txns-per-thread 2000}"

command -v "$JAVA" >/dev/null 2>&1 || { echo "error: java not found (set JAVA=)" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "error: jq not found" >&2; exit 1; }
[ -f "$JAR" ] || { echo "error: missing $JAR" >&2; exit 1; }

verdict() { # <model> <file>
    local out
    out="$("$JAVA" -jar "$JAR" --model list-append --consistency-models "$1" "$2")" || true
    printf '%s\n' "$out" | awk 'END { print $NF }'
}

gen() { # <mutation-or-empty> <isolation> <out>
    ULTIMA_MUTATION="$1" cargo run -q --release -p ultima-autobench \
        --features elle-mutation --bin elle-history -- \
        --isolation "$2" $GEN_ARGS --out "$3" 2>&1 | sed -n 's/^elle-history:/  /p'
}

echo "== build elle-history with fault injection (ultima-db/mutation-testing) =="
cargo build -q --release -p ultima-autobench --features elle-mutation --bin elle-history

echo "== control: feature ON, no mutation -> clean checks must still pass =="
gen "" si           "$DIR/control-si/history.edn"
gen "" serializable "$DIR/control-ser/history.edn"
"$ROOT/scripts/elle_check.sh" "$DIR/control-si/history.edn" "$DIR/control-ser/history.edn" \
    || { echo "FAIL: feature-on control did not pass clean checks (feature not inert)" >&2; exit 1; }
echo "OK: control passed — mutation-testing feature is inert when unselected"

fail=0

echo "== mutation skip-readset-validation: SSI must degrade -> serializable INVALID =="
gen skip-readset-validation serializable "$DIR/mut-readset/history.edn"
v="$(verdict serializable "$DIR/mut-readset/history.edn")"
if [ "$v" = "false" ]; then
    echo "OK: CAUGHT — SSI history not serializable with read-set validation disabled"
elif [ "$v" = "unknown" ]; then
    echo "FAIL: elle-cli verdict UNDECIDED (unknown) on skip-readset-validation — cannot confirm teeth (bump --cycle-search-timeout?)" >&2; fail=1
else
    echo "FAIL: skip-readset-validation NOT caught (verdict=$v) — no teeth on the SSI path" >&2; fail=1
fi

echo "== mutation skip-writeset-validation: SI must lose updates -> snapshot-isolation INVALID =="
gen skip-writeset-validation si "$DIR/mut-writeset/history.edn"
v="$(verdict snapshot-isolation "$DIR/mut-writeset/history.edn")"
if [ "$v" = "false" ]; then
    echo "OK: CAUGHT — SI history violates snapshot-isolation with write-conflict detection disabled"
elif [ "$v" = "unknown" ]; then
    echo "FAIL: elle-cli verdict UNDECIDED (unknown) on skip-writeset-validation — cannot confirm teeth (bump --cycle-search-timeout?)" >&2; fail=1
else
    echo "FAIL: skip-writeset-validation NOT caught (verdict=$v) — no teeth on the OCC path" >&2; fail=1
fi

echo "== mutation drop-merge-key: commit merge drops an edit -> snapshot-isolation INVALID =="
gen drop-merge-key si "$DIR/mut-merge/history.edn"
v="$(verdict snapshot-isolation "$DIR/mut-merge/history.edn")"
if [ "$v" = "false" ]; then
    echo "OK: CAUGHT — SI history violates snapshot-isolation when the commit merge drops a key"
elif [ "$v" = "unknown" ]; then
    echo "FAIL: elle-cli verdict UNDECIDED (unknown) on drop-merge-key — cannot confirm teeth (bump --cycle-search-timeout?)" >&2; fail=1
else
    echo "FAIL: drop-merge-key NOT caught (verdict=$v) — no teeth on the commit-merge path" >&2; fail=1
fi

if [ "$fail" -eq 0 ]; then
    echo "elle mutation-testing passed: all injected bugs caught"
else
    echo "elle mutation-testing FAILED" >&2; exit 1
fi
