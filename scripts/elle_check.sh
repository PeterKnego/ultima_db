#!/usr/bin/env bash
# Check elle-history EDN histories with the vendored elle-cli (task41).
# Usage: scripts/elle_check.sh <si-history.edn> <serializable-history.edn>
set -euo pipefail

JAVA="${JAVA:-java}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/tools/elle-cli/elle-cli-0.1.9-standalone.jar"
FIXTURE="$ROOT/tools/elle-cli/fixtures/known_bad.edn"

usage="usage: elle_check.sh <si-history.edn> <serializable-history.edn>"
SI_HIST="${1:?$usage}"
SER_HIST="${2:?$usage}"

command -v "$JAVA" >/dev/null 2>&1 || { echo "error: java not found (set JAVA=/path/to/java)" >&2; exit 1; }
[ -f "$JAR" ] || { echo "error: missing $JAR (vendored jar; see tools/elle-cli/README.md)" >&2; exit 1; }
[ -f "$FIXTURE" ] || { echo "error: missing $FIXTURE" >&2; exit 1; }

# verdict <consistency-model> <history-file>: echoes true|false|unknown.
# elle-cli prints "<file> \t <verdict>" and exits nonzero on a false verdict,
# so the exit code is useless — parse stdout and require a recognizable verdict.
verdict() {
    local out v
    out="$("$JAVA" -jar "$JAR" --model list-append --consistency-models "$1" "$2")" || true
    v="$(printf '%s\n' "$out" | awk 'END { print $NF }')"
    case "$v" in
        true|false|unknown) printf '%s\n' "$v" ;;
        *) echo "error: no verdict from elle-cli on $2 (output: '$out')" >&2; exit 1 ;;
    esac
}

# require <expected> <actual> <label>: hard assertion (unknown never passes).
require() {
    if [ "$2" != "$1" ]; then
        echo "FAIL: $3 (verdict: $2, expected $1)" >&2
        [ "$2" = "unknown" ] && echo "hint: bump elle-cli --cycle-search-timeout or shrink the history" >&2
        exit 1
    fi
    echo "OK: $3"
}

echo "== fixture smoke test (checker must reject a known write-skew history) =="
require false "$(verdict serializable "$FIXTURE")" "known-bad fixture rejected under serializable"
require true "$(verdict snapshot-isolation "$FIXTURE")" "known-bad fixture (write skew) accepted under snapshot-isolation"

echo "== SI history vs snapshot-isolation (UltimaDB SI claim) =="
require true "$(verdict snapshot-isolation "$SI_HIST")" "SI history satisfies snapshot-isolation"

echo "== SI history vs serializable (write skew expected under SI) =="
v="$(verdict serializable "$SI_HIST")"
case "$v" in
    false) echo "OK: SI history is not serializable (write skew present, as expected)" ;;
    true)  echo "WARN: SI history unexpectedly serializable — no write skew observed;" \
                "raise contention (lower --keys, raise --threads/--txns-per-thread)" ;;
    *)     echo "FAIL: verdict '$v' on $SI_HIST (bump --cycle-search-timeout?)" >&2; exit 1 ;;
esac

echo "== Serializable history vs serializable (UltimaDB SSI claim) =="
require true "$(verdict serializable "$SER_HIST")" "Serializable history satisfies serializable"

echo "elle consistency check passed"
