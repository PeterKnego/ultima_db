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
command -v jq >/dev/null 2>&1 || { echo "error: jq not found (required for anomaly classification)" >&2; exit 1; }

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

# classify <consistency-model> <history-file>: echoes "<valid?>|<sorted,joined anomaly-types>"
# from elle-cli --verbose. e.g. "false|G2-item" or "true|". Hard-fails on no JSON.
classify() {
    local out
    out="$("$JAVA" -jar "$JAR" --model list-append --consistency-models "$1" --verbose "$2")" || true
    printf '%s' "$out" \
        | jq -r '((.["valid?"])|tostring) + "|" + (((.["anomaly-types"]) // []) | sort | join(","))' 2>/dev/null \
        || { echo "error: elle-cli produced no JSON report for $2" >&2; exit 1; }
}

# is_subset_of_whitelist <comma-list> <space-list>: 0 if every element of the
# comma list appears in the space-separated whitelist; 1 otherwise.
is_subset_of_whitelist() {
    local IFS=,; local t
    for t in $1; do
        [ -z "$t" ] && continue
        case " $2 " in *" $t "*) ;; *) return 1 ;; esac
    done
    return 0
}

# Only anomaly a correct SI implementation may exhibit (list-append workload).
# Widening this set requires human review — see task47.
SI_ALLOWED="G2-item"

echo "== fixture smoke test (checker must reject a known write-skew history) =="
require false "$(verdict serializable "$FIXTURE")" "known-bad fixture rejected under serializable"
require true "$(verdict snapshot-isolation "$FIXTURE")" "known-bad fixture (write skew) accepted under snapshot-isolation"

echo "== SI history vs snapshot-isolation (UltimaDB SI claim) =="
require true "$(verdict snapshot-isolation "$SI_HIST")" "SI history satisfies snapshot-isolation"

echo "== SI history vs serializable, classified (write skew and nothing worse) =="
si="$(classify serializable "$SI_HIST")"; si_valid="${si%%|*}"; si_types="${si#*|}"
case "$si_valid" in
    true)  echo "WARN: SI history unexpectedly serializable — no write skew observed;" \
                "raise contention (lower --keys, raise --threads/--txns-per-thread)" ;;
    false)
        if is_subset_of_whitelist "$si_types" "$SI_ALLOWED"; then
            echo "OK: SI anomalies ⊆ {$SI_ALLOWED} (got: ${si_types:-none}) — write skew only"
        else
            echo "FAIL: SI exhibits anomalies outside {$SI_ALLOWED}: $si_types" >&2; exit 1
        fi ;;
    *) echo "FAIL: unrecognized valid? '$si_valid' on $SI_HIST" >&2; exit 1 ;;
esac

echo "== SSI history vs serializable, classified (no anomalies) =="
ssi="$(classify serializable "$SER_HIST")"; ssi_valid="${ssi%%|*}"; ssi_types="${ssi#*|}"
require true "$ssi_valid" "SSI history satisfies serializable"
if [ -n "$ssi_types" ]; then
    echo "FAIL: SSI exhibits anomaly-types: $ssi_types" >&2; exit 1
fi
echo "OK: SSI history has no anomaly-types"

echo "elle consistency check passed"
