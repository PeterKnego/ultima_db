#!/usr/bin/env bash
#
# bench_history.sh — Run YCSB benchmarks across commits and compare results.
#
# Results are cached under target/bench-history/<commit-sha>/
# so re-runs skip commits that already have results.
#
# Usage: ./scripts/bench_history.sh
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS_DIR="$REPO_ROOT/target/bench-history"
CLONE_DIR="$(mktemp -d)"

# First commit that has ycsb_bench.rs
FIRST_COMMIT="85486b6"

cleanup() {
    rm -rf "$CLONE_DIR"
}
trap cleanup EXIT

echo "=== UltimaDB Benchmark History ==="
echo "Results dir: $RESULTS_DIR"
echo "Temp clone:  $CLONE_DIR"
echo ""

# Clone the repo
git clone --quiet "$REPO_ROOT" "$CLONE_DIR/ultima_db"
WORK="$CLONE_DIR/ultima_db"

# Get all commits from FIRST_COMMIT to current HEAD (inclusive)
COMMITS=()
while IFS= read -r line; do
    COMMITS+=("$line")
done < <(git -C "$REPO_ROOT" log --reverse --format="%H" "${FIRST_COMMIT}^..HEAD")

echo "Commits to benchmark: ${#COMMITS[@]}"
echo ""

mkdir -p "$RESULTS_DIR"

for FULL_SHA in "${COMMITS[@]}"; do
    SHORT_SHA="${FULL_SHA:0:7}"
    COMMIT_DIR="$RESULTS_DIR/$FULL_SHA"
    COMMIT_MSG="$(git -C "$REPO_ROOT" log --format="%s" -1 "$FULL_SHA")"

    # Skip if results already exist
    if [[ -f "$COMMIT_DIR/summary.json" ]]; then
        echo "[$SHORT_SHA] CACHED  -- $COMMIT_MSG"
        continue
    fi

    echo "[$SHORT_SHA] RUNNING -- $COMMIT_MSG"

    # Checkout the commit in the clone
    git -C "$WORK" checkout --quiet "$FULL_SHA"

    mkdir -p "$COMMIT_DIR"

    # Clear criterion cache so stale results from prior checkouts aren't collected
    rm -rf "$WORK/target/criterion"

    # Run ycsb_bench
    if [[ -f "$WORK/benches/ycsb_bench.rs" ]]; then
        echo "  Running ycsb_bench..."
        (cd "$WORK" && cargo bench --bench ycsb_bench 2>&1) | tee "$COMMIT_DIR/ycsb_bench.log" | grep -E "time:" || true
    fi

    # Run ycsb_multiwriter_bench
    if [[ -f "$WORK/benches/ycsb_multiwriter_bench.rs" ]]; then
        echo "  Running ycsb_multiwriter_bench..."
        (cd "$WORK" && cargo bench --bench ycsb_multiwriter_bench 2>&1) | tee "$COMMIT_DIR/multiwriter_bench.log" | grep -E "time:" || true
    fi

    # Collect ALL estimates.json from criterion output dynamically.
    # Walk the criterion tree and find every estimates.json, then normalize
    # the benchmark name from the directory path.
    python3 -c "
import json, os, sys

work_criterion = os.path.join(sys.argv[1], 'target', 'criterion')
commit_dir = sys.argv[2]
sha = sys.argv[3]
msg = sys.argv[4]

results = {}
if os.path.isdir(work_criterion):
    for root, dirs, files in os.walk(work_criterion):
        if 'estimates.json' in files:
            est_path = os.path.join(root, 'estimates.json')
            # Derive bench name from path: strip criterion base, remove trailing /new or /base
            rel = os.path.relpath(root, work_criterion)
            parts = rel.split(os.sep)
            # Skip comparison dirs (report, change)
            if parts[0] in ('report',):
                continue
            # The parent of estimates.json is typically 'new', 'base', or the bench id
            # We want the group/id, not 'new'/'base'
            if parts[-1] in ('new', 'base'):
                parts = parts[:-1]
            # Skip non-ultima benchmarks (fjall_, rocksdb_, redb_)
            name = '/'.join(parts)
            if any(name.startswith(p) for p in ('fjall_', 'rocksdb_', 'redb_')):
                continue
            # Skip the top-level 'report' or 'change' dirs
            if name in ('report', 'change'):
                continue
            try:
                with open(est_path) as f:
                    data = json.load(f)
                if 'mean' in data:
                    safe_name = name.replace('/', '_')
                    # Normalize: strip _burst and _ultima suffixes so the
                    # same logical benchmark merges into one row across commits.
                    for suffix in ('_burst', '_ultima'):
                        if safe_name.endswith(suffix):
                            safe_name = safe_name[:-len(suffix)]
                            break
                    results[safe_name] = {
                        'mean_ns': data['mean']['point_estimate'],
                        'median_ns': data['median']['point_estimate'],
                        'std_dev_ns': data['std_dev']['point_estimate'],
                    }
            except Exception:
                pass

summary = {
    'commit': sha,
    'message': msg,
    'benchmarks': results,
}
with open(os.path.join(commit_dir, 'summary.json'), 'w') as f:
    json.dump(summary, f, indent=2)
print('  Collected {} benchmarks'.format(len(results)))
" "$WORK" "$COMMIT_DIR" "$FULL_SHA" "$COMMIT_MSG"

    echo ""
done

# --- Comparison report ---
echo ""
echo "==========================================="
echo "  COMPARISON REPORT"
echo "==========================================="
echo ""

python3 -c "
import json, os, sys, subprocess

results_dir = sys.argv[1]
repo_root = sys.argv[2]
first_commit = sys.argv[3]

# Get commit order from git log
result = subprocess.run(
    ['git', '-C', repo_root, 'log', '--reverse', '--format=%H', first_commit + '^..HEAD'],
    capture_output=True, text=True
)
commit_order = result.stdout.strip().split('\n')

# Load summaries in commit order
summaries = []
for sha in commit_order:
    summary_path = os.path.join(results_dir, sha, 'summary.json')
    if os.path.exists(summary_path):
        with open(summary_path) as f:
            summaries.append(json.load(f))

if len(summaries) < 2:
    print('Need at least 2 commits to compare.')
    sys.exit(0)

# Collect all benchmark names across all commits
all_benchmarks = set()
for s in summaries:
    all_benchmarks.update(s.get('benchmarks', {}).keys())

# Print header
hdr = '{:<45}'.format('Benchmark')
for s in summaries:
    sha = s['commit'][:7]
    hdr += '  {:>12}'.format(sha)
print(hdr)
print('-' * (45 + 14 * len(summaries)))

for bench in sorted(all_benchmarks):
    line = '{:<45}'.format(bench)
    prev_val = None
    for s in summaries:
        b = s.get('benchmarks', {}).get(bench)
        if b:
            val = b['mean_ns']
            us = val / 1000.0
            if prev_val is not None:
                pct = ((val - prev_val) / prev_val) * 100
                if pct > 3:
                    line += '  {:>8.0f}us \033[31m+{:.0f}%\033[0m'.format(us, pct)
                elif pct < -3:
                    line += '  {:>8.0f}us \033[32m{:.0f}%\033[0m'.format(us, pct)
                else:
                    line += '  {:>8.0f}us  ~'.format(us)
            else:
                line += '  {:>8.0f}us    '.format(us)
            prev_val = val
        else:
            line += '  {:>12}'.format('--')
            prev_val = None
    print(line)

print()
print('Legend: \033[32m-N%\033[0m = faster, \033[31m+N%\033[0m = slower, ~ = within 3%')
print()

# First vs last comparison
first = summaries[0]
last = summaries[-1]
print('Overall: {} ({})'.format(first['commit'][:7], first['message'][:50]))
print('     vs: {} ({})'.format(last['commit'][:7], last['message'][:50]))
print()
common = set(first.get('benchmarks', {}).keys()) & set(last.get('benchmarks', {}).keys())
for bench in sorted(common):
    v0 = first['benchmarks'][bench]['mean_ns']
    v1 = last['benchmarks'][bench]['mean_ns']
    pct = ((v1 - v0) / v0) * 100
    us0 = v0 / 1000
    us1 = v1 / 1000
    if pct > 3:
        color = '\033[31m'
    elif pct < -3:
        color = '\033[32m'
    else:
        color = ''
    reset = '\033[0m' if color else ''
    print('  {:<43} {:>8.0f}us -> {:>8.0f}us  {}{:+.1f}%{}'.format(bench, us0, us1, color, pct, reset))
" "$RESULTS_DIR" "$REPO_ROOT" "$FIRST_COMMIT"

echo ""
echo "Raw results: $RESULTS_DIR"
