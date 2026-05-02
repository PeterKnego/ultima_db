#!/usr/bin/env bash
# Prepend SPDX + copyright header to every .rs source file in the workspace.
# Idempotent: skips files that already contain "SPDX-License-Identifier".
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HEADER='// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego'

added=0
skipped=0
while IFS= read -r -d '' f; do
  if grep -q 'SPDX-License-Identifier' "$f"; then
    skipped=$((skipped + 1))
    continue
  fi
  tmp="$(mktemp)"
  { printf '%s\n\n' "$HEADER"; cat "$f"; } > "$tmp"
  mv "$tmp" "$f"
  added=$((added + 1))
done < <(find "$ROOT" -name '*.rs' -not -path '*/target/*' -print0)

echo "stamped: $added, already-stamped: $skipped"
