# Elle Anomaly Classification + Mutation Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the task45 Elle harness assert the exact anomaly set per isolation level, and add feature-gated fault injection that proves the checks catch real commit-path bugs. Per `docs/superpowers/specs/2026-07-07-elle-anomaly-classification-and-mutation-testing-design.md`.

**Architecture:** Part 1 enhances `scripts/elle_check.sh` to parse elle-cli `--verbose` JSON with `jq` and assert SI anomalies ⊆ `{G2-item}` and SSI anomalies = ∅ — no production-code change. Part 2 adds a `mutation-testing` cargo feature + `src/mutation.rs` (env-selected fault selector, inert unless activated) with two cfg-gated injection points in `src/store.rs`, driven by a new `scripts/elle_mutation.sh` that inverts the harness (a clean check must fail when mutated).

**Tech Stack:** bash + `jq` + vendored elle-cli 0.1.9 (Java 21); Rust (cargo feature, `OnceLock`, env var); the existing `ultima-autobench` `elle-history` binary.

## Global Constraints

- No new Cargo dependencies. `jq` and `java` are new *runtime* tools for the check scripts (guarded with `command -v`).
- `mutation-testing` feature is OFF by default and compiled into no normal build. **Feature-on + `ULTIMA_MUTATION` unset ⇒ byte-for-byte normal behavior** (a tested invariant).
- Root `cargo test` (feature off) must be unchanged. Clippy clean: `cargo clippy -- -D warnings` and `cargo clippy --features mutation-testing -- -D warnings` and `cargo clippy -p ultima-autobench --all-targets -- -D warnings`.
- elle-cli `--verbose` emits JSON: `{"valid?":bool, "anomaly-types":[...]}` (the `anomaly-types` key is absent when valid). Verdict stdout (non-verbose) is `<file>\t<true|false|unknown>`; `unknown` is always a hard failure.
- SI whitelist is exactly `SI_ALLOWED="G2-item"`. Widening requires human review.
- Injection sites (exact): `validate_write_set` at `src/store.rs:3079`, `validate_read_set` at `src/store.rs:3156`. Both are `fn (&self, inner: &StoreInner) -> Option<Error>`; returning `None` means "no conflict".

---

### Task 1: Anomaly classification in `elle_check.sh`

**Files:**
- Modify: `scripts/elle_check.sh` (add `jq` guard + `classify`/`is_subset_of_whitelist` helpers; replace the SI/SSI verdict block, current lines 46-59)

**Interfaces:**
- Consumes: existing `verdict()`/`require()` helpers, `$JAVA`, `$JAR`, `$SI_HIST`, `$SER_HIST`.
- Produces: a stronger `scripts/elle_check.sh` (same CLI: `elle_check.sh <si.edn> <ser.edn>`) that `make consistency/elle` and Task 3 both call unchanged.

- [ ] **Step 1: Confirm elle-cli `--verbose` JSON shape (both valid and invalid)**

Run against any existing history (generate a quick one if none):

```bash
cargo run -q --release -p ultima-autobench --bin elle-history -- \
  --isolation serializable --threads 8 --txns-per-thread 400 --out /tmp/t-ssi.edn
cargo run -q --release -p ultima-autobench --bin elle-history -- \
  --isolation si --threads 8 --txns-per-thread 400 --out /tmp/t-si.edn
java -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar --model list-append \
  --consistency-models serializable --verbose /tmp/t-ssi.edn \
  | jq -r '((.["valid?"])|tostring) + "|" + (((.["anomaly-types"]) // []) | sort | join(","))'
java -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar --model list-append \
  --consistency-models serializable --verbose /tmp/t-si.edn \
  | jq -r '((.["valid?"])|tostring) + "|" + (((.["anomaly-types"]) // []) | sort | join(","))'
```

Expected: SSI prints `true|` ; SI prints `false|G2-item`. If SI prints anything other than `false|G2-item` (e.g. an empty history WARN case `true|`), regenerate with higher contention. This validates the helper's jq filter before wiring it in.

- [ ] **Step 2: Add the `jq` guard and helpers to `scripts/elle_check.sh`**

After the existing `[ -f "$FIXTURE" ] || {...}` line (line 17), add the jq guard:

```bash
command -v jq >/dev/null 2>&1 || { echo "error: jq not found (required for anomaly classification)" >&2; exit 1; }
```

After the `verdict()` function (after line 30), add:

```bash
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
```

- [ ] **Step 3: Replace the SI/SSI verdict block (current lines 46-59)**

Replace exactly this block:

```bash
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
```

with:

```bash
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
```

(The fixture smoke test at lines 42-44 and the final `elle consistency check passed` line stay unchanged.)

- [ ] **Step 4: Run the full pipeline; classification must pass**

Run: `make consistency/elle`
Expected: both passes end with `elle consistency check passed`; the SI step prints `OK: SI anomalies ⊆ {G2-item} (got: G2-item) — write skew only` and the SSI step prints `OK: SSI history has no anomaly-types`. A `WARN` on the SI step (no write skew) means the run was under-contended — re-run; it should classify on a normal run.

- [ ] **Step 5: Negative check — a lost-update history must fail classification**

Craft a history where an SI check would still be "not serializable" but for a *forbidden* reason, and confirm the whitelist rejects it:

```bash
printf '%s\n' \
'{:index 0, :type :invoke, :f :txn, :process 0, :time 0, :value [[:append 1 1]]}' \
'{:index 1, :type :ok, :f :txn, :process 0, :time 1, :value [[:append 1 1]]}' \
'{:index 2, :type :invoke, :f :txn, :process 1, :time 2, :value [[:append 1 2]]}' \
'{:index 3, :type :ok, :f :txn, :process 1, :time 3, :value [[:append 1 2]]}' \
'{:index 4, :type :invoke, :f :txn, :process 0, :time 4, :value [[:r 1 nil]]}' \
'{:index 5, :type :ok, :f :txn, :process 0, :time 5, :value [[:r 1 [2]]]}' > /tmp/lost.edn
java -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar --model list-append \
  --consistency-models serializable --verbose /tmp/lost.edn \
  | jq -r '(((.["anomaly-types"]) // []) | sort | join(","))'
```

Expected: prints an anomaly set that is **not** a subset of `{G2-item}` (e.g. contains `G-single` / `incompatible-order` / lost-update-family). This confirms the whitelist would `FAIL` on a real lost update. (Informational check; no assertion wired — it documents that the classifier discriminates.)

- [ ] **Step 6: Commit**

```bash
git add scripts/elle_check.sh
git commit -m "feat(elle): anomaly classification — SI ⊆ {G2-item}, SSI = ∅ (task47)"
```

---

### Task 2: `mutation-testing` feature + fault-injection module + injection points

**Files:**
- Create: `src/mutation.rs`
- Modify: `src/lib.rs` (add cfg-gated `mod mutation;` after `pub(crate) mod intents;`)
- Modify: `Cargo.toml` (add `mutation-testing = []` feature)
- Modify: `src/store.rs:3079` (inject into `validate_write_set`), `src/store.rs:3156` (inject into `validate_read_set`)

**Interfaces:**
- Produces: `crate::mutation::active() -> Option<crate::mutation::Mutation>` and the enum `Mutation::{SkipReadSetValidation, SkipWriteSetValidation}`, compiled only under `feature = "mutation-testing"`. Consumed by the two injection points and by Task 3's `elle-mutation` build.

- [ ] **Step 1: Write `src/mutation.rs` with the failing unit tests**

Create `src/mutation.rs`:

```rust
//! Test-only fault injection for the Elle mutation-testing harness (task47).
//! Compiled ONLY under the `mutation-testing` cargo feature and selected at
//! runtime by the `ULTIMA_MUTATION` env var. Feature-on + var-unset = no
//! mutation, so a mutation-testing build with the var unset behaves normally.

use std::sync::OnceLock;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Mutation {
    /// SSI bug: `validate_read_set` never fires — read-set validation disabled.
    SkipReadSetValidation,
    /// OCC bug: `validate_write_set` never reports a conflict — lost updates.
    SkipWriteSetValidation,
}

/// Pure mapping from the env-var value to a mutation (testable without env).
fn parse(v: Option<&str>) -> Option<Mutation> {
    match v {
        Some("skip-readset-validation") => Some(Mutation::SkipReadSetValidation),
        Some("skip-writeset-validation") => Some(Mutation::SkipWriteSetValidation),
        None | Some("") => None,
        Some(other) => panic!("unknown ULTIMA_MUTATION value: {other}"),
    }
}

/// The active mutation for this process, read once from `ULTIMA_MUTATION`.
pub(crate) fn active() -> Option<Mutation> {
    static M: OnceLock<Option<Mutation>> = OnceLock::new();
    *M.get_or_init(|| parse(std::env::var("ULTIMA_MUTATION").ok().as_deref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_maps_known_values() {
        assert_eq!(parse(Some("skip-readset-validation")), Some(Mutation::SkipReadSetValidation));
        assert_eq!(parse(Some("skip-writeset-validation")), Some(Mutation::SkipWriteSetValidation));
        assert_eq!(parse(None), None);
        assert_eq!(parse(Some("")), None);
    }

    #[test]
    #[should_panic(expected = "unknown ULTIMA_MUTATION")]
    fn parse_panics_on_unknown() {
        let _ = parse(Some("bogus"));
    }
}
```

- [ ] **Step 2: Wire the module + feature so the tests compile**

In `src/lib.rs`, after the line `pub(crate) mod intents;`, add:

```rust
#[cfg(feature = "mutation-testing")]
pub(crate) mod mutation;
```

In `Cargo.toml`, in the `[features]` block, after the `wal-iouring = [...]` line, add:

```toml
# Test-only fault injection for the Elle mutation-testing harness (task47).
# Off by default; compiled into no normal build. Inert unless ULTIMA_MUTATION is set.
mutation-testing = []
```

- [ ] **Step 3: Run the mutation unit tests**

Run: `cargo test --features mutation-testing mutation::`
Expected: `parse_maps_known_values` and `parse_panics_on_unknown` PASS.

- [ ] **Step 4: Confirm the feature is absent from normal builds**

Run: `cargo build 2>&1 | tail -2 && cargo test --lib mutation:: 2>&1 | tail -3`
Expected: normal build succeeds; `cargo test --lib mutation::` (feature off) runs **0** tests (module not compiled) — confirms `mutation.rs` is gated out.

- [ ] **Step 5: Add the two injection points in `src/store.rs`**

Line numbers below are approximate (~3079, ~3156) — anchor on the unique
function-signature lines, which are unaffected by the earlier steps. Both
functions have identical signatures except the name, so match on the full line.

At `validate_write_set` (~`src/store.rs:3079`), immediately after the line
`    fn validate_write_set(&self, inner: &StoreInner) -> Option<Error> {`
insert:

```rust
        #[cfg(feature = "mutation-testing")]
        if matches!(
            crate::mutation::active(),
            Some(crate::mutation::Mutation::SkipWriteSetValidation)
        ) {
            return None; // BUG(task47): commit-time write-conflict detection disabled → lost update
        }
```

At `validate_read_set` (~`src/store.rs:3156`), immediately after the line
`    fn validate_read_set(&self, inner: &StoreInner) -> Option<Error> {`
insert:

```rust
        #[cfg(feature = "mutation-testing")]
        if matches!(
            crate::mutation::active(),
            Some(crate::mutation::Mutation::SkipReadSetValidation)
        ) {
            return None; // BUG(task47): SSI read-set validation disabled → write skew reappears
        }
```

- [ ] **Step 6: Verify both builds compile and are clippy-clean**

Run:
```bash
cargo build --features mutation-testing 2>&1 | tail -2
cargo clippy --features mutation-testing -- -D warnings 2>&1 | tail -2
cargo clippy -- -D warnings 2>&1 | tail -2
```
Expected: all succeed with no warnings. (Feature-off clippy proves the injections don't affect normal builds; feature-on clippy proves the `matches!` form is clean.)

- [ ] **Step 7: Confirm feature-on is inert without the env var (regression safety)**

Run: `cargo test --features mutation-testing --lib 2>&1 | grep -E 'test result|FAILED'`
Expected: all lib tests PASS — with the feature compiled in but `ULTIMA_MUTATION` unset, `active()` returns `None`, so the store behaves normally and the existing suite is unaffected.

- [ ] **Step 8: Commit**

```bash
git add src/mutation.rs src/lib.rs Cargo.toml src/store.rs
git commit -m "feat(store): mutation-testing feature + commit-path fault injection (task47)"
```

---

### Task 3: autobench feature, mutation driver, make target

**Files:**
- Modify: `autobench/Cargo.toml` (add `[features] elle-mutation = ["ultima-db/mutation-testing"]`)
- Create: `scripts/elle_mutation.sh` (mode 755)
- Modify: `Makefile` (add `consistency/elle-mutation` to `.PHONY` line 1 and the target after `consistency/elle`)

**Interfaces:**
- Consumes: `scripts/elle_check.sh` (Task 1), `crate::mutation` + injection points (Task 2), the `elle-history` binary and its `--isolation/--out` flags.
- Produces: `scripts/elle_mutation.sh` (no args; honors `ELLE_DIR`, `ELLE_MUTATION_ARGS`) and `make consistency/elle-mutation`.

- [ ] **Step 1: Add the forwarding feature to autobench**

In `autobench/Cargo.toml`, after the `[dependencies]` block (before `[dev-dependencies]` if present, else at end), add:

```toml
[features]
# Build ultima-db with commit-path fault injection for scripts/elle_mutation.sh.
# Opt-in only — perf gates never enable it. See task47.
elle-mutation = ["ultima-db/mutation-testing"]
```

- [ ] **Step 2: Verify the forwarding build compiles**

Run: `cargo build --release -p ultima-autobench --features elle-mutation --bin elle-history 2>&1 | tail -2`
Expected: builds successfully (ultima-db compiled with `mutation-testing`).

- [ ] **Step 3: Manually confirm each mutation is caught (de-risk before scripting)**

```bash
D=/tmp/elle-mut-probe; A="--threads 8 --keys 8 --txns-per-thread 2000"
JAR=tools/elle-cli/elle-cli-0.1.9-standalone.jar
run() { ULTIMA_MUTATION="$1" cargo run -q --release -p ultima-autobench --features elle-mutation \
        --bin elle-history -- --isolation "$2" $A --out "$3"; }
run skip-readset-validation serializable $D/readset.edn 2>&1 | sed -n 's/^elle-history:/  /p'
java -jar $JAR --model list-append --consistency-models serializable $D/readset.edn
run skip-writeset-validation si $D/writeset.edn 2>&1 | sed -n 's/^elle-history:/  /p'
java -jar $JAR --model list-append --consistency-models snapshot-isolation $D/writeset.edn
```

Expected: the read-set run reports `serialization_failure=0` (validation disabled) and elle-cli verdict `false` (write skew now present); the write-set run's elle-cli verdict `false` (lost update violates snapshot-isolation). **If either verdict is `true`, the mutation is not being caught** — raise contention in `$A` (more threads, fewer keys, more txns) until it is; a mutation that cannot be caught is a bug to fix here, not to weaken. Record the working `$A` for Step 4's default.

- [ ] **Step 4: Write `scripts/elle_mutation.sh`**

Create `scripts/elle_mutation.sh` (use the `$A` confirmed in Step 3 as `ELLE_MUTATION_ARGS` default):

```bash
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
else
    echo "FAIL: skip-readset-validation NOT caught (verdict=$v) — no teeth on the SSI path" >&2; fail=1
fi

echo "== mutation skip-writeset-validation: SI must lose updates -> snapshot-isolation INVALID =="
gen skip-writeset-validation si "$DIR/mut-writeset/history.edn"
v="$(verdict snapshot-isolation "$DIR/mut-writeset/history.edn")"
if [ "$v" = "false" ]; then
    echo "OK: CAUGHT — SI history violates snapshot-isolation with write-conflict detection disabled"
else
    echo "FAIL: skip-writeset-validation NOT caught (verdict=$v) — no teeth on the OCC path" >&2; fail=1
fi

if [ "$fail" -eq 0 ]; then
    echo "elle mutation-testing passed: all injected bugs caught"
else
    echo "elle mutation-testing FAILED" >&2; exit 1
fi
```

```bash
chmod +x scripts/elle_mutation.sh
```

- [ ] **Step 5: Add the Makefile target**

Append `consistency/elle-mutation` to the `.PHONY` list (line 1), and after the `consistency/elle:` target's recipe block add:

```make
# Mutation test: inject known bugs into the commit path and confirm Elle catches
# them (opt-in; builds ultima-db with the mutation-testing feature). See task47.
consistency/elle-mutation:
	scripts/elle_mutation.sh
```

- [ ] **Step 6: Run the mutation-test pipeline end-to-end**

Run: `make consistency/elle-mutation`
Expected: `OK: control passed …`, then both mutations report `OK: CAUGHT …`, ending with `elle mutation-testing passed: all injected bugs caught`. If a mutation reports `FAIL … NOT caught`, return to Step 3 and raise contention.

- [ ] **Step 7: Confirm perf gate is unaffected by the new feature**

Run: `cargo build --release -p ultima-autobench --bin mw-commit-microbench 2>&1 | tail -1`
Expected: builds without `elle-mutation` (the feature is opt-in; default autobench build excludes it, so `make perf/check` never carries fault injection).

- [ ] **Step 8: Commit**

```bash
git add autobench/Cargo.toml scripts/elle_mutation.sh Makefile
git commit -m "feat: make consistency/elle-mutation — prove the Elle harness catches injected bugs (task47)"
```

---

### Task 4: Documentation

**Files:**
- Create: `docs/tasks/task47_elle_anomaly_and_mutation.md`
- Modify: `docs/tasks/task45_elle_consistency_harness.md` (add a pointer)
- Modify: `CLAUDE.md` (autobench bullet: mention `consistency/elle-mutation`)

**Interfaces:**
- Consumes: everything above (documents it).

- [ ] **Step 1: Write `docs/tasks/task47_elle_anomaly_and_mutation.md`**

Cover, in real prose (not an outline):

- **Anomaly classification.** `elle_check.sh` now parses elle-cli `--verbose` JSON: SI histories must classify as anomalies ⊆ `{G2-item}` (write skew and nothing worse — a `G-single`/`lost-update`/`G1c` fails), SSI histories as `∅`. Two independent detectors for a lost-update regression (SI-validity flip + whitelist). Note the whitelist is workload-specific and widening needs human review. Requires `jq`.
- **Mutation testing.** The `mutation-testing` cargo feature (off in every normal build) + `src/mutation.rs` selects an injected bug from `ULTIMA_MUTATION` via `OnceLock`. Two injection points: `validate_read_set → None` (SSI degrades to SI, write skew reappears) and `validate_write_set → None` (overlapping-key writers stop conflicting → lost update via the merge slow path). Safety invariant: **feature-on + env-unset = normal behavior**, verified by the control run.
- **How to run:** `make consistency/elle-mutation`; tune contention with `ELLE_MUTATION_ARGS`. The driver inverts the harness — a clean check must fail when mutated — plus a control proving the feature is inert.
- **Why it matters:** a checker that never fails is worthless; this is the teeth-test. Reference the task45 episode where the first known-bad fixture was legally serializable.
- **Limitations:** two mutations (SSI read-set, OCC write-set); a `merge_keys_from` key-drop mutation is a natural third, omitted for minimal surface. Not wired into CI (introducing CI remains a separate maintainer decision).

- [ ] **Step 2: Add a pointer from task45**

In `docs/tasks/task45_elle_consistency_harness.md`, under the top intro (after the `make consistency/elle` code block), add:

```markdown
> **Extended in task47** (`docs/tasks/task47_elle_anomaly_and_mutation.md`): the
> checks now assert the exact anomaly set (SI ⊆ `{G2-item}`, SSI = ∅), and
> `make consistency/elle-mutation` injects commit-path bugs to prove the harness
> catches them.
```

- [ ] **Step 3: Update CLAUDE.md autobench bullet**

In `CLAUDE.md`, find the `autobench/` bullet sentence that ends `see docs/tasks/task45_elle_consistency_harness.md).` and append:

```
`make consistency/elle-mutation` injects commit-path bugs to prove the checks have teeth (task47).
```

- [ ] **Step 4: Commit**

```bash
git add docs/tasks/task47_elle_anomaly_and_mutation.md docs/tasks/task45_elle_consistency_harness.md CLAUDE.md
git commit -m "docs: task47 Elle anomaly classification + mutation testing"
```

---

## Verification (whole feature)

- `cargo test` (feature off) — unchanged/green; `cargo test --features mutation-testing mutation::` — 2 pass.
- `cargo clippy -- -D warnings`, `cargo clippy --features mutation-testing -- -D warnings`, `cargo clippy -p ultima-autobench --all-targets -- -D warnings` — all clean.
- `make consistency/elle` — passes with classification (`SI anomalies ⊆ {G2-item}`, `SSI has no anomaly-types`).
- `make consistency/elle-mutation` — control passes, both mutations CAUGHT.
- `make perf/check` build path excludes `elle-mutation` (fault injection never in perf/normal builds).
