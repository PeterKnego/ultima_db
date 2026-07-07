# Elle Anomaly Classification + Mutation Testing (task47) — Design

**Date:** 2026-07-07
**Status:** Draft for review (autonomous session)
**Builds on:** task45 Elle consistency harness
(`docs/tasks/task45_elle_consistency_harness.md`)

## Goal

Two paired upgrades to the existing Elle harness that make its verdicts sharper
and prove the harness actually has teeth:

1. **Anomaly classification.** Today the harness asserts pass/fail against a
   consistency model. Upgrade it to assert the *exact anomaly set*: the SI
   history must exhibit **`G2-item` (write skew) and nothing else** — no `G0`,
   `G1a/b/c`, `G-single`, `lost-update`, or `dirty-update`; the SSI history must
   exhibit **no anomalies at all**. This turns a boolean into a conformance
   statement and catches a regression where SI silently degrades to a weaker
   model while still failing the plain serializable check for the "wrong"
   reason.

2. **Mutation testing.** Deliberately inject known bugs into UltimaDB's
   OCC/SSI commit path behind a cargo feature that is absent from all normal
   builds, and confirm Elle *catches* each one. A checker that never fails is
   worthless; this proves the checks fire on real defects. (We already got a
   preview of this value in task45, when the first "known-bad" fixture turned
   out to be legally serializable and the self-test caught it.)

Both stay inside the existing `autobench` + `scripts/` + `make consistency/elle`
machinery. Anomaly classification adds **no** production-code changes; mutation
testing adds a feature-gated, env-selected fault-injection module that is inert
unless explicitly activated.

## Background: what elle-cli emits (verified)

`elle-cli --verbose` prints a JSON object per history. On a write-skew history:

```json
{"valid?": false,
 "anomaly-types": ["G2-item"],
 "not": ["repeatable-read"],
 "also-not": ["serializable", "strong-serializable", "strong-session-serializable"]}
```

- `"valid?"` — boolean verdict against the requested `--consistency-models`.
- `"anomaly-types"` — the machine-parseable array this design keys on.
- Empty/absent `anomaly-types` with `valid? true` ⇒ no anomalies found.

A **real generated** SI history (8 threads × 800 txns) reports exactly
`"anomaly-types": ["G2-item"]` under `--consistency-models serializable`, so the
SI whitelist is precisely `{G2-item}`. `jq` is available for parsing.

---

## Part 1: Anomaly classification

### Where it lives

Enhance `scripts/elle_check.sh` only. No driver or Makefile change; the same
histories `make consistency/elle` already generates are re-examined with
`--verbose`.

### New helper

```bash
# anomaly_types <model> <file>: echoes the sorted, comma-joined anomaly-types
# array from elle-cli --verbose (empty string if none). Hard-fails if elle-cli
# emits no parseable JSON.
anomaly_types() {
    local out
    out="$("$JAVA" -jar "$JAR" --model list-append --consistency-models "$1" --verbose "$2")" || true
    printf '%s' "$out" | jq -r '(.["anomaly-types"] // []) | sort | join(",")' 2>/dev/null \
        || { echo "error: elle-cli produced no JSON anomaly report for $2" >&2; exit 1; }
}
```

### Assertions (replace the current boolean checks, per pass)

Constant: `SI_ALLOWED_ANOMALIES="G2-item"` — the *only* anomaly type a correct
SI implementation may exhibit under this list-append workload. Widening this set
requires human review (a documented invariant, deliberately brittle-toward-safety).

1. **SI vs `snapshot-isolation`** → `valid? true` (unchanged; means nothing SI
   forbids is present — no dirty write/read, no lost update, no read skew).
2. **SI vs `serializable`, classified**:
   - If `valid? true` (no write skew this run) → **WARN** "no write skew
     observed — raise contention", exit 0 (probabilistic, as today).
   - Else assert `anomaly_types serializable $SI_HIST` is a subset of
     `SI_ALLOWED_ANOMALIES`. Any type outside the whitelist (e.g. `G-single`,
     `lost-update`, `G1c`) → **FAIL** "SI exhibits <type>, expected only write
     skew".
3. **SSI vs `serializable`, classified** → `valid? true` **and**
   `anomaly_types serializable $SSI_HIST` is empty. A non-empty set → **FAIL**
   "SSI exhibits <types>".

The existing fixture self-test and the SI/SSI validity checks remain; step 2/3
strengthen (not replace) the verdict with the anomaly set. Subset test in bash:
split the comma-joined actual set, fail if any element ∉ whitelist.

### Why this is stronger

A boolean "SI is not serializable" passes for *any* anomaly. Classification
pins it to write skew specifically, so a merge/OCC regression that produced a
lost update (which would still make SI "not serializable") is now caught here —
and independently by check 1 flipping to invalid. Two independent detectors for
the same class of bug.

---

## Part 2: Mutation testing

### Injection mechanism

A cargo feature `mutation-testing` on the **root crate**, off by default and
compiled into no normal build. Under it, a new module `src/mutation.rs` selects
an injected bug at runtime from the `ULTIMA_MUTATION` env var (read once via
`OnceLock`). **Feature enabled + env unset ⇒ no mutation active ⇒ behavior is
byte-for-byte normal.** That invariant is itself tested (a control run).

`src/mutation.rs` (compiled only under the feature):

```rust
//! Test-only fault injection for the Elle mutation-testing harness. Compiled
//! ONLY under the `mutation-testing` cargo feature and selected at runtime by
//! the ULTIMA_MUTATION env var. Feature-on + var-unset = no mutation.
use std::sync::OnceLock;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Mutation {
    /// SSI: validate_read_set never fires — read-set validation disabled.
    SkipReadSetValidation,
    /// OCC: validate_write_set never reports key-overlap conflicts.
    SkipWriteSetValidation,
}

pub(crate) fn active() -> Option<Mutation> {
    static M: OnceLock<Option<Mutation>> = OnceLock::new();
    *M.get_or_init(|| match std::env::var("ULTIMA_MUTATION").ok().as_deref() {
        Some("skip-readset-validation") => Some(Mutation::SkipReadSetValidation),
        Some("skip-writeset-validation") => Some(Mutation::SkipWriteSetValidation),
        None | Some("") => None,
        Some(other) => panic!("unknown ULTIMA_MUTATION value: {other}"),
    })
}
```

`src/lib.rs`: `#[cfg(feature = "mutation-testing")] mod mutation;`

### Injection points (both in `src/store.rs`)

At the top of `validate_read_set` (store.rs:3156):

```rust
#[cfg(feature = "mutation-testing")]
if crate::mutation::active() == Some(crate::mutation::Mutation::SkipReadSetValidation) {
    return None; // BUG: SSI never aborts on a stale read
}
```

At the top of `validate_write_set` (store.rs:3079), before its
table-deletion and key-overlap conflict scans:

```rust
#[cfg(feature = "mutation-testing")]
if crate::mutation::active() == Some(crate::mutation::Mutation::SkipWriteSetValidation) {
    return None; // BUG: commit-time write-conflict detection disabled → lost update
}
```

This disables commit-time write-conflict detection wholesale; under the
list-append workload (no table deletes) that isolates cleanly to the key-overlap
OCC path — two concurrent writers to the same key both commit, and the second's
`merge_keys_from` overwrites the first's append (lost update). The write-site
intent check (store.rs:1779) only catches writers whose intents overlap *in
time*, so the non-overlapping-in-time case still slips through and produces the
lost update Elle detects.

Both injections are single cfg-gated statements; with the feature off they
vanish, with it on but env unset they are unreachable (`active()` returns
`None`).

### autobench wiring

`autobench/Cargo.toml` gains a forwarding feature so the mutation code is opt-in
even within autobench (perf gates never enable it):

```toml
[features]
elle-mutation = ["ultima-db/mutation-testing"]
```

### Mutation-test driver: `scripts/elle_mutation.sh`

The assertion **inverts** the clean harness: a check that passes on clean code
must *fail* with the mutation active. That is robust to the exact anomaly name
Elle assigns.

For each case: generate a history with `ULTIMA_MUTATION=<bug>` set, then run the
relevant clean assertion and require it to now report an anomaly.

| Mutation | Isolation run | Clean expectation | Mutated expectation (teeth) |
|---|---|---|---|
| `skip-readset-validation` | serializable | SSI history `serializable`-valid | now **invalid** (write skew reappears — SSI degraded to SI) |
| `skip-writeset-validation` | si | SI history `snapshot-isolation`-valid | now **invalid** (lost update from unconflicted overlapping writers) |
| *(none)* — control | si + serializable | clean checks pass | still pass (feature-on, env-unset ⇒ no change) |

Script shape (`set -euo pipefail`):

1. Build once: `cargo build --release -p ultima-autobench --bin elle-history --features elle-mutation`.
2. Control: with no `ULTIMA_MUTATION`, generate SI+SSI histories and run
   `elle_check.sh` — must pass. (Proves the feature build itself is inert.)
3. For each mutation: set `ULTIMA_MUTATION`, generate the history for its
   isolation level, ask elle-cli for the clean verdict, and **require it to be
   `false`** (anomaly caught). If elle-cli still says `true`, FAIL:
   "mutation <m> NOT caught — harness has no teeth".
4. Report each caught mutation + the anomaly-types Elle assigned (informational).

Generation sets `ULTIMA_MUTATION` as an env-var prefix on `cargo run` (env vars
propagate to the child process), so the driver needs no manual binary lookup:
`ULTIMA_MUTATION=<bug> cargo run --release -p ultima-autobench --features
elle-mutation --bin elle-history -- --isolation <iso> --out <path>`.

### Make target

```make
consistency/elle-mutation:      # opt-in; builds ultima-db with fault injection
	scripts/elle_mutation.sh
```

Added to `.PHONY`. Not part of `make consistency/elle` (different feature build).

### Empirical confirmation required during implementation

The plan must verify each mutation actually produces a *detected* anomaly with
the default workload. If `skip-writeset-validation` does not reliably yield a
lost update Elle flags (e.g. contention too low), raise contention for that run
(smaller `--keys`, more threads) rather than weakening the assertion. A mutation
that cannot be caught is a spec bug to fix, not to paper over.

---

## Components summary

| File | Change |
|---|---|
| `scripts/elle_check.sh` | + `anomaly_types()` helper; classify SI (⊆ `{G2-item}`) and SSI (∅) |
| `src/mutation.rs` | **new**, feature-gated fault-injection selector |
| `src/lib.rs` | + `#[cfg(feature="mutation-testing")] mod mutation;` |
| `src/store.rs` | + 2 cfg-gated injection statements |
| `Cargo.toml` | + `mutation-testing = []` feature |
| `autobench/Cargo.toml` | + `elle-mutation = ["ultima-db/mutation-testing"]` |
| `scripts/elle_mutation.sh` | **new** mutation-test driver |
| `Makefile` | + `consistency/elle-mutation`; `.PHONY` |
| `docs/tasks/task47_*.md` | **new** feature doc; task45 gets a pointer |

## Error handling

- Classification: `anomaly_types` hard-fails if elle-cli emits no JSON (never
  silently treats a parse failure as "no anomalies"). `unknown` verdicts remain
  hard failures as in task45.
- Mutation driver: an uncaught mutation is a hard failure. A `panic!` on an
  unknown `ULTIMA_MUTATION` value surfaces typos immediately.

## Testing

- **Classification**: exercised by `make consistency/elle` (SI must classify as
  exactly `{G2-item}`, SSI as `∅`). A unit test is unnecessary — the real
  histories are the test.
- **Mutation module**: a `#[cfg(feature="mutation-testing")]` unit test in
  `src/mutation.rs` asserting `active()` maps each env value correctly is
  optional; the end-to-end `make consistency/elle-mutation` run is the
  acceptance test.
- **Regression safety**: root `cargo test` (feature off) must be unchanged;
  `cargo build --features mutation-testing` with no env var must produce a store
  that still passes `make consistency/elle` (the control case).

## Known limitations / non-goals

- Two mutations (SSI read-set, OCC write-set). A third — dropping a key in
  `Table::merge_keys_from` — is a natural follow-up but omitted here to keep the
  injection surface minimal; the two chosen cover both isolation paths.
- Mutation testing is manual/opt-in, not wired into CI (introducing CI to this
  repo remains a separate maintainer decision).
- Classification whitelist is workload-specific (`{G2-item}` for point+scan
  list-append). A predicate/range workload could legitimately surface other
  anti-dependency variants and would need its own whitelist.
