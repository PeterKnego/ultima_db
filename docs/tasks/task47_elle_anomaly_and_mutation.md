# Task 47: Elle Anomaly Classification + Mutation Testing

Two paired upgrades to the task45 Elle consistency harness
(`docs/tasks/task45_elle_consistency_harness.md`): sharpening its verdicts from
a boolean pass/fail into an exact anomaly-set assertion, and proving — via
deliberate fault injection — that the harness actually catches real
commit-path bugs rather than passing by construction.

Design history: `docs/superpowers/specs/2026-07-07-elle-anomaly-classification-and-mutation-testing-design.md`.

## Anomaly classification

`scripts/elle_check.sh` previously asked elle-cli for a plain boolean verdict
per (history, consistency-model) pair. That's a weak assertion: "SI is not
serializable" is true whether the underlying anomaly is the expected write
skew or something a lot worse — a lost update or a `G1c` cycle that happens to
also fail the serializable check for the wrong reason. The check now parses
elle-cli's `--verbose` JSON output and asserts the *exact* anomaly-types set,
not just the verdict.

elle-cli's `--verbose` mode emits a JSON object per history, e.g. for a
write-skew history:

```json
{"valid?": false,
 "anomaly-types": ["G2-item"],
 "not": ["repeatable-read"],
 "also-not": ["serializable", ...]}
```

`scripts/elle_check.sh` adds a `classify()` helper that runs elle-cli with
`--verbose` and pipes the output through `jq` into a single
`"<valid?>|<sorted,comma-joined anomaly-types>"` string (e.g. `false|G2-item`
or `true|`). It hard-fails (`exit 1`) if elle-cli produces no parseable JSON —
a parse failure is never silently treated as "no anomalies found". `jq` is
now a hard runtime dependency of the script, checked up front alongside
`java`.

The per-history assertions:

- **SI vs `snapshot-isolation`** — unchanged: `valid? true` (SI must not
  exhibit anything SI itself forbids — dirty writes/reads, lost updates, read
  skew).
- **SI vs `serializable`, classified** — if `valid? true`, the run didn't
  produce write skew this time (contention is probabilistic); the script
  prints a WARN and exits 0, same as before. If `valid? false`, the
  `anomaly-types` set must be a subset of the whitelist
  `SI_ALLOWED="G2-item"` — the only anomaly a correct SI implementation may
  exhibit on this list-append workload. Anything outside that set (a
  `G-single`, `lost-update`, or `G1c`, for example) is a hard `FAIL`, because
  it means SI is doing something worse than the write skew it's allowed to
  do.
- **SSI vs `serializable`, classified** — `valid? true` **and**
  `anomaly-types` must be empty. A non-empty set fails even if `valid?`
  happened to read `true` (belt-and-suspenders: elle-cli shouldn't emit both,
  but the script doesn't trust that).

This gives two independent detectors for the same class of regression: a
commit-path bug that produces a lost update under nominal "SI" would flip
check 1 (SI vs `snapshot-isolation`) to `invalid` on its own, *and* if it
somehow didn't, check 2's whitelist would still catch the stray anomaly type
under the serializable check. The whitelist is workload-specific — it was
derived empirically from a real generated SI history (8 threads × 800 txns)
under the point-history list-append workload, which reported exactly
`anomaly-types: ["G2-item"]`. A different workload shape (predicate reads,
range scans with different semantics) could legitimately surface other
anti-dependency anomaly variants, so **widening `SI_ALLOWED` requires human
review** — it is a deliberately brittle-toward-safety invariant, not something
to relax to make a new workload pass.

No driver or Makefile changes were needed for this half of the work; `make
consistency/elle` re-examines the exact same histories it already generates,
just with a stricter check.

## Mutation testing

A checker that never fails is worthless. The second half of task47 answers
"does this harness actually have teeth?" by deliberately breaking UltimaDB's
commit path in two specific, known ways and confirming Elle flags each one.
(Task45 got a preview of this value for free: its first "known-bad" fixture
turned out to be legally serializable, and iterating on that fixture is what
motivated formalizing the exercise here.)

### Injection mechanism

A new root-crate cargo feature, `mutation-testing = []` (in `Cargo.toml`), is
**off in every normal build** — `cargo build`, `cargo test`, `cargo bench`,
and the default `ultima-autobench` build (including `make perf/check`) never
compile it in. Under the feature, `src/lib.rs` compiles in a new module,
`src/mutation.rs`, gated as `#[cfg(feature = "mutation-testing")] pub(crate)
mod mutation;`.

`src/mutation.rs` defines a `Mutation` enum with three variants and an
`active()` accessor that reads the `ULTIMA_MUTATION` env var exactly once via
a `OnceLock`:

```rust
pub(crate) enum Mutation {
    SkipReadSetValidation,
    SkipWriteSetValidation,
    DropMergeKey,
}
```

`parse()` maps the env value: `"skip-readset-validation"` →
`SkipReadSetValidation`, `"skip-writeset-validation"` →
`SkipWriteSetValidation`, `"drop-merge-key"` → `DropMergeKey`,
`None`/`""` → `None` (no mutation active), any other
string → `panic!` (typos in `ULTIMA_MUTATION` surface immediately rather than
silently no-op'ing). `parse()` is a pure function with its own unit tests
(`mutation::tests::parse_maps_known_values`,
`mutation::tests::parse_panics_on_unknown`), run only under
`--features mutation-testing`.

**Safety invariant: feature-on + env-unset = normal behavior.** This is the
property that makes the feature safe to compile into a mutation-testing build
without accidentally shipping a bug — `active()` returns `None` whenever
`ULTIMA_MUTATION` is unset, so both injection sites below become dead code at
runtime. This was verified two ways: a plain `cargo test --features
mutation-testing --lib` run (no env var set) passes all 380 lib tests (378
feature-off tests plus the 2 `mutation::` unit tests — no other test
count drift), and the mutation driver's own **control run** (below) builds
the feature in and requires the clean Elle checks to still pass before it
will attempt either mutation.

### Injection points

Two injection points live in `src/store.rs`, one statement each, at the top
of the two commit-time validation functions; a third lives in
`src/table.rs` inside the commit merge itself:

- `validate_write_set` (src/store.rs:3079) — OCC's commit-time write-conflict
  scan. With `ULTIMA_MUTATION=skip-writeset-validation`, it returns `None`
  (no conflict) unconditionally, before its table-deletion and key-overlap
  scans run. This disables commit-time write-conflict detection wholesale;
  under the list-append workload (no table deletes) it isolates to the
  key-overlap OCC path — two concurrent writers to the same key both commit,
  and the second commit's `merge_keys_from` overwrites the first's append, a
  lost update. The separate eager per-op write-conflict check in
  `claim_intent` (`src/store.rs:1845`), called from `update()`/`delete()`,
  only catches writers whose intents overlap *in time*, so
  non-overlapping-in-time writers still slip through this mutation and
  produce the anomaly Elle detects.
- `validate_read_set` (src/store.rs:3163) — SSI's read-set validation. With
  `ULTIMA_MUTATION=skip-readset-validation`, it returns `None`
  unconditionally, so SSI silently degrades to plain SI: write skew, which
  SSI is supposed to forbid, reappears.
- `Table::merge_keys_from` (src/table.rs) — the commit slow-path merge that
  replays a writer's edited keys onto the current latest snapshot's table.
  With `ULTIMA_MUTATION=drop-merge-key`, it silently skips the *first* key of
  each merge (`continue` before the upsert/delete), losing that edit. Unlike
  the other two, this bug lives *below* the isolation layer: OCC/SSI
  validation has already passed (the dropped key is disjoint from every
  concurrent commit, so it never was a conflict), yet the writer's append at
  that key never lands — a lost update the merge is contractually required to
  preserve. It only bites in the MultiWriter slow path (a concurrent commit
  touched the same table since the writer's base); under the small-keyspace
  list-append workload that path runs constantly.

The two `store.rs` sites are a single `#[cfg(feature = "mutation-testing")]`-
gated `if matches!(crate::mutation::active(),
Some(crate::mutation::Mutation::...)) { return None; }`; the `table.rs` site
is a gated `let mut drop_first = matches!(...)` plus an in-loop `if drop_first
{ drop_first = false; continue; }`. With the feature off all three vanish
entirely; with it on but `ULTIMA_MUTATION` unset, `active()` returns `None`
and they're unreachable.

### autobench wiring

`autobench/Cargo.toml` gains a forwarding feature,
`elle-mutation = ["ultima-db/mutation-testing"]`, so the fault-injection build
stays opt-in even inside `ultima-autobench` — the default `ultima-autobench`
build graph (and therefore `make perf/check`) never pulls in
`mutation-testing`; confirmed via `cargo tree -p ultima-autobench -e
features` showing no trace of it without the flag, and a plain `cargo build
--release -p ultima-autobench --bin mw-commit-microbench` (no
`--features elle-mutation`) building clean.

### Driver: `scripts/elle_mutation.sh`

`scripts/elle_mutation.sh` builds `elle-history` once with
`--features elle-mutation`, then:

1. **Control**: generates SI and SSI histories with `ULTIMA_MUTATION` unset
   (feature compiled in, no mutation selected) and runs the ordinary
   `scripts/elle_check.sh` against them — this must still pass, or the driver
   fails with "feature-on control did not pass clean checks (feature not
   inert)". This is the practical confirmation of the safety invariant above.
2. **`skip-readset-validation`** (generated under `--isolation serializable`):
   the SSI history must now come back `serializable`-**invalid** (write skew
   reappearing). If elle-cli still reports `true`, the driver fails loudly:
   "skip-readset-validation NOT caught — no teeth on the SSI path".
3. **`skip-writeset-validation`** (generated under `--isolation si`): the SI
   history must now come back `snapshot-isolation`-**invalid** (lost update).
   Same hard-failure-on-miss behavior.
4. **`drop-merge-key`** (generated under `--isolation si`): the SI history
   must likewise come back `snapshot-isolation`-**invalid** (lost update from
   the commit merge silently dropping an edit). Same hard-failure-on-miss
   behavior — "drop-merge-key NOT caught — no teeth on the commit-merge path".

The assertion deliberately **inverts** the clean harness rather than pattern
matching a specific anomaly name — it just requires the previously-clean
verdict to flip to `false`. That makes it robust to exactly which anomaly
type elle-cli assigns to a given mutation.

Contention is tuned via `GEN_ARGS`/`ELLE_MUTATION_ARGS` (default `--threads 8
--keys 8 --txns-per-thread 2000` — 8 threads hammering only 8 pre-seeded keys)
so all three injected bugs are reliably observable in a single run: heavy
overlap on a small keyspace gives the mutated commit path many chances per run
to let a lost update or a write-skew pair through, and eliding detection even
once is enough for elle-cli to flag the whole history invalid. The
`drop-merge-key` mutation additionally needs the MultiWriter *slow-path* merge
to fire (a concurrent commit on the same table), which the small keyspace
makes near-constant. All three mutations were caught on the first attempt at
this contention level during implementation — no escalation (smaller `--keys`,
more threads/txns) was needed in practice, though the design explicitly calls
for raising contention rather than weakening the assertion if a mutation ever
isn't reliably caught.

### `make consistency/elle-mutation`

```make
consistency/elle-mutation:      # opt-in; builds ultima-db with fault injection
	scripts/elle_mutation.sh
```

Added alongside `consistency/elle` in the `.PHONY` line. It is a separate
target, not folded into `make consistency/elle`, because it requires a
different feature-enabled build of `ultima-db`.

## How to run

```bash
make consistency/elle           # now asserts SI ⊆ {G2-item}, SSI = ∅
make consistency/elle-mutation  # proves the above checks have teeth
ELLE_MUTATION_ARGS="--threads 16 --keys 4 --txns-per-thread 4000" make consistency/elle-mutation
```

Both require `java` (Temurin 21, per task45) and `jq`; `elle-mutation`
additionally needs a `cargo build --features elle-mutation` of
`ultima-autobench`, which the script performs itself.

## Limitations

- Three mutations are wired up, covering both isolation-enforcement paths and
  the commit merge below them: the SSI read-set bypass, the OCC write-set
  bypass, and the `drop-merge-key` bug in `Table::merge_keys_from` (a lost
  update in the merge slow path, below the isolation layer). Further injection
  sites (e.g. corrupting `next_id` reconciliation, or dropping an index
  maintenance hook) are possible but were left out — the three wired ones
  already exercise conflict detection on both read and write sets plus the
  per-key merge that reconciles disjoint concurrent writers.
- Mutation testing runs on the **weekly** `elle-deep` job
  (`.github/workflows/consistency.yml`, alongside the canonical
  `make consistency/elle`) and on `workflow_dispatch`, not on every PR — the
  fault-injection build and extra generations make it too heavy to gate PRs on.
  PRs are gated by the bounded `elle` job (the plain consistency check). Run it
  locally any time with `make consistency/elle-mutation`.
- The anomaly whitelist (`SI_ALLOWED="G2-item"`) is specific to the
  list-append workload `make consistency/elle` generates. The predicate
  (index) pass added later shares it unchanged — index reads degrade to the
  same coarse `table_scan` read as a full scan, so they exhibit the same
  `{G2-item}` profile (see task45 "Predicate reads"). A genuinely
  differently-shaped workload (e.g. a domain-invariant predicate-write-skew
  test) could still legitimately need its own whitelist, hence the
  human-review requirement on widening it.
