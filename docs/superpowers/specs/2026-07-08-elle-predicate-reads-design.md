# Design: Predicate-read mode for the Elle consistency harness

Date: 2026-07-08
Status: approved (brainstorming), pending spec review
Extends: task45 (Elle consistency harness), task47 (anomaly classification + mutation)

## Goal

Widen the `elle-history` workload (`autobench/src/bin/elle-history.rs`) to include
**predicate reads** — reads that select a group of rows via a secondary index
(`get_by_index` for equality, `index_range` for a range) rather than a point
`get` or a full-table `range` scan.

The purpose is **coverage of the secondary-index read path under concurrency**.
Today the Elle harness never calls `get_by_index` / `index_range`, so their
interaction with MultiWriter SI/SSI is exercised only by narrow unit tests, never
end-to-end against a checked history. This change drives those methods under
contention and verifies:

1. Index reads register the SSI coarse `table_scan` read correctly, so a
   predicate reader is serialization-failed by a concurrent write to the table
   (SSI stays clean — no phantoms).
2. Index membership stays correct under concurrent `update`s (an integrity
   assertion inside the transaction).

### Non-goals

- **Not** a phantom / predicate-write-skew demonstration. That would need a
  domain-invariant checker (SmallBank-style) because elle-cli's `list-append`
  model has no notion of a predicate. Explicitly out of scope (a possible
  future task).
- **Not** a change to UltimaDB's SSI read-set granularity. Index reads continue
  to degrade to `table_scan`; finer-grained predicate tracking is a separate,
  larger feature.
- **No new elle-cli model or jar change.** The emitted history stays valid
  `list-append` EDN.

## Key facts that shape the design

- Every secondary-index read in `Table`/`TableWriter` registers a coarse
  `table_scan` in the SSI read-set (`src/store.rs:2128–2164`:
  `get_unique`, `get_by_index`, `get_by_key`, `index_range`). So a predicate
  read taints the whole table for conflict detection — sound (prevents
  phantoms) but coarse.
- Consequence for the workload: a predicate read behaves **exactly like the
  existing full-`range` scan** for conflict purposes. Under **SSI** it is clean;
  under **SI** it is untracked, so predicate-gated write skew appears — the same
  `{G2-item}` anomaly class the scan pass already produces.
- **Therefore no new anomaly whitelist is needed.** The task45/47 remark that a
  predicate workload "could legitimately need its own whitelist" turns out false
  for this design: predicate reads share the scan profile. This is a documented
  finding, not a gap.

## Design

### 1. Row schema + index

```rust
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ElleRow {
    bucket: u64,      // static: assigned once at seed, never mutated
    list: Vec<u64>,   // the list-append value
}
```

At seed time key `i` (0-based insert order) gets `bucket = (i % args.buckets)`.
After `insert_batch`, define a non-unique index in the same setup transaction:

```rust
t.define_index("bucket", IndexKind::NonUnique, |r: &ElleRow| r.bucket)?;
```

Because `bucket` is assigned once and never changed, the set of keys in each
bucket is **statically known** from the seed order and **invariant** for the
whole run. The op generator computes bucket membership directly (no query
needed), which keeps generation deterministic and decoupled from store state;
the actual index call inside the transaction is what exercises the read path.

### 2. CLI flags

- `--buckets <N>` (default `4`): number of index buckets.
- `--predicate-ratio <p>` (default `0.0`): probability a transaction is a
  predicate transaction.

Per-transaction mode is rolled in precedence order **predicate → scan → point**,
**before** op generation (predicate mode needs the selected bucket to restrict
which keys the ops touch — see §4):

```rust
let mode = if rng.chance(predicate_ratio) { Mode::Predicate }
           else if rng.chance(scan_ratio)  { Mode::Scan }
           else                            { Mode::Point };
```

This reorders the per-txn RNG draws relative to the current code (which rolls
`scan` *after* `gen_ops`), so the exact byte output for a given seed changes
versus the pre-change binary. That is acceptable: the histories are regenerated
and checked on every run — there is no committed golden history to match. The
determinism guarantee that matters is unchanged and preserved: **same
`--seed` + same args ⇒ same history** (the PRNG is untouched; per-thread streams
remain `seed + thread index`). A unit test pins this reproducibility
(`generation_is_reproducible_for_a_fixed_seed`); backward-identity with the
pre-change stream is explicitly *not* a goal.

### 3. Predicate selection + transaction execution

A predicate transaction:

1. Rolls **equality vs range** from the RNG:
   - equality: pick a bucket `b ∈ [0, buckets)` → `get_by_index("bucket", &b)`.
   - range: pick `lo ≤ hi` in `[0, buckets)` → `index_range("bucket", lo..=hi)`.
2. Computes the **expected key set** from static membership.
3. Generates ops (reads/appends) restricted to that key set (see §4).
4. In `run_txn`:
   - Executes the index read (registers the SSI `table_scan`, seeds the local
     working copy for read-your-writes).
   - **Asserts** the returned id-set equals the expected static membership; a
     mismatch means concurrent `update`s corrupted the index, which is a real
     bug — surfaced as `TxnFail::Other` (retires the process → degraded run →
     non-zero exit).
   - Serves reads/appends from the local copy; appends preserve the row's
     `bucket`.
   - Commits; `WriteConflict` / `SerializationFailure` handled exactly as the
     existing point/scan paths.

If a predicate happens to select an empty key set (only possible if `buckets`
exceeds `keys`; the seed guarantees every bucket `< min(buckets, keys)` is
non-empty), the transaction performs the index read and commits with no ops —
harmless.

### 4. Op generation

`gen_ops` gains a key-subset parameter. Point/scan modes pass the full key list
(unchanged). Predicate mode passes the bucket/range's key subset, so all reads
and appends target keys that were read up front — preserving read-your-writes
without any point `get` outside the predicate. Values remain globally unique
(shared `AtomicU64`), unchanged.

### 5. History mapping (EDN)

Like scan mode, the up-front index read is **invisible** to the emitted history
— it exists to register the `table_scan` and seed the local copy. The emitted
`:value` is the sequence of generated `[:r k [list]]` / `[:append k v]` ops over
the bucket's keys. The EDN format and `edn_event` are unchanged; elle-cli sees a
normal `list-append` transaction over a subset of keys.

### 6. Local working copy refactor

The transaction's local copy changes from `HashMap<u64, Vec<u64>>` to carry the
full row so `update` can preserve `bucket`:

```rust
let mut local: HashMap<u64, ElleRow> = ...; // seeded from range/index result
```

This is mechanical and applies to scan mode too. Point mode reads `bucket` from
the `get()` result on each append.

### 7. Tests (in `elle-history.rs` `#[cfg(test)]`)

- `predicate_txn_reads_its_own_appends` — a predicate transaction (both an
  equality and a range variant) serves later reads from the local copy including
  its own earlier appends within the bucket (mirrors the existing
  `scan_txn_reads_its_own_appends`).
- `predicate_txn_preserves_bucket_on_append` — after appending to a row, its
  `bucket` is unchanged, so `get_by_index` still returns it (index membership
  intact).
- `gen_ops_restricts_to_key_subset` — predicate op generation only touches keys
  in the provided subset; values stay unique.
- `generation_is_reproducible_for_a_fixed_seed` — two generator runs with the
  same seed + args produce the identical op sequence (determinism guard; does
  **not** assert backward-identity with the pre-change stream).

### 8. Harness + CI

Add a **third pass** to `make consistency/elle` after the point and scan passes:
generate SI and SSI histories with `--predicate-ratio $(ELLE_PREDICATE_RATIO)`
and `--buckets $(ELLE_BUCKETS)`, then run `scripts/elle_check.sh` on them.

- New Makefile vars: `ELLE_PREDICATE_RATIO ?= 0.5`, `ELLE_BUCKETS ?= 4`.
- The bounded PR job (`ELLE_ARGS="--threads 8 --keys 8 --txns-per-thread 800"`)
  picks the predicate pass up automatically — adds ~30–60s to the PR run. Keys=8
  / buckets=4 → ~2 keys per bucket; range predicates give larger groups. The
  whitelist gate (`{G2-item}` under SI, clean under SSI) is reused unchanged.
- The weekly `elle-deep` job runs it at canonical sizing.

`scripts/elle_check.sh` needs **no change** — it already checks a SI/SSI history
pair against the `{G2-item}` whitelist, which the predicate pass shares.

### 9. Docs

Add a "Predicate reads" section to `docs/tasks/task45_elle_consistency_harness.md`:
the new mode, the `table_scan`-degradation finding, why no new whitelist is
needed, and the new flags / Makefile pass. Update the task45 "Known limitations"
bullet that anticipated a predicate whitelist. This design-history spec is kept
under `docs/superpowers/specs/` per the repo's feature-doc convention.

## Risks & mitigations

- **Bucket corruption on update** (writing a row without its bucket) → silently
  shifts index membership and would break both the in-txn assertion and Elle
  semantics. Mitigated by: threading the full row through `local`, the
  `preserves_bucket_on_append` test, and the in-transaction membership
  assertion (which fails loudly rather than producing a wrong-but-plausible
  history).
- **Reproducibility regression** (same seed no longer → same history) → pinned
  by `generation_is_reproducible_for_a_fixed_seed`. Note the per-txn RNG draw
  order changes versus the pre-change binary (mode now rolled before `gen_ops`);
  backward-identity is intentionally not preserved, only same-seed determinism.
- **CI time creep** on the PR gate → bounded args; the pass is a fixed ~30–60s.
  If it becomes a problem it is trivially movable to the weekly job by dropping
  the third pass from the default `make consistency/elle` (documented).

## Verification plan

1. `cargo test -p ultima-autobench --bin elle-history` — new + existing unit
   tests pass.
2. `cargo clippy -p ultima-autobench --bin elle-history -- -D warnings`.
3. Local `ELLE_ARGS="--threads 8 --keys 8 --txns-per-thread 800"
   make consistency/elle` — all three passes (point, scan, predicate) green,
   SI shows `{G2-item}`, SSI clean.
4. `make consistency/elle-mutation` still passes (unchanged; sanity that the
   schema change didn't disturb the mutation build).
5. Open a PR; confirm the `elle` CI job (now including the predicate pass) is
   green end-to-end.
