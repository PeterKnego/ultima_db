# Elle Predicate-Read Workload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a predicate-read mode to the `elle-history` workload that selects row groups via a secondary index (`get_by_index` equality / `index_range` range), exercising UltimaDB's index read path end-to-end under MultiWriter SI/SSI.

**Architecture:** Each row gains a static `bucket` field (assigned round-robin at seed, never mutated) with a non-unique index. A predicate transaction reads a bucket (equality) or bucket range via the index — which registers the SSI coarse `table_scan` read exactly like the existing full scan — asserts the returned membership matches the statically-known set (an index-integrity check), then reads/appends within that group from a local working copy. Anomaly profile is unchanged from the scan pass (SSI clean, SI `{G2-item}`), so no whitelist change.

**Tech Stack:** Rust, `clap`, UltimaDB (`ultima-db` crate), the vendored elle-cli jar, GNU Make.

## Global Constraints

- **No new dependencies.** Use only what `autobench` already depends on.
- **`cargo clippy -p ultima-autobench --bin elle-history -- -D warnings` must pass.**
- **The touched file must pass `cargo fmt --check`** (the repo has pre-existing whole-tree rustfmt-version drift unrelated to this change; only `autobench/src/bin/elle-history.rs` must be clean).
- **The emitted EDN `list-append` format is unchanged** — `edn_event` is not modified; elle-cli and its jar are untouched.
- **The anomaly whitelist is unchanged** — `scripts/elle_check.sh` (`SI_ALLOWED="G2-item"`) is not modified; the predicate pass reuses it.
- **Determinism:** same `--seed` + same args ⇒ same history. The PRNG (`SplitMix64`, per-thread stream `seed + thread index`) is not modified. Backward byte-identity with the pre-change binary is explicitly NOT required (mode is now rolled before op generation).
- **Default `--predicate-ratio 0.0`** — predicate mode is off unless requested.

**File under change (all tasks):** `autobench/src/bin/elle-history.rs` (Tasks 1–3), `Makefile` (Task 4), `docs/tasks/task45_elle_consistency_harness.md` (Task 5).

Reference (current, pre-change) structure of `elle-history.rs`:
- imports L11; `struct ElleRow` L48–51; `enum Op` L53–57; `fn gen_ops` L94–111; `fn edn_event` L113–145; `enum TxnFail` L147–154; `fn run_txn` L161–219; `struct Stats` L221–228; `struct Shared` L230–247; `fn worker` L249–293; `fn main` L295–371; tests L373–511.

---

### Task 1: Row schema, seeded buckets, index, and bucket-preserving reads/writes

Add the `bucket` field + non-unique index, extract `build_store`/`seed` helpers, and refactor the transaction's local working copy to carry the full row so `update` preserves `bucket`. `run_txn` still takes `scan: bool` (Mode enum comes in Task 3).

**Files:**
- Modify: `autobench/src/bin/elle-history.rs` (imports, `ElleRow`, `run_txn`, `main` seed/store-build, tests)

**Interfaces:**
- Consumes: `ultima_db::{IndexKind, IsolationLevel, Store, StoreConfig, WriterMode}`.
- Produces:
  - `struct ElleRow { bucket: u64, list: Vec<u64> }`
  - `fn build_store(isolation: IsolationLevel) -> Store`
  - `fn seed(store: &Store, keys: usize, buckets: usize) -> Vec<u64>` — inserts `keys` rows with `bucket = i % buckets`, defines the non-unique `"bucket"` index, returns row ids in seed order (`ids[i]` has bucket `i % buckets`).
  - `fn run_txn(store: &Store, ops: &[Op], scan: bool) -> Result<Vec<Op>, TxnFail>` (signature unchanged this task; internals refactored).

- [ ] **Step 1: Add `IndexKind` to the import and the `bucket` field.**

Change L11 import to include `IndexKind`:

```rust
use ultima_db::{CommitWaiter, Error, IndexKind, IsolationLevel, Store, StoreConfig, WriterMode};
```

Change `ElleRow` (L48–51) to:

```rust
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ElleRow {
    /// Static bucket, assigned once at seed (`i % buckets`), never mutated.
    /// Indexed for predicate reads.
    bucket: u64,
    list: Vec<u64>,
}
```

- [ ] **Step 2: Write the failing test for `seed` (index membership).**

Add to the `tests` module:

```rust
#[test]
fn seed_assigns_round_robin_buckets_and_indexes_them() {
    use std::collections::BTreeSet;
    let store = build_store(IsolationLevel::Serializable);
    let ids = seed(&store, 6, 3); // buckets: 0,1,2,0,1,2

    let mut tx = store.begin_write(None).unwrap();
    let t = tx.open_table::<ElleRow>("elle").unwrap();
    let bucket0: BTreeSet<u64> =
        t.get_by_index("bucket", &0u64).unwrap().iter().map(|(id, _)| *id).collect();
    assert_eq!(bucket0, BTreeSet::from([ids[0], ids[3]]));
    let bucket2: BTreeSet<u64> =
        t.get_by_index("bucket", &2u64).unwrap().iter().map(|(id, _)| *id).collect();
    assert_eq!(bucket2, BTreeSet::from([ids[2], ids[5]]));
}
```

- [ ] **Step 3: Run the test to verify it fails.**

Run: `cargo test -p ultima-autobench --bin elle-history seed_assigns_round_robin -- --nocapture`
Expected: FAIL to compile (`build_store`/`seed` not defined).

- [ ] **Step 4: Add `build_store` and `seed`, and rewrite `main`'s store build + seed to use them.**

Add these free functions (place just above `fn worker`):

```rust
fn build_store(isolation: IsolationLevel) -> Store {
    Store::new(
        StoreConfig::builder()
            .writer_mode(WriterMode::MultiWriter)
            .isolation_level(isolation)
            .build(),
    )
    .expect("store construction")
}

/// Seed `keys` rows with round-robin buckets (`ids[i]` has bucket `i % buckets`)
/// and define the non-unique `"bucket"` index. Returns row ids in seed order.
fn seed(store: &Store, keys: usize, buckets: usize) -> Vec<u64> {
    let mut tx = store.begin_write(None).expect("setup begin_write");
    let ids = {
        let mut t = tx.open_table::<ElleRow>("elle").expect("setup open_table");
        let rows: Vec<ElleRow> = (0..keys)
            .map(|i| ElleRow { bucket: (i % buckets) as u64, list: Vec::new() })
            .collect();
        let ids = t.insert_batch(rows).expect("setup insert_batch");
        t.define_index("bucket", IndexKind::NonUnique, |r: &ElleRow| r.bucket)
            .expect("setup define_index");
        ids
    };
    tx.commit().expect("setup commit");
    ids
}
```

In `main`, replace the store construction (L306–312) with:

```rust
    let store = build_store(isolation);
```

and replace the seed block (L316–325, the `let keys = { ... };`) with:

```rust
    let keys = seed(&store, args.keys, args.buckets);
```

> Note: `args.buckets` is added in Task 3. For this task, temporarily hardcode the seed call in `main` as `seed(&store, args.keys, 4)` so the file compiles; Task 3 swaps `4` for `args.buckets`. (The tests call `seed` directly with explicit buckets, so they are unaffected.)

- [ ] **Step 5: Run the test to verify it passes.**

Run: `cargo test -p ultima-autobench --bin elle-history seed_assigns_round_robin`
Expected: PASS.

- [ ] **Step 6: Refactor `run_txn`'s local copy to carry the full row (preserve `bucket`).**

Replace the body of `run_txn` (L161–219) with (signature unchanged, still `scan: bool`):

```rust
fn run_txn(store: &Store, ops: &[Op], scan: bool) -> Result<Vec<Op>, TxnFail> {
    let mut tx = store.begin_write(None).map_err(TxnFail::Other)?;
    let mut completed = Vec::with_capacity(ops.len());
    {
        let mut t = tx.open_table::<ElleRow>("elle").map_err(TxnFail::Other)?;
        // Scan mode reads the whole table up front (registering the SSI coarse
        // `table_scan` read) into a local working copy so later reads observe
        // earlier appends in this txn. The copy carries the full row so appends
        // preserve the (static) `bucket`.
        let mut local: std::collections::HashMap<u64, ElleRow> = if scan {
            t.range(..).map(|(id, row)| (id, row.clone())).collect()
        } else {
            std::collections::HashMap::new()
        };
        for op in ops {
            match op {
                Op::Read { key, .. } => {
                    let list = if scan {
                        local.get(key).map(|r| r.list.clone()).unwrap_or_default()
                    } else {
                        t.get(*key).map(|r| r.list.clone()).unwrap_or_default()
                    };
                    completed.push(Op::Read { key: *key, result: Some(list) });
                }
                Op::Append { key, value } => {
                    let next = if scan {
                        let row = local.get_mut(key).expect("scan seeds every key");
                        row.list.push(*value);
                        row.clone()
                    } else {
                        // get() both fetches the row and registers the SSI point read.
                        let mut row = t.get(*key).cloned().expect("seeded key exists");
                        row.list.push(*value);
                        row
                    };
                    match t.update(*key, next) {
                        Ok(()) => completed.push(op.clone()),
                        Err(Error::WriteConflict { wait_for, .. }) => {
                            return Err(TxnFail::Conflict(wait_for));
                        }
                        Err(e) => return Err(TxnFail::Other(e)),
                    }
                }
            }
        }
    }
    match tx.commit() {
        Ok(_) => Ok(completed),
        Err(Error::WriteConflict { wait_for, .. }) => Err(TxnFail::Conflict(wait_for)),
        Err(Error::SerializationFailure { .. }) => Err(TxnFail::Serialization),
        Err(e) => Err(TxnFail::Other(e)),
    }
}
```

- [ ] **Step 7: Update the existing `scan_txn_reads_its_own_appends` test and add a bucket-preservation test.**

In `scan_txn_reads_its_own_appends`, change the two `ElleRow` constructions and the seed insert to include `bucket`. Replace the seed block inside that test:

```rust
        let id = {
            let mut t = tx.open_table::<ElleRow>("elle").unwrap();
            t.insert(ElleRow { bucket: 0, list: vec![] }).unwrap()
        };
```

The rest of that test is unchanged (it calls `run_txn(&store, &ops, true)`).

Add a new test:

```rust
#[test]
fn point_append_preserves_bucket_and_index_membership() {
    let store = build_store(IsolationLevel::SnapshotIsolation);
    let ids = seed(&store, 3, 3); // ids[1] has bucket 1

    let ops = vec![Op::Append { key: ids[1], value: 5 }];
    run_txn(&store, &ops, false).expect("uncontended append commits");

    let mut tx = store.begin_write(None).unwrap();
    let t = tx.open_table::<ElleRow>("elle").unwrap();
    assert_eq!(t.get(ids[1]).unwrap().bucket, 1, "append must not change bucket");
    let bucket1: Vec<u64> =
        t.get_by_index("bucket", &1u64).unwrap().iter().map(|(id, _)| *id).collect();
    assert!(bucket1.contains(&ids[1]), "row must still be in its bucket after append");
}
```

- [ ] **Step 8: Run all `elle-history` tests.**

Run: `cargo test -p ultima-autobench --bin elle-history`
Expected: PASS (all existing + 2 new tests).

- [ ] **Step 9: Commit.**

```bash
git add autobench/src/bin/elle-history.rs
git commit -m "feat(elle): add static bucket field + index; carry full row in txn-local copy"
```

---

### Task 2: Pure helpers — bucket membership, predicate selection, empty-key guard

Add the pure, independently-testable building blocks for predicate mode: the `Predicate` descriptor, static membership computation, deterministic predicate selection, and a guard so `gen_ops` returns no ops for an empty key set.

**Files:**
- Modify: `autobench/src/bin/elle-history.rs` (`gen_ops`, new `Predicate`/helpers, tests)

**Interfaces:**
- Produces:
  - `enum Predicate { Equality(u64), Range(u64, u64) }` (derives `Clone, Copy, Debug, PartialEq, Eq`)
  - `fn bucket_members(ids: &[u64], buckets: usize) -> Vec<Vec<u64>>`
  - `fn select_predicate(rng: &mut SplitMix64, buckets: usize, members: &[Vec<u64>]) -> (Predicate, Vec<u64>)`
  - `gen_ops` returns `Vec::new()` when `keys.is_empty()`.

- [ ] **Step 1: Write failing tests for the helpers.**

Add to the `tests` module:

```rust
#[test]
fn bucket_members_round_robin() {
    let ids = vec![10, 20, 30, 40, 50];
    let m = bucket_members(&ids, 2);
    assert_eq!(m, vec![vec![10, 30, 50], vec![20, 40]]);
}

#[test]
fn select_predicate_equality_and_range_within_bounds() {
    let ids = vec![10, 20, 30, 40];
    let members = bucket_members(&ids, 2); // [[10,30],[20,40]]
    let mut rng = SplitMix64(1);
    for _ in 0..200 {
        let (p, keys) = select_predicate(&mut rng, 2, &members);
        match p {
            Predicate::Equality(b) => {
                assert!(b < 2);
                assert_eq!(keys, members[b as usize]);
            }
            Predicate::Range(lo, hi) => {
                assert!(lo <= hi && hi < 2);
                let mut expected = Vec::new();
                for b in lo..=hi {
                    expected.extend_from_slice(&members[b as usize]);
                }
                assert_eq!(keys, expected);
            }
        }
    }
}

#[test]
fn gen_ops_empty_keys_returns_no_ops() {
    let counter = AtomicU64::new(0);
    let mut rng = SplitMix64(7);
    assert!(gen_ops(&mut rng, &[], 4, 0.4, &counter).is_empty());
}
```

- [ ] **Step 2: Run the tests to verify they fail.**

Run: `cargo test -p ultima-autobench --bin elle-history -- bucket_members_round_robin select_predicate gen_ops_empty`
Expected: FAIL to compile (`bucket_members`, `select_predicate`, `Predicate` not defined).

- [ ] **Step 3: Add the `Predicate` enum and helper functions.**

Add above `fn gen_ops`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Predicate {
    /// Equality lookup on one bucket (`get_by_index`).
    Equality(u64),
    /// Inclusive bucket range (`index_range`), `lo <= hi`.
    Range(u64, u64),
}

/// Static bucket membership: `members[b]` = ids at seed positions `i` where
/// `i % buckets == b`, in seed order. Mirrors `seed`'s assignment exactly.
fn bucket_members(ids: &[u64], buckets: usize) -> Vec<Vec<u64>> {
    let mut m = vec![Vec::new(); buckets];
    for (i, &id) in ids.iter().enumerate() {
        m[i % buckets].push(id);
    }
    m
}

/// Deterministically choose an equality or range predicate (50/50) and resolve
/// its expected key set from static membership. Consumes RNG draws in a fixed
/// order so a given RNG state always yields the same predicate.
fn select_predicate(
    rng: &mut SplitMix64,
    buckets: usize,
    members: &[Vec<u64>],
) -> (Predicate, Vec<u64>) {
    if rng.chance(0.5) {
        let b = rng.below(buckets as u64);
        (Predicate::Equality(b), members[b as usize].clone())
    } else {
        let lo = rng.below(buckets as u64);
        let hi = lo + rng.below(buckets as u64 - lo); // lo..=hi, within [0, buckets)
        let mut keys = Vec::new();
        for b in lo..=hi {
            keys.extend_from_slice(&members[b as usize]);
        }
        (Predicate::Range(lo, hi), keys)
    }
}
```

- [ ] **Step 4: Add the empty-key guard to `gen_ops`.**

At the top of `gen_ops`'s body (before the `(0..ops_per_txn)` map), add:

```rust
    if keys.is_empty() {
        return Vec::new();
    }
```

- [ ] **Step 5: Run the tests to verify they pass.**

Run: `cargo test -p ultima-autobench --bin elle-history -- bucket_members_round_robin select_predicate gen_ops_empty`
Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add autobench/src/bin/elle-history.rs
git commit -m "feat(elle): predicate-selection helpers + empty-key guard in gen_ops"
```

---

### Task 3: Predicate mode — Mode enum, index-read execution, CLI flags, worker wiring

Replace `run_txn`'s `scan: bool` with a `Mode` enum, add the predicate branch (index read + membership assertion + local seeding), add `--buckets`/`--predicate-ratio`, thread `members` through `Shared`, and roll the mode in `worker` before op generation.

**Files:**
- Modify: `autobench/src/bin/elle-history.rs` (`TxnFail`, `Args`, `run_txn`, `Stats`, `Shared`, `worker`, `main`, tests)

**Interfaces:**
- Consumes: `Predicate`, `bucket_members`, `select_predicate`, `build_store`, `seed` (Tasks 1–2); `ultima_db`'s `TableWriter::get_by_index`/`index_range` (`Result<Vec<(u64, &R)>>`).
- Produces:
  - `enum Mode { Point, Scan, Predicate { query: Predicate, expected: Vec<u64> } }`
  - `TxnFail::IndexIntegrity(String)` variant
  - `fn run_txn(store: &Store, ops: &[Op], mode: &Mode) -> Result<Vec<Op>, TxnFail>`
  - `Args.buckets: usize` (default 4), `Args.predicate_ratio: f64` (default 0.0)
  - `Shared.members: Vec<Vec<u64>>`, `Stats.predicate_txns: AtomicU64`

- [ ] **Step 1: Write the failing predicate tests.**

Add to the `tests` module:

```rust
#[test]
fn predicate_txn_reads_its_own_appends_equality() {
    let store = build_store(IsolationLevel::Serializable);
    let ids = seed(&store, 4, 2); // bucket 0 = {ids[0], ids[2]}
    let members = bucket_members(&ids, 2);

    // Equality predicate on bucket 0, ops within that bucket.
    let mode = Mode::Predicate { query: Predicate::Equality(0), expected: members[0].clone() };
    let ops = vec![
        Op::Append { key: ids[0], value: 100 },
        Op::Read { key: ids[0], result: None },
        Op::Append { key: ids[2], value: 200 },
        Op::Read { key: ids[2], result: None },
    ];
    let completed = run_txn(&store, &ops, &mode).expect("uncontended predicate txn commits");
    match &completed[1] {
        Op::Read { result: Some(l), .. } => assert_eq!(l, &vec![100]),
        other => panic!("expected read of [100], got {other:?}"),
    }
    match &completed[3] {
        Op::Read { result: Some(l), .. } => assert_eq!(l, &vec![200]),
        other => panic!("expected read of [200], got {other:?}"),
    }
}

#[test]
fn predicate_txn_reads_its_own_appends_range() {
    let store = build_store(IsolationLevel::Serializable);
    let ids = seed(&store, 4, 2);
    let members = bucket_members(&ids, 2);
    let mut expected = members[0].clone();
    expected.extend_from_slice(&members[1]); // range 0..=1 = all keys

    let mode = Mode::Predicate { query: Predicate::Range(0, 1), expected };
    let ops = vec![
        Op::Append { key: ids[1], value: 7 },
        Op::Read { key: ids[1], result: None },
    ];
    let completed = run_txn(&store, &ops, &mode).expect("range predicate txn commits");
    match &completed[1] {
        Op::Read { result: Some(l), .. } => assert_eq!(l, &vec![7]),
        other => panic!("expected read of [7], got {other:?}"),
    }
}

#[test]
fn predicate_txn_preserves_bucket_after_append() {
    let store = build_store(IsolationLevel::SnapshotIsolation);
    let ids = seed(&store, 4, 2);
    let members = bucket_members(&ids, 2);

    let mode = Mode::Predicate { query: Predicate::Equality(1), expected: members[1].clone() };
    let ops = vec![Op::Append { key: ids[1], value: 9 }];
    run_txn(&store, &ops, &mode).expect("commit");

    // The row is still in bucket 1, so a fresh equality predicate still finds it.
    let mut tx = store.begin_write(None).unwrap();
    let t = tx.open_table::<ElleRow>("elle").unwrap();
    let b1: Vec<u64> =
        t.get_by_index("bucket", &1u64).unwrap().iter().map(|(id, _)| *id).collect();
    assert!(b1.contains(&ids[1]));
}

#[test]
fn generation_is_reproducible_for_a_fixed_seed() {
    let ids = vec![10, 20, 30, 40];
    let members = bucket_members(&ids, 2);

    let run = || {
        let counter = AtomicU64::new(1);
        let mut rng = SplitMix64(99);
        let (p, keys) = select_predicate(&mut rng, 2, &members);
        let ops = gen_ops(&mut rng, &keys, 4, 0.4, &counter);
        (p, keys, format!("{ops:?}"))
    };
    assert_eq!(run(), run(), "same seed must produce identical predicate + ops");
}
```

- [ ] **Step 2: Run the tests to verify they fail.**

Run: `cargo test -p ultima-autobench --bin elle-history -- predicate_txn generation_is_reproducible`
Expected: FAIL to compile (`Mode` not defined; `run_txn` takes `bool`).

- [ ] **Step 3: Add the `Mode` enum and `TxnFail::IndexIntegrity`.**

Add above `fn run_txn`:

```rust
enum Mode {
    Point,
    Scan,
    /// Predicate read over the `bucket` index. `expected` is the statically
    /// known membership the index read must return (integrity check).
    Predicate { query: Predicate, expected: Vec<u64> },
}
```

Add a variant to `TxnFail` (L147–154):

```rust
enum TxnFail {
    /// WriteConflict at the write site or at commit: definitely not committed.
    Conflict(Option<CommitWaiter>),
    /// SSI read-set validation failure at commit: definitely not committed.
    Serialization,
    /// A predicate index read returned a membership other than the static
    /// expectation — an index-maintenance bug, surfaced loudly.
    IndexIntegrity(String),
    /// Anything else: outcome treated as indeterminate.
    Other(Error),
}
```

- [ ] **Step 4: Rewrite `run_txn` to take `&Mode` with the predicate branch.**

Replace the whole `run_txn` function with:

```rust
/// Run one list-append transaction. Returns completed ops (reads filled in) on
/// commit. In `Scan`/`Predicate` mode the transaction reads a group of rows up
/// front (registering the SSI coarse `table_scan` read) and serves reads/appends
/// from a local working copy, preserving read-your-writes; `Point` mode uses per-
/// op `get`. Predicate mode additionally asserts the index returned exactly the
/// statically-known bucket membership.
fn run_txn(store: &Store, ops: &[Op], mode: &Mode) -> Result<Vec<Op>, TxnFail> {
    let mut tx = store.begin_write(None).map_err(TxnFail::Other)?;
    let mut completed = Vec::with_capacity(ops.len());
    {
        let mut t = tx.open_table::<ElleRow>("elle").map_err(TxnFail::Other)?;
        let mut local: std::collections::HashMap<u64, ElleRow> = match mode {
            Mode::Scan => t.range(..).map(|(id, row)| (id, row.clone())).collect(),
            Mode::Predicate { query, expected } => {
                // The index read registers the SSI coarse `table_scan` read.
                let result = match query {
                    Predicate::Equality(b) => {
                        t.get_by_index("bucket", b).map_err(TxnFail::Other)?
                    }
                    Predicate::Range(lo, hi) => {
                        t.index_range("bucket", *lo..=*hi).map_err(TxnFail::Other)?
                    }
                };
                let mut got: Vec<u64> = result.iter().map(|(id, _)| *id).collect();
                let seeded: std::collections::HashMap<u64, ElleRow> =
                    result.iter().map(|(id, row)| (*id, (*row).clone())).collect();
                got.sort_unstable();
                let mut exp = expected.clone();
                exp.sort_unstable();
                if got != exp {
                    return Err(TxnFail::IndexIntegrity(format!(
                        "predicate {query:?} returned ids {got:?}, expected {exp:?}"
                    )));
                }
                seeded
            }
            Mode::Point => std::collections::HashMap::new(),
        };
        let from_local = !matches!(mode, Mode::Point);
        for op in ops {
            match op {
                Op::Read { key, .. } => {
                    let list = if from_local {
                        local.get(key).map(|r| r.list.clone()).unwrap_or_default()
                    } else {
                        t.get(*key).map(|r| r.list.clone()).unwrap_or_default()
                    };
                    completed.push(Op::Read { key: *key, result: Some(list) });
                }
                Op::Append { key, value } => {
                    let next = if from_local {
                        let row = local.get_mut(key).expect("local seeds every op key");
                        row.list.push(*value);
                        row.clone()
                    } else {
                        let mut row = t.get(*key).cloned().expect("seeded key exists");
                        row.list.push(*value);
                        row
                    };
                    match t.update(*key, next) {
                        Ok(()) => completed.push(op.clone()),
                        Err(Error::WriteConflict { wait_for, .. }) => {
                            return Err(TxnFail::Conflict(wait_for));
                        }
                        Err(e) => return Err(TxnFail::Other(e)),
                    }
                }
            }
        }
    }
    match tx.commit() {
        Ok(_) => Ok(completed),
        Err(Error::WriteConflict { wait_for, .. }) => Err(TxnFail::Conflict(wait_for)),
        Err(Error::SerializationFailure { .. }) => Err(TxnFail::Serialization),
        Err(e) => Err(TxnFail::Other(e)),
    }
}
```

- [ ] **Step 5: Run the predicate tests to verify they pass.**

Run: `cargo test -p ultima-autobench --bin elle-history -- predicate_txn generation_is_reproducible`
Expected: PASS.

- [ ] **Step 6: Update the two existing tests that call `run_txn`.**

`scan_txn_reads_its_own_appends` (calls `run_txn(&store, &ops, true)`) → `run_txn(&store, &ops, &Mode::Scan)`.
`point_append_preserves_bucket_and_index_membership` (Task 1, calls `run_txn(&store, &ops, false)`) → `run_txn(&store, &ops, &Mode::Point)`.

- [ ] **Step 7: Add the CLI flags, `Stats`/`Shared` fields, and validation.**

In `Args` (after the `scan_ratio` field, ~L39), add:

```rust
    /// Number of index buckets for predicate reads (rows are assigned
    /// `id_index % buckets` at seed).
    #[arg(long, default_value_t = 4)]
    buckets: usize,
    /// Probability a transaction reads a bucket group via the `bucket` index
    /// (`get_by_index`/`index_range`) instead of point/scan. Exercises the
    /// index read path; degrades to the same SSI `table_scan` tracking as a
    /// full scan. Default 0.0 preserves the point/scan workload.
    #[arg(long, default_value_t = 0.0)]
    predicate_ratio: f64,
```

In `Stats` (L221–228), add field `predicate_txns: AtomicU64,`.
In `Shared` (L230–237), add field `members: Vec<Vec<u64>>,`.

In `main`, change the temporary `seed(&store, args.keys, 4)` (Task 1 Step 4) to `seed(&store, args.keys, args.buckets)`, and just after computing `keys` add:

```rust
    if args.buckets == 0 {
        eprintln!("elle-history: --buckets must be >= 1");
        std::process::exit(2);
    }
    let members = bucket_members(&keys, args.buckets);
```

(Place the `buckets == 0` check before `seed` is called; move it above the `let keys = ...` line and call `bucket_members` after.)

In the `Shared { ... }` initializer in `main`, add `members,`.

- [ ] **Step 8: Rewrite `worker` to roll the mode before op generation.**

Replace `worker` (L249–293) with:

```rust
fn worker(shared: &Shared, args: &Args, process: usize) {
    let mut rng = SplitMix64(args.seed.wrapping_add(process as u64));
    for _ in 0..args.txns_per_thread {
        // Mode is rolled before op generation because predicate mode restricts
        // ops to the selected bucket's keys. predicate -> scan -> point.
        let mode = if rng.chance(args.predicate_ratio) {
            let (query, expected) = select_predicate(&mut rng, args.buckets, &shared.members);
            shared.stats.predicate_txns.fetch_add(1, Ordering::Relaxed);
            Mode::Predicate { query, expected }
        } else if rng.chance(args.scan_ratio) {
            shared.stats.scan_txns.fetch_add(1, Ordering::Relaxed);
            Mode::Scan
        } else {
            Mode::Point
        };
        let key_subset: &[u64] = match &mode {
            Mode::Predicate { expected, .. } => expected,
            _ => &shared.keys,
        };
        let ops = gen_ops(
            &mut rng,
            key_subset,
            args.ops_per_txn,
            args.read_ratio,
            &shared.values,
        );
        shared.push(EventType::Invoke, process, ops.clone());
        match run_txn(&shared.store, &ops, &mode) {
            Ok(completed) => {
                shared.stats.ok.fetch_add(1, Ordering::Relaxed);
                shared.push(EventType::Ok, process, completed);
            }
            Err(TxnFail::Conflict(waiter)) => {
                shared.stats.write_conflict.fetch_add(1, Ordering::Relaxed);
                shared.push(EventType::Fail, process, ops);
                if let Some(w) = waiter {
                    w.wait();
                }
            }
            Err(TxnFail::Serialization) => {
                shared.stats.serialization_failure.fetch_add(1, Ordering::Relaxed);
                shared.push(EventType::Fail, process, ops);
            }
            Err(TxnFail::IndexIntegrity(msg)) => {
                eprintln!("elle-history: process {process}: INDEX INTEGRITY VIOLATION: {msg}");
                shared.stats.info.fetch_add(1, Ordering::Relaxed);
                shared.push(EventType::Info, process, ops);
                return;
            }
            Err(TxnFail::Other(e)) => {
                eprintln!("elle-history: process {process}: unexpected error, retiring: {e}");
                shared.stats.info.fetch_add(1, Ordering::Relaxed);
                shared.push(EventType::Info, process, ops);
                return;
            }
        }
    }
}
```

- [ ] **Step 9: Add `predicate_txns` to the summary line.**

In `main`'s final `eprintln!` (L356–366), append `predicate_txns={}` to the format string and `s.predicate_txns.load(Ordering::Relaxed),` to the args (place next to `scan_txns`):

```rust
    eprintln!(
        "elle-history: isolation={} scan_ratio={} predicate_ratio={} events={} ok={} write_conflict={} serialization_failure={} info={} scan_txns={} predicate_txns={}",
        args.isolation,
        args.scan_ratio,
        args.predicate_ratio,
        history.len(),
        s.ok.load(Ordering::Relaxed),
        s.write_conflict.load(Ordering::Relaxed),
        s.serialization_failure.load(Ordering::Relaxed),
        s.info.load(Ordering::Relaxed),
        s.scan_txns.load(Ordering::Relaxed),
        s.predicate_txns.load(Ordering::Relaxed),
    );
```

- [ ] **Step 10: Full build, clippy, fmt, and test.**

Run:
```bash
cargo clippy -p ultima-autobench --bin elle-history -- -D warnings
cargo fmt -p ultima-autobench
cargo test -p ultima-autobench --bin elle-history
```
Expected: clippy clean; fmt makes no changes to unrelated files (verify `git diff --stat` touches only `elle-history.rs`); all tests PASS.

- [ ] **Step 11: Smoke-run predicate mode end-to-end.**

Run:
```bash
cargo run -q --release -p ultima-autobench --bin elle-history -- \
  --isolation serializable --predicate-ratio 1.0 --buckets 4 \
  --threads 4 --keys 16 --txns-per-thread 300 --out /tmp/pred-smoke.edn
```
Expected: prints a summary line with `predicate_txns=` ~1200 and `info=0`; `/tmp/pred-smoke.edn` exists and is non-empty. (An `info>0` / `INDEX INTEGRITY VIOLATION` line is a failure.)

- [ ] **Step 12: Commit.**

```bash
git add autobench/src/bin/elle-history.rs
git commit -m "feat(elle): predicate-read mode via bucket index (get_by_index/index_range)"
```

---

### Task 4: Add the predicate pass to `make consistency/elle`

**Files:**
- Modify: `Makefile` (the `consistency/elle` target + new vars)

**Interfaces:**
- Consumes: `elle-history`'s `--predicate-ratio`/`--buckets` (Task 3); `scripts/elle_check.sh` (unchanged).

- [ ] **Step 1: Add the vars and the third pass.**

In the Makefile, next to `ELLE_SCAN_RATIO ?= 0.5`, add:

```make
ELLE_PREDICATE_RATIO ?= 0.5
ELLE_BUCKETS ?= 4
```

At the end of the `consistency/elle:` recipe (after the scan pass's `scripts/elle_check.sh ...` line), append:

```make
	cargo run --release -p ultima-autobench --bin elle-history -- \
		--isolation si --predicate-ratio $(ELLE_PREDICATE_RATIO) --buckets $(ELLE_BUCKETS) $(ELLE_ARGS) --out $(ELLE_DIR)/pred-si/history.edn
	cargo run --release -p ultima-autobench --bin elle-history -- \
		--isolation serializable --predicate-ratio $(ELLE_PREDICATE_RATIO) --buckets $(ELLE_BUCKETS) $(ELLE_ARGS) --out $(ELLE_DIR)/pred-ser/history.edn
	scripts/elle_check.sh $(ELLE_DIR)/pred-si/history.edn $(ELLE_DIR)/pred-ser/history.edn
```

Update the comment block above `consistency/elle:` to say "Three passes: point reads, a scan-heavy pass, then a predicate (index) pass."

- [ ] **Step 2: Run the full check with bounded args (the CI PR-gate config).**

Run:
```bash
ELLE_ARGS="--threads 8 --keys 8 --txns-per-thread 800" ELLE_DIR=/tmp/ultima-elle-pred make consistency/elle
```
Expected: all three passes print `elle consistency check passed`; the predicate pass's SI history shows `OK: SI anomalies ⊆ {G2-item}` (or a `WARN` if no write skew observed — acceptable, not a failure), SSI clean.

- [ ] **Step 3: Commit.**

```bash
git add Makefile
git commit -m "ci(elle): add predicate (index) pass to make consistency/elle"
```

---

### Task 5: Document the predicate-read mode in task45

**Files:**
- Modify: `docs/tasks/task45_elle_consistency_harness.md`

- [ ] **Step 1: Add a "Predicate reads" subsection.**

Add a new section (after the "How the workload maps onto UltimaDB" section) with this content:

```markdown
## Predicate reads (index pass)

A third read shape exercises UltimaDB's secondary-index read path. Each row
carries a static `bucket` field (assigned `id_index % --buckets` at seed, never
mutated) with a non-unique index. A predicate transaction (probability
`--predicate-ratio`) selects a key group via the index — `get_by_index(bucket)`
for equality or `index_range(lo..=hi)` for a range — then reads/appends within
that group, serving read-your-writes from a local copy exactly like the scan
pass. The index read is invisible to the emitted history (like the scan), so the
EDN stays valid `list-append`.

Two properties are checked:

1. **SSI conflict tracking.** Every index read registers the coarse `table_scan`
   read (`src/store.rs`), so a predicate reader is serialization-failed by any
   concurrent write to the table — phantoms are prevented. The predicate pass is
   therefore clean under SSI, exactly like the scan pass.
2. **Index integrity under concurrency.** Each predicate transaction asserts the
   index returned exactly the statically-known bucket membership; a mismatch
   (an index-maintenance bug under concurrent `update`s) retires the process and
   fails the run.

Because index reads degrade to `table_scan`, a predicate read has the **same
conflict profile as a full scan**: SSI clean, SI shows `{G2-item}` write skew and
nothing worse. So the predicate pass reuses the existing whitelist — **no new
anomaly type is needed.** (This resolves the task45/47 note that a predicate
workload "could need its own whitelist": for the index-read shape it does not.)

`make consistency/elle` runs a predicate pass after the point and scan passes,
tuned by `ELLE_PREDICATE_RATIO` (default 0.5) and `ELLE_BUCKETS` (default 4).
```

- [ ] **Step 2: Update the "Known limitations (v1)" section.**

Find the bullet anticipating a predicate whitelist (search for "predicate") and replace it with:

```markdown
- Predicate reads are covered by the index pass (see "Predicate reads" above),
  but only via UltimaDB's coarse `table_scan` degradation — the harness does not
  yet test *fine-grained* predicate read-set tracking (there is none to test) or
  predicate write skew via a domain invariant (a possible future SmallBank-style
  workload, deliberately out of scope here).
```

If no such bullet exists, add the above as a new bullet.

- [ ] **Step 3: Commit.**

```bash
git add docs/tasks/task45_elle_consistency_harness.md
git commit -m "docs(elle): document the predicate-read (index) pass in task45"
```

---

### Task 6: Final verification and PR

**Files:** none (verification only)

- [ ] **Step 1: Full workspace sanity on the touched crate.**

Run:
```bash
cargo clippy -p ultima-autobench --bin elle-history -- -D warnings
cargo test -p ultima-autobench --bin elle-history
git diff --stat
```
Expected: clippy clean; all tests pass; `git diff` (vs branch base) touches only `elle-history.rs`, `Makefile`, `task45_...md`, and the plan/spec docs.

- [ ] **Step 2: Confirm the touched source file is fmt-clean.**

Run: `cargo fmt --check 2>&1 | grep 'elle-history.rs' || echo "elle-history.rs clean"`
Expected: `elle-history.rs clean` (repo-wide fmt drift on other files is pre-existing and out of scope).

- [ ] **Step 3: Full bounded three-pass consistency run.**

Run:
```bash
ELLE_ARGS="--threads 8 --keys 8 --txns-per-thread 800" ELLE_DIR=/tmp/ultima-elle-final make consistency/elle
```
Expected: three `elle consistency check passed` lines (point, scan, predicate).

- [ ] **Step 4: Confirm the mutation suite still passes (schema change didn't disturb it).**

Run: `make consistency/elle-mutation`
Expected: `elle mutation-testing passed: all injected bugs caught`.

- [ ] **Step 5: Push and open a PR.**

```bash
git push -u origin feat/elle-predicate-reads
gh pr create --base main --head feat/elle-predicate-reads \
  --title "feat(elle): predicate-read (index) workload pass (extends task45)" \
  --body "See docs/superpowers/specs/2026-07-08-elle-predicate-reads-design.md and docs/tasks/task45 'Predicate reads'. Exercises get_by_index/index_range end-to-end under MultiWriter SI/SSI; SSI clean, SI {G2-item} (same whitelist); in-txn index-integrity assertion. CI 'elle' job now runs a third (predicate) pass."
```

- [ ] **Step 6: Watch the `elle` CI job to green.**

Run: `gh run watch $(gh run list --branch feat/elle-predicate-reads --limit 1 --json databaseId -q '.[0].databaseId') --exit-status`
Expected: the `elle` job passes (now including the predicate pass).

---

## Self-Review

**Spec coverage:**
- Row schema + static bucket + index → Task 1. ✓
- CLI `--buckets`/`--predicate-ratio` → Task 3 Step 7. ✓
- Equality + range predicates (`get_by_index`/`index_range`) → Task 3 Step 4. ✓
- Index read registers `table_scan` (verified via SSI-clean predicate pass) → Task 4 Step 2, Task 6 Step 3. ✓
- In-txn membership assertion (`TxnFail::IndexIntegrity`) → Task 3 Steps 3–4. ✓
- Ops restricted to predicate key group → Task 3 Step 8 (`key_subset`). ✓
- Index read invisible to history / EDN unchanged → `edn_event` never touched; `run_txn` emits only per-op reads/appends. ✓
- `local` refactor to carry full row → Task 1 Step 6. ✓
- Whitelist unchanged → `elle_check.sh` never modified (Task 4 reuses it). ✓
- Tests (RYW equality+range, bucket preservation, gen bounds, reproducibility) → Tasks 1–3. ✓
- Makefile third pass + PR/weekly coverage → Task 4. ✓
- Docs in task45 + limitations update → Task 5. ✓
- Same-seed determinism, no backward-identity → Task 3 `generation_is_reproducible_for_a_fixed_seed`. ✓

**Placeholder scan:** The only literal placeholder is the intentional temporary `seed(&store, args.keys, 4)` in Task 1 Step 4, explicitly swapped to `args.buckets` in Task 3 Step 7 (documented in-line). No TBD/TODO/"handle edge cases".

**Type consistency:** `Mode`/`Predicate`/`TxnFail::IndexIntegrity` names and the `run_txn(&Mode)` signature are consistent across Tasks 3's tests and impl; `bucket_members`/`select_predicate` signatures match their Task 2 definitions and Task 3 call sites; `get_by_index`/`index_range` return `Result<Vec<(u64, &R)>>` as used.
