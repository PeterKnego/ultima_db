# ultima_vector Input Validation (task40) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reject non-finite (NaN/±Inf) embeddings and queries at every `VectorCollection` boundary, and make raw `Distance` length mismatches panic in release instead of returning garbage.

**Architecture:** One private `ensure_finite` helper called next to the existing `DimMismatch` checks at the four entry points (insert, update_embedding, search, restore). Reject-at-boundary means the graph can never contain a non-finite value, so hot distance loops stay check-free. Separately, `debug_assert_eq!` length checks in the distance kernels are promoted to release `assert_eq!`.

**Tech Stack:** Rust, thiserror, criterion (bench smoke check only). Crate: `ultima-vector` (workspace member at `ultima_vector/`).

**Spec:** `docs/superpowers/specs/2026-07-07-vector-input-validation-design.md`

## Global Constraints

- Branch: `fix/vector-input-validation` (already created).
- `cargo clippy -- -D warnings` must pass (zero warnings) at every commit.
- Root `cargo test` does NOT cover member crates — always run `cargo test -p ultima-vector` explicitly.
- Error message format is exactly: `non-finite value ({value}) at index {index} in vector`.
- Validation order at every entry point: dim check first, then finiteness.
- No opt-out flags, no changes to zero-vector semantics or `normalize_*` helpers.

---

### Task 1: `Error::NonFinite` variant + `ensure_finite` helper

**Files:**
- Modify: `ultima_vector/src/error.rs`
- Create: `ultima_vector/src/validate.rs`
- Modify: `ultima_vector/src/lib.rs` (add `mod validate;`)

**Interfaces:**
- Produces: `Error::NonFinite { index: usize, value: f32 }` (public enum variant); `pub(crate) fn validate::ensure_finite(v: &[f32]) -> Result<()>` — used by Tasks 2–4.

- [ ] **Step 1: Write the failing unit tests** — create `ultima_vector/src/validate.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Input validation helpers.

use crate::error::{Error, Result};

/// Reject NaN and ±Inf. Every embedding/query is validated once at its
/// `VectorCollection` entry point, so the HNSW graph can never contain a
/// non-finite value and the hot distance loops stay check-free.
pub(crate) fn ensure_finite(v: &[f32]) -> Result<()> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_vector_passes() {
        assert!(ensure_finite(&[1.0, -2.5, 0.0, f32::MIN, f32::MAX]).is_ok());
        assert!(ensure_finite(&[]).is_ok());
    }

    #[test]
    fn nan_rejected_with_index() {
        let err = ensure_finite(&[1.0, f32::NAN, 3.0]).unwrap_err();
        match err {
            Error::NonFinite { index, value } => {
                assert_eq!(index, 1);
                assert!(value.is_nan());
            }
            other => panic!("expected NonFinite, got {other:?}"),
        }
    }

    #[test]
    fn pos_and_neg_infinity_rejected() {
        assert!(matches!(
            ensure_finite(&[f32::INFINITY]).unwrap_err(),
            Error::NonFinite { index: 0, .. }
        ));
        assert!(matches!(
            ensure_finite(&[0.0, 0.0, f32::NEG_INFINITY]).unwrap_err(),
            Error::NonFinite { index: 2, .. }
        ));
    }

    #[test]
    fn error_message_names_index_and_value() {
        let msg = ensure_finite(&[f32::INFINITY]).unwrap_err().to_string();
        assert_eq!(msg, "non-finite value (inf) at index 0 in vector");
    }
}
```

And register the module in `ultima_vector/src/lib.rs` after `pub mod row;`:

```rust
mod validate;
```

- [ ] **Step 2: Add the error variant** — in `ultima_vector/src/error.rs`, after the `DimMismatch` variant:

```rust
    #[error("non-finite value ({value}) at index {index} in vector")]
    NonFinite { index: usize, value: f32 },
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p ultima-vector validate::`
Expected: FAIL — `nan_rejected_with_index` etc. panic at `todo!()` (`finite_vector_passes` also fails; the compile succeeds because the variant exists).

- [ ] **Step 4: Implement `ensure_finite`** — replace the `todo!()` body:

```rust
    match v.iter().position(|x| !x.is_finite()) {
        Some(index) => Err(Error::NonFinite {
            index,
            value: v[index],
        }),
        None => Ok(()),
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ultima-vector validate::`
Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add ultima_vector/src/error.rs ultima_vector/src/validate.rs ultima_vector/src/lib.rs
git commit -m "feat(ultima_vector): Error::NonFinite + ensure_finite helper"
```

---

### Task 2: Reject non-finite embeddings on the write path

**Files:**
- Modify: `ultima_vector/src/hnsw/insert.rs` (two call sites: `insert` ~line 42, `update_embedding` ~line 107)
- Test: `ultima_vector/src/collection.rs` (tests module)

**Interfaces:**
- Consumes: `crate::validate::ensure_finite` (Task 1).
- Produces: `upsert`/`upsert_in`/`bulk_insert`/`update_embedding[_in]` now return `Err(Error::NonFinite { .. })` on NaN/±Inf input.

- [ ] **Step 1: Write the failing tests** — append inside `mod tests` in `ultima_vector/src/collection.rs`:

```rust
    #[test]
    fn upsert_rejects_non_finite_embedding() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let err = coll.upsert(vec![1.0, bad], 7u64).unwrap_err();
            assert!(
                matches!(err, crate::Error::NonFinite { index: 1, .. }),
                "expected NonFinite at index 1, got {err:?}"
            );
        }
        // Nothing was inserted.
        let res = coll.search(&[1.0, 0.0], 5, None, None).unwrap();
        assert!(res.is_empty());
    }

    #[test]
    fn bulk_insert_non_finite_aborts_whole_batch() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let items = vec![
            (vec![1.0, 0.0], 1u64),
            (vec![f32::NAN, 0.0], 2), // poison row
        ];
        let err = coll.bulk_insert(items).unwrap_err();
        assert!(matches!(err, crate::Error::NonFinite { index: 0, .. }));
        // Tx was dropped without commit, so the first row is also gone.
        let res = coll.search(&[1.0, 0.0], 3, None, None).unwrap();
        assert!(res.is_empty(), "failed bulk_insert must roll back");
    }

    #[test]
    fn update_embedding_rejects_non_finite_and_keeps_old() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        let id = coll.upsert(vec![1.0, 0.0], 42u64).unwrap();

        let err = coll
            .update_embedding(id, vec![f32::NEG_INFINITY, 0.0])
            .unwrap_err();
        assert!(matches!(err, crate::Error::NonFinite { index: 0, .. }));

        // Old embedding is intact and searchable at its old location.
        let res = coll.search(&[1.0, 0.0], 1, None, None).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].0, id);
        assert!((res[0].1 - 0.0).abs() < 1e-5);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ultima-vector collection::tests::upsert_rejects_non_finite -- --nocapture`
(and the other two by name)
Expected: FAIL — `unwrap_err()` panics because the insert currently succeeds.

- [ ] **Step 3: Add the checks** — in `ultima_vector/src/hnsw/insert.rs`, in `insert` directly after the existing dim check (the `if embedding.len() != params.dim { ... }` block ending ~line 47):

```rust
    crate::validate::ensure_finite(&embedding)?;
```

Add the identical line in `update_embedding`, directly after its dim check (block ending ~line 112).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ultima-vector collection::`
Expected: all collection tests pass (existing + 3 new).

- [ ] **Step 5: Commit**

```bash
git add ultima_vector/src/hnsw/insert.rs ultima_vector/src/collection.rs
git commit -m "feat(ultima_vector): reject non-finite embeddings on insert/update"
```

---

### Task 3: Reject non-finite query vectors on the search path

**Files:**
- Modify: `ultima_vector/src/collection.rs` (`search_in`, dim check at ~line 259)
- Test: `ultima_vector/src/collection.rs` (tests module)

**Interfaces:**
- Consumes: `crate::validate::ensure_finite` (Task 1).
- Produces: `search`/`search_in` return `Err(Error::NonFinite { .. })` on a NaN/±Inf query.

- [ ] **Step 1: Write the failing test** — append inside `mod tests` in `ultima_vector/src/collection.rs`:

```rust
    #[test]
    fn search_rejects_non_finite_query() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let coll: VectorCollection<u64, Cosine> =
            VectorCollection::open(store, "vec", HnswParams::for_dim(2), Cosine).unwrap();
        coll.upsert(vec![1.0, 0.0], 1u64).unwrap();
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let err = coll.search(&[bad, 0.0], 5, None, None).unwrap_err();
            assert!(
                matches!(err, crate::Error::NonFinite { index: 0, .. }),
                "expected NonFinite at index 0, got {err:?}"
            );
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ultima-vector search_rejects_non_finite_query`
Expected: FAIL — search currently succeeds (returns garbage-ordered results).

- [ ] **Step 3: Add the check** — in `search_in`, directly after the `DimMismatch` early return:

```rust
        crate::validate::ensure_finite(query)?;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p ultima-vector search_rejects_non_finite_query`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ultima_vector/src/collection.rs
git commit -m "feat(ultima_vector): reject non-finite query vectors in search"
```

---

### Task 4: Validate finiteness on restore

**Files:**
- Modify: `ultima_vector/src/collection.rs` (`restore_iter` per-row loop, ~line 178)
- Test: `ultima_vector/tests/restore.rs`

**Interfaces:**
- Consumes: `crate::validate::ensure_finite` (Task 1).
- Produces: `restore_iter`/`restore_vec` return `Err(Error::NonFinite { .. })` if any row's embedding is non-finite; nothing is installed.

- [ ] **Step 1: Write the failing test** — append to `ultima_vector/tests/restore.rs` (mirrors `restore_dim_mismatch_errors_before_install`):

```rust
#[test]
fn restore_non_finite_embedding_errors_before_install() {
    use ultima_vector::error::Error;

    let store = Store::default();
    let coll = open_coll(store.clone());
    let v_before = store.latest_version();

    let bad = vec![(
        1u64,
        VectorRow {
            embedding: vec![1.0, f32::NAN, 0.0, 0.0],
            meta: 1,
            hnsw: ultima_vector::row::HnswState::empty(0),
        },
    )];
    let res = coll.restore_vec(bad, EntryPoint::default());
    assert!(matches!(res, Err(Error::NonFinite { index: 1, .. })));
    assert_eq!(
        store.latest_version(),
        v_before,
        "store must be unchanged on validation failure"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ultima-vector --test restore restore_non_finite`
Expected: FAIL — restore currently succeeds.

- [ ] **Step 3: Add the check** — in `restore_iter`'s per-row loop, directly after the `DimMismatch` early return (before the `HnswState` layer check, per the dim-then-finiteness order):

```rust
            crate::validate::ensure_finite(&row.embedding)?;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ultima-vector --test restore`
Expected: all restore tests pass (existing + 1 new).

- [ ] **Step 5: Commit**

```bash
git add ultima_vector/src/collection.rs ultima_vector/tests/restore.rs
git commit -m "feat(ultima_vector): validate finiteness on restore"
```

---

### Task 5: Promote `Distance` length checks to release asserts

**Files:**
- Modify: `ultima_vector/src/distance/simd.rs:191-209` (four dispatch wrappers)
- Modify: `ultima_vector/src/distance/mod.rs` (trait doc, `distance_many` default, `Cosine::distance_many`, `CosineNormalized::distance`)
- Test: `ultima_vector/src/distance/mod.rs` (tests module)

**Interfaces:**
- Produces: all crate-provided `Distance` impls panic on length mismatch in ALL build profiles (message: `vector dim mismatch`).

- [ ] **Step 1: Write the should_panic tests** — append inside `mod tests` in `ultima_vector/src/distance/mod.rs`:

```rust
    #[test]
    #[should_panic(expected = "vector dim mismatch")]
    fn cosine_len_mismatch_panics() {
        Cosine.distance(&[1.0, 2.0], &[1.0]);
    }

    #[test]
    #[should_panic(expected = "vector dim mismatch")]
    fn l2_len_mismatch_panics() {
        L2.distance(&[1.0, 2.0], &[1.0]);
    }

    #[test]
    #[should_panic(expected = "vector dim mismatch")]
    fn dot_product_len_mismatch_panics() {
        DotProduct.distance(&[1.0, 2.0], &[1.0]);
    }

    #[test]
    #[should_panic(expected = "vector dim mismatch")]
    fn cosine_normalized_len_mismatch_panics() {
        CosineNormalized.distance(&[1.0, 0.0], &[1.0]);
    }

    #[test]
    #[should_panic(expected = "targets/out length mismatch")]
    fn distance_many_out_len_mismatch_panics() {
        let t: Vec<&[f32]> = vec![&[1.0, 0.0]];
        let mut out = vec![0.0; 2];
        Cosine.distance_many(&[1.0, 0.0], &t, &mut out);
    }
```

- [ ] **Step 2: Run in RELEASE to verify they fail** (debug_asserts already fire in debug, so release is the meaningful red):

Run: `cargo test -p ultima-vector --release len_mismatch_panics`
Expected: FAIL — no panic occurs in release (tests report "did not panic"). Note: `cosine_normalized_len_mismatch_panics` may pass already if slice indexing happens to panic; the other three must fail.

- [ ] **Step 3: Promote the asserts.**

In `ultima_vector/src/distance/simd.rs`, change all four `debug_assert_eq!(...)` in the "Public dispatch wrappers" section (`dot`, `l2_squared`, `cosine`, `cosine_with_query_norm`) to `assert_eq!` (same args/message).

In `ultima_vector/src/distance/mod.rs`:
- `CosineNormalized::distance`: `debug_assert_eq!` → `assert_eq!`.
- Trait default `distance_many` and `Cosine::distance_many`: change `debug_assert_eq!(targets.len(), out.len());` to `assert_eq!(targets.len(), out.len(), "targets/out length mismatch");`.
- Add to the `Distance` trait doc comment (after the existing paragraph):

```rust
/// # Panics
///
/// Implementations provided by this crate panic if `a.len() != b.len()`,
/// and `distance_many` panics if `targets.len() != out.len()` — in all
/// build profiles, not just debug. Silent prefix-truncation in release
/// was a garbage-results footgun.
```

- [ ] **Step 4: Run tests in both profiles**

Run: `cargo test -p ultima-vector --release len_mismatch_panics && cargo test -p ultima-vector distance::`
Expected: PASS in both.

- [ ] **Step 5: Bench smoke check** (sanity only, NOT a gate — sandbox noise floor is ±2x):

Run: `cargo bench -p ultima-vector --bench distance -- cosine_1024_dim768`
Expected: same order of magnitude as before the change (a predicted length-compare before a dim-768 loop cannot plausibly register). Record the number in the commit message.

- [ ] **Step 6: Commit**

```bash
git add ultima_vector/src/distance/simd.rs ultima_vector/src/distance/mod.rs
git commit -m "fix(ultima_vector): panic on Distance length mismatch in release builds"
```

---

### Task 6: Feature doc, CLAUDE.md note, full verification

**Files:**
- Create: `docs/tasks/task40_vector_input_validation.md`
- Modify: `CLAUDE.md` (ultima_vector bullet in Architecture section)

**Interfaces:**
- Consumes: everything above (documents it).

- [ ] **Step 1: Write the feature doc** — `docs/tasks/task40_vector_input_validation.md`:

```markdown
# Task 40: ultima_vector Input Validation

**Origin:** 2026-06 deep-review deferred backlog. Two silent-wrong-results
holes: NaN embeddings poisoned HNSW ordering permanently; mismatched-length
slices through the raw `Distance` impls computed garbage over the shorter
prefix in release builds (`debug_assert` only).

## What changed

- New `Error::NonFinite { index, value }`.
- `validate::ensure_finite` (private) rejects NaN and ±Inf; called at every
  `VectorCollection` boundary, always directly after the existing
  `DimMismatch` check: `hnsw::insert::insert` (covers `upsert`, `upsert_in`,
  `bulk_insert`), `hnsw::insert::update_embedding`, `search_in` (query),
  `restore_iter` (per row, always on — pre-fix backups may contain NaN).
- Crate-provided `Distance` impls now `assert_eq!` on length in all build
  profiles (was `debug_assert_eq!`): panic beats silent garbage. Panic
  contract documented on the trait.

## Decisions

- **Reject all non-finite, not just NaN** — ±Inf also produces NaN distances
  (Inf−Inf, Inf·0) and is never legitimate embedding output.
- **Reject-at-boundary, not in kernels** — the graph can never contain a
  non-finite value, so the hot distance loops need no checks. The O(dim)
  boundary scan is negligible vs an HNSW insert.
- **No restore opt-out** — YAGNI until a measured bottleneck; restore already
  scans per-row for dim and layer-count.
- **Unchanged:** zero-vector semantics (`Cosine` → 1.0), `normalize_*`
  helpers, `distance_many` out-buffer contract (now asserted).

## Compatibility

Previously-accepted (garbage-producing) inputs now error or panic. Bug fix,
but observable for any caller feeding NaN/Inf or mismatched slices.

Design history: `docs/superpowers/specs/2026-07-07-vector-input-validation-design.md`,
`docs/superpowers/plans/2026-07-07-vector-input-validation.md`.
```

- [ ] **Step 2: Update CLAUDE.md** — in the `**ultima_vector**` bullet, after the sentence about `normalize_in_place` / `normalize_many`, add:

```
Inputs are validated at collection boundaries: dim mismatch and non-finite values (NaN/±Inf) are rejected with `Error::DimMismatch`/`Error::NonFinite` on insert, update, search, and restore; the raw `Distance` impls panic on length mismatch in all build profiles (task40).
```

- [ ] **Step 3: Full verification**

Run: `cargo test -p ultima-vector && cargo test && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all pass, zero warnings.

- [ ] **Step 4: Commit**

```bash
git add docs/tasks/task40_vector_input_validation.md CLAUDE.md
git commit -m "docs: task40 vector input validation feature doc"
```
