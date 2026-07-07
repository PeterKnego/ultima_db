# ultima_vector input validation — design

**Date:** 2026-07-07
**Status:** approved (brainstormed with Peter; all three policy questions answered)
**Origin:** 2026-06 deep-review deferred backlog — "NaN embeddings accepted by
ultima_vector (poison HNSW ordering silently); mismatched-length vectors via the
raw `Distance` trait return garbage in release (debug_assert only)."

## Problem

Two silent-wrong-results holes in `ultima_vector`:

1. **Non-finite values.** Nothing anywhere checks for NaN/±Inf. A NaN embedding
   is inserted into the HNSW graph and poisons the ordering comparators for
   every future search, permanently. A NaN query silently returns garbage for
   that search.
2. **Raw `Distance` length mismatch.** `Cosine.distance(&a, &b)` and friends
   only `debug_assert_eq!` on lengths; release builds silently compute over the
   shorter prefix. (Collection entry points already validate dim with
   `Error::DimMismatch`, so this hole only affects direct users of the public
   `Distance` impls.)

## Decisions (with Peter, 2026-07-07)

- **Reject all non-finite values** (`!is_finite()`: NaN and ±Inf) at
  `VectorCollection` write/search boundaries. Inf also produces NaN distances
  (Inf−Inf, Inf·0) and is never legitimate embedding-model output.
- **Promote the `Distance` length `debug_assert`s to release `assert!`s** —
  panic instead of silent garbage. One predictable branch per call, negligible
  vs a dim-384 kernel loop; sanity-check with the distance criterion bench.
- **`restore_iter` validates finiteness too** (always on, no opt-out flag).
  Backups written before this fix may contain NaN rows; restore is the only
  path by which they could survive into a new store. Consistent with restore's
  existing per-row dim and layer checks.

## Design

### Error variant (`src/error.rs`)

```rust
#[error("non-finite value ({value}) at index {index} in vector")]
NonFinite { index: usize, value: f32 },
```

### Validation helper

Private `fn ensure_finite(v: &[f32]) -> Result<()>` (in `lib.rs` or a tiny
`validate.rs`): first `!is_finite()` index → `Err(NonFinite { index, value })`.
O(dim) scan, negligible next to an HNSW insert.

### Call sites

Placed directly next to the existing `DimMismatch` checks so every entry point
validates both, in the same order (dim first, then finiteness):

1. `hnsw/insert.rs::insert` — covers `upsert`, `upsert_in`, `bulk_insert`
2. `hnsw/insert.rs::update_embedding` — covers `update_embedding[_in]`
3. `collection.rs::search_in` — query vector (covers `search`)
4. `collection.rs::restore_iter` — per-row loop

Reject-at-boundary means the graph can never contain a non-finite value, so
the hot search/insert distance loops need no checks.

### Raw `Distance` trait hardening

Promote `debug_assert_eq!` → `assert_eq!` in:

- `distance/simd.rs` public entry kernels: `cosine`, `l2_squared`, `dot`,
  `cosine_with_query_norm`
- `distance/mod.rs`: `CosineNormalized::distance`, and the `distance_many`
  targets/out length checks (both the trait default and `Cosine`'s override)

Document the panic contract on the `Distance` trait docs ("panics on length
mismatch").

Perf: run the existing distance criterion bench as a smoke check. Sandbox noise
floor is ±2x (see bench A/B methodology memory), so this is a sanity check,
not a gate — a predicted compare before a dim-384 loop cannot plausibly
register.

### Explicitly unchanged

- Zero-vector handling (`Cosine` returns 1.0 — documented behavior).
- `normalize_in_place` / `normalize_many` on zero vectors (no-op, documented).
- No opt-out flag for restore validation (YAGNI until a measured bottleneck).
- `hnsw/insert.rs` internals beyond the two boundary checks.

## Testing (TDD, red first)

- `upsert`, `update_embedding`, `search`, `restore_vec` each reject NaN, +Inf,
  and −Inf with `NonFinite { index, .. }` and the correct index.
- `update_embedding` failure leaves the old embedding intact (searchable at
  old location).
- A NaN row mid-`bulk_insert` rolls back the whole batch (mirrors the existing
  `bulk_insert_dim_mismatch_aborts_whole_batch` test).
- `restore_vec` with a NaN row installs nothing.
- `#[should_panic]` tests for each raw `Distance` impl on mismatched lengths —
  now valid under both debug and release profiles.

## Compatibility note

Previously-accepted (garbage-producing) inputs now error or panic. Strictly a
bug fix per the deferred-review framing, but an observable behavior change for
any caller currently feeding NaN/Inf or mismatched-length slices.

## Docs

Canonical feature doc: `docs/tasks/task40_vector_input_validation.md`.
