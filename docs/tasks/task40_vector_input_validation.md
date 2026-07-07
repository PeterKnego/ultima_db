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
  helpers' zero-vector no-op, `distance_many` out-buffer contract (now
  asserted).

## Perf

Bench smoke after the assert promotion: `distance/many/cosine_1024_dim768`
at 89.5 µs (~87 ns/call) — one predicted length-compare before a dim-768
kernel loop does not register (sandbox noise floor is ±2x; sanity check,
not a gate).

## Compatibility

Previously-accepted (garbage-producing) inputs now error or panic. Bug fix,
but observable for any caller feeding NaN/Inf or mismatched slices.

Design history: `docs/superpowers/specs/2026-07-07-vector-input-validation-design.md`,
`docs/superpowers/plans/2026-07-07-vector-input-validation.md`.
