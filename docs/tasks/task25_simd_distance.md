# Task 25 — `ultima_vector`: SIMD distance kernels

SIMD-accelerated f32 distance kernels for `ultima_vector` (`Cosine`, `L2`,
`DotProduct`), built on `pulp` for runtime-dispatched portable SIMD across
AVX-512 / AVX2 / NEON / scalar fallback. Adds an opt-in pre-normalized
cosine fast path (`CosineNormalized`), bulk normalization helpers, and a
batched `Distance::distance_many` API used by the brute-force-fallback
path.

## Context

`ultima_vector` (task 22) shipped with scalar `for`-loop distance kernels
in `src/distance.rs`. Distance is the inner loop of every HNSW search:
each visited node + each brute-force-fallback id pays one
`distance(query, &row.embedding)` call. Replacing the scalar loops with
SIMD is the highest-impact CPU optimization available without touching
storage layout or the algorithm.

The scope-narrowing decisions made up front:

- **f32 only.** `VectorRow::embedding` is `Vec<f32>` today; quantization
  (i8 SQ, PQ, binary) is a separate future task and lives only as
  documented extension paths in this doc.
- **Both x86-64 AVX2/AVX-512 and aarch64 NEON first-class.** Runtime
  dispatch on x86-64 (so a single binary picks the best ISA at startup);
  NEON is mandatory on aarch64 so no dispatch needed there.
- **Stable Rust.** `std::simd` is still nightly; off the table.

## Decisions

- **Use `pulp` (v0.22) for SIMD, not hand-rolled `std::arch` intrinsics.**
  Pulp generates per-ISA monomorphizations from one kernel definition and
  has built-in runtime dispatch via `Arch::detect()`. Trade-off accepted:
  ~5-20% off the hand-rolled peak in exchange for one kernel per metric
  instead of three (NEON + AVX2 + AVX-512), no `unsafe` in our code, and
  a much smaller maintenance surface. Validated empirically — see
  *Calibration* below.
- **Calibration spike before committing.** Hand-rolled NEON + AVX2 L2
  references were written and benchmarked side-by-side against pulp's L2.
  On Apple Silicon: ratio 1.169 at dim 128, 1.010-1.023 at dims
  ≥ 384. `worst_ratio = 1.169` falls in the
  "PROCEED but document" band of the decision matrix. The spike was
  removed after the call. Hand-rolled remains an escape hatch if a
  specific kernel later underperforms by more than 25%.
- **Cached global `pulp::Arch`.** Detection runs once via `OnceLock`;
  every subsequent `distance()` call hits the cached handle. The detected
  ISA is a property of the running CPU, not of any metric or collection
  — one cache, in one place.
- **Unaligned SIMD loads, `Vec<f32>` storage unchanged.** On Zen 3+ /
  Ice Lake+ / M1+ unaligned loads carry no throughput penalty against
  long FMA chains. Forcing 32/64-byte alignment would invade `VectorRow`
  layout, persistence serde, and MVCC clone semantics for ~0-5% on a
  microbench. Not worth it. (Future PQ codes — a *new* storage format —
  should be designed aligned from the start.)
- **`CosineNormalized` is a separate ZST, not a flag on `Cosine`.** The
  fast path (`1 - dot(a, b)` for unit-norm vectors) requires that both
  stored embeddings and queries are pre-normalized. A separate type
  encodes this precondition in the type system; runtime is free (no
  branch) and accidental misuse via a forgotten flag is impossible.
  Pairs with public `normalize_in_place` / `normalize_many` helpers.
- **`Distance::distance_many(query, targets, out)` with a default impl
  looping `distance`.** The override on `Cosine` precomputes
  `‖query‖²` once and feeds it to a `CosineWithQNormKernel` that has
  two accumulators instead of three. `L2`, `DotProduct`,
  `CosineNormalized` inherit the default. Used by `brute_force_filtered`
  in `hnsw/search.rs` (one batched call, tombstoned rows skipped before
  the kernel).
- **No call-site change inside `search_layer`.** Layer beam search
  visits one neighbor at a time; the heap update order matters and
  batching would restructure the algorithm. The win there comes from
  the kernel itself getting faster.
- **Scalar oracle preserved as test infrastructure.** `distance/scalar.rs`
  holds the original three scalar fns. `mod scalar;` is `#[cfg(test)]`-
  gated so the functions don't bloat release builds; the SIMD tests in
  `distance/simd.rs` use the scalar versions as the equivalence oracle.

## Code map

```
ultima_vector/
  src/
    distance/
      mod.rs        public Distance trait, Cosine/L2/DotProduct/CosineNormalized,
                    normalize_in_place / normalize_many, inline tests
      simd.rs       cached pulp::Arch + WithSimd kernel structs + dispatch wrappers
                    (Dot/L2/Cosine/CosineWithQNorm/NormSquared/ScaleInPlace)
      scalar.rs     reference implementations (test oracle, #[cfg(test)] only)
    hnsw/search.rs  brute_force_filtered uses Distance::distance_many
  benches/
    distance.rs    criterion: pairwise per-metric × dim {128,384,768,1536},
                   distance_many batch, end-to-end HNSW QPS @ n=10k dim=768
```

Public API additions: `CosineNormalized`, `normalize_in_place`,
`normalize_many`, `Distance::distance_many`. The original surface
(`Distance` trait, `Cosine`, `L2`, `DotProduct`) is source-compatible.

## Implementation notes

- **Pulp idiom.** Each kernel is a small struct (e.g.
  `DotKernel { a, b }`) implementing `WithSimd` with a `#[inline(always)]`
  `with_simd<S: Simd>` body. The body splits the input via
  `S::as_simd_f32s(slice) -> (&[S::f32s], &[f32])`, processes the SIMD
  head with `mul_add_f32s` accumulators, calls `reduce_sum_f32s` to
  collapse lanes, then folds the scalar tail. Mutable variants
  (`ScaleInPlaceKernel`) use `S::as_mut_simd_f32s`.
- **Numerical drift.** SIMD reductions reorder additions, so kernel
  outputs differ from the scalar reference by ~1-2 ULPs on long sums.
  Tests use a relative+absolute tolerance `1e-4 * |expected| + 1e-6`.
  HNSW recall tests don't notice — they measure recall, not exact
  distances.
- **Zero-vector handling.** Cosine kernels return 1.0 if either norm is
  zero (matches the scalar contract). `normalize_in_place` early-returns
  on zero norm (vector stays zero rather than producing NaN).
- **Tail handling.** `as_simd_f32s` splits at the lane boundary; any
  remainder is folded scalar inside the same kernel. The test sweep
  `[1,3,4,5,7,8,9,15,16,17,31,33,64,128,384,768,1024,1031,1536]` covers
  every `dim % LANES` for SIMD widths 4 (NEON), 8 (AVX2), 16 (AVX-512).
- **`#[cfg(test)]` on `mod scalar;`.** Once the public `Distance` impls
  were routed through `simd::*` (Task 5 of the implementation plan),
  scalar functions were unused outside tests. Gating to test builds
  removes them from release artifacts entirely; they remain available
  to the SIMD equivalence tests via `crate::distance::scalar::*`.
- **`brute_force_filtered` borrow form.** The batched callsite gathers
  `(id, &[f32])` pairs by borrowing each row's embedding through the
  `TableReader` for the function's lifetime — no clone needed. Tombstoned
  rows are filtered before the kernel call, so distance compute is
  never paid for dead rows.

## Verification

- **SIMD vs scalar equivalence** (`distance::simd::tests`):
  `dot_matches_scalar`, `l2_matches_scalar`, `cosine_matches_scalar` over
  the dim sweep; `cosine_zero_vector_returns_one`,
  `identical_vectors_zero_distance`; `forced_scalar_matches_reference`
  exercises `pulp::Scalar.vectorize(kernel)` directly so the scalar lane
  is tested even on hosts that always have SIMD.
- **CosineNormalized + normalize helpers** (`distance::tests`):
  `cosine_normalized_matches_cosine_after_normalize`,
  `normalize_in_place_yields_unit_norm`,
  `normalize_in_place_zero_vector_stays_zero`,
  `normalize_many_normalizes_each_independently`.
- **`distance_many` equivalence**:
  `distance_many_matches_loop_of_distance` validates all four impls
  (Cosine override + three default-impl inheritors) against a loop of
  `distance` calls.
- **HNSW integration tests** (`tests/integration.rs`,
  `tests/concurrency.rs`, `tests/filtered.rs`, `tests/restore.rs`) all
  pass unchanged — they exercise the full algorithm with SIMD-computed
  distances.
- `cargo clippy -p ultima-vector --all-targets -- -D warnings` clean.
- `cargo bench -p ultima-vector --bench distance --no-run` builds clean.

### Calibration numbers (Apple Silicon, criterion median)

| dim  | pulp (ns) | hand-rolled NEON (ns) | ratio |
|------|-----------|------------------------|-------|
| 128  | 15.143    | 12.958                 | 1.169 |
| 384  | 56.945    | 56.385                 | 1.010 |
| 768  | 130.01    | 127.05                 | 1.023 |
| 1536 | 324.12    | 320.87                 | 1.010 |

Pulp lands within 2.5% of hand-rolled at production-relevant dims; the
17% gap at dim 128 reflects fixed-cost amortization over a 2 ns workload
and isn't on any HNSW hot path. The spike code (`distance/handrolled.rs`,
`benches/calibration.rs`) was removed after the decision.

## Future work

- **Scalar quantization (i8 SQ).** Same kernel shape; `i32` accumulators
  via `VPDPBUSD` (AVX-VNNI on x86-64) or `vdotq_s32` (ARM dotprod
  extension). ~3-4× throughput plus 4× memory savings. Requires a new
  row type with per-vector scale/zero-point.
- **Product quantization (PQ).** AVX2 16-way `VPSHUFB` LUT scan against
  precomputed query tables. Higher recall-per-byte than SQ; needs
  quantized + aligned code storage (a separate row schema).
- **Binary quantization.** XOR + POPCNT (`vcnt`/`popcntq`). Fastest of
  all metrics; bit-packed storage.
- **Prefetch hints in `search_layer`.** `core::intrinsics::prefetch_read_data`
  on the next neighbor's embedding slice one iteration ahead. Modest win
  (~5-15%) given the B-tree-row indirection pattern. Lower priority than
  quantization.
- **Hand-rolled escape hatch.** If a specific kernel ever underperforms
  pulp's codegen by > 25% on a target ISA, replace just that kernel with
  hand-rolled `std::arch` and keep pulp for the rest. The
  `Arch`-dispatched kernel pattern accommodates the swap with no
  structural change to the public API.
