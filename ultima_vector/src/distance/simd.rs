// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! SIMD kernels for distance metrics, dispatched at runtime via `pulp::Arch`.
//!
//! Each metric is a small struct implementing `pulp::WithSimd`. The struct
//! captures the inputs; `with_simd` runs the kernel under whichever ISA the
//! cached `Arch` selected (AVX-512 / AVX2 / NEON / scalar).
//!
//! Numerical note: SIMD reductions reorder additions, so results differ from
//! the scalar reference by ~1-2 ULPs. Tests use a relative-plus-absolute
//! tolerance to absorb this.

use std::sync::OnceLock;

use pulp::{Arch, Simd, WithSimd};

static ARCH: OnceLock<Arch> = OnceLock::new();

/// Cached `pulp::Arch`. Detection runs once on first call.
pub(crate) fn arch() -> &'static Arch {
    ARCH.get_or_init(Arch::new)
}

// ---------------- Kernels ----------------

pub(crate) struct DotKernel<'a> {
    pub a: &'a [f32],
    pub b: &'a [f32],
}

impl<'a> WithSimd for DotKernel<'a> {
    type Output = f32;

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
        let (a_head, a_tail) = S::as_simd_f32s(self.a);
        let (b_head, b_tail) = S::as_simd_f32s(self.b);
        let mut acc = simd.splat_f32s(0.0);
        for (av, bv) in a_head.iter().zip(b_head.iter()) {
            acc = simd.mul_add_f32s(*av, *bv, acc);
        }
        let mut sum = simd.reduce_sum_f32s(acc);
        for (av, bv) in a_tail.iter().zip(b_tail.iter()) {
            sum += av * bv;
        }
        sum
    }
}

pub(crate) struct L2Kernel<'a> {
    pub a: &'a [f32],
    pub b: &'a [f32],
}

impl<'a> WithSimd for L2Kernel<'a> {
    type Output = f32;

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
        let (a_head, a_tail) = S::as_simd_f32s(self.a);
        let (b_head, b_tail) = S::as_simd_f32s(self.b);
        let mut acc = simd.splat_f32s(0.0);
        for (av, bv) in a_head.iter().zip(b_head.iter()) {
            let diff = simd.sub_f32s(*av, *bv);
            acc = simd.mul_add_f32s(diff, diff, acc);
        }
        let mut sum = simd.reduce_sum_f32s(acc);
        for (av, bv) in a_tail.iter().zip(b_tail.iter()) {
            let d = av - bv;
            sum += d * d;
        }
        sum
    }
}

pub(crate) struct CosineKernel<'a> {
    pub a: &'a [f32],
    pub b: &'a [f32],
}

impl<'a> WithSimd for CosineKernel<'a> {
    type Output = f32;

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
        let (a_head, a_tail) = S::as_simd_f32s(self.a);
        let (b_head, b_tail) = S::as_simd_f32s(self.b);
        let mut dot = simd.splat_f32s(0.0);
        let mut na = simd.splat_f32s(0.0);
        let mut nb = simd.splat_f32s(0.0);
        for (av, bv) in a_head.iter().zip(b_head.iter()) {
            dot = simd.mul_add_f32s(*av, *bv, dot);
            na = simd.mul_add_f32s(*av, *av, na);
            nb = simd.mul_add_f32s(*bv, *bv, nb);
        }
        let mut dot_s = simd.reduce_sum_f32s(dot);
        let mut na_s = simd.reduce_sum_f32s(na);
        let mut nb_s = simd.reduce_sum_f32s(nb);
        for (av, bv) in a_tail.iter().zip(b_tail.iter()) {
            dot_s += av * bv;
            na_s += av * av;
            nb_s += bv * bv;
        }
        if na_s == 0.0 || nb_s == 0.0 {
            return 1.0;
        }
        1.0 - dot_s / (na_s * nb_s).sqrt()
    }
}

pub(crate) struct NormSquaredKernel<'a> {
    pub v: &'a [f32],
}

impl<'a> WithSimd for NormSquaredKernel<'a> {
    type Output = f32;

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
        let (head, tail) = S::as_simd_f32s(self.v);
        let mut acc = simd.splat_f32s(0.0);
        for x in head.iter() {
            acc = simd.mul_add_f32s(*x, *x, acc);
        }
        let mut sum = simd.reduce_sum_f32s(acc);
        for &x in tail.iter() {
            sum += x * x;
        }
        sum
    }
}

pub(crate) struct ScaleInPlaceKernel<'a> {
    pub v: &'a mut [f32],
    pub factor: f32,
}

impl<'a> WithSimd for ScaleInPlaceKernel<'a> {
    type Output = ();

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
        let factor_v = simd.splat_f32s(self.factor);
        let (head, tail) = S::as_mut_simd_f32s(self.v);
        for x in head.iter_mut() {
            *x = simd.mul_f32s(*x, factor_v);
        }
        for x in tail.iter_mut() {
            *x *= self.factor;
        }
    }
}

/// Cosine distance when `‖query‖²` is known. Saves one FMA-chain per target
/// in batched scans.
pub(crate) struct CosineWithQNormKernel<'a> {
    pub q: &'a [f32],
    pub t: &'a [f32],
    pub q_norm_sq: f32,
}

impl<'a> WithSimd for CosineWithQNormKernel<'a> {
    type Output = f32;

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) -> Self::Output {
        let (q_head, q_tail) = S::as_simd_f32s(self.q);
        let (t_head, t_tail) = S::as_simd_f32s(self.t);
        let mut dot = simd.splat_f32s(0.0);
        let mut nt = simd.splat_f32s(0.0);
        for (qv, tv) in q_head.iter().zip(t_head.iter()) {
            dot = simd.mul_add_f32s(*qv, *tv, dot);
            nt = simd.mul_add_f32s(*tv, *tv, nt);
        }
        let mut dot_s = simd.reduce_sum_f32s(dot);
        let mut nt_s = simd.reduce_sum_f32s(nt);
        for (qv, tv) in q_tail.iter().zip(t_tail.iter()) {
            dot_s += qv * tv;
            nt_s += tv * tv;
        }
        if self.q_norm_sq == 0.0 || nt_s == 0.0 {
            return 1.0;
        }
        1.0 - dot_s / (self.q_norm_sq * nt_s).sqrt()
    }
}

// ---------------- Public dispatch wrappers ----------------

pub(crate) fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dim mismatch");
    arch().dispatch(DotKernel { a, b })
}

pub(crate) fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dim mismatch");
    arch().dispatch(L2Kernel { a, b })
}

pub(crate) fn cosine(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dim mismatch");
    arch().dispatch(CosineKernel { a, b })
}

pub(crate) fn cosine_with_query_norm(q: &[f32], t: &[f32], q_norm_sq: f32) -> f32 {
    debug_assert_eq!(q.len(), t.len(), "vector dim mismatch");
    arch().dispatch(CosineWithQNormKernel { q, t, q_norm_sq })
}

pub(crate) fn norm_squared(v: &[f32]) -> f32 {
    arch().dispatch(NormSquaredKernel { v })
}

pub(crate) fn scale_in_place(v: &mut [f32], factor: f32) {
    arch().dispatch(ScaleInPlaceKernel { v, factor });
}

// ---------------- Tests ----------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::scalar;
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    /// Relative + absolute tolerance: SIMD reductions reorder adds; expect
    /// ~1-2 ULPs of drift on long sums.
    fn close(actual: f32, expected: f32) {
        let tol = 1e-4 * expected.abs() + 1e-6;
        assert!(
            (actual - expected).abs() < tol,
            "expected {expected}, got {actual} (tol {tol})"
        );
    }

    fn rand_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect()
    }

    /// Sweep covers tail remainders for SIMD widths 4 (NEON), 8 (AVX2), 16 (AVX-512).
    const DIMS: &[usize] = &[
        1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 33, 64, 128, 384, 768, 1024, 1031, 1536,
    ];

    #[test]
    fn dot_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(0xD07);
        for &d in DIMS {
            let a = rand_vec(&mut rng, d);
            let b = rand_vec(&mut rng, d);
            close(dot(&a, &b), scalar::dot(&a, &b));
        }
    }

    #[test]
    fn l2_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(0x12);
        for &d in DIMS {
            let a = rand_vec(&mut rng, d);
            let b = rand_vec(&mut rng, d);
            close(l2_squared(&a, &b), scalar::l2_squared(&a, &b));
        }
    }

    #[test]
    fn cosine_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(0xC05);
        for &d in DIMS {
            let a = rand_vec(&mut rng, d);
            let b = rand_vec(&mut rng, d);
            close(cosine(&a, &b), scalar::cosine(&a, &b));
        }
    }

    #[test]
    fn cosine_zero_vector_returns_one() {
        let z = vec![0.0; 32];
        let v = vec![1.0; 32];
        close(cosine(&z, &v), 1.0);
        close(cosine(&v, &z), 1.0);
        close(cosine(&z, &z), 1.0);
    }

    #[test]
    fn identical_vectors_zero_distance() {
        let mut rng = StdRng::seed_from_u64(0x1D);
        for &d in DIMS {
            let a = rand_vec(&mut rng, d);
            close(l2_squared(&a, &a), 0.0);
            close(cosine(&a, &a), 0.0);
        }
    }

    /// Force the scalar lane by skipping `Arch` and dispatching directly to
    /// `pulp::Scalar`. Ensures the scalar path stays correct on hosts that
    /// always have SIMD.
    #[test]
    fn forced_scalar_matches_reference() {
        let mut rng = StdRng::seed_from_u64(0x5CA1);
        for &d in DIMS {
            let a = rand_vec(&mut rng, d);
            let b = rand_vec(&mut rng, d);
            let dot_s = pulp::Scalar.vectorize(DotKernel { a: &a, b: &b });
            let l2_s = pulp::Scalar.vectorize(L2Kernel { a: &a, b: &b });
            let cos_s = pulp::Scalar.vectorize(CosineKernel { a: &a, b: &b });
            close(dot_s, scalar::dot(&a, &b));
            close(l2_s, scalar::l2_squared(&a, &b));
            close(cos_s, scalar::cosine(&a, &b));
        }
    }
}
