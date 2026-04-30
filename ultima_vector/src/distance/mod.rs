//! Distance metrics for vector similarity search.
//!
//! All metrics return values where **smaller = closer**, so a single comparator
//! drives the HNSW heaps regardless of which metric is in use.

#[cfg(test)]
mod scalar;
mod simd;

/// Distance function applied between two equal-length vectors.
///
/// Implementors must return `0.0` for two identical vectors and otherwise
/// satisfy `distance(a, b) >= 0` for `Cosine` and `L2`. `DotProduct` returns
/// the negated inner product so smaller-is-closer holds uniformly.
pub trait Distance: Send + Sync + 'static {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32;
}

/// `1 - cos_sim(a, b)`, in `[0, 2]`. Returns 1.0 if either operand is the zero vector.
#[derive(Default, Clone, Copy, Debug)]
pub struct Cosine;

/// Squared L2 (Euclidean) distance. Monotonic with L2, cheaper to compute.
#[derive(Default, Clone, Copy, Debug)]
pub struct L2;

/// Negated inner product, so smaller-is-closer.
#[derive(Default, Clone, Copy, Debug)]
pub struct DotProduct;

impl Distance for Cosine {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        simd::cosine(a, b)
    }
}

impl Distance for L2 {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        simd::l2_squared(a, b)
    }
}

impl Distance for DotProduct {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        -simd::dot(a, b)
    }
}

/// Cosine distance over **pre-normalized** vectors. Both stored embeddings
/// and queries must already have unit L2 norm; cosine then reduces to
/// `1.0 - dot(a, b)`. Half the FMAs of [`Cosine`].
///
/// Use [`normalize_in_place`] / [`normalize_many`] to prepare inputs.
#[derive(Default, Clone, Copy, Debug)]
pub struct CosineNormalized;

impl Distance for CosineNormalized {
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "vector dim mismatch");
        1.0 - simd::dot(a, b)
    }
}

/// Scale `v` to unit L2 norm. Zero vectors are left unchanged.
pub fn normalize_in_place(v: &mut [f32]) {
    let n2 = simd::norm_squared(v);
    if n2 == 0.0 {
        return;
    }
    let inv = 1.0 / n2.sqrt();
    simd::scale_in_place(v, inv);
}

/// Apply [`normalize_in_place`] to each vector in `vectors`.
pub fn normalize_many(vectors: &mut [Vec<f32>]) {
    for v in vectors.iter_mut() {
        normalize_in_place(v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f32, b: f32) {
        assert!((a - b).abs() < 1e-5, "expected ≈ {b}, got {a}");
    }

    #[test]
    fn cosine_identical_vectors_is_zero() {
        let a = [1.0, 2.0, 3.0];
        approx(Cosine.distance(&a, &a), 0.0);
    }

    #[test]
    fn cosine_orthogonal_vectors_is_one() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        approx(Cosine.distance(&a, &b), 1.0);
    }

    #[test]
    fn cosine_opposite_vectors_is_two() {
        let a = [1.0, 0.0];
        let b = [-1.0, 0.0];
        approx(Cosine.distance(&a, &b), 2.0);
    }

    #[test]
    fn cosine_zero_vectors_returns_max() {
        let a = [0.0, 0.0];
        let b = [1.0, 1.0];
        approx(Cosine.distance(&a, &b), 1.0);
    }

    #[test]
    fn l2_returns_squared_distance() {
        let a = [1.0, 2.0];
        let b = [4.0, 6.0];
        approx(L2.distance(&a, &b), 25.0);
    }

    #[test]
    fn l2_same_vector_is_zero() {
        let a = [1.0, 2.0, 3.0];
        approx(L2.distance(&a, &a), 0.0);
    }

    #[test]
    fn dot_product_negative_of_inner_product() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 5.0, 6.0];
        approx(DotProduct.distance(&a, &b), -32.0);
    }

    #[test]
    fn dot_product_orthogonal_is_zero() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        approx(DotProduct.distance(&a, &b), 0.0);
    }

    #[test]
    fn cosine_normalized_matches_cosine_after_normalize() {
        use rand::{RngExt, SeedableRng, rngs::StdRng};
        let mut rng = StdRng::seed_from_u64(0xC051);
        for d in [16usize, 64, 384, 768, 1031] {
            let mut a: Vec<f32> = (0..d).map(|_| rng.random_range(-1.0..1.0)).collect();
            let mut b: Vec<f32> = (0..d).map(|_| rng.random_range(-1.0..1.0)).collect();
            let cos = Cosine.distance(&a, &b);
            normalize_in_place(&mut a);
            normalize_in_place(&mut b);
            let cosn = CosineNormalized.distance(&a, &b);
            let tol = 1e-4 * cos.abs() + 1e-5;
            assert!((cos - cosn).abs() < tol, "{cos} vs {cosn}");
        }
    }

    #[test]
    fn normalize_in_place_yields_unit_norm() {
        let mut v = vec![3.0, 4.0];
        normalize_in_place(&mut v);
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((n - 1.0).abs() < 1e-6);
    }

    #[test]
    fn normalize_in_place_zero_vector_stays_zero() {
        let mut v = vec![0.0; 8];
        normalize_in_place(&mut v);
        assert!(v.iter().all(|&x| x == 0.0));
    }

    #[test]
    fn normalize_many_normalizes_each_independently() {
        let mut vs: Vec<Vec<f32>> = vec![
            vec![3.0, 4.0],
            vec![1.0, 0.0, 0.0],
            vec![0.0; 4],
        ];
        normalize_many(&mut vs);
        let n0: f32 = vs[0].iter().map(|x| x * x).sum::<f32>().sqrt();
        let n1: f32 = vs[1].iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((n0 - 1.0).abs() < 1e-6);
        assert!((n1 - 1.0).abs() < 1e-6);
        assert!(vs[2].iter().all(|&x| x == 0.0));
    }
}
