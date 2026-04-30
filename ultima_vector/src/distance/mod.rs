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
}
