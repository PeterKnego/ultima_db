//! Geometric level sampling for HNSW.
//!
//! Levels follow `floor(-ln(uniform()) / ln(m))` so that
//! `P(level >= L) = (1/m)^L`. The result is capped at `max_level`.

use rand::{Rng, RngExt};

#[derive(Clone, Copy, Debug)]
pub struct LevelSampler {
    /// `1 / ln(m)`, precomputed.
    inv_ln_m: f64,
    max_level: u8,
}

impl LevelSampler {
    pub fn new(m: usize, max_level: u8) -> Self {
        assert!(m >= 2, "m must be >= 2");
        let inv_ln_m = 1.0 / (m as f64).ln();
        Self { inv_ln_m, max_level }
    }

    /// Sample a level in `[0, max_level]`. Geometric in `m` with the given cap.
    pub fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u8 {
        // `gen_range(0.0..1.0)` excludes 0, so `ln(u)` is finite negative.
        let u: f64 = rng.random_range(f64::MIN_POSITIVE..1.0);
        let level = (-u.ln() * self.inv_ln_m).floor();
        if level < 0.0 {
            return 0;
        }
        let level = level as u32;
        let max = u32::from(self.max_level);
        level.min(max) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn sample_never_exceeds_max_level() {
        let sampler = LevelSampler::new(16, 4);
        let mut rng = StdRng::seed_from_u64(0xDEAD);
        for _ in 0..10_000 {
            let l = sampler.sample(&mut rng);
            assert!(l <= 4, "level {l} exceeds max_level 4");
        }
    }

    #[test]
    fn sample_distribution_is_geometric_in_m() {
        let m = 4;
        let sampler = LevelSampler::new(m, 8);
        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        let n = 200_000;
        let mut at_or_above = [0u32; 9];
        for _ in 0..n {
            let l = sampler.sample(&mut rng);
            for slot in at_or_above.iter_mut().take(l as usize + 1) {
                *slot += 1;
            }
        }
        let p1 = at_or_above[1] as f64 / n as f64;
        let p2 = at_or_above[2] as f64 / n as f64;
        let p3 = at_or_above[3] as f64 / n as f64;
        assert!((p1 - 0.25).abs() < 0.02, "p(>=1) = {p1}, want ~0.25");
        assert!((p2 - 0.0625).abs() < 0.01, "p(>=2) = {p2}, want ~0.0625");
        assert!((p3 - 0.015625).abs() < 0.005, "p(>=3) = {p3}, want ~0.0156");
    }

    #[test]
    fn deterministic_with_seeded_rng() {
        let sampler = LevelSampler::new(16, 6);
        let mut a = StdRng::seed_from_u64(42);
        let mut b = StdRng::seed_from_u64(42);
        let seq_a: Vec<u8> = (0..100).map(|_| sampler.sample(&mut a)).collect();
        let seq_b: Vec<u8> = (0..100).map(|_| sampler.sample(&mut b)).collect();
        assert_eq!(seq_a, seq_b);
    }
}
