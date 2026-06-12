// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Batched-sample timing helpers (pattern from uc_autobench's
//! shmem-microbench): time a batch of B ops with one Instant read, record
//! elapsed/B as one fractional per-op sample, compute percentiles over the
//! samples. Amortizes the host clock tick below 1 ns of resolution.

use std::time::Instant;

/// Nearest-rank (floor) percentile, p in [0, 100]. Sorts in place.
/// Empty input returns 0.0. NaN inputs panic.
pub fn percentile(samples: &mut [f64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((samples.len() - 1) as f64 * p / 100.0) as usize;
    samples[idx]
}

/// Run `samples` batches of `batch` calls to `op`; return per-op ns samples.
pub fn batched_samples_ns(samples: usize, batch: usize, mut op: impl FnMut()) -> Vec<f64> {
    let mut out = Vec::with_capacity(samples);
    for _ in 0..samples {
        let t = Instant::now();
        for _ in 0..batch {
            op();
        }
        out.push(t.elapsed().as_nanos() as f64 / batch as f64);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_picks_expected_rank() {
        let mut s: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        assert_eq!(percentile(&mut s, 50.0), 50.0);
        assert_eq!(percentile(&mut s, 99.0), 99.0);
        assert_eq!(percentile(&mut s, 100.0), 100.0);
    }

    #[test]
    fn percentile_empty_is_zero() {
        assert_eq!(percentile(&mut [] as &mut [f64], 99.0), 0.0);
    }

    #[test]
    fn percentile_p0_is_minimum() {
        let mut s = vec![3.0, 1.0, 2.0];
        assert_eq!(percentile(&mut s, 0.0), 1.0);
    }

    #[test]
    fn percentile_single_element() {
        assert_eq!(percentile(&mut [42.0], 0.0), 42.0);
        assert_eq!(percentile(&mut [42.0], 100.0), 42.0);
    }

    #[test]
    fn batched_samples_counts_and_positive() {
        let s = batched_samples_ns(10, 100, || { std::hint::black_box(0u64); });
        assert_eq!(s.len(), 10);
        assert!(s.iter().all(|&v| v >= 0.0));
    }
}
