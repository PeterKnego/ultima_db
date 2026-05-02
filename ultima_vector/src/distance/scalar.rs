// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Scalar reference implementations of distance metrics.
//!
//! These exist for two reasons:
//! 1. They serve as the `Distance` impl bodies on hosts where SIMD detection
//!    falls through to scalar.
//! 2. They are the test oracle the SIMD kernels are validated against.

pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dim mismatch");
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        let ai = a[i];
        let bi = b[i];
        dot += ai * bi;
        na += ai * ai;
        nb += bi * bi;
    }
    if na == 0.0 || nb == 0.0 {
        return 1.0;
    }
    let denom = (na * nb).sqrt();
    1.0 - dot / denom
}

pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dim mismatch");
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dim mismatch");
    let mut acc = 0.0f32;
    for i in 0..a.len() {
        acc += a[i] * b[i];
    }
    acc
}
