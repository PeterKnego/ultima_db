// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Quick-mode smoke tests: the fitness functions must emit every metric key
//! with a positive value. Keeps the microbenches from silently rotting.

use ultima_autobench::journal_bench;

#[test]
fn journal_metrics_all_present_and_positive() {
    let m = journal_bench::run(&journal_bench::Config::quick());
    for key in journal_bench::METRIC_KEYS {
        let v = m.get(*key).copied().unwrap_or(f64::NAN);
        assert!(v.is_finite() && v > 0.0, "metric {key} missing or non-positive: {v}");
    }
}
