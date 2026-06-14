// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Quick-mode smoke tests: the fitness functions must emit every metric key
//! with a positive value. Keeps the microbenches from silently rotting.

use ultima_autobench::journal_bench;
use ultima_autobench::mw_commit_bench;
use ultima_autobench::smr_bench;

#[test]
fn journal_metrics_all_present_and_positive() {
    let m = journal_bench::run(&journal_bench::Config::quick());
    for key in journal_bench::METRIC_KEYS {
        let v = m.get(*key).copied().unwrap_or(f64::NAN);
        assert!(v.is_finite() && v > 0.0, "metric {key} missing or non-positive: {v}");
    }
}

#[test]
fn smr_metrics_all_present_and_positive() {
    let m = smr_bench::run(&smr_bench::Config::quick());
    for key in smr_bench::METRIC_KEYS {
        let v = m.get(*key).copied().unwrap_or(f64::NAN);
        assert!(v.is_finite() && v > 0.0, "metric {key} missing or non-positive: {v}");
    }
}

#[test]
fn mw_commit_metrics_all_present_and_positive() {
    let m = mw_commit_bench::run(&mw_commit_bench::Config::quick());
    for key in mw_commit_bench::METRIC_KEYS {
        let v = m.get(*key).copied().unwrap_or(f64::NAN);
        // mw_conflict_rate must be > 0 in quick mode too — the deterministic
        // hot-key schedule guarantees overlapping commits (see Config::quick).
        assert!(v.is_finite() && v > 0.0, "metric {key} missing or non-positive: {v}");
    }
}
