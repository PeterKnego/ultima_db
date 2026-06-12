// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Committed perf baselines + tolerance checking (`--check` mode).
//! File format: { "metrics": { name: { value, tolerance_pct, direction } } }

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Minimize,
    Maximize,
}

/// Infer direction from the metric name: throughput-style names are
/// maximize, everything else (latencies, durations) is minimize.
pub fn infer_direction(metric: &str) -> Direction {
    if metric.contains("throughput") {
        Direction::Maximize
    } else {
        Direction::Minimize
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetricBaseline {
    pub value: f64,
    pub tolerance_pct: f64,
    pub direction: Direction,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Baselines {
    pub metrics: BTreeMap<String, MetricBaseline>,
}

#[derive(Debug, PartialEq)]
pub struct Breach {
    pub metric: String,
    pub value: f64,
    pub baseline: f64,
    pub regress_pct: f64,
}

impl Baselines {
    pub fn from_metrics(metrics: &BTreeMap<String, f64>, tolerance_pct: f64) -> Self {
        let metrics = metrics
            .iter()
            .map(|(k, &v)| {
                (
                    k.clone(),
                    MetricBaseline {
                        value: v,
                        tolerance_pct,
                        direction: infer_direction(k),
                    },
                )
            })
            .collect();
        Self { metrics }
    }

    /// Every baseline metric must be present and within tolerance.
    /// Metrics in `current` but not in the baseline are ignored.
    pub fn check(&self, current: &BTreeMap<String, f64>) -> Vec<Breach> {
        let mut breaches = Vec::new();
        for (name, base) in &self.metrics {
            let Some(&value) = current.get(name) else {
                breaches.push(Breach {
                    metric: name.clone(),
                    value: f64::NAN,
                    baseline: base.value,
                    regress_pct: f64::NAN,
                });
                continue;
            };
            if base.value == 0.0 {
                continue; // Sentinel: 0.0 is a placeholder for unmeasured slots — never breach.
            }
            let regress_pct = match base.direction {
                Direction::Minimize => (value - base.value) / base.value * 100.0,
                Direction::Maximize => (base.value - value) / base.value * 100.0,
            };
            // NaN current values pass silently (NaN > x is false) — callers must
            // not insert NaN metrics; JSON parsing cannot produce them.
            if regress_pct > base.tolerance_pct {
                breaches.push(Breach {
                    metric: name.clone(),
                    value,
                    baseline: base.value,
                    regress_pct,
                });
            }
        }
        breaches
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(value: f64, tol: f64, dir: Direction) -> Baselines {
        let mut b = Baselines::default();
        b.metrics.insert(
            "m".into(),
            MetricBaseline {
                value,
                tolerance_pct: tol,
                direction: dir,
            },
        );
        b
    }

    fn cur(v: f64) -> BTreeMap<String, f64> {
        BTreeMap::from([("m".to_string(), v)])
    }

    #[test]
    fn minimize_within_tolerance_passes() {
        assert!(base(100.0, 10.0, Direction::Minimize).check(&cur(109.0)).is_empty());
    }

    #[test]
    fn minimize_over_tolerance_breaches() {
        let b = base(100.0, 10.0, Direction::Minimize).check(&cur(111.0));
        assert_eq!(b.len(), 1);
        assert!((b[0].regress_pct - 11.0).abs() < 1e-9);
    }

    #[test]
    fn maximize_drop_breaches() {
        let b = base(1000.0, 10.0, Direction::Maximize).check(&cur(890.0));
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn maximize_gain_passes() {
        assert!(base(1000.0, 10.0, Direction::Maximize).check(&cur(2000.0)).is_empty());
    }

    #[test]
    fn missing_metric_breaches() {
        let b = base(100.0, 10.0, Direction::Minimize).check(&BTreeMap::new());
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn json_round_trip() {
        let b = Baselines::from_metrics(&cur(42.0), 10.0);
        let s = serde_json::to_string_pretty(&b).unwrap();
        let back: Baselines = serde_json::from_str(&s).unwrap();
        assert_eq!(back.metrics["m"].value, 42.0);
        assert_eq!(back.metrics["m"].direction, Direction::Minimize);
    }

    #[test]
    fn infer_direction_by_name() {
        assert_eq!(infer_direction("group_commit_throughput"), Direction::Maximize);
        assert_eq!(infer_direction("apply_p99_ns"), Direction::Minimize);
    }
}
