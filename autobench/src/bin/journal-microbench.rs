// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Fitness binary for the `journal-commit` task. One JSON object on stdout.
//! `--check --baseline <file>` exits 2 on tolerance breach (regression gate).

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Parser;
use ultima_autobench::baseline::Baselines;
use ultima_autobench::journal_bench;

#[derive(Parser)]
struct Args {
    /// Emit machine-readable JSON on stdout (the only mode).
    #[arg(long)]
    json: bool,
    /// Compare against --baseline and exit 2 on breach.
    #[arg(long)]
    check: bool,
    #[arg(long)]
    baseline: Option<PathBuf>,
    /// Write a fresh baselines file from this run's metrics, then exit.
    #[arg(long)]
    write_baseline: Option<PathBuf>,
    /// Tolerance recorded into --write-baseline (fsync-heavy: 10%).
    #[arg(long, default_value_t = 10.0)]
    tolerance_pct: f64,
}

fn main() {
    let args = Args::parse();
    assert!(args.json, "only --json mode is supported");
    let metrics: BTreeMap<String, f64> = journal_bench::run(&journal_bench::Config::from_env());
    println!("{}", serde_json::to_string(&metrics).unwrap());

    if let Some(path) = args.write_baseline {
        let b = Baselines::from_metrics(&metrics, args.tolerance_pct);
        std::fs::write(&path, serde_json::to_string_pretty(&b).unwrap()).unwrap();
        eprintln!("baseline written to {}", path.display());
        return;
    }
    if args.check {
        let path = args.baseline.expect("--check requires --baseline <file>");
        let b: Baselines =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let breaches = b.check(&metrics);
        if !breaches.is_empty() {
            for br in &breaches {
                eprintln!(
                    "PERF REGRESSION {}: {:.1} vs baseline {:.1} ({:+.1}%)",
                    br.metric, br.value, br.baseline, br.regress_pct
                );
            }
            std::process::exit(2);
        }
        eprintln!("perf check OK ({} metrics within tolerance)", b.metrics.len());
    }
}
