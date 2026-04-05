# Task 6: Benchmark Harness

## Motivation

UltimaDB's core value proposition — persistent CoW B-trees, O(1) clones, snapshot isolation — depends on performance characteristics that are easy to regress silently. A single-record insert that accidentally triggers a full tree copy instead of a path copy won't break any test, but will destroy throughput. We need:

1. **Repeatable microbenchmarks** to track operation costs across commits.
2. **Baseline comparison** to catch regressions in PRs before merge.
3. **Flamegraph profiling** to identify hotspots (unnecessary Arc clones, deep B-tree rebalancing, index overhead).

---

## Design decisions

### 1. Criterion as the benchmark framework

**Alternatives considered:**

| Approach | Pros | Cons |
|----------|------|------|
| **A. Criterion** | Statistical rigor (confidence intervals, outlier detection); HTML reports; baseline save/compare; pluggable profiler API | External dependency; slower iteration (warmup + measurement) |
| B. `#[bench]` (nightly) | Zero dependencies; built into rustc | Nightly-only; no statistical analysis; no baseline comparison; no profiler hooks |
| C. `divan` | Attribute macro API; simpler setup | Newer/less ecosystem support; no pluggable profiler API; no critcmp integration |

**Chosen: A.** Criterion's pluggable profiler API is the deciding factor — it allows pprof-rs integration for per-benchmark flamegraphs without a separate profiling step. The `html_reports` feature generates trend charts across runs. Its JSON output feeds directly into critcmp for cross-branch comparison.

### 2. pprof-rs for flamegraphs (not cargo-flamegraph)

**Alternatives considered:**

| Approach | Pros | Cons |
|----------|------|------|
| **A. pprof-rs via Criterion's profiler hook** | Per-benchmark flamegraphs; no external tooling; works on macOS without full Xcode; integrated into `cargo bench` | Adds dev-dependency; sampling-based (may miss very short functions) |
| B. `cargo-flamegraph` | Whole-binary profiling; no code changes | Requires `dtrace`/`xctrace` on macOS (needs full Xcode, not just CLT); produces one flamegraph for entire run; separate CLI tool |
| C. `samply` | Modern sampling profiler; Firefox Profiler UI | Separate tool; no Criterion integration; macOS support experimental |

**Chosen: A.** On macOS, `cargo-flamegraph` fails without full Xcode installed (`xctrace` is not available with Command Line Tools alone). pprof-rs uses signal-based CPU sampling that works cross-platform without external dependencies. The Criterion profiler hook means `cargo bench --bench store_bench -- --profile-time 5` generates targeted flamegraphs per benchmark function at `target/criterion/<name>/profile/flamegraph.svg`.

### 3. critcmp for baseline comparison

critcmp is the standard companion tool for Criterion. It reads Criterion's JSON baseline output from `target/criterion/` and produces a comparison table with percentage change and statistical significance.

**Workflow:**
```
main branch:    cargo bench -- --save-baseline main
feature branch: cargo bench -- --save-baseline feature
compare:        critcmp main feature
```

No alternatives were seriously considered — critcmp is the only tool that reads Criterion's baseline format directly. The alternative would be manual inspection of Criterion's HTML reports, which doesn't scale for CI.

### 4. Debug symbols in bench profile

Without debug info, flamegraphs show mangled or missing symbols. `.cargo/config.toml` sets `[profile.bench] debug = true` to preserve symbols in the optimized bench binary. This increases binary size but does not affect runtime performance (debug info is metadata, not executed code).

### 5. Makefile integration with install guards

Benchmark targets are added to the existing Makefile. Targets that require external CLI tools (`critcmp`, `cargo-flamegraph`) use a `check_cmd` macro that fails with a clear install instruction rather than a cryptic "command not found". Targets that only use `cargo bench` (which resolves Criterion as a dev-dependency) need no guard.

---

## Architecture

### Dependency graph

```
cargo bench
    │
    ▼
criterion (dev-dependency)
    │── statistical engine (warmup, measurement, outlier detection)
    │── HTML report generation (html_reports feature)
    │── JSON baseline output (--save-baseline)
    │── profiler hook API (.with_profiler())
    │
    ▼
pprof-rs (dev-dependency, criterion + flamegraph features)
    │── signal-based CPU sampling (no dtrace/xctrace needed)
    │── flamegraph SVG generation via inferno
    │── activated by --profile-time flag
    │
    ▼
target/criterion/
    ├── <bench_name>/
    │   ├── new/           # latest measurement data
    │   ├── base/          # saved baseline data
    │   ├── report/        # HTML report with charts
    │   └── profile/
    │       └── flamegraph.svg   # per-benchmark flamegraph
    └── report/            # index HTML across all benchmarks

critcmp (external CLI)
    │── reads target/criterion/ baselines
    │── produces text comparison table
    └── exit code for CI integration
```

### Benchmark binary structure

```rust
// benches/store_bench.rs
criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_create  // add more targets here
}
criterion_main!(benches);
```

The `PProfProfiler` is configured with a sampling frequency of 100 Hz. It activates only when `--profile-time <seconds>` is passed — normal `cargo bench` runs skip profiling entirely, so there is no overhead during regular benchmark measurement.

### Makefile targets

| Target | Command | Guard | Purpose |
|--------|---------|-------|---------|
| `bench` | `cargo bench` | none | Run all benchmarks with statistical analysis |
| `bench/save` | `cargo bench -- --save-baseline $(NAME)` | none | Save results as a named baseline |
| `bench/compare` | `critcmp $(BASE) $(NEW)` | `critcmp` installed | Compare two saved baselines |
| `bench/flamegraph` | `cargo bench --bench store_bench -- --profile-time 5` | none | Generate per-benchmark flamegraphs |

---

## Files changed

| File | Change |
|------|--------|
| `Cargo.toml` | Added `pprof = { version = "0.14", features = ["criterion", "flamegraph"] }` to dev-dependencies |
| `benches/store_bench.rs` | Configured `PProfProfiler` via `criterion_group!` macro |
| `.cargo/config.toml` | Created; `[profile.bench] debug = true` for flamegraph symbol resolution |
| `.gitignore` | Added `flamegraph.svg`, `perf.data`, `perf.data.old` |
| `Makefile` | Added `bench`, `bench/save`, `bench/compare`, `bench/flamegraph` targets with `check_cmd` guard |

---

## Usage

```bash
# Run benchmarks
make bench

# Save baseline before a change
make bench/save NAME=before

# Save baseline after a change
make bench/save NAME=after

# Compare (requires: cargo install critcmp)
make bench/compare BASE=before NEW=after

# Generate flamegraphs (opens in browser)
make bench/flamegraph
open target/criterion/store_create/profile/flamegraph.svg
```

---

## Limitations and future work

- **Minimal benchmark coverage.** Only `Store::new()` is benchmarked. Future work should add benchmarks for BTree insert/get/remove at various sizes, Table CRUD and batch operations, index lookups, and transaction commit cycles. This is deliberately separate from the harness setup.
- **No CI integration.** critcmp can return non-zero exit codes for regressions beyond a threshold (`critcmp --threshold 5`), which could gate PRs. Not configured yet.
- **pprof sampling resolution.** At 100 Hz, very short functions (< 10μs) may not appear in flamegraphs. The frequency can be increased but may introduce overhead.
- **Single benchmark binary.** All benchmarks live in `store_bench.rs`. If the file grows large, splitting into multiple `[[bench]]` targets (e.g., `btree_bench`, `table_bench`, `tx_bench`) would improve organization and allow targeted profiling.
