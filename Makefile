.PHONY: build test test/unit test/integration lint coverage coverage/vector clean bench bench/scaling bench/ycsb bench/ycsb/fjall bench/ycsb/rocksdb bench/ycsb/redb bench/ycsb/compare bench/multiwriter bench/multiwriter/rocksdb bench/multiwriter/fjall bench/multiwriter/clean bench/multiwriter/compare bench/smallbank bench/smallbank/persistent bench/save bench/compare bench/flamegraph bench/compare-engines perf/check perf/baseline

build:
	cargo build

test: lint test/unit test/integration

test/unit:
	cargo test --lib

test/integration:
	cargo test --test store_integration

lint:
	cargo clippy -- -D warnings

coverage:
	cargo llvm-cov --features persistence,fulltext --html
	@echo "Report: target/llvm-cov/html/index.html"

coverage/vector:
	cargo llvm-cov -p ultima-vector --features persistence --html --output-dir target/llvm-cov/vector
	@echo "Report: target/llvm-cov/vector/html/index.html"

clean:
	cargo clean

# Benchmarking

define check_cmd
	@command -v $(1) >/dev/null 2>&1 || { echo "Error: '$(1)' is not installed. Run: cargo install $(1)"; exit 1; }
endef

# First-party tier (default). Competitor baselines: bench/compare-engines.
bench:
	cargo bench --features bench-internals

bench/scaling:
	cargo bench --bench multiwriter_scaling_bench --features persistence

bench/ycsb:
	cargo bench --bench ycsb_bench

bench/ycsb/fjall:
	cargo bench -p compare-benches --bench ycsb_fjall_bench

bench/ycsb/rocksdb:
	cargo bench -p compare-benches --bench ycsb_rocksdb_bench

bench/ycsb/redb:
	cargo bench -p compare-benches --bench ycsb_redb_bench

# Run all YCSB suites across both durability tiers (non-durable + strict) with
# named baselines and compare side-by-side per tier.
#
# Requires ULTIMA_BENCH_DIR to point at a REAL disk-backed dir: on hosts where
# /tmp is a tmpfs (RAM), the default temp dir makes every "on-disk" engine
# in-memory with free fsyncs, silently invalidating the comparison. All engines
# commit per-op; ULTIMA_BENCH_DURABILITY selects the tier.
bench/ycsb/compare:
	$(call check_cmd,critcmp)
	@if [ -z "$(ULTIMA_BENCH_DIR)" ]; then \
	  echo "ERROR: ULTIMA_BENCH_DIR is not set — refusing to run."; \
	  echo "  Point it at a real disk-backed dir (NOT a tmpfs like /tmp):"; \
	  echo "    make bench/ycsb/compare ULTIMA_BENCH_DIR=\$$HOME/bench-disk"; \
	  echo "  Check with: df -T \$$ULTIMA_BENCH_DIR  (want ext4/xfs/etc, not tmpfs)."; \
	  exit 1; \
	fi
	@mkdir -p "$(ULTIMA_BENCH_DIR)"
	@for tier in nondurable strict; do \
	  echo "===== YCSB tier: $$tier ====="; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench --bench ycsb_bench -- --save-baseline ultima_$$tier || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_fjall_bench -- --save-baseline fjall_$$tier || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_rocksdb_bench -- --save-baseline rocksdb_$$tier || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_redb_bench -- --save-baseline redb_$$tier || exit 1; \
	done
	@echo "===== non-durable tier (WAL written, no fsync) ====="
	critcmp -g '(.+)/[^/]+' ultima_nondurable fjall_nondurable rocksdb_nondurable redb_nondurable
	@echo "===== strict tier (fsync per commit) ====="
	critcmp -g '(.+)/[^/]+' ultima_strict fjall_strict rocksdb_strict redb_strict

# Multi-writer contention benchmarks
# Use bench/multiwriter/clean to remove stale criterion data before comparing

bench/multiwriter/clean:
	rm -rf target/criterion/multiwriter*

bench/multiwriter:
	cargo bench --bench ycsb_multiwriter_bench

bench/multiwriter/rocksdb:
	cargo bench -p compare-benches --bench ycsb_multiwriter_rocksdb_bench

bench/multiwriter/fjall:
	cargo bench -p compare-benches --bench ycsb_multiwriter_fjall_bench

bench/multiwriter/compare:
	$(call check_cmd,critcmp)
	cargo bench --bench ycsb_multiwriter_bench -- --save-baseline mw-ultima
	cargo bench -p compare-benches --bench ycsb_multiwriter_rocksdb_bench -- --save-baseline mw-rocksdb
	cargo bench -p compare-benches --bench ycsb_multiwriter_fjall_bench -- --save-baseline mw-fjall
	critcmp mw-ultima mw-rocksdb mw-fjall

# SmallBank multi-table transactional benchmark

bench/smallbank:
	$(call check_cmd,critcmp)
	cargo bench --bench smallbank_bench
	critcmp smallbank -g '([^/]+)/[^/]+' -f smallbank

bench/smallbank/persistent:
	$(call check_cmd,critcmp)
	cargo bench --bench smallbank_bench --features persistence -- --save-baseline smallbank
	critcmp smallbank -g '([^/]+)/[^/]+' -f smallbank

# Multi-writer persistence benchmark (threaded commits with WAL)

bench/multiwriter/persistent:
	cargo bench --bench multiwriter_persistence_bench --features persistence

# Save a named baseline (usage: make bench/save NAME=main)
bench/save:
	cargo bench -- --save-baseline $(NAME)

# Compare two baselines (usage: make bench/compare BASE=main NEW=feature)
bench/compare:
	$(call check_cmd,critcmp)
	critcmp $(BASE) $(NEW)

# Generate per-benchmark flamegraphs via pprof (no Xcode/dtrace needed)
bench/flamegraph:
	cargo bench --bench ycsb_bench -- --profile-time 5
	@echo "Flamegraphs: target/criterion/*/profile/flamegraph.svg"

# Competitor baseline tier (RocksDB/Fjall/ReDB) — not part of `make bench`
bench/compare-engines:
	cargo bench -p compare-benches

# Perf regression gate (fitness binaries in --check mode, ~3-6 min total)
perf/check:
	cargo run -p ultima-autobench --bin smr-apply-microbench --release -- \
		--json --check --baseline autobench/baselines/smr-apply.json > /dev/null
	cargo run -p ultima-autobench --bin mw-commit-microbench --release -- \
		--json --check --baseline autobench/baselines/multiwriter-commit.json > /dev/null

# Re-record perf baselines (run only after a deliberate perf change lands)
perf/baseline:
	cargo run -p ultima-autobench --bin smr-apply-microbench --release -- \
		--json --write-baseline autobench/baselines/smr-apply.json > /dev/null
	cargo run -p ultima-autobench --bin mw-commit-microbench --release -- \
		--json --write-baseline autobench/baselines/multiwriter-commit.json > /dev/null
