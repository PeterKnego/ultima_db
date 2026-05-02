.PHONY: build test test/unit test/integration lint coverage coverage/vector clean bench bench/ycsb bench/ycsb/fjall bench/ycsb/rocksdb bench/ycsb/redb bench/ycsb/compare bench/multiwriter bench/multiwriter/rocksdb bench/multiwriter/fjall bench/multiwriter/clean bench/multiwriter/compare bench/smallbank bench/smallbank/persistent bench/save bench/compare bench/flamegraph

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

coverage/journal:
	cargo llvm-cov -p ultima-journal --html --output-dir target/llvm-cov/journal
	@echo "Report: target/llvm-cov/journal/html/index.html"

clean:
	cargo clean

# Benchmarking

define check_cmd
	@command -v $(1) >/dev/null 2>&1 || { echo "Error: '$(1)' is not installed. Run: cargo install $(1)"; exit 1; }
endef

bench:
	cargo bench

bench/ycsb:
	cargo bench --bench ycsb_bench

bench/ycsb/fjall:
	cargo bench --bench ycsb_fjall_bench

bench/ycsb/rocksdb:
	cargo bench --bench ycsb_rocksdb_bench

bench/ycsb/redb:
	cargo bench --bench ycsb_redb_bench

# Run all YCSB suites with named baselines and compare side-by-side
bench/ycsb/compare:
	$(call check_cmd,critcmp)
	cargo bench --bench ycsb_bench -- --save-baseline ultima
	cargo bench --bench ycsb_fjall_bench -- --save-baseline fjall
	cargo bench --bench ycsb_rocksdb_bench -- --save-baseline rocksdb
	cargo bench --bench ycsb_redb_bench -- --save-baseline redb
	critcmp -g '(.+)/[^/]+' ultima fjall rocksdb redb

# Multi-writer contention benchmarks
# Use bench/multiwriter/clean to remove stale criterion data before comparing

bench/multiwriter/clean:
	rm -rf target/criterion/multiwriter*

bench/multiwriter:
	cargo bench --bench ycsb_multiwriter_bench

bench/multiwriter/rocksdb:
	cargo bench --bench ycsb_multiwriter_rocksdb_bench

bench/multiwriter/fjall:
	cargo bench --bench ycsb_multiwriter_fjall_bench

bench/multiwriter/compare:
	$(call check_cmd,critcmp)
	cargo bench --bench ycsb_multiwriter_bench -- --save-baseline mw-ultima
	cargo bench --bench ycsb_multiwriter_rocksdb_bench -- --save-baseline mw-rocksdb
	cargo bench --bench ycsb_multiwriter_fjall_bench -- --save-baseline mw-fjall
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
