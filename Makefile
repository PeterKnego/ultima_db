.PHONY: build test test/unit test/integration lint coverage clean bench bench/save bench/compare bench/flamegraph

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
	cargo llvm-cov --html
	@echo "Report: target/llvm-cov/html/index.html"

clean:
	cargo clean

# Benchmarking

define check_cmd
	@command -v $(1) >/dev/null 2>&1 || { echo "Error: '$(1)' is not installed. Run: cargo install $(1)"; exit 1; }
endef

bench:
	cargo bench

# Save a named baseline (usage: make bench/save NAME=main)
bench/save:
	cargo bench -- --save-baseline $(NAME)

# Compare two baselines (usage: make bench/compare BASE=main NEW=feature)
bench/compare:
	$(call check_cmd,critcmp)
	critcmp $(BASE) $(NEW)

# Generate per-benchmark flamegraphs via pprof (no Xcode/dtrace needed)
bench/flamegraph:
	cargo bench --bench store_bench -- --profile-time 5
	@echo "Flamegraphs: target/criterion/*/profile/flamegraph.svg"
