.PHONY: build test test/unit test/integration lint coverage clean

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
