.PHONY: build test test/unit test/integration lint coverage coverage/vector clean bench bench/scaling bench/ycsb bench/ycsb/fjall bench/ycsb/rocksdb bench/ycsb/redb bench/ycsb/compare bench/wal-ab bench/fanout bench/smr-ab bench/fanout-micro bench/bulk-load/compare bench/multiwriter bench/multiwriter/rocksdb bench/multiwriter/fjall bench/multiwriter/clean bench/multiwriter/compare bench/smallbank bench/smallbank/persistent bench/save bench/compare bench/flamegraph bench/compare-engines perf/check perf/baseline consistency/elle consistency/elle-mutation test/formal-kernel formal/drift-check

build:
	cargo build

test: lint test/unit test/integration

test/unit:
	cargo test --lib

test/integration:
	cargo test --test store_integration

# Formal verification tier (opt-in): differential test of the Lean-verified
# B-tree kernel port (formal/). Lean proofs: see formal/README.md.
test/formal-kernel:
	cargo test --manifest-path formal/kernel/Cargo.toml

# Drift guard: fail if src/btree.rs changed without a matching formal/ update.
# Override for changes outside the verified insert/get path: ACK_NO_FORMAL=1.
formal/drift-check:
	formal/scripts/check-drift.sh

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
# The UltimaDB STRICT arm runs `standalone_fast` (ULTIMA_BENCH_INLINE=1 +
# ULTIMA_BENCH_PREALLOC=1 → ConsistentInline + CoalescedPrealloc) — the shippable
# fast durable single-writer preset the competitors are compared against. Without
# these the arm falls back to the ~3.8×-slower Consistent+Coalesced default,
# understating UltimaDB (the wal-ab A/B sweep still exercises that default arm).
# The non-durable arm stays Eventual to match the competitors' no-fsync path.
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
	  if [ "$$tier" = strict ]; then UD_FAST="ULTIMA_BENCH_INLINE=1 ULTIMA_BENCH_PREALLOC=1"; else UD_FAST=""; fi; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier env $$UD_FAST cargo bench --bench ycsb_bench -- --save-baseline ultima_$$tier || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_fjall_bench -- --save-baseline fjall_$$tier || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_rocksdb_bench -- --save-baseline rocksdb_$$tier || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_redb_bench -- --save-baseline redb_$$tier || exit 1; \
	done
	@echo "===== non-durable tier (WAL written, no fsync) ====="
	critcmp -g '(.+)/[^/]+' ultima_nondurable fjall_nondurable rocksdb_nondurable redb_nondurable
	@echo "===== strict tier (fsync per commit) ====="
	critcmp -g '(.+)/[^/]+' ultima_strict fjall_strict rocksdb_strict redb_strict

# WAL/durability A/B sweep (ultima-only): the standalone_fast toggles on real
# NVMe. Baselines: nondurable (Eventual), strict-consistent (bg-thread fsync),
# strict-inline (off-lock fsync), strict-standalone_fast (inline + prealloc).
# ycsb_bench reads ULTIMA_BENCH_{DURABILITY,INLINE,PREALLOC} (benches/ycsb_bench.rs).
# Requires ULTIMA_BENCH_DIR set (empty is refused); point it at a real disk-backed
# dir, not a tmpfs (same guard as bench/ycsb/compare).
bench/wal-ab:
	$(call check_cmd,critcmp)
	@if [ -z "$(ULTIMA_BENCH_DIR)" ]; then \
	  echo "ERROR: ULTIMA_BENCH_DIR is not set — refusing to run."; \
	  echo "  Point it at a real disk-backed dir (NOT a tmpfs like /tmp):"; \
	  echo "    make bench/wal-ab ULTIMA_BENCH_DIR=\$$HOME/bench-disk"; \
	  exit 1; \
	fi
	@mkdir -p "$(ULTIMA_BENCH_DIR)"
	ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=nondurable \
	  cargo bench --bench ycsb_bench -- --save-baseline wal_nondurable
	ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=strict \
	  cargo bench --bench ycsb_bench -- --save-baseline wal_strict_consistent
	ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=strict ULTIMA_BENCH_INLINE=1 \
	  cargo bench --bench ycsb_bench -- --save-baseline wal_strict_inline
	ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=strict ULTIMA_BENCH_INLINE=1 ULTIMA_BENCH_PREALLOC=1 \
	  cargo bench --bench ycsb_bench -- --save-baseline wal_strict_standalone_fast
	@echo "===== WAL A/B (lower = better) ====="
	critcmp wal_nondurable wal_strict_consistent wal_strict_inline wal_strict_standalone_fast

# B-tree fanout (T) A/B sweep. Pure in-memory (no ULTIMA_BENCH_DIR / disk needed):
# rewrites the compile-time T const + rebuilds per value, times get/insert/remove
# at 1M random keys, prints a table normalized to T=32. See scripts/fanout_ab.sh.
bench/fanout: ## B-tree fanout (T) A/B sweep — get/insert/remove @1M random keys
	scripts/fanout_ab.sh

# B-tree fanout (T) A/B on the CONTENDED SMR-apply + read-under-load workload
# (the perf-gate regime). Complements bench/fanout (uncontended bulk ops): here
# concurrent make_mut CoW-clones make bigger nodes costlier. See scripts/smr_apply_ab.sh.
bench/smr-ab: ## SMR-apply/read-p99 fanout (T) A/B — the contended perf-gate workload
	scripts/smr_apply_ab.sh

# B-tree fanout (T) read-vs-write ASYMMETRY sweep: get/insert/update/remove in
# BOTH the warm (CoW-clone, ~T/lnT) and cold (in-place, U-shape) regimes over
# T in 8..256. Checks the asymmetry formula + corollary. See scripts/fanout_micro_ab.sh.
bench/fanout-micro: ## Fanout (T) read-vs-write asymmetry — warm+cold, get/insert/update/remove @T 8..256
	scripts/fanout_micro_ab.sh

# Bulk-load ingest comparison (build empty db of N records). Five arms:
# UltimaDB insert_batch, UltimaDB Store::bulk_load, RocksDB, Fjall, ReDB.
# Same ULTIMA_BENCH_DIR real-disk guard as bench/ycsb/compare.
bench/bulk-load/compare:
	$(call check_cmd,critcmp)
	@if [ -z "$(ULTIMA_BENCH_DIR)" ]; then \
	  echo "ERROR: ULTIMA_BENCH_DIR is not set — refusing to run."; \
	  echo "  Point it at a real disk-backed dir (NOT a tmpfs like /tmp):"; \
	  echo "    make bench/bulk-load/compare ULTIMA_BENCH_DIR=\$$HOME/bench-disk"; \
	  exit 1; \
	fi
	@mkdir -p "$(ULTIMA_BENCH_DIR)"
	@for tier in nondurable strict; do \
	  echo "===== bulk-load tier: $$tier ====="; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench --bench ycsb_bulk_load_ultima_batch_bench  -- --save-baseline ub_batch_$$tier  || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench --bench ycsb_bulk_load_ultima_sorted_bench -- --save-baseline ub_sorted_$$tier || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_bulk_load_rocksdb_bench -- --save-baseline ub_rocksdb_$$tier || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_bulk_load_fjall_bench   -- --save-baseline ub_fjall_$$tier   || exit 1; \
	  ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=$$tier cargo bench -p compare-benches --bench ycsb_bulk_load_redb_bench    -- --save-baseline ub_redb_$$tier    || exit 1; \
	done
	@echo "===== non-durable tier (build cost) ====="
	critcmp ub_batch_nondurable ub_sorted_nondurable ub_rocksdb_nondurable ub_fjall_nondurable ub_redb_nondurable
	@echo "===== strict tier (one fsync at end of load) ====="
	critcmp ub_batch_strict ub_sorted_strict ub_rocksdb_strict ub_fjall_strict ub_redb_strict

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

# Transactional consistency check (Elle list-append via vendored elle-cli,
# needs java) — opt-in tier, not part of `make test`. Tune via ELLE_ARGS. See task45.
# Three passes: point reads, a scan-heavy pass, then a predicate (index) pass.
# Each pass must satisfy its isolation claim (SSI point/scan/predicate read-set).
ELLE_DIR ?= /tmp/ultima-elle
ELLE_SCAN_RATIO ?= 0.5
ELLE_PREDICATE_RATIO ?= 0.5
ELLE_BUCKETS ?= 4
consistency/elle:
	cargo run --release -p ultima-autobench --bin elle-history -- \
		--isolation si $(ELLE_ARGS) --out $(ELLE_DIR)/point-si/history.edn
	cargo run --release -p ultima-autobench --bin elle-history -- \
		--isolation serializable $(ELLE_ARGS) --out $(ELLE_DIR)/point-ser/history.edn
	scripts/elle_check.sh $(ELLE_DIR)/point-si/history.edn $(ELLE_DIR)/point-ser/history.edn
	cargo run --release -p ultima-autobench --bin elle-history -- \
		--isolation si --scan-ratio $(ELLE_SCAN_RATIO) $(ELLE_ARGS) --out $(ELLE_DIR)/scan-si/history.edn
	cargo run --release -p ultima-autobench --bin elle-history -- \
		--isolation serializable --scan-ratio $(ELLE_SCAN_RATIO) $(ELLE_ARGS) --out $(ELLE_DIR)/scan-ser/history.edn
	scripts/elle_check.sh $(ELLE_DIR)/scan-si/history.edn $(ELLE_DIR)/scan-ser/history.edn
	cargo run --release -p ultima-autobench --bin elle-history -- \
		--isolation si --predicate-ratio $(ELLE_PREDICATE_RATIO) --buckets $(ELLE_BUCKETS) $(ELLE_ARGS) --out $(ELLE_DIR)/pred-si/history.edn
	cargo run --release -p ultima-autobench --bin elle-history -- \
		--isolation serializable --predicate-ratio $(ELLE_PREDICATE_RATIO) --buckets $(ELLE_BUCKETS) $(ELLE_ARGS) --out $(ELLE_DIR)/pred-ser/history.edn
	scripts/elle_check.sh $(ELLE_DIR)/pred-si/history.edn $(ELLE_DIR)/pred-ser/history.edn

# Mutation test: inject known bugs into the commit path and confirm Elle catches
# them (opt-in; builds ultima-db with the mutation-testing feature). See task47.
consistency/elle-mutation:
	scripts/elle_mutation.sh

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
