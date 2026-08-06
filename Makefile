.PHONY: build test test/unit test/integration test/lifecycle-races lint coverage coverage/vector clean bench bench/scaling bench/ycsb bench/ycsb/fjall bench/ycsb/rocksdb bench/ycsb/redb bench/ycsb/compare bench/wal-ab bench/smr-ycsb bench/fanout bench/smr-ab bench/fanout-micro bench/bulk-load/compare bench/multiwriter bench/multiwriter/rocksdb bench/multiwriter/fjall bench/multiwriter/clean bench/multiwriter/compare bench/smallbank bench/smallbank/persistent bench/save bench/compare bench/flamegraph bench/compare-engines perf/check perf/baseline consistency/elle consistency/elle-mutation test/formal-kernel test/formal-key-kernel formal/drift-check formal/cite-check formal/tla-smoke formal/tla-model formal/tla-modes formal/tla-manifest formal/tla-calibrate

build:
	cargo build

test: lint test/unit test/integration test/lifecycle-races

test/unit:
	cargo test --lib

test/integration:
	cargo test --test store_integration

# The table-lifecycle race matrix (tests/table_lifecycle_races.rs). Three of its
# 42 cells are `#[ignore]`d as questions awaiting a ruling — six tests, since
# each is observed under both isolation levels; their recorded behaviour is what
# the doc comments cite, so it has to be *run* or it rots.
# Nothing else passes `--ignored`, which is why this target exists separately
# from `test/integration`.
test/lifecycle-races:
	cargo test --features persistence,fulltext,metrics --test table_lifecycle_races
	cargo test --features persistence,fulltext,metrics --test table_lifecycle_races -- --ignored

# Formal verification tier (opt-in): differential test of the Lean-verified
# B-tree kernel port (formal/). Lean proofs: see formal/README.md.
test/formal-kernel:
	cargo test --manifest-path formal/kernel/Cargo.toml

# Key-encoding kernel port (formal/key_kernel). Lean proofs: see formal/README.md.
test/formal-key-kernel:
	cargo test --manifest-path formal/key_kernel/Cargo.toml

# TLA+ toolchain gate (S0) for the WAL crash-safety scout. Runs a tiny
# durability spec plus a canary that MUST fail — a checker that only ever
# reports success is indistinguishable from a broken one. See
# formal/tla/wal/README.md. State goes on real disk: /tmp here is tmpfs.
TLC_JAR ?= tools/tla/tla2tools-1.7.4.jar
TLC_METADIR ?= $(HOME)/tlc-states
TLC = java -XX:+UseSerialGC -Xmx2g -cp ../../../$(TLC_JAR) tlc2.TLC -metadir $(TLC_METADIR) -workers 2

# A canary must be asserted on TLC's exit code 12 (invariant violated), never
# on "nonzero". Measured on tla2tools 1.7.4 / TLC 2.19: 0 = clean,
# 12 = invariant violated, 150 = parse error, 151 = invariant undefined.
# A `|| echo "violated (expected)"` therefore reports success for a TYPO in
# the invariant name — the one gate whose whole purpose is that it cannot
# lie, quietly lying.
formal/tla-smoke:
	@mkdir -p $(TLC_METADIR)
	@cd formal/tla/wal && $(TLC) S0Smoke.tla > /dev/null; rc=$$?; \
	  if [ $$rc -ne 0 ]; then echo "S0Smoke FAILED — the gate spec should verify clean (TLC exit $$rc)"; exit 1; fi; \
	  echo "S0Smoke: no error (expected)"
	@cd formal/tla/wal && $(TLC) S0Canary.tla > /dev/null 2>&1; rc=$$?; \
	  if [ $$rc -ne 12 ]; then \
	    echo "S0Canary FAILED — TLC exit $$rc, expected 12 (invariant violated)."; \
	    echo "  0 = TLC did not catch a broken invariant; 150/151 = parse error or"; \
	    echo "  undefined invariant, i.e. the gate checked nothing. Either way it is lying."; \
	    exit 1; \
	  fi; \
	  echo "S0Canary: invariant violated, TLC exit 12 (expected — TLC discriminates)"

# S1 model: the Standalone commit pipeline (WalCrash.tla). Each baseline is
# paired with a vacuity canary that runs FIRST and must go red — a model where
# nothing ever promotes verifies every safety property trivially, which is how
# a checking effort produces confident, meaningless greens.
#
# Both writer modes are checked. SingleWriter is strictly serial by
# construction (the writer slot is held through the fsync wait), so it cannot
# reach parking-while-another-writer-proceeds, batched fsync, or the version
# bump — MultiWriter is the config that exercises the protocol.
#
# Three baselines, each preceded by a canary that MUST go red. The third
# (CoalescedPrealloc) exists because it is the only sink for which
# Store::recover passes tail_tolerant = true, and therefore the only config
# that reaches the tolerant half of scan_wal.
#
# The last run is not a canary but an OWED PROPERTY, also expected red:
# StrictScanErrLosesDurableAck records a known gap (a torn tail costs a
# strict-scan store its whole log, durable acked commits included) so that
# "we know about this" is re-checked rather than remembered.
formal/tla-model:
	@mkdir -p $(TLC_METADIR)
	@$(MAKE) --no-print-directory formal/tla-manifest
	@cd formal/tla/wal && $(TLC) -config Vacuity.cfg WalCrash.tla > /dev/null 2>&1; rc=$$?; \
	  if [ $$rc -ne 12 ]; then \
	    echo "vacuity canary (SingleWriter) FAILED — TLC exit $$rc, expected 12."; \
	    echo "  0 = nothing promotes, the model is inert and every green below is"; \
	    echo "  meaningless; 150/151 = the canary checked nothing."; \
	    exit 1; \
	  fi; \
	  echo "vacuity canary (SingleWriter): violated, TLC exit 12 (expected)"
	@cd formal/tla/wal && $(TLC) -config WalCrash.cfg WalCrash.tla > /dev/null; rc=$$?; \
	  if [ $$rc -ne 0 ]; then echo "WalCrash baseline (SingleWriter) FAILED (TLC exit $$rc)"; exit 1; fi; \
	  echo "WalCrash baseline (SingleWriter): no error (expected)"
	@cd formal/tla/wal && $(TLC) -config VacuityMW.cfg WalCrash.tla > /dev/null 2>&1; rc=$$?; \
	  if [ $$rc -ne 12 ]; then \
	    echo "vacuity canary (MultiWriter) FAILED — TLC exit $$rc, expected 12."; \
	    exit 1; \
	  fi; \
	  echo "vacuity canary (MultiWriter): violated, TLC exit 12 (expected)"
	@cd formal/tla/wal && $(TLC) -config WalCrashMW.cfg WalCrash.tla > /dev/null; rc=$$?; \
	  if [ $$rc -ne 0 ]; then echo "WalCrash baseline (MultiWriter) FAILED (TLC exit $$rc)"; exit 1; fi; \
	  echo "WalCrash baseline (MultiWriter): no error (expected)"
	@cd formal/tla/wal && $(TLC) -config VacuityCrash.cfg WalCrash.tla > /dev/null 2>&1; rc=$$?; \
	  if [ $$rc -ne 12 ]; then \
	    echo "crash canary (SingleWriter) FAILED — TLC exit $$rc, expected 12."; \
	    echo "  0 = no behaviour crashes and recovers, so RecoverySound is checked"; \
	    echo "  over zero crash behaviours; 150/151 = the canary checked nothing."; \
	    exit 1; \
	  fi; \
	  echo "crash canary (SingleWriter): violated, TLC exit 12 (expected)"
	@cd formal/tla/wal && $(TLC) -config VacuityCrashMW.cfg WalCrash.tla > /dev/null 2>&1; rc=$$?; \
	  if [ $$rc -ne 12 ]; then \
	    echo "crash canary (MultiWriter) FAILED — TLC exit $$rc, expected 12."; exit 1; \
	  fi; \
	  echo "crash canary (MultiWriter): violated, TLC exit 12 (expected)"
	@cd formal/tla/wal && $(TLC) -config VacuityCrashPrealloc.cfg WalCrash.tla > /dev/null 2>&1; rc=$$?; \
	  if [ $$rc -ne 12 ]; then \
	    echo "crash canary (CoalescedPrealloc) FAILED — TLC exit $$rc, expected 12."; exit 1; \
	  fi; \
	  echo "crash canary (CoalescedPrealloc): violated, TLC exit 12 (expected)"
	@cd formal/tla/wal && $(TLC) -config WalCrashPrealloc.cfg WalCrash.tla > /dev/null; rc=$$?; \
	  if [ $$rc -ne 0 ]; then echo "WalCrash baseline (CoalescedPrealloc) FAILED (TLC exit $$rc)"; exit 1; fi; \
	  echo "WalCrash baseline (CoalescedPrealloc): no error (expected)"
	@cd formal/tla/wal && $(TLC) -config StrictScanErr.cfg WalCrash.tla > /dev/null 2>&1; rc=$$?; \
	  if [ $$rc -ne 12 ]; then \
	    echo "owed property StrictScanErrLosesDurableAck FAILED — TLC exit $$rc, expected 12."; \
	    echo "  0 = either the strict-scan error path stopped being reachable (the model"; \
	    echo "  rotted) or the behaviour changed — in which case write the real property"; \
	    echo "  and delete this one; 150/151 = the check checked nothing."; \
	    exit 1; \
	  fi; \
	  echo "owed property StrictScanErrLosesDurableAck: violated, TLC exit 12 (expected — known gap, still there)"
	@$(MAKE) --no-print-directory formal/tla-modes

# The Durability x WalWrite matrix (Task 3): every combination the Standalone
# pipeline actually offers, each paired with at least one canary that must go
# red. The expected exit code is written down PER CONFIG rather than inferred
# from the filename — a naming convention would silently reclassify a config
# the day someone renames it, and the whole point of these gates is that they
# cannot quietly pass. 0 = clean, 12 = invariant violated; 150 (parse error)
# and 151 (undefined invariant) fail either way, which is the hole this
# discipline exists to close.
#
# ConsistentInline appears only with SingleWriter: Store::new rejects it under
# MultiWriter (task38), so a MultiWriter config would model a store that
# cannot be constructed. That restriction is now enforced by an ASSUME in
# WalCrash.tla, not by this comment — a comment cannot stop a config from
# quietly coming back exit 0 over an unconstructible store. A violated ASSUME
# is TLC exit 10, which fails every entry in this table whatever it expects.
#
# modes/ConsistentPrealloc3.cfg is MaxCommits = 3 and is the only config that
# reaches an extend from a non-empty log (the production shape). ~1.4 s.
#
# mutations/ is the CALIBRATION battery (Tasks 4-5): each config re-runs a
# committed baseline with the MUTATION constant flipped. M1–M3 re-create the
# three lost-update interleavings that actually shipped
# (docs/tasks/task15_three_phase_consistent_persistence.md, "Promotion
# ordering"); M4–M5 re-create the two preallocation subtleties task37 is
# built around — M4 scans a preallocated WAL strictly, so a legal torn tail
# aborts recovery (task37 §7), and M5 writes a batch into a freshly extended
# region whose size was never sync_all'd (task37 §4 invariant 2). They must
# go RED. A model that verifies clean but cannot re-find the bugs it was
# built for produces confident greens that mean nothing, so these are gated
# exactly like the canaries — exit 12, not "nonzero". Mutations are
# constant-gated inside WalCrash.tla and never forked .tla copies: a forked
# copy drifts from the baseline and silently stops testing the real model.
#
# Several mutation configs report the SHALLOWEST counterexample, which is not
# always the documented symptom. M2Fork / M3Dup / M4Abort / M5Strand each pin
# one symptom in isolation, and each has a same-bound MUTATION = "NONE"
# control: mutations/CalibrationControl3.cfg for the first two,
# modes/ConsistentPreallocScanErrCheck.cfg for M4Abort,
# modes/ConsistentPrealloc3.cfg for M5Strand, modes/ConsistentPrealloc.cfg for
# M6 and M7. Re-bounding or deleting one of those silently removes the
# evidence while the gate stays green.
#
# STATE COUNTS ARE NOT TRIPWIRES FOR THE RED CONFIGS. TLC halts at the first
# counterexample, so with -workers 2 the reported counts vary run to run --
# widely, and do not treat any observed range as a band: M1 came back 221,
# 246, 249, 251 and 256 across five runs, against a deterministic 238 at
# -workers 1. The trace DEPTH is stable and is what the Task 4 report records.
TLA_MODES = \
  modes/ConsistentFsWriteCanary.cfg:12 \
  modes/ConsistentFsWrite.cfg:0 \
  modes/ConsistentCoalescedCanary.cfg:12 \
  modes/ConsistentCoalesced.cfg:0 \
  modes/ConsistentPreallocCanary.cfg:12 \
  modes/ConsistentPreallocExtendCanary.cfg:12 \
  modes/ConsistentPreallocTornTailCanary.cfg:12 \
  modes/ConsistentPrealloc.cfg:0 \
  modes/ConsistentPreallocScanErrCheck.cfg:0 \
  modes/ConsistentPrealloc3Canary.cfg:12 \
  modes/ConsistentPrealloc3LiveLogCanary.cfg:12 \
  modes/ConsistentPrealloc3ChunkCanary.cfg:12 \
  modes/ConsistentPrealloc3.cfg:0 \
  modes/InlineFsWriteCanary.cfg:12 \
  modes/InlineFsWrite.cfg:0 \
  modes/InlinePreallocCanary.cfg:12 \
  modes/InlinePreallocExtendCanary.cfg:12 \
  modes/InlinePrealloc.cfg:0 \
  modes/EventualFsWriteCanary.cfg:12 \
  modes/EventualFsWriteLossCanary.cfg:12 \
  modes/EventualFsWrite.cfg:0 \
  modes/ConsistentAckKeptCheck.cfg:0 \
  mutations/M1.cfg:12 \
  mutations/M2.cfg:12 \
  mutations/M2Fork.cfg:12 \
  mutations/M3.cfg:12 \
  mutations/M3Dup.cfg:12 \
  mutations/M4.cfg:12 \
  mutations/M4Abort.cfg:12 \
  mutations/M5.cfg:12 \
  mutations/M5Strand.cfg:12 \
  mutations/M6.cfg:12 \
  mutations/M7.cfg:12 \
  mutations/CalibrationControl3.cfg:0

formal/tla-modes:
	@mkdir -p $(TLC_METADIR)
	@cd formal/tla/wal && for pair in $(TLA_MODES); do \
	  cfg=$${pair%:*}; want=$${pair##*:}; \
	  $(TLC) -config $$cfg WalCrash.tla > /dev/null 2>&1; rc=$$?; \
	  if [ $$rc -ne $$want ]; then \
	    echo "$$cfg FAILED — TLC exit $$rc, expected $$want."; \
	    echo "  12 expected but 0 seen = the canary's mechanism is unreachable and"; \
	    echo "  the paired baseline is green over dead code; 0 expected but 12 seen"; \
	    echo "  = a real property broke; 150/151 = the run checked nothing."; \
	    exit 1; \
	  fi; \
	  echo "$$cfg: TLC exit $$rc (expected $$want)"; \
	done

# The calibration manifest (Task 6). Until now, the only thing standing
# between M2's and M3's documented symptoms and oblivion was a PROSE COMMENT
# above TLA_MODES saying "deleting one, or re-bounding it, silently removes
# that evidence". M2.cfg and M3.cfg keep passing without M2Fork.cfg and
# M3Dup.cfg -- they just stop matching the shipped bug, which is the entire
# claim the calibration makes. A comment cannot fail a build.
#
# Each row pins the four things that carry a mutation's evidence:
#
#   cfg : invariant : MUTATION : MaxCommits : expected TLC exit code
#
# and the check asserts all five, plus that the config is still listed in
# TLA_MODES at the same exit code. So deleting the file fails; renaming it
# fails; dropping it from TLA_MODES fails; swapping its target invariant
# fails; and re-bounding MaxCommits 3 -> 2 fails -- which is the silent one,
# because M2Fork/M3Dup/M5Strand at MaxCommits = 2 would still be *green* while
# checking a state space that cannot reach the symptom (see "Which config
# carries which mechanism" in formal/tla/wal/README.md).
#
# The invariant named per row is the evidence-carrying one, not the whole
# INVARIANT list: baseline-shaped configs declare five and only one is the
# reason that row exists.
#
# TWO THINGS THIS DOES NOT CATCH -- see RESULTS.md §7, "What the manifest
# still does not catch", before trusting it further than it goes:
#   1. It checks the target invariant is DECLARED, not that it is the one TLC
#      reported. ADDING a second INVARIANT line to a single-invariant config
#      (M2Fork, M3Dup, M5Strand, M4Abort) keeps this green AND keeps exit 12
#      while the red moves to the added invariant. If you are here to add an
#      invariant to one of those four, that is the failure mode.
#   2. M6/M7 pin RecoverySound, which most baselines also declare; their real
#      evidence is a CLAUSE of it, which no config-level check can express.
#
# Controls are here too, at exit 0. A mutation row proving "violated" means
# nothing without a same-bound MUTATION = "NONE" run proving the bound itself
# is not what went red.
TLA_CALIB = \
  mutations/M1.cfg:PromotionFaithful:M1:2:12 \
  mutations/M2.cfg:PromotionFaithful:M2:2:12 \
  mutations/M2Fork.cfg:ForkFromPromotePredecessor:M2:3:12 \
  mutations/M3.cfg:PromotionFaithful:M3:2:12 \
  mutations/M3Dup.cfg:NoDupLive:M3:3:12 \
  mutations/M4.cfg:TailTolerance:M4:2:12 \
  mutations/M4Abort.cfg:StrictScanErrLosesDurableAck:M4:2:12 \
  mutations/M5.cfg:PreallocInvariant:M5:2:12 \
  mutations/M5Strand.cfg:NoAckLossAfterLiveExtend:M5:3:12 \
  mutations/M6.cfg:RecoverySound:M6:2:12 \
  mutations/M7.cfg:RecoverySound:M7:2:12 \
  mutations/CalibrationControl3.cfg:ForkFromPromotePredecessor:NONE:3:0 \
  modes/ConsistentPrealloc.cfg:RecoverySound:NONE:2:0 \
  modes/ConsistentPrealloc3.cfg:NoAckLossAfterLiveExtend:NONE:3:0 \
  modes/ConsistentPreallocScanErrCheck.cfg:StrictScanErrLosesDurableAck:NONE:2:0

# Structural half: no TLC, runs in well under a second, and is therefore
# wired into formal/tla-model so the guard rides on the target people already
# run rather than on one they have to remember.
formal/tla-manifest:
	@cd formal/tla/wal && for spec in $(TLA_CALIB); do \
	  cfg=`echo $$spec | cut -d: -f1`; inv=`echo $$spec | cut -d: -f2`; \
	  mut=`echo $$spec | cut -d: -f3`; bound=`echo $$spec | cut -d: -f4`; \
	  want=`echo $$spec | cut -d: -f5`; \
	  if [ ! -f "$$cfg" ]; then \
	    echo "calibration manifest FAILED — $$cfg is missing."; \
	    echo "  It carries the MUTATION = \"$$mut\" evidence at MaxCommits = $$bound."; \
	    echo "  If the mutation is genuinely retired, delete its manifest row too"; \
	    echo "  and say so in formal/tla/wal/RESULTS.md — do not just delete the file."; \
	    exit 1; \
	  fi; \
	  if ! grep -qE "^INVARIANT[[:space:]]+$$inv[[:space:]]*$$" "$$cfg"; then \
	    echo "calibration manifest FAILED — $$cfg no longer declares INVARIANT $$inv."; \
	    echo "  That invariant is the evidence this config exists to carry."; \
	    exit 1; \
	  fi; \
	  if ! grep -qE "^[[:space:]]*MUTATION[[:space:]]*=[[:space:]]*\"$$mut\"[[:space:]]*$$" "$$cfg"; then \
	    echo "calibration manifest FAILED — $$cfg is no longer MUTATION = \"$$mut\"."; \
	    exit 1; \
	  fi; \
	  if ! grep -qE "^[[:space:]]*MaxCommits[[:space:]]*=[[:space:]]*$$bound[[:space:]]*$$" "$$cfg"; then \
	    echo "calibration manifest FAILED — $$cfg is no longer MaxCommits = $$bound."; \
	    echo "  Re-bounding is the SILENT failure: the config stays green while"; \
	    echo "  checking a state space too small to reach the symptom."; \
	    exit 1; \
	  fi; \
	  case " $(TLA_MODES) " in \
	    *" $$cfg:$$want "*) ;; \
	    *) echo "calibration manifest FAILED — $$cfg is not in TLA_MODES at exit $$want."; \
	       echo "  A calibration config outside TLA_MODES is never run by any gate."; \
	       exit 1;; \
	  esac; \
	  echo "$$cfg: INVARIANT $$inv, MUTATION \"$$mut\", MaxCommits $$bound, TLA_MODES:$$want (ok)"; \
	done

# Behavioural half: re-run every mutation and every control and assert the
# exit code. Same discipline as the canaries -- exact code, never "nonzero",
# because 150 (parse error) and 151 (undefined invariant) are nonzero too and
# mean the run checked nothing. Overlaps formal/tla-modes by design: this is
# the standing guard for "the model is still discriminating", runnable on its
# own without the full matrix.
formal/tla-calibrate:
	@mkdir -p $(TLC_METADIR)
	@$(MAKE) --no-print-directory formal/tla-manifest
	@cd formal/tla/wal && for spec in $(TLA_CALIB); do \
	  cfg=`echo $$spec | cut -d: -f1`; mut=`echo $$spec | cut -d: -f3`; \
	  want=`echo $$spec | cut -d: -f5`; \
	  $(TLC) -config $$cfg WalCrash.tla > /dev/null 2>&1; rc=$$?; \
	  if [ $$rc -ne $$want ]; then \
	    echo "$$cfg FAILED — TLC exit $$rc, expected $$want (MUTATION = \"$$mut\")."; \
	    echo "  12 expected but 0 seen = the model STOPPED BEING DISCRIMINATING:"; \
	    echo "  it can no longer re-find a bug it was calibrated against, so every"; \
	    echo "  green verdict elsewhere is a green with nothing behind it."; \
	    echo "  0 expected but 12 seen = a control broke; the bound, not the"; \
	    echo "  mutation, is what the neighbouring red was measuring."; \
	    echo "  150/151 = the run checked nothing."; \
	    exit 1; \
	  fi; \
	  echo "$$cfg: TLC exit $$rc (expected $$want)"; \
	done

# Drift guard: fail if src/btree.rs or src/primary_key.rs changed without a
# matching formal/ update (formal/kernel/ and formal/key_kernel/ respectively).
# Override for changes outside the verified surface: ACK_NO_FORMAL=1.
formal/drift-check:
	formal/scripts/check-drift.sh

# Cite guard: verify that every src/*.rs:LINE cite in formal/tla/wal/ still
# points at what it claims, via the expectation tokens in
# formal/tla/wal/cite-anchors.tsv. Complements formal/drift-check, which only
# knows that *something* under formal/ changed. Needs nothing but python3 and
# git; runs in well under a second.
formal/cite-check:
	formal/scripts/check-cites.py

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

# UltimaDB-only YCSB A/B: eventual-durability Standalone (WAL, async fsync) vs
# checkpoint-only SMR (no per-commit WAL). Single-writer. Isolates the cost of the
# per-commit WAL serialize + background-thread machinery. Needs a real disk dir.
bench/smr-ycsb: ## SMR (checkpoint-only) vs Eventual YCSB A/B — single-writer
	$(call check_cmd,critcmp)
	@if [ -z "$(ULTIMA_BENCH_DIR)" ]; then \
	  echo "ERROR: ULTIMA_BENCH_DIR is not set — refusing to run."; \
	  echo "  Point it at a real disk-backed dir (NOT a tmpfs like /tmp):"; \
	  echo "    make bench/smr-ycsb ULTIMA_BENCH_DIR=\$$HOME/bench-disk"; \
	  exit 1; \
	fi
	@mkdir -p "$(ULTIMA_BENCH_DIR)"
	ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_DURABILITY=nondurable \
	  cargo bench --bench ycsb_bench -- --save-baseline smr_eventual
	ULTIMA_BENCH_DIR="$(ULTIMA_BENCH_DIR)" ULTIMA_BENCH_SMR=1 \
	  cargo bench --bench ycsb_bench -- --save-baseline smr_checkpoint_only
	@echo "===== SMR vs Eventual YCSB (lower = better) ====="
	critcmp smr_eventual smr_checkpoint_only

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
