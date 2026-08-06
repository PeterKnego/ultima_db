# Task 60 — in-flight WAL fault injection

**Delivered:** three fault-injection test binaries — `tests/wal_fault_failed_extend.rs`,
`tests/wal_fault_fsync.rs`, `tests/wal_fault_torn_tail.rs` — plus three new
variants on the existing task47 `mutation-testing` seam, run by
`make test/wal-faults` and by two CI steps.

**Design history:** `docs/superpowers/specs/2026-08-06-wal-inflight-fault-injection-design.md`
and `docs/superpowers/plans/2026-08-06-wal-inflight-fault-injection.md`.

---

## 1. Why this suite exists

### The distinction is *when*, not *what*

`tests/corruption_recovery.rs` is not inadequate and this suite does not
replace it. It has **11 real tests** — truncated tails, zero tails, garbage
tails, bit-flips in the first and last entries, three checkpoint-corruption
cases — and they cover a different thing:

- **Post-hoc.** Corrupt a *closed* file, then recover. No seam needed: the test
  just writes bytes. This is what `corruption_recovery.rs` does, through one
  config helper, all 11 at `Durability::Eventual` + `WalWrite::PerEntry`
  (`tests/corruption_recovery.rs:34-35`).
- **In-flight.** A syscall fails *partway through an operation*, while the sink
  still holds in-memory state that the file no longer matches.

The second is **structurally unreachable** from the first. You cannot edit a
closed file into a state where `PreallocFileSink::capacity` disagrees with
`metadata().len()`, because the disagreement lives in memory that no longer
exists once the file is closed. That is not a gap in the old suite; it is a
different failure mode with no way in.

### F2 is the canonical case, and it shipped unproven

`preallocate_to` (`src/wal.rs:628`) zero-fills `[from, to)` then `sync_all`s,
establishing task37 §4 invariant 2: *the size is durable before any record is
written into the region*. When `ENOSPC` interrupts the zero-fill, the error
escapes before both the sync and `self.capacity = new_cap`, so the file is
physically longer than `capacity` and that extension was never synced — and the
next `open_with_chunk` adopts it via `metadata().len()`.

`1e5d2b7` ("fix(wal): roll the size back when a WAL extend fails partway", #23)
fixed that with a `set_len` rollback (`src/wal.rs:1223-1224`). **The rollback
was never executed by a test.** Its accompanying test is a regression guard
whose assertions held *before* the fix too: it fails on the first write via a
read-only handle, which leaves no partial extension to roll back.

That is the whole reason for this work. A fix nothing executes is a comment.

## 2. The seam: one mechanism, extended

`src/mutation.rs` (task47) already had the right shape — compiled only under the
non-default `mutation-testing` feature, selected at runtime by `ULTIMA_MUTATION`,
and feature-on-but-variable-unset behaves normally. **It was extended rather
than duplicated**: three payload-carrying variants beside the three existing
payload-free logic switches.

| variant | `ULTIMA_MUTATION` | injected at |
|---|---|---|
| `FailWriteAfter(u64)` | `fail-write-after=<n>` | `preallocate_to`'s zero-fill loop (`src/wal.rs:639-649`) |
| `FailSync` | `fail-sync` | `preallocate_to`'s `sync_all` (`:658-664`) **and** `PreallocFileSink::sync`'s trailing `sync_data` (`:1249-1254`) |
| `TearFrameAt(u64)` | `tear-frame-at=<n>` | `PreallocFileSink::sync`'s positioned batch write (`:1230-1245`) |

`FailWriteAfter`'s threshold is **bytes actually written, not iterations**. That
matters and was proven, not assumed: an iteration-counting implementation passes
the same negative test. Measured against a 1 MiB `preallocate_to` —
`=0` → `Err`, file 0 bytes; `=65536` → `Err`, file **exactly** 65536 bytes (a
genuine partial extension); `=1048576` → `Ok`, full length. No iteration counter
produces that table.

The default build is unaffected. Exactly one line escapes the `#[cfg]` gate (the
`to_write` binding, literally `&self.buf[..]` when the feature is off), and both
commits were extracted into throwaway trees with separate target dirs and their
test result lines diffed — identical.

### No counters — a deliberate constraint

`active()` memoises into a `OnceLock<Option<Mutation>>`. A payload parsed once
per process is fine; a *counter* ("fail the 3rd write") is not. **Decision:
process-global and parameter-only.** A counter makes the harness stateful and
the tests order-dependent, which is exactly what this repo's deterministic-test
discipline exists to avoid. A test that needs a specific write to fail arranges
for it to be the only one — which every case here allows, and §4 shows how.

Parsing fails closed: `fail-write-after` (no payload), `=abc` and `=-1` all
panic through the `.or_else(|| panic!(...))` at `src/mutation.rs:41`, and the
catch-all `Some(other) => panic!` arm is still last. There is no spelling that
silently yields "no mutation".

## 3. **One `ULTIMA_MUTATION` value per test binary** — read this before adding a fourth fault

This is the single most important thing to know about this suite, and the
plan got it wrong at first.

`crate::mutation::active()` memoises in a `OnceLock`, so **the first read wins
for the whole process** and every later `set_var` is silently ignored. Two tests
in one *file* therefore share whichever mutation happened to be read first — and
the loser still passes, because a wrong-fault `Err` is still an `Err`.

Measured, not reasoned: a scratch `tear-frame-at=64` test co-resident with the
failed-extend test observed `Err(Poisoned(..injected ENOSPC))` and a 0-byte WAL;
run alone with `--exact` the same test observed `Ok(1)` and a 16 MiB WAL. Same
code, opposite behaviour, both green.

> **`--test-threads=1` is not the mechanism and does not help.** The falsifying
> run above already had it. It is one *process* either way, and the `OnceLock`
> is per-process. The plan originally claimed determinism came from "the
> no-counters constraint plus `--test-threads=1`"; that half is wrong and was
> amended (`f398ba2`).

The actual mechanism is **process isolation**, and cargo gives it for free: one
binary per `tests/*.rs`, no `[[test]]` stanza needed. Hence three files rather
than one `wal_inflight_faults.rs`. Each file's module doc carries the rule
verbatim: *do not add a test with a different `ULTIMA_MUTATION` value to this
file*. A fourth fault means a fourth file and a fourth line in
`make test/wal-faults`.

The corollary that bit an early draft: `assert!(res.is_err())` does **not** pin
error identity, and under a memoised-mutation mix-up that is precisely how a
test passes while naming a fault it never ran. Every fault test here matches on
the error *variant* and asserts the injected message is nested inside it.

## 4. The oracle

Not "does it return an error" — that tests the injection, not the system. The
assertion is the durability contract, checked after recovery. Three clauses,
one discharged per test:

| clause | discharged by |
|---|---|
| **(2)** In-memory state never claims more than disk holds | `wal_fault_failed_extend.rs` — after a failed extend, the file is exactly the last size that was `sync_all`'d |
| **(1)** Every acknowledged commit survives recovery | `wal_fault_fsync.rs` — contrapositive: a commit that could not be synced must not return `Ok` |
| **(3)** A failure is either clean or loud, never silent partial state | `wal_fault_torn_tail.rs` — and this is the one that found F1 |

### `wal_fault_failed_extend.rs` — the acceptance gate

`fail-write-after=65536`, one 64 KiB zero-fill iteration: the smallest amount
that leaves a real partial extension. The assertion is the **exact** file
length, 0.

**Not `len % 4096 == 0`,** which is what the plan originally specified and which
would have been vacuous: `WAL_PREALLOC_CHUNK` is 16 MiB (`src/wal.rs:1157`), not
4096, and 65536 is itself a multiple of 4096 — the modulo holds with *and*
without the rollback. Caught before dispatch; it is exactly the "wrong in a way
that still passes" failure the plan's own self-review flagged.

**The gate, reproduced independently and in both directions, twice:** with the
rollback present the test passes and `wal.bin` is 0 bytes; with
`src/wal.rs:1223-1224` removed it fails and `wal.bin` is exactly 65536.
`src/wal.rs` verified unchanged by checksum after each run.

The payload is load-bearing in **both** directions and the file says so: it must
be `> 0` (with `=0` nothing is written, so there is no partial extension and the
file is 0 bytes with *and* without the rollback — the test would pass against
the bug it exists to catch) and `<` the extend size (at or above it the
injection never fires and `preallocate_to` simply succeeds).

### `wal_fault_fsync.rs` — a failing barrier is never a durable ack

Nothing anywhere in the repo assumed `sync_all`/`sync_data` could fail.

`Durability::Consistent` is deliberate: under `Eventual` the commit returns
*before* the fsync, so its `Ok` carries no durability claim and there is nothing
to falsify. Under `Consistent` the committing thread parks on the WAL background
thread's durability channel, so the sink's error is the commit's error —
`Err(Poisoned("WAL durability failure: … injected fsync failure"))`, via
`src/store.rs:3895`.

**The brief's assertion for this test was vacuous and was not shipped.** It
proposed `!acked || recovered`; since `commit()` returns `Err`, that is trivially
true — and worse than vacuous, since it would report green for a recovery
returning 0, 2 *or* 3 rows, including the one illegal value. Replaced with five:
the error identity; `latest_version() == 0` (not promoted); a subsequent
`begin_write` also `Poisoned` (the store stops accepting writes); an anti-vacuity
guard; and all-or-nothing recovery.

`FailSync` fires at both injection points and cannot be aimed by the env var, so
`wal.bin` is pre-sized to 1 MiB before the store opens. That reproduces the state
after any prior run or prune (`open_with_chunk` adopts `metadata().len()` as
`capacity`), the extend block is skipped, and the fault lands on the
**steady-state** barrier — the only arrangement in which the data actually
reached the file at the moment durability was lost. The guard is load-bearing,
and that was measured: deleting the pre-size turns the test red at assertion 4
(`left: 0, right: 1048576`) while assertions 1–3 still pass. Without it the test
would have been green in a degenerate state.

Can-it-fail: mutating `w.wait()?` → `let _ = w.wait();` at `src/store.rs:3895`
gives *"an injected fsync failure must surface as Error::Poisoned, got Ok(1)"*.

### `wal_fault_torn_tail.rs` — F1

See §5. `tear-frame-at=512`, aimed by *size* rather than by call index: the
injection writes `&self.buf[..n.min(self.buf.len())]`, so a batch no larger than
`n` is written whole and only a larger one tears. Three 32-byte seed commits then
one 5502-byte batch puts the tear on the last batch and nowhere else. `TEAR_AT`
is load-bearing in both directions; window measured `[32, 5502)`, both edges red.

## 5. F1's disposition — an unresolved cell, not a known bug

`a_torn_tail_costs_a_strict_scan_its_durably_acked_commits` is `#[ignore]`d.
**The `#[ignore]` means "awaiting a ruling", not "known broken".** The test
passes. What nobody has decided is whether the behaviour it records is the one
UltimaDB wants. This follows `tests/table_lifecycle_races.rs`'s treatment of its
three unruled cells (task59 §7).

### What was measured, 2026-08-06, ext4

Three commits acknowledged durable under `Durability::Consistent` (frames at 0,
32, 64 — 32 bytes each), then a final batch torn at 96: length prefix declares
5494 bytes, 512 written, `wal.bin` a full 16 777 216-byte preallocated chunk.

```text
STRICT   (WalWrite::PerEntry, tail_tolerant = false)
  recover()        = Err(WalCorrupted("CRC mismatch at entry starting at byte 96"))
  open_table("t")  = Err(TableNotFound("t"))
  latest_version() = 0        — all three acknowledged commits unreachable

TOLERANT (WalWrite::CoalescedPrealloc, tail_tolerant = true), SAME BYTES
  recover()        = Ok(())
  rows             = 3        — exactly the acknowledged commits
```

The test asserts byte-identity between the two reads rather than assuming it.
Nothing on disk is unrecoverable: **the recovery policy alone decides between
"all three commits" and "no database"**, and the affected set is 2 of the 3
`WalWrite` variants including the `#[default]` one, under both durable tiers.

### Why it is a ruling and not a fix

Both readings are defensible, and the file carries both:

- **As designed.** A non-preallocated sink appends into a file whose tail is not
  known-zero, so a CRC failure there is genuinely ambiguous — a torn write, or
  silent corruption mid-log with valid records after it. Refusing the file and
  telling the operator is the conservative reading; quietly truncating at the
  first bad frame turns real corruption into silent data loss.
- **A durability hole.** The scan already knows how many frames verified and
  already returns that offset in the message. A commit that returned `Ok` under
  `Consistent` was promised to survive; here it does not, and the operator's only
  lever is deleting the WAL — which destroys exactly the acknowledged commits.

### Two things to know before ruling it away

1. **A *fix* for F1 turns this test red.** Mutation S (`if tail_tolerant` →
   `if true` at `src/wal.rs:702`) is what a fix looks like, and it reddens the
   strict `match`. Red here therefore means *"an unruled behaviour changed"* —
   re-read the doc comment and update the recorded value — **not** "regression".
   Red anywhere else in this suite is a bug. Same triage rule as
   `test/lifecycle-races`' `--ignored` pass.
2. **`formal/tla/wal/mutations/M4Abort.cfg` uses `StrictScanErrLosesDurableAck`
   as its target.** Resolving F1 and deleting the property costs mutation M4 its
   witness, so the harm has to be re-homed first. The property is carried as a
   *checked owed property* at `formal/tla/wal/RESULTS.md:197`: `make
   formal/tla-model` asserts it reports **violated** at depth 9, exactly like a
   canary. The model said the loss happens; nothing executed that claim until
   this file.

### The vacuity trap, confirmed real

At `TEAR_AT = 12` the strict scan *still* returns `Err(WalCorrupted(..))` — the
right error type, from the right fault, for the wrong reason: it errors at byte
**0**, meaning nothing was ever intact and no acknowledged commit was lost. A
bare `is_err()` walks straight into it. What catches it is the **byte-offset
equality** against the independently computed `seed_frames_end` (red: `0` vs
`96`) and the tolerant row count (red: table absent, not 3 rows).

One assertion is deliberately unfalsifiable and labelled as such: `torn.is_ok()`
is structurally guaranteed by `TearFrameAt`, which writes a prefix and returns
`Ok(())` unconditionally. It is a property of the *fault model*, not of the
store, retained because it is what distinguishes a torn tail from a short one.
Declared, not disguised.

## 6. What this harness cannot cover

Two real limits, both load-bearing on how much the suite proves.

**(a) The "un-acked write is LOST" half of the durability contract is out of
reach entirely.** The fault is raised *inside the process that owns the dirty
pages*, so a `write` without `fsync` still survives via the page cache. On Linux
all three rows replay. Only a block-layer injector (dm-flakey) or real power
loss models the other outcome. This is a property of in-process injection, not
of these tests.

**(b) `wal_fault_fsync.rs` deliberately leaves the recovered row count
unpinned** (`n == 0 || n == ROWS`). Pinning `ROWS` would encode "an un-acked
write survives" as a contract, which it is not — see (a). The cost, stated
rather than hidden: `n == 0` is accepted although it is not a natural outcome of
this harness, so a `scan_wal` tail-tolerance regression that dropped the final
un-synced frame would pass silently here.

## 7. What is not covered, deliberately

- **Post-hoc corruption** — `tests/corruption_recovery.rs`, 11 tests. Not
  duplicated. See §1.
- **Counter-based injection** ("fail the 3rd write") — see §2.
- **Faults in checkpoint writing** — a separate surface with its own recovery
  path. Worth doing; not here.
- **`Persistence::Smr`** — checkpoint-only, so the WAL write path is never
  exercised.
- **MultiWriter.** All three tests run under the default `SingleWriter`. See the
  follow-up in §9 — this one is not just uncovered, it is *unreachable* by
  anything in the repository.

## 8. Where it runs

Nothing in `.github/workflows/` or the `Makefile` built the `mutation-testing`
feature at all before this task — `consistency/elle-mutation` shells out to
`scripts/elle_mutation.sh` and is opt-in. So all three binaries would have been
dead code in every gate, which is the same failure mode as the unproven rollback
they exist to catch.

```
make test/wal-faults      # two cargo invocations, ~1s of test time
```

and it **is in the aggregate `test` target**. The reasoning, since it costs a
second full compile (a distinct feature set; the existing `test` target already
spans two): `consistency/elle-mutation` is opt-in because it costs *minutes* and
a java toolchain, not because the feature is expensive. These three binaries run
in under a second, and leaving them opt-in would reproduce precisely the "a gate
nothing invokes" problem that made `1e5d2b7` ship unproven.

CI (`.github/workflows/ci.yml`) was extended for the same reason and it is the
stronger half of the argument: **CI does not invoke `make test`** — it runs
`make lint` plus explicit `cargo test` lines. A make target alone would still
have gated nothing on a PR. Two steps, split because the torn-tail cell needs
`-- --ignored`:

```yaml
- name: Tests (in-flight WAL faults)
  run: cargo test --features persistence,fulltext,mutation-testing \
         --test wal_fault_failed_extend --test wal_fault_fsync
- name: Tests (in-flight WAL faults, --ignored F1 cell)
  run: cargo test --features persistence,fulltext,mutation-testing \
         --test wal_fault_torn_tail -- --ignored
```

`make lint` still lints only the default feature set. Clippy is clean under
`persistence,fulltext`, `persistence,fulltext,metrics` and
`persistence,fulltext,mutation-testing` (both plain and `--all-targets`), but
only the first is gated. Extending `make lint` to a matrix is a separate change
with a wider blast radius than this task.

### The `formal/tla/wal/` cite corpus had to be re-anchored

The `src/wal.rs` injection points are pure insertions, but they moved every line
below them and **broke all 31 working-tree `src/wal.rs` anchors** in
`formal/tla/wal/cite-anchors.tsv` — i.e. `formal/scripts/check-cites.py`, which
CI's `formal / drift` job runs on every PR. Re-anchored here: **129 cite
occurrences across 15 corpus files plus the manifest** — the 31 the checker
flagged, plus 6 more ranges whose *end* had drifted without tripping it, because
their expectation token happened to stay inside the stale range. (`check-cites.py`
verifies the token, not the extent; a range that has silently stopped covering
the function it names still passes.) The map is piecewise — `+0` / `+24` / `+39` / `+45`, with one
line rewritten — derived from the diff, **not** a uniform offset. Two
column-boxed comment lines in `WalCrash.tla` needed their padding re-trimmed
because `977` → `1001` is one character wider.

The lesson generalises past this task: **any change to `src/wal.rs`,
`src/store.rs` or `src/persistence.rs` — including a pure insertion with no
semantic content — owes a `make formal/cite-check` run.** The drift guard does
not catch it; its predicate is "did anything under `formal/` change", which has
no relation to whether the cites were re-checked.

## 9. Follow-ups and open questions

**Booked from this work:**

- **The MultiWriter durability-failure branch at `src/store.rs:4248` is executed
  by nothing in the repository.** Proven, not inferred: its head was replaced
  with a `panic!` and the entire suite run — it never fired, including its
  `gate.advance()`. The live path under the default `SingleWriter` is
  `src/store.rs:3895`. **A MultiWriter fsync-failure variant is the natural next
  test**, and it would be the first thing ever to reach that branch.
- **`src/mutation.rs` is now the second fault-injection surface in production
  code** (after task47). The design spec flagged deciding whether that is *a
  pattern or an accumulation* as a plan-owner call, to be made before the code
  landed rather than after. It was not made. **Recorded as open; not ruled on
  here.**

**Deferred minors, carried rather than dropped:**

- The error identity pinned by `wal_fault_fsync.rs` is `Consistent`-specific:
  under `ConsistentInline` the same fault surfaces as `Error::Persistence`
  (`src/wal.rs:900`), which the test file does not say.
- The `n == 0` gap of §6(b) deserves a labelled-observation assertion rather
  than living only in prose.
- The un-acked-lost limitation of §6(a) is thin in the test file itself.
- `wal_fault_fsync.rs`'s assertions 2 and 3 overlap existing mock-WAL unit tests
  (`src/store.rs:7394`, `:7420`); real sink vs mock is the difference, and
  saying so in-file would be worth a line.
- `wal_fault_torn_tail.rs` adopts `tests/table_lifecycle_races.rs`'s
  ignore-string but not its **registry guard** (`:1819-1933`), which is what
  stops an unruled cell being silently deleted. With one cell the guard is
  arguably overkill; with a second it will not be.

**Fixed here:** stale line cites in `tests/wal_fault_torn_tail.rs`
(`src/wal.rs` `:678`→`677`, `:697-699`→`705-707`, `:601-610`→`602-611`;
`src/store.rs:1072-1078`→`1073-1078`), each verified against the current file
rather than shifted; and a contradiction about which of the torn-tail
preconditions is anti-vacuity load-bearing (`torn_len + 8 > TEAR_AT` is the
**upper**-edge guard and goes red at `TEAR_AT >= 5502`; only `seed_frames_end >
0` is weak by construction, and it is weak because it never checks a CRC).

## 10. Maintenance notes

- **Restore a verification mutation with `git checkout`, then `touch` the file.**
  Cargo decides what to rebuild from mtime, so a `cp -p` restore leaves the
  *mutated* binary in place and the next run reports the mutation's failures
  against what looks like clean source. Same trap as task59 §5.
- **Adding a fourth fault means a fourth `tests/*.rs` file.** See §3. Not a
  fourth test in an existing one.
- `CARGO_TARGET_DIR` must not be under `/tmp` — it is tmpfs, and
  `src/test_scratch.rs`'s guard refuses. These tests use `scratch_dir()` rather
  than `tempfile::tempdir()` for the same reason: `fsync` on tmpfs is a no-op,
  which for the fsync and torn-tail tests would void the entire subject matter.
