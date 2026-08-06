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

The default build is unaffected. Every injection block is `#[cfg]`-gated, and so
is the `to_write` binding — in **both** directions (`src/wal.rs:1236` selects the
mutation arm, `:1243` the `not(mutation-testing)` arm, which is literally
`&self.buf[..]`). The one line that *escapes* the gate is the existing
`write_all(to_write)` at `src/wal.rs:1245`, rewritten from `write_all(&self.buf)`
— semantically identical off the feature: no allocation, no branch, no reborrow
difference. Both commits were extracted into throwaway trees with separate target
dirs and their test result lines diffed — identical.

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

> **`--test-threads=1` is not the mechanism and does not help *with this*.** The
> falsifying run above already had it. It is one *process* either way, and the
> `OnceLock` is per-process. The plan originally claimed determinism came from
> "the no-counters constraint plus `--test-threads=1`"; that half is wrong and
> was amended (`f398ba2`).

### …and yet every invocation passes `--test-threads=1`, for an unrelated reason

The correction above was over-generalised into "thread count is irrelevant
here", and it is not. **Two independent facts, and they must not be collapsed:**

- **One mutation value per binary** is what makes the fault *deterministic*. No
  thread count substitutes for it.
- **`--test-threads=1`** is what makes each test's `unsafe { env::set_var }`
  *sound*. Nothing else substitutes for that.

None of the three binaries is single-test: each has `mod common;`, which
`#[path]`-includes `src/test_scratch.rs`, whose `#[cfg(test)] mod tests` adds two
more `#[test]` fns. libtest runs them concurrently by default, so `set_var`
executes while other test threads are live — and both `scratch_dir()`
(`ULTIMA_ALLOW_TMPFS`, `src/test_scratch.rs:63`) and `Store::new`
(`ULTIMA_OVERLAY_CAP`, `src/store.rs:563`) call `std::env::var*`. Concurrent
getenv/setenv is UB. There was no *live* race — neither co-resident test reaches
either read — but the invariant the `SAFETY` notes claimed ("single-threaded
test binary") was false, and the files' own advice (*"do not add a test with a
**different** `ULTIMA_MUTATION` value"*, i.e. a same-value one is fine) steers
straight into it: any added test calling `scratch_dir()` makes the race real.

So `make test/wal-faults` and both CI steps pass `-- --test-threads=1`, and the
`SAFETY` notes now name that as their justification instead of asserting
something untrue. Keep the flag on any new invocation.

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
- **The fsync path of every sink except `PreallocFileSink` — including the
  `#[default]` one.** This is a *reduction from the design spec* and the most
  consequential thing on this list, so it is spelled out rather than implied.
  The spec's fault table put `FailSync` at three sites: `preallocate_to`'s
  `sync_all`, and `WalSink::sync` for all three sinks (`src/wal.rs:1079`,
  `:1128`, `:1204`). **Only the prealloc sink's got the injection**
  (`src/wal.rs:1249-1254`, plus `preallocate_to`'s at `:658-664`).
  `FileSink::sync` (`:1079-1083`, i.e. `WalWrite::PerEntry` — **the
  `#[default]`**) and `BufferedFileSink::sync` (`:1128-1137`, i.e.
  `WalWrite::Coalesced`) carry none. So `wal_fault_fsync.rs` exercises a failing
  durability barrier for `CoalescedPrealloc` only, and the configuration most
  users actually run has no fsync-fault test at all.

  What limits the damage, and it is genuinely a limit rather than an excuse: the
  store-side propagation machinery under test — the poison latch, the durability
  waiter, non-promotion, the refusal of subsequent writes — is sink-independent,
  reached through `WalSink::sync`'s `Result` and not through any sink's
  internals. The sink-specific part that goes untested is the small matter of
  *which* error each sink returns, and there `PerEntry` and `Coalesced` differ
  from `CoalescedPrealloc` (`Error::Persistence` straight from the `map_err`, no
  positioned-write bookkeeping). Adding the two injections is two `#[cfg]`
  blocks; see the follow-up in §9.
- **`FailWriteAfter` in the sinks' batch write.** The spec's table named that
  surface too; the variant landed only in `preallocate_to`'s zero-fill
  (`src/wal.rs:639-649`). `TearFrameAt` covers the batch-write surface instead,
  which is a deliberate substitution (a torn frame is the interesting shape of a
  partial batch write) and is why this one is a substitution rather than a gap.

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
         --test wal_fault_failed_extend --test wal_fault_fsync \
         --test wal_fault_torn_tail -- --test-threads=1
- name: Tests (in-flight WAL faults, --ignored F1 cell)
  run: |                       # + an explicit non-zero-match assertion, see below
    cargo test --features persistence,fulltext,mutation-testing \
      --test wal_fault_torn_tail -- --ignored --test-threads=1 \
      --exact a_torn_tail_costs_a_strict_scan_its_durably_acked_commits | tee /tmp/f1.log
    grep -q '^test a_torn_tail_…_acked_commits \.\.\. ok$' /tmp/f1.log || exit 1
```

`--test-threads=1` on both: see §3's second subsection — it is the `unsafe
set_var` soundness condition, not the determinism mechanism.

### The `--ignored` step had to be made non-vacuous by hand

**A libtest filter matching zero tests exits 0.** Measured:

```text
$ cargo test … --test wal_fault_torn_tail -- --ignored a_torn_tail_costs_…
running 0 tests
test result: ok. 0 passed; 0 failed; … 3 filtered out
EXIT=0
```

So the original two-step arrangement would have stayed green if F1 were deleted,
renamed, or de-ignored — and the de-ignore case was the worse one, because F1 ran
in **no** non-`--ignored` invocation anywhere, so removing `#[ignore]` deleted it
from CI entirely. That is strictly weaker than the `table_lifecycle_races`
precedent it was modelled on: that suite runs a plain pass *and* an `--ignored`
pass, so de-ignoring a cell merely moves it between two live gates.

Both halves are closed:

1. **`--test wal_fault_torn_tail` was added to the first invocation.** F1 stays
   gated if it is ever de-ignored. (Today it contributes only its two
   scratch-dir guards there.)
2. **The `--ignored` step asserts the match explicitly.** There is no flag for
   it: cargo 1.96 has no `--no-tests` (`error: unexpected argument '--no-tests'`)
   and libtest's `-- --help` lists nothing equivalent — `--exact` narrows a
   filter but a zero-match `--exact` run still exits 0. So the step greps its own
   output for `test <name> … ok` and fails otherwise. Verified in both
   directions: renaming the cell turns the old command shape green (`EXIT=0`) and
   the new one red; de-ignoring it turns the *first* invocation into F1's gate
   and the second red, which is deliberate — landing the ruling means moving the
   cell, and that should be a loud edit, not a silent one.

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
the function it names still passes.)

The map is **seven buckets**, derived line by line from the diff, not a uniform
offset — and not the four it is tempting to summarise it as, because both
injection sites insert *inside* a block rather than between blocks:

| old `src/wal.rs` | shift | |
|---|---|---|
| ≤ 635 | `+0` | above the first insertion |
| 636 | `+2` | `while remaining > 0 {` — the `written` counter went in above it |
| 637–639 | `+13` | the zero-fill body — the `FailWriteAfter` block went in above it |
| 640 | `+17` | the `}` closing that `while` |
| 641–1205 | `+24` | below the whole first hunk |
| 1206 | → **1245**, and the line was **rewritten** (`write_all(&self.buf)` → `write_all(to_write)`), so its manifest token changed too |
| 1207–1209 | `+39` | between the `TearFrameAt` block and the `FailSync` block |
| ≥ 1210 | `+45` | below both |

The three middle buckets are the ones a summary loses, and they are not
cosmetic: the anchor `633-640` (the zero-fill loop) becomes `633-657`, whereas
`+24` would put its end at 664 — a *different* `}` at the same indent, closing
the injected `FailSync` `if` rather than the `while`. It would have passed the
checker, since the expectation token is on the range's first line. **Do not
diff-align braces mechanically; read what each one closes.** (`difflib` gets
this wrong too, aligning old 640 to 664 and old 1209 to 1254.)

Two column-boxed comment lines in `WalCrash.tla` needed their padding re-trimmed
because `977` → `1001` is one character wider.

Three lessons generalise past this task, in increasing order of how hard they
are to notice:

1. **Any change to `src/wal.rs`, `src/store.rs` or `src/persistence.rs` —
   including a pure insertion with no semantic content — owes a
   `make formal/cite-check` run.** The drift guard does not catch it; its
   predicate is "did anything under `formal/` change", which has no relation to
   whether the cites were re-checked.

   **And `check-cites.py` only governs `formal/tla/wal/`.** Cites anywhere else
   — `docs/tasks/`, `docs/superpowers/`, module docs, test files — have no
   guard at all and rot silently. This task's own design and plan documents were
   among the casualties: both still cited the F2 rollback at `src/wal.rs:1199`
   after the implementation moved it to `:1223-1224`, and they are the first
   thing a future contributor reads. All **20** `src/*.rs` cites across the two
   were re-checked by hand against the current source; **17 were stale and were
   corrected, 3 were already right** — and those 3 are exactly the cites the line
   shift could not have moved (`src/wal.rs:628`, above the first insertion, and
   `src/store.rs:4438-4444` twice, in a file this branch never touched). Both docs
   now carry a dated note saying they name the *current* tree. The alternative
   idiom, when a document genuinely means
   historical code, is the corpus's frozen form — `<sha>^ src/wal.rs:1130-1136`
   — which `check-cites.py` resolves with `git show` and verifies like any other.
2. **The checker verifies a range's token, not its extent.** Six ranges here had
   stopped covering the function they name while staying green. After a
   re-anchor, re-read the *range*, not just the first line.
3. **A large mechanical re-anchor can leave every anchor correct and the
   surrounding sentences wrong, and nothing in the repo detects that.** This
   one did: `formal/tla/wal/README.md:493-530` taught "a bare cite may not name
   a range two source files both anchor" using `720` as its worked example —
   valid in both `src/store.rs:720` and, before this task, `prune_wal` at line
   720 of `src/wal.rs`. Moving `prune_wal` to `src/wal.rs:744` dissolved the
   collision, so the paragraph went on asserting a refusal (`AMBIGUOUS BARE
   CITE`) that has no live instance — with `check-cites.py` green throughout,
   because 744 genuinely is `prune_wal`. **Re-read the prose around every changed
   cite, not just the cite.** The paragraph now states the rule, records that no
   collision exists in the tree today, and carries the disappearance as its
   example; both directions were verified by running the checker rather than
   asserted.

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
  here** — it is a decision owed by the repo owner, not an implementer's backlog
  item, and the final whole-branch review re-flagged it as the most substantive
  open question on the branch.
- **`FailSync` reaches only `PreallocFileSink`, so the `#[default]` sink's fsync
  path is untested.** The spec named three `WalSink::sync` sites; one landed. See
  §7 for the full statement and why the marginal loss is bounded. The work is two
  `#[cfg]` blocks at `src/wal.rs:1079-1083` and `:1128-1137` plus a test file per
  mutation value — but note that `FailSync` cannot be aimed, so a `PerEntry` test
  would need the same pre-sizing trick `wal_fault_fsync.rs` uses, and the
  expected error identity differs (`Error::Persistence`, not `Poisoned`, for the
  non-prealloc sinks under some tiers — see the `ConsistentInline` minor below).

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
  arguably overkill; with a second it will not be. Partly mitigated by the
  explicit non-zero-match assertion added to the gate (§8), which catches a
  deleted or renamed cell — but that guard lives in the `Makefile` and
  `ci.yml`, not beside the test, and the precedent's own doc admits even the
  registry guard only ties one direction.

The list above was carried in the plan's `.superpowers/` ledger, which is
gitignored and evaporates at merge; the final whole-branch review made moving it
here a condition. Four ledger-only items were named. Three were **fixed in the
final round rather than deferred** — the false `SAFETY` note (§3), the vacuous
`--ignored` gate (§8), and the `open_with_chunk` cite in
`tests/wal_fault_torn_tail.rs:25`. The fourth was ruled **no action**: an earlier
report claimed the known `tests/store_integration.rs` flake
(`concurrent_same_table_overlapping_keys_with_retry`) "did not appear in any
run", which overstated a claim about a report rather than about the code; the
flake is real, pre-existing, and out of scope here.

**Fixed here:** stale line cites in `tests/wal_fault_torn_tail.rs`
(`src/wal.rs` `:678`→`677`, `:697-699`→`705-707`, `:601-610`→`602-611`;
`src/store.rs:1072-1078`→`1073-1078`), each verified against the current file
rather than shifted; and a contradiction about which of the torn-tail
preconditions is anti-vacuity load-bearing (`torn_len + 8 > TEAR_AT` is the
**upper**-edge guard and goes red at `TEAR_AT >= 5502`; only `seed_frames_end >
0` is weak by construction, and it is weak because it never checks a CRC).

**Fixed in the final whole-branch fix round:** the false `SAFETY: single-threaded
test binary` note on all three `unsafe { env::set_var }` blocks, closed by
passing `--test-threads=1` everywhere rather than by re-wording the claim (§3);
the vacuously-green `--ignored` CI/`make` step (§8); the `mutation-testing`
feature described as Elle-only in `src/mutation.rs` and
`docs/reference/cargo-features.md`; two cite imprecisions in
`tests/wal_fault_torn_tail.rs` (`read_wal`/`prune_wal` conflated at
`src/wal.rs:730-732` — `prune_wal` is `:744` and inherits strictness *through*
`read_wal`; `open_with_chunk` cited at its body line `:1192` rather than its
signature `:1177`); the payload of `wal_fault_failed_extend.rs` hoisted into a
documented `const FAIL_WRITE_AFTER` to match its two siblings, and its trailing
`remove_var` given the "hygiene only" note they carry; §2's inverted account of
which line escapes the `#[cfg]` gate; and §7 gaining the fsync-injection scope
reduction it had omitted.

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
