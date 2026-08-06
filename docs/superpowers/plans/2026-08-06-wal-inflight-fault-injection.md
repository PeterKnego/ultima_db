# WAL In-flight Fault Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute the WAL write path's error branches — which no test has ever reached — so that the `preallocate_to` rollback shipped unproven in `1e5d2b7`, the untested fsync-failure path, and the F1 property that exists only in the TLA model are all covered against real I/O.

**Architecture:** Extend the existing task47 mutation harness (`src/mutation.rs`, gated on the non-default `mutation-testing` feature and selected by `ULTIMA_MUTATION`) with three parameterised I/O faults. Assert the durability contract after recovery, not the error itself.

**Tech Stack:** Rust, `cargo test`, the existing `mutation-testing` cargo feature. No new dependencies.

**Design spec:** `docs/superpowers/specs/2026-08-06-wal-inflight-fault-injection-design.md` — read it before Task 1.

## Global Constraints

- **Do not duplicate `tests/corruption_recovery.rs`.** Its 11 tests cover *post-hoc* corruption (truncate, zero tail, garbage tail, bit-flip, checkpoint damage). This plan covers only faults that occur *during* an operation, which post-hoc editing cannot produce.
- All injection code is `#[cfg(feature = "mutation-testing")]`. A default build must be byte-identical in behaviour. Follow the call-site pattern at `src/store.rs:4438-4444`.
- **No counters in the harness.** `ULTIMA_MUTATION` is parsed once per process into a `OnceLock`; faults carry parameters, never "fail the Nth call". A test that needs a specific write to fail arranges for it to be the only one.
- The oracle is the durability contract, not the error: every acknowledged commit survives recovery; in-memory state never claims more than disk holds; a failure is either clean or loud.
- **Every test must fail against the code it targets when that code is reverted** — demonstrated with output, not asserted.
- clippy `-D warnings` clean under `persistence,fulltext`, `persistence,fulltext,metrics`, **and** `persistence,fulltext,mutation-testing`.
- Run `cargo test --features persistence,fulltext,metrics` before every commit — CI uses that combination.
- Do not run `cargo fmt` (repo-wide rustfmt-version drift; match surrounding style).
- `CARGO_TARGET_DIR` must not be under `/tmp` — it is tmpfs here and `src/test_scratch.rs`'s durability guard will refuse.
- Known flake, not yours: `tests/store_integration.rs::concurrent_same_table_overlapping_keys_with_retry`, ~1 in 5, threaded.

---

## File Structure

- **Modify `src/mutation.rs`** — three new variants with payloads, plus parsing. One responsibility: what fault is active.
- **Modify `src/wal.rs`** — three `#[cfg]`-gated injection points: the zero-fill loop in `preallocate_to` (`:628-643`), that function's `sync_all` (`:641`), and `PreallocFileSink::sync`'s positioned write (`:1205`).
- **Create one test file per mutation value** — `tests/wal_fault_failed_extend.rs` (Task 3), `tests/wal_fault_fsync.rs` (Task 4), `tests/wal_fault_torn_tail.rs` (Task 5). Siblings to `corruption_recovery.rs`, deliberately separate from it: that file's helpers all assume post-hoc editing of a closed file.

  **One mutation value per binary is a correctness requirement, not tidiness.** `crate::mutation::active()` memoises in a `OnceLock`, so the *first* `ULTIMA_MUTATION` read wins for the whole process. Demonstrated: a second test in the same file setting `tear-frame-at=64` ran co-resident and got `Err(Poisoned(…injected ENOSPC))` — the *first* test's fault — and passed; run alone it got `Ok`. It executed a fault it never named, and went green. `--test-threads=1` does not help; it is one process either way. Cargo's auto-discovery gives one process per `tests/*.rs` file, so no `[[test]]` stanza is needed.
- **Modify `Makefile`** — a target that runs this suite with the feature enabled, since a default `cargo test` cannot reach it.

---

### Task 1: The fault variants

**Files:**
- Modify: `src/mutation.rs`

**Interfaces:**
- Produces: `Mutation::FailWriteAfter(u64)`, `Mutation::FailSync`, `Mutation::TearFrameAt(u64)`, parsed from `fail-write-after=<n>`, `fail-sync`, `tear-frame-at=<n>`.

- [ ] **Step 1: Write the failing test**

Add to `src/mutation.rs`'s existing `mod tests`:

```rust
    #[test]
    fn parses_the_io_fault_variants() {
        assert_eq!(parse(Some("fail-write-after=0")), Some(Mutation::FailWriteAfter(0)));
        assert_eq!(parse(Some("fail-write-after=65536")), Some(Mutation::FailWriteAfter(65536)));
        assert_eq!(parse(Some("fail-sync")), Some(Mutation::FailSync));
        assert_eq!(parse(Some("tear-frame-at=12")), Some(Mutation::TearFrameAt(12)));
    }

    #[test]
    #[should_panic(expected = "unknown ULTIMA_MUTATION")]
    fn rejects_an_io_fault_with_no_payload() {
        // `fail-write-after` without `=<n>` is a typo, not a default — an
        // unparameterised fault would silently fail the *first* write and make
        // every test using it pass for the wrong reason.
        let _ = parse(Some("fail-write-after"));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features persistence,fulltext,mutation-testing --lib mutation::`
Expected: FAIL — the variants do not exist.

- [ ] **Step 3: Add the variants**

Extend the enum (keep the existing three untouched):

```rust
    /// I/O fault: the next `write_all` in the WAL write path writes `n` bytes
    /// and then returns `ENOSPC`. Models a disk filling mid-operation, which
    /// leaves the file longer than the sink's in-memory `capacity`.
    FailWriteAfter(u64),
    /// I/O fault: the next `sync_all`/`sync_data` in the WAL write path returns
    /// an error instead of succeeding.
    FailSync,
    /// I/O fault: the sink's positioned batch write is truncated at this byte
    /// offset — a torn frame, produced while the sink still believes it wrote
    /// the whole batch.
    TearFrameAt(u64),
```

and extend `parse` (`src/mutation.rs:21-29`):

```rust
        Some(s) if s.starts_with("fail-write-after=") => s["fail-write-after=".len()..]
            .parse()
            .ok()
            .map(Mutation::FailWriteAfter)
            .or_else(|| panic!("unknown ULTIMA_MUTATION value: {s}")),
        Some("fail-sync") => Some(Mutation::FailSync),
        Some(s) if s.starts_with("tear-frame-at=") => s["tear-frame-at=".len()..]
            .parse()
            .ok()
            .map(Mutation::TearFrameAt)
            .or_else(|| panic!("unknown ULTIMA_MUTATION value: {s}")),
```

The existing `Some(other) => panic!(...)` arm stays last and catches the
unparameterised spellings.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --features persistence,fulltext,mutation-testing --lib mutation::`
Expected: PASS.

- [ ] **Step 5: Confirm the default build is unaffected**

Run: `cargo test --features persistence,fulltext,metrics --lib mutation::`
Expected: the module is `#[cfg(feature = "mutation-testing")]`-gated at its
`mod` declaration in `src/lib.rs`; confirm it compiles out and no test in the
default build references the new variants.

- [ ] **Step 6: Commit**

```bash
git add src/mutation.rs
git commit -m "feat(mutation): parameterised I/O fault variants (WAL in-flight injection)

Three faults the existing logic mutations cannot express, because they need
a parameter: which write fails and after how many bytes. Unparameterised
spellings panic rather than defaulting — a fault that silently failed the
first write would make every test using it pass for the wrong reason."
```

---

### Task 2: Injection points, still unreached

**Files:**
- Modify: `src/wal.rs` — `preallocate_to` (`:628-643`) and `PreallocFileSink::sync` (`:1183-1205`)

**Interfaces:**
- Consumes: `Mutation::FailWriteAfter`, `FailSync`, `TearFrameAt` from Task 1.
- Produces: three live injection points. No test reaches them yet — Task 3 does.

- [ ] **Step 1: Inject into the zero-fill loop**

In `preallocate_to`, inside the `while remaining > 0` loop, before `write_all`:

```rust
        #[cfg(feature = "mutation-testing")]
        if let Some(crate::mutation::Mutation::FailWriteAfter(after)) = crate::mutation::active()
            && written >= after
        {
            // BUG-INJECTION: models ENOSPC partway through the zero-fill. The
            // caller must roll the size back — see the extend block's rollback.
            return Err(Error::Persistence("injected ENOSPC".into()));
        }
```

Track `written` as a running total of bytes actually written in the loop, so
`FailWriteAfter(0)` fails immediately and `FailWriteAfter(65536)` fails after the
first 64 KiB chunk — which is what produces a *partial* extension.

- [ ] **Step 2: Inject into the two syncs**

Before `file.sync_all()` in `preallocate_to` (`:641`), and before the
`sync_data()` at the end of `PreallocFileSink::sync`:

```rust
        #[cfg(feature = "mutation-testing")]
        if matches!(crate::mutation::active(), Some(crate::mutation::Mutation::FailSync)) {
            return Err(Error::Persistence("injected fsync failure".into()));
        }
```

- [ ] **Step 3: Inject the torn frame**

In `PreallocFileSink::sync`, replace the positioned `write_all(&self.buf)` with a
`#[cfg]`-gated truncating variant that writes only the first `n` bytes when
`TearFrameAt(n)` is active, then returns `Ok(())` — the sink must *believe* it
wrote the whole batch, which is what makes the tail torn rather than short.

- [ ] **Step 4: Confirm the default build is byte-identical in behaviour**

Run: `cargo test --features persistence,fulltext,metrics`
Expected: unchanged from before this task — the full suite green, same counts.

Run: `cargo clippy --features persistence,fulltext,mutation-testing --all-targets -- -D warnings`
Expected: clean. Let-chains are used elsewhere in this file, so the `if let … && …` form compiles.

- [ ] **Step 5: Commit**

```bash
git add src/wal.rs
git commit -m "feat(wal): in-flight fault injection points (mutation-testing only)

Three #[cfg]-gated points the existing post-hoc corruption tests cannot
reach: a short write inside preallocate_to's zero-fill, a failing fsync in
both sync paths, and a torn positioned write that leaves the sink believing
it wrote the whole batch."
```

---

### Task 3: F2 — the rollback that ships unproven

**Files:**
- Create: `tests/wal_fault_failed_extend.rs`

**Interfaces:**
- Consumes: the injection points from Task 2.
- Produces: `standalone_prealloc_config(dir)`, `seed_commits(dir, n)`, `recovered_count(store)` helpers for Tasks 4–5.

**This task is the reason the plan exists.** `src/wal.rs:1199`'s `set_len`
rollback has never been executed by a test.

- [ ] **Step 1: Write the failing test**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! In-flight WAL faults: a syscall fails *partway through* an operation, while
//! the sink still holds in-memory state.
//!
//! Distinct from `tests/corruption_recovery.rs`, which edits a closed file and
//! then recovers. That cannot produce the state these tests target — F2's
//! defect is precisely that `preallocate_to` leaves the file longer than
//! `capacity` *while the sink is mid-extend*.
//!
//! Requires `--features mutation-testing`; see `make test/wal-faults`.
#![cfg(feature = "mutation-testing")]

use std::path::Path;
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};

fn standalone_prealloc_config(dir: &Path) -> StoreConfig {
    StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.to_path_buf(),
            Durability::Consistent,
            WalWrite::CoalescedPrealloc,
        ))
        .build()
}

/// A failed extend must leave the on-disk size one we know is durable.
///
/// `preallocate_to` zero-fills then `sync_all`s, establishing task37 §4
/// invariant 2. When the zero-fill dies partway the error escapes before both
/// the sync and `capacity = new_cap`, so without the rollback the file is
/// physically longer than `capacity` and that extension was never synced — and
/// the next `open` adopts it via `metadata().len()`.
#[test]
fn a_failed_extend_does_not_leave_the_file_longer_than_capacity() {
    // SAFETY: single-threaded test binary; set before the store is built.
    unsafe { std::env::set_var("ULTIMA_MUTATION", "fail-write-after=65536") };
    let dir = tempfile::tempdir().unwrap();

    let store = Store::new(standalone_prealloc_config(dir.path())).unwrap();
    store.register_table::<String>("t").unwrap();

    // Write enough to force an extend past the first chunk.
    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table::<String>("t").unwrap();
    for i in 0..5000u64 {
        t.insert(format!("row{i}")).unwrap();
    }
    let res = wtx.commit();

    // The commit must fail loudly — never silently half-durable.
    assert!(res.is_err(), "an injected ENOSPC must surface, got {res:?}");
    drop(store);

    // The invariant: the file on disk is exactly the last size that was
    // sync_all'd. A fresh prealloc WAL opens with `capacity = 0`, so a failed
    // FIRST extend must roll back to 0. Without the rollback the file is
    // 65536 bytes — the partial zero-fill that ENOSPC interrupted.
    let wal = dir.path().join("wal.bin");
    let len = std::fs::metadata(&wal).unwrap().len();
    assert_eq!(
        len, 0,
        "wal.bin is {len} bytes; a failed first extend must leave it at the \
         capacity that was last sync_all'd (0), not at the partial zero-fill"
    );

    unsafe { std::env::remove_var("ULTIMA_MUTATION") };
}
```

**Do not assert `len % chunk == 0`.** `WAL_PREALLOC_CHUNK` is **16 MiB**
(`src/wal.rs:1133`), and the partial zero-fill leaves 65536 bytes — which *is* a
multiple of 4096, so a modulo assertion passes with and without the rollback and
the acceptance gate becomes vacuous. Assert the exact expected length.

Verify `WAL_PREALLOC_CHUNK` yourself before relying on any of this; if it has
changed, the 65536 figure moves with `FailWriteAfter`'s payload, not with the
chunk.

- [ ] **Step 2: Run to verify it passes with the fix present**

Run: `cargo test --features persistence,fulltext,mutation-testing --test wal_fault_failed_extend`
Expected: PASS.

- [ ] **Step 3: Prove it fails without the rollback — the acceptance gate**

Back up `src/wal.rs` with a checksum. Remove the two rollback lines at
`src/wal.rs:1199-1200` (`set_len` and its `sync_all`), leaving the `return Err(e)`.

Run the same command.
Expected: **FAIL**, with the file length not a multiple of the chunk size.

Restore `src/wal.rs` and verify the checksum matches. **Paste both outputs into
your report.** If the test passes with the rollback removed, it is not testing
the rollback — say so rather than proceeding.

- [ ] **Step 4: Commit**

```bash
git add tests/wal_fault_failed_extend.rs
git commit -m "test(wal): execute the failed-extend rollback (F2, issue #23)

The rollback at src/wal.rs:1199 shipped in 1e5d2b7 with only a regression
guard, whose assertions held before the fix too — a read-only handle fails
on the first write and leaves no partial extension to roll back. This
injects ENOSPC after the first 64 KiB chunk, which does."
```

---

### Task 4: fsync failure

**Files:**
- Create: `tests/wal_fault_fsync.rs` (its own binary — see File Structure)

- [ ] **Step 1: Write the test**

```rust
/// A failing fsync must not leave a commit reported durable.
///
/// Nothing in the suite currently assumes `sync_all`/`sync_data` can fail. The
/// contract is not that the commit succeeds — it is that a commit which
/// returned `Ok` survives recovery, and one that could not be synced does not
/// return `Ok`.
#[test]
fn a_failing_fsync_is_never_reported_as_a_durable_commit() {
    unsafe { std::env::set_var("ULTIMA_MUTATION", "fail-sync") };
    let dir = tempfile::tempdir().unwrap();

    let acked = {
        let store = Store::new(standalone_prealloc_config(dir.path())).unwrap();
        store.register_table::<String>("t").unwrap();
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<String>("t").unwrap().insert("a".into()).unwrap();
        wtx.commit().is_ok()
    };
    unsafe { std::env::remove_var("ULTIMA_MUTATION") };

    let store = Store::new(standalone_prealloc_config(dir.path())).unwrap();
    store.register_table::<String>("t").unwrap();
    let recovered = store.recover().is_ok()
        && store.begin_read(None).unwrap().open_table::<String>("t")
            .map(|t| t.len()).unwrap_or(0) == 1;

    assert!(
        !acked || recovered,
        "commit() returned Ok under an injected fsync failure but the row did \
         not survive recovery — a commit was reported durable that was not"
    );
}
```

Note `Durability::Consistent` is deliberate: under `Eventual` the commit returns
before the fsync, so `acked` carries no durability claim and the assertion is
vacuous. State that in the doc comment.

- [ ] **Step 2: Run, and report what it shows**

Run: `cargo test --features persistence,fulltext,mutation-testing --test wal_fault_failed_extend`

**Either outcome is a result.** If it passes, the fsync path already upholds the
contract and the test pins it. If it fails, that is a finding — **do not fix
`src/`**; report it with the observed behaviour.

- [ ] **Step 3: Commit**

```bash
git add tests/wal_fault_fsync.rs
git commit -m "test(wal): a failing fsync must not report a durable commit"
```

---

### Task 5: F1 — the torn tail a strict scan cannot survive

**Files:**
- Create: `tests/wal_fault_torn_tail.rs` (its own binary — see File Structure)

**F1 currently exists only as a TLA property** (`StrictScanErrLosesDurableAck`,
`formal/tla/wal/RESULTS.md:197`). This gives it an executable counterpart.

- [ ] **Step 1: Write the test**

Seed several commits under `CoalescedPrealloc`, then inject `tear-frame-at=<n>`
so the final batch is written partially while the sink believes it wrote it all.
Recover under a **strict**-scan configuration (`WalWrite::PerEntry`, whose
`Store::recover` passes `tolerant = false`) pointed at the same directory, and
observe whether the earlier, durably-acked commits are still reachable.

- [ ] **Step 2: Run it and classify the result**

**This is the one cell in the plan whose correct outcome is not settled.** F1 is
carried as an *owed property* — the model asserts the loss happens. If the
executable test reproduces it:

- **Do not pin it as correct.** Land it `#[ignore]`d carrying the question, in
  the style of `tests/table_lifecycle_races.rs`'s unresolved cells: state the
  question, record measured behaviour, and cite the TLA property.
- Report it so the plan owner can rule.

If it does *not* reproduce, that is more interesting — it means the model and
the implementation disagree, and the report should say which you believe and why.

- [ ] **Step 3: Commit**

```bash
git add tests/wal_fault_torn_tail.rs
git commit -m "test(wal): F1 — a strict scan and a torn tail (unresolved)"
```

---

### Task 6: Make the suite runnable, and record it

**Files:**
- Modify: `Makefile`
- Create: `docs/tasks/task60_wal_inflight_faults.md`

A default `cargo test` cannot reach any of this — the file is
`#![cfg(feature = "mutation-testing")]`. Without a target, the suite is dead on
arrival.

- [ ] **Step 1: Add the Makefile target**

Follow the convention of the existing `test/lifecycle-races` target: in `.PHONY`,
and reachable from an aggregate. **Decide deliberately whether it joins `test`** —
it needs a distinct feature build, so it costs a second full compile. Say which
you chose and why.

- [ ] **Step 2: Write the feature record**

`docs/tasks/task60_wal_inflight_faults.md` covering: the post-hoc/in-flight
distinction and why `corruption_recovery.rs` structurally cannot cover this; the
three faults and what each retires; the no-counters decision and its reason; the
oracle; F1's disposition; and what remains uncovered (checkpoint-write faults,
SMR mode).

- [ ] **Step 3: Verify the whole thing**

Run all four: the default suite, the metrics suite, the mutation-testing suite,
and clippy under all three feature sets. Confirm the default build is unchanged.

- [ ] **Step 4: Commit**

```bash
git add Makefile docs/tasks/task60_wal_inflight_faults.md
git commit -m "docs(task60): WAL in-flight fault injection — feature record + make target"
```

---

## Self-Review

**Spec coverage.** The three faults → Tasks 1–2 (seam) and 3–5 (one task each).
The oracle's three clauses → Task 3 (in-memory never claims more than disk),
Task 4 (acked commits survive), Task 5 (clean or loud). Success criterion 1
(F2's rollback fails when reverted) → Task 3 Step 3, the acceptance gate.
Criterion 5 (deterministic) → the no-counters constraint plus `--test-threads=1`.
The "out of scope" list has no tasks, as intended.

**Known soft spots, flagged rather than hidden.**

1. **Task 3's chunk-size assumption.** The assertion uses `len % 4096 == 0`. The
   plan tells the implementer to read `PreallocFileSink`'s `chunk` rather than
   trust it, but if the default differs the test is wrong in a way that still
   passes — the worst kind. Worth checking first.
2. **Task 4 may be vacuous.** If `commit()` under an injected fsync failure
   returns `Err`, the assertion `!acked || recovered` is trivially true. That is
   the *correct* behaviour, but the test then proves little. The implementer
   should say so and consider asserting the `Err` explicitly instead.
3. **Task 5's outcome is genuinely unknown**, and that is deliberate. It is the
   one task whose result cannot be predicted from the spec, which is also why it
   is last.
4. **The `env::set_var` pattern** is `unsafe` and process-global. `--test-threads=1`
   is required and stated, but a future test added to this file without it would
   race. Task 6's record should say so.
