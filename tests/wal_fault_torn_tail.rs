// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! In-flight WAL fault F1: a torn tail costs a **strict**-scan store its whole
//! log, including commits it already acknowledged as durable.
//!
//! This is the executable counterpart of a TLA property that has never had one:
//! `StrictScanErrLosesDurableAck` (`formal/tla/wal/RESULTS.md:197`, config
//! `formal/tla/wal/StrictScanErr.cfg`), carried as a *checked owed property* —
//! `make formal/tla-model` asserts it reports **violated** at depth 9, exactly
//! like a canary. The model says the loss happens. Nothing executed that claim
//! until this file.
//!
//! ## The two scanners — the whole subject of this file
//!
//! One function, `scan_wal(path, tail_tolerant)` (`src/wal.rs:677`), reads the
//! same torn tail two opposite ways, and which one you get is decided by the
//! sink kind:
//!
//! | caller | `tail_tolerant` | CRC mismatch |
//! |---|---|---|
//! | `Store::recover` for `CoalescedPrealloc` (`src/store.rs:1073-1078`) | `true` | `break` — clean end-of-log |
//! | `Store::recover` for `PerEntry` / `Coalesced` (same lines) | `false` | `Err(WalCorrupted)`, propagated by `?` **before any entry is applied** |
//! | `read_wal` / `prune_wal` (`src/wal.rs:730-732`) | `false` | same hard error |
//! | `PreallocFileSink::open_with_chunk` (`src/wal.rs:1192`) | `true` | `break` |
//!
//! A test that observes this tail through the wrong scanner asserts on the
//! wrong error and pins the wrong behaviour, so the single test below is split
//! into labelled phases and every assertion says which scanner it exercises.
//! Phase 2 is the **strict** scanner (`WalWrite::PerEntry`) and records the
//! harm; phase 3 replays the byte-identical file through the **tolerant** one
//! (`WalWrite::CoalescedPrealloc`) and exists precisely to prove the bytes the
//! strict scan threw away were readable all along. Phase 1's writes also go
//! through the prealloc sink, because it is the only sink with a positioned
//! batch write there is anything to tear.
//!
//! ## Aiming a process-wide fault at one batch
//!
//! `crate::mutation::active()` memoises `ULTIMA_MUTATION` in a `OnceLock`, so
//! the first read wins for the whole process: `TearFrameAt` fires on **every**
//! `PreallocFileSink::sync`, not on a chosen one. It cannot be armed late.
//!
//! It can, however, be aimed by *size*. The injection writes
//! `&self.buf[..n.min(self.buf.len())]` (`src/wal.rs:1236-1242`), so a batch no
//! larger than `n` is written **whole** and only a batch larger than `n` tears.
//! Seeding with small single-row commits (32 bytes each, measured) and ending
//! with one large multi-row commit (5502 bytes, measured) puts the tear on the
//! last batch and nowhere else. `TEAR_AT` is therefore load-bearing in both
//! directions; see its docs and the calibration below.
//!
//! The same memoisation is why this file exists at all rather than being a
//! second test in `wal_fault_fsync.rs` or `wal_fault_failed_extend.rs`: a
//! co-resident test with a different value silently runs the *first* test's
//! fault and still passes, because a wrong-fault `Err` is still an `Err`.
//! `--test-threads=1` does not help — it is one process either way. Cargo gives
//! one binary per `tests/*.rs`, so process isolation is free. **Do not add a
//! test with a different `ULTIMA_MUTATION` value to this file.**
//!
//! ## Calibration — what this test can and cannot detect, measured 2026-08-06
//!
//! A test that passes against a changed system proves nothing, so both halves
//! of the strict/tolerant contrast were falsified on purpose. Run each mutation
//! on its own against an otherwise clean tree, then **restore with `git
//! checkout` and `touch src/wal.rs`** — cargo decides what to rebuild from
//! mtime, and a restore that preserves the original timestamp leaves the
//! *mutated* binary in place.
//!
//! ```text
//! cargo test --features persistence,fulltext,mutation-testing \
//!            --test wal_fault_torn_tail -- --ignored
//! ```
//!
//! **Mutation S — the policy is fixed at tolerant.** In `scan_wal`'s CRC arm
//! (`src/wal.rs:702`) replace `if tail_tolerant` with `if true`. Red at the
//! strict `match`: *"a strict scan over a torn tail must report WalCorrupted at
//! the torn frame, got `Ok(())`"*. This is the important direction — it is what
//! a **fix for F1 would look like**, and this cell is unruled, so a red result
//! here is a prompt to re-read the question, not a regression.
//!
//! **Mutation T — the policy is fixed at strict.** Same line, `if false`. Red
//! in phase 3, at `Store::new(tolerant_config(..))`: `PreallocFileSink::open`
//! scans its own file to reconstruct the write head and now refuses it, so the
//! store cannot even be constructed. That is issue #24's shared-policy wiring
//! showing up as a test failure, and it confirms the tolerant half of the
//! contrast is bound to production code rather than assumed.
//!
//! **What no mutation can falsify.** `torn.is_ok()` — the assertion that the
//! tear was *silent* — is structurally guaranteed by `TearFrameAt`, which
//! writes a prefix and returns `Ok(())` unconditionally. No change to the store
//! can make it fail; only a change to the injection can. It is recorded as a
//! property of the fault model, and it is retained because it is what
//! distinguishes a torn tail from a short one.
//!
//! `PREALLOC_CHUNK` and the `torn_len` bound are *preconditions* rather than
//! findings: they fail loudly if the setup stops producing the state F1 needs,
//! and both still hold in the vacuous *lower*-edge configuration (`TEAR_AT <
//! 32`, where every batch tears — the len prefixes survive, so
//! `seed_frames_end` still walks to 96), so neither is evidence on its own
//! there. `torn_len + 8 > TEAR_AT` is not weak in the other direction: it is
//! what goes red at the **upper** edge, and it is the only assertion that
//! does. The two assertions that carry the property are the strict byte-offset
//! equality and the tolerant row count.
//!
//! Requires `--features mutation-testing`.
#![cfg(feature = "mutation-testing")]

use std::path::Path;
use ultima_db::{Durability, Error, Persistence, Store, StoreConfig, WalWrite};

mod common;

/// Byte offset at which the sink's positioned batch write is truncated.
///
/// **Load-bearing in both directions.** The measured window is `[32, 5502)`
/// and the test is vacuous or red outside it (all four values run 2026-08-06):
///
/// * `< 32` (one seed batch) — every batch tears, including the first. The
///   strict scan still errors, so a bare `is_err()` would pass, but it errors
///   at **byte 0**: nothing was ever intact, so no acknowledged commit was
///   lost and the property under test never happened. Measured at `TEAR_AT` 12
///   and 31: `CRC mismatch at entry starting at byte 0`, tolerant recovery
///   `Ok` with the table absent. This is the trap the plan warned about, and a
///   bare `is_err()` walks straight into it. What catches it is the
///   *byte-offset equality* against `seed_frames_end` (red: `0` vs `96`) and
///   the tolerant row count (red: table absent, not 3 rows).
/// * `>= 5502` (the whole final batch) — `n.min(buf.len())` writes everything,
///   there is no tear, and both scanners recover all 203 rows. Measured at
///   5502 and 6000; the test goes red before it gets that far, on the
///   `torn_len + 8 > TEAR_AT` precondition.
///
/// 512 sits an order of magnitude clear of both edges. The lower edge moves
/// only if the WAL entry encoding changes; the upper only if `FINAL_ROWS`
/// shrinks.
const TEAR_AT: u64 = 512;

/// Single-row commits made *before* the tear, each acknowledged durable under
/// `Durability::Consistent`. These are the "durable acked" of the property
/// name. > 1 so the loss is visibly plural rather than an off-by-one tail.
const SEED_COMMITS: usize = 3;

/// Rows in the final, torn commit. Only has to make that batch comfortably
/// larger than `TEAR_AT`; 200 rows measured 5502 bytes against a 512-byte
/// tear.
const FINAL_ROWS: usize = 200;

/// `WAL_PREALLOC_CHUNK` (`src/wal.rs:1157`), duplicated because it is private.
/// Not decoration: a file this large is *why* the torn frame is evaluated at
/// all rather than skipped by `scan_wal`'s truncation guard as a short tail.
const PREALLOC_CHUNK: u64 = 16 * 1024 * 1024;

/// Tolerant scanner: `Store::recover` passes `tail_tolerant = true` for
/// `CoalescedPrealloc` and for no other sink.
fn tolerant_config(dir: &Path) -> StoreConfig {
    StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.to_path_buf(),
            Durability::Consistent,
            WalWrite::CoalescedPrealloc,
        ))
        .build()
}

/// Strict scanner: `WalWrite::PerEntry` is the `#[default]`, and its
/// `Store::recover` passes `tail_tolerant = false`. `WalWrite::Coalesced`
/// behaves identically here; `PerEntry` is chosen because it is the default.
fn strict_config(dir: &Path) -> StoreConfig {
    StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.to_path_buf(),
            Durability::Consistent,
            WalWrite::PerEntry,
        ))
        .build()
}

/// Walk the frame length prefixes from byte 0 and return the offset one past
/// the last frame that is *structurally* complete (non-zero `len`, and
/// `len + 8` bytes available). Deliberately does **not** verify CRCs — that is
/// the discriminator under test, and re-implementing it here would make the
/// assertion a tautology of `scan_wal`. Framing is `[u32 len][payload][u32
/// crc]` (`frame_entry_into`, `src/wal.rs:602-611`).
fn structural_frames_end(bytes: &[u8]) -> u64 {
    let mut offset = 0usize;
    while offset + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        if len == 0 || offset + 8 + len > bytes.len() {
            break;
        }
        offset += 8 + len;
    }
    offset as u64
}

/// Parse the trailing byte offset out of `CRC mismatch at entry starting at
/// byte N` (`src/wal.rs:705-707`).
fn crc_mismatch_offset(msg: &str) -> u64 {
    msg.rsplit(' ')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("not a CRC-mismatch message with a byte offset: {msg}"))
}

/// UNRESOLVED — needs a ruling before it is pinned. **This is F1**, and the
/// `#[ignore]` means "awaiting a ruling", **not** "known broken". The test
/// passes; what nobody has decided is whether the behaviour it records is the
/// one UltimaDB wants.
///
/// # The question
///
/// A store acknowledged three commits as durable under `Durability::Consistent`
/// — the strongest promise the API makes short of `ConsistentInline`. A later
/// batch then tore: physically written in part, believed written in full. On
/// the next start, a **strict**-scan configuration refuses the entire log,
/// including those three, and starts with no data at all. `scan_wal` returns
/// `Err(WalCorrupted)` the moment the CRC fails, and `Store::recover`
/// propagates it with `?` **before applying a single entry** — so frames it had
/// already accepted, at offsets before the tear, are discarded along with the
/// bad one.
///
/// Two defensible answers, which is why this is a ruling and not a fix:
///
/// * **As designed.** A non-preallocated sink appends into a file whose tail is
///   *not* known-zero, so a CRC failure there is genuinely ambiguous: it could
///   be a torn write, or it could be silent corruption in the middle of a log
///   that continues past it. Refusing the file and telling the operator is the
///   conservative reading, and quietly truncating at the first bad frame would
///   turn real corruption into silent data loss.
/// * **A durability hole.** The scan already knows how many frames verified
///   before the failure, and it already returns that offset in the message. A
///   commit that returned `Ok` under `Consistent` was promised to survive; here
///   it does not, and the operator's only lever is deleting the WAL — which
///   destroys exactly the acknowledged commits. `RESULTS.md` notes the scope is
///   2 of the 3 `WalWrite` variants, **including the `#[default]` one**, under
///   both durable tiers.
///
/// The model has already been made to carry the question rather than an answer:
/// `StrictScanErrLosesDurableAck` is deliberately *not* a `RecoverySound`
/// clause, because `recover()` returned `Err` and there is no recovered state
/// to predicate over — folding it in would convert a safety claim into an
/// availability one. This test takes the same line. **Before ruling it away,
/// note that `formal/tla/wal/mutations/M4Abort.cfg` uses this property as M4's
/// target**: resolving F1 and deleting it costs M4 its witness, so the harm has
/// to be re-homed first.
///
/// # Measured behaviour — 2026-08-06
///
/// `TEAR_AT = 512`, `SEED_COMMITS = 3`, `FINAL_ROWS = 200`, ext4:
///
/// ```text
/// seed commits            Ok(1) Ok(2) Ok(3)   frames at 0, 32, 64 (32 bytes each)
/// final (torn) commit     Ok(4)               frame at 96, len prefix 5494, 512 bytes written
/// wal.bin                 16 777 216 bytes    one WAL_PREALLOC_CHUNK, rest zeros
/// STRICT   (PerEntry)     recover = Err(WalCorrupted("CRC mismatch at entry starting at byte 96"))
///                         table "t" absent, latest_version 0 — all 3 acked commits unreachable
/// TOLERANT (Prealloc)     recover = Ok(()), 3 rows — exactly the acked commits
/// ```
///
/// The two scanners are reading **byte-identical files**; the test asserts that
/// explicitly. Nothing about the bytes is unrecoverable — the policy alone
/// decides between "all three commits" and "no database".
///
/// # Why the torn frame is *evaluated* rather than skipped
///
/// `scan_wal` has two cheap end-of-log exits before the CRC check: a zero `len`
/// prefix, and `offset + 4 + len + 4 > file_len`. Neither trips. The 512 bytes
/// that landed include the intact 4-byte `len` prefix, and the file is a full
/// 16 MiB preallocated chunk, so the frame's declared extent (ending at byte
/// 5598) is comfortably inside it. That is what makes a *torn* tail different
/// from a *short* one, and it is why `TearFrameAt` returns `Ok(())` after a
/// partial write rather than an error: an error would let the sink react and
/// produce the short tail, which is already ordinary end-of-log to both
/// scanners.
#[test]
#[ignore = "unresolved: see the doc comment; do not pin until ruled on"]
fn a_torn_tail_costs_a_strict_scan_its_durably_acked_commits() {
    // SAFETY: single-threaded test binary; set before any store is built.
    unsafe { std::env::set_var("ULTIMA_MUTATION", format!("tear-frame-at={TEAR_AT}")) };
    // Real disk, not tmpfs: fsync is a no-op there, and "acknowledged durable"
    // is the entire premise of this test (see src/test_scratch.rs).
    let dir = common::test_scratch::scratch_dir();
    let wal = dir.path().join("wal.bin");

    // ── Phase 1: acknowledged commits, then a torn batch ──────────────────
    // Written through the prealloc sink because that is the only sink with a
    // positioned batch write to tear. What is torn is a physically ordinary
    // event on any appending sink; the sink kind here is the injection's
    // requirement, not the property's.
    let store = Store::new(tolerant_config(dir.path())).unwrap();
    store.register_table::<String>("t").unwrap();

    for i in 0..SEED_COMMITS {
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert(format!("seed{i}")).unwrap();
        }
        // Under `Consistent` the committing thread parks until the WAL thread
        // has fsynced, so this `Ok` *is* the durability acknowledgement the
        // property is about. Each of these batches is smaller than `TEAR_AT`
        // and is therefore written whole.
        wtx.commit()
            .unwrap_or_else(|e| panic!("seed commit {i} was not acknowledged durable: {e:?}"));
    }
    let seed_frames_end = structural_frames_end(&std::fs::read(&wal).unwrap());

    let mut wtx = store.begin_write(None).unwrap();
    {
        let mut t = wtx.open_table::<String>("t").unwrap();
        for i in 0..FINAL_ROWS {
            t.insert(format!("final-row-{i}")).unwrap();
        }
    }
    let torn = wtx.commit();
    drop(store);

    // The tear is *silent*. Structurally guaranteed by the injection, which
    // writes a prefix and then returns `Ok(())` — no production code path can
    // falsify this, and it is recorded as a property of the fault model rather
    // than of the store. It matters because it is what makes the tail torn
    // rather than short: the sink advanced `write_head` by the whole batch.
    assert!(
        torn.is_ok(),
        "TearFrameAt is supposed to report success after a partial write; the \
         injection changed shape and this test no longer models a torn tail: {torn:?}"
    );

    // ── The on-disk state both scanners are about to read ─────────────────
    let bytes_before = std::fs::read(&wal).unwrap();

    // A full preallocated chunk. This is a *precondition*, not decoration:
    // `scan_wal`'s truncation guard (`offset + 4 + len + 4 > file_len`) is what
    // would otherwise turn this into an ordinary short tail, and it cannot trip
    // inside 16 MiB of zeros.
    assert_eq!(
        bytes_before.len() as u64,
        PREALLOC_CHUNK,
        "wal.bin is not one preallocated chunk, so the torn frame may be \
         reported as a short tail instead of a CRC mismatch and this test is \
         no longer about F1"
    );

    // The torn frame's length prefix survived the tear, so the scan will reach
    // it and evaluate it rather than stopping short — the whole mechanism.
    //
    // Note what this does *not* establish: `structural_frames_end` reads length
    // prefixes and never checks a CRC, so it cannot tell an intact seed frame
    // from a torn one, and `seed_frames_end > 0` would hold even if every batch
    // had torn. Deliberately so — verifying CRCs here would make the assertion
    // a re-implementation of the code under test. What the seed frames actually
    // *contain* is established at the end, by the tolerant row count.
    assert!(
        seed_frames_end > 0,
        "the {SEED_COMMITS} seed commits left no frame at all, so there is \
         nothing for a strict scan to walk past"
    );
    let torn_len =
        u32::from_le_bytes(bytes_before[seed_frames_end as usize..][..4].try_into().unwrap());
    // `+ 8` because the tear threshold is compared against the whole framed
    // batch (`[u32 len][payload][u32 crc]`), while `torn_len` is the payload.
    assert!(
        torn_len as u64 + 8 > TEAR_AT,
        "the batch at byte {seed_frames_end} is {} bytes, which the {TEAR_AT}-byte \
         tear did not truncate; TEAR_AT is at or above the final batch and \
         nothing tore",
        torn_len as u64 + 8
    );

    // ── Phase 2: the STRICT scanner (WalWrite::PerEntry) ──────────────────
    // `Store::new` succeeds: the append-mode sinks' `reject_unreadable_wal`
    // inspects only the *first* record's header, which is one of the good ones.
    // The failure therefore surfaces where it should, at `recover()`.
    let strict = Store::new(strict_config(dir.path())).unwrap();
    strict.register_table::<String>("t").unwrap();
    let recovered = strict.recover();

    // Pin the error *identity* and its byte offset, not `is_err()`. A
    // wrong-fault error is still an error — and, worse, the *right* error at
    // byte 0 would mean nothing intact preceded the tear, i.e. the vacuous
    // configuration this test is calibrated to avoid.
    match &recovered {
        Err(Error::WalCorrupted(msg)) => assert_eq!(
            crc_mismatch_offset(msg),
            seed_frames_end,
            "the strict scan failed somewhere other than the torn frame: {msg}"
        ),
        other => panic!(
            "a strict scan over a torn tail must report WalCorrupted at the \
             torn frame, got {other:?}"
        ),
    }

    // **The harm.** `recover()` propagated with `?` before applying anything,
    // so the three acknowledged commits are not merely rolled back to some
    // earlier version — the table does not exist and the store is at version 0.
    assert!(
        matches!(
            strict.begin_read(None).unwrap().open_table::<String>("t"),
            Err(Error::TableNotFound(_))
        ),
        "the strict store reached table \"t\" after a failed recover; the harm \
         this test records has changed shape"
    );
    assert_eq!(
        strict.latest_version(),
        0,
        "a strict recover that returned Err still installed a version"
    );
    drop(strict);

    // ── Phase 3: the TOLERANT scanner, on the same bytes ──────────────────
    // The contrast is the point: nothing was destroyed on disk, and nothing is
    // unreadable. Assert byte-identity first so "same file" is a measurement
    // rather than an assumption — the strict store opened the WAL in append
    // mode and a future change there would otherwise silently invalidate this.
    assert!(
        std::fs::read(&wal).unwrap() == bytes_before,
        "the strict store modified wal.bin, so the tolerant scan below is not \
         reading the same bytes and the comparison proves nothing"
    );

    let tolerant = Store::new(tolerant_config(dir.path())).unwrap();
    tolerant.register_table::<String>("t").unwrap();
    tolerant
        .recover()
        .expect("the tolerant scanner treats a torn tail as end-of-log and must recover");

    // Exactly the acknowledged commits: the torn batch is dropped (it was never
    // acked... except it *was*, see the doc comment's second reading), and
    // every commit that was acked comes back. This is also the anti-vacuity
    // guard on `TEAR_AT` from the other side — a too-small value gives 0 rows
    // here, a too-large one gives SEED_COMMITS + FINAL_ROWS.
    let rows = tolerant
        .begin_read(None)
        .unwrap()
        .open_table::<String>("t")
        .expect("tolerant recovery must reach the table")
        .len();
    assert_eq!(
        rows, SEED_COMMITS,
        "tolerant recovery reached {rows} rows, not the {SEED_COMMITS} \
         acknowledged ones; TEAR_AT is outside its calibrated window (see its \
         docs) and the strict/tolerant contrast above is not the one described"
    );

    // Hygiene only: `active()` has already memoised, so this does not
    // deactivate the fault for the rest of the process.
    unsafe { std::env::remove_var("ULTIMA_MUTATION") };
}
