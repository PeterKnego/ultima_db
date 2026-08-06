# WAL in-flight fault injection — design

**Date:** 2026-08-06
**Status:** approved design, pre-implementation
**Retires:** the untested half of the F2 fix (`1e5d2b7`), the F1 property that
exists only in the TLA model, and the entirely-untested fsync failure path.

> **Cites re-anchored 2026-08-06 (task60).** Every `src/*.rs:LINE` below names
> the **current** tree, not the tree this document was written against. The
> implementation's own `#[cfg]`-gated insertions in `src/wal.rs` moved the
> lines it cites — e.g. the F2 rollback moved from `:1199` to `:1223-1224` —
> and nothing checks cites outside `formal/tla/wal/`, so these were verified
> by hand against the source. See `docs/tasks/task60_wal_inflight_faults.md` §8.

## The gap, stated precisely

`tests/corruption_recovery.rs` already covers **post-hoc** corruption with 11
tests: truncated tails, zero tails, garbage tails, bit-flips in the first and
last entries, and three checkpoint-corruption cases. That is real coverage and
this spec does not duplicate it.

What is missing is the other kind of fault, and the distinction is **when**:

- **Post-hoc** — corrupt the file, *then* recover. Covered. Needs no seam: the
  test just writes bytes.
- **In-flight** — a syscall fails *partway through an operation*, while the sink
  still holds in-memory state, leaving the two inconsistent. **No coverage, and
  no way to produce it without a seam in the write path.**

F2 is the canonical example. `preallocate_to` (`src/wal.rs:628`) zero-fills
`[from, to)` then `sync_all`s, establishing task37 §4 invariant 2: *the size is
durable before any record is written into the region*. When ENOSPC interrupts
the zero-fill, the error escapes before `sync_all` and before
`self.capacity = new_cap`, so the file is physically longer than `capacity` and
that extension was never synced. The fix (`src/wal.rs:1223`) rolls the size back
with `set_len`. **That rollback has never been executed by a test** — its
accompanying test is explicitly a regression guard whose assertions held before
the fix too, because a read-only handle fails on the *first* write and leaves no
partial extension to roll back.

You cannot construct that state by editing the file afterwards. The failure has
to happen while the sink is mid-operation.

## What to inject, and what each retires

| fault | injects at | retires |
|---|---|---|
| **Short write / ENOSPC** after N bytes of a `write_all` | `preallocate_to`'s zero-fill loop; the sinks' batch write | F2's rollback (`src/wal.rs:1223-1224`), currently unproven |
| **`sync_all` / `sync_data` returns an error** | `preallocate_to`'s sync; `WalSink::sync` (`:1079`, `:1128`, `:1204`) | the fsync failure path — no test anywhere assumes fsync can fail |
| **Torn frame at a chosen offset**, under a **strict-scan** config | the sink's positioned write | F1 — `StrictScanErrLosesDurableAck` exists only as a TLA property |

The third is the one the existing corruption suite structurally cannot reach:
every one of its 11 tests uses `Durability::Eventual` + `WalWrite::PerEntry`,
and F1 is about `CoalescedPrealloc`'s tolerant scan versus the strict path.

## The seam

**Extend `src/mutation.rs`, do not invent a second mechanism.** It already has
the right shape (task47): compiled only under the `mutation-testing` feature
(`Cargo.toml:49`), selected at runtime by `ULTIMA_MUTATION`, and feature-on with
the variable unset behaves normally. Call sites are a `#[cfg]`-gated `matches!`
against `crate::mutation::active()` (`src/store.rs:4438-4444`).

**But the existing enum has no room for these.** Its three variants are logic
switches with no payload; I/O faults need parameters — *which* write fails, and
after how many bytes. So:

```rust
FailWriteAfter(u64),   // nth byte of the next write_all fails with ENOSPC
FailSync,              // the next sync_all/sync_data returns an error
TearFrameAt(u64),      // truncate the positioned write at this offset
```

That makes `active()`'s `OnceLock<Option<Mutation>>` insufficient — a payload
parsed once per process is fine, but a *counter* ("fail the 3rd write") is not.
**Decision: keep it process-global and parameter-only, no counters.** A test
that needs a specific write to fail arranges for it to be the only one, which
every case here allows. Counters would make the harness stateful and the tests
order-dependent, which is precisely what this repo's deterministic-test
discipline exists to avoid.

## The oracle

Not "does it return an error" — that tests the injection, not the system. The
assertion is the **durability contract**, checked after recovery:

1. **Every acknowledged commit survives.** Anything a `commit()` returned `Ok`
   for is present after recovery. This is the same umbrella property task59 uses,
   and it is what F1 violates.
2. **In-memory state never claims more than disk holds.** After a failed extend,
   `capacity` must not exceed the durable file size — that is F2's invariant,
   stated so a test can check it.
3. **A failure is either clean or loud.** An injected fault must produce either
   a correct recovery or a reported error, never a silent partial state.

## Scope

**In:** the three faults above; `Persistence::Standalone`; both
`WalWrite::PerEntry` and `CoalescedPrealloc` (F1 needs the latter); recovery
assertions per the oracle.

**Out, deliberately:**

- **Post-hoc corruption** — already covered by `tests/corruption_recovery.rs`.
  Do not duplicate it.
- **Counter-based injection** ("fail the 3rd write") — see the seam decision.
- **Faults in checkpoint writing** — a separate surface with its own recovery
  path; worth doing, not here.
- **`Persistence::Smr`** — checkpoint-only, so the WAL write path is not
  exercised.

## Success criteria

1. **F2's rollback is executed by a test that fails without it.** Revert the
   `set_len` at `src/wal.rs:1223` and a named test must go red. This is the
   whole reason the work is being done — the fix currently ships unproven.
2. **F1 is reproduced against real I/O**, not just in the model: a strict-scan
   config loses access to a durably-acked commit on a torn tail, and the test
   states plainly whether that is the behaviour being pinned or a defect awaiting
   a ruling.
3. **fsync failure produces a clean or loud outcome**, never silent partial
   durability.
4. Every test fails against the code it targets when that code is reverted —
   demonstrated, not asserted. A test that cannot fail is worse than none, and
   this project has rejected several.
5. Deterministic: no threads, no timing. The fault is chosen by an environment
   variable read once, so a run is reproducible.

## Risks

- **Production code carries fault-injection branches.** task47 set the precedent
  and the `mutation-testing` feature keeps them out of shipped builds, but this
  would be the second such module. **Someone should decide whether that is a
  pattern or an accumulation** — that is a plan-owner call, not an implementer's,
  and it should be made before the code lands rather than after.
- **The WAL background thread** owns the sink in `Standalone` mode, so an
  injected error surfaces through the durability channel rather than as a direct
  return. Tests must assert on what a *caller* observes, not on internal state.
- **F1 may be a defect rather than a behaviour.** It is currently carried as an
  *owed property* — the model asserts it happens. If the executable test makes
  the consequence vivid enough to change someone's mind, that is a finding, and
  the test should not pin it as correct until ruled on.
