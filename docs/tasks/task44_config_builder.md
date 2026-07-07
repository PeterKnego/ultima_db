# Task 44: Config Builder API + `#[non_exhaustive]`

**Origin:** 2026-06 deep-review deferred backlog — `#[non_exhaustive]` on
`StoreConfig`/`Persistence` was blocked by the `..Default::default()`
struct-literal construction style; it needed a builder first.

(Numbering note: task41 = index-DDL conflict fix (the Elle harness that briefly
shared task41 is now task45); 42 = CommitWaiter timeout, 43 = Unicode tokenizer.
This is task44.)

## What changed

- `StoreConfig::builder() -> StoreConfigBuilder` with chainable per-field
  setters and an infallible `build()`. Fields stay `pub`; the builder is the
  blessed constructor. `build()` does no validation — the
  `ConsistentInline + MultiWriter` check stays at `Store::new`.
- `Persistence::standalone(dir, durability, wal_write)` and
  `Persistence::smr(dir)` constructors (alongside the existing
  `standalone_fast`).
- `#[non_exhaustive]` on `StoreConfig`, `Persistence` (+ its `Standalone`/`Smr`
  variants), and the mode enums `Durability`, `WalWrite`, `WriterMode`,
  `IsolationLevel`. `Persistence::None` stays a bare, nameable unit variant.

## Why

Pre-1.0 API-evolution insurance: adding a `StoreConfig` field, a
`Persistence::Standalone` field (e.g. a new WAL knob), or a new mode variant is
now non-breaking for downstream crates.

## Migration notes

- In-crate construction (`src/`, including test modules) is exempt from
  `#[non_exhaustive]` and was left as struct literals.
- External crates construct via the builder/constructors. All ~81 in-repo
  external construction sites (examples, benches, tests, `ultima_vector`,
  `bench_workloads`, `autobench`) were migrated; the change was compiler-driven
  (`cargo build --all-targets --all-features` enumerated every site). One
  external `match` on `Persistence` (smallbank_bench) gained a `_` arm.
- `default()`-then-mutate on public fields still works and was left as-is where
  used.

## API notes

- `#[non_exhaustive]` on the `Persistence` *enum* only forces a `_` arm in
  external exhaustive matches; it does not block naming existing variants
  (`Persistence::None`, `matches!(p, Persistence::None)` still compile). The
  `Standalone`/`Smr` *variants* are separately `#[non_exhaustive]`, which is
  what blocks external literal construction and routes callers through the
  constructors — so their fields can grow later without breaking downstream.

Design history: `docs/superpowers/specs/2026-07-07-config-builder-design.md`,
`docs/superpowers/plans/2026-07-07-config-builder.md`.
