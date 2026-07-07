# Config Builder API + `#[non_exhaustive]` (task44) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `StoreConfig` builder and `Persistence` constructors, then mark `StoreConfig`, `Persistence` (+ its `Standalone`/`Smr` variants), and the config mode enums `#[non_exhaustive]` — making future field/variant additions non-breaking. Migrate all in-repo external construction sites.

**Architecture:** Additive builder + constructors first (nothing breaks), then the `#[non_exhaustive]` attributes (breaks external literal construction), then a compiler-driven fix of every flagged site. In-crate `src/` construction is unaffected by `#[non_exhaustive]`.

**Spec:** `docs/superpowers/specs/2026-07-07-config-builder-design.md`

## Global Constraints

- Branch: `fix/config-builder` (already created).
- Fields on `StoreConfig` stay `pub`.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` must pass at the end.
- The full existing suite must pass: `cargo test`, `cargo test --features persistence`, `cargo test --features fulltext`, `cargo test -p ultima-vector`.
- `build()` is infallible; validation stays at `Store::new`.

---

### Task 1: Builder + constructors (additive, zero breakage)

**Files:**
- Modify: `src/store.rs` — after the `impl Default for StoreConfig` block (~line 129)
- Modify: `src/persistence.rs` — in the `impl Persistence` block (~line 108, next to `standalone_fast`)
- Test: `src/store.rs` and `src/persistence.rs` test modules

**Interfaces:**
- Produces: `StoreConfig::builder() -> StoreConfigBuilder`; `StoreConfigBuilder` with per-field setters returning `Self` and `build(self) -> StoreConfig`; `Persistence::standalone(dir, durability, wal_write) -> Persistence`; `Persistence::smr(dir) -> Persistence`.

- [ ] **Step 1: Write the builder unit tests.** Add to the `tests` module in `src/store.rs`:

```rust
    #[test]
    fn builder_matches_literal_all_fields() {
        let built = StoreConfig::builder()
            .num_snapshots_retained(3)
            .auto_snapshot_gc(false)
            .writer_mode(WriterMode::MultiWriter)
            .isolation_level(IsolationLevel::Serializable)
            .require_explicit_version(true)
            .build();
        assert_eq!(built.num_snapshots_retained, 3);
        assert!(!built.auto_snapshot_gc);
        assert_eq!(built.writer_mode, WriterMode::MultiWriter);
        assert_eq!(built.isolation_level, IsolationLevel::Serializable);
        assert!(built.require_explicit_version);
    }

    #[test]
    fn builder_default_equals_config_default() {
        let built = StoreConfig::builder().build();
        let def = StoreConfig::default();
        assert_eq!(built.num_snapshots_retained, def.num_snapshots_retained);
        assert_eq!(built.auto_snapshot_gc, def.auto_snapshot_gc);
        assert_eq!(built.writer_mode, def.writer_mode);
        assert_eq!(built.isolation_level, def.isolation_level);
        assert_eq!(built.require_explicit_version, def.require_explicit_version);
    }
```

This requires `WriterMode` and `IsolationLevel` to derive `PartialEq + Debug` — verify they do (they are simple enums; add the derives in Step 3 if missing).

- [ ] **Step 2: Run the tests to verify they fail (compile error — no `builder`).**

Run: `cargo test --lib builder_matches_literal_all_fields`
Expected: FAIL to compile — `no function or associated item named 'builder'`.

- [ ] **Step 3: Add the builder.** In `src/store.rs`, immediately after the `impl Default for StoreConfig { … }` block, add:

```rust
impl StoreConfig {
    /// Start building a [`StoreConfig`]. Chain the setters you need, then
    /// call [`StoreConfigBuilder::build`]. Unset fields take their
    /// [`Default`] values.
    ///
    /// ```
    /// use ultima_db::{StoreConfig, WriterMode};
    /// let config = StoreConfig::builder()
    ///     .writer_mode(WriterMode::MultiWriter)
    ///     .num_snapshots_retained(5)
    ///     .build();
    /// ```
    pub fn builder() -> StoreConfigBuilder {
        StoreConfigBuilder::default()
    }
}

/// Builder for [`StoreConfig`]. Obtain via [`StoreConfig::builder`].
#[derive(Debug, Clone, Default)]
pub struct StoreConfigBuilder {
    config: StoreConfig,
}

impl StoreConfigBuilder {
    /// See [`StoreConfig::num_snapshots_retained`].
    pub fn num_snapshots_retained(mut self, n: usize) -> Self {
        self.config.num_snapshots_retained = n;
        self
    }
    /// See [`StoreConfig::auto_snapshot_gc`].
    pub fn auto_snapshot_gc(mut self, enabled: bool) -> Self {
        self.config.auto_snapshot_gc = enabled;
        self
    }
    /// See [`StoreConfig::writer_mode`].
    pub fn writer_mode(mut self, mode: WriterMode) -> Self {
        self.config.writer_mode = mode;
        self
    }
    /// See [`StoreConfig::isolation_level`].
    pub fn isolation_level(mut self, level: IsolationLevel) -> Self {
        self.config.isolation_level = level;
        self
    }
    /// See [`StoreConfig::require_explicit_version`].
    pub fn require_explicit_version(mut self, required: bool) -> Self {
        self.config.require_explicit_version = required;
        self
    }
    /// See [`StoreConfig::persistence`].
    #[cfg(feature = "persistence")]
    pub fn persistence(mut self, persistence: crate::persistence::Persistence) -> Self {
        self.config.persistence = persistence;
        self
    }
    /// Finalize the configuration.
    pub fn build(self) -> StoreConfig {
        self.config
    }
}
```

Verify `WriterMode` and `IsolationLevel` derive `Debug, Clone, Copy, PartialEq, Eq` — if any is missing (needed by the test's `assert_eq!`), add it to their `#[derive(...)]`.

- [ ] **Step 4: Run the builder tests to verify they pass.**

Run: `cargo test --lib builder_`
Expected: 2 passed.

- [ ] **Step 5: Write the Persistence constructor tests.** Add to the `tests` module in `src/persistence.rs` (create the module if none exists: `#[cfg(test)] mod tests { use super::*; … }`):

```rust
    #[test]
    fn standalone_constructor_matches_literal() {
        let c = Persistence::standalone("/tmp/x", Durability::Consistent, WalWrite::Coalesced);
        match c {
            Persistence::Standalone { dir, durability, wal_write } => {
                assert_eq!(dir, std::path::PathBuf::from("/tmp/x"));
                assert_eq!(durability, Durability::Consistent);
                assert_eq!(wal_write, WalWrite::Coalesced);
            }
            _ => panic!("expected Standalone"),
        }
    }

    #[test]
    fn smr_constructor_matches_literal() {
        match Persistence::smr("/tmp/y") {
            Persistence::Smr { dir } => assert_eq!(dir, std::path::PathBuf::from("/tmp/y")),
            _ => panic!("expected Smr"),
        }
    }
```

- [ ] **Step 6: Run to verify they fail (no `standalone`/`smr`).**

Run: `cargo test --lib --features persistence standalone_constructor_matches_literal`
Expected: FAIL to compile.

- [ ] **Step 7: Add the constructors.** In `src/persistence.rs`, in the `impl Persistence` block (next to `standalone_fast`), add:

```rust
    /// Construct a [`Persistence::Standalone`] config: UltimaDB owns
    /// durability via WAL + checkpoints in `dir`.
    pub fn standalone(
        dir: impl Into<PathBuf>,
        durability: Durability,
        wal_write: WalWrite,
    ) -> Self {
        Persistence::Standalone {
            dir: dir.into(),
            durability,
            wal_write,
        }
    }

    /// Construct a [`Persistence::Smr`] config: checkpoint-only, for
    /// deployments where a consensus log provides durability.
    pub fn smr(dir: impl Into<PathBuf>) -> Self {
        Persistence::Smr { dir: dir.into() }
    }
```

- [ ] **Step 8: Run the constructor tests.**

Run: `cargo test --lib --features persistence standalone_constructor smr_constructor`
Expected: 2 passed.

- [ ] **Step 9: Commit.**

```bash
git add src/store.rs src/persistence.rs
git commit -m "feat(config): StoreConfig builder + Persistence constructors (task44)"
```

---

### Task 2: `#[non_exhaustive]` + compiler-driven migration

**Files:**
- Modify: `src/store.rs` — `StoreConfig` (~line 91), `WriterMode` (~line 52), `IsolationLevel` (~line 70)
- Modify: `src/persistence.rs` — `Persistence` enum + `Standalone`/`Smr` variants (~line 86), `Durability` (~line 38), `WalWrite` (~line 63)
- Modify (migration): every external construction site the compiler flags across `examples/ benches/ tests/ ultima_vector/ bench_workloads/ autobench/`

**Interfaces:**
- Consumes: the builder + constructors from Task 1.
- Produces: the four config types + mode enums are `#[non_exhaustive]`; all in-repo external sites use the builder/constructors.

- [ ] **Step 1: Add the attributes.** Add `#[non_exhaustive]` immediately above each of:
  - `pub struct StoreConfig {` in `src/store.rs`
  - `pub enum WriterMode {` in `src/store.rs`
  - `pub enum IsolationLevel {` in `src/store.rs`
  - `pub enum Persistence {` in `src/persistence.rs`
  - `pub enum Durability {` in `src/persistence.rs`
  - `pub enum WalWrite {` in `src/persistence.rs`

  And add `#[non_exhaustive]` on the `Standalone` and `Smr` variants inside `Persistence` (above each variant name). Do NOT mark `None`.

  Note: `#[non_exhaustive]` on a variant with `#[default]` (there is none here — `None` is the default and stays bare) is fine. `WalWrite`'s `#[default]` is on `PerEntry` (a unit variant, unmarked) — leave it.

- [ ] **Step 2: Build in-crate first to confirm `src/` is unaffected.**

Run: `cargo build --features "persistence fulltext"`
Expected: builds clean (in-crate construction is exempt from `#[non_exhaustive]`).

- [ ] **Step 3: Enumerate every broken external site.**

Run: `cargo build --all-targets --all-features 2>&1 | grep -E "error\[E0639\]|error\[E0638\]|error\[.*\]|-->" | head -120`
Expected: a list of E0639 (cannot create non-exhaustive struct/variant outside crate) / E0638 (non-exhaustive match) errors with file:line locations. This is the migration worklist.

- [ ] **Step 4: Migrate each flagged site.** Apply these transformations mechanically (compiler drives the list; repeat build+fix until clean):

  - `StoreConfig { a: x, b: y, ..Default::default() }` → `StoreConfig::builder().a(x).b(y).build()`.
  - `StoreConfig { ..Default::default() }` or a bare `StoreConfig::default()` — already fine (default() is allowed); no change.
  - A site that does `let mut c = StoreConfig::default(); c.field = v;` — already fine; no change.
  - `Persistence::Standalone { dir: d, durability: dur, wal_write: w }` → `Persistence::standalone(d, dur, w)`.
  - `Persistence::Smr { dir: d }` → `Persistence::smr(d)`.
  - Any external exhaustive `match`/`if let` chain on `Persistence`/`Durability`/`WalWrite`/`WriterMode`/`IsolationLevel` missing a catch-all → add a `_ => …` arm (only if the compiler flags E0638; single-pattern `matches!` and `if let` do not need it).

  Work file-by-file; after each file, re-run the Step 3 build to shrink the list.

- [ ] **Step 5: Confirm the whole workspace builds.**

Run: `cargo build --all-targets --all-features 2>&1 | tail -3`
Expected: `Finished` with no errors.

- [ ] **Step 6: Run the full test matrix.**

Run:
```
cargo test --all-features 2>&1 | grep -E "test result: FAILED|error" ; echo done1
cargo test -p ultima-vector --all-features 2>&1 | grep -E "FAILED|error" ; echo done2
```
Expected: no FAILED/error lines. (If `store::tests::mock_wal_incremental_flush` fails once, re-run it in isolation — it is a known pre-existing flake, not caused by this change.)

- [ ] **Step 7: Commit.**

```bash
git add -A
git commit -m "feat(config): mark StoreConfig/Persistence/mode enums non_exhaustive; migrate sites (task44)"
```

---

### Task 3: Docs, rustdoc examples, full verification

**Files:**
- Create: `docs/tasks/task44_config_builder.md`
- Modify: `CLAUDE.md` (construction guidance)
- Modify: any rustdoc example showing `StoreConfig { … }` (find via grep)

**Interfaces:**
- Consumes: Tasks 1–2.

- [ ] **Step 1: Find rustdoc/doc examples using the struct literal.**

Run: `grep -rn "StoreConfig {" src/ CLAUDE.md README.md docs/configuration.md 2>/dev/null | grep -v "test\|StoreConfigBuilder\|struct StoreConfig"`
Expected: a short list of doc-comment / markdown examples. Convert each `StoreConfig { … }` to `StoreConfig::builder()….build()` and each `Persistence::Standalone { … }` to `Persistence::standalone(…)`. (In-crate `src/` *code* literals are fine and must NOT be changed — only doc-comment examples that render as doctests or user guidance.)

- [ ] **Step 2: Write the feature doc** — `docs/tasks/task44_config_builder.md`:

```markdown
# Task 44: Config Builder API + `#[non_exhaustive]`

**Origin:** 2026-06 deep-review deferred backlog — `#[non_exhaustive]` on
`StoreConfig`/`Persistence` was blocked by the `..Default::default()`
struct-literal construction style; it needed a builder first.

(Numbering: task41 is used twice on main — index-DDL conflict fix and the Elle
consistency harness; 42 = CommitWaiter timeout, 43 = Unicode tokenizer. This is
task44.)

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

- In-crate construction (`src/`) is exempt from `#[non_exhaustive]` and was left
  as struct literals.
- External crates construct via the builder/constructors. `default()`-then-
  mutate on public fields still works and was left as-is where used.
- All in-repo external sites (examples, benches, tests, `ultima_vector`,
  `bench_workloads`, `autobench`) were migrated; the change was compiler-driven
  (`cargo build --all-targets --all-features` enumerated every site).

Design history: `docs/superpowers/specs/2026-07-07-config-builder-design.md`,
`docs/superpowers/plans/2026-07-07-config-builder.md`.
```

- [ ] **Step 3: Update CLAUDE.md.** Find the sentence in the `Store` architecture bullet that says construction is via `Store::new(StoreConfig)` (or `Store::default()`). Append: `Build `StoreConfig` via `StoreConfig::builder()....build()` (the config types are `#[non_exhaustive]`; see task44).` Adjust wording to fit the existing sentence.

- [ ] **Step 4: Full verification.**

Run:
```
cargo test --all-features 2>&1 | grep -E "test result: FAILED|^error" ; echo t-done
cargo test -p ultima-vector --all-features 2>&1 | grep -E "FAILED|^error" ; echo v-done
cargo clippy --workspace --all-targets --all-features -- -D warnings 2>&1 | grep -iE "warning|^error" ; echo c-done
```
Expected: no failures, no warnings.

- [ ] **Step 5: Commit.**

```bash
git add docs/tasks/task44_config_builder.md CLAUDE.md src/ README.md docs/configuration.md
git commit -m "docs: task44 config builder feature doc; builder in rustdoc/CLAUDE.md"
```
