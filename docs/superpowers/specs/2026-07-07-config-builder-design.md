# Config builder API + `#[non_exhaustive]` — design

**Date:** 2026-07-07
**Status:** approved (brainstormed with Peter)
**Origin:** 2026-06 deep-review deferred backlog — "`#[non_exhaustive]` on
`StoreConfig`/`Persistence`: incompatible with the `..Default::default()`
struct-literal construction style used everywhere; would need a builder API
first."
**Task number:** task44 (task41 doubled — index-DDL + Elle; 42 CommitWaiter, 43
Unicode tokenizer).

## Goal

Pre-1.0 API-evolution insurance: make it non-breaking to add fields to
`StoreConfig`, add fields to `Persistence::Standalone`/`Smr`, and add new
variants to the config mode enums — by marking them `#[non_exhaustive]` and
providing ergonomic non-literal constructors.

## Decisions (with Peter, 2026-07-07)

- **Comprehensive scope**: `#[non_exhaustive]` on `StoreConfig`, `Persistence`
  (enum + `Standalone`/`Smr` variants), and the mode enums `Durability`,
  `WalWrite`, `WriterMode`, `IsolationLevel`.
- **`StoreConfig` fields stay `pub`.** A builder is added as the blessed
  constructor, but external `default()`-then-mutate code (`config.writer_mode =
  …`) keeps working; only struct-literal construction sites migrate.

## Design

### StoreConfig builder (`src/store.rs`)

```rust
#[non_exhaustive]
pub struct StoreConfig { /* existing pub fields, unchanged */ }

impl StoreConfig {
    /// Start building a config. Chain setters, then `.build()`.
    pub fn builder() -> StoreConfigBuilder { StoreConfigBuilder::default() }
}

#[derive(Debug, Clone, Default)]
pub struct StoreConfigBuilder { config: StoreConfig }

impl StoreConfigBuilder {
    pub fn num_snapshots_retained(mut self, n: usize) -> Self { self.config.num_snapshots_retained = n; self }
    pub fn auto_snapshot_gc(mut self, b: bool) -> Self { self.config.auto_snapshot_gc = b; self }
    pub fn writer_mode(mut self, m: WriterMode) -> Self { self.config.writer_mode = m; self }
    pub fn isolation_level(mut self, l: IsolationLevel) -> Self { self.config.isolation_level = l; self }
    pub fn require_explicit_version(mut self, b: bool) -> Self { self.config.require_explicit_version = b; self }
    #[cfg(feature = "persistence")]
    pub fn persistence(mut self, p: crate::persistence::Persistence) -> Self { self.config.persistence = p; self }
    pub fn build(self) -> StoreConfig { self.config }
}
```

`StoreConfigBuilder` derives `Default` — its wrapped `config: StoreConfig` gets
`StoreConfig::default()`, so builder defaults == config defaults. `build()` is
infallible; `StoreConfig` is plain data and the `ConsistentInline +
MultiWriter` validation stays at `Store::new`. `#[non_exhaustive]` only affects
external crates; in-crate `src/` construction (the `impl Default`, `StoreInner`,
etc.) is unaffected.

### Persistence constructors (`src/persistence.rs`)

```rust
#[non_exhaustive]
pub enum Persistence {
    #[default] None,
    #[non_exhaustive] Standalone { dir: PathBuf, durability: Durability, wal_write: WalWrite },
    #[non_exhaustive] Smr { dir: PathBuf },
}

impl Persistence {
    pub fn standalone(dir: impl Into<PathBuf>, durability: Durability, wal_write: WalWrite) -> Self { … }
    pub fn smr(dir: impl Into<PathBuf>) -> Self { … }
    // standalone_fast() already exists
}
```

`Persistence::None` is a unit variant (not marked `#[non_exhaustive]`), so
external code can still name it (`Persistence::None`, `matches!(p,
Persistence::None)`). Marking `Standalone`/`Smr` non_exhaustive blocks external
literal construction so fields can be added later; the constructors are the
external path.

### Mode enums (`src/persistence.rs`, `src/store.rs`)

`#[non_exhaustive]` on `Durability`, `WalWrite`, `WriterMode`,
`IsolationLevel`. No external exhaustive matches on these exist today, so the
only effect is future-proofing (a later added variant won't break external
matches, which already use `_` or specific patterns).

## Migration (compiler-driven)

1. Add builder + `standalone`/`smr` constructors — purely additive, everything
   still compiles.
2. Add the `#[non_exhaustive]` attributes — breaks external literal
   construction.
3. `cargo build --all-targets --all-features` enumerates every broken site
   (~51 `StoreConfig { … }` + ~15 `Persistence::Standalone { … }`, across
   `examples/ benches/ tests/ ultima_vector/ bench_workloads/ autobench/`). Fix
   each:
   - `StoreConfig { w: x, ..Default::default() }` → `StoreConfig::builder().w(x).build()`
   - `Persistence::Standalone { dir, durability, wal_write }` → `Persistence::standalone(dir, durability, wal_write)`
   - `Persistence::Smr { dir }` → `Persistence::smr(dir)`
   - any external exhaustive `match` on a now-non_exhaustive enum → add `_` arm
   - external `default()`-then-mutate sites are untouched
4. Update rustdoc/CLAUDE.md examples that show `StoreConfig { … }` to the builder.

## Testing

- Unit tests: builder output equals the equivalent (in-crate) literal for all
  fields; builder with no setters equals `StoreConfig::default()`;
  `Persistence::standalone(dir, d, w)` equals the literal; `Persistence::smr`
  likewise.
- Regression: the full existing suite (root `cargo test`, `--features
  persistence`, `--features fulltext`, `-p ultima-vector`) passing after
  migration is the primary safety net.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`.

## Docs

Canonical: `docs/tasks/task44_config_builder.md`. Update CLAUDE.md construction
guidance (show the builder) and the deferred-review reference.
