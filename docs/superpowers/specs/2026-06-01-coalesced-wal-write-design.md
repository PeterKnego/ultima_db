# Config-selectable coalesced WAL writes — design

**Date:** 2026-06-01
**Branch:** `feat/wal-coalesced-write` (off `main` @ `4ae6aa4`)
**Status:** approved design, pending implementation plan

## Goal

Expose a production-safe WAL write strategy as a `StoreConfig`/`Persistence`
option: **coalesce a commit batch into a single `write` per fsync, while keeping
full `sync_all` (fsync) durability**. This captures most of the group-commit
throughput win measured in task30 (the coalescing half of it) **without** the
`fsync → fdatasync` durability-primitive change. `fdatasync` remains a bench-only
comparison knob, not a production option.

Default behavior is unchanged: the store keeps the current per-entry-write +
`sync_all` path unless the option is explicitly set.

## Background (from task30)

`docs/tasks/task30_wal_backends.md` compared four `WalSink` implementations. The
two relevant findings:

- A single Consistent commit is fsync-floored (~430–715 µs) regardless of sink.
- Eventual **batch** throughput is where sinks differ: `BufferedFileSink`
  (coalesced write + `fdatasync`) beat `FileSink` (per-entry write + `sync_all`)
  by 28–45%.

That win has two separable sources: (a) coalescing N per-entry `write_all` calls
into one per batch, and (b) `fdatasync` instead of `sync_all`. Part (a) is
dependency-free, `unsafe`-free, and **durability-neutral**. Part (b) changes the
durability primitive (equivalent on ext4/xfs for an append-only WAL, but a real
per-filesystem assumption and a silent change to crash semantics).

This feature ships part (a) as a production option and leaves part (b) bench-only.

## Architecture

### 1. Generalize `BufferedFileSink` with `datasync: bool` (`src/wal.rs`)

`BufferedFileSink` currently coalesces writes and always calls `sync_data`
(fdatasync). Add a `datasync: bool` field:

```rust
struct BufferedFileSink {
    file: File,
    buf: Vec<u8>,
    datasync: bool,
}

impl BufferedFileSink {
    fn open(dir: &Path, datasync: bool) -> Result<Self> { /* as today + datasync */ }
}

impl WalSink for BufferedFileSink {
    fn append(&mut self, entry: &WalEntry) -> Result<()> {
        self.buf.extend_from_slice(&frame_entry(entry)?);
        Ok(())
    }
    fn sync(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.file.write_all(&self.buf).map_err(|e| Error::Persistence(e.to_string()))?;
            self.buf.clear();
        }
        if self.datasync {
            self.file.sync_data().map_err(|e| Error::Persistence(e.to_string()))
        } else {
            self.file.sync_all().map_err(|e| Error::Persistence(e.to_string()))
        }
    }
}
```

One struct, two durability behaviors. No near-duplicate sink type.

### 2. New ungated `WalSinkKind::Coalesced` (`src/wal.rs`)

```rust
pub enum WalSinkKind {
    FsWrite,
    /// Coalesced write + sync_all (fsync). Production-safe.
    Coalesced,
    /// Coalesced write + sync_data (fdatasync). Bench comparison only.
    BufferedFile,
    #[cfg(feature = "bench-internals")] Mmap,
    #[cfg(all(target_os = "linux", feature = "wal-iouring"))] IoUring,
}
```

`with_sink_kind` arms:
- `FsWrite      => FileSink::open(dir)`
- `Coalesced    => BufferedFileSink::open(dir, false)`  *(sync_all)*
- `BufferedFile => BufferedFileSink::open(dir, true)`   *(sync_data)*

`Coalesced` is ungated (dependency-free), so it is reachable in normal/persistence
builds — unlike `Mmap`/`IoUring`.

### 3. Public `WalWrite` enum + `Persistence::Standalone` field (`src/persistence.rs`)

```rust
/// How the WAL writes a committed batch to disk (Standalone mode).
///
/// Orthogonal to [`Durability`], which controls *when* `commit()` returns.
/// All four combinations are valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalWrite {
    /// One `write` per entry, then `sync_all` per batch. The original behavior.
    #[default]
    PerEntry,
    /// The whole batch is coalesced into a single `write`, then `sync_all`.
    /// Same durability as `PerEntry` (full fsync); fewer syscalls per batch —
    /// better group-commit throughput under Eventual / high-concurrency loads.
    Coalesced,
}

pub enum Persistence {
    None,
    Standalone {
        dir: PathBuf,
        durability: Durability,
        wal_write: WalWrite,   // NEW required field
    },
    Smr { dir: PathBuf },
}
```

Adding `wal_write` to the struct-like variant is a **breaking change** for every
existing `Persistence::Standalone { dir, durability }` construction. This was
chosen deliberately (cohesion: all WAL knobs in one place) over a non-breaking
`StoreConfig` field. All in-repo constructors (examples, tests, docs) are updated
as part of this work. No deprecation shim — the crate is 0.1.0.

### 4. Store wiring (`src/store.rs` ~244)

Replace the `WalHandle::new(...)` call in the `Standalone` arm with a mapping that
honors `wal_write`:

```rust
Persistence::Standalone { dir, durability, wal_write } => {
    let consistent = matches!(durability, Durability::Consistent);
    let kind = match wal_write {
        WalWrite::PerEntry  => WalSinkKind::FsWrite,
        WalWrite::Coalesced => WalSinkKind::Coalesced,
    };
    Some(WalHandle::with_sink_kind(dir, consistent, Arc::clone(&wal_poison), kind)?)
}
```

`WalHandle::new` is unchanged (still delegates to `FsWrite`) for any other caller.
`WalWrite` is orthogonal to `Durability`: `consistent` still derives only from
`durability`, and the sink choice derives only from `wal_write`.

## Error handling

No change. `BufferedFileSink` already maps I/O errors to `Error::Persistence`, and
the background WAL loop's poison/watermark handling is sink-agnostic. The
`datasync` flag affects only which fsync variant runs, not the error paths.

## Testing

1. **Sink unit test:** `BufferedFileSink::open(dir, false)` (sync_all) round-trips
   N entries through `read_wal`. (The existing fdatasync round-trip test is
   updated to the new `open(dir, true)` signature.)
2. **Store-level recovery round-trip (the coverage gap from task30):** for each of
   `(Durability::Consistent, WalWrite::Coalesced)` and
   `(Durability::Eventual, WalWrite::Coalesced)`: build a `Standalone` store,
   register a table, commit several transactions, drop the store, reopen with the
   same config, `recover()`, and assert all rows are present and correct. This is
   the first end-to-end exercise of a coalesced sink through `Store`.
3. **Default-unchanged test:** a `Standalone { .., wal_write: WalWrite::PerEntry }`
   store behaves identically to before (sanity that the default path is intact).
4. **Constructor updates:** every `Persistence::Standalone { .. }` in `examples/`,
   `tests/`, and unit tests gains `wal_write: WalWrite::PerEntry` (or `Coalesced`
   where a test specifically exercises it). The build must be clean across all
   feature configs.
5. `cargo clippy -- -D warnings` clean (default, `persistence`, `bench-internals`,
   `bench-internals wal-iouring`).

## Benchmark

Add a `("coalesced", WalSinkKind::Coalesced)` column to `benches/wal_bench.rs`
`KINDS`, so the matrix shows the production option (coalesced + sync_all) next to
`fswrite` (per-entry + sync_all) and `buffered` (coalesced + fdatasync). Expected
shape: `coalesced` lands between `fswrite` and `buffered` on batch throughput
(it gets the write-coalescing win but pays full fsync).

## Documentation

New `docs/tasks/task31_coalesced_wal_write.md`: documents the `WalWrite` config
option, the orthogonality with `Durability`, the measured throughput of
`coalesced` vs `fswrite`/`buffered`, and the explicit decision that production
keeps `sync_all` (fdatasync remains bench-only). Cross-references task30.

## Implementation order

1. Generalize `BufferedFileSink` with `datasync` (+ update its existing caller/test).
2. Add `WalSinkKind::Coalesced` + `with_sink_kind` arm; add `coalesced` to the bench.
3. Add public `WalWrite` enum + `Persistence::Standalone` field; update ALL in-repo
   constructors; build green across configs.
4. Wire the store mapping (`store.rs`).
5. Store-level recovery round-trip tests (Consistent×Coalesced, Eventual×Coalesced)
   + default-unchanged test.
6. Run the bench; write `task31` doc with real numbers.

## Out of scope

- Exposing `fdatasync` (`BufferedFile`) or `Mmap`/`IoUring` as production options —
  they stay bench-only/experimental.
- Changing the default (stays `PerEntry`). Adopting `Coalesced` as the default is a
  separate future decision.
- A deprecation/compat shim for the `Persistence::Standalone` field addition.
