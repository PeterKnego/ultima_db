# WAL write-backend comparison — design

**Date:** 2026-05-31
**Branch:** `bench/wal-baseline` (rebased onto `origin/main` @ `9a506ab`, task28/29)
**Status:** approved design (revised for `WalSink`), pending implementation plan

## Goal

Speed up the on-disk write path of the write-ahead log by comparing I/O
mechanisms for an append-only log, measured against the existing WAL
microbenchmark:

1. **fs write** — the current `write()` + `fsync` approach (baseline).
2. **fs write, buffered** — coalesced single `write` + `fdatasync` (fs-write's best trick).
3. **mmap append** — memcpy into a mapped region + `msync`.
4. **io_uring** — submit `WRITE` + `FSYNC` via a single `io_uring_enter`.

All variants coexist behind the existing `WalSink` abstraction, runtime-selected
by a `WalSinkKind`, so a single `cargo bench` run produces an apples-to-apples
comparison matrix.

## Honest expectations (what we are and are not optimizing)

A single Consistent commit on the baseline is ~450 µs and **size-independent**
(1 KiB ≈ 16 KiB). That cost is almost entirely the **fsync** — the physical
device flush on ext4. The `write()` syscalls and bincode serialization are
~1 µs next to it (confirmed by the corrected Eventual fire-and-forget number).

| Approach        | What it changes                                                                 | What it cannot change                                   |
|-----------------|---------------------------------------------------------------------------------|---------------------------------------------------------|
| fs write        | baseline                                                                        | —                                                       |
| fs write buffered | 3 writes/entry → 1 write/batch; `fsync` → `fdatasync` (skips timestamp metadata) | the device-flush floor on a single commit               |
| mmap            | eliminates `write()` syscalls (memcpy into mapped pages); helps batched throughput & CPU | `msync` is still a device flush ≈ fsync                  |
| io_uring        | one `io_uring_enter` submits writes + fsync; async submit (large win for Eventual) | the fsync completion is still the device flush          |

The win is concentrated in **throughput / CPU / syscall-count**, especially the
**Eventual** path — not in the single-commit Consistent floor. The bench keeps
all three views (single Consistent, single Eventual, Eventual batch) so we can
see where each mechanism helps.

## Decisions (from brainstorming + WalSink review)

- **Harness:** all sink variants live in the tree, runtime-selected via
  `WalSinkKind`, compared in one bench run.
- **Build on `WalSink`, do not invent a new trait.** task28 already introduced
  `pub(crate) trait WalSink: Send { fn append(&mut self, &WalEntry) -> Result<()>;
  fn sync(&mut self) -> Result<()>; }` with a generic `WalHandle::with_sink<S>`
  construction seam and a `FileSink` impl. We add new `impl WalSink` types; the
  trait, the bg-thread loop, and the task28/29 poison + watermark machinery are
  **unchanged**.
- **Coalescing is internal to each sink, flushed in `sync()`** — not a bg-thread
  concern. `append` buffers (BufWriter / mapped region / accumulation Vec);
  `sync` makes it durable.
- **io_uring dependency:** add the `io-uring` crate gated behind a cargo feature
  (`wal-iouring`), Linux-only via `cfg`. `FileSink`/`BufferedFileSink`/`MmapSink`
  stay dependency-free and cross-platform (`mmap` via the `memmap2` dev-dep, see
  below).
- **fs-write trick kept separate from baseline.** `FileSink` is left untouched
  (it is the production sink and the comparison baseline); the buffered variant
  is a **new** `BufferedFileSink` so the A/B is clean and production behavior is
  unchanged.

## Architecture

### `WalSink` — unchanged

```rust
pub(crate) trait WalSink: Send {
    fn append(&mut self, entry: &WalEntry) -> Result<()>;  // frame + record ONE entry
    fn sync(&mut self) -> Result<()>;                      // make durable
}
```

The bg loop (`spawn_wal_thread`, `src/wal.rs`) drains a `Vec<WalEntry>` batch,
calls `append` per entry, then `sync` once, then advances epoch / watermark or
poisons on error. None of that changes.

### Sink variants

- **`FileSink`** (exists): `append` = `write_entry_to_file` (3 `write_all`);
  `sync` = `sync_all`. The baseline. **Left exactly as-is.**
- **`BufferedFileSink`** (new): owns a `BufWriter<File>`. `append` frames the
  entry into the buffer (no syscall). `sync` = `flush()` the buffer (one `write`)
  then `file.sync_data()` (`fdatasync`). `fdatasync` persists the data and the
  size change but skips timestamp metadata — durability-equivalent here and
  cheaper on ext4.
- **`MmapSink`** (new): pre-sizes the file in chunks (grow by an 8 MiB quantum via
  `set_len`) and `memcpy`s framed bytes into the mapped region at a tracked write
  head; remaps after a grow. `sync` = `msync(MS_SYNC)` over the dirty range, plus
  one `fdatasync` after a grow so the new size is durable. On `Drop`: `set_len`
  back to the exact logical write-head length, then unmap.
- **`IoUringSink`** (new, feature `wal-iouring`, Linux-only): `append` accumulates
  framed bytes + the target offset (buffers; submits nothing). `sync` submits a
  `Write` SQE + `Fsync` SQE chained with `IOSQE_IO_LINK` in a single
  `io_uring_enter`, waits on the fsync completion, advances the offset. v1 keeps
  **queue depth 1** (submit-then-wait per batch). Pipelining successive batches is
  an explicit later phase, not in v1.

### Framing helper (small refactor)

`write_entry_to_file` currently does serialize + 3 `write_all` against a `File`.
Extract the framing (`[len: u32][entry bytes][crc32: u32]`) into a pure
`frame_entry(entry: &WalEntry) -> Result<Vec<u8>>` returning the bytes.
`write_entry_to_file` becomes `file.write_all(&frame_entry(entry)?)`. The new
sinks reuse `frame_entry`, guaranteeing **byte-for-byte identical on-disk
format** across all variants.

### Selection & wiring

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WalSinkKind { FsWrite, BufferedFile, Mmap, #[cfg(feature = "wal-iouring")] IoUring }

impl WalHandle {
    pub(crate) fn with_sink_kind(
        dir: &Path, consistent: bool, poison: Arc<WalPoison>, kind: WalSinkKind,
    ) -> Result<Self> {
        match kind {
            WalSinkKind::FsWrite      => Ok(Self::with_sink(FileSink::open(dir)?,        consistent, poison)),
            WalSinkKind::BufferedFile => Ok(Self::with_sink(BufferedFileSink::open(dir)?, consistent, poison)),
            WalSinkKind::Mmap         => Ok(Self::with_sink(MmapSink::open(dir)?,         consistent, poison)),
            #[cfg(feature = "wal-iouring")]
            WalSinkKind::IoUring      => Ok(Self::with_sink(IoUringSink::open(dir)?,      consistent, poison)),
        }
    }
}
```

Each `match` arm monomorphizes the generic `with_sink<S>` with a different
concrete `S`, all returning `Self` — no boxing. `WalHandle::new` delegates to
`with_sink_kind(.., WalSinkKind::FsWrite)` (default; production behavior
unchanged). `BenchWal::new(dir, consistent, kind)` gains a `kind: WalSinkKind`
parameter; `WalSinkKind` is re-exported `#[doc(hidden)]` under `bench-internals`.
**No `StoreConfig` / public-API change** — out of scope.

## Recovery / correctness

### `read_wal` `len == 0` sentinel (required for mmap)

`read_wal` loops reading a `u32` length prefix. A pre-sized `MmapSink` file that
crashed before clean truncation leaves a zero tail; the loop reads `len == 0`,
and since `crc32(&[]) == 0` matches the zero CRC bytes it passes CRC and then
*fails* `deserialize_entry` with `WalCorrupted`. Fix: **treat `len == 0` as clean
end-of-log** (stop, do not error). A valid record always frames a non-empty
payload, so `len == 0` is an unambiguous sentinel. Strictly safer for fs-write
files too (a torn write yielding a zero length becomes a clean stop), and changes
no valid-record behavior.

### mmap is bench-only / experimental

`prune_wal` does `File::create(path)` (truncates the file in place under the bg
thread's open handle — deliberately, per its comment). Truncating out from under
an mmap *mapping* risks SIGBUS. Because mmap/io_uring sinks are scoped to the
**bench comparison only** (never selected by `StoreConfig`/recovery in this
work), this is acceptable for v1. `MmapSink` and `IoUringSink` are documented as
**experimental, bench-only, not yet safe with `prune_wal`/checkpoint**.
`FileSink`/`BufferedFileSink` have no such restriction.

### Testing requirements

- All existing WAL unit + recovery/integration tests continue to pass.
- New: `frame_entry` round-trips through `read_wal` (format unchanged).
- New: each new sink (`BufferedFileSink`, `MmapSink`, and `IoUringSink` when the
  feature is on) writes N entries that `read_wal` recovers identically to
  `FileSink` — byte-for-byte format compatibility, parameterized over the sink.
- New: a pre-sized file with a zero tail (simulating an mmap crash before clean
  truncation) recovers cleanly to the last good record (`len == 0` stop).
- New: `MmapSink` that grows past one 8 MiB quantum (many entries) recovers all
  entries (remap correctness).
- `cargo clippy -- -D warnings` passes, including `--features wal-iouring` on Linux.

## Benchmark matrix

Extend `benches/wal_bench.rs` with a **sink dimension** via criterion
`BenchmarkId`, keeping both durability paths and all five record sizes:

- `wal_commit_consistent/{fswrite,buffered,mmap,iouring}/{1,2,4,8,16}KiB`
- `wal_commit_eventual/{...}/{...}`
- `wal_eventual_batch/{...}/{...}`

The `iouring` rows are present only when `wal-iouring` is enabled; otherwise the
bench loop skips that kind. Existing safeguards stay: WAL dir pinned to
`target/wal-bench`, tmpfs/ramfs guard, fresh WAL per iteration via `iter_batched`
with setup/teardown excluded from the timed section.

## Dependencies

- `memmap2 = "0.9"` as an **optional dependency** enabled by the `bench-internals`
  feature (`bench-internals = ["persistence", "dep:memmap2"]`). It must be a real
  optional dep, not a dev-dependency: `bench-internals` can be enabled in a
  non-test build (`cargo build --features bench-internals`), where dev-deps are
  unavailable. `MmapSink` and its tests are gated `#[cfg(feature = "bench-internals")]`.
- `io-uring = "0.7"` as an **optional dependency** behind a new `wal-iouring`
  feature (`wal-iouring = ["persistence", "dep:io-uring"]`); Linux-only via
  `#[cfg(all(target_os = "linux", feature = "wal-iouring"))]`.

## Implementation order ("compare as you go")

Each numbered step ends with a bench run so we compare incrementally.

1. **`frame_entry` extraction** + `WalSinkKind` enum + `with_sink_kind` + thread
   `kind` through `BenchWal::new` + bench-matrix wiring (only `FsWrite` wired).
   Confirm no regression vs the current committed baseline.
2. **`BufferedFileSink`** (coalesce + `fdatasync`) + recovery-compat test → add
   `buffered` to the matrix → measure delta vs `fswrite`.
3. **`read_wal` `len == 0` change** + zero-tail recovery test (lands with, and is
   required by, mmap).
4. **`MmapSink`** (+ memmap2 dev-dep, grow/remap, Drop-truncate) + recovery tests
   → add `mmap` to the matrix → bench vs fs variants.
5. **`IoUringSink`** (+ io-uring dep, `wal-iouring` feature) + recovery test →
   add `iouring` to the matrix → bench vs all.
6. **Consolidate** the result matrix + recommendation into
   `docs/tasks/taskNN_wal_backends.md` (canonical doc) and delete this
   superpowers spec artifact per the project workflow.

## Out of scope

- Exposing sink selection through `StoreConfig` / public persistence config.
- Making mmap/io_uring sinks safe with `prune_wal`/checkpoint.
- io_uring batch pipelining / queue depth > 1 (explicit later phase).
- `O_DIRECT`, alternative checksums, changing the WAL record format, or
  group-commit batch-size tuning.
