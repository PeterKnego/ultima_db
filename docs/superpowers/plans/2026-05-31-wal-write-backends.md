# WAL Write-Backend Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `BufferedFileSink`, `MmapSink`, and `IoUringSink` implementations of the existing `WalSink` trait, runtime-selected via a `WalSinkKind`, and extend the WAL microbenchmark to compare all four write mechanisms across both durability modes and 1/2/4/8/16 KiB record sizes.

**Architecture:** Build on the `WalSink` trait that task28 already introduced (`fn append(&mut self, &WalEntry) -> Result<()>`, `fn sync(&mut self) -> Result<()>`) and its generic `WalHandle::with_sink<S>` seam. The background-thread loop and task28/29 poison + watermark machinery are unchanged. Each new sink coalesces internally and makes bytes durable in `sync()`. A shared `frame_entry` helper guarantees a byte-identical on-disk format across all sinks. `mmap`/`io_uring` sinks are experimental, bench-only, and not wired into `StoreConfig`.

**Tech Stack:** Rust 2024, bincode 2, crc32fast, criterion 0.8, `memmap2` (optional dep, bench-internals), `io-uring` (optional dep, `wal-iouring` feature, Linux-only).

**Spec:** `docs/superpowers/specs/2026-05-31-wal-write-backends-design.md`

**Branch:** `bench/wal-baseline` (already rebased onto `origin/main` @ `9a506ab`).

---

## File Structure

- `src/wal.rs` — all sinks (`FileSink` exists; add `BufferedFileSink`, `MmapSink`, `IoUringSink`), the `WalSinkKind` enum, `with_sink_kind`, the `frame_entry` helper, the `read_wal` `len == 0` change, `BenchWal` kind param, and all unit tests. (This file already concentrates WAL logic; we follow that.)
- `Cargo.toml` — `memmap2` optional dep + extend `bench-internals`; `io-uring` optional dep + new `wal-iouring` feature.
- `benches/wal_bench.rs` — add the sink dimension to the three groups.
- `docs/tasks/task30_wal_backends.md` — final consolidated result matrix + recommendation (Task 8).

---

## Task 1: Extract `frame_entry` helper

Pure refactor: pull the framing (`[len: u32 LE][bincode bytes][crc32: u32 LE]`) out of `write_entry_to_file` into a reusable `frame_entry` so every sink shares one definition of the on-disk record format.

**Files:**
- Modify: `src/wal.rs` (`write_entry_to_file`, ~line 275)
- Test: `src/wal.rs` (`mod tests`)

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `src/wal.rs`:

```rust
#[test]
fn frame_entry_concatenation_reads_back_via_read_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(WAL_FILENAME);
    let e1 = WalEntry { version: 1, ops: vec![WalOp::Insert { table: "t".into(), id: 1, data: vec![1, 2, 3] }] };
    let e2 = WalEntry { version: 2, ops: vec![WalOp::Delete { table: "t".into(), id: 1 }] };

    let mut bytes = frame_entry(&e1).unwrap();
    bytes.extend_from_slice(&frame_entry(&e2).unwrap());
    std::fs::write(&path, &bytes).unwrap();

    let read = read_wal(&path).unwrap();
    assert_eq!(read.len(), 2);
    assert_eq!(read[0].version, 1);
    assert_eq!(read[1].version, 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features persistence frame_entry_concatenation -- --nocapture`
Expected: FAIL — `cannot find function `frame_entry` in this scope`.

- [ ] **Step 3: Add `frame_entry` and reduce `write_entry_to_file`**

Replace the existing `write_entry_to_file` (the whole function at ~line 275) with:

```rust
/// Frame one entry as the on-disk WAL record: `[len: u32 LE][bincode][crc32: u32 LE]`.
/// Shared by every `WalSink` so all backends produce a byte-identical format.
fn frame_entry(entry: &WalEntry) -> Result<Vec<u8>> {
    let data = serialize_entry(entry)?;
    let len = data.len() as u32;
    let checksum = crc32(&data);
    let mut buf = Vec::with_capacity(4 + data.len() + 4);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&data);
    buf.extend_from_slice(&checksum.to_le_bytes());
    Ok(buf)
}

fn write_entry_to_file(file: &mut File, entry: &WalEntry) -> Result<()> {
    file.write_all(&frame_entry(entry)?)
        .map_err(|e| Error::Persistence(e.to_string()))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features persistence wal`
Expected: PASS — all WAL tests green (the existing `wal_file_write_and_read_roundtrip` etc. still pass; new test passes).

- [ ] **Step 5: Commit**

```bash
git add src/wal.rs
git commit -m "refactor(wal): extract frame_entry helper shared by all sinks"
```

---

## Task 2: `FileSink::open` + `WalSinkKind` + `with_sink_kind`

Introduce the runtime sink selector. `WalHandle::new` keeps its exact behavior by delegating to `with_sink_kind(.., FsWrite)`.

**Files:**
- Modify: `src/wal.rs` (`FileSink` ~line 448, `WalHandle::new` ~line 662)
- Test: `src/wal.rs` (`mod tests`)

- [ ] **Step 1: Write the failing test**

Add to `mod tests`:

```rust
#[test]
fn with_sink_kind_fswrite_writes_recoverable_wal() {
    let dir = tempfile::tempdir().unwrap();
    let poison = Arc::new(WalPoison::new());
    {
        let h = WalHandle::with_sink_kind(dir.path(), true, poison, WalSinkKind::FsWrite).unwrap();
        h.write(WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] })
            .unwrap()
            .wait()
            .unwrap();
    } // drop joins bg thread, fsyncs
    let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].version, 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features persistence with_sink_kind_fswrite -- --nocapture`
Expected: FAIL — `cannot find function `with_sink_kind`` / `cannot find value `WalSinkKind``.

- [ ] **Step 3: Add `WalSinkKind`, `FileSink::open`, and `with_sink_kind`**

Just above the `FileSink` struct definition (~line 447), add the enum (variants for later sinks are added in their own tasks):

```rust
/// Selects which `WalSink` implementation a `WalHandle` uses. `FsWrite` is the
/// production default; the others are experimental and bench-only.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalSinkKind {
    /// Current behavior: 3 `write_all` per entry + `fsync` per batch.
    FsWrite,
    /// Coalesced single `write` per batch + `fdatasync`.
    BufferedFile,
}
```

Add an `open` constructor to `FileSink` (in its `impl` block or a new one):

```rust
impl FileSink {
    /// Open (creating if needed) the WAL file in `dir` for appending.
    fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let wal_path = dir.join(WAL_FILENAME);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        sync_dir(dir)?;
        Ok(FileSink { file })
    }
}
```

Replace the body of `WalHandle::new` (~line 662) with a delegation, and add `with_sink_kind` next to it:

```rust
pub fn new(dir: &Path, consistent: bool, poison: Arc<WalPoison>) -> Result<Self> {
    Self::with_sink_kind(dir, consistent, poison, WalSinkKind::FsWrite)
}

/// Build a handle whose sink is chosen at runtime by `kind`. Each match arm
/// monomorphizes the generic `with_sink` with a concrete sink type.
pub(crate) fn with_sink_kind(
    dir: &Path,
    consistent: bool,
    poison: Arc<WalPoison>,
    kind: WalSinkKind,
) -> Result<Self> {
    match kind {
        WalSinkKind::FsWrite => Ok(Self::with_sink(FileSink::open(dir)?, consistent, poison)),
        WalSinkKind::BufferedFile => {
            Ok(Self::with_sink(BufferedFileSink::open(dir)?, consistent, poison))
        }
    }
}
```

> Note: the `BufferedFile` arm references `BufferedFileSink`, added in Task 4. To keep Task 2 compiling on its own, temporarily make that arm `WalSinkKind::BufferedFile => Err(Error::Persistence("BufferedFileSink not yet implemented".into())),` and replace it with the real arm in Task 4. (Both forms are shown so the worker doesn't guess.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features persistence wal`
Expected: PASS — new test green, all existing WAL tests green, `WalHandle::new` behavior unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/wal.rs
git commit -m "feat(wal): add WalSinkKind selector and with_sink_kind constructor"
```

---

## Task 3: Thread `WalSinkKind` through `BenchWal` + bench matrix (fswrite only)

Expose the selector to the bench and add the sink dimension to all three groups, wired with `FsWrite` only for now. Confirms no regression vs the committed baseline.

**Files:**
- Modify: `src/wal.rs` (`BenchWal::new`, ~line 876)
- Modify: `src/lib.rs` (re-export check — `wal` is already `pub mod`)
- Modify: `benches/wal_bench.rs`

- [ ] **Step 1: Update `BenchWal::new` to take a `kind`**

In `src/wal.rs`, change `BenchWal::new`:

```rust
/// Open a WAL in `dir`. `consistent` selects Consistent vs Eventual mode;
/// `kind` selects the sink implementation under test.
pub fn new(dir: &Path, consistent: bool, kind: WalSinkKind) -> Result<Self> {
    Ok(Self {
        inner: WalHandle::with_sink_kind(dir, consistent, Arc::new(WalPoison::new()), kind)?,
    })
}
```

- [ ] **Step 2: Rewrite `benches/wal_bench.rs` to iterate sinks**

Replace the whole file with:

```rust
// WAL-only microbenchmark — isolates the write-ahead-log I/O path from the rest
// of the store so we can measure the cost of a durable commit and compare write
// backends (sinks) across record sizes.
//
// Dimensions: sink {fswrite[,buffered,mmap,iouring]} x size {1,2,4,8,16 KiB} x
// view {consistent single, eventual single, eventual batch}.
//
// IMPORTANT: fsync on tmpfs is a no-op, which would make every number here
// meaningless. The WAL dir is pinned to `target/wal-bench` (real disk); the
// harness prints the backing filesystem at startup and panics if it resolves to
// tmpfs/ramfs.

use std::path::{Path, PathBuf};
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ultima_db::wal::{BenchWal, WalEntry, WalOp, WalSinkKind};

/// Record payload sizes to sweep (bytes).
const SIZES: &[usize] = &[1024, 2048, 4096, 8192, 16384];

/// Number of entries per Eventual fire-and-forget batch.
const EVENTUAL_BATCH: u64 = 256;

/// Sinks under test. Entries are added as each sink lands (Tasks 4/6/7).
const KINDS: &[(&str, WalSinkKind)] = &[("fswrite", WalSinkKind::FsWrite)];

/// Build a single-op WAL entry whose payload is `payload` bytes.
fn make_entry(version: u64, payload: usize) -> WalEntry {
    WalEntry {
        version,
        ops: vec![WalOp::Insert { table: "bench".to_string(), id: version, data: vec![0u8; payload] }],
    }
}

/// Resolve the (mount point, fstype) backing `path` by scanning /proc/mounts.
fn backing_fs(path: &Path) -> (String, String) {
    let canon = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
    let mut best: (usize, String, String) = (0, String::new(), "unknown".to_string());
    for line in mounts.lines() {
        let mut f = line.split_whitespace();
        let _dev = f.next();
        let mp = match f.next() {
            Some(m) => m,
            None => continue,
        };
        let fstype = f.next().unwrap_or("unknown");
        if canon.starts_with(Path::new(mp)) && mp.len() >= best.0 {
            best = (mp.len(), mp.to_string(), fstype.to_string());
        }
    }
    (best.1, best.2)
}

/// Create and return the on-disk benchmark root, asserting it is not tmpfs/ramfs.
fn bench_root() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target").join("wal-bench");
    std::fs::create_dir_all(&root).expect("create bench root");
    let (mount, fs) = backing_fs(&root);
    eprintln!("[wal_bench] WAL dir = {} | mount = {} | fs = {}", root.display(), mount, fs);
    assert!(
        fs != "tmpfs" && fs != "ramfs",
        "WAL benchmark dir is on {fs} (mount {mount}); fsync is a no-op there, so \
         results would be meaningless. Point the bench at real disk."
    );
    root
}

/// Open a fresh WAL under `root` with the given sink. The TempDir is returned so
/// the caller controls when it (and the background writer thread) is dropped.
fn new_wal(root: &Path, consistent: bool, kind: WalSinkKind) -> (tempfile::TempDir, BenchWal) {
    let dir = tempfile::Builder::new().prefix("wal").tempdir_in(root).expect("tempdir on bench root");
    let wal = BenchWal::new(dir.path(), consistent, kind).expect("open BenchWal");
    (dir, wal)
}

fn label(size: usize) -> String {
    format!("{}KiB", size / 1024)
}

fn wal_benches(c: &mut Criterion) {
    let root = bench_root();

    // --- Consistent: one durable commit (write + fsync wait) per iteration. ---
    {
        let mut g = c.benchmark_group("wal_commit_consistent");
        for &(kind_name, kind) in KINDS {
            for &size in SIZES {
                g.throughput(Throughput::Bytes(size as u64));
                g.bench_function(BenchmarkId::new(kind_name, label(size)), |b| {
                    b.iter_batched(
                        || new_wal(&root, true, kind),
                        |(dir, wal)| {
                            wal.commit_consistent(make_entry(1, size)).unwrap();
                            (dir, wal)
                        },
                        BatchSize::PerIteration,
                    );
                });
            }
        }
        g.finish();
    }

    // --- Eventual: caller-visible latency of one fire-and-forget commit (no
    // drain). The fsync runs on the bg thread and is forced off-clock at teardown.
    {
        let mut g = c.benchmark_group("wal_commit_eventual");
        for &(kind_name, kind) in KINDS {
            for &size in SIZES {
                g.throughput(Throughput::Bytes(size as u64));
                g.bench_function(BenchmarkId::new(kind_name, label(size)), |b| {
                    b.iter_batched(
                        || new_wal(&root, false, kind),
                        |(dir, wal)| {
                            wal.commit_eventual(make_entry(1, size)).unwrap();
                            (dir, wal)
                        },
                        BatchSize::PerIteration,
                    );
                });
            }
        }
        g.finish();
    }

    // --- Eventual batched: fire-and-forget batch + single drain (group commit). -
    {
        let mut g = c.benchmark_group("wal_eventual_batch");
        g.sample_size(20);
        for &(kind_name, kind) in KINDS {
            for &size in SIZES {
                g.throughput(Throughput::Bytes(size as u64 * EVENTUAL_BATCH));
                g.bench_function(BenchmarkId::new(kind_name, label(size)), |b| {
                    b.iter_batched(
                        || new_wal(&root, false, kind),
                        |(dir, wal)| {
                            for v in 1..=EVENTUAL_BATCH {
                                wal.commit_eventual(make_entry(v, size)).unwrap();
                            }
                            wal.wait_durable(EVENTUAL_BATCH).unwrap();
                            (dir, wal)
                        },
                        BatchSize::PerIteration,
                    );
                });
            }
        }
        g.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .measurement_time(Duration::from_secs(4))
        .warm_up_time(Duration::from_secs(1));
    targets = wal_benches
}
criterion_main!(benches);
```

- [ ] **Step 3: Verify build + a smoke run**

Run: `cargo build --benches --features bench-internals`
Expected: builds clean.

Run: `cargo bench --bench wal_bench --features bench-internals -- "wal_commit_consistent/fswrite/1KiB"`
Expected: prints `[wal_bench] WAL dir = ... | fs = ext4` (NOT tmpfs) and a `time:` line ~400–500 µs.

- [ ] **Step 4: Confirm no regression vs baseline**

Run: `cargo bench --bench wal_bench --features bench-internals -- "wal_commit_consistent/fswrite"`
Expected: 1/2/4/8/16 KiB all ~430–560 µs, in line with the committed baseline (criterion will report small deltas, p often > 0.05).

- [ ] **Step 5: Commit**

```bash
git add src/wal.rs benches/wal_bench.rs
git commit -m "bench(wal): add sink dimension to WAL bench (fswrite wired)"
```

---

## Task 4: `BufferedFileSink` (coalesce + fdatasync)

fs-write's best trick: buffer framed bytes in memory, write once per batch, `fdatasync`.

**Files:**
- Modify: `src/wal.rs` (add sink, finalize the `BufferedFile` match arm, add `BufferedFile` to bench `KINDS`)
- Test: `src/wal.rs` (`mod tests`)
- Modify: `benches/wal_bench.rs`

- [ ] **Step 1: Write the failing test**

Add to `mod tests`:

```rust
#[test]
fn buffered_file_sink_roundtrips_via_read_wal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut sink = BufferedFileSink::open(dir.path()).unwrap();
        for v in 1..=5u64 {
            sink.append(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), id: v, data: vec![v as u8; 32] }] }).unwrap();
        }
        sink.sync().unwrap();
    }
    let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
    assert_eq!(read.len(), 5);
    assert_eq!(read[4].version, 5);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features persistence buffered_file_sink_roundtrips -- --nocapture`
Expected: FAIL — `cannot find type `BufferedFileSink``.

- [ ] **Step 3: Implement `BufferedFileSink`**

Add near `FileSink` in `src/wal.rs`:

```rust
/// Coalescing sink: `append` frames into an in-memory buffer (no syscall);
/// `sync` writes the whole batch in one `write` then `fdatasync`s. `fdatasync`
/// persists the data and the file-size change but skips timestamp metadata.
struct BufferedFileSink {
    file: File,
    buf: Vec<u8>,
}

impl BufferedFileSink {
    fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let wal_path = dir.join(WAL_FILENAME);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        sync_dir(dir)?;
        Ok(BufferedFileSink { file, buf: Vec::new() })
    }
}

impl WalSink for BufferedFileSink {
    fn append(&mut self, entry: &WalEntry) -> Result<()> {
        self.buf.extend_from_slice(&frame_entry(entry)?);
        Ok(())
    }
    fn sync(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.file.write_all(&self.buf).map_err(|e| Error::Persistence(e.to_string()))?;
            self.buf.clear(); // retains capacity for the next batch
        }
        self.file.sync_data().map_err(|e| Error::Persistence(e.to_string()))
    }
}
```

Replace the temporary `BufferedFile` arm in `with_sink_kind` with the real one (if it was stubbed in Task 2):

```rust
WalSinkKind::BufferedFile => {
    Ok(Self::with_sink(BufferedFileSink::open(dir)?, consistent, poison))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features persistence wal`
Expected: PASS — new test + all existing WAL tests green.

- [ ] **Step 5: Add `buffered` to the bench matrix**

In `benches/wal_bench.rs`, extend `KINDS`:

```rust
const KINDS: &[(&str, WalSinkKind)] =
    &[("fswrite", WalSinkKind::FsWrite), ("buffered", WalSinkKind::BufferedFile)];
```

- [ ] **Step 6: Bench the delta**

Run: `cargo bench --bench wal_bench --features bench-internals -- "wal_eventual_batch"`
Expected: prints fswrite + buffered rows; record both. (Buffered should win on the batch throughput rows — one `write` + `fdatasync` per batch vs many `write_all` + `fsync`.)

- [ ] **Step 7: Commit**

```bash
git add src/wal.rs benches/wal_bench.rs
git commit -m "feat(wal): add BufferedFileSink (coalesced write + fdatasync)"
```

---

## Task 5: `read_wal` `len == 0` end-of-log sentinel

Required by `MmapSink` (Task 6), which pre-sizes the file and can leave a zero tail after a crash. Land it first so mmap recovery tests have it.

**Files:**
- Modify: `src/wal.rs` (`read_wal`, ~line 290)
- Test: `src/wal.rs` (`mod tests`)

- [ ] **Step 1: Write the failing test**

Add to `mod tests`:

```rust
#[test]
fn read_wal_stops_at_zero_length_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(WAL_FILENAME);
    // One valid record followed by a zero tail (as a pre-sized mmap crash leaves).
    let mut bytes = frame_entry(&WalEntry { version: 7, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap();
    bytes.extend_from_slice(&[0u8; 64]);
    std::fs::write(&path, &bytes).unwrap();

    let read = read_wal(&path).unwrap(); // must NOT return Err(WalCorrupted)
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].version, 7);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features persistence read_wal_stops_at_zero_length_tail -- --nocapture`
Expected: FAIL — `read_wal` returns `Err(WalCorrupted(...))` (zero `len` passes CRC for empty data, then deserialize fails). The `assert_eq!` / `unwrap()` panics.

- [ ] **Step 3: Add the `len == 0` stop**

In `read_wal`, immediately after `offset += 4;` that follows reading `len`, insert:

```rust
        if len == 0 {
            // Clean end-of-log: a pre-sized (mmap) file leaves a zero tail after
            // a crash, and a torn zero-length write is not a real record (every
            // valid record frames a non-empty payload). Stop, do not error.
            break;
        }
```

So the loop head reads:

```rust
    while offset + 4 <= all_bytes.len() {
        let len = u32::from_le_bytes(all_bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if len == 0 {
            break;
        }

        if offset + len + 4 > all_bytes.len() {
            // Truncated entry at end of file — stop (crash during write).
            break;
        }
        // ... unchanged ...
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features persistence wal`
Expected: PASS — new test green; existing corruption test (`...crc...`) still passes (a CRC mismatch on a non-zero-length record still errors).

- [ ] **Step 5: Commit**

```bash
git add src/wal.rs
git commit -m "fix(wal): treat zero-length record as clean end-of-log in read_wal"
```

---

## Task 6: `MmapSink` (memmap2, pre-size + memcpy + msync)

**Files:**
- Modify: `Cargo.toml` (memmap2 optional dep; extend `bench-internals`)
- Modify: `src/wal.rs` (add `MmapSink`, `WalSinkKind::Mmap` variant + arm, gated on `bench-internals`)
- Test: `src/wal.rs` (`mod tests`, gated)
- Modify: `benches/wal_bench.rs`

- [ ] **Step 1: Add the dependency**

In `Cargo.toml`, under `[dependencies]`:

```toml
memmap2 = { version = "0.9", optional = true }
```

Extend the `bench-internals` feature:

```toml
bench-internals = ["persistence", "dep:memmap2"]
```

- [ ] **Step 2: Write the failing tests**

Add to `mod tests` (gated so plain `cargo test` without the feature skips them):

```rust
#[cfg(feature = "bench-internals")]
#[test]
fn mmap_sink_roundtrips_via_read_wal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut sink = MmapSink::open(dir.path()).unwrap();
        for v in 1..=5u64 {
            sink.append(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), id: v, data: vec![v as u8; 32] }] }).unwrap();
        }
        sink.sync().unwrap();
    } // Drop truncates to logical length + syncs
    let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
    assert_eq!(read.len(), 5);
    assert_eq!(read[4].version, 5);
}

#[cfg(feature = "bench-internals")]
#[test]
fn mmap_sink_recovers_after_growing_past_quantum() {
    let dir = tempfile::tempdir().unwrap();
    // ~9.6 MiB of records forces at least one grow past the 8 MiB quantum.
    let n = 600u64;
    {
        let mut sink = MmapSink::open(dir.path()).unwrap();
        for v in 1..=n {
            sink.append(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), id: v, data: vec![0u8; 16 * 1024] }] }).unwrap();
        }
        sink.sync().unwrap();
    }
    let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
    assert_eq!(read.len() as u64, n);
    assert_eq!(read[(n - 1) as usize].version, n);
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --features bench-internals mmap_sink -- --nocapture`
Expected: FAIL — `cannot find type `MmapSink``.

- [ ] **Step 4: Implement `MmapSink`**

Add to `src/wal.rs` (gated):

```rust
/// Pre-sized mmap sink (experimental, bench-only). `append` memcpys framed bytes
/// into the mapped region at a tracked write head; `sync` `msync`s the map.
///
/// NOT safe with `prune_wal`/checkpoint (truncating the file under the mapping
/// risks SIGBUS). Assumes it opens an empty/clean file (the bench uses a fresh
/// dir per iteration). On clean `Drop` the file is truncated to the logical
/// write head; a crash leaves a zero tail that `read_wal` treats as end-of-log.
#[cfg(feature = "bench-internals")]
struct MmapSink {
    file: File,
    map: memmap2::MmapMut,
    write_head: usize,
    capacity: usize,
}

#[cfg(feature = "bench-internals")]
const MMAP_GROW_QUANTUM: u64 = 8 * 1024 * 1024;

#[cfg(feature = "bench-internals")]
impl MmapSink {
    fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let wal_path = dir.join(WAL_FILENAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&wal_path)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        sync_dir(dir)?;
        let existing = file.metadata().map_err(|e| Error::Persistence(e.to_string()))?.len();
        let capacity = ((existing / MMAP_GROW_QUANTUM) + 1) * MMAP_GROW_QUANTUM;
        file.set_len(capacity).map_err(|e| Error::Persistence(e.to_string()))?;
        let map = unsafe { memmap2::MmapMut::map_mut(&file).map_err(|e| Error::Persistence(e.to_string()))? };
        Ok(MmapSink { file, map, write_head: existing as usize, capacity: capacity as usize })
    }

    /// Grow the file + remap if `extra` more bytes would not fit.
    fn ensure_capacity(&mut self, extra: usize) -> Result<()> {
        if self.write_head + extra <= self.capacity {
            return Ok(());
        }
        let needed = (self.write_head + extra) as u64;
        let new_cap = ((needed / MMAP_GROW_QUANTUM) + 1) * MMAP_GROW_QUANTUM;
        self.file.set_len(new_cap).map_err(|e| Error::Persistence(e.to_string()))?;
        // Persist the new size before remapping so a crash can't expose a hole.
        self.file.sync_data().map_err(|e| Error::Persistence(e.to_string()))?;
        self.map = unsafe { memmap2::MmapMut::map_mut(&self.file).map_err(|e| Error::Persistence(e.to_string()))? };
        self.capacity = new_cap as usize;
        Ok(())
    }
}

#[cfg(feature = "bench-internals")]
impl WalSink for MmapSink {
    fn append(&mut self, entry: &WalEntry) -> Result<()> {
        let framed = frame_entry(entry)?;
        self.ensure_capacity(framed.len())?;
        self.map[self.write_head..self.write_head + framed.len()].copy_from_slice(&framed);
        self.write_head += framed.len();
        Ok(())
    }
    fn sync(&mut self) -> Result<()> {
        // MS_SYNC over the whole map; clean pages are skipped by the kernel.
        self.map.flush().map_err(|e| Error::Persistence(e.to_string()))
    }
}

#[cfg(feature = "bench-internals")]
impl Drop for MmapSink {
    fn drop(&mut self) {
        let _ = self.map.flush();
        let _ = self.file.set_len(self.write_head as u64);
        let _ = self.file.sync_all();
    }
}
```

Add the `Mmap` variant to `WalSinkKind` (gated) and its `with_sink_kind` arm (gated):

```rust
// in enum WalSinkKind { ... }
#[cfg(feature = "bench-internals")]
Mmap,
```

```rust
// in with_sink_kind match { ... }
#[cfg(feature = "bench-internals")]
WalSinkKind::Mmap => Ok(Self::with_sink(MmapSink::open(dir)?, consistent, poison)),
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --features bench-internals mmap_sink -- --nocapture`
Expected: PASS — both mmap tests green.

Run: `cargo test --features persistence wal`
Expected: PASS — non-feature build still compiles and all WAL tests pass (mmap items cfg'd out).

- [ ] **Step 6: Add `mmap` to the bench matrix**

In `benches/wal_bench.rs`, extend `KINDS`:

```rust
const KINDS: &[(&str, WalSinkKind)] = &[
    ("fswrite", WalSinkKind::FsWrite),
    ("buffered", WalSinkKind::BufferedFile),
    ("mmap", WalSinkKind::Mmap),
];
```

- [ ] **Step 7: Bench vs fs variants**

Run: `cargo bench --bench wal_bench --features bench-internals -- "wal_eventual_batch"`
Expected: prints fswrite + buffered + mmap rows; record them.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml src/wal.rs benches/wal_bench.rs
git commit -m "feat(wal): add experimental MmapSink (bench-only) + bench matrix entry"
```

---

## Task 7: `IoUringSink` (io-uring, `wal-iouring` feature, Linux-only)

**Files:**
- Modify: `Cargo.toml` (io-uring optional dep; `wal-iouring` feature)
- Modify: `src/wal.rs` (add `IoUringSink`, `WalSinkKind::IoUring` variant + arm, all gated)
- Test: `src/wal.rs` (`mod tests`, gated)
- Modify: `benches/wal_bench.rs`

- [ ] **Step 1: Add the dependency + feature**

In `Cargo.toml`, under `[dependencies]`:

```toml
io-uring = { version = "0.7", optional = true }
```

Under `[features]`:

```toml
wal-iouring = ["persistence", "dep:io-uring"]
```

In the `[[bench]] name = "wal_bench"` entry, leave `required-features = ["bench-internals"]` as-is (io_uring is additive; the bench compiles without it).

- [ ] **Step 2: Write the failing test**

Add to `mod tests` (gated):

```rust
#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
#[test]
fn iouring_sink_roundtrips_via_read_wal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut sink = IoUringSink::open(dir.path()).unwrap();
        for v in 1..=5u64 {
            sink.append(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), id: v, data: vec![v as u8; 32] }] }).unwrap();
        }
        sink.sync().unwrap();
    }
    let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
    assert_eq!(read.len(), 5);
    assert_eq!(read[4].version, 5);
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test --features "bench-internals wal-iouring" iouring_sink -- --nocapture`
Expected: FAIL — `cannot find type `IoUringSink``.

- [ ] **Step 4: Implement `IoUringSink`**

Add to `src/wal.rs` (gated):

```rust
/// io_uring sink (experimental, bench-only, Linux). `append` accumulates framed
/// bytes; `sync` submits one `Write` + `Fsync(DATASYNC)` chained with `IO_LINK`
/// in a single `io_uring_enter`, then waits on completion. Queue depth 1.
///
/// NOT safe with `prune_wal`/checkpoint. Writes at an explicit offset (not append
/// mode). Same on-disk format as the file sinks.
#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
struct IoUringSink {
    ring: io_uring::IoUring,
    file: File,
    offset: u64,
    buf: Vec<u8>,
}

#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
impl IoUringSink {
    fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let wal_path = dir.join(WAL_FILENAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&wal_path)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        sync_dir(dir)?;
        let offset = file.metadata().map_err(|e| Error::Persistence(e.to_string()))?.len();
        let ring = io_uring::IoUring::new(8).map_err(|e| Error::Persistence(e.to_string()))?;
        Ok(IoUringSink { ring, file, offset, buf: Vec::new() })
    }
}

#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
impl WalSink for IoUringSink {
    fn append(&mut self, entry: &WalEntry) -> Result<()> {
        self.buf.extend_from_slice(&frame_entry(entry)?);
        Ok(())
    }
    fn sync(&mut self) -> Result<()> {
        use std::os::unix::io::AsRawFd;
        if self.buf.is_empty() {
            return Ok(());
        }
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let write_e = io_uring::opcode::Write::new(fd, self.buf.as_ptr(), self.buf.len() as u32)
            .offset(self.offset)
            .build()
            .flags(io_uring::squeue::Flags::IO_LINK)
            .user_data(1);
        let fsync_e = io_uring::opcode::Fsync::new(fd)
            .flags(io_uring::types::FsyncFlags::DATASYNC)
            .build()
            .user_data(2);
        unsafe {
            let mut sq = self.ring.submission();
            sq.push(&write_e).map_err(|e| Error::Persistence(e.to_string()))?;
            sq.push(&fsync_e).map_err(|e| Error::Persistence(e.to_string()))?;
        }
        self.ring.submit_and_wait(2).map_err(|e| Error::Persistence(e.to_string()))?;

        let mut wrote: Option<i32> = None;
        let completions: Vec<_> = self.ring.completion().collect();
        for cqe in completions {
            let res = cqe.result();
            if res < 0 {
                return Err(Error::Persistence(format!(
                    "io_uring op {} failed: {}",
                    cqe.user_data(),
                    std::io::Error::from_raw_os_error(-res)
                )));
            }
            if cqe.user_data() == 1 {
                wrote = Some(res);
            }
        }
        match wrote {
            Some(n) if n as usize == self.buf.len() => {}
            other => {
                return Err(Error::Persistence(format!(
                    "io_uring short write: {other:?} of {}",
                    self.buf.len()
                )));
            }
        }
        self.offset += self.buf.len() as u64;
        self.buf.clear();
        Ok(())
    }
}
```

Add the `IoUring` variant + arm (gated):

```rust
// in enum WalSinkKind { ... }
#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
IoUring,
```

```rust
// in with_sink_kind match { ... }
#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
WalSinkKind::IoUring => Ok(Self::with_sink(IoUringSink::open(dir)?, consistent, poison)),
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --features "bench-internals wal-iouring" iouring_sink -- --nocapture`
Expected: PASS.

Run: `cargo clippy --features "bench-internals wal-iouring" -- -D warnings`
Expected: zero warnings.

Run: `cargo test --features persistence wal`
Expected: PASS — non-feature build unaffected (io_uring items cfg'd out).

- [ ] **Step 6: Add `iouring` to the bench matrix (cfg-split const)**

In `benches/wal_bench.rs`, replace the `KINDS` const with a cfg-split pair (array elements can't carry `cfg` attributes):

```rust
#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
const KINDS: &[(&str, WalSinkKind)] = &[
    ("fswrite", WalSinkKind::FsWrite),
    ("buffered", WalSinkKind::BufferedFile),
    ("mmap", WalSinkKind::Mmap),
    ("iouring", WalSinkKind::IoUring),
];
#[cfg(not(all(target_os = "linux", feature = "wal-iouring")))]
const KINDS: &[(&str, WalSinkKind)] = &[
    ("fswrite", WalSinkKind::FsWrite),
    ("buffered", WalSinkKind::BufferedFile),
    ("mmap", WalSinkKind::Mmap),
];
```

- [ ] **Step 7: Bench vs all**

Run: `cargo bench --bench wal_bench --features "bench-internals wal-iouring" -- "wal_eventual_batch"`
Expected: prints all four sink rows per size; record them.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml src/wal.rs benches/wal_bench.rs
git commit -m "feat(wal): add experimental IoUringSink behind wal-iouring feature"
```

---

## Task 8: Full comparison run + consolidate task doc

**Files:**
- Create: `docs/tasks/task30_wal_backends.md` (pick the next free `NN`; latest is task29)
- Delete: `docs/superpowers/specs/2026-05-31-wal-write-backends-design.md`, `docs/superpowers/plans/2026-05-31-wal-write-backends.md`

- [ ] **Step 1: Run the full matrix and capture numbers**

Run: `cargo bench --bench wal_bench --features "bench-internals wal-iouring" 2>&1 | tee target/wal-bench-results.txt`
Expected: full 4 × 3 × 5 matrix; the startup line confirms `fs = ext4` (not tmpfs).

- [ ] **Step 2: Write the consolidated doc**

Create `docs/tasks/task30_wal_backends.md` with: the goal; the four sinks and what each optimizes; the `WalSink`-based architecture (no new trait); the `len == 0` recovery change; the result matrix (single-Consistent, single-Eventual, Eventual-batch for each sink × size) from Step 1; analysis (expect: fsync floor dominates single Consistent across all sinks; buffered/mmap/iouring win on batch throughput; Eventual single ~1 µs for all); the recommendation; and the explicit experimental/bench-only + not-safe-with-prune caveat for mmap/io_uring.

- [ ] **Step 3: Delete the superpowers artifacts**

```bash
git rm -f docs/superpowers/specs/2026-05-31-wal-write-backends-design.md \
          docs/superpowers/plans/2026-05-31-wal-write-backends.md 2>/dev/null || \
  rm -f docs/superpowers/specs/2026-05-31-wal-write-backends-design.md \
        docs/superpowers/plans/2026-05-31-wal-write-backends.md
```
(They are gitignored, so `git rm` will report "did not match" — the plain `rm` fallback removes the working files.)

- [ ] **Step 4: Final verification**

Run: `cargo test --features "bench-internals wal-iouring"` then `cargo clippy --features "bench-internals wal-iouring" -- -D warnings`
Expected: all tests pass; zero clippy warnings.

- [ ] **Step 5: Commit**

```bash
git add docs/tasks/task30_wal_backends.md
git commit -m "docs(wal): consolidate WAL write-backend comparison results (task30)"
```

---

## Notes on cfg gating (read before starting)

- `WalSinkKind` is always `pub` (`#[doc(hidden)]`). Its `FsWrite` and `BufferedFile` variants are unconditional (both sinks are dependency-free). `Mmap` is gated `#[cfg(feature = "bench-internals")]`; `IoUring` is gated `#[cfg(all(target_os = "linux", feature = "wal-iouring"))]`. Each gated variant's `with_sink_kind` match arm carries the **same** cfg, keeping the match exhaustive in every build.
- `memmap2` is an optional dep enabled by `bench-internals`; `MmapSink` and its tests therefore require `--features bench-internals`. Plain `cargo test` / `cargo build` does not pull memmap2 and cfg's the sink out.
- `io-uring` is an optional dep enabled by `wal-iouring`; `IoUringSink` and its test require `--features wal-iouring` on Linux.
- Run the standard gate after each task: `cargo test --features persistence` must stay green (proves the non-experimental build is unaffected).
