# Journal Segment Preallocation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the per-commit ext4 metadata-commit cost (~390 µs of the ~600 µs `fdatasync` barrier) by preallocating journal segments, with the zero-fill kept entirely off the commit path.

**Architecture:** A background `SegmentPipeline` (etcd `filePipeline` analog) keeps one zero-filled temp segment ready; on rotation the writer writes a header into it and atomically renames it into place — so appends overwrite already-written extents and `fdatasync` carries no `i_size` change. Recovery learns that a zero length-prefix means end-of-records, preserves the preallocated tail instead of physically truncating it, and re-preallocates the active segment at open. The whole feature is behind an opt-in `JournalConfig.preallocate_segments` flag (default off).

**Tech Stack:** Rust (edition 2024), `std` threads + `Condvar` for the pipeline, ext4/POSIX file primitives. Crate: `ultima-journal` at `../ultima_db/ultima_journal` (workspace root `/home/claude/ultima/ultima_db`).

## Global Constraints

- **Code lives in `ultima_db/ultima_journal`** (package name `ultima-journal`); `ultima_cluster` consumes it via the fixed path dep `../ultima_db/ultima_journal`.
- **Flag default OFF.** With `preallocate_segments = false` the existing code path must be byte-identical; the full pre-existing test suite must stay green.
- **Real zero-fill only.** Preallocation writes actual zero bytes + `sync_all`. NEVER `fallocate`/`posix_fallocate`/`set_len` to size — ext4 leaves *unwritten* extents that re-journal on first overwrite, defeating the purpose.
- **One segment of look-ahead** (double-buffer). No configurable depth.
- **`decode_record` zero-prefix change is UNCONDITIONAL** (not flag-gated). Tail-preservation, re-preallocation, orphan cleanup, and the pipeline are flag-gated.
- **Segment file naming:** active segments `seg-{seq:020}.log`; preallocated temps `seg-prealloc.{counter}.tmp`.
- **TDD, frequent commits.** `cargo clippy -p ultima-journal -- -D warnings` must pass after each task. Run tests from the workspace root `/home/claude/ultima/ultima_db`.
- **`SEGMENT_HEADER_SIZE` = 32 bytes** (`segment.rs:8`). Default `segment_size_bytes` = 64 MiB (`mod.rs:24`).

---

## File Structure

- `ultima_journal/src/journal/segment.rs` — add `decode_record` zero-prefix handling; new `SegmentFile` methods `preallocate_to`, `reset_cursor`, `physical_len`; new associated fns `create_prealloc_temp`, `activate_prealloc_temp`. Unit tests in its `#[cfg(test)] mod tests`.
- `ultima_journal/src/journal/segment_pipeline.rs` — **new** module: the `SegmentPipeline` background pre-creator.
- `ultima_journal/src/journal/writer.rs` — `WriterState.pipeline` field; flag-gated activation in `write_batch`.
- `ultima_journal/src/journal/mod.rs` — `JournalConfig.preallocate_segments`; `mod segment_pipeline`; `Journal::open` orphan cleanup + flag-gated recovery + re-preallocation + pipeline spawn; pipeline shutdown in `close`/`Drop`. Integration tests in its `#[cfg(test)] mod tests`.
- `ultima_journal/src/bench_support.rs` — re-point the bench preallocator at the new production `preallocate_to` (DRY).

---

## Task 1: `decode_record` — zero length-prefix is a torn tail, not corruption

**Files:**
- Modify: `ultima_journal/src/journal/segment.rs:128-137` (inside `decode_record`)
- Test: `ultima_journal/src/journal/segment.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: existing `pub fn decode_record(bytes, segment_name, offset) -> Result<Option<(DecodedRecord, usize)>, JournalError>`.
- Produces: same signature; new behavior — `body_len == 0` returns `Ok(None)` regardless of remaining bytes.

- [ ] **Step 1: Write the failing tests**

Add to the `#[cfg(test)] mod tests` block in `segment.rs`:

```rust
#[test]
fn decode_zero_length_prefix_is_torn_tail() {
    // A preallocated tail / not-yet-written record: u32 len-prefix == 0 with
    // abundant trailing zero bytes must read as end-of-records, NOT corruption.
    let buf = vec![0u8; 4096];
    let got = decode_record(&buf, "seg-test", 32).unwrap();
    assert!(got.is_none(), "zero length-prefix must be torn tail (Ok(None))");
}

#[test]
fn decode_nonzero_bad_length_with_trailing_bytes_still_corrupts() {
    // A non-zero but sub-minimum body_len, with enough bytes to confirm it,
    // is genuine corruption and must still error — the zero-prefix rule must
    // not weaken this.
    let mut buf = vec![0u8; 4096];
    buf[0..4].copy_from_slice(&5u32.to_le_bytes()); // body_len = 5 (< 16)
    let err = decode_record(&buf, "seg-test", 32).unwrap_err();
    assert!(matches!(err, JournalError::Corrupted { .. }));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib decode_zero_length_prefix_is_torn_tail`
Expected: FAIL — currently returns `Err(Corrupted{"body_len 0 < 16"})`, so `got.is_none()` panics.

- [ ] **Step 3: Implement the zero-prefix branch**

In `segment.rs`, replace the `if body_len < 16 {` block (currently `segment.rs:128-137`) with:

```rust
    if body_len == 0 {
        // A zero length-prefix marks an unwritten region. Two callers surface
        // here: the lock-free live reader (length is written atomically AFTER
        // the body, so 0 = record not yet committed) and a preallocated
        // segment's zero tail. Both mean "end of records" — stop cleanly
        // rather than treating the zeros as a corrupt record.
        return Ok(None);
    }
    if body_len < 16 {
        if bytes.len() < total {
            return Ok(None); // torn tail (could be incomplete record with bad length)
        }
        return Err(JournalError::Corrupted {
            segment: segment_name.into(),
            offset,
            reason: format!("body_len {} < 16 (seq+meta minimum)", body_len),
        });
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib decode_`
Expected: PASS (both new tests).

- [ ] **Step 5: Run the full segment + journal suite (no regression)**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib && cargo clippy -p ultima-journal -- -D warnings`
Expected: PASS, zero warnings.

- [ ] **Step 6: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add ultima_journal/src/journal/segment.rs
git commit -m "fix(journal): treat zero length-prefix as torn tail in decode_record

Aligns recovery with the wire format's len=0 = unwritten semantics the
live reader already honors; prerequisite for preallocated segment tails."
```

---

## Task 2: `SegmentFile` primitives — `preallocate_to`, `reset_cursor`, `physical_len`

**Files:**
- Modify: `ultima_journal/src/journal/segment.rs` (impl `SegmentFile`, near the bench helper at ~`segment.rs:356`)
- Modify: `ultima_journal/src/bench_support.rs` (re-point bench preallocator)
- Test: `ultima_journal/src/journal/segment.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces:
  - `pub(crate) fn preallocate_to(&mut self, total_len: u64) -> Result<(), JournalError>` — real zero-fill from the logical cursor `self.size` to `total_len` + `sync_all`; leaves `self.size` untouched. No-op if `total_len <= self.size`.
  - `pub(crate) fn reset_cursor(&mut self, offset: u64)` — set the logical write cursor to `offset` and drop index entries at/after it. NO file I/O (unlike `truncate`).
  - `pub(crate) fn physical_len(&self) -> Result<u64, JournalError>` — on-disk file length (may exceed logical `size` when preallocated).

- [ ] **Step 1: Write the failing tests**

Add to `segment.rs` tests:

```rust
#[test]
fn preallocate_to_extends_physical_without_moving_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg-test.log");
    let mut seg = SegmentFile::create(&path, 1).unwrap();
    let logical_before = seg.size().unwrap();
    seg.preallocate_to(1 * 1024 * 1024).unwrap();
    assert_eq!(seg.size().unwrap(), logical_before, "logical cursor unchanged");
    assert_eq!(seg.physical_len().unwrap(), 1 * 1024 * 1024, "physical extended");
}

#[test]
fn reset_cursor_sets_logical_size_without_truncating_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg-test.log");
    let mut seg = SegmentFile::create(&path, 1).unwrap();
    seg.preallocate_to(1 * 1024 * 1024).unwrap();
    seg.append_records(&[(1, 0, b"hello")]).unwrap();
    let after_append = seg.size().unwrap();
    seg.reset_cursor(SEGMENT_HEADER_SIZE as u64);
    assert_eq!(seg.size().unwrap(), SEGMENT_HEADER_SIZE as u64, "cursor reset");
    assert!(after_append > SEGMENT_HEADER_SIZE as u64);
    assert_eq!(seg.physical_len().unwrap(), 1 * 1024 * 1024, "physical preserved");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib preallocate_to_extends`
Expected: FAIL — `preallocate_to` / `physical_len` not defined.

- [ ] **Step 3: Implement the three methods**

In `segment.rs`, inside `impl SegmentFile`, after `fsync_handle` (~`segment.rs:346`), add:

```rust
    /// On-disk file length. Differs from the logical `size` cursor when the
    /// segment is preallocated (physical EOF == segment_size, logical cursor
    /// at the end of written records).
    pub(crate) fn physical_len(&self) -> Result<u64, JournalError> {
        Ok(self.file.metadata()?.len())
    }

    /// Physically zero-fill from the logical cursor `self.size` out to
    /// `total_len` and `sync_all` once, WITHOUT advancing the cursor. Forces
    /// the filesystem to allocate and mark the extents *written* up front (a
    /// real write, not a sparse `set_len`/`fallocate`, which on ext4 leaves
    /// unwritten extents that re-journal a metadata commit on first
    /// overwrite). Appends then overwrite already-written blocks, so the
    /// per-commit `sync_data` carries no `i_size`/extent-map change.
    pub(crate) fn preallocate_to(&mut self, total_len: u64) -> Result<(), JournalError> {
        if total_len <= self.size {
            return Ok(());
        }
        let zeros = vec![0u8; 1024 * 1024];
        self.file.seek(SeekFrom::Start(self.size))?;
        let mut remaining = total_len - self.size;
        while remaining > 0 {
            let n = remaining.min(zeros.len() as u64) as usize;
            self.file.write_all(&zeros[..n])?;
            remaining -= n as u64;
        }
        self.file.sync_all()?;
        Ok(())
    }

    /// Reset the logical write cursor to `offset` and drop index entries at or
    /// past it, WITHOUT touching the file (no `set_len`, no fsync). Used by
    /// preallocation recovery to rewind to the last durable record while
    /// preserving the physical zero tail for the writer to overwrite — the
    /// non-truncating counterpart of [`truncate`].
    pub(crate) fn reset_cursor(&mut self, offset: u64) {
        self.size = offset;
        self.index.retain(|(_, off)| *off < offset);
    }
```

- [ ] **Step 4: DRY the bench helper onto `preallocate_to`**

In `bench_support.rs`, the bench path should reuse the production method. Replace the body of `SegmentFile::preallocate_zerofill_for_bench` (`segment.rs`, `#[cfg(feature = "bench-support")]`) so it delegates:

```rust
    #[cfg(feature = "bench-support")]
    pub(crate) fn preallocate_zerofill_for_bench(
        &mut self,
        total_len: u64,
    ) -> Result<(), JournalError> {
        self.preallocate_to(total_len)
    }
```

- [ ] **Step 5: Run tests + bench feature build**

Run:
```bash
cd /home/claude/ultima/ultima_db
cargo test -p ultima-journal --lib preallocate_to_extends reset_cursor_sets
cargo build -p ultima-journal --features bench-support
cargo clippy -p ultima-journal -- -D warnings
```
Expected: tests PASS; bench-support build OK; zero warnings.

- [ ] **Step 6: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add ultima_journal/src/journal/segment.rs ultima_journal/src/bench_support.rs
git commit -m "feat(journal): SegmentFile preallocate_to / reset_cursor / physical_len

Production preallocation primitives; bench helper now delegates to them."
```

---

## Task 3: `SegmentFile` temp lifecycle — `create_prealloc_temp`, `activate_prealloc_temp`

**Files:**
- Modify: `ultima_journal/src/journal/segment.rs` (associated fns on `SegmentFile`)
- Test: `ultima_journal/src/journal/segment.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces:
  - `pub(crate) fn create_prealloc_temp(path: &Path, total_len: u64) -> Result<(), JournalError>` — create a NEW file at `path`, zero-fill `[0, total_len)`, `sync_all`, fsync the parent dir. No header (base_seq unknown yet).
  - `pub(crate) fn activate_prealloc_temp(temp: &Path, final_path: &Path, base_seq: u64) -> Result<SegmentFile, JournalError>` — open `temp` (rw), write the 32-byte header (`base_seq`) at offset 0, `sync_data`, `rename(temp → final_path)`, fsync the dir, return a `SegmentFile` with `size = SEGMENT_HEADER_SIZE` and physical EOF preserved.

- [ ] **Step 1: Write the failing test**

Add to `segment.rs` tests:

```rust
#[test]
fn prealloc_temp_create_then_activate_yields_usable_segment() {
    let dir = tempfile::tempdir().unwrap();
    let temp = dir.path().join("seg-prealloc.0.tmp");
    let final_path = dir.path().join("seg-00000000000000000007.log");

    SegmentFile::create_prealloc_temp(&temp, 1 * 1024 * 1024).unwrap();
    assert!(temp.exists());

    let mut seg = SegmentFile::activate_prealloc_temp(&temp, &final_path, 7).unwrap();
    assert!(!temp.exists(), "temp renamed away");
    assert!(final_path.exists());
    assert_eq!(seg.base_seq(), 7);
    assert_eq!(seg.size().unwrap(), SEGMENT_HEADER_SIZE as u64, "logical cursor at header");
    assert_eq!(seg.physical_len().unwrap(), 1 * 1024 * 1024, "preallocated tail preserved");

    // The activated segment is a normal append target.
    seg.append_records(&[(7, 0, b"first")]).unwrap();
    let scan = seg.scan().unwrap();
    assert_eq!(scan.records.len(), 1);
    assert_eq!(scan.records[0].seq, 7);
    assert!(scan.had_torn_tail, "zero tail after the record reads as torn tail");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib prealloc_temp_create_then_activate`
Expected: FAIL — `create_prealloc_temp` not defined.

- [ ] **Step 3: Implement the two associated fns**

In `segment.rs`, inside `impl SegmentFile`, after `create` (~`segment.rs:218`), add:

```rust
    /// Create a NEW preallocated temp segment file: zero-fill `[0, total_len)`
    /// with real writes, `sync_all`, and fsync the parent dir so the file
    /// survives a crash. Writes NO header — `base_seq` is unknown until the
    /// temp is activated at rotation. See `journal::segment_pipeline`.
    pub(crate) fn create_prealloc_temp(path: &Path, total_len: u64) -> Result<(), JournalError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let zeros = vec![0u8; 1024 * 1024];
        let mut remaining = total_len;
        while remaining > 0 {
            let n = remaining.min(zeros.len() as u64) as usize;
            file.write_all(&zeros[..n])?;
            remaining -= n as u64;
        }
        file.sync_all()?;
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    /// Activate a preallocated temp into the live segment `final_path`: write
    /// the real header (overwriting the zeroed first block — already allocated,
    /// so no `i_size` change), `sync_data`, atomically rename the temp into
    /// place, and fsync the dir to make the rename durable. The returned
    /// `SegmentFile` has its logical cursor at the header and the preallocated
    /// zero tail intact for appends to overwrite.
    pub(crate) fn activate_prealloc_temp(
        temp: &Path,
        final_path: &Path,
        base_seq: u64,
    ) -> Result<SegmentFile, JournalError> {
        let mut file = OpenOptions::new().read(true).write(true).open(temp)?;
        let header = SegmentHeader {
            format_ver: SEGMENT_FORMAT_V,
            base_seq,
            created_at: now_nanos(),
        };
        let bytes = encode_header(&header);
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&bytes)?;
        file.sync_data()?;
        std::fs::rename(temp, final_path)?;
        if let Some(parent) = final_path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(SegmentFile {
            path: final_path.to_path_buf(),
            file,
            size: SEGMENT_HEADER_SIZE as u64,
            base_seq,
            index: Vec::new(),
        })
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib prealloc_temp_create_then_activate`
Expected: PASS.

- [ ] **Step 5: Clippy + commit**

```bash
cd /home/claude/ultima/ultima_db
cargo clippy -p ultima-journal -- -D warnings
git add ultima_journal/src/journal/segment.rs
git commit -m "feat(journal): SegmentFile create_prealloc_temp / activate_prealloc_temp

Header-at-activation + atomic rename; the on-disk half of the pipeline."
```

---

## Task 4: `SegmentPipeline` background pre-creator

**Files:**
- Create: `ultima_journal/src/journal/segment_pipeline.rs`
- Modify: `ultima_journal/src/journal/mod.rs:4-5` (add `mod segment_pipeline;`)
- Test: `ultima_journal/src/journal/segment_pipeline.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces:
  - `pub(crate) struct SegmentPipeline`
  - `pub(crate) fn spawn(dir: PathBuf, segment_size: u64) -> Result<Arc<SegmentPipeline>, JournalError>` — starts the helper thread and blocks until the first temp is ready (pre-warm, open-time).
  - `pub(crate) fn take_ready(&self) -> Result<PathBuf, JournalError>` — block until a ready temp exists, return its path, and trigger preparation of the next. Errors if the preallocator thread has failed.
  - `pub(crate) fn shutdown(&self)` — stop the thread, join it, and remove any unconsumed ready temp.

- [ ] **Step 1: Write the failing test**

Create `segment_pipeline.rs` with the module skeleton's test first (the impl in Step 3 replaces the `todo!`s):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_hands_off_then_prepares_next() {
        let dir = tempfile::tempdir().unwrap();
        let pipe = SegmentPipeline::spawn(dir.path().to_path_buf(), 256 * 1024).unwrap();

        let first = pipe.take_ready().unwrap();
        assert!(first.exists(), "first temp ready after spawn");
        assert_eq!(first.metadata().unwrap().len(), 256 * 1024, "preallocated");

        // Next must be prepared in the background; a second take_ready returns a
        // DIFFERENT path.
        let second = pipe.take_ready().unwrap();
        assert!(second.exists());
        assert_ne!(first, second, "pipeline prepared a fresh temp");

        pipe.shutdown();
    }

    #[test]
    fn shutdown_removes_unconsumed_temp() {
        let dir = tempfile::tempdir().unwrap();
        let pipe = SegmentPipeline::spawn(dir.path().to_path_buf(), 256 * 1024).unwrap();
        pipe.shutdown();
        // After shutdown no ready temp should linger on disk.
        let leftover: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let n = e.file_name();
                let s = n.to_string_lossy();
                s.starts_with("seg-prealloc.") && s.ends_with(".tmp")
            })
            .collect();
        assert!(leftover.is_empty(), "shutdown cleaned the ready temp");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib pipeline_hands_off`
Expected: FAIL — module/types not defined (won't compile until Step 3).

- [ ] **Step 3: Implement the pipeline**

Put this at the TOP of `segment_pipeline.rs` (above the test module):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Background segment pre-creator — keeps one preallocated temp segment ready
//! so rotation never zero-fills on the commit path. The etcd `filePipeline`
//! analog. Active only when `JournalConfig.preallocate_segments` is set.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use crate::JournalError;
use crate::journal::segment::SegmentFile;

pub(crate) struct SegmentPipeline {
    shared: Arc<Shared>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

struct Shared {
    dir: PathBuf,
    segment_size: u64,
    counter: AtomicU64,
    shutdown: AtomicBool,
    slot: Mutex<Slot>,
    cv: Condvar,
}

#[derive(Default)]
struct Slot {
    ready: Option<PathBuf>,
    failed: bool,
}

impl SegmentPipeline {
    pub(crate) fn spawn(
        dir: PathBuf,
        segment_size: u64,
    ) -> Result<Arc<SegmentPipeline>, JournalError> {
        let shared = Arc::new(Shared {
            dir,
            segment_size,
            counter: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            slot: Mutex::new(Slot::default()),
            cv: Condvar::new(),
        });
        let worker = Arc::clone(&shared);
        let handle = thread::spawn(move || preallocator_loop(worker));

        // Pre-warm: block until the first temp is ready (or the worker failed),
        // so the first rotation never stalls.
        {
            let mut slot = shared.slot.lock().unwrap();
            while slot.ready.is_none() && !slot.failed {
                slot = shared.cv.wait(slot).unwrap();
            }
            if slot.failed {
                drop(slot);
                let _ = handle.join();
                return Err(prealloc_failed());
            }
        }
        Ok(Arc::new(SegmentPipeline {
            shared,
            handle: Mutex::new(Some(handle)),
        }))
    }

    /// Take the ready temp (blocking until one exists), then signal the worker
    /// to prepare the next. Returns the temp's path; the caller renames it into
    /// place via `SegmentFile::activate_prealloc_temp`.
    pub(crate) fn take_ready(&self) -> Result<PathBuf, JournalError> {
        let mut slot = self.shared.slot.lock().unwrap();
        loop {
            if slot.failed {
                return Err(prealloc_failed());
            }
            if let Some(path) = slot.ready.take() {
                // Wake the worker to prepare the next one.
                self.shared.cv.notify_all();
                return Ok(path);
            }
            slot = self.shared.cv.wait(slot).unwrap();
        }
    }

    pub(crate) fn shutdown(&self) {
        self.shared.shutdown.store(true, Ordering::Release);
        self.shared.cv.notify_all();
        if let Some(h) = self.handle.lock().unwrap().take() {
            let _ = h.join();
        }
        // Remove any temp the worker left ready but unconsumed.
        let mut slot = self.shared.slot.lock().unwrap();
        if let Some(path) = slot.ready.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn prealloc_failed() -> JournalError {
    JournalError::Io(std::io::Error::other("segment preallocation failed"))
}

fn next_temp_path(shared: &Shared) -> PathBuf {
    let n = shared.counter.fetch_add(1, Ordering::Relaxed);
    shared.dir.join(format!("seg-prealloc.{n}.tmp"))
}

fn preallocator_loop(shared: Arc<Shared>) {
    loop {
        if shared.shutdown.load(Ordering::Acquire) {
            return;
        }
        // Only build a new temp when the slot is empty.
        let need_one = {
            let slot = shared.slot.lock().unwrap();
            slot.ready.is_none() && !slot.failed
        };
        if need_one {
            let path = next_temp_path(&shared);
            match SegmentFile::create_prealloc_temp(&path, shared.segment_size) {
                Ok(()) => {
                    let mut slot = shared.slot.lock().unwrap();
                    slot.ready = Some(path);
                    shared.cv.notify_all();
                }
                Err(_) => {
                    let mut slot = shared.slot.lock().unwrap();
                    slot.failed = true;
                    shared.cv.notify_all();
                    return;
                }
            }
            continue;
        }
        // Slot full (or failed): wait until it's consumed or we're told to stop.
        let mut slot = shared.slot.lock().unwrap();
        while slot.ready.is_some()
            && !shared.shutdown.load(Ordering::Acquire)
            && !slot.failed
        {
            slot = shared.cv.wait(slot).unwrap();
        }
    }
}
```

Then register the module — in `mod.rs`, change `mod writer;` (`mod.rs:5`) area to also declare:

```rust
pub(crate) mod segment;
mod segment_pipeline;
mod writer;
```

Confirm `JournalError::Io(std::io::Error)` exists (it is used at `bench_support.rs:44` via `JournalError::Io`); if the variant constructor differs, match the existing `Io` variant shape in `error.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib pipeline_hands_off shutdown_removes_unconsumed`
Expected: PASS both.

- [ ] **Step 5: Clippy + commit**

```bash
cd /home/claude/ultima/ultima_db
cargo clippy -p ultima-journal -- -D warnings
git add ultima_journal/src/journal/segment_pipeline.rs ultima_journal/src/journal/mod.rs
git commit -m "feat(journal): SegmentPipeline background segment pre-creator

One-slot double-buffer; pre-warms first temp; clean shutdown removes it."
```

---

## Task 5: `JournalConfig.preallocate_segments` flag + `WriterState.pipeline` field

**Files:**
- Modify: `ultima_journal/src/journal/mod.rs:14-27` (`JournalConfig`)
- Modify: `ultima_journal/src/journal/writer.rs:220-254` (`WriterState`)
- Modify: `ultima_journal/src/journal/mod.rs:142-157` (`WriterState` construction in `open`)
- Test: `ultima_journal/src/journal/mod.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `SegmentPipeline` (Task 4).
- Produces:
  - `JournalConfig.preallocate_segments: bool` (default `false`).
  - `WriterState.pipeline: Option<Arc<crate::journal::segment_pipeline::SegmentPipeline>>` (default `None`; populated in Task 6).

- [ ] **Step 1: Write the failing test**

Add to `mod.rs` tests:

```rust
#[test]
fn config_preallocate_defaults_off() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = JournalConfig::new(dir.path());
    assert!(!cfg.preallocate_segments, "preallocation is opt-in (default off)");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib config_preallocate_defaults_off`
Expected: FAIL — field does not exist (compile error).

- [ ] **Step 3: Add the config field**

In `mod.rs`, extend `JournalConfig` (`mod.rs:14-18`):

```rust
#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub dir: std::path::PathBuf,
    pub segment_size_bytes: u64,
    pub durability: crate::Durability,
    /// Opt-in (default false): preallocate each segment to `segment_size_bytes`
    /// up front so the per-commit `fdatasync` skips the ext4 metadata commit a
    /// size-extending append otherwise forces. See task on segment preallocation.
    pub preallocate_segments: bool,
}
```

And the constructor (`mod.rs:21-27`):

```rust
    pub fn new(dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            segment_size_bytes: 64 * 1024 * 1024,
            durability: crate::Durability::Consistent,
            preallocate_segments: false,
        }
    }
```

- [ ] **Step 4: Add the `WriterState.pipeline` field**

In `writer.rs`, add to `WriterState` (after `dir`, `writer.rs:221`):

```rust
    /// Active only when `preallocate_segments` is set. Supplies preallocated
    /// temp segments for rotation so the zero-fill never hits the commit path.
    pub pipeline: Option<Arc<crate::journal::segment_pipeline::SegmentPipeline>>,
```

Ensure `use std::sync::Arc;` is in scope in `writer.rs` (it already uses `Arc` for `state`).

In `mod.rs`, the `WriterState { ... }` literal in `open` (`mod.rs:142-157`) must set the new field. For now (no spawn yet — Task 6 wires it) add:

```rust
            pipeline: None,
```

(placed alongside `dir`, before `segment_size`).

- [ ] **Step 5: Run test + full suite (flag-off path unchanged)**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib && cargo clippy -p ultima-journal -- -D warnings`
Expected: PASS, zero warnings.

- [ ] **Step 6: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add ultima_journal/src/journal/mod.rs ultima_journal/src/journal/writer.rs
git commit -m "feat(journal): add preallocate_segments config flag (default off) + WriterState.pipeline slot"
```

---

## Task 6: Wire activation into rotation + spawn/shutdown the pipeline

**Files:**
- Modify: `ultima_journal/src/journal/writer.rs:506-514` (`write_batch` rotation)
- Modify: `ultima_journal/src/journal/mod.rs:142-164` (`open`: spawn pipeline when flag on)
- Modify: `ultima_journal/src/journal/mod.rs:468-474` and `mod.rs:477-491` (`close` + `Drop`: shut the pipeline down)
- Test: `ultima_journal/src/journal/mod.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `SegmentPipeline::spawn/take_ready/shutdown`, `SegmentFile::activate_prealloc_temp`.
- Produces: rotation uses the pipeline when `WriterState.pipeline.is_some()`; otherwise the unchanged `SegmentFile::create` path.

- [ ] **Step 1: Write the failing test**

Add to `mod.rs` tests:

```rust
#[test]
fn preallocated_journal_rotates_and_recovers_full_range() {
    // With the flag on, write across several segments (small segment_size to
    // force rotations), wait durable, reopen, and confirm the full range.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.preallocate_segments = true;
    cfg.segment_size_bytes = 64 * 1024; // small, but >> a few records → pipeline keeps up
    {
        let j = Journal::open(cfg.clone()).unwrap();
        for i in 1..=200u64 {
            j.append(i, i, format!("rec-{i}").as_bytes()).unwrap();
        }
        j.wait_durable(200).unwrap();
        assert_eq!(j.durable_seq(), 200);
    }
    let j2 = Journal::open(cfg).unwrap();
    assert_eq!(j2.first_seq(), Some(1));
    assert_eq!(j2.last_seq(), Some(200));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib preallocated_journal_rotates_and_recovers`
Expected: FAIL — pipeline never spawned (rotation still uses plain `create`; but more importantly recovery will reject the zero tail until Task 7). It is expected to fail here; Task 7 completes recovery. (If it happens to pass after Task 6 because the small segments fill exactly, Task 7 still hardens the tail-preservation path. Treat a FAIL here as expected progress.)

> Note for the implementer: this test depends on BOTH Task 6 (spawn + activation) and Task 7 (recovery tail-preservation). Keep it failing-then-passing across the two tasks — do not delete it; Task 7 Step 4 re-runs it green.

- [ ] **Step 3: Wire activation into `write_batch`**

In `writer.rs`, replace the rotation block (`writer.rs:506-514`):

```rust
        // Rotate when there is no segment yet, or the current one is full.
        let need_new = st.segments.is_empty() || projected >= st.segment_size;
        if need_new {
            // Flush whatever was accumulated for the now-full segment first.
            flush_run(&mut st, &mut run)?;
            let final_path = st.dir.join(format!("seg-{:020}.log", req.seq));
            let seg = match st.pipeline.clone() {
                Some(pipe) => {
                    // Take a ready preallocated temp and activate it (header +
                    // atomic rename). The zero-fill already happened off the
                    // commit path on the pipeline thread.
                    let temp = pipe.take_ready()?;
                    SegmentFile::activate_prealloc_temp(&temp, &final_path, req.seq)?
                }
                None => SegmentFile::create(&final_path, req.seq)?,
            };
            st.segments.push(seg);
            projected = st.segments.last().unwrap().size()?;
        }
```

(The `st.pipeline.clone()` clones the `Option<Arc<…>>` so we don't hold an immutable borrow of `st` across the `st.segments.push`.)

- [ ] **Step 4: Spawn the pipeline in `Journal::open`**

In `mod.rs`, just before constructing `WriterState` (`mod.rs:142`), add:

```rust
        let pipeline = if config.preallocate_segments {
            Some(crate::journal::segment_pipeline::SegmentPipeline::spawn(
                config.dir.clone(),
                config.segment_size_bytes,
            )?)
        } else {
            None
        };
```

and set the field in the `WriterState { … }` literal (replacing the temporary `pipeline: None` from Task 5):

```rust
            pipeline,
```

- [ ] **Step 5: Shut the pipeline down in `close` and `Drop`**

Both `close` (`mod.rs:468-474`) and `Drop::drop` (`mod.rs:477-491`) take + join the writer. AFTER the writer thread is joined (so no activation can race), add pipeline shutdown. In each, after the writer-join block and before/around `self.durability.close()`:

```rust
        // Stop the preallocator thread and remove any unconsumed temp. Done
        // after the writer is joined so no activation can race the shutdown.
        if let Some(pipe) = self.state.lock().unwrap().pipeline.take() {
            pipe.shutdown();
        }
```

(`Option::take()` on the field leaves `None`, so a second call — `close()` then `Drop` — is a no-op.)

- [ ] **Step 6: Run the flag-off suite to confirm no regression**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib && cargo clippy -p ultima-journal -- -D warnings`
Expected: all pre-existing tests PASS (flag-off path untouched). The new `preallocated_journal_rotates_and_recovers_full_range` may still FAIL pending Task 7 — that is expected.

- [ ] **Step 7: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add ultima_journal/src/journal/writer.rs ultima_journal/src/journal/mod.rs
git commit -m "feat(journal): activate preallocated temps on rotation; spawn/shutdown pipeline

Flag-gated: rotation pulls a ready temp and renames it in; pipeline torn
down after the writer joins. Recovery of the zero tail lands next."
```

---

## Task 7: Recovery — preserve preallocated tail, re-preallocate active, clean orphans

**Files:**
- Modify: `ultima_journal/src/journal/mod.rs:38-48` (orphan cleanup at top of `open`)
- Modify: `ultima_journal/src/journal/mod.rs:50-64` (flag-gated torn-tail handling)
- Modify: `ultima_journal/src/journal/mod.rs:132-141` (re-preallocate active segment after recovery)
- Test: `ultima_journal/src/journal/mod.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `SegmentFile::reset_cursor`, `preallocate_to`, `physical_len` (Task 2); `JournalConfig.preallocate_segments` (Task 5).
- Produces: a recovered preallocated journal whose active segment keeps its zero tail and is re-preallocated to `segment_size`; orphan temps removed at open.

- [ ] **Step 1: Write the failing tests**

Add to `mod.rs` tests:

```rust
#[test]
fn open_removes_orphan_prealloc_temps() {
    let dir = tempfile::tempdir().unwrap();
    // Plant an orphan temp as if a crash hit between create and rename.
    std::fs::write(dir.path().join("seg-prealloc.9.tmp"), vec![0u8; 4096]).unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.preallocate_segments = true;
    cfg.segment_size_bytes = 64 * 1024;
    let _j = Journal::open(cfg).unwrap();
    assert!(
        !dir.path().join("seg-prealloc.9.tmp").exists(),
        "orphan temp cleaned at open"
    );
}

#[test]
fn reopen_preallocated_active_segment_keeps_zero_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.preallocate_segments = true;
    cfg.segment_size_bytes = 64 * 1024;
    {
        let j = Journal::open(cfg.clone()).unwrap();
        for i in 1..=10u64 {
            j.append(i, i, b"x").unwrap();
        }
        j.wait_durable(10).unwrap();
    }
    let j2 = Journal::open(cfg).unwrap();
    assert_eq!(j2.first_seq(), Some(1));
    assert_eq!(j2.last_seq(), Some(10));
    // Active segment must be physically re-preallocated to segment_size, not
    // truncated to the logical end.
    let st = j2.state.lock().unwrap();
    let active = st.segments.last().unwrap();
    assert_eq!(
        active.physical_len().unwrap(),
        64 * 1024,
        "active segment re-preallocated after recovery"
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/claude/ultima/ultima_db && cargo test -p ultima-journal --lib open_removes_orphan_prealloc_temps reopen_preallocated_active_segment_keeps_zero_tail`
Expected: FAIL — orphan persists; recovery currently `truncate`s the tail (physical_len would equal the logical end, not 64 KiB) or errors.

- [ ] **Step 3a: Orphan temp cleanup at top of `open`**

In `mod.rs`, right after `std::fs::create_dir_all(&config.dir)?;` (`mod.rs:39`), add:

```rust
        // Remove orphan preallocation temps (a crash between temp-create and
        // activation rename). They hold zero committed records — never
        // referenced until their atomic rename to seg-{seq}.log.
        if config.preallocate_segments {
            if let Ok(rd) = std::fs::read_dir(&config.dir) {
                for ent in rd.filter_map(|e| e.ok()) {
                    let n = ent.file_name();
                    let s = n.to_string_lossy();
                    if s.starts_with("seg-prealloc.") && s.ends_with(".tmp") {
                        let _ = std::fs::remove_file(ent.path());
                    }
                }
            }
        }
```

- [ ] **Step 3b: Flag-gated torn-tail handling**

In `mod.rs`, replace the Phase 1 torn-tail block (`mod.rs:54-62`):

```rust
            let mut seg = segment::SegmentFile::open_for_read(&ent.path())?;
            let scan = seg.scan()?;
            let scan = if scan.had_torn_tail {
                if config.preallocate_segments {
                    // Preserve the physical zero tail; only rewind the logical
                    // cursor to the last durable record. The writer resumes
                    // appending into preallocated space (no truncate-then-refill
                    // churn). The first scan's records/index already exclude the
                    // torn tail, so reuse it directly.
                    seg.reset_cursor(scan.last_durable_offset);
                    scan
                } else {
                    seg.truncate(scan.last_durable_offset)?;
                    seg.scan()?
                }
            } else {
                scan
            };
            segments_with_scan.push((seg, scan));
```

- [ ] **Step 3c: Re-preallocate the active segment after recovery**

In `mod.rs`, after the segments `Vec` is built (`mod.rs:134-140`) and before `let state = Arc::new(Mutex::new(WriterState { … }))` (`mod.rs:142`), add:

```rust
        // Re-preallocate the active segment so steady-state preallocation is in
        // effect immediately after restart — covers a segment written before
        // the flag was enabled, or one a prior recovery physically truncated.
        let mut segments = segments;
        if config.preallocate_segments {
            if let Some(active) = segments.last_mut() {
                if active.physical_len()? < config.segment_size_bytes {
                    active.preallocate_to(config.segment_size_bytes)?;
                }
            }
        }
```

(Change the existing `let segments: Vec<…> = …;` binding to `let segments: Vec<…> = …;` then this `let mut segments = segments;` rebinding, OR make the original binding `let mut segments`. Pick one; do not leave both immutable and mutable bindings unused.)

- [ ] **Step 4: Run the new tests AND the Task 6 rotation test**

Run:
```bash
cd /home/claude/ultima/ultima_db
cargo test -p ultima-journal --lib open_removes_orphan_prealloc_temps \
  reopen_preallocated_active_segment_keeps_zero_tail \
  preallocated_journal_rotates_and_recovers_full_range
```
Expected: all PASS (Task 6's test now goes green).

- [ ] **Step 5: Full suite + clippy + commit**

```bash
cd /home/claude/ultima/ultima_db
cargo test -p ultima-journal --lib
cargo clippy -p ultima-journal -- -D warnings
git add ultima_journal/src/journal/mod.rs
git commit -m "feat(journal): preallocation-aware recovery

Preserve the zero tail (reset cursor, not truncate), re-preallocate the
active segment at open, and clean orphan prealloc temps."
```

---

## Task 8: Crash-recovery + durability integration tests (flag on) + flag-off regression

**Files:**
- Test: `ultima_journal/src/journal/mod.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: the full feature (Tasks 1–7).
- Produces: durability + crash-recovery coverage with the flag on; explicit flag-off equivalence.

- [ ] **Step 1: Write the tests**

Add to `mod.rs` tests:

```rust
#[test]
fn consistent_durability_survives_reopen_preallocated() {
    // The flag-on twin of consistent_durability_survives_reopen: fdatasync into
    // a preallocated segment must still make every acked record durable.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.durability = crate::Durability::Consistent;
    cfg.preallocate_segments = true;
    {
        let j = Journal::open(cfg.clone()).unwrap();
        for i in 1..=64u64 {
            j.append(i, i, format!("rec-{i}").as_bytes()).unwrap();
        }
        j.wait_durable(64).unwrap();
        assert_eq!(j.durable_seq(), 64);
    }
    let j2 = Journal::open(cfg).unwrap();
    assert_eq!(j2.first_seq(), Some(1));
    assert_eq!(j2.last_seq(), Some(64));
}

#[test]
fn preallocated_resume_overwrites_zero_tail_correctly() {
    // Reopen a preallocated journal and append MORE: the writer must resume at
    // the logical cursor and overwrite the zero tail, and the combined range
    // must survive a second reopen.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.preallocate_segments = true;
    cfg.segment_size_bytes = 64 * 1024;
    {
        let j = Journal::open(cfg.clone()).unwrap();
        for i in 1..=20u64 {
            j.append(i, i, b"a").unwrap();
        }
        j.wait_durable(20).unwrap();
    }
    {
        let j = Journal::open(cfg.clone()).unwrap();
        assert_eq!(j.last_seq(), Some(20));
        for i in 21..=40u64 {
            j.append(i, i, b"b").unwrap();
        }
        j.wait_durable(40).unwrap();
    }
    let j3 = Journal::open(cfg).unwrap();
    assert_eq!(j3.first_seq(), Some(1));
    assert_eq!(j3.last_seq(), Some(40));
    // Spot-check a record written into the overwritten zero tail.
    let (m, p) = j3.read(30).unwrap().unwrap();
    assert_eq!(m, 30);
    assert_eq!(p, b"b");
}
```

- [ ] **Step 2: Run the new tests**

Run:
```bash
cd /home/claude/ultima/ultima_db
cargo test -p ultima-journal --lib consistent_durability_survives_reopen_preallocated \
  preallocated_resume_overwrites_zero_tail_correctly
```
Expected: PASS.

- [ ] **Step 3: Full crate suite, both feature sets**

Run:
```bash
cd /home/claude/ultima/ultima_db
cargo test -p ultima-journal --lib
cargo test -p ultima-journal --lib --features bench-support
cargo clippy -p ultima-journal --all-features -- -D warnings
```
Expected: all PASS, zero warnings. (Confirms the flag-off default path and the bench feature are both intact.)

- [ ] **Step 4: Commit**

```bash
cd /home/claude/ultima/ultima_db
git add ultima_journal/src/journal/mod.rs
git commit -m "test(journal): preallocation durability + crash-resume coverage"
```

---

## Task 9: Cluster correctness gates + microbench witness + doc consolidation

**Files:**
- Modify: `ultima_journal/src/journal/mod.rs` (no code — this task is validation + docs)
- Create: `ultima_db/docs/tasks/task36_segment_preallocation.md` (canonical record; task36 is the next free number in `ultima_db/docs/tasks/`)

**Interfaces:**
- Consumes: the full feature.
- Produces: green cluster gates with the flag enabled; the canonical task doc.

- [ ] **Step 1: Run the journal microbench witness (flag effect)**

The `fsync_prealloc_*` microbench already exists. Sanity-confirm the production path matches the bench delta on this host:

Run: `cd /home/claude/ultima/ultima_db && cargo run --release -p ultima-autobench --bin journal-microbench -- --json 2>/dev/null`
Expected: `fsync_prealloc_p50_ns` materially below `fsync_only_p50_ns` (the ~3× barrier cut). Record the numbers in the task doc.

- [ ] **Step 2: Wire the cluster gates against this journal worktree**

`ultima_cluster` depends on the journal via the fixed relative path. To run cluster gates against THIS code without pushing, create a temporary `ultima_cluster/.cargo/config.toml` (the cross-repo override used by the fdatasync work):

```toml
# TEMPORARY — do not commit. Points the journal dep at the working tree.
paths = ["/home/claude/ultima/ultima_db/ultima_journal"]
```

(If the cluster already builds against `../ultima_db/ultima_journal` directly and that IS this working tree, this override is a no-op and can be skipped. Verify with `cargo tree -p ultima-journal` from `ultima_cluster`.)

- [ ] **Step 3: Run the cluster linearizability + hard-crash gates with the flag**

The cluster must enable `preallocate_segments` wherever it builds its `JournalConfig` (search: `rg "JournalConfig::new" ultima_cluster/`). For the gate run, set the flag on in the node's journal config (a temporary local edit or a test-only env toggle — keep it OUT of the committed default until the cloud A/B).

Run:
```bash
cd /home/claude/ultima/ultima_cluster
cargo test -p uc_node --test lin_register
cargo test -p uc-crashtest --features hard-crash-tests
```
Expected: lincheck capstone Linearizable; hard-crash Linearizable across its seeds. Then remove the temporary `.cargo/config.toml`.

- [ ] **Step 4: Write the canonical task doc**

Create `ultima_db/docs/tasks/task36_segment_preallocation.md` covering: the problem (per-commit metadata-commit cost), the microbench decomposition (~390 µs jbd2 + ~210 µs device flush), the etcd `filePipeline` design, the `decode_record` zero-prefix change and its trade-off, the flag (default off), recovery semantics (preserve tail / re-preallocate / orphan cleanup), test coverage, the gate results from Step 3, and the PENDING operator cloud A/B (`submitted→persisted` p50/p99 on prod NVMe) as the authoritative validation before flipping the default. Fold in the essential rationale so the doc stands alone (per CLAUDE.md's consolidation rule); leave the superpowers spec/plan in place.

- [ ] **Step 5: Commit the doc**

```bash
cd /home/claude/ultima/ultima_db
git add docs/tasks/task36_segment_preallocation.md
git commit -m "docs(task36): journal segment preallocation — design, gates, pending cloud A/B"
```

---

## Self-Review

**Spec coverage:**
- §3 config flag (default off) → Task 5. ✓
- §4 `SegmentPipeline` + activation + lifecycle → Tasks 3, 4, 6. ✓
- §5.1 `decode_record` zero-prefix (unconditional) → Task 1. ✓
- §5.2 preserve tail (`reset_cursor` vs `truncate`, flag-gated) → Tasks 2, 7. ✓
- §5.3 re-preallocate active at open → Task 7. ✓
- §6 orphan temp cleanup → Task 7; no-truncate-on-close → implicit (close only shuts the pipeline, Task 6); `truncate_after` re-preallocation → **see note below.** ✓/⚠
- §7 testing (unit, durability, crash, flag-off, cluster gates, bench) → Tasks 1–9. ✓
- §8 O_DIRECT future extension → out of scope, no task (correct). ✓

**Gap found — §6 `truncate_after` re-preallocation:** the spec says after a `truncate_after` lands on the *active* segment with the flag on, re-preallocate its tail. The plan re-preallocates only at `open` (Task 7). This is a correctness-preserving omission (a post-`truncate_after` active segment simply runs un-preallocated until the next rotation or restart — same as the flag-off path, never wrong), but it is a perf gap vs. the spec. **Action:** added as an explicit follow-up note here rather than a full task, because `truncate_after` on the active segment is rare (Raft truncates uncommitted suffixes on leader change) and folding a `preallocate_to` call into `apply_truncate_to_segments` is a one-line addition best done with its own targeted test during implementation if the gate runs surface it. Implementer: if §6 fidelity is required, add a Task 7b mirroring the Task 7 Step 3c logic inside the `truncate_after` path (`mod.rs:402`) guarded by the flag.

**Placeholder scan:** no TBD/TODO; every code step shows complete code; commands have expected output. ✓

**Type consistency:** `preallocate_to`, `reset_cursor`, `physical_len`, `create_prealloc_temp`, `activate_prealloc_temp`, `SegmentPipeline::{spawn,take_ready,shutdown}`, `WriterState.pipeline`, `JournalConfig.preallocate_segments` are used with identical signatures across Tasks 2–8. ✓
