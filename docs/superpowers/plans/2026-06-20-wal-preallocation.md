# WAL Preallocation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in physical-zero-fill preallocation to the ultima_db store WAL's production Coalesced path so each `Durability::Consistent` commit fsync no longer drags an ext4 metadata-journal commit.

**Architecture:** A new `PreallocFileSink` writes framed entries into a physically pre-zeroed region of the single `wal.bin` via positioned writes against a tracked `write_head`, growing the file inline in 16 MiB chunks (`sync_all` on extend, `sync_data` steady-state). Recovery reconstructs `write_head` from a tail-tolerant scan; pruning rewrites a pre-sized tmp + rename. Selected by a new `WalWrite::CoalescedPrealloc` config variant; everything is opt-in and default-off.

**Tech Stack:** Rust 2024, `std::fs` / `std::io` (Seek/Write), existing `crc32`, `bincode`, the `persistence` cargo feature.

## Global Constraints

- All new disk-touching code is behind the `persistence` cargo feature (run tests with `--features persistence`).
- `cargo clippy --features persistence -- -D warnings` must pass with zero warnings.
- On-disk format is unchanged: `[len: u32 LE][bincode(WalEntry)][crc32: u32 LE]`. Reuse `frame_entry`, `crc32`, `deserialize_entry` — do not fork the format.
- Preallocation uses **real zero-fill writes**, never sparse `set_len`/`fallocate` (the point is ext4 marks extents *written*).
- Barrier discipline: `sync_all` only when the file size changes (extend); `sync_data` for steady-state batches.
- Invariant at all times: `write_head ≤ capacity ≤ physical_len(file)`.
- Default config stays `WalWrite::PerEntry`; the feature is opt-in via `WalWrite::CoalescedPrealloc`.
- Grow quantum default: `16 * 1024 * 1024` (16 MiB). Provide a `pub(crate)` test constructor to inject a tiny chunk so extend paths are testable without writing megabytes.
- New code lives in `src/wal.rs` next to the existing sinks; follow the existing sink patterns (`FileSink`, `BufferedFileSink`, `MmapSink`).

---

### Task 1: `scan_wal` — expose the durable byte offset, add tail-tolerant mode

Refactor the `read_wal` scan loop into a `scan_wal(path, tail_tolerant)` that returns both the decoded entries and the durable byte offset (the future `write_head`). `read_wal` becomes a thin strict wrapper, so every existing caller keeps identical (strict) corruption behavior. The tolerant mode treats a CRC mismatch / undecodable frame as end-of-log instead of erroring (needed because a preallocated zero tail makes a torn record look complete — see design §7).

**Files:**
- Modify: `src/wal.rs` (the `read_wal` fn at ~`src/wal.rs:317-364`)
- Test: `src/wal.rs` (`#[cfg(test)] mod tests`, already present)

**Interfaces:**
- Produces: `pub(crate) fn scan_wal(path: &Path, tail_tolerant: bool) -> Result<(Vec<WalEntry>, u64)>` — entries plus the byte offset where scanning stopped (end of last good record).
- Produces: `pub fn read_wal(path: &Path) -> Result<Vec<WalEntry>>` (unchanged signature; now delegates to `scan_wal(path, false)`).

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/wal.rs`:

```rust
#[test]
fn scan_wal_returns_durable_offset_and_strict_wrapper_matches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(WAL_FILENAME);
    // Two framed entries written back-to-back.
    let e1 = WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] };
    let e2 = WalEntry { version: 2, ops: vec![WalOp::DeleteTable { name: "t".into() }] };
    let mut bytes = frame_entry(&e1).unwrap();
    let f1_len = bytes.len() as u64;
    bytes.extend_from_slice(&frame_entry(&e2).unwrap());
    let total = bytes.len() as u64;
    std::fs::write(&path, &bytes).unwrap();

    let (entries, offset) = scan_wal(&path, false).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(offset, total, "offset is end of last good record");
    assert!(f1_len > 0);
    // Strict wrapper returns the same entries.
    assert_eq!(read_wal(&path).unwrap().len(), 2);
}

#[test]
fn scan_wal_tolerant_stops_at_crc_mismatch_strict_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(WAL_FILENAME);
    let good = WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] };
    let mut bytes = frame_entry(&good).unwrap();
    let good_len = bytes.len() as u64;
    // A second frame with a corrupted CRC, then a zero tail (preallocated space).
    let mut torn = frame_entry(&WalEntry { version: 2, ops: vec![WalOp::DeleteTable { name: "t".into() }] }).unwrap();
    let last = torn.len() - 1;
    torn[last] ^= 0xFF; // flip a CRC byte
    bytes.extend_from_slice(&torn);
    bytes.extend_from_slice(&[0u8; 4096]); // durable zeros after the torn record
    std::fs::write(&path, &bytes).unwrap();

    // Tolerant: stop at the good record, no error, offset = end of good record.
    let (entries, offset) = scan_wal(&path, true).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(offset, good_len);
    // Strict: still flags corruption.
    assert!(matches!(scan_wal(&path, false), Err(Error::WalCorrupted(_))));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --features persistence --lib wal::tests::scan_wal -- --nocapture`
Expected: FAIL — `scan_wal` not found.

- [ ] **Step 3: Implement `scan_wal` and rewrap `read_wal`**

Replace the body of `read_wal` (`src/wal.rs:317-364`) with a wrapper, and add `scan_wal` just above it:

```rust
/// Scan framed WAL records. Returns the decoded entries and the byte offset
/// where scanning stopped (end of the last good record = the durable write
/// head). A zero len-prefix and a truncated tail are always end-of-log. When
/// `tail_tolerant`, a CRC mismatch or undecodable frame is *also* treated as
/// end-of-log (a torn write into preallocated zero space looks complete); when
/// not, a CRC mismatch is a hard `WalCorrupted` error (strict corruption
/// detection for the non-preallocated path).
pub(crate) fn scan_wal(path: &Path, tail_tolerant: bool) -> Result<(Vec<WalEntry>, u64)> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(e) => return Err(Error::Persistence(e.to_string())),
    };
    let mut all_bytes = Vec::new();
    file.read_to_end(&mut all_bytes)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    let mut entries = Vec::new();
    let mut offset = 0usize;

    while offset + 4 <= all_bytes.len() {
        let len = u32::from_le_bytes(all_bytes[offset..offset + 4].try_into().unwrap()) as usize;
        if len == 0 {
            break; // zero len-prefix: clean end-of-log / preallocated tail
        }
        if offset + 4 + len + 4 > all_bytes.len() {
            break; // truncated tail (crash during write)
        }
        let data = &all_bytes[offset + 4..offset + 4 + len];
        let stored_crc =
            u32::from_le_bytes(all_bytes[offset + 4 + len..offset + 8 + len].try_into().unwrap());
        if crc32(data) != stored_crc {
            if tail_tolerant {
                break; // torn write into preallocated space: stop at last good record
            }
            return Err(Error::WalCorrupted(format!(
                "CRC mismatch at entry starting at byte {offset}"
            )));
        }
        match deserialize_entry(data) {
            Ok(entry) => entries.push(entry),
            Err(e) if tail_tolerant => {
                let _ = e;
                break;
            }
            Err(e) => return Err(e),
        }
        offset += 4 + len + 4;
    }

    Ok((entries, offset as u64))
}

/// Read all WAL entries from a file. Strict: stops at EOF / zero tail, errors
/// on CRC mismatch. Unchanged behavior for all existing callers.
pub fn read_wal(path: &Path) -> Result<Vec<WalEntry>> {
    Ok(scan_wal(path, false)?.0)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features persistence --lib wal:: -- --nocapture`
Expected: PASS (new `scan_wal` tests + all existing wal tests still green — `read_wal` behavior unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/wal.rs
git commit -m "refactor(wal): extract scan_wal with durable-offset + tail-tolerant mode"
```

---

### Task 2: `preallocate_to` — the physical zero-fill primitive

Copy the journal's zero-fill kernel, tuned for the WAL: physically write zeros from `from` to `to` and `sync_all` once. This is the only "copied from ultima_journal" code; it is ~15 lines and intentionally not shared (design §1).

**Files:**
- Modify: `src/wal.rs`
- Test: `src/wal.rs` tests module

**Interfaces:**
- Produces: `fn preallocate_to(file: &mut File, from: u64, to: u64) -> Result<()>` — zero-fill `[from, to)` with real writes, then `sync_all`. No-op if `to <= from`. Leaves the file cursor unspecified (callers seek before writing).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn preallocate_to_zero_fills_and_is_durable() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("p.bin");
    let mut f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
    preallocate_to(&mut f, 0, 8192).unwrap();
    assert_eq!(f.metadata().unwrap().len(), 8192, "physically extended");
    // All zeros.
    let mut buf = Vec::new();
    f.seek(SeekFrom::Start(0)).unwrap();
    f.read_to_end(&mut buf).unwrap();
    assert!(buf.iter().all(|&b| b == 0));
    // A positioned overwrite within the region does not change the size.
    f.seek(SeekFrom::Start(0)).unwrap();
    f.write_all(&[7u8; 16]).unwrap();
    assert_eq!(f.metadata().unwrap().len(), 8192);
    // No-op when to <= from.
    preallocate_to(&mut f, 8192, 4096).unwrap();
    assert_eq!(f.metadata().unwrap().len(), 8192);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features persistence --lib wal::tests::preallocate_to_zero_fills_and_is_durable`
Expected: FAIL — `preallocate_to` not found.

- [ ] **Step 3: Implement `preallocate_to`**

Add near the other WAL file I/O helpers in `src/wal.rs`:

```rust
/// Physically zero-fill `[from, to)` with real writes (NOT sparse `set_len`,
/// so ext4 marks the extents *written*), then `sync_all` once so the size and
/// allocation are durable before any record is written into the region. The
/// WAL counterpart of `ultima_journal`'s `SegmentFile::preallocate_to`. No-op
/// when `to <= from`.
fn preallocate_to(file: &mut File, from: u64, to: u64) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    if to <= from {
        return Ok(());
    }
    let zeros = [0u8; 1024 * 1024];
    file.seek(SeekFrom::Start(from)).map_err(|e| Error::Persistence(e.to_string()))?;
    let mut remaining = to - from;
    while remaining > 0 {
        let n = remaining.min(zeros.len() as u64) as usize;
        file.write_all(&zeros[..n]).map_err(|e| Error::Persistence(e.to_string()))?;
        remaining -= n as u64;
    }
    file.sync_all().map_err(|e| Error::Persistence(e.to_string()))?;
    Ok(())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --features persistence --lib wal::tests::preallocate_to_zero_fills_and_is_durable`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/wal.rs
git commit -m "feat(wal): add preallocate_to zero-fill primitive (copied from journal, WAL-tuned)"
```

---

### Task 3: `PreallocFileSink` — append/sync with inline grow-ahead

The core sink. Positioned writes against `write_head`, inline extend in chunks when a batch would overrun `capacity`. `open` reconstructs `write_head`/`capacity` via the tolerant scan from Task 1. No prune yet (Task 4).

**Files:**
- Modify: `src/wal.rs`
- Test: `src/wal.rs` tests module

**Interfaces:**
- Consumes: `scan_wal` (Task 1), `preallocate_to` (Task 2), existing `frame_entry`, `WalSink` trait, `WAL_FILENAME`, `sync_dir`.
- Produces:
  - `struct PreallocFileSink { file: File, path: PathBuf, buf: Vec<u8>, write_head: u64, capacity: u64, chunk: u64 }`
  - `impl PreallocFileSink { fn open(dir: &Path) -> Result<Self>; pub(crate) fn open_with_chunk(dir: &Path, chunk: u64) -> Result<Self> }`
  - `impl WalSink for PreallocFileSink` (`append`, `sync`; `prune` added in Task 4).
  - `const WAL_PREALLOC_CHUNK: u64 = 16 * 1024 * 1024;`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn prealloc_sink_roundtrips_like_buffered() {
    let dir = tempfile::tempdir().unwrap();
    let mut sink = PreallocFileSink::open(dir.path()).unwrap();
    sink.append(&WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap();
    sink.append(&WalEntry { version: 2, ops: vec![WalOp::DeleteTable { name: "t".into() }] }).unwrap();
    sink.sync().unwrap();
    let entries = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].version, 1);
    assert_eq!(entries[1].version, 2);
}

#[test]
fn prealloc_sink_extends_in_chunks_and_holds_invariant() {
    let dir = tempfile::tempdir().unwrap();
    // Tiny 256-byte chunk so a couple of entries force an extend.
    let mut sink = PreallocFileSink::open_with_chunk(dir.path(), 256).unwrap();
    let physical = |p: &std::path::Path| std::fs::metadata(p).unwrap().len();
    let path = dir.path().join(WAL_FILENAME);

    for v in 1..=20u64 {
        sink.append(&WalEntry { version: v, ops: vec![WalOp::CreateTable { name: format!("table-{v}") }] }).unwrap();
        sink.sync().unwrap();
        // Invariant: write_head <= capacity <= physical_len, capacity chunk-aligned.
        assert!(sink.write_head <= sink.capacity);
        assert!(sink.capacity <= physical(&path));
        assert_eq!(sink.capacity % 256, 0, "capacity grows in whole chunks");
    }
    assert_eq!(read_wal(&path).unwrap().len(), 20);
}

#[test]
fn prealloc_sink_steady_state_does_not_grow_physical() {
    let dir = tempfile::tempdir().unwrap();
    let mut sink = PreallocFileSink::open_with_chunk(dir.path(), 1 << 20).unwrap();
    let path = dir.path().join(WAL_FILENAME);
    sink.append(&WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap();
    sink.sync().unwrap();
    let after_first = std::fs::metadata(&path).unwrap().len();
    // A second small batch fits in the existing chunk: no physical growth.
    sink.append(&WalEntry { version: 2, ops: vec![WalOp::CreateTable { name: "u".into() }] }).unwrap();
    sink.sync().unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), after_first);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --features persistence --lib wal::tests::prealloc_sink -- --nocapture`
Expected: FAIL — `PreallocFileSink` not found.

- [ ] **Step 3: Implement the sink**

Add to `src/wal.rs` (near `BufferedFileSink`):

```rust
/// Default grow quantum for the preallocating WAL sink.
const WAL_PREALLOC_CHUNK: u64 = 16 * 1024 * 1024;

/// Production preallocating sink: positioned writes into a physically
/// zero-filled region of `wal.bin`, grown inline in `chunk`-byte steps.
/// `sync_all` only on extend (size change); `sync_data` steady-state. See
/// design doc 2026-06-20-wal-preallocation-design.md.
struct PreallocFileSink {
    file: File,
    path: std::path::PathBuf,
    buf: Vec<u8>,
    write_head: u64,
    capacity: u64,
    chunk: u64,
}

impl PreallocFileSink {
    fn open(dir: &Path) -> Result<Self> {
        Self::open_with_chunk(dir, WAL_PREALLOC_CHUNK)
    }

    pub(crate) fn open_with_chunk(dir: &Path, chunk: u64) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let path = dir.join(WAL_FILENAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        sync_dir(dir)?;
        // Reconstruct the write head from the tolerant scan; a torn tail into
        // preallocated zeros is end-of-log, not corruption.
        let (_entries, write_head) = scan_wal(&path, true)?;
        let capacity = file.metadata().map_err(|e| Error::Persistence(e.to_string()))?.len();
        Ok(PreallocFileSink { file, path, buf: Vec::new(), write_head, capacity, chunk })
    }
}

impl WalSink for PreallocFileSink {
    fn append(&mut self, entry: &WalEntry) -> Result<()> {
        self.buf.extend_from_slice(&frame_entry(entry)?);
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        if !self.buf.is_empty() {
            let need = self.write_head + self.buf.len() as u64;
            if need > self.capacity {
                // Extend by whole chunks to cover `need`; sync_all (size change).
                let new_cap = need.div_ceil(self.chunk) * self.chunk;
                preallocate_to(&mut self.file, self.capacity, new_cap)?;
                self.capacity = new_cap;
            }
            self.file.seek(SeekFrom::Start(self.write_head)).map_err(|e| Error::Persistence(e.to_string()))?;
            self.file.write_all(&self.buf).map_err(|e| Error::Persistence(e.to_string()))?;
            self.write_head += self.buf.len() as u64;
            self.buf.clear();
        }
        // Steady-state barrier: size unchanged, so fdatasync suffices.
        self.file.sync_data().map_err(|e| Error::Persistence(e.to_string()))
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features persistence --lib wal::tests::prealloc_sink -- --nocapture`
Expected: PASS (all three).

- [ ] **Step 5: Commit**

```bash
git add src/wal.rs
git commit -m "feat(wal): PreallocFileSink with inline grow-ahead (append/sync)"
```

---

### Task 4: Prune (P2 pre-sized tmp) + crash-safety

Add the preallocating prune: rewrite live entries into a tmp that is itself zero-filled to `(live + chunk)` before the entries are written, then atomic rename. After rename, reopen and reset `write_head`/`capacity`. This is the WAL's rotation point (design §6).

**Files:**
- Modify: `src/wal.rs`
- Test: `src/wal.rs` tests module

**Interfaces:**
- Consumes: `read_wal`, `frame_entry`, `preallocate_to`, `sync_dir`, `PreallocFileSink` fields from Task 3.
- Produces:
  - `fn prune_wal_prealloc(path: &Path, up_to_version: u64, chunk: u64) -> Result<Option<(u64, u64)>>` — rewrites the WAL keeping `version > up_to_version`; returns `Some((write_head, capacity))` after a rewrite or `None` if nothing was pruned.
  - `impl WalSink for PreallocFileSink { fn prune(&mut self, up_to_version: u64) -> Result<()> }`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn prune_wal_prealloc_compacts_and_presizes() {
    let dir = tempfile::tempdir().unwrap();
    let mut sink = PreallocFileSink::open_with_chunk(dir.path(), 4096).unwrap();
    for v in 1..=5u64 {
        sink.append(&WalEntry { version: v, ops: vec![WalOp::CreateTable { name: format!("t{v}") }] }).unwrap();
        sink.sync().unwrap();
    }
    // Prune everything <= version 3.
    sink.prune(3).unwrap();
    let path = dir.path().join(WAL_FILENAME);
    let entries = read_wal(&path).unwrap();
    assert_eq!(entries.iter().map(|e| e.version).collect::<Vec<_>>(), vec![4, 5]);
    // File is preallocated: physical_len == write_head rounded up + at least one chunk of zeros.
    let physical = std::fs::metadata(&path).unwrap().len();
    assert!(sink.capacity <= physical);
    assert!(sink.capacity >= sink.write_head + 4096, "a fresh chunk of zero tail exists");
    assert_eq!(sink.capacity % 4096, 0);
}

#[test]
fn prune_then_append_recovers() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut sink = PreallocFileSink::open_with_chunk(dir.path(), 4096).unwrap();
        for v in 1..=4u64 {
            sink.append(&WalEntry { version: v, ops: vec![WalOp::CreateTable { name: format!("t{v}") }] }).unwrap();
            sink.sync().unwrap();
        }
        sink.prune(2).unwrap();
        sink.append(&WalEntry { version: 5, ops: vec![WalOp::CreateTable { name: "t5".into() }] }).unwrap();
        sink.sync().unwrap();
    } // drop = simulated crash (no clean truncation)
    // Reopen and confirm the post-prune appends survive with no gap.
    let sink2 = PreallocFileSink::open_with_chunk(dir.path(), 4096).unwrap();
    let entries = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
    assert_eq!(entries.iter().map(|e| e.version).collect::<Vec<_>>(), vec![3, 4, 5]);
    assert_eq!(sink2.write_head, entries.iter().map(|e| frame_entry(e).unwrap().len() as u64).sum::<u64>());
}

#[test]
fn prune_wal_prealloc_noop_when_nothing_to_prune() {
    let dir = tempfile::tempdir().unwrap();
    let mut sink = PreallocFileSink::open_with_chunk(dir.path(), 4096).unwrap();
    sink.append(&WalEntry { version: 9, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap();
    sink.sync().unwrap();
    // up_to_version below the only entry's version: nothing removed.
    assert!(prune_wal_prealloc(&dir.path().join(WAL_FILENAME), 1, 4096).unwrap().is_none());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --features persistence --lib wal::tests::prune_ -- --nocapture`
Expected: FAIL — `prune_wal_prealloc` / `PreallocFileSink::prune` not found.

- [ ] **Step 3: Implement prune**

Add `prune_wal_prealloc` near `prune_wal`, and the `prune` method on the sink:

```rust
/// Preallocating prune (design §6, strategy P2): rewrite the live entries
/// (version > `up_to_version`) into a tmp that is first zero-filled to
/// `live_bytes + chunk`, then atomically rename it over `path`. The renamed
/// file is already preallocated. Returns `Some((write_head, capacity))` after a
/// rewrite, or `None` if nothing needed pruning. Crash-atomic via tmp+rename:
/// a crash leaves either the complete old WAL or the complete new one.
fn prune_wal_prealloc(path: &Path, up_to_version: u64, chunk: u64) -> Result<Option<(u64, u64)>> {
    use std::io::{Seek, SeekFrom, Write};
    let entries = read_wal(path)?;
    let remaining: Vec<&WalEntry> = entries.iter().filter(|e| e.version > up_to_version).collect();
    if remaining.len() == entries.len() {
        return Ok(None); // nothing to prune
    }

    let mut live = Vec::new();
    for e in &remaining {
        live.extend_from_slice(&frame_entry(e)?);
    }
    let write_head = live.len() as u64;
    let capacity = (write_head + chunk).div_ceil(chunk) * chunk;

    let tmp_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy()
    ));
    let mut tmp = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&tmp_path)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    // Pre-size to capacity with real zeros (single sync_all), then overwrite the
    // front with the live entries.
    preallocate_to(&mut tmp, 0, capacity)?;
    tmp.seek(SeekFrom::Start(0)).map_err(|e| Error::Persistence(e.to_string()))?;
    tmp.write_all(&live).map_err(|e| Error::Persistence(e.to_string()))?;
    tmp.sync_all().map_err(|e| Error::Persistence(e.to_string()))?;
    drop(tmp);
    std::fs::rename(&tmp_path, path).map_err(|e| Error::Persistence(e.to_string()))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(Some((write_head, capacity)))
}
```

Add to `impl WalSink for PreallocFileSink`:

```rust
    fn prune(&mut self, up_to_version: u64) -> Result<()> {
        if let Some((write_head, capacity)) = prune_wal_prealloc(&self.path, up_to_version, self.chunk)? {
            // Reopen the renamed (new) inode and adopt the recomputed cursors.
            self.file = OpenOptions::new()
                .read(true).write(true).create(true).truncate(false)
                .open(&self.path)
                .map_err(|e| Error::Persistence(e.to_string()))?;
            self.write_head = write_head;
            self.capacity = capacity;
            self.buf.clear();
        }
        Ok(())
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features persistence --lib wal::tests::prune_ -- --nocapture`
Expected: PASS (all three).

- [ ] **Step 5: Commit**

```bash
git add src/wal.rs
git commit -m "feat(wal): preallocating prune (P2 pre-sized tmp) for PreallocFileSink"
```

---

### Task 5: Wire `WalWrite::CoalescedPrealloc` through config → sink

Add the public config variant, the internal `WalSinkKind` variant, the `with_sink_kind` arm, and the `store.rs` mapping. After this task the feature is reachable end-to-end from `StoreConfig`.

**Files:**
- Modify: `src/persistence.rs` (`WalWrite` enum, ~`src/persistence.rs:56-65`)
- Modify: `src/wal.rs` (`WalSinkKind` enum ~`src/wal.rs:490-512`, `with_sink_kind` ~`src/wal.rs:1056-1075`)
- Modify: `src/store.rs` (`match wal_write` ~`src/store.rs:340-343`, and the recover-mode selection — see Task 6)
- Test: `tests/` integration (new file) — see Step 1

**Interfaces:**
- Consumes: `PreallocFileSink::open` (Task 3).
- Produces: `WalWrite::CoalescedPrealloc` (public), `WalSinkKind::CoalescedPrealloc` (pub(crate)), wired so a `Store` with `Persistence::Standalone { wal_write: WalWrite::CoalescedPrealloc, .. }` uses `PreallocFileSink`.

- [ ] **Step 1: Write the failing test**

Create `tests/wal_preallocation.rs`:

```rust
#![cfg(feature = "persistence")]
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct Row { v: u64 }
impl ultima_db::Record for Row {}

fn cfg(dir: &std::path::Path) -> StoreConfig {
    StoreConfig {
        persistence: Persistence::Standalone {
            dir: dir.to_path_buf(),
            durability: Durability::Consistent,
            wal_write: WalWrite::CoalescedPrealloc,
        },
        ..StoreConfig::default()
    }
}

#[test]
fn prealloc_store_commits_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::new(cfg(dir.path())).unwrap();
        store.register_table::<Row>("rows").unwrap();
        let mut wtx = store.begin_write(None).unwrap();
        { let mut t = wtx.open_table::<Row>("rows").unwrap(); t.insert(Row { v: 42 }).unwrap(); }
        wtx.commit().unwrap();
    }
    // Reopen and recover.
    let store = Store::new(cfg(dir.path())).unwrap();
    store.register_table::<Row>("rows").unwrap();
    store.recover().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get(1).map(|r| r.v), Some(42));
}
```

(`Table::get(id)` is the primary-key getter — `src/table.rs:269`; `get_by_key` is for secondary indexes. `Record` is re-exported at `ultima_db::Record`. Mirror `tests/persistence_integration.rs` for the exact register/commit/recover/read idiom.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features persistence --test wal_preallocation -- --nocapture`
Expected: FAIL — `WalWrite::CoalescedPrealloc` does not exist (compile error).

- [ ] **Step 3: Add the config + sink variants and wiring**

In `src/persistence.rs`, extend `WalWrite`:

```rust
    /// Coalesced batch write into a **preallocated** WAL file: positioned
    /// writes overwrite a physically zero-filled region, so each Consistent
    /// commit's fsync carries no ext4 metadata commit. Steady-state uses
    /// `sync_data`; the file grows in 16 MiB chunks and is re-preallocated on
    /// prune. Opt-in; recovery uses a tail-tolerant scan. See
    /// docs/superpowers/specs/2026-06-20-wal-preallocation-design.md.
    CoalescedPrealloc,
```

In `src/wal.rs`, extend `WalSinkKind`:

```rust
    /// Preallocating coalesced sink (`PreallocFileSink`). Production opt-in.
    CoalescedPrealloc,
```

In `with_sink_kind` (`src/wal.rs`), add an arm before the `#[cfg]` arms:

```rust
            WalSinkKind::CoalescedPrealloc => {
                Ok(Self::with_sink(PreallocFileSink::open(dir)?, consistent, poison))
            }
```

In `src/store.rs` (`match wal_write`, ~line 340):

```rust
                    crate::persistence::WalWrite::CoalescedPrealloc => {
                        crate::wal::WalSinkKind::CoalescedPrealloc
                    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --features persistence --test wal_preallocation::prealloc_store_commits_and_recovers -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/persistence.rs src/wal.rs src/store.rs tests/wal_preallocation.rs
git commit -m "feat(wal): wire WalWrite::CoalescedPrealloc through config to PreallocFileSink"
```

---

### Task 6: Tail-tolerant recovery selection in `Store::recover`

`read_wal` is used by `Store::recover()` to replay entries (`src/store.rs:725-728`). For the prealloc path that replay must use the tolerant scan, otherwise a torn tail aborts recovery. Route recovery through `scan_wal(path, tolerant)` where `tolerant = matches!(wal_write, WalWrite::CoalescedPrealloc)`.

**Files:**
- Modify: `src/store.rs` (the WAL replay block, ~`src/store.rs:725-735`)
- Test: `tests/wal_preallocation.rs`

**Interfaces:**
- Consumes: `scan_wal` (Task 1), `WalWrite::CoalescedPrealloc` (Task 5).
- Produces: recovery that tolerates a torn preallocated tail.

- [ ] **Step 1: Write the failing test**

Add to `tests/wal_preallocation.rs`:

```rust
#[test]
fn prealloc_store_recovers_through_torn_preallocated_tail() {
    use std::io::{Seek, SeekFrom, Write};
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::new(cfg(dir.path())).unwrap();
        store.register_table::<Row>("rows").unwrap();
        let mut wtx = store.begin_write(None).unwrap();
        { let mut t = wtx.open_table::<Row>("rows").unwrap(); t.insert(Row { v: 7 }).unwrap(); }
        wtx.commit().unwrap();
    }
    // Append a garbage "record" into the preallocated zero tail to simulate a
    // torn write that looks complete (non-zero len prefix + junk + zeros).
    let wal = dir.path().join("wal.bin");
    let durable = ultima_db::wal_durable_len_for_test(&wal); // helper exposed below
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&wal).unwrap();
    f.seek(SeekFrom::Start(durable)).unwrap();
    f.write_all(&[16u8, 0, 0, 0]).unwrap(); // len=16
    f.write_all(&[0xABu8; 16]).unwrap();     // junk body (CRC will mismatch)
    f.write_all(&[0u8, 0, 0, 0]).unwrap();   // junk crc
    f.sync_all().unwrap();
    drop(f);

    let store = Store::new(cfg(dir.path())).unwrap();
    store.register_table::<Row>("rows").unwrap();
    store.recover().unwrap(); // must NOT error on the torn tail
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<Row>("rows").unwrap();
    assert_eq!(t.get(1).map(|r| r.v), Some(7));
}
```

Expose a tiny test helper so the test can find the durable offset without reimplementing the scan. In `src/lib.rs` (or wherever `wal` items are re-exported), add behind `#[doc(hidden)]`:

```rust
#[cfg(feature = "persistence")]
#[doc(hidden)]
pub fn wal_durable_len_for_test(path: &std::path::Path) -> u64 {
    crate::wal::scan_wal(path, true).unwrap().1
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --features persistence --test wal_preallocation::prealloc_store_recovers_through_torn_preallocated_tail -- --nocapture`
Expected: FAIL — either the helper is missing (compile error) or `recover()` returns `WalCorrupted`.

- [ ] **Step 3: Route recovery through the tolerant scan**

Read the existing replay block at `src/store.rs:725-731` first to match its exact structure. It currently reads `let wal_path_buf = crate::wal::wal_path(&dir); let entries = crate::wal::read_wal(&wal_path_buf)?;` inside `if matches!(inner.config.persistence, Persistence::Standalone { .. })`. Replace the `read_wal` call with a tolerant-aware scan (compute `tolerant` while `inner` is still borrowed, before the existing `drop(inner)`):

```rust
            let tolerant = matches!(
                inner.config.persistence,
                Persistence::Standalone {
                    wal_write: crate::persistence::WalWrite::CoalescedPrealloc, ..
                }
            );
            let entries = crate::wal::scan_wal(&wal_path_buf, tolerant)?.0;
            // ... the rest of the block (base_version, drop(inner), to_replay, replay) is unchanged ...
```

Add the `wal_durable_len_for_test` helper from Step 1.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --features persistence --test wal_preallocation -- --nocapture`
Expected: PASS (both prealloc integration tests).

- [ ] **Step 5: Commit**

```bash
git add src/store.rs src/lib.rs tests/wal_preallocation.rs
git commit -m "feat(wal): tail-tolerant recovery for the preallocated WAL path"
```

---

### Task 7: Durability-equivalence + checkpoint/prune integration tests

Prove preallocation does not weaken the Consistent durability contract and that it composes with `Store::checkpoint()` (which prunes the WAL).

**Files:**
- Test: `tests/wal_preallocation.rs`

**Interfaces:**
- Consumes: the wired prealloc path (Tasks 5-6), `Store::checkpoint()`.

- [ ] **Step 1: Write the failing/ì regression tests**

```rust
#[test]
fn prealloc_every_acked_commit_survives_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::new(cfg(dir.path())).unwrap();
        store.register_table::<Row>("rows").unwrap();
        for v in 1..=50u64 {
            let mut wtx = store.begin_write(None).unwrap();
            { let mut t = wtx.open_table::<Row>("rows").unwrap(); t.insert(Row { v }).unwrap(); }
            wtx.commit().unwrap(); // Consistent: returns only after durable
        }
    } // drop without clean shutdown
    let store = Store::new(cfg(dir.path())).unwrap();
    store.register_table::<Row>("rows").unwrap();
    store.recover().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<Row>("rows").unwrap();
    for v in 1..=50u64 {
        assert_eq!(t.get(v).map(|r| r.v), Some(v), "acked commit {v} lost");
    }
}

#[test]
fn prealloc_checkpoint_prunes_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::new(cfg(dir.path())).unwrap();
        store.register_table::<Row>("rows").unwrap();
        for v in 1..=10u64 {
            let mut wtx = store.begin_write(None).unwrap();
            { let mut t = wtx.open_table::<Row>("rows").unwrap(); t.insert(Row { v }).unwrap(); }
            wtx.commit().unwrap();
        }
        store.checkpoint().unwrap(); // writes checkpoint + prunes the preallocated WAL
        // More commits after the checkpoint/prune.
        for v in 11..=15u64 {
            let mut wtx = store.begin_write(None).unwrap();
            { let mut t = wtx.open_table::<Row>("rows").unwrap(); t.insert(Row { v }).unwrap(); }
            wtx.commit().unwrap();
        }
    }
    let store = Store::new(cfg(dir.path())).unwrap();
    store.register_table::<Row>("rows").unwrap();
    store.recover().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<Row>("rows").unwrap();
    for v in 1..=15u64 {
        assert_eq!(t.get(v).map(|r| r.v), Some(v), "row {v} missing after checkpoint+prune");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail (or pass) and investigate**

Run: `cargo test --features persistence --test wal_preallocation -- --nocapture`
Expected: These exercise already-implemented behavior; if any fail, the failure is a real bug in Tasks 3-6 — fix there (do not weaken the test). Verify both PASS before committing.

- [ ] **Step 3: (only if a real bug surfaces) fix the implicated sink/prune/recover code**

Apply the minimal fix in `src/wal.rs` / `src/store.rs` indicated by the failure; re-run.

- [ ] **Step 4: Confirm green + clippy**

Run: `cargo test --features persistence --test wal_preallocation -- --nocapture && cargo clippy --features persistence -- -D warnings`
Expected: PASS, zero warnings.

- [ ] **Step 5: Commit**

```bash
git add tests/wal_preallocation.rs
git commit -m "test(wal): durability-equivalence + checkpoint/prune coverage for preallocated WAL"
```

---

### Task 8: A/B bench arm + canonical task doc

Add a Coalesced-vs-CoalescedPrealloc comparison to an existing persistence bench so the win can be measured on real disk, and consolidate the design into the canonical per-feature doc.

**Files:**
- Modify: `benches/singlewriter_persistence_bench.rs` (add a `CoalescedPrealloc` Consistent arm next to the existing `Consistent` arm at ~`benches/singlewriter_persistence_bench.rs:154`)
- Create: `docs/tasks/task37_wal_preallocation.md` (verify the next free task number with `ls docs/tasks/`)

**Interfaces:**
- Consumes: `WalWrite::CoalescedPrealloc`.

- [ ] **Step 1: Add the bench arm**

Read the existing `Consistent` group setup in `benches/singlewriter_persistence_bench.rs` (~line 150-175) and add a sibling benchmark that builds the store with `wal_write: WalWrite::CoalescedPrealloc` and the same `Durability::Consistent`, identical workload. Name the criterion id `consistent_coalesced_prealloc` alongside the existing `consistent` id so they are directly comparable.

```rust
// In the Consistent persistence group, mirror the existing arm with prealloc:
group.bench_function("consistent_coalesced_prealloc", |b| {
    b.iter_batched(
        || make_store(/* dir */, Durability::Consistent, WalWrite::CoalescedPrealloc),
        |store| { /* identical commit workload as the `consistent` arm */ black_box(run_workload(&store)); },
        criterion::BatchSize::SmallInput,
    );
});
```

(Match the exact helper names and workload closure used by the existing arm in that file — reuse them, do not duplicate the workload body.)

- [ ] **Step 2: Verify the bench compiles**

Run: `cargo bench --bench singlewriter_persistence_bench --features persistence --no-run`
Expected: compiles.

- [ ] **Step 3: Write the canonical task doc**

Create `docs/tasks/task37_wal_preallocation.md` summarizing: motivation, the copy-not-share decision, `PreallocFileSink` design, the three invariants, strategy A (inline) + P2 (pre-sized tmp), the tail-tolerant recovery change and its trade-off, the `WalWrite::CoalescedPrealloc` config, and the pending real-disk A/B (cross-reference the spec at `docs/superpowers/specs/2026-06-20-wal-preallocation-design.md` and the journal counterpart task36). Note the A/B must be run on the bench host per the project's bench-A/B methodology (sandbox noise floors reach ±2×), and that the feature stays opt-in/off until validated.

- [ ] **Step 4: Full verification sweep**

Run: `cargo test --features persistence && cargo clippy --features persistence -- -D warnings`
Expected: all tests pass, zero clippy warnings.

- [ ] **Step 5: Commit**

```bash
git add benches/singlewriter_persistence_bench.rs docs/tasks/task37_wal_preallocation.md
git commit -m "bench+docs(wal): Coalesced-vs-Prealloc A/B arm + task37 canonical doc"
```

---

## Self-Review

**Spec coverage:**
- §1 copy-not-share → Task 2 (the only copied code) + design rationale carried into Task 8 doc. ✓
- §2 components / `PreallocFileSink` → Task 3. ✓
- §3 invariants → asserted in Task 3 tests (`prealloc_sink_extends_in_chunks_and_holds_invariant`). ✓
- §4 lifecycle/sawtooth → Task 3 (steady-state no-growth test) + Task 4 (prune shrink). ✓
- §5 strategy A inline grow-ahead → Task 3 `sync`. ✓
- §6 strategy P2 pre-sized tmp prune → Task 4. ✓
- §7 tail-tolerant recovery → Task 1 (`scan_wal`) + Task 6 (`Store::recover` routing) + torn-tail tests. ✓
- §8 sync_data steady / sync_all on extend → Task 3 `sync` (encoded in barrier discipline) + global constraints. ✓
- §9 `WalWrite::CoalescedPrealloc` config → Task 5. ✓
- §10 out of scope → no tasks touch journal / shared crate / other sinks. ✓
- §11 testing → Tasks 3,4,5,6,7. ✓
- §12 validation A/B → Task 8 bench arm + doc note. ✓
- §13 risks → covered by invariant assertions (Task 3) and tolerant-scan tests (Task 1,6). ✓

**Placeholder scan:** No TBD/TODO. Two tasks (5 Step 1, 8 Step 1) instruct mirroring an existing idiom rather than inlining unknown API details — both name the exact file/anchor to copy from, which is the correct DRY instruction in an existing codebase rather than a placeholder.

**Type consistency:** `scan_wal(path, bool) -> Result<(Vec<WalEntry>, u64)>` used identically in Tasks 1, 3 (`open`), 6, helper. `PreallocFileSink` fields (`write_head`, `capacity`, `chunk`, `buf`, `file`, `path`) consistent across Tasks 3-4. `prune_wal_prealloc(path, u64, u64) -> Result<Option<(u64,u64)>>` defined Task 4, consumed by the sink's `prune` in the same task. `WalWrite::CoalescedPrealloc` / `WalSinkKind::CoalescedPrealloc` consistent Tasks 5-6. ✓
