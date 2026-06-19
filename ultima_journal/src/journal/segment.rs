// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use crate::JournalError;

pub const SEGMENT_MAGIC: &[u8; 8] = b"ULTJSEG\0";
pub const SEGMENT_FORMAT_V: u16 = 1;
pub const SEGMENT_HEADER_SIZE: usize = 32;

/// Sentinel record constants — written briefly during `truncate_after` to mark
/// intent on disk.  If a crash occurs after the sentinel fsync but before the
/// final truncation, Task 14 recovery detects the sentinel and re-truncates.
pub const SENTINEL_META: u64 = u64::MAX;
pub const SENTINEL_PAYLOAD: &[u8] = b"ULTJTRUNC";

/// Returns `true` if `rec` is a truncation sentinel written by `truncate_after`.
pub fn is_sentinel(rec: &DecodedRecord) -> bool {
    rec.meta == SENTINEL_META && rec.payload == SENTINEL_PAYLOAD
}

/// Segment header — fixed 32 bytes at the start of each segment file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub format_ver: u16,
    pub base_seq: u64,
    pub created_at: u64,
}

/// A decoded record (excluding length prefix and trailing CRC).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRecord {
    pub seq: u64,
    pub meta: u64,
    pub payload: Vec<u8>,
}

pub fn encode_header(h: &SegmentHeader) -> [u8; SEGMENT_HEADER_SIZE] {
    let mut buf = [0u8; SEGMENT_HEADER_SIZE];
    buf[0..8].copy_from_slice(SEGMENT_MAGIC);
    buf[8..10].copy_from_slice(&h.format_ver.to_le_bytes());
    buf[10..18].copy_from_slice(&h.base_seq.to_le_bytes());
    buf[18..26].copy_from_slice(&h.created_at.to_le_bytes());
    let crc = crc32fast::hash(&buf[0..26]);
    buf[26..30].copy_from_slice(&crc.to_le_bytes());
    // bytes 30..32 are reserved padding (zeroed).
    buf
}

pub fn decode_header(bytes: &[u8]) -> Result<SegmentHeader, JournalError> {
    if bytes.len() < SEGMENT_HEADER_SIZE {
        return Err(JournalError::Corrupted {
            segment: String::new(),
            offset: 0,
            reason: format!("header too short: {}", bytes.len()),
        });
    }
    if &bytes[0..8] != SEGMENT_MAGIC {
        return Err(JournalError::Corrupted {
            segment: String::new(),
            offset: 0,
            reason: "bad magic".into(),
        });
    }
    let format_ver = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    let base_seq = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
    let created_at = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
    let stored_crc = u32::from_le_bytes(bytes[26..30].try_into().unwrap());
    let actual_crc = crc32fast::hash(&bytes[0..26]);
    if stored_crc != actual_crc {
        return Err(JournalError::Corrupted {
            segment: String::new(),
            offset: 0,
            reason: "header crc mismatch".into(),
        });
    }
    Ok(SegmentHeader {
        format_ver,
        base_seq,
        created_at,
    })
}

/// Append one framed record (length prefix + seq + meta + payload + crc) to `buf`.
/// Single source of truth for the on-disk record framing.
///
/// Layout:
///   [len: u32 (= 16 + payload.len() — covers seq + meta + payload, NOT crc)]
///   [seq: u64]
///   [meta: u64]
///   [payload bytes]
///   [crc32 over seq + meta + payload]
fn encode_record_into(buf: &mut Vec<u8>, seq: u64, meta: u64, payload: &[u8]) {
    let body_len = 16usize + payload.len();
    let start = buf.len();
    buf.extend_from_slice(&(body_len as u32).to_le_bytes());
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&meta.to_le_bytes());
    buf.extend_from_slice(payload);
    let crc = crc32fast::hash(&buf[start + 4..start + 4 + body_len]);
    buf.extend_from_slice(&crc.to_le_bytes());
}

/// Returns the encoded record bytes (length-prefix + body + CRC).
#[allow(dead_code)] // Public utility; used in tests and available for callers outside this crate.
pub fn encode_record(seq: u64, meta: u64, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + 16 + payload.len() + 4);
    encode_record_into(&mut buf, seq, meta, payload);
    buf
}

/// Decode one record starting at `bytes[0]`. Returns `(record, bytes_consumed)`.
/// Returns `Ok(None)` for a torn tail (last segment) — bytes too short or
/// header truncated. Returns `Err` for confirmed corruption (bad CRC on a
/// fully-present record, malformed body length).
pub fn decode_record(
    bytes: &[u8],
    segment_name: &str,
    offset: u64,
) -> Result<Option<(DecodedRecord, usize)>, JournalError> {
    if bytes.len() < 4 {
        return Ok(None); // torn tail (length prefix incomplete)
    }
    let body_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let total = 4 + body_len + 4;

    if body_len == 0 {
        // A zero length-prefix marks an unwritten region. Two callers surface
        // here: the lock-free live reader (length is written atomically AFTER
        // the body, so 0 = record not yet committed) and a preallocated
        // segment's zero tail. Both mean "end of records" — stop cleanly
        // rather than treating the zeros as a corrupt record.
        return Ok(None);
    }
    // If body_len is invalid and we don't have enough bytes to confirm corruption,
    // treat it as a torn tail (incomplete/garbage record).
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
    if bytes.len() < total {
        return Ok(None); // torn tail (record incomplete)
    }
    let body = &bytes[4..4 + body_len];
    let stored_crc = u32::from_le_bytes(bytes[4 + body_len..total].try_into().unwrap());
    let actual_crc = crc32fast::hash(body);
    if stored_crc != actual_crc {
        return Err(JournalError::Corrupted {
            segment: segment_name.into(),
            offset,
            reason: "record crc mismatch".into(),
        });
    }
    let seq = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let meta = u64::from_le_bytes(body[8..16].try_into().unwrap());
    let payload = body[16..].to_vec();
    Ok(Some((DecodedRecord { seq, meta, payload }, total)))
}

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// One sparse-index entry every ~64 KiB of segment data. Trades a few KB of
/// memory per segment for O(log N) seek to an offset near a target seq.
const SPARSE_INDEX_GAP: u64 = 64 * 1024;

#[derive(Debug)]
pub struct ScanResult {
    pub records: Vec<DecodedRecord>,
    /// Byte offset just past the last good record.
    pub last_durable_offset: u64,
    /// True if there were trailing bytes that didn't form a valid record.
    pub had_torn_tail: bool,
    /// Sparse seq → byte offset index built during the scan. Installed into the
    /// `SegmentFile` at `Journal::open` and used by `read_record`.
    pub index: Vec<(u64, u64)>,
}

/// On-disk segment file. Owned by the journal's bg writer when active; readers
/// reopen via `open_for_read` and use `scan` / direct file reads.
pub struct SegmentFile {
    path: PathBuf,
    file: File,
    /// Current end-of-file (= last durable offset for the writer's view).
    size: u64,
    base_seq: u64,
    /// Sparse seq → byte offset index. Maintained on append, installed at open,
    /// and binary-searched by `read_record` to bound point-read I/O to one window.
    index: Vec<(u64, u64)>,
}

impl SegmentFile {
    pub fn create(path: &Path, base_seq: u64) -> Result<Self, JournalError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let header = SegmentHeader {
            format_ver: SEGMENT_FORMAT_V,
            base_seq,
            created_at: now_nanos(),
        };
        let bytes = encode_header(&header);
        file.write_all(&bytes)?;
        file.sync_all()?;
        // Make the directory entry durable too: without this, a power loss
        // can vanish the file itself — taking every record in it, including
        // ones whose fsync already succeeded and was acknowledged.
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(Self {
            path: path.to_path_buf(),
            file,
            size: SEGMENT_HEADER_SIZE as u64,
            base_seq,
            index: Vec::new(),
        })
    }

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

    pub fn open_for_read(path: &Path) -> Result<Self, JournalError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut hdr_bytes = [0u8; SEGMENT_HEADER_SIZE];
        file.read_exact(&mut hdr_bytes)
            .map_err(|e| JournalError::Corrupted {
                segment: path.display().to_string(),
                offset: 0,
                reason: format!("header read failed: {e}"),
            })?;
        let header = decode_header(&hdr_bytes)?;
        if header.format_ver != SEGMENT_FORMAT_V {
            return Err(JournalError::Corrupted {
                segment: path.display().to_string(),
                offset: 0,
                reason: format!(
                    "unsupported segment format version {} (this build expects {})",
                    header.format_ver, SEGMENT_FORMAT_V,
                ),
            });
        }
        let size = file.metadata()?.len();
        Ok(Self {
            path: path.to_path_buf(),
            file,
            size,
            base_seq: header.base_seq,
            index: Vec::new(),
        })
    }

    pub fn base_seq(&self) -> u64 {
        self.base_seq
    }
    pub fn size(&self) -> Result<u64, JournalError> {
        Ok(self.size)
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    #[allow(dead_code)] // Inspection helper; used in tests.
    pub fn index_snapshot(&self) -> &[(u64, u64)] {
        &self.index
    }

    /// Install a precomputed sparse index (built by `scan` at open time) so the
    /// read path can use windowed reads without re-scanning. See `read_record`.
    pub fn set_index(&mut self, index: Vec<(u64, u64)>) {
        debug_assert!(
            index.windows(2).all(|w| w[0].0 < w[1].0),
            "sparse index must be strictly ascending by seq"
        );
        self.index = index;
    }

    /// Append one record at end-of-file. Thin wrapper over [`append_records`].
    /// Returns the byte offset at which the record was written. Test-only:
    /// production code appends in batches via [`append_records`].
    #[cfg(test)]
    pub fn append_record(
        &mut self,
        seq: u64,
        meta: u64,
        payload: &[u8],
    ) -> Result<u64, JournalError> {
        let offset = self.size;
        self.append_records(&[(seq, meta, payload)])?;
        Ok(offset)
    }

    /// Append many records with a single `seek` + `write_all`. Caller must
    /// enforce monotonic seq. Coalesces all records into one buffer, writes
    /// once, then advances `size` and maintains the sparse index per record
    /// using the same `SPARSE_INDEX_GAP` rule as a per-record append. Returns
    /// the byte offset of the first record written.
    pub fn append_records(
        &mut self,
        records: &[(u64, u64, &[u8])],
    ) -> Result<u64, JournalError> {
        let start_offset = self.size;
        if records.is_empty() {
            return Ok(start_offset);
        }
        // Coalesce: encode every record directly into one pre-sized buffer,
        // tracking each record's absolute byte offset for the sparse index.
        let total_bytes: usize = records.iter().map(|(_, _, p)| 4 + 16 + p.len() + 4).sum();
        let mut buf = Vec::with_capacity(total_bytes);
        let mut offsets: Vec<(u64, u64)> = Vec::with_capacity(records.len());
        let mut running = self.size;
        for (seq, meta, payload) in records {
            offsets.push((*seq, running));
            running += (4 + 16 + payload.len() + 4) as u64;
            encode_record_into(&mut buf, *seq, *meta, payload);
        }
        // One seek + one write_all for the whole run. `self.size` is advanced only
        // after write_all returns; a partial write on failure is fatal to the bg
        // writer and is reconciled by recovery's torn-tail scan on the next open.
        self.file.seek(SeekFrom::Start(self.size))?;
        self.file.write_all(&buf)?;
        self.size += buf.len() as u64;
        // Maintain the sparse index per record, identical gap rule.
        for (seq, off) in offsets {
            let want_index = self
                .index
                .last()
                .is_none_or(|(_, prev)| off.saturating_sub(*prev) >= SPARSE_INDEX_GAP);
            if want_index {
                self.index.push((seq, off));
            }
        }
        Ok(start_offset)
    }

    #[allow(dead_code)] // Used by tests; the writer path now fsyncs a dup'd fd via `fsync_handle`.
    pub fn fsync(&mut self) -> Result<(), JournalError> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Return a dup'd file descriptor for the active segment so the caller can
    /// `sync_data()` it *without* holding the writer's state lock. The dup shares
    /// the same open file description (and thus the same in-kernel dirty pages),
    /// so fsyncing the clone is an identical durability barrier over the bytes
    /// written so far. Mirrors the `try_clone()` already used by `scan` /
    /// `read_span` for the read paths.
    pub(crate) fn fsync_handle(&self) -> Result<File, JournalError> {
        Ok(self.file.try_clone()?)
    }

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

    /// Bench-only: issue the `sync_data` durability barrier directly on the
    /// segment file. This is the exact commit primitive the writer pipeline
    /// runs per group-commit (`writer::fsync_active_segment`), minus the fd
    /// dup that only exists there to release the state lock across the
    /// syscall. Isolating it lets the journal microbench measure the pure
    /// fsync cost (`fsync_only_*`) separately from the page-cache write
    /// (`append_records` → `write_only_*`).
    #[cfg(feature = "bench-support")]
    pub(crate) fn sync_data_for_bench(&self) -> Result<(), JournalError> {
        self.file.sync_data().map_err(JournalError::Io)
    }

    /// Bench-only: physically zero-fill the segment out to `total_len` bytes
    /// and `sync_all` once, *without* advancing the logical write cursor
    /// (`size`). This forces the filesystem to allocate the blocks and mark
    /// the extents *written* up front — a real write, NOT a sparse
    /// `set_len`/`fallocate`, which on ext4 leaves *unwritten* extents that
    /// convert (and re-journal a metadata commit) on the first overwrite.
    /// Subsequent `append_records` then overwrite already-written blocks at the
    /// logical cursor, so the per-commit `sync_data` carries no `i_size` or
    /// extent-map change and degenerates to a pure data barrier. Mirrors the
    /// etcd WAL preallocation trick; used by the microbench's `fsync_prealloc_*`
    /// variant to isolate the ext4 journal-commit cost of a size-extending
    /// append from the raw device flush.
    #[cfg(feature = "bench-support")]
    pub(crate) fn preallocate_zerofill_for_bench(
        &mut self,
        total_len: u64,
    ) -> Result<(), JournalError> {
        self.preallocate_to(total_len)
    }

    /// Read the entire body (after header) and decode all records.
    /// Tolerant of torn tail. Builds a fresh sparse index.
    pub fn scan(&self) -> Result<ScanResult, JournalError> {
        let mut f = self.file.try_clone()?;
        f.seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let segname = self.segname();
        let mut records = Vec::new();
        let mut index: Vec<(u64, u64)> = Vec::new();
        let mut cursor = 0usize;
        let mut last_index_offset: u64 = 0;
        loop {
            let abs_offset = SEGMENT_HEADER_SIZE as u64 + cursor as u64;
            match decode_record(&buf[cursor..], &segname, abs_offset)? {
                Some((rec, n)) => {
                    if index.is_empty()
                        || abs_offset.saturating_sub(last_index_offset) >= SPARSE_INDEX_GAP
                    {
                        index.push((rec.seq, abs_offset));
                        last_index_offset = abs_offset;
                    }
                    records.push(rec);
                    cursor += n;
                }
                None => {
                    let had_torn_tail = cursor < buf.len();
                    return Ok(ScanResult {
                        records,
                        last_durable_offset: SEGMENT_HEADER_SIZE as u64 + cursor as u64,
                        had_torn_tail,
                        index,
                    });
                }
            }
        }
    }

    /// The segment's file name as a `String`, for error/corruption context.
    fn segname(&self) -> String {
        self.path.file_name().unwrap().to_string_lossy().to_string()
    }

    /// Read the raw bytes in `[start, end)` with one portable `try_clone()` +
    /// seek + `read_exact`. Offsets come from the sparse index; the caller
    /// guarantees `start <= end <= self.size`.
    fn read_span(&self, start: u64, end: u64) -> Result<Vec<u8>, JournalError> {
        let mut f = self.file.try_clone()?;
        f.seek(SeekFrom::Start(start))?;
        let mut buf = vec![0u8; (end - start) as usize];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read a single record by `seq`, using the sparse index to bound I/O to one
    /// ~64 KiB window instead of scanning the whole segment. Returns `Ok(None)`
    /// if `seq` is absent (a gap, or out of this segment's range). Falls back to
    /// a full `scan` when the index has not been populated.
    ///
    /// Only records within the read window are CRC-verified; a point read does
    /// not validate the whole segment (use `scan` for that).
    pub fn read_record(&self, seq: u64) -> Result<Option<(u64, Vec<u8>)>, JournalError> {
        // Defensive fallback: index not yet populated (e.g. opened, not installed).
        if self.index.is_empty() {
            let scan = self.scan()?;
            for r in scan.records {
                if r.seq == seq {
                    return Ok(Some((r.meta, r.payload)));
                }
                if r.seq > seq {
                    return Ok(None);
                }
            }
            return Ok(None);
        }

        // Largest index entry whose seq is <= the target. Because we pick the
        // largest seq_i <= seq, the next entry's seq exceeds seq, so the target
        // (if present) lies strictly within [start, end).
        let n = self.index.partition_point(|&(s, _)| s <= seq);
        if n == 0 {
            return Ok(None); // below this segment's first record
        }
        let start = self.index[n - 1].1;
        let end = self.index.get(n).map(|&(_, o)| o).unwrap_or(self.size);

        // Read just the bounded window [start, end).
        let buf = self.read_span(start, end)?;

        // Decode forward to the target seq.
        let segname = self.segname();
        let mut cursor = 0usize;
        while cursor < buf.len() {
            let abs_offset = start + cursor as u64;
            match decode_record(&buf[cursor..], &segname, abs_offset)? {
                Some((rec, consumed)) => {
                    if rec.seq == seq {
                        return Ok(Some((rec.meta, rec.payload)));
                    }
                    if rec.seq > seq {
                        return Ok(None);
                    }
                    cursor += consumed;
                }
                None => break,
            }
        }
        Ok(None)
    }

    /// Return every record with `lo <= seq <= hi` in this segment, reading only
    /// the index-bounded byte span covering `[lo, hi]` rather than the whole
    /// segment. Falls back to a full `scan` when the index is empty.
    ///
    /// Only records in the read span are CRC-verified; a partial range read does
    /// not validate the whole segment (use `scan` for that).
    pub fn read_window(
        &self,
        lo: u64,
        hi: u64,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, JournalError> {
        // Defensive fallback: index not yet populated.
        if self.index.is_empty() {
            let scan = self.scan()?;
            let mut out = Vec::new();
            for r in scan.records {
                if r.seq > hi {
                    break;
                }
                if r.seq >= lo {
                    out.push((r.seq, r.meta, r.payload));
                }
            }
            return Ok(out);
        }

        // Byte span covering [lo, hi]: start at the window holding lo (or the
        // first record if lo is below the segment); end at the offset of the
        // first record with seq > hi (or EOF).
        let s = self.index.partition_point(|&(sq, _)| sq <= lo);
        let start = if s == 0 { self.index[0].1 } else { self.index[s - 1].1 };
        let e = self.index.partition_point(|&(sq, _)| sq <= hi);
        let end = if e < self.index.len() {
            self.index[e].1
        } else {
            self.size
        };
        if start >= end {
            return Ok(Vec::new());
        }

        let buf = self.read_span(start, end)?;
        let segname = self.segname();
        let mut out = Vec::new();
        let mut cursor = 0usize;
        while cursor < buf.len() {
            let abs_offset = start + cursor as u64;
            match decode_record(&buf[cursor..], &segname, abs_offset)? {
                Some((rec, consumed)) => {
                    if rec.seq > hi {
                        break;
                    }
                    if rec.seq >= lo {
                        out.push((rec.seq, rec.meta, rec.payload));
                    }
                    cursor += consumed;
                }
                None => break,
            }
        }
        Ok(out)
    }

    /// Truncate the file to `len` bytes and fsync. Drops in-memory index
    /// entries whose offset is past the new size.
    pub fn truncate(&mut self, len: u64) -> Result<(), JournalError> {
        self.file.set_len(len)?;
        self.file.sync_all()?;
        self.size = len;
        self.index.retain(|(_, off)| *off < len);
        Ok(())
    }
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let h = SegmentHeader {
            format_ver: 1,
            base_seq: 42,
            created_at: 100,
        };
        let bytes = encode_header(&h);
        assert_eq!(bytes.len(), SEGMENT_HEADER_SIZE);
        let decoded = decode_header(&bytes).unwrap();
        assert_eq!(decoded, h);
    }

    #[test]
    fn header_rejects_bad_magic() {
        let mut bytes = encode_header(&SegmentHeader {
            format_ver: 1,
            base_seq: 0,
            created_at: 0,
        });
        bytes[0] = b'X';
        assert!(matches!(
            decode_header(&bytes),
            Err(JournalError::Corrupted { .. })
        ));
    }

    #[test]
    fn record_roundtrip() {
        let payload = b"hello world".to_vec();
        let bytes = encode_record(7, 0xAABB, &payload);
        let (r, n) = decode_record(&bytes, "seg", 0).unwrap().unwrap();
        assert_eq!(r.seq, 7);
        assert_eq!(r.meta, 0xAABB);
        assert_eq!(r.payload, payload);
        assert_eq!(n, bytes.len());
    }

    #[test]
    fn record_torn_tail_returns_none() {
        let bytes = encode_record(1, 0, b"abc");
        let truncated = &bytes[..bytes.len() - 1];
        assert_eq!(decode_record(truncated, "seg", 0).unwrap(), None);
    }

    #[test]
    fn record_bad_crc_errors() {
        let mut bytes = encode_record(1, 0, b"abc");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(matches!(
            decode_record(&bytes, "seg", 0),
            Err(JournalError::Corrupted { .. })
        ));
    }

    #[test]
    fn segment_open_create_writes_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000010.log");
        let _seg = SegmentFile::create(&path, 10).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), SEGMENT_HEADER_SIZE);
        let h = decode_header(&bytes).unwrap();
        assert_eq!(h.base_seq, 10);
    }

    #[test]
    fn segment_append_and_scan_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000001.log");
        let mut seg = SegmentFile::create(&path, 1).unwrap();
        seg.append_record(1, 100, b"a").unwrap();
        seg.append_record(2, 200, b"bb").unwrap();
        seg.append_record(3, 300, b"ccc").unwrap();
        seg.fsync().unwrap();

        let opened = SegmentFile::open_for_read(&path).unwrap();
        let scan = opened.scan().unwrap();
        assert_eq!(scan.records.len(), 3);
        assert_eq!(scan.records[0].seq, 1);
        assert_eq!(scan.records[2].payload, b"ccc");
        assert_eq!(scan.last_durable_offset, opened.size().unwrap());
        assert!(!scan.had_torn_tail);
        assert!(!scan.index.is_empty());
    }

    #[test]
    fn segment_scan_handles_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000001.log");
        {
            let mut seg = SegmentFile::create(&path, 1).unwrap();
            seg.append_record(1, 0, b"good").unwrap();
            seg.fsync().unwrap();
        }
        // Append garbage (simulated torn write).
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&[0u8; 5]).unwrap();
        }
        let opened = SegmentFile::open_for_read(&path).unwrap();
        let scan = opened.scan().unwrap();
        assert_eq!(scan.records.len(), 1);
        assert!(scan.had_torn_tail);
    }

    #[test]
    fn header_too_short_errors() {
        let bytes = vec![0u8; 10];
        assert!(matches!(
            decode_header(&bytes),
            Err(JournalError::Corrupted { .. })
        ));
    }

    #[test]
    fn header_crc_mismatch_errors() {
        let mut bytes = encode_header(&SegmentHeader {
            format_ver: 1,
            base_seq: 7,
            created_at: 100,
        });
        // Flip a byte in the body so CRC over 0..26 no longer matches.
        bytes[10] ^= 0xFF;
        assert!(matches!(
            decode_header(&bytes),
            Err(JournalError::Corrupted { reason, .. }) if reason.contains("crc")
        ));
    }

    #[test]
    fn record_body_len_too_small_errors() {
        // Length prefix says 5 bytes (< minimum 16). Pad with garbage so the
        // record looks "complete" — body_len < 16 must hit Err, not Ok(None).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&5u32.to_le_bytes()); // body_len = 5
        bytes.extend_from_slice(&[0u8; 5]); // body
        bytes.extend_from_slice(&[0u8; 4]); // crc placeholder
        assert!(matches!(
            decode_record(&bytes, "seg", 0),
            Err(JournalError::Corrupted { .. })
        ));
    }

    #[test]
    fn open_for_read_corrupt_header_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000001.log");
        std::fs::write(&path, b"GARBAGE!").unwrap();
        assert!(SegmentFile::open_for_read(&path).is_err());
    }

    #[test]
    fn segment_index_snapshot_returns_built_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000001.log");
        let mut seg = SegmentFile::create(&path, 1).unwrap();
        seg.append_record(1, 0, b"a").unwrap();
        // First record always indexed.
        assert!(!seg.index_snapshot().is_empty());
    }

    #[test]
    fn append_records_coalesced_matches_per_record() {
        let dir = tempfile::tempdir().unwrap();

        // Reference segment built one record at a time.
        let p1 = dir.path().join("seg-00000000000000000001.log");
        let mut a = SegmentFile::create(&p1, 1).unwrap();
        a.append_record(1, 10, b"alpha").unwrap();
        a.append_record(2, 20, b"beta").unwrap();
        a.append_record(3, 30, b"gamma").unwrap();
        a.fsync().unwrap();

        // Segment built with a single coalesced call.
        let p2 = dir.path().join("seg-00000000000000000010.log");
        let mut b = SegmentFile::create(&p2, 1).unwrap();
        b.append_records(&[(1, 10, b"alpha"), (2, 20, b"beta"), (3, 30, b"gamma")])
            .unwrap();
        b.fsync().unwrap();

        // Identical post-header bytes (headers differ only by created_at).
        let ba = std::fs::read(&p1).unwrap();
        let bb = std::fs::read(&p2).unwrap();
        assert_eq!(a.size().unwrap(), b.size().unwrap());
        assert_eq!(&ba[SEGMENT_HEADER_SIZE..], &bb[SEGMENT_HEADER_SIZE..]);

        // Identical decoded records.
        let sa = SegmentFile::open_for_read(&p1).unwrap().scan().unwrap();
        let sb = SegmentFile::open_for_read(&p2).unwrap().scan().unwrap();
        assert_eq!(sa.records, sb.records);
        assert_eq!(sb.records.len(), 3);
    }

    #[test]
    fn append_records_sparse_index_matches_per_record_across_gap() {
        let dir = tempfile::tempdir().unwrap();
        // ~20 KiB payloads → records cross the 64 KiB SPARSE_INDEX_GAP every few records.
        let payload = vec![0x5Au8; 20 * 1024];
        let recs: Vec<(u64, u64, &[u8])> =
            (1..=12u64).map(|i| (i, i * 2, payload.as_slice())).collect();

        let p1 = dir.path().join("seg-00000000000000000001.log");
        let mut a = SegmentFile::create(&p1, 1).unwrap();
        for (seq, meta, pl) in &recs {
            a.append_record(*seq, *meta, pl).unwrap();
        }

        let p2 = dir.path().join("seg-00000000000000000100.log");
        let mut b = SegmentFile::create(&p2, 1).unwrap();
        b.append_records(&recs).unwrap();

        // Same number of records, same size, and identical sparse index entries.
        assert_eq!(a.size().unwrap(), b.size().unwrap());
        assert_eq!(a.index_snapshot(), b.index_snapshot());
        // The gap was actually crossed (more than just the first record indexed).
        assert!(
            b.index_snapshot().len() > 1,
            "expected multiple sparse-index entries across the 64 KiB gap, got {}",
            b.index_snapshot().len()
        );
    }

    #[test]
    fn append_records_empty_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000001.log");
        let mut seg = SegmentFile::create(&path, 1).unwrap();
        let before = seg.size().unwrap();
        seg.append_records(&[]).unwrap();
        assert_eq!(seg.size().unwrap(), before);
    }

    #[test]
    fn read_record_matches_scan_across_windows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000001.log");
        let mut seg = SegmentFile::create(&path, 1).unwrap();
        // ~20 KiB payloads → cross the 64 KiB SPARSE_INDEX_GAP several times.
        let payload = vec![0x33u8; 20 * 1024];
        let recs: Vec<(u64, u64, &[u8])> =
            (1..=20u64).map(|i| (i, i * 3, payload.as_slice())).collect();
        seg.append_records(&recs).unwrap();
        seg.fsync().unwrap();

        // The append path populated more than one index window.
        assert!(seg.index_snapshot().len() > 1);

        // read_record matches a full scan for every seq, including boundaries.
        let scan = seg.scan().unwrap();
        for r in &scan.records {
            let got = seg.read_record(r.seq).unwrap();
            assert_eq!(got, Some((r.meta, r.payload.clone())));
        }
    }

    #[test]
    fn read_record_gap_and_out_of_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000001.log");
        let mut seg = SegmentFile::create(&path, 10).unwrap();
        // Strictly-increasing seqs with gaps: 10, 20, 30.
        seg.append_records(&[(10, 0, b"a"), (20, 0, b"b"), (30, 0, b"c")])
            .unwrap();
        seg.fsync().unwrap();

        assert_eq!(seg.read_record(10).unwrap(), Some((0u64, b"a".to_vec())));
        assert_eq!(seg.read_record(30).unwrap(), Some((0u64, b"c".to_vec())));
        assert_eq!(seg.read_record(15).unwrap(), None); // gap between records
        assert_eq!(seg.read_record(99).unwrap(), None); // above last
        assert_eq!(seg.read_record(5).unwrap(), None); // below first (partition_point == 0)
    }

    #[test]
    fn read_record_empty_index_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000001.log");
        {
            let mut seg = SegmentFile::create(&path, 1).unwrap();
            seg.append_records(&[(1, 100, b"x"), (2, 200, b"yy")]).unwrap();
            seg.fsync().unwrap();
        }
        // Reopen: open_for_read leaves the index empty (not yet populated).
        let seg = SegmentFile::open_for_read(&path).unwrap();
        assert!(seg.index_snapshot().is_empty());
        // read_record still returns correct results via the scan fallback.
        assert_eq!(seg.read_record(2).unwrap(), Some((200u64, b"yy".to_vec())));
        assert_eq!(seg.read_record(9).unwrap(), None);
    }

    #[test]
    fn read_window_matches_scan_filter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000001.log");
        let mut seg = SegmentFile::create(&path, 1).unwrap();
        // ~20 KiB payloads → multiple 64 KiB index windows.
        let payload = vec![0x44u8; 20 * 1024];
        let recs: Vec<(u64, u64, &[u8])> =
            (1..=20u64).map(|i| (i, i * 9, payload.as_slice())).collect();
        seg.append_records(&recs).unwrap();
        seg.fsync().unwrap();
        assert!(seg.index_snapshot().len() > 1);

        let all = seg.scan().unwrap().records;
        let expect = |lo: u64, hi: u64| -> Vec<(u64, u64, Vec<u8>)> {
            all.iter()
                .filter(|r| r.seq >= lo && r.seq <= hi)
                .map(|r| (r.seq, r.meta, r.payload.clone()))
                .collect()
        };
        for (lo, hi) in [(1u64, 20u64), (5, 9), (1, 1), (20, 20), (8, 15), (3, 3), (12, 100)] {
            assert_eq!(seg.read_window(lo, hi).unwrap(), expect(lo, hi), "[{lo},{hi}]");
        }
    }

    #[test]
    fn read_window_empty_and_inverted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000010.log");
        let mut seg = SegmentFile::create(&path, 10).unwrap();
        seg.append_records(&[(10, 0, b"a"), (20, 0, b"b"), (30, 0, b"c")])
            .unwrap();
        seg.fsync().unwrap();

        assert_eq!(seg.read_window(25, 15).unwrap(), Vec::new()); // inverted lo > hi
        assert_eq!(seg.read_window(1, 5).unwrap(), Vec::new()); // entirely below
        assert_eq!(seg.read_window(40, 50).unwrap(), Vec::new()); // entirely above
        assert_eq!(
            seg.read_window(20, 30).unwrap(),
            vec![(20, 0, b"b".to_vec()), (30, 0, b"c".to_vec())]
        );
    }

    #[test]
    fn read_window_empty_index_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-00000000000000000001.log");
        {
            let mut seg = SegmentFile::create(&path, 1).unwrap();
            seg.append_records(&[(1, 100, b"x"), (2, 200, b"yy"), (3, 300, b"zzz")])
                .unwrap();
            seg.fsync().unwrap();
        }
        let seg = SegmentFile::open_for_read(&path).unwrap();
        assert!(seg.index_snapshot().is_empty());
        assert_eq!(
            seg.read_window(2, 3).unwrap(),
            vec![(2, 200, b"yy".to_vec()), (3, 300, b"zzz".to_vec())]
        );
        assert_eq!(seg.read_window(5, 9).unwrap(), Vec::new());
    }

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
}
