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
    Ok(SegmentHeader { format_ver, base_seq, created_at })
}

/// Returns the encoded record bytes (length-prefix + body + CRC).
pub fn encode_record(seq: u64, meta: u64, payload: &[u8]) -> Vec<u8> {
    // Layout:
    //   [len: u32 (= 16 + payload.len() — covers seq + meta + payload, NOT crc)]
    //   [seq: u64]
    //   [meta: u64]
    //   [payload bytes]
    //   [crc32 over seq + meta + payload]
    let body_len = 16usize + payload.len();
    let total = 4 + body_len + 4;
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&(body_len as u32).to_le_bytes());
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&meta.to_le_bytes());
    buf.extend_from_slice(payload);
    let crc = crc32fast::hash(&buf[4..4 + body_len]);
    buf.extend_from_slice(&crc.to_le_bytes());
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
        return Ok(None);  // torn tail (length prefix incomplete)
    }
    let body_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let total = 4 + body_len + 4;

    // If body_len is invalid and we don't have enough bytes to confirm corruption,
    // treat it as a torn tail (incomplete/garbage record).
    if body_len < 16 {
        if bytes.len() < total {
            return Ok(None);  // torn tail (could be incomplete record with bad length)
        }
        return Err(JournalError::Corrupted {
            segment: segment_name.into(),
            offset,
            reason: format!("body_len {} < 16 (seq+meta minimum)", body_len),
        });
    }
    if bytes.len() < total {
        return Ok(None);  // torn tail (record incomplete)
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
    /// Sparse seq → byte offset index built during the scan.
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
    /// Sparse seq → byte offset index. Maintained on append; rebuilt on scan.
    index: Vec<(u64, u64)>,
}

impl SegmentFile {
    pub fn create(path: &Path, base_seq: u64) -> Result<Self, JournalError> {
        let mut file = OpenOptions::new()
            .read(true).write(true).create_new(true).open(path)?;
        let header = SegmentHeader {
            format_ver: SEGMENT_FORMAT_V,
            base_seq,
            created_at: now_nanos(),
        };
        let bytes = encode_header(&header);
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            size: SEGMENT_HEADER_SIZE as u64,
            base_seq,
            index: Vec::new(),
        })
    }

    pub fn open_for_read(path: &Path) -> Result<Self, JournalError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut hdr_bytes = [0u8; SEGMENT_HEADER_SIZE];
        file.read_exact(&mut hdr_bytes).map_err(|e| JournalError::Corrupted {
            segment: path.display().to_string(),
            offset: 0,
            reason: format!("header read failed: {e}"),
        })?;
        let header = decode_header(&hdr_bytes)?;
        let size = file.metadata()?.len();
        Ok(Self {
            path: path.to_path_buf(),
            file,
            size,
            base_seq: header.base_seq,
            index: Vec::new(),
        })
    }

    pub fn base_seq(&self) -> u64 { self.base_seq }
    pub fn size(&self) -> Result<u64, JournalError> { Ok(self.size) }
    pub fn path(&self) -> &Path { &self.path }
    pub fn index_snapshot(&self) -> &[(u64, u64)] { &self.index }

    /// Append a record at end-of-file. Caller must enforce monotonic seq.
    /// Updates the sparse index when the gap since the previous indexed
    /// record exceeds `SPARSE_INDEX_GAP`.
    pub fn append_record(
        &mut self,
        seq: u64,
        meta: u64,
        payload: &[u8],
    ) -> Result<u64, JournalError> {
        let bytes = encode_record(seq, meta, payload);
        self.file.seek(SeekFrom::Start(self.size))?;
        self.file.write_all(&bytes)?;
        let written_offset = self.size;
        self.size += bytes.len() as u64;
        let want_index = self.index.last().is_none_or(|(_, off)| {
            written_offset.saturating_sub(*off) >= SPARSE_INDEX_GAP
        });
        if want_index {
            self.index.push((seq, written_offset));
        }
        Ok(written_offset)
    }

    pub fn fsync(&mut self) -> Result<(), JournalError> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Read the entire body (after header) and decode all records.
    /// Tolerant of torn tail. Builds a fresh sparse index.
    pub fn scan(&self) -> Result<ScanResult, JournalError> {
        let mut f = self.file.try_clone()?;
        f.seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let segname = self.path.file_name().unwrap().to_string_lossy().to_string();
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
        let h = SegmentHeader { format_ver: 1, base_seq: 42, created_at: 100 };
        let bytes = encode_header(&h);
        assert_eq!(bytes.len(), SEGMENT_HEADER_SIZE);
        let decoded = decode_header(&bytes).unwrap();
        assert_eq!(decoded, h);
    }

    #[test]
    fn header_rejects_bad_magic() {
        let mut bytes = encode_header(&SegmentHeader { format_ver: 1, base_seq: 0, created_at: 0 });
        bytes[0] = b'X';
        assert!(matches!(decode_header(&bytes), Err(JournalError::Corrupted { .. })));
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
        assert!(matches!(decode_record(&bytes, "seg", 0), Err(JournalError::Corrupted { .. })));
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
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0u8; 5]).unwrap();
        }
        let opened = SegmentFile::open_for_read(&path).unwrap();
        let scan = opened.scan().unwrap();
        assert_eq!(scan.records.len(), 1);
        assert!(scan.had_torn_tail);
    }
}
