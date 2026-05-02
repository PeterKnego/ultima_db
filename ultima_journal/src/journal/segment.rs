// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use crate::JournalError;

pub const SEGMENT_MAGIC: &[u8; 8] = b"ULTJSEG\0";
pub const SEGMENT_FORMAT_V: u16 = 1;
pub const SEGMENT_HEADER_SIZE: usize = 32;

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
    if body_len < 16 {
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
}
