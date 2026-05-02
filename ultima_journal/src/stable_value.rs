// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use crate::StableValueError;

pub const SV_MAGIC: &[u8; 8] = b"ULTSVAL\0";
pub const SV_FORMAT_V: u16 = 1;
pub const SV_HEADER_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvHeader {
    pub format_ver: u16,
    pub slot_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SvSlot {
    pub r#gen: u64,
    pub state: u8,    // 0 = absent, 1 = present
    pub bytes: Vec<u8>,
}

pub fn encode_header(h: &SvHeader) -> [u8; SV_HEADER_SIZE] {
    let mut buf = [0u8; SV_HEADER_SIZE];
    buf[0..8].copy_from_slice(SV_MAGIC);
    buf[8..10].copy_from_slice(&h.format_ver.to_le_bytes());
    buf[10..14].copy_from_slice(&h.slot_size.to_le_bytes());
    let crc = crc32fast::hash(&buf[0..14]);
    buf[14..18].copy_from_slice(&crc.to_le_bytes());
    buf
}

pub fn decode_header(b: &[u8]) -> Result<SvHeader, StableValueError> {
    if b.len() < SV_HEADER_SIZE {
        return Err(StableValueError::Corrupted { reason: "header too short".into() });
    }
    if &b[0..8] != SV_MAGIC {
        return Err(StableValueError::Corrupted { reason: "bad magic".into() });
    }
    let format_ver = u16::from_le_bytes(b[8..10].try_into().unwrap());
    let slot_size = u32::from_le_bytes(b[10..14].try_into().unwrap());
    let stored_crc = u32::from_le_bytes(b[14..18].try_into().unwrap());
    let actual = crc32fast::hash(&b[0..14]);
    if stored_crc != actual {
        return Err(StableValueError::Corrupted { reason: "header crc mismatch".into() });
    }
    Ok(SvHeader { format_ver, slot_size })
}

pub fn encode_slot(s: &SvSlot, slot_size: u32) -> Result<Vec<u8>, StableValueError> {
    let needed = 8 + 1 + 4 + s.bytes.len() + 4;
    if needed > slot_size as usize {
        return Err(StableValueError::PayloadTooLarge {
            limit: slot_size - 17,
            got: s.bytes.len() as u32,
        });
    }
    let mut buf = vec![0u8; slot_size as usize];
    buf[0..8].copy_from_slice(&s.r#gen.to_le_bytes());
    buf[8] = s.state;
    buf[9..13].copy_from_slice(&(s.bytes.len() as u32).to_le_bytes());
    buf[13..13 + s.bytes.len()].copy_from_slice(&s.bytes);
    let crc_offset = slot_size as usize - 4;
    let crc = crc32fast::hash(&buf[0..crc_offset]);
    buf[crc_offset..].copy_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

pub fn decode_slot(b: &[u8]) -> Result<Option<SvSlot>, StableValueError> {
    if b.len() < 17 { return Ok(None); }
    let crc_offset = b.len() - 4;
    let stored_crc = u32::from_le_bytes(b[crc_offset..].try_into().unwrap());
    let actual = crc32fast::hash(&b[0..crc_offset]);
    if stored_crc != actual { return Ok(None); }
    let r#gen = u64::from_le_bytes(b[0..8].try_into().unwrap());
    let state = b[8];
    let len = u32::from_le_bytes(b[9..13].try_into().unwrap()) as usize;
    if 13 + len > crc_offset {
        return Ok(None);
    }
    let bytes = b[13..13 + len].to_vec();
    Ok(Some(SvSlot { r#gen, state, bytes }))
}

/// Placeholder — replaced in subsequent tasks.
pub struct StableValue<T>(std::marker::PhantomData<T>);
/// Placeholder — replaced in subsequent tasks.
pub struct StableValueConfig;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let h = SvHeader { format_ver: 1, slot_size: 4096 };
        let bytes = encode_header(&h);
        assert_eq!(decode_header(&bytes).unwrap(), h);
    }

    #[test]
    fn slot_roundtrip() {
        let s = SvSlot { r#gen: 7, state: 1, bytes: b"hello".to_vec() };
        let bytes = encode_slot(&s, 4096).unwrap();
        assert_eq!(bytes.len(), 4096);
        assert_eq!(decode_slot(&bytes).unwrap().unwrap(), s);
    }

    #[test]
    fn slot_with_bad_crc_returns_none() {
        let s = SvSlot { r#gen: 1, state: 1, bytes: vec![1, 2, 3] };
        let mut bytes = encode_slot(&s, 1024).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(decode_slot(&bytes).unwrap().is_none());
    }
}
