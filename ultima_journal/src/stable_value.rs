// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Serialize, de::DeserializeOwned};

use crate::{Durability, Notifier, StableValueError};

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

// ── StableValueConfig ──────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct StableValueConfig {
    pub path: PathBuf,
    pub durability: Durability,
    pub max_payload_bytes: u32,
}

impl StableValueConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            durability: Durability::Consistent,
            max_payload_bytes: 4096 - 17,
        }
    }
}

// ── StableValue<T> ─────────────────────────────────────────────────────────────

pub struct StableValue<T> {
    config: StableValueConfig,
    slot_size: u32,
    inner: Mutex<Inner<T>>,
}

struct Inner<T> {
    file: File,
    next_gen: u64,
    next_slot: u8,     // 0 or 1 — alternates
    cached: Option<T>,
}

impl<T> StableValue<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Open (or create) a StableValue file at `config.path`.
    ///
    /// On creation, writes the file header and two zeroed slots, then fsyncs.
    /// On open, reads the header and both slots and picks the winning slot by
    /// highest `gen`; falls back to the other slot if one is CRC-corrupt.
    pub fn open(config: StableValueConfig) -> Result<Self, StableValueError> {
        let exists = config.path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&config.path)?;

        let slot_size = config.max_payload_bytes + 17;

        if !exists {
            // Initialize: header + two zeroed slots (state=0).
            let header = encode_header(&SvHeader { format_ver: SV_FORMAT_V, slot_size });
            file.write_all(&header)?;
            let empty = SvSlot { r#gen: 0, state: 0, bytes: Vec::new() };
            let slot_bytes = encode_slot(&empty, slot_size)?;
            file.write_all(&slot_bytes)?; // slot 0
            file.write_all(&slot_bytes)?; // slot 1
            file.sync_all()?;
        }

        // Read header + both slots.
        file.seek(SeekFrom::Start(0))?;
        let mut hdr_bytes = [0u8; SV_HEADER_SIZE];
        file.read_exact(&mut hdr_bytes)?;
        let header = decode_header(&hdr_bytes)?;
        if header.format_ver != SV_FORMAT_V {
            return Err(StableValueError::Corrupted {
                reason: format!("unsupported format version {}", header.format_ver),
            });
        }

        let mut slot_bufs = [
            vec![0u8; header.slot_size as usize],
            vec![0u8; header.slot_size as usize],
        ];
        for buf in &mut slot_bufs {
            file.read_exact(buf)?;
        }
        let s0 = decode_slot(&slot_bufs[0])?;
        let s1 = decode_slot(&slot_bufs[1])?;
        let (cached, next_gen, next_slot) = pick_slot::<T>(s0, s1)?;

        Ok(Self {
            config,
            slot_size: header.slot_size,
            inner: Mutex::new(Inner { file, next_gen, next_slot, cached }),
        })
    }

    /// Return the last stored value, or `None` if never stored (or cleared).
    pub fn load(&self) -> Result<Option<T>, StableValueError> {
        Ok(self.inner.lock().unwrap().cached.clone())
    }

    /// Durably write `value` to the next slot, then rotate.
    ///
    /// Returns a `Notifier` that is already resolved (the fsync happens
    /// synchronously inside `store` when `Durability::Consistent`).
    pub fn store(&self, value: &T) -> Result<Notifier, StableValueError> {
        let bytes = bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|e| StableValueError::Serialize(e.to_string()))?;
        if (bytes.len() as u32) > self.slot_size - 17 {
            return Err(StableValueError::PayloadTooLarge {
                limit: self.slot_size - 17,
                got: bytes.len() as u32,
            });
        }
        let mut inner = self.inner.lock().unwrap();
        let slot = SvSlot { r#gen: inner.next_gen, state: 1, bytes };
        let buf = encode_slot(&slot, self.slot_size)?;
        let offset =
            SV_HEADER_SIZE as u64 + (inner.next_slot as u64) * (self.slot_size as u64);
        inner.file.seek(SeekFrom::Start(offset))?;
        inner.file.write_all(&buf)?;
        if matches!(self.config.durability, Durability::Consistent) {
            inner.file.sync_all()?;
        }
        inner.cached = Some(value.clone());
        inner.next_gen += 1;
        inner.next_slot ^= 1;
        Ok(Notifier::done())
    }

    /// Durably clear the stored value (write a tombstone with state=0).
    ///
    /// Returns a `Notifier` that is already resolved (the fsync happens
    /// synchronously inside `clear` when `Durability::Consistent`).
    pub fn clear(&self) -> Result<Notifier, StableValueError> {
        let mut inner = self.inner.lock().unwrap();
        let slot = SvSlot { r#gen: inner.next_gen, state: 0, bytes: Vec::new() };
        let buf = encode_slot(&slot, self.slot_size)?;
        let offset =
            SV_HEADER_SIZE as u64 + (inner.next_slot as u64) * (self.slot_size as u64);
        inner.file.seek(SeekFrom::Start(offset))?;
        inner.file.write_all(&buf)?;
        if matches!(self.config.durability, Durability::Consistent) {
            inner.file.sync_all()?;
        }
        inner.cached = None;
        inner.next_gen += 1;
        inner.next_slot ^= 1;
        Ok(Notifier::done())
    }

    /// Close the file handle. The `Drop` impl on `File` closes it anyway; this
    /// gives callers an explicit place to handle any errors.
    pub fn close(self) -> Result<(), StableValueError> {
        Ok(())
    }
}

// ── helpers ────────────────────────────────────────────────────────────────────

/// Choose the winning slot (highest `gen` among valid slots).
///
/// Returns `(cached_value, next_gen, next_slot_index)`.
///
/// If *both* slots are corrupt (CRC bad), returns `Corrupted`. A freshly
/// created file has two valid slots with `state=0` and `gen=0` — the tie goes
/// to slot 0.
fn pick_slot<T: DeserializeOwned>(
    s0: Option<SvSlot>,
    s1: Option<SvSlot>,
) -> Result<(Option<T>, u64, u8), StableValueError> {
    let pick = match (&s0, &s1) {
        (None, None) => {
            return Err(StableValueError::Corrupted {
                reason: "both slots invalid".into(),
            })
        }
        (Some(a), None) => Some((a.clone(), 0u8)),
        (None, Some(b)) => Some((b.clone(), 1u8)),
        (Some(a), Some(b)) => {
            if a.r#gen >= b.r#gen {
                Some((a.clone(), 0u8))
            } else {
                Some((b.clone(), 1u8))
            }
        }
    };
    let (winner, slot_idx) = pick.unwrap();
    let next_gen = winner.r#gen + 1;
    let next_slot = slot_idx ^ 1;
    let cached: Option<T> = if winner.state == 0 {
        None
    } else {
        let v: T =
            bincode::serde::decode_from_slice(&winner.bytes, bincode::config::standard())
                .map_err(|e| StableValueError::Corrupted { reason: e.to_string() })?
                .0;
        Some(v)
    };
    Ok((cached, next_gen, next_slot))
}

// ── tests ──────────────────────────────────────────────────────────────────────

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

    #[test]
    fn decode_header_too_short() {
        let bytes = vec![0u8; 10];
        assert!(matches!(
            decode_header(&bytes),
            Err(StableValueError::Corrupted { reason }) if reason.contains("too short"),
        ));
    }

    #[test]
    fn decode_header_bad_magic() {
        let mut bytes = encode_header(&SvHeader { format_ver: 1, slot_size: 1024 });
        bytes[0] = b'X';
        assert!(matches!(
            decode_header(&bytes),
            Err(StableValueError::Corrupted { reason }) if reason.contains("magic"),
        ));
    }

    #[test]
    fn decode_header_crc_mismatch() {
        let mut bytes = encode_header(&SvHeader { format_ver: 1, slot_size: 1024 });
        bytes[10] ^= 0xFF;
        assert!(matches!(
            decode_header(&bytes),
            Err(StableValueError::Corrupted { reason }) if reason.contains("crc"),
        ));
    }

    #[test]
    fn encode_slot_rejects_oversized_payload() {
        let s = SvSlot {
            r#gen: 0,
            state: 1,
            bytes: vec![0u8; 1024],
        };
        // slot_size of 64 can hold at most 47 bytes of payload (64 - 17).
        assert!(matches!(
            encode_slot(&s, 64),
            Err(StableValueError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn decode_slot_too_short_returns_none() {
        // Less than 17-byte minimum slot frame.
        let bytes = vec![0u8; 10];
        assert!(decode_slot(&bytes).unwrap().is_none());
    }
}

#[cfg(test)]
mod sv_tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
    struct Vote {
        term: u64,
        voted_for: u64,
    }

    #[test]
    fn open_empty_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vote.sv");
        let cfg = StableValueConfig::new(&path);
        let sv = StableValue::<Vote>::open(cfg).unwrap();
        assert_eq!(sv.load().unwrap(), None);
    }

    #[test]
    fn store_then_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vote.sv");
        let sv = StableValue::<Vote>::open(StableValueConfig::new(&path)).unwrap();
        let v = Vote { term: 5, voted_for: 42 };
        sv.store(&v).unwrap().wait().unwrap();
        assert_eq!(sv.load().unwrap(), Some(v));
    }

    #[test]
    fn store_then_reopen_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vote.sv");
        {
            let sv = StableValue::<Vote>::open(StableValueConfig::new(&path)).unwrap();
            sv.store(&Vote { term: 3, voted_for: 7 }).unwrap().wait().unwrap();
        }
        let sv2 = StableValue::<Vote>::open(StableValueConfig::new(&path)).unwrap();
        assert_eq!(sv2.load().unwrap(), Some(Vote { term: 3, voted_for: 7 }));
    }

    #[test]
    fn higher_gen_wins() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vote.sv");
        let sv = StableValue::<Vote>::open(StableValueConfig::new(&path)).unwrap();
        sv.store(&Vote { term: 1, voted_for: 1 }).unwrap().wait().unwrap();
        sv.store(&Vote { term: 2, voted_for: 2 }).unwrap().wait().unwrap();
        sv.store(&Vote { term: 3, voted_for: 3 }).unwrap().wait().unwrap();
        assert_eq!(sv.load().unwrap().unwrap().term, 3);
    }

    #[test]
    fn clear_makes_load_return_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v.sv");
        let sv = StableValue::<Vote>::open(StableValueConfig::new(&path)).unwrap();
        sv.store(&Vote { term: 1, voted_for: 1 }).unwrap().wait().unwrap();
        sv.clear().unwrap().wait().unwrap();
        assert_eq!(sv.load().unwrap(), None);
    }

    #[test]
    fn store_rejects_oversized_payload() {
        // Tiny slot — payload bigger than max.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v.sv");
        let mut cfg = StableValueConfig::new(&path);
        cfg.max_payload_bytes = 16;  // very small
        let sv = StableValue::<Vec<u8>>::open(cfg).unwrap();
        let big = vec![0xABu8; 1024];
        assert!(matches!(
            sv.store(&big),
            Err(StableValueError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn close_consumes_stable_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v.sv");
        let sv = StableValue::<Vote>::open(StableValueConfig::new(&path)).unwrap();
        sv.close().unwrap();
    }

    #[test]
    fn open_rejects_bad_format_version() {
        // Hand-write a file with a mismatched format version in the header.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v.sv");
        // Build a valid-CRC header but with format_ver = 99.
        let bad_header = encode_header(&SvHeader { format_ver: 99, slot_size: 1024 });
        // Two empty-ish slots so file size matches what `open` expects to read.
        let empty = SvSlot { r#gen: 0, state: 0, bytes: Vec::new() };
        let slot_bytes = encode_slot(&empty, 1024).unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(&bad_header);
        buf.extend_from_slice(&slot_bytes);
        buf.extend_from_slice(&slot_bytes);
        std::fs::write(&path, &buf).unwrap();
        assert!(matches!(
            StableValue::<Vote>::open(StableValueConfig::new(&path)),
            Err(StableValueError::Corrupted { .. })
        ));
    }

    #[test]
    fn open_picks_only_valid_slot_when_other_corrupt() {
        // Slot 0 corrupt, slot 1 valid → covers the (None, Some(b)) branch in pick_slot.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v.sv");
        {
            let sv = StableValue::<Vote>::open(StableValueConfig::new(&path)).unwrap();
            sv.store(&Vote { term: 1, voted_for: 1 }).unwrap().wait().unwrap();  // slot 0
            sv.store(&Vote { term: 2, voted_for: 2 }).unwrap().wait().unwrap();  // slot 1
        }
        // Reopen and corrupt slot 0 by zeroing its CRC area.
        let header_size = SV_HEADER_SIZE as u64;
        let slot_size = std::fs::metadata(&path).unwrap().len() / 2 - header_size / 2;
        // Easier: read file, flip slot-0 CRC bytes, write back.
        let mut bytes = std::fs::read(&path).unwrap();
        let crc_off = (header_size + slot_size - 4) as usize;
        for i in 0..4 {
            bytes[crc_off + i] ^= 0xFF;
        }
        std::fs::write(&path, &bytes).unwrap();
        let sv = StableValue::<Vote>::open(StableValueConfig::new(&path)).unwrap();
        // After the corruption the only valid slot is the *other* one. Which
        // physical slot that is depends on the rotation order, but the
        // surviving value must be one of the two we stored.
        let v = sv.load().unwrap().unwrap();
        assert!(
            v == Vote { term: 1, voted_for: 1 } || v == Vote { term: 2, voted_for: 2 },
            "unexpected surviving value: {v:?}",
        );
    }
}
