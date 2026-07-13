// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use super::SnapshotStreamError;

/// The 8-byte magic prefix (`b"ULTSNAP\0"`) that opens (and, as a bookend,
/// closes) every snapshot-stream wire payload. `decode_file_header` rejects
/// any stream not starting with these bytes as `SnapshotStreamError::BadMagic`.
pub const FILE_MAGIC: &[u8; 8] = b"ULTSNAP\0";
/// The wire format version this build writes and accepts. Bumped only on a
/// breaking wire-format change; `decode_file_header` rejects any other value
/// as `SnapshotStreamError::BadFormatVersion`.
pub const FILE_FORMAT_V: u16 = 1;

/// Decoded file-level header: format version, source snapshot version, and
/// declared table count. See `docs/tasks/task27_snapshot_stream.md` for the
/// full wire layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    /// Wire format version this payload was encoded with; must equal
    /// [`FILE_FORMAT_V`] for this build to decode it.
    pub format_ver: u16,
    /// The `Store` snapshot version the stream was built from.
    pub store_ver: u64,
    /// Number of table sections that follow in the stream.
    pub table_count: u32,
}

/// Decoded per-table header: name, best-effort type hint, row count, and
/// index definitions (names/kinds only — no key bytes; see
/// `docs/tasks/task27_snapshot_stream.md` "Wire format").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableHeader {
    /// Table name, used by the install path to look up the destination
    /// table by name (the authoritative dispatch key).
    pub name: String,
    /// Registry-assigned type id recorded at build time. A best-effort
    /// mismatch hint only — not stable across Rust builds — since install
    /// dispatches by table `name`, not by this id.
    pub record_type_id: u64,
    /// Number of rows in this table's row stream.
    pub row_count: u64,
    /// Secondary index definitions (name + kind) declared on the source
    /// table. The destination must already have matching `define_index`
    /// calls in place; only names/kinds are shipped, not key bytes.
    pub indexes: Vec<IndexDef>,
}

/// A secondary index's wire-level identity: its storage kind and name. Does
/// not carry the `KeyExtractor` closure — that must already exist on the
/// destination `Store`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    /// `0` = `IndexKind::Unique`, `1` = `IndexKind::NonUnique`. Custom
    /// indexes are rejected before this point (see
    /// `SnapshotStreamError::CustomIndexUnsupported`).
    pub kind: u8, // 0 = Unique, 1 = NonUnique
    /// Name of the index, as passed to `define_index`.
    pub name: String,
}

/// Appends `h` to `buf` in the wire format's little-endian file-header
/// layout (magic bytes, `format_ver`, `store_ver`, `table_count`).
pub fn encode_file_header(h: &FileHeader, buf: &mut Vec<u8>) {
    buf.extend_from_slice(FILE_MAGIC);
    buf.extend_from_slice(&h.format_ver.to_le_bytes());
    buf.extend_from_slice(&h.store_ver.to_le_bytes());
    buf.extend_from_slice(&h.table_count.to_le_bytes());
}

/// Decodes a [`FileHeader`] from the start of `b`. Returns the parsed
/// header and the number of bytes consumed. Fails with `Truncated` if `b` is
/// shorter than a full header, `BadMagic` if the magic prefix doesn't match
/// [`FILE_MAGIC`], or `BadFormatVersion` if `format_ver` isn't
/// [`FILE_FORMAT_V`].
pub fn decode_file_header(b: &[u8]) -> Result<(FileHeader, usize), SnapshotStreamError> {
    if b.len() < 8 + 2 + 8 + 4 {
        return Err(SnapshotStreamError::Truncated);
    }
    if &b[0..8] != FILE_MAGIC {
        return Err(SnapshotStreamError::BadMagic);
    }
    let format_ver = u16::from_le_bytes(b[8..10].try_into().unwrap());
    if format_ver != FILE_FORMAT_V {
        return Err(SnapshotStreamError::BadFormatVersion(format_ver));
    }
    let store_ver = u64::from_le_bytes(b[10..18].try_into().unwrap());
    let table_count = u32::from_le_bytes(b[18..22].try_into().unwrap());
    Ok((
        FileHeader {
            format_ver,
            store_ver,
            table_count,
        },
        22,
    ))
}

/// Appends `h` to `buf` in the wire format's little-endian table-header
/// layout (name length + utf-8 name, `record_type_id`, `row_count`, and each
/// index's kind + name).
pub fn encode_table_header(h: &TableHeader, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(h.name.len() as u16).to_le_bytes());
    buf.extend_from_slice(h.name.as_bytes());
    buf.extend_from_slice(&h.record_type_id.to_le_bytes());
    buf.extend_from_slice(&h.row_count.to_le_bytes());
    buf.extend_from_slice(&(h.indexes.len() as u16).to_le_bytes());
    for idx in &h.indexes {
        buf.push(idx.kind);
        buf.extend_from_slice(&(idx.name.len() as u16).to_le_bytes());
        buf.extend_from_slice(idx.name.as_bytes());
    }
}

/// Decodes a [`TableHeader`] from the start of `b`. Returns the parsed
/// header and the number of bytes consumed. Fails with `Truncated` if `b`
/// ends before a declared field/name is fully present, or `Malformed` if a
/// name is not valid UTF-8.
pub fn decode_table_header(b: &[u8]) -> Result<(TableHeader, usize), SnapshotStreamError> {
    let mut p = 0;
    if b.len() < p + 2 {
        return Err(SnapshotStreamError::Truncated);
    }
    let name_len = u16::from_le_bytes(b[p..p + 2].try_into().unwrap()) as usize;
    p += 2;
    if b.len() < p + name_len {
        return Err(SnapshotStreamError::Truncated);
    }
    let name = std::str::from_utf8(&b[p..p + name_len])
        .map_err(|_| SnapshotStreamError::Malformed("invalid UTF-8 in table name"))?
        .to_string();
    p += name_len;
    if b.len() < p + 8 + 8 + 2 {
        return Err(SnapshotStreamError::Truncated);
    }
    let record_type_id = u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
    p += 8;
    let row_count = u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
    p += 8;
    let idx_count = u16::from_le_bytes(b[p..p + 2].try_into().unwrap()) as usize;
    p += 2;
    let mut indexes = Vec::with_capacity(idx_count);
    for _ in 0..idx_count {
        if b.len() < p + 1 + 2 {
            return Err(SnapshotStreamError::Truncated);
        }
        let kind = b[p];
        p += 1;
        let nlen = u16::from_le_bytes(b[p..p + 2].try_into().unwrap()) as usize;
        p += 2;
        if b.len() < p + nlen {
            return Err(SnapshotStreamError::Truncated);
        }
        let nname = std::str::from_utf8(&b[p..p + nlen])
            .map_err(|_| SnapshotStreamError::Malformed("invalid UTF-8 in index name"))?
            .to_string();
        p += nlen;
        indexes.push(IndexDef { kind, name: nname });
    }
    Ok((
        TableHeader {
            name,
            record_type_id,
            row_count,
            indexes,
        },
        p,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_header_roundtrip() {
        let h = FileHeader {
            format_ver: 1,
            store_ver: 42,
            table_count: 3,
        };
        let mut buf = Vec::new();
        encode_file_header(&h, &mut buf);
        let (decoded, n) = decode_file_header(&buf).unwrap();
        assert_eq!(decoded, h);
        assert_eq!(n, buf.len());
    }

    #[test]
    fn table_header_roundtrip() {
        let h = TableHeader {
            name: "users".to_string(),
            record_type_id: 0xDEADBEEF,
            row_count: 1_000_000,
            indexes: vec![
                IndexDef {
                    kind: 0,
                    name: "by_email".to_string(),
                },
                IndexDef {
                    kind: 1,
                    name: "by_status".to_string(),
                },
            ],
        };
        let mut buf = Vec::new();
        encode_table_header(&h, &mut buf);
        let (decoded, n) = decode_table_header(&buf).unwrap();
        assert_eq!(decoded, h);
        assert_eq!(n, buf.len());
    }
}
