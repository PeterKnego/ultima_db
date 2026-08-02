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
///
/// v2 (this build) made the format primary-key generic. Two changes, both
/// breaking:
///
/// - Each row is `key_len(u32-le) | key_bytes | val_len(u32-le) | val_bytes`.
///   v1 wrote a fixed 8-byte little-endian `u64` in place of the first two
///   fields, which could not carry a `String`, `Vec<u8>`, or tuple key. The
///   key bytes are [`PrimaryKey::encode`](crate::PrimaryKey::encode) output,
///   whose lexicographic order equals key order — so the receiver's
///   strict-ascent check over decoded keys is equivalent to one over the
///   raw bytes, which is what keeps `BTree::from_sorted` correct.
/// - [`TableHeader`] carries `key_type_id` and `key_type`, the source table's
///   primary-key identity. See those fields for why they are checked rather
///   than ignored.
pub const FILE_FORMAT_V: u16 = 2;

/// Decoded file-level header: format version, source snapshot version, and
/// declared table count. See
/// [the task27 design notes](https://github.com/PeterKnego/ultima_db/blob/main/docs/tasks/task27_snapshot_stream.md)
/// for the full wire layout.
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
/// [the task27 design notes](https://github.com/PeterKnego/ultima_db/blob/main/docs/tasks/task27_snapshot_stream.md),
/// "Wire format").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableHeader {
    /// Table name, used by the install path to look up the destination
    /// table by name (the authoritative dispatch key).
    pub name: String,
    /// Registry-assigned type id recorded at build time. A best-effort
    /// mismatch hint only — not stable across Rust builds — since install
    /// dispatches by table `name`, not by this id.
    pub record_type_id: u64,
    /// [`PrimaryKey::KEY_TYPE_ID`](crate::PrimaryKey::KEY_TYPE_ID) of the
    /// source table's primary key, taken from the *live* table the rows were
    /// read out of.
    ///
    /// Unlike `record_type_id` this is *enforced* on install: the row keys
    /// are opaque bytes, and several key types accept the same bytes (the
    /// eight bytes of `1u64` are also a valid NUL-padded `String`), so a
    /// stream aimed at a destination with a different `K` would decode
    /// cleanly, pass the strict-ascent check, and install a table full of
    /// garbage keys with no error anywhere. Comparing this id at the trust
    /// boundary catches it — see
    /// [`SnapshotStreamError::KeyTypeMismatch`](crate::SnapshotStreamError::KeyTypeMismatch).
    ///
    /// It is the id and not `key_type` below that decides, because the id is
    /// declared by the key type itself: `std::any::type_name` is neither
    /// promised stable across compiler versions (which would refuse a stream
    /// that would have decoded — safe, but noise) nor injective across crate
    /// versions of a third-party key type (which would *accept* a stream that
    /// must not be). The same id is what the WAL and checkpoint formats
    /// carry, so all three agree on what "same key type" means.
    pub key_type_id: u32,
    /// `std::any::type_name` of the source table's primary key, carried for
    /// diagnostics: a mismatch is decided by `key_type_id`, but an id alone
    /// is a poor error message, and the emitting end is the only place the
    /// name is known. Never compared.
    pub key_type: String,
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
/// layout (name length + utf-8 name, `record_type_id`, `key_type_id`,
/// `key_type` length + utf-8 name, `row_count`, and each index's kind + name).
pub fn encode_table_header(h: &TableHeader, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(h.name.len() as u16).to_le_bytes());
    buf.extend_from_slice(h.name.as_bytes());
    buf.extend_from_slice(&h.record_type_id.to_le_bytes());
    buf.extend_from_slice(&h.key_type_id.to_le_bytes());
    buf.extend_from_slice(&(h.key_type.len() as u16).to_le_bytes());
    buf.extend_from_slice(h.key_type.as_bytes());
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
    if b.len() < p + 8 + 4 + 2 {
        return Err(SnapshotStreamError::Truncated);
    }
    let record_type_id = u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
    p += 8;
    let key_type_id = u32::from_le_bytes(b[p..p + 4].try_into().unwrap());
    p += 4;
    let key_type_len = u16::from_le_bytes(b[p..p + 2].try_into().unwrap()) as usize;
    p += 2;
    if b.len() < p + key_type_len {
        return Err(SnapshotStreamError::Truncated);
    }
    let key_type = std::str::from_utf8(&b[p..p + key_type_len])
        .map_err(|_| SnapshotStreamError::Malformed("invalid UTF-8 in key type name"))?
        .to_string();
    p += key_type_len;
    if b.len() < p + 8 + 2 {
        return Err(SnapshotStreamError::Truncated);
    }
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
            key_type_id,
            key_type,
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
            format_ver: FILE_FORMAT_V,
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
            key_type_id: <String as crate::PrimaryKey>::KEY_TYPE_ID,
            key_type: "alloc::string::String".to_string(),
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

    /// Every prefix of a table header must be rejected as `Truncated` rather
    /// than panicking on an out-of-range slice. The `key_type_id`/`key_type`
    /// pair added in v2 sits between two other length-prefixed fields, so its
    /// bounds check is easy to get subtly wrong.
    #[test]
    fn truncated_table_header_never_panics() {
        let h = TableHeader {
            name: "users".to_string(),
            record_type_id: 7,
            key_type_id: <String as crate::PrimaryKey>::KEY_TYPE_ID,
            key_type: "alloc::string::String".to_string(),
            row_count: 3,
            indexes: vec![IndexDef {
                kind: 0,
                name: "by_email".to_string(),
            }],
        };
        let mut buf = Vec::new();
        encode_table_header(&h, &mut buf);
        for cut in 0..buf.len() {
            assert!(
                matches!(
                    decode_table_header(&buf[..cut]),
                    Err(SnapshotStreamError::Truncated)
                ),
                "prefix of length {cut} should decode as Truncated"
            );
        }
    }

    /// A v1 payload (or anything else) must be refused by version, not
    /// misparsed: v1 rows carried a fixed 8-byte key where v2 carries a
    /// length prefix, so there is no safe way to read one as the other.
    #[test]
    fn v1_format_version_is_rejected() {
        let mut buf = Vec::new();
        encode_file_header(
            &FileHeader {
                format_ver: 1,
                store_ver: 9,
                table_count: 1,
            },
            &mut buf,
        );
        assert!(matches!(
            decode_file_header(&buf),
            Err(SnapshotStreamError::BadFormatVersion(1))
        ));
    }
}
