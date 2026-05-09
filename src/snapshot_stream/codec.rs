// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use super::SnapshotStreamError;

pub const FILE_MAGIC: &[u8; 8] = b"ULTSNAP\0";
pub const FILE_FORMAT_V: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub format_ver: u16,
    pub store_ver: u64,
    pub table_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableHeader {
    pub name: String,
    pub record_type_id: u64,
    pub row_count: u64,
    pub indexes: Vec<IndexDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    pub kind: u8,    // 0 = Unique, 1 = NonUnique
    pub name: String,
}

pub fn encode_file_header(h: &FileHeader, buf: &mut Vec<u8>) {
    buf.extend_from_slice(FILE_MAGIC);
    buf.extend_from_slice(&h.format_ver.to_le_bytes());
    buf.extend_from_slice(&h.store_ver.to_le_bytes());
    buf.extend_from_slice(&h.table_count.to_le_bytes());
}

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
