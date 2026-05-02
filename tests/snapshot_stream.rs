// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Integration tests for the snapshot_stream build path (Task 3).
//!
//! These tests require the `persistence` feature because `Store::snapshot_stream`
//! and `Store::register_table` are gated on it.

#[cfg(feature = "persistence")]
mod tests {
    use std::io::Read;

    use serde::{Deserialize, Serialize};
    use ultima_db::snapshot_stream::codec::{FILE_FORMAT_V, FILE_MAGIC};
    use ultima_db::{Store, StoreConfig};

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
    struct Row {
        value: u64,
    }

    /// The snapshot stream must start with the 8-byte magic `ULTSNAP\0`
    /// followed by the format version (u16 LE = 1), the store version (u64 LE),
    /// and the table count (u32 LE).
    #[test]
    fn snapshot_stream_starts_with_magic_and_header() {
        let store = Store::new(StoreConfig::default()).unwrap();
        store.register_table::<Row>("rows").unwrap();

        {
            let mut tx = store.begin_write(None).unwrap();
            let mut table = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=10u64 {
                table.insert(Row { value: i }).unwrap();
            }
            tx.commit().unwrap();
        }

        let mut reader = store.snapshot_stream(None).unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();

        // Magic bytes at offset 0.
        assert_eq!(&buf[0..8], FILE_MAGIC, "file magic mismatch");

        // Format version at offset 8 (u16 LE).
        let format_ver = u16::from_le_bytes(buf[8..10].try_into().unwrap());
        assert_eq!(format_ver, FILE_FORMAT_V, "format version mismatch");

        // Table count (u32 LE) at offset 18 — must be 1 ("rows").
        let table_count = u32::from_le_bytes(buf[18..22].try_into().unwrap());
        assert_eq!(table_count, 1, "expected exactly 1 table in snapshot");
    }

    /// The stream must also end with the bookend magic after the file trailer.
    #[test]
    fn snapshot_stream_ends_with_bookend_magic() {
        let store = Store::new(StoreConfig::default()).unwrap();
        store.register_table::<Row>("rows").unwrap();

        {
            let mut tx = store.begin_write(None).unwrap();
            let mut table = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=5u64 {
                table.insert(Row { value: i }).unwrap();
            }
            tx.commit().unwrap();
        }

        let mut reader = store.snapshot_stream(None).unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();

        // Bookend magic is the last 8 bytes of the stream.
        let bookend = &buf[buf.len() - 8..];
        assert_eq!(bookend, FILE_MAGIC, "bookend magic mismatch");
    }

    /// Empty store (no tables, no rows) should still produce a valid stream.
    #[test]
    fn snapshot_stream_empty_store() {
        let store = Store::new(StoreConfig::default()).unwrap();

        let mut reader = store.snapshot_stream(None).unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();

        // Magic + format_ver(2) + store_ver(8) + table_count(4) = 22 bytes header
        // + total_rows(8) + total_crc(4) + bookend(8) = 20 bytes trailer
        // = 42 bytes total.
        assert!(buf.len() >= 42, "stream too short for empty snapshot: {} bytes", buf.len());
        assert_eq!(&buf[0..8], FILE_MAGIC);
        let table_count = u32::from_le_bytes(buf[18..22].try_into().unwrap());
        assert_eq!(table_count, 0, "no tables should be in the empty snapshot");
    }

    /// Requesting a non-existent version should return an error.
    #[test]
    fn snapshot_stream_unknown_version_errors() {
        let store = Store::new(StoreConfig::default()).unwrap();
        let res = store.snapshot_stream(Some(999_999));
        assert!(res.is_err(), "expected error for unknown version");
    }

    /// Stream encodes the correct number of rows in the table header.
    #[test]
    fn snapshot_stream_table_header_row_count() {
        let store = Store::new(StoreConfig::default()).unwrap();
        store.register_table::<Row>("rows").unwrap();

        {
            let mut tx = store.begin_write(None).unwrap();
            let mut table = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=42u64 {
                table.insert(Row { value: i }).unwrap();
            }
            tx.commit().unwrap();
        }

        let mut reader = store.snapshot_stream(None).unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();

        // File header is 22 bytes, then table header starts.
        // Table header layout: name_len(2) + name_bytes + type_id(8) + row_count(8) + ...
        let p = 22usize; // after file header
        let name_len = u16::from_le_bytes(buf[p..p + 2].try_into().unwrap()) as usize;
        let row_count_offset = p + 2 + name_len + 8; // skip name_len + name + type_id
        let row_count = u64::from_le_bytes(buf[row_count_offset..row_count_offset + 8].try_into().unwrap());
        assert_eq!(row_count, 42, "table header row_count mismatch");
    }

    /// Snapshot at a specific version streams from that frozen version, not latest.
    #[test]
    fn snapshot_stream_at_specific_version() {
        let store = Store::new(StoreConfig::default()).unwrap();
        store.register_table::<Row>("rows").unwrap();

        // Write version 1: 5 rows.
        {
            let mut tx = store.begin_write(None).unwrap();
            let mut table = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=5u64 {
                table.insert(Row { value: i }).unwrap();
            }
            tx.commit().unwrap();
        }
        let v1 = store.latest_version();

        // Write version 2: 5 more rows.
        {
            let mut tx = store.begin_write(None).unwrap();
            let mut table = tx.open_table::<Row>("rows").unwrap();
            for i in 6..=10u64 {
                table.insert(Row { value: i }).unwrap();
            }
            tx.commit().unwrap();
        }

        // Stream from v1 — should report 5 rows, not 10.
        let mut reader = store.snapshot_stream(Some(v1)).unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();

        let p = 22usize;
        let name_len = u16::from_le_bytes(buf[p..p + 2].try_into().unwrap()) as usize;
        let row_count_offset = p + 2 + name_len + 8;
        let row_count = u64::from_le_bytes(buf[row_count_offset..row_count_offset + 8].try_into().unwrap());
        assert_eq!(row_count, 5, "streaming v1 should have 5 rows");
    }
}
