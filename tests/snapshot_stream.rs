// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Integration tests for the snapshot_stream build and install paths
//! (Tasks 3 and 4).
//!
//! These tests require the `persistence` feature because `Store::snapshot_stream`,
//! `Store::install_snapshot_stream`, and `Store::register_table` are gated on it.

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

    // ── Task 4 tests: install_snapshot_stream ────────────────────────────────

    /// Full roundtrip: build a snapshot stream from a source store, install
    /// it into a fresh destination store, and verify all rows survive.
    #[test]
    fn build_then_install_roundtrips_full_state() {
        // Source store: 100 rows.
        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<Row>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=100u64 {
                t.insert(Row { value: i * 10 }).unwrap();
            }
            tx.commit().unwrap();
        }

        // Drain snapshot to bytes.
        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        // Destination store: install.
        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<Row>("rows").unwrap();
        let new_ver = dst
            .install_snapshot_stream(
                std::io::Cursor::new(&bytes),
                Default::default(),
            )
            .unwrap();
        assert!(new_ver > 0, "install must produce a positive version");

        // Verify all 100 rows.
        let read = dst.begin_read(None).unwrap();
        let t = read.open_table::<Row>("rows").unwrap();
        for i in 1..=100u64 {
            let row = t.get(i).expect("key must exist");
            assert_eq!(row.value, i * 10, "row value mismatch for id={i}");
        }
    }

    /// Truncated wire bytes must fail cleanly without modifying the destination.
    #[test]
    fn truncated_wire_bytes_fail_install_cleanly() {
        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<Row>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=50u64 {
                t.insert(Row { value: i }).unwrap();
            }
            tx.commit().unwrap();
        }

        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<Row>("rows").unwrap();

        // Truncate to half — must error.
        let half = bytes.len() / 2;
        let res = dst.install_snapshot_stream(
            std::io::Cursor::new(&bytes[..half]),
            Default::default(),
        );
        assert!(res.is_err(), "truncated stream must fail");

        // Destination must remain empty — the table should not exist in the
        // snapshot (open_table returns KeyNotFound for an unwritten table).
        let read = dst.begin_read(None).unwrap();
        let res = read.open_table::<Row>("rows");
        assert!(
            res.is_err(),
            "destination must have no 'rows' table after failed install"
        );
    }

    /// Corrupt a byte in the row data — the table CRC check must catch it.
    #[test]
    fn corrupted_byte_fails_crc_check() {
        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<Row>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=10u64 {
                t.insert(Row { value: i }).unwrap();
            }
            tx.commit().unwrap();
        }

        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        // Flip a byte roughly in the middle of the row data.
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;

        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<Row>("rows").unwrap();
        let res = dst.install_snapshot_stream(
            std::io::Cursor::new(&bytes),
            Default::default(),
        );
        assert!(
            matches!(res, Err(ultima_db::SnapshotStreamError::BadCrc { .. })),
            "corrupted bytes must produce BadCrc error, got: {res:?}"
        );
    }

    /// Unknown table with OnUnknown::Drop should succeed (table is skipped).
    #[test]
    fn unknown_table_drop_succeeds() {
        use ultima_db::snapshot_stream::install::{InstallOptions, OnUnknown};

        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<Row>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            t.insert(Row { value: 42 }).unwrap();
            tx.commit().unwrap();
        }

        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        // Destination does NOT register "rows" — it's unknown.
        let dst = Store::new(StoreConfig::default()).unwrap();
        let res = dst.install_snapshot_stream(
            std::io::Cursor::new(&bytes),
            InstallOptions {
                on_unknown_tables: OnUnknown::Drop,
                ..Default::default()
            },
        );
        assert!(res.is_ok(), "Drop policy must succeed even with unknown table");
    }

    /// Unknown table with OnUnknown::Error must fail.
    #[test]
    fn unknown_table_error_fails() {
        use ultima_db::snapshot_stream::install::{InstallOptions, OnUnknown};
        use ultima_db::SnapshotStreamError;

        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<Row>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            t.insert(Row { value: 7 }).unwrap();
            tx.commit().unwrap();
        }

        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        // Destination does NOT register "rows".
        let dst = Store::new(StoreConfig::default()).unwrap();
        let res = dst.install_snapshot_stream(
            std::io::Cursor::new(&bytes),
            InstallOptions {
                on_unknown_tables: OnUnknown::Error,
                ..Default::default()
            },
        );
        assert!(
            matches!(res, Err(SnapshotStreamError::UnknownTable { .. })),
            "Error policy must produce UnknownTable, got: {res:?}"
        );
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

    // ── Task 5 tests: list_checkpoints + open_checkpoint_reader ─────────────

    /// `list_checkpoints` returns the versions of all on-disk checkpoint files,
    /// in ascending order.
    #[test]
    fn list_checkpoints_returns_versions() {
        use ultima_db::Persistence;

        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig {
            persistence: Persistence::Smr { dir: dir.path().to_path_buf() },
            ..StoreConfig::default()
        };
        let store = Store::new(cfg).unwrap();
        store.register_table::<Row>("rows").unwrap();

        // First write + checkpoint.
        {
            let mut tx = store.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            t.insert(Row { value: 1 }).unwrap();
            tx.commit().unwrap();
        }
        store.checkpoint().unwrap();
        let v1 = store.latest_version();

        // Second write + checkpoint.
        {
            let mut tx = store.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            t.insert(Row { value: 2 }).unwrap();
            tx.commit().unwrap();
        }
        store.checkpoint().unwrap();
        let v2 = store.latest_version();

        // Note: checkpoint() prunes old checkpoints (keeps only the latest),
        // so we only expect v2 to be present on disk.
        let cps = store.list_checkpoints().unwrap();
        assert!(cps.contains(&v2), "v2={v2} must be in checkpoint list: {cps:?}");
        // v1 has been pruned by the second checkpoint().
        // v1 < v2 always holds.
        assert!(v1 < v2, "v1={v1} must be < v2={v2}");
        assert!(cps.windows(2).all(|w| w[0] <= w[1]), "list must be sorted");
    }

    /// `open_checkpoint_reader` opens a specific on-disk checkpoint and returns
    /// a valid `SnapshotReader` that emits the `ULTSNAP` wire format.
    #[test]
    fn open_checkpoint_reader_streams_disk_checkpoint() {
        use ultima_db::Persistence;
        use ultima_db::snapshot_stream::codec::FILE_MAGIC;

        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig {
            persistence: Persistence::Smr { dir: dir.path().to_path_buf() },
            ..StoreConfig::default()
        };
        let store = Store::new(cfg).unwrap();
        store.register_table::<Row>("rows").unwrap();

        // Write 20 rows and checkpoint.
        {
            let mut tx = store.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=20u64 {
                t.insert(Row { value: i }).unwrap();
            }
            tx.commit().unwrap();
        }
        store.checkpoint().unwrap();
        let v = store.latest_version();

        // Open the checkpoint as a reader (without installing it).
        let mut reader = store.open_checkpoint_reader(v).unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();

        // Verify wire format magic and minimum size.
        assert_eq!(&bytes[0..8], FILE_MAGIC, "must start with FILE_MAGIC");
        assert!(bytes.len() > 100, "stream too short: {} bytes", bytes.len());
    }

    // ── Task 6 tests: concurrency test ─────────────────────────────────────

    /// Snapshot build must not block concurrent writes. This test verifies
    /// that writers can make progress while a snapshot stream is being read
    /// from a frozen `Arc<Snapshot>`.
    #[test]
    fn snapshot_build_does_not_block_concurrent_writes() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        let store = Arc::new(Store::new(StoreConfig::default()).unwrap());
        store.register_table::<Row>("rows").unwrap();

        // Pre-populate with 10,000 rows to ensure the snapshot build takes
        // non-trivial time.
        {
            let mut tx = store.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=10_000u64 {
                t.insert(Row { value: i }).unwrap();
            }
            tx.commit().unwrap();
        }

        let stop = Arc::new(AtomicBool::new(false));
        let writes_done = Arc::new(AtomicU64::new(0));

        // Spawn a writer thread that continuously inserts rows with value=u64::MAX
        // until signaled to stop.
        let store_w = Arc::clone(&store);
        let stop_w = Arc::clone(&stop);
        let writes_w = Arc::clone(&writes_done);
        let writer = std::thread::spawn(move || {
            while !stop_w.load(Ordering::SeqCst) {
                let mut tx = store_w.begin_write(None).unwrap();
                let mut t = tx.open_table::<Row>("rows").unwrap();
                t.insert(Row { value: u64::MAX }).unwrap();
                tx.commit().unwrap();
                writes_w.fetch_add(1, Ordering::SeqCst);
            }
        });

        // Record the write count before snapshot build.
        let pre_snapshot_writes = writes_done.load(Ordering::SeqCst);

        // Build and drain the snapshot stream on the main thread.
        let mut reader = store.snapshot_stream(None).unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();

        // Record the write count after snapshot build completes.
        let post_snapshot_writes = writes_done.load(Ordering::SeqCst);

        // Signal the writer to stop and wait for it.
        stop.store(true, Ordering::SeqCst);
        writer.join().unwrap();

        // Verify that writes continued during the snapshot build.
        assert!(
            post_snapshot_writes > pre_snapshot_writes,
            "writer did not progress during snapshot build (pre={}, post={})",
            pre_snapshot_writes,
            post_snapshot_writes
        );

        // Verify that the snapshot stream is non-trivially sized (contains data).
        assert!(
            buf.len() > 100_000,
            "snapshot buffer too small: {} bytes",
            buf.len()
        );
    }

    /// Secondary indexes defined on the destination side must survive a
    /// snapshot install. The wire format ships index names + kinds; the
    /// install path clones the destination's existing index definitions
    /// (KeyExtractors compiled into the dest binary) and rebuilds them
    /// over the new rows.
    #[test]
    fn secondary_indexes_survive_install() {
        use ultima_db::IndexKind;

        #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
        struct User {
            email: String,
            age: u64,
        }

        // Source: populate with 10 users.
        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<User>("users").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<User>("users").unwrap();
            for i in 1..=10u64 {
                t.insert(User {
                    email: format!("u{i}@example.com"),
                    age: 20 + i,
                })
                .unwrap();
            }
            tx.commit().unwrap();
        }

        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        // Destination: register table + define the index in an empty
        // table BEFORE install. install_snapshot_stream then clones the
        // KeyExtractor and rebuilds the index over the installed rows.
        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<User>("users").unwrap();
        {
            let mut tx = dst.begin_write(None).unwrap();
            let mut t = tx.open_table::<User>("users").unwrap();
            t.define_index("by_email", IndexKind::Unique, |u: &User| u.email.clone())
                .unwrap();
            tx.commit().unwrap();
        }

        dst.install_snapshot_stream(std::io::Cursor::new(&bytes), Default::default())
            .unwrap();

        // Read back via the secondary index — must work.
        let read = dst.begin_read(None).unwrap();
        let t = read.open_table::<User>("users").unwrap();
        let by_email = t
            .get_unique::<String>("by_email", &"u5@example.com".to_string())
            .unwrap();
        assert!(by_email.is_some(), "secondary index lookup failed");
        assert_eq!(by_email.unwrap().1.age, 25);
    }

    /// Multi-table snapshots: a stream with two distinct record types must
    /// roundtrip cleanly, with both tables installed atomically.
    #[test]
    fn multi_table_snapshot_roundtrips() {
        #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
        struct Account {
            balance: i64,
        }
        #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
        struct Tx {
            note: String,
        }

        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<Account>("accounts").unwrap();
        src.register_table::<Tx>("txs").unwrap();
        {
            let mut wtx = src.begin_write(None).unwrap();
            {
                let mut a = wtx.open_table::<Account>("accounts").unwrap();
                a.insert(Account { balance: 100 }).unwrap();
                a.insert(Account { balance: 250 }).unwrap();
            }
            {
                let mut t = wtx.open_table::<Tx>("txs").unwrap();
                t.insert(Tx { note: "deposit".into() }).unwrap();
                t.insert(Tx { note: "withdraw".into() }).unwrap();
                t.insert(Tx { note: "transfer".into() }).unwrap();
            }
            wtx.commit().unwrap();
        }

        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<Account>("accounts").unwrap();
        dst.register_table::<Tx>("txs").unwrap();
        dst.install_snapshot_stream(std::io::Cursor::new(&bytes), Default::default())
            .unwrap();

        let read = dst.begin_read(None).unwrap();
        let accounts = read.open_table::<Account>("accounts").unwrap();
        assert_eq!(accounts.get(1).unwrap().balance, 100);
        assert_eq!(accounts.get(2).unwrap().balance, 250);
        let txs = read.open_table::<Tx>("txs").unwrap();
        assert_eq!(txs.get(1).unwrap().note, "deposit");
        assert_eq!(txs.get(3).unwrap().note, "transfer");
    }

    /// A wire stream declaring `row_count = u64::MAX` must NOT abort the
    /// process via `Vec::with_capacity` — the install path must cap the
    /// pre-allocation hint by the remaining bytes and surface the corruption
    /// as a normal error.
    #[test]
    fn install_rejects_huge_row_count_without_aborting() {
        use ultima_db::snapshot_stream::codec::{
            FILE_FORMAT_V, FILE_MAGIC, FileHeader, TableHeader, encode_file_header,
            encode_table_header,
        };

        let mut bytes = Vec::new();
        encode_file_header(
            &FileHeader {
                format_ver: FILE_FORMAT_V,
                store_ver: 1,
                table_count: 1,
            },
            &mut bytes,
        );
        encode_table_header(
            &TableHeader {
                name: "rows".to_string(),
                record_type_id: 0,
                row_count: u64::MAX,
                indexes: Vec::new(),
            },
            &mut bytes,
        );
        // No row payload, no trailer — install must fail cleanly long before
        // allocating, and certainly without aborting.
        bytes.extend_from_slice(FILE_MAGIC);

        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<Row>("rows").unwrap();
        let res = dst.install_snapshot_stream(std::io::Cursor::new(&bytes), Default::default());
        assert!(res.is_err(), "must fail rather than abort, got Ok");
    }

    /// A custom secondary index on the destination must not panic the
    /// install path; the call must surface a clean `CustomIndexUnsupported`
    /// error so callers can react.
    #[test]
    fn install_rejects_custom_index_on_destination() {
        use ultima_db::snapshot_stream::install::InstallOptions;
        use ultima_db::{CustomIndex, SnapshotStreamError};

        #[derive(Clone)]
        struct CountIndex {
            count: usize,
        }
        impl CustomIndex<Row> for CountIndex {
            fn on_insert(&mut self, _id: u64, _record: &Row) -> ultima_db::Result<()> {
                self.count += 1;
                Ok(())
            }
            fn on_update(
                &mut self,
                _id: u64,
                _old: &Row,
                _new: &Row,
            ) -> ultima_db::Result<()> {
                Ok(())
            }
            fn on_delete(&mut self, _id: u64, _record: &Row) {
                self.count = self.count.saturating_sub(1);
            }
        }

        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<Row>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            t.insert(Row { value: 1 }).unwrap();
            tx.commit().unwrap();
        }
        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<Row>("rows").unwrap();
        {
            let mut tx = dst.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            t.define_custom_index("counter", CountIndex { count: 0 })
                .unwrap();
            tx.commit().unwrap();
        }

        let res = dst.install_snapshot_stream(
            std::io::Cursor::new(&bytes),
            InstallOptions::default(),
        );
        assert!(
            matches!(res, Err(SnapshotStreamError::CustomIndexUnsupported { .. })),
            "expected CustomIndexUnsupported, got: {res:?}"
        );
    }

    /// `OnExtra::Drop` must remove tables present in the destination snapshot
    /// but absent from the wire stream, matching Raft `InstallSnapshot`
    /// semantics.
    #[test]
    fn install_drop_extras_replaces_destination_state() {
        use ultima_db::snapshot_stream::install::{InstallOptions, OnExtra};

        #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
        struct A {
            v: u64,
        }
        #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
        struct B {
            v: u64,
        }

        // Source has only table "a".
        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<A>("a").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<A>("a").unwrap();
            t.insert(A { v: 1 }).unwrap();
            tx.commit().unwrap();
        }
        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        // Destination has both "a" and a stray "b" populated.
        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<A>("a").unwrap();
        dst.register_table::<B>("b").unwrap();
        {
            let mut tx = dst.begin_write(None).unwrap();
            let mut ta = tx.open_table::<A>("a").unwrap();
            ta.insert(A { v: 99 }).unwrap();
            drop(ta);
            let mut tb = tx.open_table::<B>("b").unwrap();
            tb.insert(B { v: 7 }).unwrap();
            drop(tb);
            tx.commit().unwrap();
        }

        dst.install_snapshot_stream(
            std::io::Cursor::new(&bytes),
            InstallOptions {
                on_extra_tables: OnExtra::Drop,
                ..Default::default()
            },
        )
        .unwrap();

        let read = dst.begin_read(None).unwrap();
        // "a" replaced by source state.
        let ta = read.open_table::<A>("a").unwrap();
        assert_eq!(ta.get(1), Some(&A { v: 1 }));
        // "b" must be absent from the new snapshot.
        let tb = read.open_table::<B>("b");
        assert!(tb.is_err(), "extra table 'b' must be dropped");
    }

    /// Default `OnExtra::Keep` preserves dst-only tables (backwards-compat
    /// with pre-fix behaviour).
    #[test]
    fn install_keep_extras_preserves_destination_state() {
        #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
        struct A {
            v: u64,
        }
        #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
        struct B {
            v: u64,
        }

        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<A>("a").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<A>("a").unwrap();
            t.insert(A { v: 1 }).unwrap();
            tx.commit().unwrap();
        }
        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<A>("a").unwrap();
        dst.register_table::<B>("b").unwrap();
        {
            let mut tx = dst.begin_write(None).unwrap();
            let mut tb = tx.open_table::<B>("b").unwrap();
            tb.insert(B { v: 7 }).unwrap();
            tx.commit().unwrap();
        }

        dst.install_snapshot_stream(std::io::Cursor::new(&bytes), Default::default())
            .unwrap();

        let read = dst.begin_read(None).unwrap();
        let tb = read.open_table::<B>("b").unwrap();
        assert_eq!(tb.get(1), Some(&B { v: 7 }), "extras must survive Keep");
    }

    /// `snapshot_stream` for a missing version must surface the dedicated
    /// `VersionNotFound` variant rather than a misleading `BulkLoad` wrap.
    #[test]
    fn snapshot_stream_missing_version_returns_version_not_found() {
        use ultima_db::SnapshotStreamError;

        let store = Store::new(StoreConfig::default()).unwrap();
        store.register_table::<Row>("rows").unwrap();
        let res = store.snapshot_stream(Some(999));
        assert!(
            matches!(&res, Err(SnapshotStreamError::VersionNotFound(999))),
            "expected VersionNotFound(999)"
        );
        // ensure we don't accidentally make the result usable downstream
        drop(res);
    }

    /// Invalid UTF-8 bytes inside a table-name length region must report the
    /// dedicated `Malformed` error variant, not the misleading `Truncated`
    /// (the bytes ARE present, they just aren't valid UTF-8).
    #[test]
    fn malformed_utf8_in_table_name_reports_malformed() {
        use ultima_db::SnapshotStreamError;
        use ultima_db::snapshot_stream::codec::{
            FILE_FORMAT_V, FILE_MAGIC, FileHeader, encode_file_header,
        };

        let mut bytes = Vec::new();
        encode_file_header(
            &FileHeader {
                format_ver: FILE_FORMAT_V,
                store_ver: 1,
                table_count: 1,
            },
            &mut bytes,
        );
        // Hand-roll a table header with invalid UTF-8 in the name slot.
        let bad_name: [u8; 4] = [0xFF, 0xFE, 0xFD, 0xFC];
        bytes.extend_from_slice(&(bad_name.len() as u16).to_le_bytes());
        bytes.extend_from_slice(&bad_name);
        // Rest doesn't matter — decode bails on the UTF-8 error.
        bytes.extend_from_slice(&[0u8; 18]);
        bytes.extend_from_slice(FILE_MAGIC);

        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<Row>("rows").unwrap();
        let res = dst.install_snapshot_stream(std::io::Cursor::new(&bytes), Default::default());
        assert!(
            matches!(res, Err(SnapshotStreamError::Malformed(_))),
            "expected Malformed for bad UTF-8, got: {res:?}"
        );
    }
}
