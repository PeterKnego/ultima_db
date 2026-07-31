// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Integration tests for the snapshot_stream build and install paths
//! (Tasks 3 and 4).
//!
//! These tests require the `persistence` feature because `Store::snapshot_stream`,
//! `Store::install_snapshot_stream`, and `Store::register_table` are gated on it.

#[cfg(feature = "persistence")]
mod common;

#[cfg(feature = "persistence")]
mod tests {
    use std::io::Read;

    use serde::{Deserialize, Serialize};
    use ultima_db::snapshot_stream::codec::{
        FILE_FORMAT_V, FILE_MAGIC, decode_file_header, decode_table_header,
    };
    use ultima_db::{Store, StoreConfig};

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
    struct Row {
        value: u64,
    }

    /// The snapshot stream must start with the 8-byte magic `ULTSNAP\0`
    /// followed by the format version (u16 LE, currently 2), the store version
    /// (u64 LE), and the table count (u32 LE).
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
        assert!(
            buf.len() >= 42,
            "stream too short for empty snapshot: {} bytes",
            buf.len()
        );
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

        let (_, n) = decode_file_header(&buf).unwrap();
        let (header, _) = decode_table_header(&buf[n..]).unwrap();
        assert_eq!(header.row_count, 42, "table header row_count mismatch");
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
            .install_snapshot_stream(std::io::Cursor::new(&bytes), Default::default())
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

    /// `InstallOptions::commit_version = Some(v)` lands the installed snapshot
    /// at exactly version `v` (the SMR position-as-version hook), instead of
    /// `latest_version + 1`, and keeps the auto-assign counter ahead of it.
    #[test]
    fn install_honors_commit_version() {
        use ultima_db::snapshot_stream::install::InstallOptions;

        // Source snapshot with 5 rows.
        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<Row>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=5u64 {
                t.insert(Row { value: i * 100 }).unwrap();
            }
            tx.commit().unwrap();
        }
        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        // Destination advanced to version 7 by an ordinary pinned write.
        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<Row>("rows").unwrap();
        {
            let mut tx = dst.begin_write(Some(7)).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            t.insert(Row { value: 9 }).unwrap();
            tx.commit().unwrap();
        }
        assert_eq!(dst.latest_version(), 7);

        // Install pinned to version 4242 (a sparse byte position).
        let installed = dst
            .install_snapshot_stream(
                std::io::Cursor::new(&bytes),
                InstallOptions {
                    commit_version: Some(4242),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(installed, 4242, "install must land at the pinned version");
        assert_eq!(dst.latest_version(), 4242);

        // The rows came from the source snapshot.
        {
            let read = dst.begin_read(None).unwrap();
            let t = read.open_table::<Row>("rows").unwrap();
            for i in 1..=5u64 {
                assert_eq!(t.get(i).expect("row must exist").value, i * 100);
            }
        }

        // A later auto-assigned write does not collide with 4242.
        {
            let mut tx = dst.begin_write(None).unwrap();
            tx.open_table::<Row>("rows").unwrap();
            tx.commit().unwrap();
        }
        assert!(dst.latest_version() > 4242, "next write must exceed 4242");
    }

    /// A `commit_version` at or below the current `latest_version` is refused
    /// (the version space must strictly increase).
    #[test]
    fn install_rejects_commit_version_not_above_latest() {
        use ultima_db::snapshot_stream::install::InstallOptions;
        use ultima_db::snapshot_stream::SnapshotStreamError;

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
            let mut tx = dst.begin_write(Some(7)).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            t.insert(Row { value: 9 }).unwrap();
            tx.commit().unwrap();
        }
        assert_eq!(dst.latest_version(), 7);

        let err = dst.install_snapshot_stream(
            std::io::Cursor::new(&bytes),
            InstallOptions {
                commit_version: Some(3),
                ..Default::default()
            },
        );
        assert!(
            matches!(
                err,
                Err(SnapshotStreamError::InvalidCommitVersion { requested: 3, latest: 7 })
            ),
            "expected InvalidCommitVersion, got {err:?}"
        );
        // Destination unchanged.
        assert_eq!(dst.latest_version(), 7);
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
        let res =
            dst.install_snapshot_stream(std::io::Cursor::new(&bytes[..half]), Default::default());
        assert!(res.is_err(), "truncated stream must fail");

        // Destination must remain empty — the table should not exist in the
        // snapshot (open_table returns TableNotFound for an unwritten table).
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

        // Flip a byte inside the first row's *value* payload. Aiming at the
        // middle of the buffer would land on a length field as often as not,
        // and a corrupt length is caught as `Truncated` before the CRC is
        // ever recomputed — a different (also correct) rejection, but not the
        // one under test here.
        let (_, file_n) = decode_file_header(&bytes).unwrap();
        let (_, table_n) = decode_table_header(&bytes[file_n..]).unwrap();
        let row_start = file_n + table_n;
        let key_len =
            u32::from_le_bytes(bytes[row_start..row_start + 4].try_into().unwrap()) as usize;
        let value_start = row_start + 4 + key_len + 4;
        bytes[value_start] ^= 0xFF;

        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<Row>("rows").unwrap();
        let res = dst.install_snapshot_stream(std::io::Cursor::new(&bytes), Default::default());
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
        assert!(
            res.is_ok(),
            "Drop policy must succeed even with unknown table"
        );
    }

    /// Unknown table with OnUnknown::Error must fail.
    #[test]
    fn unknown_table_error_fails() {
        use ultima_db::SnapshotStreamError;
        use ultima_db::snapshot_stream::install::{InstallOptions, OnUnknown};

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

        let (_, n) = decode_file_header(&buf).unwrap();
        let (header, _) = decode_table_header(&buf[n..]).unwrap();
        assert_eq!(header.row_count, 5, "streaming v1 should have 5 rows");
    }

    // ── Task 5 tests: list_checkpoints + open_checkpoint_reader ─────────────

    /// `list_checkpoints` returns the versions of all on-disk checkpoint files,
    /// in ascending order.
    #[test]
    fn list_checkpoints_returns_versions() {
        use ultima_db::Persistence;

        let dir = crate::common::test_scratch::scratch_dir();
        let cfg = StoreConfig::builder()
            .persistence(Persistence::smr(dir.path().to_path_buf()))
            .build();
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
        assert!(
            cps.contains(&v2),
            "v2={v2} must be in checkpoint list: {cps:?}"
        );
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

        let dir = crate::common::test_scratch::scratch_dir();
        let cfg = StoreConfig::builder()
            .persistence(Persistence::smr(dir.path().to_path_buf()))
            .build();
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
                t.insert(Tx {
                    note: "deposit".into(),
                })
                .unwrap();
                t.insert(Tx {
                    note: "withdraw".into(),
                })
                .unwrap();
                t.insert(Tx {
                    note: "transfer".into(),
                })
                .unwrap();
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
                key_type_id: <u64 as ultima_db::PrimaryKey>::KEY_TYPE_ID,
                key_type: std::any::type_name::<u64>().to_string(),
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
            fn on_update(&mut self, _id: u64, _old: &Row, _new: &Row) -> ultima_db::Result<()> {
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

        let res =
            dst.install_snapshot_stream(std::io::Cursor::new(&bytes), InstallOptions::default());
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

    /// A `String`-keyed table must survive a full stream → install round
    /// trip. This is the case that proves the format is genuinely key-generic
    /// rather than "u64 with wider framing": `String` keys are
    /// variable-length, so the row framing does real work, and they are
    /// ordered by their bytes, so the receiver's strict-ascent check is
    /// exercised against an encoding that is not a fixed-width integer.
    #[test]
    fn round_trips_a_string_keyed_table() {
        // Deliberately spans encoded lengths 1..=9 and includes a pair that
        // shares a prefix ("b", "bb") — the case a fixed-width key format
        // cannot represent and a non-order-preserving encoding gets wrong.
        let keys = ["", "a", "ab", "b", "bb", "zzzzzzzzz"];

        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table_keyed::<Row, String>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table_keyed::<Row, String>("rows").unwrap();
            // Inserted out of order on purpose: the table stores them in key
            // order, and that is the order the stream must emit.
            for (i, k) in [3usize, 0, 5, 1, 4, 2].into_iter().enumerate() {
                t.put(keys[k].to_string(), Row { value: i as u64 })
                    .unwrap();
            }
            tx.commit().unwrap();
        }

        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        // The invariant the type-erased install path rests on: the wire's
        // keys are ascending as *raw bytes*, which is only equivalent to
        // ascending as `K` because `PrimaryKey::encode` is order-preserving.
        // Assert it on the bytes themselves, where the receiver sees them.
        let (_, file_n) = decode_file_header(&bytes).unwrap();
        let (header, table_n) = decode_table_header(&bytes[file_n..]).unwrap();
        let mut p = file_n + table_n;
        let mut wire_keys: Vec<Vec<u8>> = Vec::new();
        for _ in 0..header.row_count {
            let key_len = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            wire_keys.push(bytes[p..p + key_len].to_vec());
            p += key_len;
            let val_len = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            p += 4 + val_len;
        }
        assert!(
            wire_keys.windows(2).all(|w| w[0] < w[1]),
            "encoded keys must be strictly ascending on the wire: {wire_keys:?}"
        );
        let expected_bytes: Vec<Vec<u8>> = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
        assert_eq!(
            wire_keys, expected_bytes,
            "byte order must agree with key order"
        );

        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table_keyed::<Row, String>("rows").unwrap();
        dst.install_snapshot_stream(std::io::Cursor::new(&bytes), Default::default())
            .unwrap();

        let rtx = dst.begin_read(None).unwrap();
        let t = rtx.open_table_keyed::<Row, String>("rows").unwrap();
        assert_eq!(t.len(), keys.len());
        let src_rtx = src.begin_read(None).unwrap();
        let src_t = src_rtx.open_table_keyed::<Row, String>("rows").unwrap();
        for k in keys {
            assert_eq!(
                t.get(k.to_string()),
                src_t.get(k.to_string()),
                "row {k:?} did not survive the round trip"
            );
        }
    }

    /// The keys the wire carries are opaque bytes, and several key types
    /// accept the same bytes — the eight bytes of `1u64` are also a valid
    /// (NUL-padded) `String`, so `String::decode` succeeds, the strict-ascent
    /// check downstream passes, and the table would install full of garbage
    /// keys with no error anywhere. The table header therefore names the key
    /// type the source encoded with, and a destination that disagrees refuses
    /// the install. Also: the destination must be left untouched.
    #[test]
    fn install_refuses_a_stream_encoded_with_a_different_key_type() {
        use ultima_db::snapshot_stream::install::InstallOptions;
        use ultima_db::{PrimaryKey, SnapshotStreamError};

        // The hazard, stated as an assertion: decoding a u64 stream key as a
        // String is not a decode *failure* that would surface on its own. It
        // silently succeeds.
        assert_eq!(
            <String as PrimaryKey>::decode(&1u64.encode()).unwrap().len(),
            8,
            "u64 key bytes decode cleanly as a String — hence the explicit guard"
        );

        // Source: an ordinary u64-keyed table with ids 1..=3.
        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<Row>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table::<Row>("rows").unwrap();
            for i in 1..=3u64 {
                t.insert(Row { value: i }).unwrap();
            }
            tx.commit().unwrap();
        }
        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        // Destination: same name, same record type, String primary key.
        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table_keyed::<Row, String>("rows").unwrap();
        {
            let mut tx = dst.begin_write(None).unwrap();
            let mut t = tx.open_table_keyed::<Row, String>("rows").unwrap();
            t.put("keep-me".to_string(), Row { value: 99 }).unwrap();
            tx.commit().unwrap();
        }
        let before = dst.begin_read(None).unwrap().version();

        let res =
            dst.install_snapshot_stream(std::io::Cursor::new(&bytes), InstallOptions::default());
        match res {
            Err(SnapshotStreamError::KeyTypeMismatch {
                table,
                stream,
                destination,
            }) => {
                assert_eq!(table, "rows");
                assert_eq!(stream, std::any::type_name::<u64>());
                assert_eq!(destination, std::any::type_name::<String>());
            }
            other => panic!("expected KeyTypeMismatch, got: {other:?}"),
        }

        // Untouched: same version, same single pre-existing row.
        let rtx = dst.begin_read(None).unwrap();
        assert_eq!(rtx.version(), before);
        let t = rtx.open_table_keyed::<Row, String>("rows").unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t.get("keep-me".to_string()), Some(&Row { value: 99 }));
    }

    /// The check is decided by `key_type_id`, not by the type *name*.
    ///
    /// The name is only a diagnostic: `std::any::type_name` is not injective
    /// across crate versions of a third-party key type, so two incompatible
    /// key types can print identically. Here the stream keeps a name the
    /// destination agrees with and carries a different `key_type_id`; the
    /// install must still refuse it.
    #[test]
    fn install_decides_the_key_type_check_on_the_id_not_the_name() {
        use ultima_db::snapshot_stream::install::InstallOptions;
        use ultima_db::SnapshotStreamError;

        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table::<Row>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            tx.open_table::<Row>("rows")
                .unwrap()
                .insert(Row { value: 1 })
                .unwrap();
            tx.commit().unwrap();
        }
        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        // Patch the header's `key_type_id` and nothing else. Layout: file
        // header (22) | name_len (2) | name | record_type_id (8) |
        // key_type_id (4) | ...
        let at = 22 + 2 + "rows".len() + 8;
        let original = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        assert_eq!(
            original,
            <u64 as ultima_db::PrimaryKey>::KEY_TYPE_ID,
            "the patch offset must land on the key type id"
        );
        bytes[at..at + 4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());

        // The name field still says `u64`, and the destination *is* `u64`.
        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<Row>("rows").unwrap();
        match dst.install_snapshot_stream(std::io::Cursor::new(&bytes), InstallOptions::default()) {
            Err(SnapshotStreamError::KeyTypeMismatch { stream, .. }) => {
                assert_eq!(
                    stream,
                    std::any::type_name::<u64>(),
                    "the name is reported as the stream sent it"
                );
            }
            other => panic!("expected KeyTypeMismatch, got: {other:?}"),
        }
    }

    /// A `String`-keyed table is emittable now — the restriction the old
    /// `NonU64Key` guard imposed at this end of the pipe is gone. What must
    /// still hold is that the emitted header names `String`, so the receiver
    /// can tell.
    #[test]
    fn snapshot_stream_emits_a_non_u64_keyed_table() {
        use ultima_db::snapshot_stream::codec::{decode_file_header, decode_table_header};

        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table_keyed::<Row, String>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table_keyed::<Row, String>("rows").unwrap();
            t.put("a".to_string(), Row { value: 1 }).unwrap();
            tx.commit().unwrap();
        }

        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .expect("emitting a String-keyed table must succeed");

        let (_, n) = decode_file_header(&bytes).unwrap();
        let (header, _) = decode_table_header(&bytes[n..]).unwrap();
        assert_eq!(header.name, "rows");
        assert_eq!(header.key_type, std::any::type_name::<String>());
        assert_eq!(header.row_count, 1);
    }

    /// The snapshot wire format and the WAL must enforce the *same* key-length
    /// cap. A key one of them accepts and the other truncates is a row that
    /// survives one durability path and is corrupted by the other.
    ///
    /// On emit the check is not defensive against a hostile caller — it is
    /// against the `u32` length prefix: an over-long key truncates its own
    /// length and produces a stream that still passes every CRC downstream.
    #[test]
    fn emit_refuses_an_over_long_key() {
        const MAX: usize = 64 * 1024;

        let src = Store::new(StoreConfig::default()).unwrap();
        src.register_table_keyed::<Row, String>("rows").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            let mut t = tx.open_table_keyed::<Row, String>("rows").unwrap();
            // One byte over the shared cap.
            t.put("k".repeat(MAX + 1), Row { value: 1 }).unwrap();
            tx.commit().unwrap();
        }

        let mut bytes = Vec::new();
        let err = src
            .snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .expect_err("an over-long key must not reach the wire");
        // `Read::read` stringifies the error (io::Error::other(e.to_string())),
        // so the message is the only thing observable here; the typed
        // `KeyTooLong` variant is asserted on the install side below.
        let msg = err.to_string();
        assert!(
            msg.contains(&MAX.to_string()) && msg.contains(&(MAX + 1).to_string()),
            "the error must name both the key length and the cap, got: {msg}"
        );
        assert!(
            !bytes.windows(64).any(|w| w == [b'k'; 64]),
            "no part of the over-long key may reach the wire"
        );

        // Exactly at the cap is fine — the bound is inclusive, as the WAL's is.
        let ok_src = Store::new(StoreConfig::default()).unwrap();
        ok_src.register_table_keyed::<Row, String>("rows").unwrap();
        {
            let mut tx = ok_src.begin_write(None).unwrap();
            let mut t = tx.open_table_keyed::<Row, String>("rows").unwrap();
            t.put("k".repeat(MAX), Row { value: 1 }).unwrap();
            tx.commit().unwrap();
        }
        let mut ok_bytes = Vec::new();
        ok_src
            .snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut ok_bytes)
            .expect("a key exactly at the cap must be emittable");
    }

    /// The install side of the same cap. The length comes off untrusted bytes,
    /// so it must be rejected *before* the key is allocated — a corrupt
    /// `key_len` near `u32::MAX` must not drive a multi-GB allocation.
    #[test]
    fn install_refuses_an_over_long_key_length() {
        use ultima_db::SnapshotStreamError;
        use ultima_db::snapshot_stream::codec::{
            FileHeader, TableHeader, encode_file_header, encode_table_header,
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
                key_type_id: <u64 as ultima_db::PrimaryKey>::KEY_TYPE_ID,
                key_type: std::any::type_name::<u64>().to_string(),
                row_count: 1,
                indexes: Vec::new(),
            },
            &mut bytes,
        );
        // A row claiming a ~4 GB key, with no payload behind it.
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        bytes.extend_from_slice(FILE_MAGIC);

        let dst = Store::new(StoreConfig::default()).unwrap();
        dst.register_table::<Row>("rows").unwrap();
        let res = dst.install_snapshot_stream(std::io::Cursor::new(&bytes), Default::default());
        assert!(
            matches!(res, Err(SnapshotStreamError::KeyTooLong { .. })),
            "expected KeyTooLong, got {res:?}"
        );
    }
}
