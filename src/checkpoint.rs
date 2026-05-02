// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Checkpoint serialization and deserialization.
//!
//! A checkpoint is a full serialized snapshot of all tables at a specific version.
//! Used for fast recovery in both Standalone and SMR modes.
//!
//! File format:
//! ```text
//! [magic: 4 bytes "ULDB"]
//! [format_version: u32]
//! [snapshot_version: u64]
//! [num_tables: u32]
//! for each table:
//!     [name_len: u32][name: bytes]
//!     [data_len: u64][serialized table data: bytes]
//! [crc32: u32]
//! ```

#![allow(dead_code)]

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::registry::TableRegistry;
use crate::store::Snapshot;
use crate::wal::crc32;
use crate::{Error, Result};

const MAGIC: &[u8; 4] = b"ULDB";
const FORMAT_VERSION: u32 = 1;

/// Serialize a snapshot to bytes using the type registry.
fn serialize_snapshot(
    snapshot: &Snapshot,
    registry: &TableRegistry,
) -> Result<Vec<u8>> {
    let config = bincode::config::standard();
    let mut buf = Vec::new();

    // Header
    buf.extend_from_slice(MAGIC);
    bincode::encode_into_std_write(FORMAT_VERSION, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    bincode::encode_into_std_write(snapshot.version, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    // Only serialize tables that are registered in the registry.
    let registered_tables: Vec<(
        &String,
        &std::sync::Arc<dyn crate::table::MergeableTable>,
    )> = snapshot
        .tables
        .iter()
        .filter(|(name, _)| registry.contains(name))
        .collect();

    bincode::encode_into_std_write(registered_tables.len() as u32, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    for (name, table_any) in registered_tables {
        let info = registry.get(name)
            .ok_or_else(|| Error::TableNotRegistered(name.clone()))?;

        // Table name
        bincode::encode_into_std_write(name.as_str(), &mut buf, config)
            .map_err(|e| Error::Persistence(e.to_string()))?;

        // Serialize table data — serialize_table takes &dyn Any, so upcast
        // from &dyn MergeableTable via as_any().
        let table_bytes = (info.serialize_table)(table_any.as_ref().as_any())?;
        bincode::encode_into_std_write(table_bytes.len() as u64, &mut buf, config)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        buf.extend_from_slice(&table_bytes);
    }

    // Append CRC32 of everything before it
    let checksum = crc32(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());

    Ok(buf)
}

/// Deserialize a snapshot from bytes using the type registry.
fn deserialize_snapshot(
    data: &[u8],
    registry: &TableRegistry,
) -> Result<Snapshot> {
    // Minimum: 4 (magic) + 1 (format_version varint) + 1 (version varint)
    //        + 1 (num_tables varint) + 4 (crc32) = 11 bytes
    if data.len() < 4 + 1 + 1 + 1 + 4 {
        return Err(Error::CheckpointCorrupted("file too short".into()));
    }

    // Verify CRC (last 4 bytes)
    let crc_offset = data.len() - 4;
    let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());
    let computed_crc = crc32(&data[..crc_offset]);
    if stored_crc != computed_crc {
        return Err(Error::CheckpointCorrupted("CRC mismatch".into()));
    }

    let payload = &data[..crc_offset];
    let config = bincode::config::standard();
    let mut offset = 0;

    // Magic
    if &payload[offset..offset + 4] != MAGIC {
        return Err(Error::CheckpointCorrupted("bad magic".into()));
    }
    offset += 4;

    // Format version
    let (fmt_version, read): (u32, _) = bincode::decode_from_slice(&payload[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;
    if fmt_version != FORMAT_VERSION {
        return Err(Error::CheckpointCorrupted(format!(
            "unsupported format version: {fmt_version}"
        )));
    }

    // Snapshot version
    let (version, read): (u64, _) = bincode::decode_from_slice(&payload[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;

    // Number of tables
    let (num_tables, read): (u32, _) = bincode::decode_from_slice(&payload[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;

    let mut tables = std::collections::BTreeMap::new();

    for _ in 0..num_tables {
        // Table name
        let (name, read): (String, _) = bincode::decode_from_slice(&payload[offset..], config)
            .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
        offset += read;

        // Table data length
        let (data_len, read): (u64, _) = bincode::decode_from_slice(&payload[offset..], config)
            .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
        offset += read;

        let end = offset + data_len as usize;
        if end > payload.len() {
            return Err(Error::CheckpointCorrupted("truncated table data".into()));
        }
        let table_bytes = &payload[offset..end];
        offset = end;

        let info = registry.get(&name)
            .ok_or_else(|| Error::TableNotRegistered(name.clone()))?;
        let table_any = (info.deserialize_table)(table_bytes)?;
        tables.insert(name, std::sync::Arc::from(table_any));
    }

    Ok(Snapshot { version, tables })
}

// ---------------------------------------------------------------------------
// Checkpoint file management
// ---------------------------------------------------------------------------

fn checkpoint_filename(version: u64) -> String {
    format!("checkpoint_{version}.bin")
}

/// Find the latest checkpoint file in a directory.
pub(crate) fn find_latest_checkpoint(dir: &Path) -> Result<Option<PathBuf>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(Error::Persistence(e.to_string())),
    };

    let mut best: Option<(u64, PathBuf)> = None;
    for entry in entries {
        let entry = entry.map_err(|e| Error::Persistence(e.to_string()))?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(rest) = name_str.strip_prefix("checkpoint_")
            && let Some(ver_str) = rest.strip_suffix(".bin")
            && let Ok(ver) = ver_str.parse::<u64>()
            && best.as_ref().is_none_or(|(v, _)| ver > *v)
        {
            best = Some((ver, entry.path()));
        }
    }

    Ok(best.map(|(_, path)| path))
}

/// Write a checkpoint to disk.
///
/// Uses write-to-temp + atomic rename to avoid leaving a corrupt checkpoint
/// file if the process crashes mid-write.
pub(crate) fn write_checkpoint(
    dir: &Path,
    snapshot: &Snapshot,
    registry: &TableRegistry,
) -> Result<u64> {
    std::fs::create_dir_all(dir)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    let data = serialize_snapshot(snapshot, registry)?;
    let final_path = dir.join(checkpoint_filename(snapshot.version));
    let tmp_path = dir.join(format!("{}.tmp", checkpoint_filename(snapshot.version)));

    let mut file = File::create(&tmp_path)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    file.write_all(&data)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    file.sync_all()
        .map_err(|e| Error::Persistence(e.to_string()))?;
    drop(file);
    std::fs::rename(&tmp_path, &final_path)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    Ok(snapshot.version)
}

/// Load a checkpoint from a file.
pub(crate) fn load_checkpoint(
    path: &Path,
    registry: &TableRegistry,
) -> Result<Snapshot> {
    let mut file = File::open(path)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    deserialize_snapshot(&data, registry)
}

/// Delete all checkpoint files except the one for `keep_version`.
pub(crate) fn cleanup_old_checkpoints(dir: &Path, keep_version: u64) -> Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };

    for entry in entries {
        let entry = entry.map_err(|e| Error::Persistence(e.to_string()))?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with("checkpoint_")
            && name_str.ends_with(".bin")
            && name_str != checkpoint_filename(keep_version)
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::TableRegistry;
    use crate::table::Table;

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct User {
        name: String,
        age: u32,
    }

    fn make_snapshot_with_users() -> (Snapshot, TableRegistry) {
        let mut reg = TableRegistry::default();
        reg.register::<User>("users").unwrap();

        let mut table = Table::<User>::new();
        table.insert(User { name: "Alice".into(), age: 30 }).unwrap();
        table.insert(User { name: "Bob".into(), age: 25 }).unwrap();

        let mut tables = std::collections::BTreeMap::new();
        tables.insert("users".to_string(), std::sync::Arc::new(table) as std::sync::Arc<dyn crate::table::MergeableTable>);

        let snapshot = Snapshot { version: 42, tables };
        (snapshot, reg)
    }

    #[test]
    fn checkpoint_serialize_deserialize_roundtrip() {
        let (snapshot, reg) = make_snapshot_with_users();
        let data = serialize_snapshot(&snapshot, &reg).unwrap();
        let recovered = deserialize_snapshot(&data, &reg).unwrap();
        assert_eq!(recovered.version, 42);
        let table = recovered.tables.get("users").unwrap()
            .as_any().downcast_ref::<Table<User>>().unwrap();
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(1).unwrap(), &User { name: "Alice".into(), age: 30 });
        assert_eq!(table.get(2).unwrap(), &User { name: "Bob".into(), age: 25 });
    }

    #[test]
    fn checkpoint_crc_corruption_detected() {
        let (snapshot, reg) = make_snapshot_with_users();
        let mut data = serialize_snapshot(&snapshot, &reg).unwrap();
        data[10] ^= 0xFF; // corrupt a byte
        assert!(matches!(deserialize_snapshot(&data, &reg), Err(Error::CheckpointCorrupted(_))));
    }

    #[test]
    fn checkpoint_file_write_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let (snapshot, reg) = make_snapshot_with_users();

        write_checkpoint(dir.path(), &snapshot, &reg).unwrap();
        let path = find_latest_checkpoint(dir.path()).unwrap().unwrap();
        let recovered = load_checkpoint(&path, &reg).unwrap();
        assert_eq!(recovered.version, 42);
    }

    #[test]
    fn find_latest_checkpoint_picks_highest_version() {
        let dir = tempfile::tempdir().unwrap();
        let (snapshot, reg) = make_snapshot_with_users();

        // Write checkpoints at versions 10, 42, 5
        let mut snap10 = snapshot.clone();
        snap10.version = 10;
        write_checkpoint(dir.path(), &snap10, &reg).unwrap();

        write_checkpoint(dir.path(), &snapshot, &reg).unwrap(); // version 42

        let mut snap5 = snapshot.clone();
        snap5.version = 5;
        write_checkpoint(dir.path(), &snap5, &reg).unwrap();

        let latest = find_latest_checkpoint(dir.path()).unwrap().unwrap();
        assert!(latest.to_string_lossy().contains("checkpoint_42"));
    }

    #[test]
    fn cleanup_old_checkpoints_keeps_only_latest() {
        let dir = tempfile::tempdir().unwrap();
        let (snapshot, reg) = make_snapshot_with_users();

        let mut snap10 = snapshot.clone();
        snap10.version = 10;
        write_checkpoint(dir.path(), &snap10, &reg).unwrap();
        write_checkpoint(dir.path(), &snapshot, &reg).unwrap(); // version 42

        cleanup_old_checkpoints(dir.path(), 42).unwrap();

        let files: Vec<_> = std::fs::read_dir(dir.path()).unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(files.len(), 1);
        assert!(files[0].contains("checkpoint_42"));
    }

    #[test]
    fn deserialize_too_short_errors() {
        let reg = TableRegistry::default();
        let data = vec![0u8; 5]; // too short for any valid checkpoint
        let result = deserialize_snapshot(&data, &reg);
        assert!(matches!(result, Err(Error::CheckpointCorrupted(ref msg)) if msg.contains("too short")));
    }

    #[test]
    fn deserialize_bad_magic_errors() {
        let config = bincode::config::standard();
        let reg = TableRegistry::default();
        // Build a payload with wrong magic but valid structure and CRC
        let mut data = Vec::new();
        data.extend_from_slice(b"XXXX"); // bad magic
        bincode::encode_into_std_write(FORMAT_VERSION, &mut data, config).unwrap();
        bincode::encode_into_std_write(1u64, &mut data, config).unwrap();
        bincode::encode_into_std_write(0u32, &mut data, config).unwrap();
        let checksum = crc32(&data);
        data.extend_from_slice(&checksum.to_le_bytes());
        let result = deserialize_snapshot(&data, &reg);
        assert!(matches!(result, Err(Error::CheckpointCorrupted(ref msg)) if msg.contains("bad magic")));
    }

    #[test]
    fn deserialize_unsupported_format_version_errors() {
        let config = bincode::config::standard();
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        bincode::encode_into_std_write(999u32, &mut data, config).unwrap(); // bad format version
        bincode::encode_into_std_write(1u64, &mut data, config).unwrap(); // snapshot version
        bincode::encode_into_std_write(0u32, &mut data, config).unwrap(); // 0 tables
        let checksum = crc32(&data);
        data.extend_from_slice(&checksum.to_le_bytes());

        let reg = TableRegistry::default();
        let result = deserialize_snapshot(&data, &reg);
        assert!(
            matches!(result, Err(Error::CheckpointCorrupted(ref msg)) if msg.contains("unsupported format")),
        );
    }

    #[test]
    fn deserialize_truncated_table_data_errors() {
        let config = bincode::config::standard();
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        bincode::encode_into_std_write(FORMAT_VERSION, &mut data, config).unwrap();
        bincode::encode_into_std_write(1u64, &mut data, config).unwrap(); // version
        bincode::encode_into_std_write(1u32, &mut data, config).unwrap(); // 1 table

        // Table name
        bincode::encode_into_std_write("users", &mut data, config).unwrap();
        // Claim data_len = 9999 but don't write that much data
        bincode::encode_into_std_write(9999u64, &mut data, config).unwrap();

        let checksum = crc32(&data);
        data.extend_from_slice(&checksum.to_le_bytes());

        let mut reg = TableRegistry::default();
        reg.register::<User>("users").unwrap();
        let result = deserialize_snapshot(&data, &reg);
        assert!(matches!(result, Err(Error::CheckpointCorrupted(ref msg)) if msg.contains("truncated")));
    }

    #[test]
    fn deserialize_unregistered_table_errors() {
        // Serialize a valid snapshot with a "users" table
        let (snapshot, reg) = make_snapshot_with_users();
        let data = serialize_snapshot(&snapshot, &reg).unwrap();

        // Try to deserialize with an empty registry (no "users" registered)
        let empty_reg = TableRegistry::default();
        let result = deserialize_snapshot(&data, &empty_reg);
        assert!(matches!(result, Err(Error::TableNotRegistered(ref name)) if name == "users"));
    }

    #[test]
    fn find_latest_checkpoint_nonexistent_dir() {
        let result = find_latest_checkpoint(std::path::Path::new("/nonexistent/path/that/does/not/exist")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn find_latest_checkpoint_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let result = find_latest_checkpoint(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn find_latest_checkpoint_ignores_non_checkpoint_files() {
        let dir = tempfile::tempdir().unwrap();
        // Create non-checkpoint files
        std::fs::write(dir.path().join("wal.bin"), b"data").unwrap();
        std::fs::write(dir.path().join("random.txt"), b"data").unwrap();
        let result = find_latest_checkpoint(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn cleanup_old_checkpoints_nonexistent_dir() {
        // Should not error on missing directory
        cleanup_old_checkpoints(std::path::Path::new("/nonexistent/dir"), 1).unwrap();
    }

    #[test]
    fn empty_snapshot_roundtrip() {
        let reg = TableRegistry::default();
        let snapshot = Snapshot { version: 1, tables: std::collections::BTreeMap::new() };
        let data = serialize_snapshot(&snapshot, &reg).unwrap();
        let recovered = deserialize_snapshot(&data, &reg).unwrap();
        assert_eq!(recovered.version, 1);
        assert!(recovered.tables.is_empty());
    }

    #[test]
    fn multi_table_snapshot_roundtrip() {
        #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Order { item: String, qty: u32 }

        let mut reg = TableRegistry::default();
        reg.register::<User>("users").unwrap();
        reg.register::<Order>("orders").unwrap();

        let mut user_table = Table::<User>::new();
        user_table.insert(User { name: "Alice".into(), age: 30 }).unwrap();

        let mut order_table = Table::<Order>::new();
        order_table.insert(Order { item: "Widget".into(), qty: 5 }).unwrap();
        order_table.insert(Order { item: "Gadget".into(), qty: 3 }).unwrap();

        let mut tables = std::collections::BTreeMap::new();
        tables.insert("users".to_string(), std::sync::Arc::new(user_table) as std::sync::Arc<dyn crate::table::MergeableTable>);
        tables.insert("orders".to_string(), std::sync::Arc::new(order_table) as std::sync::Arc<dyn crate::table::MergeableTable>);

        let snapshot = Snapshot { version: 7, tables };
        let data = serialize_snapshot(&snapshot, &reg).unwrap();
        let recovered = deserialize_snapshot(&data, &reg).unwrap();

        assert_eq!(recovered.version, 7);
        assert_eq!(recovered.tables.len(), 2);

        let users = recovered.tables.get("users").unwrap()
            .as_any().downcast_ref::<Table<User>>().unwrap();
        assert_eq!(users.len(), 1);

        let orders = recovered.tables.get("orders").unwrap()
            .as_any().downcast_ref::<Table<Order>>().unwrap();
        assert_eq!(orders.len(), 2);
        assert_eq!(orders.get(1).unwrap().item, "Widget");
        assert_eq!(orders.get(2).unwrap().qty, 3);
    }

    #[test]
    fn snapshot_with_unregistered_table_skips_it() {
        // Snapshot has "users" and "logs", but only "users" is registered
        let mut reg = TableRegistry::default();
        reg.register::<User>("users").unwrap();

        let mut user_table = Table::<User>::new();
        user_table.insert(User { name: "Alice".into(), age: 30 }).unwrap();

        let log_table = Table::<String>::new();

        let mut tables = std::collections::BTreeMap::new();
        tables.insert("users".to_string(), std::sync::Arc::new(user_table) as std::sync::Arc<dyn crate::table::MergeableTable>);
        tables.insert("logs".to_string(), std::sync::Arc::new(log_table) as std::sync::Arc<dyn crate::table::MergeableTable>);

        let snapshot = Snapshot { version: 1, tables };
        let data = serialize_snapshot(&snapshot, &reg).unwrap();
        let recovered = deserialize_snapshot(&data, &reg).unwrap();

        // Only "users" should be in the recovered snapshot
        assert_eq!(recovered.tables.len(), 1);
        assert!(recovered.tables.contains_key("users"));
    }

    #[test]
    fn write_checkpoint_creates_tmp_then_renames() {
        let dir = tempfile::tempdir().unwrap();
        let (snapshot, reg) = make_snapshot_with_users();

        write_checkpoint(dir.path(), &snapshot, &reg).unwrap();

        // Final file should exist, tmp should not
        let final_path = dir.path().join("checkpoint_42.bin");
        let tmp_path = dir.path().join("checkpoint_42.bin.tmp");
        assert!(final_path.exists());
        assert!(!tmp_path.exists());
    }
}
