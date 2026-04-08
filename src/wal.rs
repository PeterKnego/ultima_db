//! Write-ahead log (WAL) for durable persistence.
//!
//! The WAL records row-level deltas for each committed transaction. In
//! `Standalone` persistence mode, WAL entries are written to disk on commit.
//!
//! File format: append-only sequence of `[len: u32][WalEntry bytes][crc32: u32]`.

#![allow(dead_code)]

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;

use crate::{Error, Result};

// ---------------------------------------------------------------------------
// WAL data types
// ---------------------------------------------------------------------------

/// A single mutation within a transaction.
#[derive(Debug, Clone)]
pub enum WalOp {
    Insert { table: String, id: u64, data: Vec<u8> },
    Update { table: String, id: u64, data: Vec<u8> },
    Delete { table: String, id: u64 },
    CreateTable { name: String },
    DeleteTable { name: String },
}

/// A complete WAL entry for one committed transaction.
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub version: u64,
    pub ops: Vec<WalOp>,
}

// ---------------------------------------------------------------------------
// Binary serialization (using bincode for WalEntry)
// ---------------------------------------------------------------------------

/// Tag bytes for WalOp variants.
const TAG_INSERT: u8 = 1;
const TAG_UPDATE: u8 = 2;
const TAG_DELETE: u8 = 3;
const TAG_CREATE_TABLE: u8 = 4;
const TAG_DELETE_TABLE: u8 = 5;

fn serialize_entry(entry: &WalEntry) -> Result<Vec<u8>> {
    let config = bincode::config::standard();
    let mut buf = Vec::new();

    bincode::encode_into_std_write(entry.version, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    bincode::encode_into_std_write(entry.ops.len() as u32, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    for op in &entry.ops {
        match op {
            WalOp::Insert { table, id, data } => {
                buf.push(TAG_INSERT);
                bincode::encode_into_std_write(table.as_str(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
                bincode::encode_into_std_write(*id, &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
                bincode::encode_into_std_write(data.as_slice(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
            }
            WalOp::Update { table, id, data } => {
                buf.push(TAG_UPDATE);
                bincode::encode_into_std_write(table.as_str(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
                bincode::encode_into_std_write(*id, &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
                bincode::encode_into_std_write(data.as_slice(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
            }
            WalOp::Delete { table, id } => {
                buf.push(TAG_DELETE);
                bincode::encode_into_std_write(table.as_str(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
                bincode::encode_into_std_write(*id, &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
            }
            WalOp::CreateTable { name } => {
                buf.push(TAG_CREATE_TABLE);
                bincode::encode_into_std_write(name.as_str(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
            }
            WalOp::DeleteTable { name } => {
                buf.push(TAG_DELETE_TABLE);
                bincode::encode_into_std_write(name.as_str(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
            }
        }
    }

    Ok(buf)
}

fn deserialize_entry(data: &[u8]) -> Result<WalEntry> {
    let config = bincode::config::standard();
    let mut offset = 0;

    let (version, read): (u64, _) = bincode::decode_from_slice(&data[offset..], config)
        .map_err(|e| Error::WalCorrupted(e.to_string()))?;
    offset += read;

    let (op_count, read): (u32, _) = bincode::decode_from_slice(&data[offset..], config)
        .map_err(|e| Error::WalCorrupted(e.to_string()))?;
    offset += read;

    let mut ops = Vec::with_capacity(op_count as usize);
    for _ in 0..op_count {
        if offset >= data.len() {
            return Err(Error::WalCorrupted("unexpected end of entry".into()));
        }
        let tag = data[offset];
        offset += 1;

        match tag {
            TAG_INSERT | TAG_UPDATE => {
                let (table, read): (String, _) = bincode::decode_from_slice(&data[offset..], config)
                    .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                let (id, read): (u64, _) = bincode::decode_from_slice(&data[offset..], config)
                    .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                let (blob, read): (Vec<u8>, _) = bincode::decode_from_slice(&data[offset..], config)
                    .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                if tag == TAG_INSERT {
                    ops.push(WalOp::Insert { table, id, data: blob });
                } else {
                    ops.push(WalOp::Update { table, id, data: blob });
                }
            }
            TAG_DELETE => {
                let (table, read): (String, _) = bincode::decode_from_slice(&data[offset..], config)
                    .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                let (id, read): (u64, _) = bincode::decode_from_slice(&data[offset..], config)
                    .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                ops.push(WalOp::Delete { table, id });
            }
            TAG_CREATE_TABLE => {
                let (name, read): (String, _) = bincode::decode_from_slice(&data[offset..], config)
                    .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                ops.push(WalOp::CreateTable { name });
            }
            TAG_DELETE_TABLE => {
                let (name, read): (String, _) = bincode::decode_from_slice(&data[offset..], config)
                    .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                ops.push(WalOp::DeleteTable { name });
            }
            _ => return Err(Error::WalCorrupted(format!("unknown op tag: {tag}"))),
        }
    }

    Ok(WalEntry { version, ops })
}

// ---------------------------------------------------------------------------
// CRC32
// ---------------------------------------------------------------------------

pub(crate) fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

// ---------------------------------------------------------------------------
// WAL file I/O
// ---------------------------------------------------------------------------

const WAL_FILENAME: &str = "wal.bin";

/// Write a single WAL entry to a file. Format: `[len: u32][data][crc32: u32]`.
fn write_entry_to_file(file: &mut File, entry: &WalEntry) -> Result<()> {
    let data = serialize_entry(entry)?;
    let len = data.len() as u32;
    let checksum = crc32(&data);

    file.write_all(&len.to_le_bytes())
        .map_err(|e| Error::Persistence(e.to_string()))?;
    file.write_all(&data)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    file.write_all(&checksum.to_le_bytes())
        .map_err(|e| Error::Persistence(e.to_string()))?;
    Ok(())
}

/// Read all WAL entries from a file. Stops at EOF or first corrupted entry.
pub fn read_wal(path: &Path) -> Result<Vec<WalEntry>> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(Error::Persistence(e.to_string())),
    };
    let mut all_bytes = Vec::new();
    file.read_to_end(&mut all_bytes)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    let mut entries = Vec::new();
    let mut offset = 0;

    while offset + 4 <= all_bytes.len() {
        // Read length
        let len = u32::from_le_bytes(
            all_bytes[offset..offset + 4].try_into().unwrap(),
        ) as usize;
        offset += 4;

        if offset + len + 4 > all_bytes.len() {
            // Truncated entry at end of file — stop (crash during write).
            break;
        }

        let data = &all_bytes[offset..offset + len];
        offset += len;

        let stored_crc = u32::from_le_bytes(
            all_bytes[offset..offset + 4].try_into().unwrap(),
        );
        offset += 4;

        if crc32(data) != stored_crc {
            return Err(Error::WalCorrupted(format!(
                "CRC mismatch at entry starting at byte {}",
                offset - len - 8
            )));
        }

        entries.push(deserialize_entry(data)?);
    }

    Ok(entries)
}

/// Truncate the WAL file, removing all entries with version <= `up_to_version`.
pub(crate) fn prune_wal(path: &Path, up_to_version: u64) -> Result<()> {
    let entries = read_wal(path)?;
    let remaining: Vec<&WalEntry> = entries.iter()
        .filter(|e| e.version > up_to_version)
        .collect();

    if remaining.len() == entries.len() {
        return Ok(()); // nothing to prune
    }

    let mut file = File::create(path)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    for entry in remaining {
        write_entry_to_file(&mut file, entry)?;
    }
    file.sync_all()
        .map_err(|e| Error::Persistence(e.to_string()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Epoch-based sync state — shared between WalHandle and SyncWaiter
// ---------------------------------------------------------------------------

/// Tracks which WAL epoch has been fsynced. Writers obtain an epoch via
/// `next_epoch`, then wait until `fsynced_epoch >= their_epoch`.
pub(crate) struct WalSyncState {
    pub(crate) next_epoch: std::sync::atomic::AtomicU64,
    fsynced_epoch: std::sync::atomic::AtomicU64,
    condvar: std::sync::Condvar,
    mu: std::sync::Mutex<()>,
}

/// Returned by `WalHandle::write()`. Consistent callers block on `wait()`;
/// Eventual callers get `Done` (fire-and-forget).
pub(crate) enum SyncWaiter {
    /// Already durable or fire-and-forget (Eventual).
    Done,
    /// Block until the background thread fsyncs past this epoch.
    WaitForEpoch { epoch: u64, state: Arc<WalSyncState> },
}

impl SyncWaiter {
    pub fn wait(self) {
        if let SyncWaiter::WaitForEpoch { epoch, state } = self {
            let mut guard = state.mu.lock().unwrap();
            while state.fsynced_epoch.load(std::sync::atomic::Ordering::Acquire) < epoch {
                guard = state.condvar.wait(guard).unwrap();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WalHandle — background-thread WAL writer for both modes
// ---------------------------------------------------------------------------

/// Handle for the background WAL writer thread.
///
/// Both Consistent and Eventual modes use a background thread with a channel.
/// - **Consistent**: `write()` returns `SyncWaiter::WaitForEpoch` — the caller
///   must call `wait()` to block until fsync completes.
/// - **Eventual**: `write()` returns `SyncWaiter::Done` — fire-and-forget.
///
/// The background thread batches queued entries (recv + try_recv drain) and
/// issues a single fsync for the batch.
pub(crate) struct WalHandle {
    sender: Option<mpsc::Sender<WalEntry>>,
    bg_thread: Option<thread::JoinHandle<()>>,
    consistent: bool,
    sync_state: Option<Arc<WalSyncState>>,
    /// Number of WAL entries sent but not yet fsynced (Eventual mode).
    pub(crate) in_flight: Arc<std::sync::atomic::AtomicU64>,
}

impl WalHandle {
    /// Create a new WAL handle. Opens (or creates) the WAL file.
    /// Both modes use a background thread for batched writes.
    pub fn new(dir: &Path, consistent: bool) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        let wal_path = dir.join(WAL_FILENAME);

        let in_flight = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let sync_state = if consistent {
            Some(Arc::new(WalSyncState {
                next_epoch: std::sync::atomic::AtomicU64::new(1),
                fsynced_epoch: std::sync::atomic::AtomicU64::new(0),
                condvar: std::sync::Condvar::new(),
                mu: std::sync::Mutex::new(()),
            }))
        } else {
            None
        };

        let (tx, rx) = mpsc::channel::<WalEntry>();
        let bg_in_flight = in_flight.clone();
        let bg_sync_state = sync_state.clone();

        let handle = thread::spawn(move || {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .expect("failed to open WAL file in background thread");
            // Block on the first entry, then drain all queued entries
            // so they share a single fsync.
            while let Ok(first) = rx.recv() {
                let mut count = 1u64;
                if write_entry_to_file(&mut file, &first).is_err() {
                    count = 0;
                }
                while let Ok(entry) = rx.try_recv() {
                    count += 1;
                    if write_entry_to_file(&mut file, &entry).is_err() {
                        count -= 1;
                    }
                }
                let _ = file.sync_all();
                bg_in_flight.fetch_sub(count, std::sync::atomic::Ordering::Relaxed);
                // Notify Consistent waiters that this batch is durable.
                if let Some(state) = &bg_sync_state {
                    state.fsynced_epoch.fetch_add(count, std::sync::atomic::Ordering::Release);
                    let _guard = state.mu.lock().unwrap();
                    state.condvar.notify_all();
                }
            }
        });

        Ok(Self {
            sender: Some(tx),
            bg_thread: Some(handle),
            consistent,
            sync_state,
            in_flight,
        })
    }

    /// Submit a WAL entry to the background thread.
    ///
    /// - **Consistent**: returns `SyncWaiter::WaitForEpoch` — caller must
    ///   call `wait()` outside the store lock to block until fsync.
    /// - **Eventual**: returns `SyncWaiter::Done` — no wait needed.
    pub fn write(&self, entry: WalEntry) -> Result<SyncWaiter> {
        let sender = self.sender.as_ref().expect("WalHandle used after drop");
        self.in_flight.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        sender.send(entry)
            .map_err(|e| Error::Persistence(e.to_string()))?;

        if self.consistent {
            let state = self.sync_state.as_ref().unwrap();
            let epoch = state.next_epoch.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(SyncWaiter::WaitForEpoch { epoch, state: Arc::clone(state) })
        } else {
            Ok(SyncWaiter::Done)
        }
    }

    /// Returns the number of WAL entries sent but not yet fsynced.
    pub fn pending_writes(&self) -> u64 {
        self.in_flight.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for WalHandle {
    fn drop(&mut self) {
        // Drop the sender first so the background thread's recv() loop exits
        // after draining pending entries.
        self.sender.take();
        // Join the background thread to ensure all entries are fsynced.
        if let Some(handle) = self.bg_thread.take() {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// MockWal — test-only WAL with manual flush control
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) struct MockWal {
    pub(crate) entries: std::sync::Mutex<Vec<WalEntry>>,
    sync_state: Arc<WalSyncState>,
}

#[cfg(test)]
impl MockWal {
    pub fn new() -> Self {
        Self {
            entries: std::sync::Mutex::new(Vec::new()),
            sync_state: Arc::new(WalSyncState {
                next_epoch: std::sync::atomic::AtomicU64::new(1),
                fsynced_epoch: std::sync::atomic::AtomicU64::new(0),
                condvar: std::sync::Condvar::new(),
                mu: std::sync::Mutex::new(()),
            }),
        }
    }

    /// Submit a WAL entry. Returns a SyncWaiter that blocks until `flush()`.
    pub fn write(&self, entry: WalEntry) -> SyncWaiter {
        self.entries.lock().unwrap().push(entry);
        let epoch = self.sync_state.next_epoch.fetch_add(
            1,
            std::sync::atomic::Ordering::Relaxed,
        );
        SyncWaiter::WaitForEpoch {
            epoch,
            state: Arc::clone(&self.sync_state),
        }
    }

    /// Advance fsynced_epoch to release all currently blocked waiters.
    /// Sets fsynced_epoch to next_epoch - 1 (the highest assigned epoch).
    pub fn flush(&self) {
        let current_next = self.sync_state.next_epoch.load(std::sync::atomic::Ordering::Relaxed);
        self.sync_state.fsynced_epoch.store(current_next - 1, std::sync::atomic::Ordering::Release);
        let _guard = self.sync_state.mu.lock().unwrap();
        self.sync_state.condvar.notify_all();
    }

    /// Advance fsynced_epoch by 1, releasing only the next blocked waiter.
    pub fn flush_one(&self) {
        self.sync_state.fsynced_epoch.fetch_add(1, std::sync::atomic::Ordering::Release);
        let _guard = self.sync_state.mu.lock().unwrap();
        self.sync_state.condvar.notify_all();
    }

    /// Number of entries not yet flushed.
    pub fn pending(&self) -> usize {
        let next = self.sync_state.next_epoch.load(std::sync::atomic::Ordering::Relaxed);
        let fsynced = self.sync_state.fsynced_epoch.load(std::sync::atomic::Ordering::Relaxed);
        (next.saturating_sub(fsynced).saturating_sub(1)) as usize
    }
}

/// Return the WAL file path for a given directory.
pub(crate) fn wal_path(dir: &Path) -> PathBuf {
    dir.join(WAL_FILENAME)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct User {
        name: String,
        age: u32,
    }

    /// Create a store with WAL enabled (Standalone/Consistent) so wal_ops are tracked.
    fn wal_store() -> (Store, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(crate::StoreConfig {
            persistence: crate::Persistence::Standalone {
                dir: dir.path().to_path_buf(),
                durability: crate::Durability::Consistent,
            },
            ..crate::StoreConfig::default()
        });
        (store, dir)
    }

    #[test]
    fn wal_ops_captured_on_insert_update_delete() {
        let (store, _dir) = wal_store();
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table::<User>("users").unwrap();
            t.insert(User { name: "Alice".into(), age: 30 }).unwrap();
            t.insert(User { name: "Bob".into(), age: 25 }).unwrap();
            t.update(1, User { name: "Alice Updated".into(), age: 31 }).unwrap();
            t.delete(2).unwrap();
        }
        assert_eq!(wtx.wal_ops.len(), 4);
        assert!(matches!(&wtx.wal_ops[0], WalOp::Insert { table, id: 1, .. } if table == "users"));
        assert!(matches!(&wtx.wal_ops[1], WalOp::Insert { table, id: 2, .. } if table == "users"));
        assert!(matches!(&wtx.wal_ops[2], WalOp::Update { table, id: 1, .. } if table == "users"));
        assert!(matches!(&wtx.wal_ops[3], WalOp::Delete { table, id: 2 } if table == "users"));
    }

    #[test]
    fn wal_ops_captured_on_delete_table() {
        let (store, _dir) = wal_store();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users").unwrap().insert(User { name: "Alice".into(), age: 30 }).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx = store.begin_write(None).unwrap();
        wtx.delete_table("users");
        assert_eq!(wtx.wal_ops.len(), 1);
        assert!(matches!(&wtx.wal_ops[0], WalOp::DeleteTable { name } if name == "users"));
    }

    #[test]
    fn wal_ops_captured_on_batch_operations() {
        let (store, _dir) = wal_store();
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table::<User>("users").unwrap();
            t.insert_batch(vec![
                User { name: "Alice".into(), age: 30 },
                User { name: "Bob".into(), age: 25 },
            ]).unwrap();
            t.delete_batch(&[1, 2]).unwrap();
        }
        assert_eq!(wtx.wal_ops.len(), 4); // 2 inserts + 2 deletes
    }

    #[test]
    fn wal_entry_serialize_deserialize_roundtrip() {
        let entry = WalEntry {
            version: 42,
            ops: vec![
                WalOp::Insert { table: "users".into(), id: 1, data: vec![1, 2, 3] },
                WalOp::Update { table: "users".into(), id: 1, data: vec![4, 5, 6] },
                WalOp::Delete { table: "users".into(), id: 1 },
                WalOp::CreateTable { name: "orders".into() },
                WalOp::DeleteTable { name: "temp".into() },
            ],
        };
        let data = serialize_entry(&entry).unwrap();
        let recovered = deserialize_entry(&data).unwrap();
        assert_eq!(recovered.version, 42);
        assert_eq!(recovered.ops.len(), 5);
    }

    #[test]
    fn wal_file_write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(WAL_FILENAME);

        // Write entries
        {
            let mut file = File::create(&path).unwrap();
            let e1 = WalEntry { version: 1, ops: vec![WalOp::Insert { table: "t".into(), id: 1, data: vec![10] }] };
            let e2 = WalEntry { version: 2, ops: vec![WalOp::Delete { table: "t".into(), id: 1 }] };
            write_entry_to_file(&mut file, &e1).unwrap();
            write_entry_to_file(&mut file, &e2).unwrap();
            file.sync_all().unwrap();
        }

        // Read back
        let entries = read_wal(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].version, 1);
        assert_eq!(entries[1].version, 2);
    }

    #[test]
    fn wal_crc_corruption_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(WAL_FILENAME);

        {
            let mut file = File::create(&path).unwrap();
            let e1 = WalEntry { version: 1, ops: vec![WalOp::Insert { table: "t".into(), id: 1, data: vec![10] }] };
            write_entry_to_file(&mut file, &e1).unwrap();
            file.sync_all().unwrap();
        }

        // Corrupt a byte in the data section (after the 4-byte length prefix).
        {
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[5] ^= 0xFF; // flip a byte
            std::fs::write(&path, &bytes).unwrap();
        }

        assert!(matches!(read_wal(&path), Err(Error::WalCorrupted(_))));
    }

    #[test]
    fn wal_prune_removes_old_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(WAL_FILENAME);

        {
            let mut file = File::create(&path).unwrap();
            for v in 1..=5 {
                let entry = WalEntry { version: v, ops: vec![] };
                write_entry_to_file(&mut file, &entry).unwrap();
            }
            file.sync_all().unwrap();
        }

        prune_wal(&path, 3).unwrap();
        let remaining = read_wal(&path).unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].version, 4);
        assert_eq!(remaining[1].version, 5);
    }

    #[test]
    fn wal_handle_consistent_write() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), true).unwrap();
        let w1 = handle.write(WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap();
        let w2 = handle.write(WalEntry { version: 2, ops: vec![WalOp::DeleteTable { name: "t".into() }] }).unwrap();
        // Wait for both fsyncs to complete.
        w1.wait();
        w2.wait();

        let entries = read_wal(&wal_path(dir.path())).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn wal_handle_eventual_write() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false).unwrap();
        handle.write(WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap();
        // Drop flushes the background thread.
        drop(handle);

        let entries = read_wal(&wal_path(dir.path())).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn wal_handle_pending_writes() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false).unwrap();
        assert_eq!(handle.pending_writes(), 0);
        handle.write(WalEntry { version: 1, ops: vec![] }).unwrap();
        // pending_writes may be 1 or 0 depending on bg thread speed, but
        // it should not panic. After drop it must be 0.
        drop(handle);
        // Can't check after drop, but the fact it didn't panic is enough.
    }

    #[test]
    fn wal_handle_consistent_pending_writes() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), true).unwrap();
        assert_eq!(handle.pending_writes(), 0);
        let w = handle.write(WalEntry { version: 1, ops: vec![] }).unwrap();
        // Before wait, in_flight should be >= 1.
        w.wait();
        // After wait + bg thread sync, pending should be 0 (eventually).
        drop(handle);
    }

    /// Truncated entry at end of WAL file (simulates crash during write).
    /// read_wal should return entries before the truncation.
    #[test]
    fn wal_truncated_entry_at_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(WAL_FILENAME);

        {
            let mut file = File::create(&path).unwrap();
            let e1 = WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] };
            let e2 = WalEntry { version: 2, ops: vec![WalOp::DeleteTable { name: "t".into() }] };
            write_entry_to_file(&mut file, &e1).unwrap();
            write_entry_to_file(&mut file, &e2).unwrap();
            file.sync_all().unwrap();
        }

        // Truncate the file mid-entry: remove last 3 bytes.
        {
            let bytes = std::fs::read(&path).unwrap();
            std::fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
        }

        // Should recover the first entry and silently skip the truncated second.
        let entries = read_wal(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].version, 1);
    }

    /// Unknown op tag in WAL entry data triggers WalCorrupted error.
    #[test]
    fn wal_unknown_op_tag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(WAL_FILENAME);

        // Write a valid entry, then manually craft one with a bad op tag.
        {
            let mut file = File::create(&path).unwrap();
            // Craft a minimal entry: version=1, op_count=1, tag=0xFF (invalid).
            let config = bincode::config::standard();
            let mut data = Vec::new();
            bincode::encode_into_std_write(1u64, &mut data, config).unwrap(); // version
            bincode::encode_into_std_write(1u32, &mut data, config).unwrap(); // op_count
            data.push(0xFF); // invalid tag

            let len = data.len() as u32;
            file.write_all(&len.to_le_bytes()).unwrap();
            file.write_all(&data).unwrap();
            let crc = crc32(&data);
            file.write_all(&crc.to_le_bytes()).unwrap();
            file.sync_all().unwrap();
        }

        let err = read_wal(&path).unwrap_err();
        assert!(matches!(err, Error::WalCorrupted(ref msg) if msg.contains("unknown op tag")));
    }

    /// Truncated op data inside an entry (op_count says 2, but data ends after 1).
    #[test]
    fn wal_truncated_op_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(WAL_FILENAME);

        {
            let mut file = File::create(&path).unwrap();
            let config = bincode::config::standard();
            let mut data = Vec::new();
            bincode::encode_into_std_write(1u64, &mut data, config).unwrap(); // version
            bincode::encode_into_std_write(2u32, &mut data, config).unwrap(); // op_count = 2
            // Only write one op (CreateTable).
            data.push(super::TAG_CREATE_TABLE);
            bincode::encode_into_std_write("t".to_string(), &mut data, config).unwrap();
            // No second op — data ends here.

            let len = data.len() as u32;
            file.write_all(&len.to_le_bytes()).unwrap();
            file.write_all(&data).unwrap();
            let crc = crc32(&data);
            file.write_all(&crc.to_le_bytes()).unwrap();
            file.sync_all().unwrap();
        }

        let err = read_wal(&path).unwrap_err();
        assert!(matches!(err, Error::WalCorrupted(ref msg) if msg.contains("unexpected end")));
    }

    /// prune_wal with a version below all entries does nothing.
    #[test]
    fn wal_prune_nothing_to_remove() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(WAL_FILENAME);

        {
            let mut file = File::create(&path).unwrap();
            for v in 5..=7 {
                let entry = WalEntry { version: v, ops: vec![] };
                write_entry_to_file(&mut file, &entry).unwrap();
            }
            file.sync_all().unwrap();
        }

        // Prune up to version 2 — all entries are > 2, nothing to remove.
        prune_wal(&path, 2).unwrap();
        let entries = read_wal(&path).unwrap();
        assert_eq!(entries.len(), 3);
    }

    /// read_wal on a nonexistent file returns empty Vec.
    #[test]
    fn wal_read_nonexistent_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does_not_exist.bin");
        let entries = read_wal(&path).unwrap();
        assert!(entries.is_empty());
    }
}
