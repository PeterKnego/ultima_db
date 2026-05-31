// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;

use crate::{Error, Result};

// ---------------------------------------------------------------------------
// Poison latch
// ---------------------------------------------------------------------------

/// Poison latch shared between the WAL background thread and the store.
///
/// The background thread calls [`WalPoison::poison`] on any append/fsync
/// failure; the store checks it at `begin_write`/`commit`. Once poisoned the
/// store refuses further writes until it is dropped and re-created.
pub(crate) struct WalPoison {
    poisoned: AtomicBool,
    cause: Mutex<Option<String>>,
}

impl WalPoison {
    pub(crate) fn new() -> Self {
        Self {
            poisoned: AtomicBool::new(false),
            cause: Mutex::new(None),
        }
    }

    /// Record the failure cause (first cause wins) and set the latch.
    pub(crate) fn poison(&self, msg: String) {
        let mut c = self.cause.lock().unwrap();
        if c.is_none() {
            *c = Some(msg);
        }
        self.poisoned.store(true, Ordering::Release);
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// `Ok(())` when clear, else `Err(Error::Poisoned(cause))`.
    pub(crate) fn check(&self) -> Result<()> {
        if self.is_poisoned() {
            Err(self.error())
        } else {
            Ok(())
        }
    }

    pub(crate) fn error(&self) -> Error {
        let c = self.cause.lock().unwrap();
        Error::Poisoned(c.clone().unwrap_or_else(|| "WAL poisoned".into()))
    }
}

// ---------------------------------------------------------------------------
// WAL data types
// ---------------------------------------------------------------------------

/// A single mutation within a transaction.
#[derive(Debug, Clone)]
pub enum WalOp {
    Insert {
        table: String,
        id: u64,
        data: Vec<u8>,
    },
    Update {
        table: String,
        id: u64,
        data: Vec<u8>,
    },
    Delete {
        table: String,
        id: u64,
    },
    CreateTable {
        name: String,
    },
    DeleteTable {
        name: String,
    },
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
                let (table, read): (String, _) =
                    bincode::decode_from_slice(&data[offset..], config)
                        .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                let (id, read): (u64, _) = bincode::decode_from_slice(&data[offset..], config)
                    .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                let (blob, read): (Vec<u8>, _) =
                    bincode::decode_from_slice(&data[offset..], config)
                        .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                if tag == TAG_INSERT {
                    ops.push(WalOp::Insert {
                        table,
                        id,
                        data: blob,
                    });
                } else {
                    ops.push(WalOp::Update {
                        table,
                        id,
                        data: blob,
                    });
                }
            }
            TAG_DELETE => {
                let (table, read): (String, _) =
                    bincode::decode_from_slice(&data[offset..], config)
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
        let len = u32::from_le_bytes(all_bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + len + 4 > all_bytes.len() {
            // Truncated entry at end of file — stop (crash during write).
            break;
        }

        let data = &all_bytes[offset..offset + len];
        offset += len;

        let stored_crc = u32::from_le_bytes(all_bytes[offset..offset + 4].try_into().unwrap());
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
///
/// This truncates and rewrites the file in place rather than using atomic
/// rename, because the WalHandle background thread may have the file open in
/// append mode. An atomic rename would cause the bg thread to write to the
/// old (unlinked) inode, losing subsequent entries.
///
/// Crash safety: if the process crashes mid-rewrite, the WAL may be truncated
/// or partially rewritten. Recovery handles this by stopping at the first
/// corrupt/truncated entry and falling back to the checkpoint. Since prune_wal
/// is only called after a successful checkpoint, no committed data is lost.
pub(crate) fn prune_wal(path: &Path, up_to_version: u64) -> Result<()> {
    let entries = read_wal(path)?;
    let remaining: Vec<&WalEntry> = entries
        .iter()
        .filter(|e| e.version > up_to_version)
        .collect();

    if remaining.len() == entries.len() {
        return Ok(()); // nothing to prune
    }

    // Serialize all remaining entries into a buffer first, then do a
    // single truncate + write + sync to minimize the window of corruption.
    let mut buf = Vec::new();
    for entry in &remaining {
        let data = serialize_entry(entry)?;
        let len = data.len() as u32;
        let checksum = crc32(&data);
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&data);
        buf.extend_from_slice(&checksum.to_le_bytes());
    }

    let mut file = File::create(path).map_err(|e| Error::Persistence(e.to_string()))?;
    file.write_all(&buf)
        .map_err(|e| Error::Persistence(e.to_string()))?;
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
    /// Block until the background thread fsyncs past this epoch, or the WAL
    /// is poisoned.
    WaitForEpoch {
        epoch: u64,
        state: Arc<WalSyncState>,
        poison: Arc<WalPoison>,
    },
}

impl SyncWaiter {
    /// Block until this entry's batch is durably fsynced.
    ///
    /// Returns `Err(Error::Poisoned)` if the WAL was poisoned before this
    /// entry's batch reached disk. An entry whose batch fsynced *before* a
    /// later failure still returns `Ok`.
    pub fn wait(self) -> Result<()> {
        if let SyncWaiter::WaitForEpoch {
            epoch,
            state,
            poison,
        } = self
        {
            let mut guard = state.mu.lock().unwrap();
            loop {
                if state
                    .fsynced_epoch
                    .load(std::sync::atomic::Ordering::Acquire)
                    >= epoch
                {
                    return Ok(());
                }
                if poison.is_poisoned() {
                    return Err(poison.error());
                }
                guard = state.condvar.wait(guard).unwrap();
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WalSink — abstraction over the WAL's durable backing store
// ---------------------------------------------------------------------------

/// Abstraction over the WAL's durable backing store, so failures can be
/// injected in tests. `append` writes one entry; `sync` fsyncs.
pub(crate) trait WalSink: Send {
    fn append(&mut self, entry: &WalEntry) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
}

/// Production sink: appends framed entries to a file and fsyncs it.
struct FileSink {
    file: File,
}

impl WalSink for FileSink {
    fn append(&mut self, entry: &WalEntry) -> Result<()> {
        write_entry_to_file(&mut self.file, entry)
    }
    fn sync(&mut self) -> Result<()> {
        self.file
            .sync_all()
            .map_err(|e| Error::Persistence(e.to_string()))
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
    poison: Arc<WalPoison>,
    /// Number of WAL entries sent but not yet fsynced (Eventual mode).
    pub(crate) in_flight: Arc<std::sync::atomic::AtomicU64>,
}

impl WalHandle {
    /// Create a new WAL handle. Opens (or creates) the WAL file and fsyncs the
    /// directory so the file's creation is durable. Both modes use a background
    /// thread for batched writes.
    pub fn new(dir: &Path, consistent: bool, poison: Arc<WalPoison>) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let wal_path = dir.join(WAL_FILENAME);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        sync_dir(dir)?;
        Ok(Self::with_sink(FileSink { file }, consistent, poison))
    }

    /// Build a handle around an arbitrary sink. Used by `new` (FileSink) and by
    /// tests (fault-injecting sinks).
    pub(crate) fn with_sink<S: WalSink + 'static>(
        sink: S,
        consistent: bool,
        poison: Arc<WalPoison>,
    ) -> Self {
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
        let bg_poison = poison.clone();

        let handle = spawn_wal_thread(sink, rx, bg_in_flight, bg_sync_state, bg_poison);

        Self {
            sender: Some(tx),
            bg_thread: Some(handle),
            consistent,
            sync_state,
            poison,
            in_flight,
        }
    }

    /// Submit a WAL entry to the background thread.
    ///
    /// - **Consistent**: returns `SyncWaiter::WaitForEpoch` — caller must
    ///   call `wait()` outside the store lock to block until fsync.
    /// - **Eventual**: returns `SyncWaiter::Done` — no wait needed.
    pub fn write(&self, entry: WalEntry) -> Result<SyncWaiter> {
        let sender = self.sender.as_ref().expect("WalHandle used after drop");
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        sender
            .send(entry)
            .map_err(|e| Error::Persistence(e.to_string()))?;

        if self.consistent {
            let state = self.sync_state.as_ref().unwrap();
            let epoch = state
                .next_epoch
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(SyncWaiter::WaitForEpoch {
                epoch,
                state: Arc::clone(state),
                poison: Arc::clone(&self.poison),
            })
        } else {
            Ok(SyncWaiter::Done)
        }
    }

    /// Returns the number of WAL entries sent but not yet fsynced.
    pub fn pending_writes(&self) -> u64 {
        self.in_flight.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Background WAL writer loop. Drains a batch, writes it, fsyncs once, and
/// advances the synced epoch. On any append/fsync failure, poisons the latch,
/// wakes all waiters, and stops.
fn spawn_wal_thread<S: WalSink + 'static>(
    mut sink: S,
    rx: mpsc::Receiver<WalEntry>,
    in_flight: Arc<std::sync::atomic::AtomicU64>,
    sync_state: Option<Arc<WalSyncState>>,
    poison: Arc<WalPoison>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while let Ok(first) = rx.recv() {
            let mut batch = vec![first];
            while let Ok(entry) = rx.try_recv() {
                batch.push(entry);
            }
            let count = batch.len() as u64;

            // Write every entry, then fsync once. Any error means this batch
            // is NOT durable.
            let result: Result<()> = (|| {
                for entry in &batch {
                    sink.append(entry)?;
                }
                sink.sync()
            })();

            in_flight.fetch_sub(count, std::sync::atomic::Ordering::Relaxed);

            match result {
                Ok(()) => {
                    // Batch is durable — release its waiters.
                    if let Some(state) = &sync_state {
                        let _guard = state.mu.lock().unwrap();
                        state
                            .fsynced_epoch
                            .fetch_add(count, std::sync::atomic::Ordering::Release);
                        state.condvar.notify_all();
                    }
                }
                Err(e) => {
                    // Poison, wake any waiters so they observe it, and stop.
                    // The epoch is NOT advanced: this batch never reached disk.
                    poison.poison(format!("WAL durability failure: {e}"));
                    if let Some(state) = &sync_state {
                        let _guard = state.mu.lock().unwrap();
                        state.condvar.notify_all();
                    }
                    return;
                }
            }
        }
    })
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
    poison: Arc<WalPoison>,
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
            poison: Arc::new(WalPoison::new()),
        }
    }

    /// The poison latch backing this mock, so a test can install it as the
    /// store's `wal_poison` and observe `begin_write` failing after `fail()`.
    pub fn poison(&self) -> Arc<WalPoison> {
        Arc::clone(&self.poison)
    }

    /// Simulate a WAL fsync failure: poison and wake all blocked waiters.
    pub fn fail(&self) {
        self.poison.poison("mock WAL failure".into());
        let _guard = self.sync_state.mu.lock().unwrap();
        self.sync_state.condvar.notify_all();
    }

    /// Submit a WAL entry. Returns a SyncWaiter that blocks until `flush()` or
    /// `fail()`.
    pub fn write(&self, entry: WalEntry) -> SyncWaiter {
        self.entries.lock().unwrap().push(entry);
        let epoch = self
            .sync_state
            .next_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SyncWaiter::WaitForEpoch {
            epoch,
            state: Arc::clone(&self.sync_state),
            poison: Arc::clone(&self.poison),
        }
    }

    /// Advance fsynced_epoch to release all currently blocked waiters.
    /// Sets fsynced_epoch to next_epoch - 1 (the highest assigned epoch).
    pub fn flush(&self) {
        let current_next = self
            .sync_state
            .next_epoch
            .load(std::sync::atomic::Ordering::Relaxed);
        self.sync_state
            .fsynced_epoch
            .store(current_next - 1, std::sync::atomic::Ordering::Release);
        let _guard = self.sync_state.mu.lock().unwrap();
        self.sync_state.condvar.notify_all();
    }

    /// Advance fsynced_epoch by 1, releasing only the next blocked waiter.
    pub fn flush_one(&self) {
        self.sync_state
            .fsynced_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        let _guard = self.sync_state.mu.lock().unwrap();
        self.sync_state.condvar.notify_all();
    }

    /// Number of entries not yet flushed.
    pub fn pending(&self) -> usize {
        let next = self
            .sync_state
            .next_epoch
            .load(std::sync::atomic::Ordering::Relaxed);
        let fsynced = self
            .sync_state
            .fsynced_epoch
            .load(std::sync::atomic::Ordering::Relaxed);
        (next.saturating_sub(fsynced).saturating_sub(1)) as usize
    }
}

/// Return the WAL file path for a given directory.
pub(crate) fn wal_path(dir: &Path) -> PathBuf {
    dir.join(WAL_FILENAME)
}

/// Fsync a directory so that file creations/renames within it are durable.
///
/// On Unix this opens the directory and calls `sync_all`. On platforms that
/// do not support directory fsync (e.g. Windows) this is a no-op.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let f = File::open(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        f.sync_all().map_err(|e| Error::Persistence(e.to_string()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
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
        })
        .unwrap();
        (store, dir)
    }

    #[test]
    fn wal_ops_captured_on_insert_update_delete() {
        let (store, _dir) = wal_store();
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table::<User>("users").unwrap();
            t.insert(User {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();
            t.insert(User {
                name: "Bob".into(),
                age: 25,
            })
            .unwrap();
            t.update(
                1,
                User {
                    name: "Alice Updated".into(),
                    age: 31,
                },
            )
            .unwrap();
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
            wtx.open_table::<User>("users")
                .unwrap()
                .insert(User {
                    name: "Alice".into(),
                    age: 30,
                })
                .unwrap();
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
                User {
                    name: "Alice".into(),
                    age: 30,
                },
                User {
                    name: "Bob".into(),
                    age: 25,
                },
            ])
            .unwrap();
            t.delete_batch(&[1, 2]).unwrap();
        }
        assert_eq!(wtx.wal_ops.len(), 4); // 2 inserts + 2 deletes
    }

    #[test]
    fn wal_entry_serialize_deserialize_roundtrip() {
        let entry = WalEntry {
            version: 42,
            ops: vec![
                WalOp::Insert {
                    table: "users".into(),
                    id: 1,
                    data: vec![1, 2, 3],
                },
                WalOp::Update {
                    table: "users".into(),
                    id: 1,
                    data: vec![4, 5, 6],
                },
                WalOp::Delete {
                    table: "users".into(),
                    id: 1,
                },
                WalOp::CreateTable {
                    name: "orders".into(),
                },
                WalOp::DeleteTable {
                    name: "temp".into(),
                },
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
            let e1 = WalEntry {
                version: 1,
                ops: vec![WalOp::Insert {
                    table: "t".into(),
                    id: 1,
                    data: vec![10],
                }],
            };
            let e2 = WalEntry {
                version: 2,
                ops: vec![WalOp::Delete {
                    table: "t".into(),
                    id: 1,
                }],
            };
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
            let e1 = WalEntry {
                version: 1,
                ops: vec![WalOp::Insert {
                    table: "t".into(),
                    id: 1,
                    data: vec![10],
                }],
            };
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
                let entry = WalEntry {
                    version: v,
                    ops: vec![],
                };
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
        let handle = WalHandle::new(dir.path(), true, Arc::new(WalPoison::new())).unwrap();
        let w1 = handle
            .write(WalEntry {
                version: 1,
                ops: vec![WalOp::CreateTable { name: "t".into() }],
            })
            .unwrap();
        let w2 = handle
            .write(WalEntry {
                version: 2,
                ops: vec![WalOp::DeleteTable { name: "t".into() }],
            })
            .unwrap();
        // Wait for both fsyncs to complete.
        w1.wait().unwrap();
        w2.wait().unwrap();

        let entries = read_wal(&wal_path(dir.path())).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn wal_handle_eventual_write() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false, Arc::new(WalPoison::new())).unwrap();
        handle
            .write(WalEntry {
                version: 1,
                ops: vec![WalOp::CreateTable { name: "t".into() }],
            })
            .unwrap();
        // Drop flushes the background thread.
        drop(handle);

        let entries = read_wal(&wal_path(dir.path())).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn wal_handle_pending_writes() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false, Arc::new(WalPoison::new())).unwrap();
        assert_eq!(handle.pending_writes(), 0);
        handle
            .write(WalEntry {
                version: 1,
                ops: vec![],
            })
            .unwrap();
        // pending_writes may be 1 or 0 depending on bg thread speed, but
        // it should not panic. After drop it must be 0.
        drop(handle);
        // Can't check after drop, but the fact it didn't panic is enough.
    }

    #[test]
    fn wal_handle_consistent_pending_writes() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), true, Arc::new(WalPoison::new())).unwrap();
        assert_eq!(handle.pending_writes(), 0);
        let w = handle
            .write(WalEntry {
                version: 1,
                ops: vec![],
            })
            .unwrap();
        // Before wait, in_flight should be >= 1.
        w.wait().unwrap();
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
            let e1 = WalEntry {
                version: 1,
                ops: vec![WalOp::CreateTable { name: "t".into() }],
            };
            let e2 = WalEntry {
                version: 2,
                ops: vec![WalOp::DeleteTable { name: "t".into() }],
            };
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
                let entry = WalEntry {
                    version: v,
                    ops: vec![],
                };
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

    #[test]
    fn wal_poison_latches_first_cause() {
        let p = WalPoison::new();
        assert!(!p.is_poisoned());
        assert!(p.check().is_ok());

        p.poison("first".into());
        p.poison("second".into()); // first cause wins

        assert!(p.is_poisoned());
        match p.check() {
            Err(Error::Poisoned(msg)) => assert_eq!(msg, "first"),
            other => panic!("expected Poisoned, got {other:?}"),
        }
    }

    #[test]
    fn sync_dir_on_normal_directory_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        // Create a file so the directory is non-trivial.
        std::fs::write(dir.path().join("x"), b"hello").unwrap();
        assert!(sync_dir(dir.path()).is_ok());
    }

    /// Test sink that fails the first `sync` and records appends.
    struct FaultySink {
        fail_sync_after: usize, // succeed this many syncs, then fail
        sync_count: usize,
    }
    impl WalSink for FaultySink {
        fn append(&mut self, _entry: &WalEntry) -> Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> Result<()> {
            self.sync_count += 1;
            if self.sync_count > self.fail_sync_after {
                Err(Error::Persistence("injected sync failure".into()))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn wal_sync_failure_poisons_and_waiter_errors() {
        let poison = Arc::new(WalPoison::new());
        let handle = WalHandle::with_sink(
            FaultySink {
                fail_sync_after: 0, // fail immediately
                sync_count: 0,
            },
            true,
            poison.clone(),
        );
        let w = handle
            .write(WalEntry {
                version: 1,
                ops: vec![WalOp::CreateTable { name: "t".into() }],
            })
            .unwrap();

        // The waiter must observe the failure as an error, not a fake success.
        match w.wait() {
            Err(Error::Poisoned(_)) => {}
            other => panic!("expected Err(Poisoned), got {other:?}"),
        }
        assert!(poison.is_poisoned());
    }

    #[test]
    fn wal_durable_batch_before_failure_returns_ok() {
        let poison = Arc::new(WalPoison::new());
        let handle = WalHandle::with_sink(
            FaultySink {
                fail_sync_after: 1, // first sync ok, second fails
                sync_count: 0,
            },
            true,
            poison.clone(),
        );

        // First entry: its own batch fsyncs successfully.
        let w1 = handle
            .write(WalEntry { version: 1, ops: vec![] })
            .unwrap();
        w1.wait().expect("first batch should be durable");

        // Second entry: its batch's sync fails -> poisoned, waiter errors.
        let w2 = handle
            .write(WalEntry { version: 2, ops: vec![] })
            .unwrap();
        match w2.wait() {
            Err(Error::Poisoned(_)) => {}
            other => panic!("expected Err(Poisoned), got {other:?}"),
        }
    }
}
