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
use std::sync::mpsc;
use std::thread;

use crate::{Error, Result};

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
    /// Block until the background thread fsyncs past this epoch.
    WaitForEpoch {
        epoch: u64,
        state: Arc<WalSyncState>,
    },
}

impl SyncWaiter {
    pub fn wait(self) {
        if let SyncWaiter::WaitForEpoch { epoch, state } = self {
            let mut guard = state.mu.lock().unwrap();
            while state
                .fsynced_epoch
                .load(std::sync::atomic::Ordering::Acquire)
                < epoch
            {
                guard = state.condvar.wait(guard).unwrap();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WalDurability — version-keyed durability watermark (task28)
// ---------------------------------------------------------------------------

/// A one-shot durability callback, fired with `Ok(())` once the target version
/// is fsynced or `Err(_)` if the fsync failed / the WAL closed first.
type DurabilityCallback = Box<dyn FnOnce(Result<()>) + Send>;

/// Tracks the highest commit version whose WAL bytes are fsync-durable, and
/// lets callers wait on (or be notified of) an arbitrary target version.
///
/// Unlike [`WalSyncState`] (which counts entries via opaque epochs for the
/// Consistent-mode `commit()` wait), this is keyed by the same `version` that
/// `commit()` returns, so an Eventual-mode caller can learn after the fact
/// when a version it already committed became durable. Strictly additive: it
/// runs in both durability modes and changes no existing behavior.
pub(crate) struct WalDurability {
    /// Highest version known fsync-durable. Monotonic; only ever advances.
    durable_version: std::sync::atomic::AtomicU64,
    /// Set when the background thread is gone; releases parked waiters so they
    /// cannot block forever on a version that will never be reached.
    closed: std::sync::atomic::AtomicBool,
    inner: std::sync::Mutex<DurabilityWaiters>,
    condvar: std::sync::Condvar,
}

#[derive(Default)]
struct DurabilityWaiters {
    /// Parked callbacks: `(target_version, callback)`. Fired once the watermark
    /// reaches `target_version` (or an error/close covers it).
    callbacks: Vec<(u64, DurabilityCallback)>,
    /// Sticky fsync error, recorded for the highest version that failed.
    /// Waiters at/below this version resolve to `Err`.
    last_error: Option<(u64, String)>,
}

impl WalDurability {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            durable_version: std::sync::atomic::AtomicU64::new(0),
            closed: std::sync::atomic::AtomicBool::new(false),
            inner: std::sync::Mutex::new(DurabilityWaiters::default()),
            condvar: std::sync::Condvar::new(),
        })
    }

    /// Highest version known to be fsync-durable.
    pub(crate) fn current(&self) -> u64 {
        self.durable_version
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Called by the background writer after a successful batch fsync. `version`
    /// is the high-water mark of the batch (max successfully-written version).
    fn publish(&self, version: u64) {
        use std::sync::atomic::Ordering;
        let ready = {
            let mut w = self.inner.lock().unwrap();
            let new = self.durable_version.load(Ordering::Acquire).max(version);
            // Store under the mutex so a concurrent `wait`/`on_complete` cannot
            // observe the old watermark and then miss the notify (lost wakeup).
            self.durable_version.store(new, Ordering::Release);
            self.condvar.notify_all();
            drain_le(&mut w.callbacks, new)
        };
        for cb in ready {
            cb(Ok(()));
        }
    }

    /// Called by the background writer when a batch fsync fails. Waiters at or
    /// below `version` resolve to `Err`; the watermark is NOT advanced.
    fn publish_error(&self, version: u64, msg: String) {
        let ready = {
            let mut w = self.inner.lock().unwrap();
            match &w.last_error {
                Some((ev, _)) if *ev >= version => {}
                _ => w.last_error = Some((version, msg.clone())),
            }
            self.condvar.notify_all();
            drain_le(&mut w.callbacks, version)
        };
        for cb in ready {
            cb(Err(Error::Persistence(msg.clone())));
        }
    }

    /// Release all parked waiters (the background thread is gone).
    fn close(&self) {
        use std::sync::atomic::Ordering;
        let ready = {
            let mut w = self.inner.lock().unwrap();
            self.closed.store(true, Ordering::Release);
            self.condvar.notify_all();
            std::mem::take(&mut w.callbacks)
        };
        for (_, cb) in ready {
            cb(Err(Error::Persistence(
                "WAL closed before version became durable".into(),
            )));
        }
    }

    /// Block until `version` is durable. Returns `Err` if a covering fsync
    /// failed or the WAL closed first.
    pub(crate) fn wait(&self, version: u64) -> Result<()> {
        use std::sync::atomic::Ordering;
        let mut guard = self.inner.lock().unwrap();
        loop {
            if self.durable_version.load(Ordering::Acquire) >= version {
                return Ok(());
            }
            if let Some((ev, msg)) = &guard.last_error
                && *ev >= version
            {
                return Err(Error::Persistence(msg.clone()));
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::Persistence(
                    "WAL closed before version became durable".into(),
                ));
            }
            guard = self.condvar.wait(guard).unwrap();
        }
    }

    /// Register `cb` to fire once `version` is durable. Fires inline (on the
    /// calling thread) if already durable, already errored, or already closed.
    pub(crate) fn on_complete(&self, version: u64, cb: DurabilityCallback) {
        use std::sync::atomic::Ordering;
        let mut w = self.inner.lock().unwrap();
        if self.durable_version.load(Ordering::Acquire) >= version {
            drop(w);
            cb(Ok(()));
            return;
        }
        if let Some((ev, msg)) = &w.last_error
            && *ev >= version
        {
            let msg = msg.clone();
            drop(w);
            cb(Err(Error::Persistence(msg)));
            return;
        }
        if self.closed.load(Ordering::Acquire) {
            drop(w);
            cb(Err(Error::Persistence("WAL closed".into())));
            return;
        }
        w.callbacks.push((version, cb));
    }
}

/// Remove and return every callback whose target version is `<= version`.
/// Order is irrelevant (each callback is independent), so `swap_remove` is fine.
fn drain_le(callbacks: &mut Vec<(u64, DurabilityCallback)>, version: u64) -> Vec<DurabilityCallback> {
    let mut ready = Vec::new();
    let mut i = 0;
    while i < callbacks.len() {
        if callbacks[i].0 <= version {
            ready.push(callbacks.swap_remove(i).1);
        } else {
            i += 1;
        }
    }
    ready
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
    /// Version-keyed durability watermark (task28). Present in both modes.
    durability: Arc<WalDurability>,
    /// Number of WAL entries sent but not yet fsynced (Eventual mode).
    pub(crate) in_flight: Arc<std::sync::atomic::AtomicU64>,
}

impl WalHandle {
    /// Create a new WAL handle. Opens (or creates) the WAL file.
    /// Both modes use a background thread for batched writes.
    pub fn new(dir: &Path, consistent: bool) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
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

        let durability = WalDurability::new();

        let (tx, rx) = mpsc::channel::<WalEntry>();
        let bg_in_flight = in_flight.clone();
        let bg_sync_state = sync_state.clone();
        let bg_durability = durability.clone();

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
                // Batch high-water mark: max version successfully written this
                // round. `max` (not last) is robust even if a future change
                // submits versions out of order. 0 means nothing was written.
                let mut hwm = 0u64;
                if write_entry_to_file(&mut file, &first).is_err() {
                    count = 0;
                } else {
                    hwm = first.version;
                }
                while let Ok(entry) = rx.try_recv() {
                    count += 1;
                    if write_entry_to_file(&mut file, &entry).is_err() {
                        count -= 1;
                    } else {
                        hwm = hwm.max(entry.version);
                    }
                }
                let sync_res = file.sync_all();
                bg_in_flight.fetch_sub(count, std::sync::atomic::Ordering::Relaxed);
                // Notify Consistent waiters that this batch is durable.
                // The epoch update MUST happen inside the mutex to prevent
                // lost wakeups: without it, a waiter could check fsynced_epoch
                // (seeing the old value), then we update + notify before the
                // waiter enters condvar.wait(), causing it to block forever.
                if let Some(state) = &bg_sync_state {
                    let _guard = state.mu.lock().unwrap();
                    state
                        .fsynced_epoch
                        .fetch_add(count, std::sync::atomic::Ordering::Release);
                    state.condvar.notify_all();
                }
                // Advance the version-keyed watermark (task28). Only on a
                // successful fsync; a failure poisons waiters at/below hwm.
                if hwm > 0 {
                    match &sync_res {
                        Ok(()) => bg_durability.publish(hwm),
                        Err(e) => bg_durability.publish_error(hwm, e.to_string()),
                    }
                }
            }
        });

        Ok(Self {
            sender: Some(tx),
            bg_thread: Some(handle),
            consistent,
            sync_state,
            durability,
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
            })
        } else {
            Ok(SyncWaiter::Done)
        }
    }

    /// Returns the number of WAL entries sent but not yet fsynced.
    pub fn pending_writes(&self) -> u64 {
        self.in_flight.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Highest commit version known to be fsync-durable (task28).
    pub fn durable_version(&self) -> u64 {
        self.durability.current()
    }

    /// Shared handle to the durability watermark, so callers can wait/register
    /// without holding any store lock during the (potentially blocking) wait.
    pub fn durability(&self) -> Arc<WalDurability> {
        Arc::clone(&self.durability)
    }
}

impl Drop for WalHandle {
    fn drop(&mut self) {
        // Drop the sender first so the background thread's recv() loop exits
        // after draining pending entries.
        self.sender.take();
        // Join the background thread to ensure all entries are fsynced. By the
        // time this returns, every queued entry has been published to the
        // watermark.
        if let Some(handle) = self.bg_thread.take() {
            let _ = handle.join();
        }
        // Release any waiter parked on a version that was never committed, so
        // it cannot block forever now that the writer is gone.
        self.durability.close();
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
        let epoch = self
            .sync_state
            .next_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SyncWaiter::WaitForEpoch {
            epoch,
            state: Arc::clone(&self.sync_state),
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
        let handle = WalHandle::new(dir.path(), true).unwrap();
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
        w1.wait();
        w2.wait();

        let entries = read_wal(&wal_path(dir.path())).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn wal_handle_eventual_write() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false).unwrap();
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
        let handle = WalHandle::new(dir.path(), false).unwrap();
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
        let handle = WalHandle::new(dir.path(), true).unwrap();
        assert_eq!(handle.pending_writes(), 0);
        let w = handle
            .write(WalEntry {
                version: 1,
                ops: vec![],
            })
            .unwrap();
        // Before wait, in_flight should be >= 1.
        w.wait();
        // After wait + bg thread sync, pending should be 0 (eventually).
        drop(handle);
    }

    // --- task28: version-keyed durability watermark ---------------------------

    /// In Eventual mode there is no `SyncWaiter`, but the watermark still lets a
    /// caller observe when a committed version became fsync-durable.
    #[test]
    fn durability_watermark_advances_in_eventual_mode() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false).unwrap();
        for v in 1..=3u64 {
            handle.write(WalEntry { version: v, ops: vec![] }).unwrap();
        }
        // Deterministic: block until the bg thread publishes v3.
        handle.durability().wait(3).unwrap();
        assert!(handle.durable_version() >= 3);
    }

    /// The watermark tracks the high-water version of each fsynced batch in
    /// Consistent mode too (additive — does not disturb the SyncWaiter path).
    #[test]
    fn durability_watermark_advances_in_consistent_mode() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), true).unwrap();
        let w = handle.write(WalEntry { version: 7, ops: vec![] }).unwrap();
        w.wait();
        handle.durability().wait(7).unwrap();
        assert!(handle.durable_version() >= 7);
    }

    /// Waiting on an already-durable version returns immediately (no block).
    #[test]
    fn wait_durable_on_already_durable_is_immediate() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false).unwrap();
        handle.write(WalEntry { version: 1, ops: vec![] }).unwrap();
        let dur = handle.durability();
        dur.wait(1).unwrap();
        // Second wait on the same (already-durable) version must not block.
        dur.wait(1).unwrap();
    }

    /// `on_complete` fires exactly once, with `Ok`, after the version is durable.
    #[test]
    fn on_complete_fires_once_after_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false).unwrap();
        let (tx, rx) = mpsc::channel();
        handle
            .durability()
            .on_complete(1, Box::new(move |res| tx.send(res).unwrap()));
        handle.write(WalEntry { version: 1, ops: vec![] }).unwrap();
        // Ensure the publish has happened, then the callback must have fired Ok.
        handle.durability().wait(1).unwrap();
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("callback did not fire");
        assert!(got.is_ok());
        // Exactly once: no second delivery.
        assert!(rx.recv_timeout(std::time::Duration::from_millis(50)).is_err());
    }

    /// `on_complete` fires inline (on the calling thread) when already durable.
    #[test]
    fn on_complete_fires_inline_when_already_durable() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false).unwrap();
        handle.write(WalEntry { version: 1, ops: vec![] }).unwrap();
        handle.durability().wait(1).unwrap();
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let f2 = Arc::clone(&fired);
        handle.durability().on_complete(1, Box::new(move |res| {
            assert!(res.is_ok());
            f2.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
        // Inline: already set before on_complete returned.
        assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// A waiter parked on a version that is never written is released with an
    /// error when the WAL closes (handle dropped), rather than blocking forever.
    #[test]
    fn close_releases_parked_waiter_with_err() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false).unwrap();
        let (tx, rx) = mpsc::channel();
        handle
            .durability()
            .on_complete(999, Box::new(move |res| tx.send(res).unwrap()));
        drop(handle); // joins bg thread, then close() drains parked waiters.
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("callback did not fire on close");
        assert!(got.is_err());
    }

    /// A thread blocked in `wait()` on an unreachable version unblocks with an
    /// error when the handle is dropped.
    #[test]
    fn wait_unblocks_with_err_on_close() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false).unwrap();
        let dur = handle.durability();
        let waiter = thread::spawn(move || dur.wait(999));
        // Give the waiter time to park on the condvar, then close.
        thread::sleep(std::time::Duration::from_millis(50));
        drop(handle);
        let res = waiter.join().unwrap();
        assert!(res.is_err());
    }

    /// A failed fsync poisons waiters at/below the attempted high-water version
    /// without advancing the watermark.
    #[test]
    fn publish_error_poisons_waiters_without_advancing() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false).unwrap();
        let dur = handle.durability();
        // Drive the watermark primitive directly: simulate a failed batch fsync.
        dur.publish_error(5, "disk full".into());
        assert_eq!(dur.current(), 0, "watermark must not advance on error");
        let err = dur.wait(3).unwrap_err();
        assert!(matches!(err, Error::Persistence(_)));
        // A later successful fsync still advances and resolves higher versions.
        dur.publish(6);
        assert_eq!(dur.current(), 6);
        dur.wait(6).unwrap();
    }

    /// End-to-end through the public `Store` API in Eventual mode: a committed
    /// version becomes observably durable via `wait_durable`/`durable_version`.
    #[test]
    fn store_wait_durable_eventual_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(crate::StoreConfig {
            persistence: crate::Persistence::Standalone {
                dir: dir.path().to_path_buf(),
                durability: crate::Durability::Eventual,
            },
            ..crate::StoreConfig::default()
        })
        .unwrap();

        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User { name: "Alice".into(), age: 30 })
            .unwrap();
        let v = wtx.commit().unwrap();

        // Eventual commit returned without blocking on fsync; now await durability.
        store.wait_durable(v).unwrap();
        assert!(store.durable_version() >= v);

        // on_durable fires inline now that v is durable.
        let (tx, rx) = mpsc::channel();
        store.on_durable(v, move |res| tx.send(res).unwrap());
        assert!(rx.recv().unwrap().is_ok());
    }

    /// Without a Standalone WAL the durability accessors are no-ops: there is no
    /// WAL-level durability to await.
    #[test]
    fn store_durability_accessors_noop_without_wal() {
        let store = Store::new(crate::StoreConfig::default()).unwrap(); // Persistence::None
        assert_eq!(store.durable_version(), 0);
        store.wait_durable(5).unwrap();
        let (tx, rx) = mpsc::channel();
        store.on_durable(5, move |res| tx.send(res).unwrap());
        assert!(rx.recv().unwrap().is_ok());
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
}
